package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import eu.wohlben.qits.eventstream.persistence.ConsumedEvents;
import eu.wohlben.qits.eventstream.persistence.ConsumerWatermarkRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Reads the log forward for every {@link QitsDurableEventListener}, from its watermark to the head.
 *
 * <p>This is the cure for the thing the stream cannot do. A frame broadcast while a consumer is
 * down, restarting or mid-cutover is gone from the socket's point of view; it is still in
 * qits-events' log, and this pages it back out. Hence the two moments it runs: <b>on a schedule</b>,
 * so a dropped connection costs at most {@code qits.eventstream.catchup-interval}, and <b>once at
 * startup</b>, which is the cutover case exactly.
 *
 * <p>Per listener, the loop is short and every line of it is a decision:
 *
 * <ul>
 *   <li><b>No watermark yet?</b> Initialize it at the newest matching event and handle nothing —
 *       consume-from-now. Replaying all of history into a consumer that has never run is never the
 *       default; {@link QitsDurableEventListener#replayFromEpoch()} is the opt-in.
 *   <li><b>Page ascending from the watermark</b>, and run every row through {@link DurableFunnel} —
 *       the same funnel the live stream uses, so the claim rows dedupe whatever the stream already
 *       delivered.
 *   <li><b>Advance the watermark only when a page is processed in full.</b> A handler that threw
 *       leaves the sweep where it stood, so the event is offered again next time instead of being
 *       read past.
 *   <li><b>Prune</b> the claims the watermark has left behind by more than the horizon. Below the
 *       watermark an event is settled forever, so its claim protects against nothing.
 * </ul>
 *
 * <p><b>Live frames never advance the watermark</b>, and that asymmetry is the design rather than an
 * omission: a frame is ahead of the watermark by definition, and treating it as progress would read
 * past everything between the two. The claim row is what stops the sweep from handling it twice when
 * it eventually gets there.
 *
 * <p>{@link #catchUp()} is public and returns what it did, which is how the paging is tested: a
 * suite disables the scheduler and the startup run and drives this by hand, exactly as it does with
 * {@link OutboxSweeper}.
 */
@ApplicationScoped
public class CatchupSweeper {

  private static final Logger LOG = Logger.getLogger(CatchupSweeper.class);

  /**
   * One page's bite. A constant rather than a config key, like {@code Outbox.SWEEP_BATCH}: it is
   * bounded by the log's own {@code limit} cap of 1000, and the loop pages until it reaches the
   * head, so this changes how many round trips a backlog costs and nothing that is visible.
   */
  static final int PAGE_SIZE = 200;

  @Inject EventDispatcher dispatcher;

  @Inject EventsQuery events;

  @Inject DurableFunnel funnel;

  @Inject ConsumerWatermarkRepository watermarks;

  @Inject ConsumedEvents consumed;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  @ConfigProperty(name = "qits.eventstream.catchup-at-startup", defaultValue = "true")
  boolean catchUpAtStartup;

  @ConfigProperty(name = "qits.eventstream.prune-horizon", defaultValue = "P1D")
  Duration pruneHorizon;

  /**
   * The scheduled tick. {@code SKIP} for the reason the outbox's has it: a sweep still paging
   * through a backlog when the next one fires must not have a second thread offering the same rows —
   * the claim would hold, but the work would be done twice.
   */
  @Scheduled(
      every = "{qits.eventstream.catchup-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    catchUp();
  }

  /**
   * The startup run — the cutover cure, and the reason a restart is not a hole.
   *
   * <p><b>On its own thread, because boot must not wait for it.</b> An unreachable qits-events would
   * otherwise add a connect timeout per listener to every start, and a real backlog would add the
   * time it takes to drain. Nothing about the application depends on the sweep having finished.
   *
   * <p>{@code qits.eventstream.catchup-at-startup=false} turns it off, which is what a test suite
   * wants: a sweep landing behind a test is the same kind of non-determinism {@code
   * quarkus.scheduler.enabled=false} already keeps out, and a suite drives {@link #catchUp()}
   * itself.
   */
  void onStart(@Observes StartupEvent ignored) {
    if (!enabled || !catchUpAtStartup || dispatcher.durableListeners().isEmpty()) {
      return;
    }
    Thread.ofVirtual().name("eventstream-catchup-startup").start(this::catchUp);
  }

  /** Catch every durable listener up to the head of the log. Returns how many events were handled. */
  public int catchUp() {
    if (!enabled) {
      return 0;
    }
    int handled = 0;
    for (QitsDurableEventListener listener : dispatcher.durableListeners()) {
      try {
        handled += catchUp(listener);
      } catch (EventsQuery.Unavailable unreachable) {
        // One line per listener per sweep, and the sweep is thirty-secondly: the log being
        // unreachable is a condition rather than an event, and nothing is lost by it — the
        // watermark stayed where it was and the next sweep reads the same rows.
        LOG.warnf("catch-up could not read the log: %s", unreachable.getMessage());
      } catch (RuntimeException e) {
        LOG.errorf(e, "catch-up failed for %s", listener.getClass().getName());
      }
    }
    return handled;
  }

  private int catchUp(QitsDurableEventListener listener) {
    String consumerId = listener.consumerId();
    if (consumerId == null || consumerId.isBlank()) {
      LOG.errorf(
          "durable listener %s has no consumerId; it cannot be caught up",
          listener.getClass().getName());
      return 0;
    }
    Set<String> names = listener.signatures();
    if (names == null || names.isEmpty()) {
      // Wants nothing, so there is nothing to be behind on — and no watermark is written, which
      // keeps a listener that has not made its mind up yet out of the tables entirely.
      return 0;
    }

    ConsumerWatermark mark = read(consumerId);
    if (mark == null) {
      initialize(listener, consumerId, names);
      return 0;
    }

    int handled = 0;
    String cursor = cursorOf(mark);
    while (true) {
      EventPage page = events.after(names, cursor, PAGE_SIZE);
      List<EventFrame> rows = page.events();
      if (rows == null || rows.isEmpty()) {
        break;
      }
      for (EventFrame frame : rows) {
        DurableFunnel.Result result = funnel.offer(listener, frame);
        if (result == DurableFunnel.Result.FAILED) {
          // The page is not processed, so the watermark does not move. Everything before this row
          // on this page is claimed and will be skipped next time; this row is owed.
          LOG.warnf(
              "catch-up for %s stopped at %s; the watermark stays at %s",
              consumerId, frame.id(), cursor);
          return handled;
        }
        if (result == DurableFunnel.Result.HANDLED) {
          handled++;
        }
      }
      EventFrame last = rows.get(rows.size() - 1);
      cursor = advance(consumerId, last);
      if (page.nextCursor() == null) {
        // The log says this was the last page. A full page is NOT the signal — that is the one
        // thing a reader of this route must not infer for itself.
        break;
      }
    }
    prune(consumerId);
    return handled;
  }

  /**
   * A consumer with no watermark starts at the <b>head</b> of the log.
   *
   * <p>The alternative — the epoch — would make the first deployment of any new subscriber replay
   * every matching event the platform has ever recorded, acting a second time on things something
   * else acted on months ago. {@link QitsDurableEventListener#replayFromEpoch()} is there for the
   * consumers that genuinely want that, and it is consulted here and nowhere else.
   *
   * <p>An empty answer is the same place as the epoch and is stored as such: a consumer with nothing
   * behind it has nothing to skip.
   *
   * <p>If the log cannot be read, <b>no watermark is written at all</b> and {@link
   * EventsQuery.Unavailable} propagates. Initializing to a guess would settle every event of the
   * outage.
   */
  private void initialize(QitsDurableEventListener listener, String consumerId, Set<String> names) {
    if (listener.replayFromEpoch()) {
      write(consumerId, Instant.EPOCH, null);
      LOG.infof("durable consumer %s initialized at the start of the log, as it asked", consumerId);
      return;
    }
    EventFrame newest = events.newest(names);
    if (newest == null) {
      write(consumerId, Instant.EPOCH, null);
      LOG.infof("durable consumer %s initialized: the log holds nothing it wants yet", consumerId);
      return;
    }
    write(consumerId, newest.occurredAt(), newest.id());
    LOG.infof(
        "durable consumer %s initialized at the head of the log (%s); it consumes from now on",
        consumerId, newest.id());
  }

  private ConsumerWatermark read(String consumerId) {
    return QuarkusTransaction.requiringNew().call(() -> watermarks.findById(consumerId));
  }

  private void write(String consumerId, Instant occurredAt, String eventId) {
    QuarkusTransaction.requiringNew().run(() -> watermarks.put(consumerId, occurredAt, eventId));
  }

  /** Move the watermark to this row and return the cursor that resumes after it. */
  private String advance(String consumerId, EventFrame last) {
    write(consumerId, last.occurredAt(), last.id());
    return last.occurredAt() + "," + last.id();
  }

  /**
   * A watermark's cursor, or null for "the start of the log" — a null {@code eventId} is the
   * before-the-first-row state, and the log answers a blank cursor id with a 400, so the absence has
   * to be the absence of the parameter.
   */
  private static String cursorOf(ConsumerWatermark mark) {
    return mark.eventId == null ? null : mark.occurredAt + "," + mark.eventId;
  }

  /**
   * Drop the claims the watermark has left behind. The horizon is generous on purpose: the cut mixes
   * the log's clock (the watermark) with this consumer's (the claim), and a day of slack is more
   * than any two hosts of one platform will differ by.
   */
  private void prune(String consumerId) {
    ConsumerWatermark mark = read(consumerId);
    if (mark == null) {
      return;
    }
    Instant cut = mark.occurredAt.minus(pruneHorizon);
    int dropped = QuarkusTransaction.requiringNew().call(() -> consumed.pruneBefore(consumerId, cut));
    if (dropped > 0) {
      LOG.debugf("pruned %d settled claim(s) for %s", dropped, consumerId);
    }
  }
}
