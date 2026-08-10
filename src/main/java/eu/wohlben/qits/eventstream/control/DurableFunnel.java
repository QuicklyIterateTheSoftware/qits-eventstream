package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.CausationScope;
import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.persistence.ConsumedEvents;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * <b>One funnel, both channels.</b> Every event a {@link QitsDurableEventListener} ever sees passes
 * through this one method — a live frame off the stream and a row read back by {@link
 * CatchupSweeper} take the identical path — and that is what makes the guarantee statable at all:
 * <b>exactly-once effect per (listener, event id)</b>, whatever mix of stream delivery, catch-up and
 * publisher retry produced the arrivals.
 *
 * <p>The path is three steps and one transaction:
 *
 * <ol>
 *   <li>ask the listener whether it {@code selects} the event — if not, nothing is stored and
 *       nothing runs, which is what keeps the claim ledger proportional to the work;
 *   <li>claim the event in {@code consumed_event}. A duplicate key means another channel already
 *       handled it, and the answer is to say nothing and move on;
 *   <li>call the handler, <em>inside</em> the same transaction.
 * </ol>
 *
 * <p><b>Step three being inside step two is the failure policy.</b> A handler that throws rolls its
 * claim back with it, so the event stays owed: the watermark does not pass it, and the next sweep
 * offers it again. The handler's own database writes join that transaction too, so the effect and
 * the claim commit together or not at all — which is the difference between "we recorded that we
 * handled it" and "we handled it".
 *
 * <p>The scope of the arriving event is established here rather than by the caller, because there
 * are two callers and they must not differ: a follow-up published from a handler records the same
 * parent whether the event came off the socket or out of the log.
 */
@ApplicationScoped
public class DurableFunnel {

  private static final Logger LOG = Logger.getLogger(DurableFunnel.class);

  /** What became of one offer. */
  public enum Result {

    /** Selected, claimed, and the handler returned. */
    HANDLED,

    /** Not selected, or already claimed by the other channel. Settled either way. */
    SKIPPED,

    /** The predicate or the handler threw. Nothing was committed and the event is still owed. */
    FAILED
  }

  @Inject ConsumedEvents consumed;

  @Inject Clock clock;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  /**
   * Offer one event to one durable listener.
   *
   * <p>Never throws: both callers are loops over other people's code — a socket callback and a
   * scheduler tick — and one listener's failure is that listener's, reported as {@link
   * Result#FAILED} so the caller can leave the event owed rather than settle it.
   */
  public Result offer(QitsDurableEventListener listener, EventFrame frame) {
    if (!enabled) {
      // Belt and braces: with the module dark nothing dials and nothing sweeps, so there is no
      // caller — but "no tables touched at runtime" is the darkness contract and it is cheaper to
      // make it structural here than to prove it about two other classes.
      return Result.SKIPPED;
    }
    if (frame.id() == null || frame.id().isBlank()) {
      // Nothing to key a claim on, so exactly-once is not available for it. Louder than a debug:
      // qits-events always sends an id, so an event without one is a contract failure somewhere.
      LOG.warnf("durable delivery of %s skipped: the frame carries no id", frame.name());
      return Result.SKIPPED;
    }
    String consumerId;
    try {
      consumerId = listener.consumerId();
      if (consumerId == null || consumerId.isBlank()) {
        LOG.errorf(
            "durable listener %s has no consumerId; it can store nothing",
            listener.getClass().getName());
        return Result.FAILED;
      }
      if (!listener.selects(frame)) {
        return Result.SKIPPED;
      }
    } catch (RuntimeException e) {
      // An event whose selection could not be decided is owed, not settled. Answering "no" here
      // would let the watermark pass an event the listener may well have wanted.
      LOG.errorf(
          e,
          "durable listener %s could not decide on %s %s; the event stays owed",
          listener.getClass().getName(),
          frame.name(),
          frame.id());
      return Result.FAILED;
    }
    try {
      boolean claimed =
          QuarkusTransaction.requiringNew()
              .call(
                  () -> {
                    if (!consumed.claim(consumerId, frame.id(), clock.instant())) {
                      return false;
                    }
                    CausationScope.with(
                        EventDispatcher.causeOf(frame), () -> listener.onFrame(frame));
                    return true;
                  });
      if (claimed) {
        LOG.debugf("durable listener %s handled %s %s", consumerId, frame.name(), frame.id());
        return Result.HANDLED;
      }
      LOG.debugf("durable listener %s had already handled %s", consumerId, frame.id());
      return Result.SKIPPED;
    } catch (Exception e) {
      LOG.errorf(
          e,
          "durable listener %s failed on %s %s; the claim was rolled back and the event stays owed",
          consumerId,
          frame.name(),
          frame.id());
      return Result.FAILED;
    }
  }
}
