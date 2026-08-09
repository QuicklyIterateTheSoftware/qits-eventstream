# qits-eventstream

The platform's **event bus client**. A service that carries this jar can announce that something
happened, and can be told when something happened elsewhere. The far end is
[qits-events](https://github.com/QuicklyIterateTheSoftware/qits-events), which stores the log and
broadcasts the stream; everything here is the client half of that.

**It enables event STREAMING. It does not do event sourcing.** There is no event store here, no
aggregate, no replay, and the one table it owns is a retry queue that is *empty* when things are
working. The module was called `eventsourcing` for its first four commits, inside qits-ci, and the
name is the only thing about it that changed on the way out. The superproject's
`eventsourcing-plan.md` and `event-causation-plan.md` — where the design is argued — still carry the
old spelling, because they are documents that were written rather than code that runs.

    <dependency>
        <groupId>eu.wohlben.qits</groupId>
        <artifactId>qits-eventstream</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

Quarkus, JDK 25. A consumer needs no other wiring: the jar carries its own beans index, its own
config defaults and its own database.

## The whole public surface

Five types. Everything else in here is how they are kept.

| | |
|---|---|
| `QitsEvent` | something that happened. Implement it on a record. |
| `QitsEventBus` | `publish(event)` / `publish(event, parentEventId)`. Inject it. |
| `QitsEventListener<E>` | consume one event type. Implement it on a bean. |
| `QitsRawEventListener` | consume frames for names chosen at runtime. |
| `CausationScope` | the ambient cause, for carrying an edge across a thread. |

### Publishing

```java
public record BuildSuccessful(String repoId, String runId, Instant finishedAt) implements QitsEvent {
  // The interface's four methods are excluded from the payload, so the record's own components are
  // the whole wire body. An event may hold its eventId as an ordinary component; the mix-in hides
  // the accessor, and identity travels in the envelope.
}

@Inject QitsEventBus bus;

bus.publish(new BuildSuccessful(repoId, runId, finishedAt));
```

`publish` **never throws and never blocks for long**. It attempts the idempotent
`PUT /events/api/events/{id}` inline, and if that does not land within
`qits.eventstream.publish-timeout` (5s) the event goes to the outbox and a scheduled sweeper owns
delivery from there. A caller therefore never sees a delivery failure and never has to decide what
to do about one — which is what lets a publish sit in the middle of a business transition without
becoming a way for that transition to fail.

### Consuming

```java
@ApplicationScoped
public class BuildSuccessfulListener implements QitsEventListener<BuildSuccessful> {

  @Override public Class<BuildSuccessful> eventType() { return BuildSuccessful.class; }

  @Override public void onEvent(BuildSuccessful event) { ... }
}
```

That is the whole registration. No channel name, no annotation, no configuration. The subscriber
collects every listener bean at startup, subscribes to the union of their event names, and dials
qits-events' `/events/stream` on `StartupEvent`. **An application with no listener beans never dials
at all** — there is nothing to subscribe to.

**Delivery is at-most-once and the stream is live-only.** A listener is not a queue consumer: it
sees what is broadcast while it is connected, and events that occurred during a disconnect are not
replayed. Catch-up from the event log is a separate, later feature. So a listener may do anything
that tolerates being skipped, and nothing that must happen exactly once.

`onEvent` runs on a worker thread, one frame at a time, and a throw is logged and swallowed rather
than taking the socket down. Anything slow belongs on the listener's own executor — see *Causation*
below for what that costs and how to pay it.

### Consuming frames instead of types

`QitsRawEventListener` names a `Set<String>` of event *names* at runtime and receives the
`EventFrame` itself. **Reach for it only when the interest is genuinely unknowable at startup** — a
trigger engine whose selections live in files inside other repositories, an audit sink. A raw
listener that could have named its event type is a typed listener with extra steps.

The subscribe frame is the **union** of every typed listener's signature and every raw listener's
current set, sorted. The literal `"*"` (`QitsRawEventListener.ALL`) anywhere in that union collapses
the whole frame to `["*"]`: once one consumer wants everything, narrowing the wire buys nothing, and
the surplus frames are dropped in dispatch at no cost to anyone else.

`signatures()` is asked per subscribe **and per frame**, which is what makes the set dynamic. One
edge follows from that and is worth knowing: a *widened* set takes effect for dispatch immediately
but reaches qits-events only at the next reconnect, since the subscription lives on the connection
and nothing re-dials on a listener changing its mind. **A listener whose interest can grow should
return `Set.of(ALL)` once and filter for itself.**

Dispatch order is **typed first, raw second**, and it is a contract rather than an accident. Both
run for a frame both want, each listener gets it once, and containment is symmetric: a throw out of
`onFrame`, or out of `signatures()`, costs that listener and nobody else.

### Causation

The envelope carries a nullable `parentId` — the event that caused this one — and `publish` is the
only place it is resolved. **The precedence rule, whole: an explicit non-null argument wins; a null
or absent one falls back to `CausationScope.current()`; outside any scope the event is a root.** So
`publish(e)` *is* `publish(e, null)`.

The dispatcher runs every listener for a frame inside one `CausationScope` of that frame's id, so a
listener that publishes *on the dispatch thread* records the edge with nobody passing an argument. A
hand-off to your own executor leaves that scope behind — a plain `ThreadLocal` does not follow work,
deliberately — so carry it:

```java
UUID cause = CausationScope.current();                     // on the dispatch thread
executor.submit(() -> CausationScope.with(cause, () -> bus.publish(followUp)));
```

or pass it: `bus.publish(followUp, cause)`. What is not an option is doing neither and believing the
chain was recorded: **a dropped parent is a root event, and that loss cannot be backfilled from
anything.**

`CausationScope.with(null, …)` is the deliberate detach — a statement about a region, as against
`publish(e, null)`, which only means "I have no argument to pass". The asymmetry is settled.

## Configuration

Shipped as `META-INF/microprofile-config.properties` at **ordinal 100**, so the consuming
application (250) and the environment (300) override any of it. A library jar's own
`application.properties` would be ignored, which is why the defaults live where they do.

| key | default | |
|---|---|---|
| `qits.events.url` | `http://qits-events:8080` | scheme + host + port, **no path**. This module appends `/events/api/events/{id}` and `/events/stream` itself, swapping the scheme to `ws(s)` for the second. A path here yields a doubled one and a 404 nothing retries out of. |
| `qits.eventstream.enabled` | `true` | the master switch. |
| `qits.eventstream.publish-timeout` | `PT5S` | the inline attempt's deadline, after which the outbox owns the event. |
| `qits.eventstream.max-attempts` | `5` | **counts the inline attempt**, so five means the PUT plus four sweeps. |
| `qits.eventstream.sweep-interval` | `10s` | how often the sweeper looks. A floor on how late a retry can be, never a cause of an early one. |
| `qits.eventstream.redial-initial-backoff` | `PT1S` | doubled per consecutive failure. |
| `qits.eventstream.redial-max-backoff` | `PT30S` | the cap. |

The default `qits.events.url` is the qits-net alias, which is right for any deployment on that
network and wrong for a host-run process — a stack that publishes qits-events on a mapped localhost
port overrides it.

**The switch ships ON, and the darkness belongs to the consumer.** A library that shipped dark is a
library whose first deployment discovers it was never wired up. The `%dev` / `%test` `false` belongs
in the consuming application's `application.properties`, exactly where it goes for OTel — so a
`quarkus:dev` with no qits-events on the far side makes no dials rather than retries. Off means
`publish()` is a debug log, the sweeper does nothing and the subscriber never dials. There is no
half-enabled state.

### The database, and the resource a deployment must declare

This jar owns its **own** named datasource, persistence unit and Flyway lineage — `eventstream`,
migrations at `db/eventstream/migration` — and never shares the consuming service's database or its
migration history. **The store is PostgreSQL**, reached through the platform's generic resource
contract:

    quarkus.datasource.eventstream.db-kind=postgresql
    quarkus.datasource.eventstream.jdbc.url=${QITS_RESOURCE_EVENTSTREAM_URL}
    quarkus.datasource.eventstream.username=${QITS_RESOURCE_EVENTSTREAM_USERNAME}
    quarkus.datasource.eventstream.password=${QITS_RESOURCE_EVENTSTREAM_PASSWORD}

**Adding this jar to a deployable adds one line to its deployment spec.** The consuming repository
declares the resource in `.config/qits/deployments.yml` —

    resources: postgresql:eventstream:<database>

— and qits-deployments creates the role and the database before the cutover, then injects those
three variables into the container. **The resource must be named `eventstream`**: the variable names
follow the name, so a spec that calls it anything else leaves this jar's expressions unresolved.

**The triple has no defaults, and that is the refuse-to-boot stance.** An unset variable is an
unresolvable expression, so the process dies at Flyway naming what is missing rather than opening
some fallback store nobody meant. There is no local file to fall back to any more — which retires
the `${user.home}` default that once cost a rollout, since a container with no `HOME` resolved it to
`?` and only the packaged artifact in its real environment ever found out.

**`enabled=false` does not stop the datasource.** Quarkus opens the connection and runs Flyway at
boot regardless, so a consuming **test suite** must point `quarkus.datasource.eventstream` at a
database of its own — the resource variables are a deployment fact and a suite has none.

## What is on the wire

The `PUT` body, and — plus the row's id — what comes back out of the stream:

```json
{"description":null,"name":"BuildSuccessful","occurredAt":"2026-07-31T12:46:03Z",
 "parentId":"6c3f2b1a-…","payload":"{\"repoId\":\"…\",\"runId\":\"…\"}"}
```

`payload` is the event's own fields as a canonical JSON **string** — a string, not a nested object,
because the server stores and compares it verbatim and never has to parse it. Everything `QitsEvent`
declares is excluded from it: identity and causation travel in the envelope.

**Canonical means the string is a function of the value and of nothing else.** Keys sorted, no
insignificant whitespace, absent fields omitted rather than written as explicit nulls. qits-events
compares `name` + `occurredAt` + `payload` + `parentId` byte-for-byte to tell an idempotent replay
(200) from a reused UUID (400), so two serializations of one event that differ by a space are, to
the far end, a contradiction. `CanonicalJson` therefore builds its **own** `ObjectMapper` and sets
every knob explicitly. Read AGENTS.md before touching any of it.

`eventId` is fixed at construction and **never regenerated**. It is the `{id}` of the PUT, which is
the only reason a retry is safe: a request whose response was lost replays as a 200 instead of
writing the event twice.

## The outbox

**Failure-path only, and empty in a healthy process.** A publish that lands writes nothing; a row
that is delivered on retry is deleted. So the row count is a health signal rather than a log — the
log is qits-events — and a monitoring check on this table is asking a real question.

Attempts are spaced `1s · 4^(n-1)`, capped at five minutes, held per row in `next_attempt_at`. Two
ways to stop: the budget runs out, or a 400 comes back, which means a UUID was reused and no amount
of retrying will change it. Both log; both leave the row `FAILED`.

The known hole is named in `OutboxEvent`'s javadoc and deliberately left open: a crash between the
inline attempt failing and the row committing loses the event.

## Building

    ./mvnw verify

**A clone of this repo alone builds and tests green** — no monorepo, no docker, no credentials, no
prior `mvn install` anywhere. That is the gate, and it is the reason this pom duplicates versions
instead of inheriting them. The suite starts its own stub qits-events on a JDK `HttpServer` and runs
the outbox on a **real postgres it spawns itself** — zonky's binaries, resolved as ordinary Maven
artifacts and started as a child process, never a container — so nothing is skipped for want of
infrastructure. 76 tests, about fifteen seconds.

`.sdkmanrc` names `25.0.2-graalce`. The jar compiles into a consumer's GraalVM native image, but
**the consumer owns the reflection registration** — read AGENTS.md's section on it before shipping a
binary, because the failure it describes is silent.

## Working on it

`AGENTS.md` (symlinked as `CLAUDE.md`) is the rest: the rules that bite, each with the measurement
that bought it.
