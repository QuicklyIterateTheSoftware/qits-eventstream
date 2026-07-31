# qits-eventstream — working notes

Read `README.md` first: it defines the public surface, the wire contract and the config surface.
This file is the working conventions on top of it, and the rules that bite. Every one of them was
bought by something going wrong, and the measurement is kept beside the rule because a rule without
its reason gets "simplified" by the next person.

## The rule that shapes everything

**A clone of this repo alone builds and tests green.** `./mvnw verify` — no monorepo, no docker, no
credentials, no prior `mvn install` anywhere, no submodule to initialise. Anything that would break
that is not a tradeoff to weigh, it is the thing this repo exists to make possible.

That is why the pom duplicates versions instead of inheriting them, and why the suite stands up its
own stub qits-events (`StubEventsServer`, a JDK `HttpServer` and a `websockets-next` client dialling
it) rather than needing a real one. There are no integration tests and no `skipITs` property: the
whole suite is surefire, and there is nothing here that only real infrastructure could exercise.

**This is a library, so there is one maven module at the root and no `service/` split.** The repo
produces one jar. The pom is the parent and the module at once.

## Where things live

`eu.wohlben.qits.eventstream.*`:

- the root package — `QitsEvent`, `QitsEventBus`, `QitsEventListener`, `QitsRawEventListener`,
  `CausationScope`. The public surface, and the only things a consumer should name.
- `control/` — `CanonicalJson`, `EventsPublisher`, `Outbox`, `OutboxSweeper`, `RetrySchedule`,
  `EventDispatcher`, `EventStreamSubscriber`, `EventstreamClock`, and the two wire records
  `EventEnvelope` and `EventFrame`. `EventFrame` is public because a raw listener receives it.
- `entity/`, `persistence/` — the outbox row and its Panache repository, in this jar's own named
  persistence unit.

**THE EXTRACTION RULE, which outlived the extraction.** No `eu.wohlben.qits.*` package other than
this one may appear in these sources, main or test. It was written inside qits-ci to keep lifting
this module out a `git mv` plus a pom; that is done, but the direction it protects is permanent — a
"temporary" dependency pointing back at a consumer is inherited by every other consumer of this jar,
and the split is undone. `ExtractionRuleTest` greps the sources, so it fails a build rather than a
review. Its javadoc argues the widening.

## The six things that are easy to get wrong

- **The canonical form is a wire contract, not a formatting preference.** qits-events stores the
  `payload` string verbatim and compares it byte-for-byte to tell an idempotent replay (200) from a
  reused UUID (400), so two serializations of one event that differ by a space are a contradiction
  to the other side. `CanonicalJson` therefore builds its **own** `ObjectMapper` rather than
  injecting the CDI one — a consuming application's `ObjectMapperCustomizer`s must not be able to
  reach it — and sets every knob that could vary explicitly. Its class javadoc names each and why.
  Do not "fix" a serialization problem by injecting the CDI mapper.
- **`eventId` is fixed at construction and never regenerated.** It is the `{id}` of the PUT, which
  is the only reason a retry is safe: a request whose response was lost replays as a 200 instead of
  writing the event twice. An event class may hold it as an ordinary record component; the library
  keeps everything `QitsEvent` declares out of the payload, so identity travels in the envelope.
- **The publisher's `HttpClient` is pinned to HTTP/1.1.** The JDK default is HTTP/2 with an `h2c`
  upgrade, and an upgrade carrying a request body **delivers that body twice** — measured against
  the test stub, once through the server's upgrade handler and again as an HTTP/2 data frame ninety
  milliseconds later. Idempotency made it harmless and therefore invisible; it was a doubled request
  on every publish. Do not drop the `version(...)` line. The client is an **instance** field, not a
  static one: a static `HttpClient` is created at image build time and native-image refuses the heap
  it lands in.
- **The outbox is failure-path-only, and empty in a healthy process.** A publish that lands writes
  nothing; a row that is delivered on retry is deleted. So the row count is a health signal rather
  than a log, and the log is qits-events. The known hole — a crash between the inline attempt
  failing and the row committing — is named in `OutboxEvent`'s javadoc and deliberately left open.
- **Causation is stamped by the bus, in the envelope, and it never touches an event class.**
  `EventEnvelope` carries a nullable `parentId` and `QitsEventBus.publish` is the only place it is
  resolved. The precedence is the whole rule: **an explicit non-null argument wins; a null or absent
  one falls back to `CausationScope.current()`; outside any scope the event is a root.** So
  `publish(e)` *is* `publish(e, null)` — one implementation, one call shape — and the deliberate
  detach is spelled `CausationScope.with(null, …)`, which is a statement about a region rather than
  about one call. That asymmetry is settled, not an oversight.

  `QitsEvent` gained **no** fifth method and no event class gained a component, which is the
  decision rather than an omission. A record is immutable and its parent is known later than it is;
  a default method reading a thread-local would answer differently on the sweeper's thread an hour
  later, which is exactly what `eventId`'s stability argument forbids; and a fifth accessor would
  need a fifth `@JsonIgnore` in `CanonicalJson`'s mix-in — the one place this code has already been
  bitten silently. `CanonicalJson` and its mix-in are therefore **unchanged**, and a payload is
  byte-identical whether the event was published under a parent or not. That last property has a
  test, because it is the same lesson the mix-in taught.

  **The outbox's `parent_id` column is the load-bearing line of the whole feature.** qits-events
  compares `name` + `occurredAt` + `payload` + `parentId` — `description` stays outside it — so a
  sweeper that rebuilt the envelope without the parent would send a *different* request than the
  attempt it is retrying: a 400 against its own landed first attempt, or a caused event quietly
  republished as a chain root. The parent is fixed when the envelope is built and stored with it,
  exactly as the payload is, and the sweep re-reads nothing ambient. `CausationStampingTest` sweeps
  inside a *different* scope to prove it.

  **`CausationScope` is public API for one reason**, and it is the reason to keep it public: this
  library tells listeners that "anything slow belongs on the listener's own executor", and a
  hand-off to an executor is precisely what drops the ambient value. A plain `ThreadLocal` does not
  follow work, deliberately — inheritance copies at thread *creation*, which pooled executors do
  long before any consumption. Advice a library gives has to come with the bridge that makes it
  safe; both forms are in `QitsEventListener`'s javadoc, which is where a listener author looks.

  **No cycle guard and no self-parent repair, here or anywhere on this side.** A guard that catches
  only `A → A` cannot see `A → B → A` and its presence would tell a reader that cycles are handled;
  detection belongs where the graph is visible. qits-events does reject a self-edge, because that
  one is decidable from a single row.
- **There are two consuming seams, and the typed one is the one to reach for.** `QitsEventListener<E>`
  names an event *class* at compile time; `QitsRawEventListener` names a `Set<String>` of event
  *names* at runtime and receives the `EventFrame` itself. The raw one exists for consumers whose
  interest is genuinely unknowable at startup. A raw listener that could have named its event type
  is a typed listener with extra steps.

  **The subscribe frame is the union of both, and `"*"` collapses it.** `EventDispatcher.signatures()`
  unions every typed listener's signature with every raw listener's current set, sorted; the literal
  `"*"` anywhere in that union makes the whole frame `["*"]`. `SubscriptionUnionTest` exhausts the
  arithmetic; `EventStreamSubscriberTest` asserts once, on the wire, that this is the function
  actually used.

  **The bean set is resolved once; a raw listener's signature set is asked per subscribe and per
  frame.** That is what makes it dynamic, and it has one edge: a *widened* set takes effect for
  dispatch immediately but reaches qits-events only at the next reconnect, since the subscription
  lives on the connection and nothing re-dials on a listener changing its mind. So a consumer whose
  interest can grow should return `Set.of(ALL)` once and filter for itself, and the subscriber's "no
  signatures, no stream" rule then never surprises anyone.

  **Dispatch order is typed first, raw second, and it is a contract rather than an accident.** Both
  run for a frame both want, each listener gets it once, and containment is symmetric — a throw out
  of `onFrame`, or out of `signatures()`, costs that listener and nobody else, exactly as a throw
  out of `onEvent` does, because the caller is still a socket callback. **Causation is identical
  too**: both paths run inside the *same single* `CausationScope` of the arriving frame's id.

## What a consumer has to do, and what it must not forget

Registering a listener really is "add a bean" — no channel name, no annotation — and no
`@Unremovable` is needed, because `EventDispatcher`'s `Instance<QitsEventListener<?>>` is what ArC
counts as a use. That is asserted rather than trusted, since a removed listener subscribes to
nothing and says nothing about it: the suite keeps a listener injected nowhere and a raw listener
whose signature no `eventType()` produces, and requires both in the subscribe frame.

Beyond that there are three facts a consumer gets wrong once each.

- **The darkness belongs to the consumer, not to the library.** The jar ships
  `qits.eventstream.enabled=true` — a library that shipped dark is one whose first deployment
  discovers it was never wired up — and the consuming application's `application.properties` carries
  the `%dev`/`%test` `false`, exactly as it does for the OTel keys. Nothing else about the bus
  should be restated there: `qits.events.url`, the outbox datasource, the timeouts and the retry
  budget are ordinal-100 defaults in this jar, and a copy in the app's file is a second place to
  change.
- **Dark does not mean absent.** `enabled=false` stops publishing, sweeping and dialling; it does
  not stop the datasource. Quarkus opens the connection and runs Flyway at boot regardless, so a
  consumer's `src/test/resources/application.properties` must point `quarkus.datasource.eventstream`
  at in-memory H2 — measured, not assumed: without those lines a suite creates and migrates a real
  `~/.qits/data/eventstream`, and two builds on one host race for its single-writer file.

  **The deployment side of the same sentence cost a rollout: adding this jar to a deployable adds a
  MANDATORY deployment variable.** `QUARKUS_DATASOURCE_EVENTSTREAM_JDBC_URL` must point at the data
  volume. The shipped default interpolates `${user.home}`, which is the platform's convention and
  right for a host-run process — but in a container with no `HOME` the native binary resolves it to
  `?`, and H2 rejects a path implicitly relative to the working directory rather than falling back
  to one. The process dies at Flyway before serving anything: `Failed to start quarkus` /
  `FlywaySqlUnableToConnectToDbException`. **A config default no JVM test exercises, failing only in
  the packaged artifact in its real environment.** It fails loudly and safely — a health gate keeps
  the previous container — but it fails.
- **The reflection registration is the consumer's, and it is the one that fails quietly.** A
  deployable that native-image-compiles must carry a `@RegisterForReflection` naming its own event
  classes, `EventEnvelope`, `EventFrame` **and** `CanonicalJson$QitsEventMixin` (by string name; it
  is not public). qits-ci's `EventWireReflection` is the worked example.

  Without it the binary throws Jackson's `No serializer found for class … you may need to configure
  reflection` on **every** publish — inside `CanonicalJson` and therefore *before* an envelope
  existed, so the event never reached the outbox either. Not a delayed delivery, a lost one, with a
  single WARN to say so.

  Nothing registers them automatically because **`CanonicalJson` builds its own `ObjectMapper` on
  purpose** (above, and not negotiable): the graph that mapper serializes is invisible to the build
  step that scans for what needs reflecting on. **The mix-in is in the list because two binaries
  were built to find out, and it is the worse of the two failures.** Jackson reads its `@JsonIgnore`s
  with `getDeclaredMethods()`; with the record types registered and the mix-in left out, a green
  build published a payload containing `"eventId":"00a32ad6-…"` — no crash, no log, identity present
  in a body that is supposed to carry none. A wire contract violation that breaks nothing visible is
  not a lesser bug than one that throws.

  The registration lives in the consuming deployable rather than in this jar because the deployable
  is what gets built into an image and it is the deployable that knows its own event classes. A JVM
  test can only guard the list's *completeness* — on a JVM these classes reflect whether anyone
  registered them or not. The correctness proof is the binary, running.

**The far end of that failure was mute, and that is fixed here.** `EventDispatcher` logged a frame
it could not read at DEBUG, so a binary that could not deserialize `EventFrame` would have consumed
the entire stream in silence for as long as it ran. It is a WARN now, naming the frame's `name` and
`id` when the text is JSON at all (a second, untyped read — `readTree` needs no reflection, which is
precisely why it still works when binding does not). An unknown *signature* stays DEBUG: that one is
ordinary traffic, since a subscription set is a filter rather than a promise.

## The rollout order, which is one-directional

**qits-events ships first.** Quarkus does not fail on unknown JSON properties, so a publisher that
stamps a field against a qits-events that has not yet learned it has its parents silently dropped —
every chain of that window recorded as roots, and causation cannot be backfilled from anything. The
other direction (a server that knows the field, a publisher that never sends it) is the
compatibility clause, since an absent `parentId` binds to null.

The same clause governs anything added to the envelope later. Add it to the server, deploy, then
add it here.

## The suite

76 tests, all surefire, about twelve seconds. Three conventions in
`src/test/resources/application.properties` are load-bearing:

- **`quarkus.http.test-port=0`.** This module registers no route of its own — `websockets-next` is
  here for its CLIENT — but the extension brings an HTTP server along and a `@QuarkusTest` starts
  it. Several test classes ask for different configurations, so Quarkus restarts between them, and
  on the default 8081 the next instance races the previous one's release. Port 0 takes a free one.
  Keep it: 8081 is occupied on more than one development host.
- **`qits.eventstream.sweep-interval=24h`.** The scheduler must not sweep behind a test's back:
  every outbox assertion drives `OutboxSweeper#sweep` by hand, against a clock it moved, and a
  background tick landing in the middle makes the attempt counts non-deterministic. The job still
  registers, so the `@Scheduled` wiring is real — it simply never fires inside a suite run.
- **The redial backoffs, shrunk to milliseconds.** The SHAPE is what is under test (a drop is
  followed by a new connection carrying a fresh subscribe frame), not the duration, and the shipped
  values are in the jar's own properties where they belong.

`EventstreamClock` is an `@ApplicationScoped` `@DefaultBean` producer, which is what lets `TestClock`
outrank it with no alternative, no priority and no profile. Everything time-dependent here is
arithmetic on instants, and the only way to test that without sleeping through it is to move the
clock.

`StubEventsServer` is the far end: a JDK `HttpServer` for the PUT and a `websockets-next` endpoint
for the stream, with scripted responses and recorded requests. It is what makes the clone-alone rule
survivable, and it is the thing to extend when a test needs the server to behave a new way — not a
mock of `EventsPublisher`, which would stop the wire being under test.

## Not yet here

- **No CI pipeline.** There is no maven registry on the platform, so a `.config/qits/` recipe for
  this repo would have nothing to publish to. Consumers vendor or submodule this source; when a
  registry exists, the pipeline mirrors qits-spa-ui-components' publish-if-absent shape.
- **No catch-up.** The stream is live-only. Reading the log back from qits-events is a feature that
  will need the frame's `id`, which is why the frame carries one already.
