# `@lockness/realtime` — agent brief

Real-time WebSockets + broadcasting. A handler over Hono's `upgradeWebSocket`
with lifecycle hooks and a server-derived identity; public/private/presence
channels with an app authorizer; a memory/Redis broadcast driver; a JSON wire
protocol; a soft `@lockness/events` → broadcast bridge. The broadcaster
satisfies `@lockness/notification`'s `BroadcasterLike`.

## Invariants

- **The dependency contract below is binding.** Importing anything outside it
  fails `deno task deps:analyze`.
- **`upgradeWebSocket` comes only from `@lockness/hono/deno`**, imported once in
  `websocket.ts` (the main barrel exposes WS types only; hard rule #1).
  `WSContext` is `import type` from `@lockness/hono/network` (an allowed edge,
  no mirror).
- **`@lockness/events` is a SOFT edge** — the events→broadcast bridge soft-loads
  it and types the dispatcher/event shapes with **local structural interfaces**.
  An `import`/`import type` from `@lockness/events` hardens the edge and fails
  the gate.
- **Connection identity is server-derived at the upgrade** (verified session /
  token), immutable, distinct from `metadata`; a wire frame is never an identity
  source (S1).
- **Origin is checked fail-closed** — exact origin triple, absent/empty/`null`
  rejected, no substring/implicit wildcard (CSWSH, S5).
- **A private/presence channel event reaches a connection only after the
  authorizer approved it** (S1 disclosure control); a Redis-received message is
  re-authorized on the receiving instance (S6).
- **The events bridge forwards only `broadcastWith()`** — minimal default, never
  the whole event (leak-by-default, S2).
- **No `any` in exported signatures; JSDoc on every export; no direct `hono`.**

## Dependency contract

<!-- generated:deps -->

| Direction                                      | Packages                                 |
| :--------------------------------------------- | :--------------------------------------- |
| Imports (static)                               | `contract`, `hono`, `redis`              |
| Imports (soft, via `tryImportOptionalPackage`) | `events`                                 |
| Imported by                                    | —                                        |
| **Must never import**                          | nothing — no package depends on this one |

Enforced by `deno task deps:analyze` against `deps.policy.jsonc`. A soft edge is
deliberately **not** declared in this package's `deno.json`: the consuming
application installs it, or the feature stays off.

<!-- /generated:deps -->

## Public surface

<!-- generated:surface -->

| Kind      | Exports                                                                                                                                                                                                                                                                                                                                                                                                                |
| :-------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| class     | `ChannelLimitError`, `ChannelManager`, `ChannelNameError`, `ConnectionIdError`, `MemoryBroadcastDriver`, `PresenceMemberIdError`, `ProtocolError`, `RedisBroadcastDriver`, `WSContext`                                                                                                                                                                                                                                 |
| function  | `channelKind`, `createWebSocketHandler`, `decodeClientMessage`, `encodeServerMessage`, `forwardEvent`, `isBroadcastable`, `isValidName`, `startBroadcasting`                                                                                                                                                                                                                                                           |
| interface | `AnyEventPayload`, `BroadcastBridgeOptions`, `BroadcastDriver`, `BroadcastMessage`, `Broadcastable`, `ChannelManagerOptions`, `Connection`, `ControlMessage`, `ControlRefusal`, `DispatcherLike`, `PresenceCapableDriver`, `PresenceMember`, `RealtimeControlConfig`, `RedisBroadcastDriverOptions`, `RedisCommandClient`, `RedisSubscriber`, `Socket`, `SubscribeResult`, `WebSocketHandlerOptions`, `WebSocketHooks` |
| typeAlias | `AuthorizeResult`, `Authorizer`, `ChannelKind`, `ChannelLimitScope`, `ClientMessage`, `OutboundFrame`, `RedisBroadcastConnectionConfig`, `ServerMessage`, `WSMessageReceive`                                                                                                                                                                                                                                           |
| variable  | `CHANNEL_LIMIT_SCOPES`, `MAX_CHANNELS_PER_CONNECTION`, `MAX_FRAME_BYTES`, `MAX_NAME_LENGTH`, `MAX_WATCHED_CHANNELS`                                                                                                                                                                                                                                                                                                    |

Anything not listed is internal and free to change.

<!-- /generated:surface -->

## Where to work

| Task                                                             | File                                     |
| :--------------------------------------------------------------- | :--------------------------------------- |
| The WS handler + `Connection` + origin/identity guards           | `websocket.ts`, `types.ts`               |
| Channels (public/private/presence) + authorizer + presence       | `channel.ts`                             |
| Subscribe/broadcast/eviction + the `BroadcasterLike` broadcaster | `manager.ts`                             |
| The broadcast driver seam (memory/Redis)                         | `driver.ts`, `drivers/{memory,redis}.ts` |
| The wire protocol + name validation                              | `protocol.ts`                            |
| The events→broadcast bridge + marker                             | `events_bridge.ts`, `broadcastable.ts`   |
| The optional browser client                                      | `client.ts` (leaf, `./client` subpath)   |

## Pitfalls

- **`heartbeatIntervalMs` and `livenessTtlSeconds` are ONE setting with two
  numbers.** The heartbeat is what keeps this instance's `{prefix}:alive:<id>`
  key alive, and that key's TTL is `livenessTtlSeconds`. Beat slower than the
  TTL and a healthy instance lets its own key lapse between beats, so every peer
  sweeps its presence members out of the roster while its sockets stay open —
  with nothing in the log looking wrong. The constructor now refuses an interval
  above half the TTL
  ([#293](https://github.com/locknessland/lockness-monorepo/issues/293)); two
  beats per window, because one lands on the boundary and races the expiry.
- **Refusing a bad state can move a mutant FURTHER from killable — and
  "unreachable" is a claim about the CONFIGURATION, not about the guard.** #293
  was filed expecting its guard to make the `id === this.instanceId` self-skip a
  killable mutant. It did the opposite: the row diverges only when an instance
  lets its own liveness key lapse, and the guard makes that configuration
  unconstructible. Measured on a real broker after the guard landed — still
  green. "We added validation, so the mutant is now covered" remains the
  reasoning to distrust.

  **That closed the configural path and left the transient one open**, and the
  row is RED since
  [#310](https://github.com/locknessland/lockness-monorepo/issues/310): the
  liveness `SET` can fail for a window while the instance is otherwise healthy —
  `#heartbeat` catches and logs at WARN, so it keeps accepting joins and writing
  rosters while its own key expires underneath it. `withFaultyInstance`
  (`tests/live_realtime.ts`) injects exactly that and nothing else; the battery
  is `tests/mutations/self_skip_310.ts`, and it SURVIVES against the suite
  without that scenario. The self-skip would have stayed either way — a guard
  unreachable from every valid configuration is the desired state, not a
  redundancy to delete — but unreachability is no longer the reason for leaving
  it untested.

- **The watched-channel caps REFUSE, and the reservation is not the cap.**
  `#checkChannelCaps` throws `ChannelLimitError` before any membership mutation
  ([#322](https://github.com/locknessland/lockness-monorepo/issues/322)). Three
  things a test gets wrong here. First, `conn()` in
  `tests/channel_watch_295.test.ts` is **anonymous** (`identity: null`), so a
  loop of them hits `anonymousHostingShare × maxWatchedChannels` — 800, not 1
  000 — and a test that means to reach the full cap must use `identified()` or
  it measures the reservation and reports it as the cap. Second, the scope is
  decided by whether the reservation is **in effect**, not by who called: at
  `share: 1` the ceiling equals the cap and an anonymous breach must report
  `'instance'`, or an operator is sent to tune a dial already at maximum. Third,
  a small custom `maxWatchedChannels` needs a smaller `maxChannelsPerConnection`
  — the 100 default above an instance cap of 10 is refused at construction, and
  that refusal is the point, not an obstacle.
- **A `console.warn`-absence assertion is a tautology once the warn is gone.**
  `#295/FR-017`'s guard was
  `warnings.filter(w => w.includes('watched
  channels')) === []`. It passed for
  every input the moment #322 removed both warns — reading as a guard while
  proving nothing. Assert the refusal directly.
- **Assert on `commandLog()`, never on a wrapped `command`.** A test that wraps
  the driver's `command` function sees only what the driver issues _directly_ —
  a script's own writes reach the store through the Lua evaluator, so they never
  pass the wrapper. #276 shipped an assertion that was structurally unable to
  fail for exactly that reason, and it looked like coverage. `FakeRedis` exposes
  `commandLog()`, which records every command including script-internal ones.
- **A modelled command can model the wrong thing, and the throwing `default:`
  will not catch it.** That guard fires for an _unmodelled_ command; #276 was
  wrong twice over commands the fake already had. Both times the divergence was
  a silently-ignored option token — an `EXPIRE … GT` armed where real Redis
  refuses it, so an inert production guard looked like a working one. Since #280
  every modelled arm **refuses** an argument it does not read — an unknown
  option, a wrong arity, a non-numeric score — rather than ignoring it, and
  refusals are recorded so a caller that catches and warns cannot swallow one
  (`assertNoRejections()`). That is the rule to keep: if you add an argument to
  a call site, add it to the arm or make the arm refuse it. Never let it pass
  unread.
- **A new `case` in `FakeRedis.#exec` ships with a conformance sequence.**
  `tests/live_fake_conformance.test.ts` drives identical command sequences
  through the fake and a real broker and diffs the replies, so a modelled arm
  that lies is caught by Redis itself rather than by whoever next re-reads it.
  Run it with `LOCKNESS_REDIS_PORT=<port> deno task test:redis`; it is `ignored`
  without a broker and never runs in the default suite. Two rules that file
  learned the hard way: resolve the key namespace **once** (`runNamespace()`
  mints a fresh one per call, and calling it per key made every step operate on
  an empty key, so the suite passed while detecting nothing), and put a declared
  modelling gap **last** on its key, because the broker applies what the fake
  refuses and every later step on that key then diverges for the wrong reason.
- **The fake is not Redis, and the gap is the interesting part.** It models the
  driver's command surface only, so a behaviour no test exercises is one nobody
  has checked — #280's audit found five divergences the issue had not listed,
  two clocks and a `DEL` that never reached sorted sets among them. When you
  start relying on a semantic, add a row to `fake_redis_conformance.test.ts`
  stating what real Redis does; that file is the record of what has actually
  been verified.
- **A containment check must never consult `keys()`.** `live_realtime.ts`'s
  `keys()` is a verbatim second copy of the driver's nine name templates. It is
  correct for _read-back_ — asserting a key holds what you put there — and wrong
  for _containment_, because a check that reads it asserts agreement between two
  models rather than anchoring. `tests/prefix_anchoring.test.ts` observes the
  ports instead, and decides which strings are prefix-derived differentially, by
  running the same exercise under two prefixes.
- **Containment cannot be checked with a keyspace scan.**
  `SCAN MATCH ${prefix}*` returns only keys already under the prefix, so it
  cannot see the defect. A whole-keyspace diff is no better: five of the ten
  derived names never become a key at all — two are `PUBLISH`/`PSUBSCRIBE`
  arguments, one is the subscribe pattern, and two are read-only by design. Only
  the port sees all ten.
- **"Anchored" is prefix PLUS a separator, and since #288 that separator always
  begins with `__`.** `startsWith` was never enough — `${prefix}:*` "begins
  with" the prefix and still over-matched into a nested deployment, which was a
  real, reproduced disclosure. It is closed: every derived name sits behind a
  `__`-leading separator and no accepted prefix may contain `__`, so the two
  halves are one decision. **Adding a separator that does not begin with `__`
  reopens it and passes every test in the suite** — the rule lives at
  `RESERVED_SEPARATOR_LEAD` with its proof, not here.
- **Release ordering: #288's wire change must land in or before the FIRST
  release that publishes `@lockness/realtime`.** It had never been published
  when the change landed (JSR 404, 2026-09-06) and is imported by no other
  package, which is why it carries no compatibility shim. That is a fact about a
  moment, not a property: this repo versions in lockstep, so the next `/ship`
  publishes the package. If a release goes out ahead of this change, the shim
  question reopens and a dual-publish path has to be designed.
- **`prefix` bounds OUTBOUND routing; it is not an inbound boundary.** Inbound:
  anything on the broker can `PUBLISH` into — and read from — this deployment's
  topics and keys; that is a Redis-ACL problem. Outbound: structural, per the
  point above. The `RedisBroadcastDriverOptions.prefix` docstring is the single
  home for the statement; everything else points at it. It used to say
  "multi-tenant isolation", which is the wording that led operators to the
  nested prefixes #288 was about.

- **The test double used to answer `nil` to any command it did not model**,
  which made an unmodelled command a silent no-op with a green suite. It now
  throws (`tests/fake_redis.ts`). If you add a driver command, model it — the
  failure will tell you. Both #276 plan audits named this independently as the
  likeliest way that feature could have shipped broken. 2026-09-05.
- **Revocation liveness is decided by Redis, never by `Date.now()`.** The score
  in `{prefix}:revocations` is compared against a `TIME` read inside the script.
  A stored expiry judged against an instance's clock would let a fast-clocked
  host delete revocations that are live for the whole fleet (#276). 2026-09-05.
- **Presence membership is CROSS-PROCESS authoritative** since #268 (shipped
  2026-09-05, `6ed138f4`). The roster lives in the driver — `rosterSnapshot`
  reads `roster.listMembers()` off it, never local state (`manager.ts`) — and
  every instance shares it. This line previously said "single-process
  authoritative for the MVP, the `here` set is per-instance", which was true
  before #268 and is the premise #312 had to disprove: a reader of the stale
  version reasons that a member "landed in the roster on one instance", and
  reopens a question that is settled. A refused `presence-join` control frame
  loses the live PUSH to peers already in the channel; it does not lose roster
  state, and the divergence is bounded by the connection's lifetime because
  `disconnect` removes the member on the ordinary path
  ([#312](https://github.com/locknessland/lockness-monorepo/issues/312)).
  2026-09-07.
- Nothing imports `realtime` (pure sink), and `@lockness/core` is untouched
  (app-wired) — keep it that way.
- `@lockness/notification` is a **dev/test dependency only** (the SC-005
  `BroadcasterLike` conformance test); never import it from source.

- **An instance subscribes per CHANNEL, not per prefix (#295).** `onMessage`
  registers the decoder and subscribes nothing when the driver's subscriber can
  do it per channel; the subscriptions come from `watchChannel`, called by
  `ChannelManager` on a 0→1 transition and `unwatchChannel` on 1→0. Two
  consequences that bite:
  - **`#joinLocal` / `#leaveLocal` are the ONLY writers of `subscriptions`.** A
    membership mutation written anywhere else leaves a channel
    hosted-but-unwatched: every message dropped while `subscribe` answers
    `{ ok: true }`, and nothing logged. The 0→1 test is computed in the same
    synchronous turn as the mutation; only the wire op is awaited.
  - **The empty `Set` is deleted**, so `subscriptions.has(channel)` is the one
    spelling of "this instance hosts it".
- **The watch pair is detected as a SET, by `channelWatcher`.** A driver with
  `watchChannel` and no `unwatchChannel` keeps the old prefix-wide behaviour: a
  subscription set that grows and never shrinks is worse than the glob it
  replaces, and invisible, because delivery stays correct.
- **`eventPattern` is a separate builder from `topic`, same bytes today.**
  `PUBLISH` is a literal context and `PSUBSCRIBE`/`SUBSCRIBE` a subscription
  one. Calling `topic()` from `watchChannel` returns the right string now, which
  is exactly what makes the first escaping ever added to `topic()` corrupt the
  subscription silently.
- **A test that drives the driver WITHOUT a manager must watch its channels
  itself.** `onMessage` subscribes nothing, so a fixture that only registers
  seams receives nothing — and its isolation assertions then pass for the
  emptiest possible reason. `prefix_anchoring.test.ts` and
  `redis_broker_integration.test.ts`'s nested-deployment scenario both do this
  explicitly.

## Tests

<!-- generated:tests -->

41 test files for 16 source files:

- `packages/realtime/tests/broadcaster.test.ts`
- `packages/realtime/tests/channel_name_boundary.test.ts`
- `packages/realtime/tests/channel_watch_295.test.ts`
- `packages/realtime/tests/channels.test.ts`
- `packages/realtime/tests/client.test.ts`
- `packages/realtime/tests/connection_id_charset.test.ts`
- `packages/realtime/tests/control_auth.test.ts`
- `packages/realtime/tests/control_mac_coverage.test.ts`
- `packages/realtime/tests/control_plane.test.ts`
- `packages/realtime/tests/control_refusal.test.ts`
- `packages/realtime/tests/control_replay.test.ts`
- `packages/realtime/tests/control_replay_window.test.ts`
- `packages/realtime/tests/deliver_local_reauth.test.ts`
- `packages/realtime/tests/disconnect_propagation.test.ts`
- `packages/realtime/tests/driver_contract.test.ts`
- `packages/realtime/tests/driver_redis.test.ts`
- `packages/realtime/tests/driver_redis_live.test.ts`
- `packages/realtime/tests/events_bridge.test.ts`
- `packages/realtime/tests/eviction_control.test.ts`
- `packages/realtime/tests/eviction_durable.test.ts`
- `packages/realtime/tests/eviction_reconnect.test.ts`
- `packages/realtime/tests/fake_redis_conformance.test.ts`
- `packages/realtime/tests/handler.test.ts`
- `packages/realtime/tests/identity.test.ts`
- `packages/realtime/tests/live_fake_conformance.test.ts`
- `packages/realtime/tests/log_encoding_291.test.ts`
- `packages/realtime/tests/manager.test.ts`
- `packages/realtime/tests/memory_driver.test.ts`
- `packages/realtime/tests/origin.test.ts`
- `packages/realtime/tests/prefix_anchoring.test.ts`
- `packages/realtime/tests/presence.test.ts`
- `packages/realtime/tests/presence_authoritative.test.ts`
- `packages/realtime/tests/presence_member_id.test.ts`
- `packages/realtime/tests/presence_roster_guard.test.ts`
- `packages/realtime/tests/presence_sweep.test.ts`
- `packages/realtime/tests/protocol.test.ts`
- `packages/realtime/tests/redis_broker_integration.test.ts`
- `packages/realtime/tests/revocation_atomicity.test.ts`
- `packages/realtime/tests/revocation_retry.test.ts`
- `packages/realtime/tests/roster_control_atomicity.test.ts`
- `packages/realtime/tests/websocket.test.ts`

11 mutation batteries — **`deno test` does not run these.** Each is an
executable that mutates a source file and re-runs the suites that should notice.
Run them with `deno task mutate` (all of them, one at a time) or
`deno task mutate <name>` (one); nightly CI runs the full sweep. See
[testing.md](../../docs/testing.md#mutation-batteries).

- `packages/realtime/tests/mutations/channel_name_314.ts`
- `packages/realtime/tests/mutations/connection_id_304.ts`
- `packages/realtime/tests/mutations/fake_redis_280.ts`
- `packages/realtime/tests/mutations/live_conformance_285.ts`
- `packages/realtime/tests/mutations/log_encoding_291.ts`
- `packages/realtime/tests/mutations/prefix_288.ts`
- `packages/realtime/tests/mutations/presence_member_306.ts`
- `packages/realtime/tests/mutations/revocation_retry_308.ts`
- `packages/realtime/tests/mutations/self_skip_310.ts`
- `packages/realtime/tests/mutations/subscription_identity_315.ts`
- `packages/realtime/tests/mutations/sweep_parse_316.ts`

<!-- /generated:tests -->

## Before you call it done

<!-- generated:gate -->

The framework-wide gate, from the repository root:

```bash
deno fmt && deno lint && deno check && deno task test
deno task deps:analyze     # cycles, declaration drift, tier policy
deno task agents:brief     # refresh this file's generated blocks
```

Then, specific to this package: run its 41 test files directly —

```bash
deno test -A packages/realtime/
```

<!-- /generated:gate -->

---

_Framework-wide rules live in the root [AGENTS.md](../../AGENTS.md). The
dependency contract, public surface, tests and closing gate are generated by
`deno task agents:brief` from the code itself — fix the code, not those blocks.
Everything else is hand-written and preserved._
