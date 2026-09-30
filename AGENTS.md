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
  authorizer approved that object's own subscribe** (S1 disclosure control); a
  Redis-received message is re-authorized on the receiving instance (S6).
- **The events bridge forwards only `broadcastWith()`** — minimal default, never
  the whole event (leak-by-default, S2).
- **No `any` in exported signatures; JSDoc on every export; no direct `hono`.**

## Dependency contract

<!-- generated:deps -->

| Direction                                      | Packages                                             |
| :--------------------------------------------- | :--------------------------------------------------- |
| Imports (static)                               | `contract`, `deprecation-contracts`, `hono`, `redis` |
| Imports (soft, via `tryImportOptionalPackage`) | `events`                                             |
| Imported by                                    | —                                                    |
| **Must never import**                          | nothing — no package depends on this one             |

Enforced by `deno task deps:analyze` against `deps.policy.jsonc`. A soft edge is
deliberately **not** declared in this package's `deno.json`: the consuming
application installs it, or the feature stays off.

<!-- /generated:deps -->

## Public surface

<!-- generated:surface -->

| Kind      | Exports                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| :-------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| class     | `AuthorizeResultError`, `ChannelLimitError`, `ChannelManager`, `ChannelNameError`, `ConnectionDisconnectedError`, `ConnectionIdError`, `ConnectionIdInUseError`, `ConnectionNotRegisteredError`, `MemoryBroadcastDriver`, `PresenceMemberIdError`, `PresenceMemberShapeError`, `PresenceMemberSizeError`, `ProtocolError`, `RedisBroadcastDriver`, `RevocationScopeError`, `WSContext`                                                                                                                                                                                                                              |
| function  | `channelKind`, `createWebSocketHandler`, `decodeClientMessage`, `encodeServerMessage`, `forwardEvent`, `isBroadcastable`, `isValidName`, `startBroadcasting`                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| interface | `AnyEventPayload`, `BroadcastBridgeOptions`, `BroadcastDriver`, `BroadcastMessage`, `Broadcastable`, `ChannelManagerOptions`, `ChannelRevocation`, `Connection`, `ConnectionRevocation`, `ControlMessage`, `ControlRefusal`, `DispatcherLike`, `PassSample`, `PresenceCapableDriver`, `PresenceMember`, `PresenceSnapshot`, `RealtimeControlConfig`, `RedisBroadcastDriverOptions`, `RedisCommandClient`, `RedisSubscriber`, `RevocationStoreDriver`, `RevocationTally`, `RosterDeparture`, `RosterHold`, `RosterRelease`, `RosterWindow`, `Socket`, `SubscribeResult`, `WebSocketHandlerOptions`, `WebSocketHooks` |
| typeAlias | `AuthorizeResult`, `Authorizer`, `ChannelKind`, `ChannelLimitScope`, `ClientMessage`, `DisconnectOutcome`, `LeaveOutcome`, `OutboundFrame`, `RedisBroadcastConnectionConfig`, `Revocation`, `RevokeChannelOutcome`, `ServerMessage`, `WSMessageReceive`                                                                                                                                                                                                                                                                                                                                                             |
| variable  | `CHANNEL_LIMIT_SCOPES`, `MAX_CHANNELS_PER_CONNECTION`, `MAX_FRAME_BYTES`, `MAX_NAME_LENGTH`, `MAX_PRESENCE_MEMBER_BYTES`, `MAX_PRESENCE_SNAPSHOT_MEMBERS`, `MAX_ROSTER_READ_SELF_IDS`, `MAX_WATCHED_CHANNELS`                                                                                                                                                                                                                                                                                                                                                                                                       |

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
| When the driver is asked for a roster (never what it answers)    | `roster_read_barrier.ts`                 |
| Which members a subscribe returns (the cut, K, self kept)        | `presence_snapshot.ts`                   |
| How much one roster read ingests (`readRoster`, `total`, cap)    | `driver.ts`, `drivers/{memory,redis}.ts` |
| When a broken revocation guarantee is reported (one deadline)    | `drivers/enforcement_deadline.ts`        |
| What a completed pass reports (`onPassComplete`, `PassSample`)   | `drivers/redis.ts`                       |
| A last-resort log line that must never throw (#391)              | `marked_fallback.ts`                     |

`onPassComplete` is the Redis driver's measurement seam (#360, ADR
[012](../../docs/adr/012-measurements-reach-the-app-through-a-seam.md)): one
sample per completed pass, taken at its two end sites — the revocation pass's
`finally` in `#startRevocationPass`, after the deadline call, and the ghost
sweep's `finally` in `#armReconcile`, after the re-arm. `#emitPassSample`'s
JSDoc is the one home of that rule — the counts included (#384): `attempts` and
`failures` come from the start site's record, never from a field. `PassSample`'s
is the one home of what each field means; `RevocationTally`'s, in `driver.ts`,
of what the re-check's counts mean.

## Pitfalls

- **A pass sample is taken at its pass's end site and nowhere else**
  ([#360](https://github.com/locknessland/lockness-monorepo/issues/360), ADR
  [012](../../docs/adr/012-measurements-reach-the-app-through-a-seam.md)).
  **Never await a pass handler**: an observer must not stretch the pass it
  measures. **Never sample from anywhere but a pass's end site**, and **the end
  site reads its start site's closure, never `#revocationPass` or
  `#sweepPass`**: the revocation end site starts a trailing pass before it
  samples, and the field then holds that pass. **Never add a second
  `performance.now()`**: both passes are timed on `#passClock()`. Instrument
  names never appear in code — they live in `docs/observability-and-crypto.md` §
  Framework instruments.
- **"Clean" is decided at the revocation end site and nowhere else**
  ([#384](https://github.com/locknessland/lockness-monorepo/issues/384)). Never
  decide it in `EnforcementDeadline` (it only learns `passSucceeded` or the
  verdict-free `passEnded`), **never WARN on a value that is not tally-shaped**
  (a `() => void` handler can resolve a stray value), and **never let a
  malformed tally re-arm the deadline**. `decodeRevocationTally` is the one
  decoder and never throws; `REVOCATION_TALLY_MALFORMED` is written only in
  `#runRevocationReconcile`, before the end site can start a trailing pass.

- **The Redis revocation pass is bounded, and the bound is checked**
  ([#362](https://github.com/locknessland/lockness-monorepo/issues/362), ADR
  [011](../../docs/adr/011-realtime-revocation-bound-is-checked.md)). **Never
  free the revocation pass slot on a timer**: a stalled pass is reported by the
  enforcement deadline, not abandoned, and one pass runs at a time. **Never add
  a second deadline clock**: intervals are read on `#passClock()` alone, and
  **never on the epoch `now()`**, which is the control-frame stamp clock.
  **Never log from the pass's end site**: every deadline line is written in the
  deadline's own timer callback, in the #369 shape. The boot relation between
  `reconcileIntervalMs` and `revocationTtlSeconds` is stated once, in the
  configuration paragraph of `docs/realtime.md`.
- **A revocation record outlives the fleet's longest live TTL through the
  revocation floor**
  ([#380](https://github.com/locknessland/lockness-monorepo/issues/380), ADR
  [013](../../docs/adr/013-realtime-revocation-ttl-floor.md)). **Never decode
  the floor in Lua**: `decodeRevocationFloor` is the one decoder, and the mark
  script does not change. **Never let a floor entry or an unreadable floor fail
  a mark**: a bad member is skipped and counted, and an unreadable floor marks
  at `MAX_REVOCATION_TTL_SECONDS` (fail closed); only the `EVAL` fails a mark.
  **Never write the floor outside `FLOOR_WRITE`**, whose JSDoc names its only
  two callers (the reap and the announce). **Never announce outside the
  first-registration gate**, and **never put the index key on the announce**: it
  is one-key so no reap predicate can match it (`tests/revocation_wire.ts` is
  the test-side home of both shapes). Witness:
  `revocation_ttl_floor_380.test.ts`; battery
  `tests/mutations/revocation_ttl_floor_380.ts`.
- **A wrong-typed floor key self-heals inside `FLOOR_WRITE`'s own atomic `EVAL`,
  and never by widening what it may `DEL`**
  ([#405](https://github.com/locknessland/lockness-monorepo/issues/405), ADR 013
  §2). `FLOOR_WRITE` reads the key's Redis type first
  (`redis.call('TYPE', floor)['ok']`) and `DEL`s it when it is anything but
  `zset`/`none`, before the `ZADD`/`ZREMRANGEBYSCORE`/`EXPIRE` body runs — so a
  corrupt key heals in the same pass that hit it, never a second round trip.
  **The `DEL` names the floor key ONLY — never the revocation index, and never
  generalise it to a second key "while we're in there".** The index is a
  different structure with its own failure mode and, since #411, its OWN
  separate heal fragment (`INDEX_HEAL`) and its own sole delete, the reap's
  `ZREMRANGEBYSCORE`; widening this `DEL`'s reach would let a floor heal destroy
  live revocations. **Never `pcall`, `else`, `~=` or reassignment** in
  `FLOOR_WRITE`: the shared Lua evaluator (`packages/redis/tests/lua_eval.ts`)
  proves all four unsupported, and it is shared by `session`, `queue` and
  `core`'s scheduler locks — extending it further for one driver's edge case is
  a second, weaker home. Both callers' replies changed to carry `kind` (the
  reap's `{t, kind}`, the announce's bare `kind`; widened again by #411 below);
  `decodeReapReply` is the one decoder of the pair. Witness:
  `revocation_floor_wrong_type_405.test.ts` (fake rows: `string`/`hash`/`set`;
  live-broker rows, gated on `LOCKNESS_REDIS_INTEGRATION`: `list`/`stream`,
  which the fake never models); battery
  `tests/mutations/revocation_floor_wrong_type_405.ts`.
- **A wrong-typed INDEX key self-heals the SAME way, inside
  `REAP_REVOKED_SCRIPT`'s and `MARK_REVOKED_SCRIPT`'s own atomic `EVAL`s,
  through `INDEX_HEAL` — the `FLOOR_WRITE`-shaped fragment generalised to the
  index** ([#411](https://github.com/locknessland/lockness-monorepo/issues/411),
  ADR 013 §2/§4). `INDEX_HEAL` reads `redis.call('TYPE', index)['ok']` and
  `DEL`s the index when it is anything but `zset`/`none`, spliced before the
  reap's `ZREMRANGEBYSCORE` and before the mark's `ZADD` — so a corrupt index
  heals in the same pass that hit it. **`DEL` is safe here for a DIFFERENT
  reason than the floor's**: the floor is safe because it is fully re-derivable,
  while the index holds primary, non-derived records and is safe because any
  command able to change a key's TYPE has already discarded the prior value,
  unconditionally, before Redis ever raises `WRONGTYPE` — the former zset is
  already gone at the Redis layer the instant `TYPE` disagrees, before this heal
  runs. **The reap never `ZADD`s the index, so a healed index is ABSENT
  afterward, not a fresh `zset`** — unlike the floor, which the reap's
  `FLOOR_WRITE` always rewrites in the same call; the mark DOES `ZADD` the same
  key it heals, inside the same `EVAL`, so a mark-side heal IS a `zset` again
  immediately. Both scripts' reply widened again: the reap's to
  `{t, indexKind, floorKind}` (`decodeReapReply`, the pair from #405 widened to
  a triple), the mark's to `{indexKind}` (new `decodeMarkReply`). The heal WARN
  is `REVOCATION_INDEX_WRONG_TYPE`, wording twin to
  `REVOCATION_FLOOR_WRONG_TYPE`, written through the SAME `#warnFloor` sink —
  never a sibling — via the new `#warnIfIndexHealed`. **Folded LOW**:
  `#announceFloor`'s bare `asBulk(reply)` is now the strict
  `decodeAnnounceReply`, which throws on a non-bulk reply instead of silently
  skipping the heal check — the failure lands in the announce's existing
  WARN+retry `catch`. Witness: `revocation_index_wrong_type_411.test.ts` (fake
  rows: `string`/`hash`/`set` × {reap, mark}; live-broker rows, gated on
  `LOCKNESS_REDIS_INTEGRATION`: `list`/`stream` × {reap, mark}); battery
  `tests/mutations/revocation_index_wrong_type_411.ts`.
- **The presence roster's five keys do NOT all get the same remedy — presence
  and holders fail closed, owned and instances self-heal**
  ([#414](https://github.com/locknessland/lockness-monorepo/issues/414), ADR
  [016](../../docs/adr/016-realtime-presence-roster-key-remedy.md)). The
  standing rule: a key self-heals only if it feeds no `arrived`/`gone` decision
  AND is either single-writer-scoped or fully re-derived on a bounded cadence —
  otherwise it fails closed, loudly. Presence (`presence:<channel>`) and holders
  (`holders:<channel> <id>`) hold live membership or decide `HLEN` (ADR 006), so
  **neither may ever gain a `TYPE`-gated `DEL`** — a refactor that "generalises
  INDEX_HEAL to the roster too" is the defect, not a cleanup. The owned set
  (`owned:<instanceId>`) and the instances set (`instances`) DO self-heal,
  through `OWNED_HEAL` / `INSTANCES_HEAL` — the `INDEX_HEAL` shape spliced into
  `HOLD_MEMBER_SCRIPT` (both), `RELEASE_MEMBER_SCRIPT` (owned only) and
  `DEREGISTER_INSTANCE_SCRIPT` (instances only, before its gated `SREM`).
  **`HOLD_MEMBER_SCRIPT` also gained an up-front, NO-HEAL presence guard** — a
  bare `HGET` of the field it is about to write, as its literal first statement,
  before the holders `HSET`: without it, a presence-only corruption let the
  holders write commit and only THEN abort on presence, orphaning a holder entry
  with no owned/instances entry (ADR 004 §5's shape, a different cause). **Never
  reorder that guard below the holders write "to keep related writes together"**
  — that reintroduces the exact orphan the ordering witness pins.
  `RELEASE_MEMBER_SCRIPT` needed no equivalent change: its own presence read
  already runs before its first write, an existing-order accident now
  load-bearing. Reply shapes widened: `HOLD_MEMBER_SCRIPT` to
  `{arrived, ownedKind, instancesKind}`, `RELEASE_MEMBER_SCRIPT` to
  `{value, ownedKind}` (bare on _refused_), `DEREGISTER_INSTANCE_SCRIPT` to
  `{code, instancesKind}` — decoded by `decodeHoldReply` / `decodeReleaseReply`
  / `decodeDeregisterReply`, WARN'd through the SAME `#warnFloor` sink via
  `#warnIfOwnedHealed` / `#warnIfInstancesHealed` — never a sibling.
  **`fake_redis.ts` gained `#assertHashKey` / `#assertSetKey`**, and a plain
  `SET` now clears the other three type maps first: before this, every hash/set
  arm wrote into its own map unconditionally, so a corrupted presence/holders
  key read as merely ABSENT to a later `HGET` — indistinguishable from one that
  never existed — and the boot heartbeat's own raw `SADD` on the instances key
  could silently "heal" a corruption before the script's own `TYPE` read ever
  saw it. Witness: `presence_roster_wrong_type_414.test.ts` (fake rows:
  `string`/`hash`/`zset` for owned/instances self-heal on
  HOLD/RELEASE/DEREGISTER, and for presence/holders fail-closed on
  holdMember/releaseMember/readRoster; one row pinning the ordering fix;
  live-broker rows, gated on `LOCKNESS_REDIS_INTEGRATION`: `list`/`stream` on
  HOLD_MEMBER_SCRIPT); battery `tests/mutations/presence_key_guards_414.ts`.
- **The local presence view is deduplicated in ONE place, `#localRoster`, and
  nowhere else**
  ([#343](https://github.com/locknessland/lockness-monorepo/issues/343)). The
  `presence` map is keyed by connection id; the roster by `String(member.id)`.
  Every read of the map's values — the `here` local fallback, the roster-less
  branch of `rosterSnapshot`, `#syncRosterMember`'s scan — goes through
  `#localRoster` → `uniqueMembers` (first occurrence wins, so the
  earliest-joined connection's `info`, matching the slot). Two tempting
  "simplifications" are wrong. **Never dedupe in `boundPresenceSnapshot`**: the
  authoritative roster is already unique, the pass would be O(room) on the hot
  read #333 made cheap, it would hide a driver returning duplicates, and the cut
  must return its input unchanged when it fits. **Never re-key the map by
  member**: the #327 check-and-claim, the #334 last-member delete and the self
  lookup all key on the connection. A new read of
  `presence.get(channel)?.values()` that skips `#localRoster` is the defect
  coming back. Witness: `presence_local_member_343.test.ts`; battery
  `tests/mutations/presence_local_member_343.ts`.

- **The roster read barrier is TRAILING-edge, and turning it into an ordinary
  single-flight is a correctness regression, not an optimisation.**
  `roster_read_barrier.ts` answers a caller that arrived mid-read with the
  _next_ read, never the one already running. The difference is one expression
  and it looks like waste. It is not: a join commits its roster write before it
  reads, and the Redis client chains commands onto its tail synchronously, so a
  joiner has always appeared in its own `here`. Share an already-running read
  and a joiner can subscribe to a room and be handed a roster it is not in —
  silently, with nothing thrown, logged, or type-changed. The witness is
  `presence_roster_read_333.test.ts`'s `'a joiner sees ITSELF in its own reply'`
  and the first row of `tests/mutations/roster_read_barrier_333.ts`. Everything
  else in the suite passes under both designs.

- **A presence join announces LAST, and the bookkeeping stays first.** It all
  lives in `#joinPresence` since
  [#328](https://github.com/locknessland/lockness-monorepo/issues/328); the cap
  check that pairs with it stays in `subscribe`. The announcement is made by the
  queued roster write itself, after the hold returns `arrived` (#344), so it can
  never claim a membership the authoritative roster has not accepted. But
  `#joinLocal` must NOT move below the roster write, because its set/index adds
  run in the same synchronous turn as `#checkChannelCaps` and that pairing is
  the whole of what keeps the channel cap exact — an awaited round-trip between
  them lets K pipelined subscribes read one count and all act on it (`onMessage`
  is dispatched unserialized). **The intuitive fix is the wrong one**: hoisting
  the roster write to the front looks like it breaks the cap and does not (every
  racer suspends before the read); only an await _between_ the check and the
  adds does — measured at 5 joins admitted against 1 free slot
  ([#323](https://github.com/locknessland/lockness-monorepo/issues/323)).
- **Two check-then-act pairs share ONE synchronous turn, and since
  [#328](https://github.com/locknessland/lockness-monorepo/issues/328) they sit
  in two different methods.** `#checkChannelCaps` reads in `subscribe`; the
  re-join guard's `members.has(...)` decides and `members.set(...)` claims in
  `#joinPresence`; `#joinLocal`'s adds spend. All of it still lands before
  anything yields, because **an `async` body runs synchronously until its first
  `await`** — so `await this.#joinPresence(...)` does not break the turn, and
  the extraction was safe for exactly that reason. **Anything you add above
  `#joinPresence`'s first `await` must keep it true**, and a call boundary makes
  that easier to get wrong, not harder: the two halves no longer sit in one
  screen. Moving the claim below `#joinLocal` is the tidy-looking edit, and
  **every sequential test still passes**; only the pipelined witness dies,
  because `#joinLocal` awaits `#watch` and K frames dispatched by
  `void guard(...)` all read "not a member" and all join
  ([#327](https://github.com/locknessland/lockness-monorepo/issues/327)).
  Measured both ways before the row was written.
- **Every authoritative roster write goes through `#syncRosterMember`, and none
  of them carries its desired state from its caller**
  ([#330](https://github.com/locknessland/lockness-monorepo/issues/330), and
  [ADR 003](../../docs/adr/003-realtime-roster-write-ownership.md) is the
  standing constraint). The caller passes an `origin` — the connection to
  announce as and the member naming the `(channel, member.id)` slot — never the
  desired state, which is read from the local `presence` map **inside** the
  serial tail, at issue time. A direct `roster.holdMember` /
  `roster.releaseMember` is the defect, not a shortcut: `#joinPresence` claims,
  suspends at `#watch`, and its write would otherwise be issued from a state
  that no longer holds the membership — leaving a hold in the authoritative
  roster with no local membership, which only a sweep of a DEAD instance
  reclaims — and it would bypass the only code that turns the `arrived` / `gone`
  bit into a frame. The three remedies ADR 003 rejects are each intuitive enough
  to be proposed again; read it before proposing one. It returns `Promise<void>`
  since #344; the old "member or `undefined`" contract is gone. **On a
  roster-less driver the bits come from `#heldSlots`**, touched only inside the
  queued run and updated BEFORE the announcement, so a write queued behind sees
  the slot already held or released. Deriving them from `presence` sizes in
  `subscribe` / `unsubscribe` instead is per-connection announcing again; an
  early "no roster" return would silence every first `joined`
  ([#342](https://github.com/locknessland/lockness-monorepo/issues/342)).
- **Announcements live in the queue, never back in `subscribe` / `unsubscribe`**
  ([#344](https://github.com/locknessland/lockness-monorepo/issues/344),
  [ADR 004](../../docs/adr/004-realtime-roster-slots-held-per-instance.md)).
  `#announcePresence` is the only sender of a local `joined` / `left` and of a
  `presence-join` / `presence-leave` publish, and it has exactly **two**
  callers, each on a bit a roster write returned: `#syncRosterMember`'s queued
  run (on `arrived` or `gone`, never otherwise) and `#announceDeparture`, the
  handler a driver calls through `onRosterDeparture` for a slot it emptied on
  another process's behalf — the Redis ghost sweep
  ([#348](https://github.com/locknessland/lockness-monorepo/issues/348),
  [ADR 005](../../docs/adr/005-realtime-swept-departures-announced.md)). The
  departure is **not** queued on the slot's tail and nothing is awaited before
  `#announcePresence`: behind an in-flight hold the room would hear `joined`
  then `left` for a member who is present (W8; battery rows M8, M9).
  `handleControl`'s re-emit is the one receive-side exception. Adding an emit or
  publish to `#joinPresence`, `unsubscribe`, the #323 compensation or a new verb
  "so the frame goes out sooner" brings back a `joined` per tab and a `left` for
  a member still present, and a join overtaken by its leave announcing both
  (#330). Three details a tidy-up breaks: **the WARN is never rethrown** — a
  throw inside the tail rejects another call's write and rolls back a committed
  hold — and carries only channel, action and error, never the member id or
  `info`; **no connection hears presence about its own member id** — `joined`
  AND `left` (#349, ADR 007). The exclusion lives in
  `emitPresence(channel,
  frame)` itself, read from `presence` at emit time,
  with **no option**: the old `exceptMemberId` is gone, so no call site passes
  it and none can forget it. Do not reintroduce an `except` argument, exclude at
  the call sites, condition it on `action`, or narrow it to `origin.clientId` —
  that sends a second tab, a remote claimer, or a lapsed instance's own tabs a
  frame about themselves. In a consistent roster a `left` excludes nobody
  anyway. Witnesses: `presence_member_transitions_344.test.ts` (W9),
  `lapse_rehold_349.test.ts` (W3, W3b); battery
  `tests/mutations/presence_member_transitions_344.ts`, and
  `tests/mutations/lapse_rehold_349.ts` M8 / M12.
- **`PresenceMember` is bounded at ADMISSION, and the bound cannot move to the
  publish**
  ([#326](https://github.com/locknessland/lockness-monorepo/issues/326)). The
  roster write happens BEFORE the control publish, so a size check on the
  publish is always too late: the member is already authoritative when the frame
  announcing it is refused, which is a member present in the room and invisible
  to every peer — a cloak an end user buys with a long enough `info`. The
  driver's publish-side check stays as defence in depth and now throws rather
  than returning, but it is not the fix and must not be treated as one.
  `maxPresenceMemberBytes` defaults to half `control.maxPayloadBytes`; the gap
  is the frame envelope, and **the two move together or the invariant "an
  admitted member can always be announced" breaks**.
- **An authorizer result is classified ONCE, in `classifyAuthorizeResult`, and
  anything outside `true` / `false` / a non-array object THROWS**
  ([#347](https://github.com/locknessland/lockness-monorepo/issues/347)). The
  gate used to be `result === false`, so `undefined`, `null`, `0` and `''`
  admitted to private channels and a falsy presence "member" joined with no
  roster entry. Two "simplifications" bring it back. **Never map `invalid` to
  `{ ok: false }`**: #331 gives that one meaning, and a forgotten `return` would
  become a deny-all indistinguishable from policy — the maintainer decided
  `null` / `undefined` throw, not deny Laravel-style. **Never move the
  classification below `#checkChannelCaps` or `connections.set`**: an invalid
  result must be refused before anything exists to undo, and a full connection
  must hear `AuthorizeResultError`, not `ChannelLimitError`. The classifier
  lives in `channel.ts`, not `protocol.ts` (which imports `channel.ts` — a
  cycle), and does not call #346's member-id predicate. `typeLabel` is the one
  place an offending value becomes a log-safe TYPE label; an error message never
  echoes the value. Witness: `authorize_result_347.test.ts`; battery
  `tests/mutations/authorize_result_347.ts`. **The classifiers are total**
  ([#353](https://github.com/locknessland/lockness-monorepo/issues/353)):
  `objectLabel` (`channel.ts`) is the one inspection beyond `typeof`, and it
  runs `Array.isArray` and `instanceof` inside a try, because both throw on a
  revoked Proxy. A value it cannot inspect is `'uninspectable object'`. **Never
  make that label `'object'`**: `isAdmittingObject` asks
  `objectLabel(...) === 'object'`, so the tidy "use `typeof`'s answer" edit
  ADMITS the value (battery M3). `protocol.ts`'s wire predicates share
  `isNonArrayObject` for the same reason, and `isPresenceMemberWire` keeps its
  key and field reads in a try of their own: a LIVE Proxy's `ownKeys` or `get`
  trap runs there, never in `isNonArrayObject`, and the departure handler must
  not throw (battery M7–M9). A result whose `then` cannot be read never reaches
  the classifier: `await` reads it first and rejects with that read's own error
  (a revoked Proxy's `TypeError`, a trap's or getter's error), propagated
  unchanged, and nothing is written. The "join without a member" invariant sits
  above `#checkChannelCaps`, like every other refusal. Witness:
  `manager_debt_353.test.ts`; battery `tests/mutations/manager_debt_353.ts`.
- **A member is parsed once, by `admitPresenceMember` (`presence_member.ts`);
  never read `id`/`info` from the authorizer's object anywhere else, and never
  validate an in-memory copy**
  ([#350](https://github.com/locknessland/lockness-monorepo/issues/350)). The
  authorizer's object is untrusted: a getter or Proxy answers a second read
  differently, a `toJSON` decides what a serialization ships, and a `Date`
  `info` is an object in memory and a string on the wire. So admission reads the
  keys, `id` and `info` once each, serializes `{ id, info }` once, parses those
  bytes back and checks the PARSED copy with `isPresenceMemberWire` — the
  receivers' predicate on the receivers' representation. That copy is the only
  member stored, held, snapshotted or announced. Three tidy-ups bring the leak
  back: returning the candidate (or a spread of it) "to save a parse",
  re-reading `candidate.id` / `candidate.info` when building the draft, and
  checking the draft instead of `JSON.parse(text)`. An own key beside
  `id`/`info` is REFUSED (`PresenceMemberShapeError`), never stripped — the
  product default; the security property holds either way (battery M2).
  Admission is synchronous and stays above `#checkChannelCaps`. The rule has
  changed four times (#306, #326, #346, #350): it changes in
  `presence_member.ts`, not in the orchestrator. Witness:
  `presence_member_admission_350.test.ts`; battery
  `tests/mutations/presence_member_admission_350.ts`.
- **An object result is admitted ONLY by `admitPresenceMember`, on EVERY channel
  kind ([#357](https://github.com/locknessland/lockness-monorepo/issues/357)); a
  private channel discards the member, not the check.** Before #357 a private
  channel admitted any object for being one, and a lookup that found nothing
  usually is one — a Deno KV `{ key, value: null, versionstamp: null }`, a pg
  `QueryResult` with `rows: []`, `{}` — so an untyped authorizer returning its
  lookup put every authenticated user on someone else's private channel. The
  invariant: for every result other than `true`, `subscribe` gives the same
  outcome on `private-X` as on `presence-X`; the kind decides only whether the
  admitted member is seated. The admission sits where #350's did — after the
  `deny` return, before the member invariant, the caps and every write,
  synchronous — and `classifyAuthorizeResult` takes no `kind`. The member errors
  end with `PRIVATE_CHANNEL_HINT` so `admitPresenceMember` stays
  channel-agnostic. Three tidy-ups reopen or distort it:
  - moving the call back inside `if (kind === 'presence')` because "a private
    channel never reads the member" (M1);
  - "saving the parse" on private with an in-memory predicate (M2);
  - making private `true`-only without the product veto, because the type
    sanctions a member on any channel (M3).

  Witness: `authorize_result_357.test.ts`; battery
  `tests/mutations/authorize_result_357.ts`.
- **A `PresenceMember` is deep-frozen where it is minted (`admitPresenceMember`,
  `#parseRosterValue`, `#verifyAndDecode`) and nowhere else**
  ([#354](https://github.com/locknessland/lockness-monorepo/issues/354)).
  `freezePresenceMember` (`presence_member.ts`; package-internal — not exported
  from `mod.ts`) walks with an explicit stack — a recursive walk overflows near
  16 000 levels — and has no `Object.isFrozen` short-circuit. Members are then
  shared BY REFERENCE (local map, memory roster, every snapshot, `encode`, every
  caller of one barrier read), and that is safe only because they are frozen.
  Three edits look reasonable and are wrong:
  - **Never add a per-caller copy in `#closingRead`**: that is the per-caller
    cost #333/#341 exist to refuse, in CPU instead of bytes.
  - **Never freeze at the exit**: it runs per caller, is unsound for objects a
    third-party driver still owns, and misses `encode` and `holdMember`.
  - **Never drop the Redis freeze because "Redis returns fresh objects"**: they
    are fresh per read, but shared by every caller in a barrier batch.

  `here` and `here.members` stay the caller's and are never frozen (battery M7).
  Witness: `presence_member_frozen_354.test.ts`; battery
  `tests/mutations/presence_member_frozen_354.ts`, whose type-level rows T1/T2
  run `deno check` because a non-compiling mutant is DEAD to `runBattery`.
- **A wire presence member is one predicate, `isPresenceMemberWire` in
  `protocol.ts`**
  ([#348](https://github.com/locknessland/lockness-monorepo/issues/348), made
  strict by
  [#350](https://github.com/locknessland/lockness-monorepo/issues/350)): a
  non-array object whose EVERY key is `id` or `info`, an id passing
  `isPresenceMemberIdValue`, an `info` passing `isPresenceMemberInfoValue`. The
  join's admission (on the parsed copy), the Redis frame ingest
  (`isPlainMember`) and the manager's departure handler all ask it, so no
  instance admits or shows a member every peer refuses. The key rule is an
  ALLOW-LIST — #348's count (`<= 2`) let `{ id, smuggled }` through. The roster
  read (`#parseRosterValue`) asks the id and `info` halves only and REDUCES to
  `{ id, info }`: skipping a legacy entry with extra keys would hide a 0.3.0
  member for its session. Package-internal — not exported from `mod.ts`. Never
  re-spell a half at a call site.
- **A presence member id's TYPE is one predicate, `isPresenceMemberIdValue` in
  `protocol.ts`, shared by three sites that must never drift apart**
  ([#346](https://github.com/locknessland/lockness-monorepo/issues/346)): the
  join's id check (`assertUsableMemberId` in `presence_member.ts` since #350),
  and on Redis the frame ingest `isPlainMember` and the roster read
  `#parseRosterValue`. Before it, the two Redis sites each had a copy and the
  join had none, so a `null` / `undefined` / object id joined locally, merged
  different people under one `String(id)` key, and was dropped by every peer.
  **Never inline a `typeof` at one site** — widen one copy and a join succeeds
  here while every peer drops it, silently. **Never add #306's length bound to
  the receive side**: a roster entry skipped for length still counts in `total`
  (#339). The join checks the type BEFORE `String(id)`, which throws on a
  null-prototype object. `PresenceMemberIdError` names a non-primitive id by
  `typeLabel` only. The predicate is not exported from `mod.ts`. Witness:
  `presence_member_id_type_346.test.ts`; battery
  `tests/mutations/presence_member_type_346.ts`.
- **A denial never revokes, and making it revoke was tried and rejected**
  ([#331](https://github.com/locknessland/lockness-monorepo/issues/331)).
  `authorize` runs on every subscribe including a re-subscribe, and when one
  that previously approved now denies, `subscribe` answers `{ ok: false }` and
  changes nothing. It looks like a bug and is a decision: `false` already means
  "refuse this attempt", which in a real deployment includes "not this fast" and
  "the DB blipped" — the `Authorizer` docstring sanctions authorizers that are
  rate-limit increments and DB reads, **on admission; that is not a verb budget
  and cannot be made into one** (#329) — so revoking on it would turn a
  transient failure into an eviction with no compile error and no way to express
  the difference while `AuthorizeResult` stays `boolean | PresenceMember`. It
  would also be a **second, weaker revocation path** beside `evict`, which
  writes its record first and is recovered by `onRevocationReconcile`; a
  denial-driven removal survives nothing. `revokeChannel`
  ([#332](https://github.com/locknessland/lockness-monorepo/issues/332)) is the
  scoped verb that residue asked for, and it extends the SAME durable path
  rather than adding a weaker one beside it. `authorize_denial_331.test.ts` is
  the witness, and it fails the moment a revoke is added here.
- **The revocation delimiter is a SPACE, and it must stay outside `NAME_RE`.** A
  channel-scoped record is the index member `"<target> <channel> <id>"`
  ([#332](https://github.com/locknessland/lockness-monorepo/issues/332),
  [#337](https://github.com/locknessland/lockness-monorepo/issues/337)). A
  connection id is asserted against `/^[A-Za-z0-9:._-]+$/` before it is minted,
  so it can never contain a space — which is what makes an instance on the
  published release skip a composite **structurally** rather than by recognising
  it. Change the delimiter to a `:` or a `.` "for readability" and a composite
  collides with a real id, at which point that instance applies a room ban as a
  `4403` kill of the whole session. Same-version behaviour is identical;
  `mixed_fleet_332.test.ts` is the only witness, and
  `mutations/channel_revoke_332.ts` carries the row.
- **Never re-add `markRevoked` "for compatibility".** A one-parameter
  implementation satisfies a two-parameter signature, so an old driver would
  type-check, ignore the `channel`, and write a **connection**-scoped record —
  which any reader applies as `revokeLocal` → hard-close 4403. That is not a
  per-channel revoke degrading to nothing; it is one **escalating to a socket
  kill**, with no error, no warning and no type failure. The construction-time
  throw in `assertNotLegacyRevocationDriver` exists for exactly this, and it is
  a SEPARATE unit from `revocationStore` on purpose: fused, a later pass
  "restoring consistency" with the two non-throwing sibling probes deletes it.
- **`listRevocations` fails CLOSED.** The revocation index is the one
  cross-instance write channel with no MAC, and after #332 its decoder also
  decides **scope** — so a member returned as `{ target }` because its channel
  half would not parse becomes a whole-connection revocation. Drop what does not
  fully decode; never narrow it. The reap is score-only and the charset filter
  is non-destructive, and both must stay that way: a reader that cannot use a
  record must not be the reader that destroys it, or a rolling deploy deletes
  live revocations. **The read path never deletes; the reap
  (`REAP_REVOKED_SCRIPT`) is the only delete** (#359).
- **A clear names an id, never a pair**
  ([#337](https://github.com/locknessland/lockness-monorepo/issues/337)). Every
  `revokeChannel` call mints its own `ChannelRevocation.id`, and the id is part
  of the index member, so `ZREM` of that member is already a compare-and-delete.
  The correctness argument is one line: a clear can only name an id its caller
  saw before its leave, and a newer write has an id nobody has seen yet. Keyed
  on the pair, the clear for an older revocation erased a newer one in flight,
  and when the newer one's frame was lost nothing enforced it. Three places
  carry the id, and dropping it from any one passes every test that revokes a
  pair only once: the member (`#encodeRevocation`), the frame (`revocationId` on
  the publish), and the MAC (`#canonical`, appended last). The reconcile
  **groups by pair and leaves once**, clearing every listed id on `'left'`.
  Applying records one at a time looks equivalent, but the second one finds
  `'not-subscribed'`, survives, and re-kicks a re-subscribed client on the next
  tick. The witness is `revocation_clear_race_337.test.ts`, and
  `mutations/channel_revoke_332.ts` carries all four rows.
- **`revocationId` is the one control field a `0.3.0` peer cannot verify, and
  that is acceptable only because of WHICH kind carries it.** A field on a kind
  a published peer acts on (`evict`, presence) would cost that peer the action.
  On `revoke-channel`, which `0.3.0` never acted on, it costs a MAC WARN.
  Omitted when `undefined`, it leaves every other kind byte-identical, and
  `mixed_fleet_332.test.ts` pins both halves.
- **Clear-on-apply is asymmetric on purpose.** A connection-scoped record is
  moot once the socket dies, so `evict` leaves it to the TTL; a channel-scoped
  one has a live socket to act on for the whole TTL, so an uncleared record
  re-applies the leave at every reconcile tick and kicks a client that
  legitimately re-subscribed. Removing the clear "for consistency with `evict`"
  is the regression.
- **`revokeChannel` asserts its channel; `unsubscribe` deliberately does not.**
  Not drift. The first MINTS both names onto the control plane and into a
  durable record, where an unusable name is a frame every peer drops and a
  revocation that reported success having revoked nothing. The second is a
  CLEANUP path reached from `disconnect`'s loop, where refusing strands the
  state it would have removed (#314).
- **`unsubscribe`'s outcome is a SERVER value.** Its three states together tell
  whoever receives them whether an arbitrary connection id is live in the fleet,
  whether this instance owns it, and whether it is in a given room. The
  framework ships no socket-to-manager wiring, so the application writes that
  handler — and the id-shaped signature invites passing a client-supplied id.
  Say so at every site that returns one.
- **A roster slot is HELD per instance, and every hold and release is one
  operation**
  ([#323](https://github.com/locknessland/lockness-monorepo/issues/323),
  [#345](https://github.com/locknessland/lockness-monorepo/issues/345)). On
  Redis a slot is the presence-hash field plus its holders hash (`holdersKey`:
  `instanceId → entry`, no TTL), and each holder has an owned-set entry.
  `HOLD_MEMBER_SCRIPT` writes all three and `SADD`s the instance into the
  instances set in one `EVAL`, so no hold exists that the sweep cannot find;
  `RELEASE_MEMBER_SCRIPT` drops only the releaser's entries, deletes the field
  only when no holder is left, and copies another holder's entry in only when
  the shown one was the releaser's (or when neither exists: a non-holder's
  release restores a missing field). **`arrived` and `gone` are decided inside
  those scripts** and decoded by strict decoders: `decodeHoldReply` (1 / 0 /
  throw) and `decodeReleaseReply`, which since #355 names **four** replies — the
  released entry (**emptied**, #348), `KEPT` (**kept**: other holders keep the
  slot), 0 (**absent**) and `REFUSED` (**refused**) — into an unexported
  `ReleaseOutcome`, and throws on anything else. The deregistration has its own
  decoder, `decodeDeregisterReply`: 0 / `REFUSED` (renewed) / `KEPT` (a late
  hold) / throw. **Two decoders, seven replies, one spelling each**: `KEPT = 2`
  and `REFUSED = 3` are named constants in `drivers/redis.ts` (exported for the
  tests, not from `mod.ts`), interpolated into both scripts. Never reuse `1` —
  it is a pre-#348 release reply that must still throw — and never let a
  decoder's error message carry the reply (constant messages, S4). An `HLEN` or
  owner read from TypeScript races another instance's hold, and truthiness would
  announce from an error reply. **The sweep is a release with `deadId`** — the
  same script — and its reply is **not** ignored: `#announceSwept` (called only
  from the sweep's `#sweepPage`) is the only caller of the departure handler,
  and calls it before its first await; it drops (one WARN, channel only) an
  entry whose channel is not a valid name, that does not decode, or whose member
  id is not its slot, and hands the rest to the manager with no I/O await in
  between. `releaseMember` never reports one (a second `left`). It never `DEL`s
  the owned set, or a hold landing mid-sweep becomes unreachable. Three edits
  look harmless and bring #345 back: a raw presence `HDEL` anywhere, an `EXPIRE`
  on the holders hash, and passing `this.instanceId` to the sweep's release.
  Witness: `roster_holders_345.test.ts`; battery
  `tests/mutations/presence_member_holds_345.ts`.
- **One sweep pass at a time, and a sweep writes only while its target is dead**
  ([#355](https://github.com/locknessland/lockness-monorepo/issues/355), ADR
  006). `#armReconcile` is the ONLY place the sweep timer is armed — one
  `setTimeout`, re-armed from the pass's `finally`, never while closing — and
  `#reconcile` has one caller, the callback it arms. Do not put the sweep back
  on a `setInterval`, add an in-flight flag, or call `#reconcile` from anywhere
  else. The `EXISTS` in `#reconcile` only **selects** candidates: each sweep
  write re-checks liveness **inside its script** — the release with
  `ARGV[4] = '1'` and `KEYS[4]` = the **releaser's** liveness key (built only in
  `#release`; the sweeper's own key would refuse every sweep release), and
  `DEREGISTER_INSTANCE_SCRIPT`, which deregisters only while the instance is
  dead **and** owns nothing. A leave passes `'0'`; a TypeScript `EXISTS` before
  a write races the renewal. `#sweepPage` counts N = emptied + kept and E =
  emptied, per release; `#sweepOwned` returns how the sweep ended;
  `#sweepInstance` holds the one per-instance `catch` and the ONE log site, one
  line per instance — "released" (N > 0, also when `close()` cut the sweep
  short), "renewed" (a refused reply: stop that instance, no deregistration) or
  "failed" (its own `catch`: no deregistration, the pass goes on). `close()`
  drops the revocation handler, THEN awaits `#reconcilePass`, THEN drops the
  departure handler; the pass reads `#closing` at **four** points and nowhere
  else — the top of each instance (`#reconcile`), before each page read and
  before the deregistration (`#sweepOwned`, the page-read check since #358),
  before each release (`#sweepPage`) — never between a release reply and the
  handler. A "released" line on a `kept` or `closed` end at N > 0 carries the
  _unfinished_ suffix (#358). Witness: `reconcile_single_pass_355.test.ts`;
  battery `tests/mutations/reconcile_single_pass_355.ts`.
- **The owned set is read in ONE place, in bounded pages**
  ([#358](https://github.com/locknessland/lockness-monorepo/issues/358),
  [ADR 008](../../docs/adr/008-realtime-sweep-reads-owned-set-in-pages.md)).
  `#sweepOwned`'s `SSCAN <owned key> <cursor> COUNT OWNED_SCAN_COUNT` — no
  option but `COUNT`, the page size a module constant that is not configurable —
  is the only read of an owned set. Never an `SMEMBERS` / `SRANDMEMBER` / `SPOP`
  of it, a second `SSCAN` site, a read inside a Lua script, or a literal
  `'100'`. The scan is **one full iteration per pass**, ending only when the
  cursor returns `'0'`: no page or entry budget, no cursor kept in memory or in
  Redis, and an empty page with a non-zero cursor does not end it.
  `decodeScanReply` is the one SCAN-envelope decoder (strict canonical cursor,
  one constant message, `SCAN_REPLY_REFUSED`); a later paged read reuses it.
  - **No page read sits between a release reply and its announcement** (#348
    A1): `#sweepPage` holds the per-entry loop moved verbatim, and the next
    `SSCAN` is issued only after the page's last `#announceSwept`. Never
    prefetch a page, and never collect announcements to make after the loop.
    Keep that loop at 8 / 12 spaces with its names: nine battery rows anchor on
    it.
  - **One SCAN guarantee is relied on, and two scripts cover the rest.** A
    member present for the whole iteration is returned at least once. A
    duplicate, or an entry another survivor already released, is an _absent_
    release (`RELEASE_MEMBER_SCRIPT`'s atomic read-and-delete — no TypeScript
    "seen" set); a member added mid-iteration may be missed, and
    `DEREGISTER_INSTANCE_SCRIPT` then answers _kept_ (`SweepStop` `'kept'`), so
    the instance stays registered for the next pass. No TS flag, `SCARD` or page
    count gates the deregistration.
  - **A multi-page test waits for the pass with FakeTime's drain** — the home of
    this rule. After the tick that fires the pass, `await time.runMicrotasks()`
    (or `tickAsync(0)`): it runs real macrotasks, so a 300-entry sweep finishes
    inside it. Never a fixed count of microtasks (it reads a half-finished pass
    as finished), never a new per-file `settle()` copy, and `close()` only where
    `close()` is the subject — it sets `#closing` first and truncates the pass.
    FakeRedis's scan core pages anything over `COUNT` members, places members by
    `FakeRedis.scanSlot`, and refuses past a per-key and cumulative call
    ceiling, so a scan that never advances fails instead of hanging. Witness:
    `sweep_paging_358.test.ts`; battery `tests/mutations/sweep_paging_358.ts`.
    **Revocation passes too** (#359): a test that needs a finished revocation
    pass drains after the tick that fires it, and after firing the reconnect
    seam, which returns `void` — never `await fireReconnect()` as if it handed
    back the pass. A witness that holds a page or an apply waits on the gate's
    `reached`.
- **The revocation index is read in ONE place, in bounded pages, one pass at a
  time** ([#359](https://github.com/locknessland/lockness-monorepo/issues/359),
  [ADR 009](../../docs/adr/009-realtime-revocation-recheck-reads-index-in-pages.md)).
  `listRevocations` reaps with `REAP_REVOKED_SCRIPT` (it answers the pass's one
  `now`, `t`), then reads `ZSCAN <index> <cursor> COUNT REVOCATION_SCAN_COUNT` —
  no option but `COUNT`, never a literal `'100'`, never a `ZRANGE*` read of the
  index in production — from cursor `'0'` to cursor `'0'`, with no budget and no
  resume state. What a port implementation must return is the `listRevocations`
  JSDoc in `driver.ts`; the enforcement bound is `onRevocationReconcile`'s JSDoc
  in `drivers/redis.ts`. Link to both, never restate them.
  - **Nothing is applied before the enumeration ends** (#337 across pages). The
    manager's re-check awaits the whole `listRevocations(owns)` result, then
    groups by pair, then applies. A per-page callback, an `AsyncIterable` seam
    or grouping per page makes two records of one pair on two pages leave twice,
    and the second leave kicks a client that has legitimately re-subscribed.
    `owns` only lets the driver drop foreign records early; the manager's own
    `connections.has` check still decides.
  - **One revocation pass at a time, with two homes, and both are needed.** The
    driver's single-flight — `#startRevocationPass` (the one entry for the
    timer, the reconnect seam and the #308 retry; a reconnect or retry during a
    pass becomes ONE trailing pass, `'reconnect'` winning) and
    `#armRevocationReconcile` (the one arming site: a `setTimeout` armed from
    the end of the pass that consumed it, never a `setInterval`) — decides when
    a pass runs. The manager's serial tail behind `reconcileRevocations()`
    decides that no two re-checks overlap, because the lapse run's re-check
    (#349) is a second caller the driver never sees: without it, an older
    snapshot re-kicks a client that re-subscribed after the first run's leave.
    Never a boolean flag, a coalescing flag in the manager, or a tail continued
    only on success.
  - **A malformed page fails the pass; a malformed pair is counted and WARNed.**
    A bad envelope, an odd item list, a bad reap reply or a closing driver
    throws a constant message — never `[]`, which reads as "nobody is revoked".
    A pair inside a well-formed page whose score is not canonical epoch seconds
    (`EPOCH_SECONDS`, the one grammar) is skipped and counted, and the pass logs
    ONE `REVOCATION_PAIRS_SKIPPED` WARN with the count, never broker bytes.
    Never a throw (a planted `+inf` is never reaped and would fail every pass),
    never silent (a broker formatting scores differently would leave every
    revocation unenforced). Witness: `revocation_paging_359.test.ts`; battery
    `tests/mutations/revocation_paging_359.ts`.
- **Retirement is never an ownership reader's business**
  ([#361](https://github.com/locknessland/lockness-monorepo/issues/361)). What
  "retired" means is `#retired`'s JSDoc in `manager.ts`, and why is
  [ADR 010](../../docs/adr/010-realtime-disconnect-retires-the-connection-object.md);
  read those, do not restate them. The pitfalls: consulting it in the revocation
  decider or any other ownership reader (`connections` answers ownership),
  re-keying it by id, reading it anywhere but `#assertAdmissible`, and deleting
  from `connections` at `disconnect`'s entry. Witness:
  `disconnect_admission_361.test.ts`; battery
  `tests/mutations/disconnect_admission_361.ts`.
- **`register` is the only writer of `connections`, and a teardown acts only on
  its owner**
  ([#370](https://github.com/locknessland/lockness-monorepo/issues/370),
  [#363](https://github.com/locknessland/lockness-monorepo/issues/363)). The
  rules are the JSDoc of `#assertAdmissible`, `#assertBound` and `#isOwner` in
  `manager.ts`, and why is ADR 010 §7; read those, do not restate them. The
  pitfalls: re-adding a `connections` write anywhere but `register`; narrowing
  clause 2 back to a retiring holder; comparing a binding to an object outside
  those three deciders (`handlerHooks` and `disconnect` ask `#isOwner`); calling
  `#assertAdmissible` from `subscribe` or `#assertBound` from `register`; and
  making `handlerHooks.onClose` pass `conn.id`. Witness:
  `register_only_admission_370.test.ts`; battery
  `tests/mutations/register_only_admission_370.ts`.
- **`handlerHooks` runs the app's `onClose` once per socket whose `onOpen` ran**
  ([#404](https://github.com/locknessland/lockness-monorepo/issues/404)) —
  evicted ones included, refused ones never. The pairing is a closure-local
  `WeakSet` of opened objects; its comment in `handlerHooks` and ADR 010 §7 hold
  why. The pitfalls: gating that hook on `#isOwner` (an evicted socket loses its
  hook); `has` instead of `delete` (a second close runs it again); the `add`
  above `register` (a refused socket counts as opened); and asking the set who
  owns an id. Witness: `onclose_pairing_404.test.ts`; battery
  `tests/mutations/onclose_pairing_404.ts`.
- **One teardown per object, ever**
  ([#393](https://github.com/locknessland/lockness-monorepo/issues/393)).
  `#retired` is a `WeakMap<Connection, Promise<DisconnectOutcome>>`; a second
  `disconnect` of an object already present joins that promise instead of
  computing its own snapshot of `#channelsByClient`. The rule and why are
  `disconnect`'s JSDoc in `manager.ts` and ADR 010's `#393` subsection; read
  those, do not restate them. Since #392, the write sits in the private
  `#teardown(target)` `disconnect` delegates to, not in `disconnect` itself —
  same synchronous turn either way, since `disconnect` awaits nothing before
  delegating. The pitfalls: re-keying `#retired` by `clientId` (a settled entry
  never clears, so a later, genuinely different object under a by-then-free id
  would silently join the old one's promise); writing `#retired` anywhere but
  `#teardown`'s own synchronous prefix, before `#teardownChannels`'s first
  `await`, which is what makes a same-turn double call join instead of race; and
  asking `#assertAdmissible` for anything but `.has(connection)`. Witness:
  `joined_teardown_393.test.ts`; battery
  `tests/mutations/joined_teardown_393.ts`.
- **`disconnect`'s id-form deprecation notice is per MANAGER, and only for
  application callers**
  ([#392](https://github.com/locknessland/lockness-monorepo/issues/392)).
  `disconnect` is a thin wrapper: `typeof target === 'string'` fires
  `#warnIdForm` (guarded by `#idFormWarned`, set once and never cleared), then
  delegates to `#teardown`. `revokeLocal` — `evict`'s own local path — calls
  `#teardown` directly and never reaches `#warnIdForm`; this is what keeps the
  framework's own id-form use silent. The pitfalls: gating the notice on
  anything but `typeof target === 'string'` (an object-form call must never
  raise it — architect-expert rejected a whole-method `@Deprecated()` for
  exactly this); dropping `#idFormWarned`'s guard (a line per socket close under
  churn is worse than one line ever); and routing `revokeLocal` back through the
  public `disconnect` (the framework's own internal caller would then warn about
  its own use of the shape it warns application code about). Witness:
  `deprecate_disconnect_id_392.test.ts`; battery
  `tests/mutations/deprecate_disconnect_id_392.ts`.
- **Every realtime reply that grows with a collection has a named bound** — the
  inventory for `MAX_REPLY_BYTES`'s rule (`@lockness/redis`, `resp.ts`: the
  caller bounds the reply; the cap is a backstop that costs the whole socket).
  Keep this list here and nowhere else; add a row before adding such a read.
  - roster read → `READ_ROSTER_SCRIPT` (bounded inside the script, #341);
  - a dead instance's owned set → `OWNED_SCAN_COUNT` (paged, #358);
  - the instance set (`SMEMBERS` in `#reconcile`) → unbounded, small by
    construction (one entry per running instance);
  - the revocation index → `REVOCATION_SCAN_COUNT` (paged, #359); the reap
    answers `{t, indexKind, floorKind}` (#405/#411: `t` the reaped second,
    `indexKind` and `floorKind` each key's prior Redis type), not one integer;
  - the revocation floor (`ZRANGEBYSCORE` in `markRevocation`, #380) →
    unbounded, small by construction (one member per distinct live TTL);
    `MAX_REPLY_BYTES` is the backstop, and an oversized reply is a read failure,
    so the mark fails closed at `MAX_REVOCATION_TTL_SECONDS`. Never a `LIMIT`:
    truncation fails open.
- **A lapsed-but-alive instance re-asserts its slots, and the pieces live in
  fixed homes**
  ([#349](https://github.com/locknessland/lockness-monorepo/issues/349),
  [ADR 007](../../docs/adr/007-realtime-lapsed-instance-reasserts.md)).
  - **The lapse bit is the renewal's own reply.** `#heartbeat` writes
    `SET … EX … GET`; a nil means the key was re-created. `decodeBeatReply` is
    the one reader (nil → lapsed, bulk → continuous, else a constant throw), and
    it runs **inside** the `SET`'s `try` — moved after it, a refused reply
    escapes an interval callback and `holdMember`'s boot beat. The `catch` keeps
    its two lines (#355 M20's anchor).
  - **The hold gate is read at the tail.** `#holdIssued` is set only in
    `holdMember`, just before its `EVAL`, after the boot beat, never cleared;
    the tail reads it once both writes are done. Not at `holdMember`'s entry
    (the boot nil would count), not "skip the first beat", not read when the
    beat is issued (a hold that overtook the boot `SET` and was swept is
    missed). `#lapseSuspected` carries a failed beat, or a failed run, to the
    next successful beat; a failed `SADD` sets nothing.
  - **`LapseRun` (`drivers/lapse_run.ts`) owns when the handler runs**: never
    awaited by the beat, one run in flight plus exactly one trailing run, none
    once closed, a run never throws (one WARN plus `onFailure`). Do not await it
    from the heartbeat, add a retry timer, or inline the scheduling in the
    driver. `close()` calls `this.#lapse.close()` right after the timers and
    `await stopped` on its own line after the sweep pass — never `Promise.all`,
    and never between `revocationHandler = undefined` and its comment (#355
    M15's anchor).
  - **Revocations are re-checked first.** `#reassertRoster` starts with
    `reconcileRevocations()`; a failure is one WARN and the re-assert goes on —
    it never joins the run's rejection, or a broken store re-asserts every beat.
    Inside `reconcileRevocations`, each revocation is applied in its own `try`:
    one that throws is one WARN naming no target, never the end of the pass — or
    it would starve every revocation listed behind it, pass after pass.
    `close()` waits for this re-check too when it is the run's step in flight.
  - **One slot at a time, through `#syncRosterMember`.** Never `Promise.all` (K
    writes in front of the next heartbeat cause the next lapse), never
    `roster.holdMember` directly (a leave queued on the slot is overtaken), and
    never an announcement of its own: `#announcePresence` keeps its **two**
    callers, and the hold's `arrived` decides the frame. Every slot is tried
    before the one aggregate rejection. The slots are a **snapshot** taken
    before the first write: a walk over the live `presence` map would write
    every join that lands during the run, and under steady joins never end.
  - **The hook rule.** `onRosterLapse` was the fifth optional hook; the shared
    lifecycle is stated once on `BroadcastDriver`. A new hook needs a payload
    **and** a delivery contract that differ from every existing one. Witnesses:
    `lapse_rehold_349.test.ts`, `lapse_run_349.test.ts`; battery
    `tests/mutations/lapse_rehold_349.ts`.
- **A roster release that could not commit is retried, never merely WARNed**
  ([#371](https://github.com/locknessland/lockness-monorepo/issues/371)). Two
  catch sites that used to end at an inline `console.warn` — `unsubscribe`'s
  post-leave release, and `#joinPresence`'s #323/#373 compensation's reclaim —
  now call `#recordOwedRelease`, which queues the slot in `#owedReleases`
  (`ChannelManager`'s own ledger, keyed like `#rosterTails`) instead.
  - **A trigger, never a desired state.** Draining a slot re-issues it through
    `#syncRosterMember`, which re-derives what to write from `presence` at drain
    time — the ledger remembers only THAT a slot needs another pass, never WHAT
    to write. This is why it does not reopen ADR 003. `#drainOwedReleases` walks
    it one entry at a time — never `Promise.all`, `#reassertRoster`'s own
    reasoning — and deletes an entry only if nothing fresher overwrote it while
    its retry was in flight.
  - **Bounded at `MAX_PENDING_ROSTER_RELEASES`** (package-internal, not an
    option). A slot already queued always coalesces onto its newest failure;
    only a genuinely new slot can be refused, with the pre-#371 wording.
  - **`onRosterMaintenance` is the SIXTH optional hook** — never a payload added
    to `onRosterLapse` or `onRevocationReconcile`: its payload is nothing and
    its cadence is "every successful heartbeat, unconditionally", which is
    neither of theirs. Fired from the Redis driver's `#heartbeat` tail, after
    the liveness `SET` succeeds — **never** from `onRevocationReconcile`'s pass,
    which would corrupt the #362/#384 deadline seam, and never gated on
    `#holdIssued`. Its own scheduler, `RosterMaintenanceRun`
    (`drivers/roster_maintenance_run.ts`), is `LapseRun`'s shape without an
    `AbortSignal` — the handler takes no argument, so `close()` only refuses a
    new run and waits for one already in flight. ADR:
    [015](../../docs/adr/015-realtime-owed-release-retried-by-maintenance-drain.md),
    amending ADR 003 §7 and ADR 007 §2. Witnesses: `owed_release_371.test.ts`,
    `roster_maintenance_run_371.test.ts`; battery
    `tests/mutations/owed_release_371.ts`.
- **`heartbeatIntervalMs` and `livenessTtlSeconds` are ONE setting with two
  numbers.** The heartbeat is what keeps this instance's `{prefix}:alive:<id>`
  key alive, and that key's TTL is `livenessTtlSeconds`. Beat slower than the
  TTL and a healthy instance lets its own key lapse between beats, so every peer
  sweeps its presence members out of the roster while its sockets stay open —
  with nothing in the log looking wrong. The constructor now refuses an interval
  above half the TTL
  ([#293](https://github.com/locknessland/lockness-monorepo/issues/293)); two
  beats per window, because one lands on the boundary and races the expiry.
  **That relation is not a ceiling**: the TTL has no upper bound, so the
  constructor also refuses an interval above `MAX_TIMER_MS`, which Deno would
  fire after 1 ms
  ([#381](https://github.com/locknessland/lockness-monorepo/issues/381)). Never
  add a second timer literal; witness `heartbeat_ceiling_381.test.ts`, battery
  `tests/mutations/heartbeat_ceiling_381.ts`. **`#heartbeat` writes the liveness
  key BEFORE `SADD instances`** (#355), and still attempts the `SADD` when the
  `SET` failed: registered with no liveness key, an instance is exactly what a
  peer's sweep takes for dead, while a failed `SET` must still leave it
  registered (#310's scenario). The heartbeat stays an unguarded `setInterval` —
  a guard would turn one slow renewal into a lapse.
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
  is `tests/mutations/self_skip_310.ts`, and its one row is KILLED by that
  scenario — re-proven after #355 reordered `#heartbeat`. It needs a live broker
  and refuses to start without one: the suite it mutates would be ignored, which
  the harness would read as a survivor. The self-skip would have stayed either
  way — a guard unreachable from every valid configuration is the desired state,
  not a redundancy to delete — but unreachability is no longer the reason for
  leaving it untested.

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
  in `{prefix}:revocations` is compared against the reap's `TIME` — read once
  inside `REAP_REVOKED_SCRIPT`, carried to every page as `t`, never re-read per
  page and never `Date.now()` (#359). A stored expiry judged against an
  instance's clock would let a fast-clocked host delete revocations that are
  live for the whole fleet (#276). 2026-09-05.
- **Presence membership is CROSS-PROCESS authoritative** since #268 (shipped
  2026-09-05, `6ed138f4`). The roster lives in the driver — `rosterSnapshot`
  reads a bounded `roster.readRoster(channel, K, selfIds)` window off it (#341),
  never local state (`manager.ts`). **One documented exception since #323**:
  when that read THROWS, `subscribe` returns this instance's own members rather
  than failing a join that has already committed everywhere. The fallback is in
  `#closingRead` (the one closing read every presence exit of `subscribe`
  shares), not in `rosterSnapshot`, and it warns — but a reader who takes "never
  local state" as unconditional will be wrong on that path. And every instance
  shares it. This line previously said "single-process authoritative for the
  MVP, the `here` set is per-instance", which was true before #268 and is the
  premise #312 had to disprove: a reader of the stale version reasons that a
  member "landed in the roster on one instance", and reopens a question that is
  settled. A refused `presence-join` control frame loses the live PUSH to peers
  already in the channel; it does not lose roster state, and the divergence is
  bounded by the connection's lifetime because `disconnect` removes the member
  on the ordinary path
  ([#312](https://github.com/locknessland/lockness-monorepo/issues/312)).
  2026-09-07.
- **The presence snapshot is cut in ONE place, `#closingRead`, by
  `boundPresenceSnapshot`**
  ([#339](https://github.com/locknessland/lockness-monorepo/issues/339)). Five
  rules travel with it, and each has been, or will be, proposed as a fix:
  - **The cut and the self rule never move into the barrier or a driver.** The
    barrier hands one read to several callers; a cut there keeps one caller's
    self and drops another's. The cut is per caller, after the read settles, on
    both the authoritative and the local roster. Since #341 the driver DOES
    bound the read to K — that bounds what is ingested; K and the self rule
    still live only in `presence_snapshot.ts`, which does not trust the driver
    to honour K and cuts again.
  - **Silent.** Truncation is the designed reply, not a fault: no log, no
    counter, no error type. `presence_snapshot.ts` reaches no logger.
  - **`total` costs no extra command and is snapshot-time only.** Since #341 it
    is counted by the driver INSIDE the one read (`HLEN` in the read `EVAL`,
    `size` on the memory map) — never `members.length`, which is the window. Do
    not add an `HLEN` outside the script, a live counter, or `total` on
    `joined`/`left` frames — a per-frame count is the state #329 declined.
  - **No sort.** Driver order is kept; a sort is O(N log N) per caller on a
    shared read, and nobody asks for an order.
  - **`rosterSnapshot`'s spread stays.** It is what gives each caller its own
    array (`presence_roster_read_333.test.ts`); the cut returns its input
    unchanged when the room fits, so it is not a substitute.
- **The roster read is bounded IN THE DRIVER, and there is no unbounded
  fallback**
  ([#341](https://github.com/locknessland/lockness-monorepo/issues/341)).
  `readRoster(channel, limit, selfIds)` is the only roster read on the seam;
  `listMembers` is refused at construction by `assertNotLegacyRosterDriver`
  (`manager.ts`), including beside `readRoster`. Three things get proposed and
  are wrong:
  - **An `HGETALL` "for small rooms", or any second read shape.** A room that is
    small is the one a client grows; any branch that reads the whole hash must
    live inside the one read `EVAL` and stay bounded by `limit`. Keeping
    `listMembers` as a fallback keeps the unbounded read public — the defect.
  - **Counting frames in a batch.** `roster_read_barrier.ts` batches self ids as
    a `Set` of `String(id)`; a repeated id joins its pending batch without
    counting, and the cap `MAX_ROSTER_READ_SELF_IDS` counts DISTINCT ids. N
    pipelined re-join frames from one socket (unmetered, #329) cost two reads;
    counted per frame they cost ⌈N / 1 000⌉ + 1 (S1).
  - **Deciding self with the pre-await id.** `#closingRead`'s `fetchSelfId` only
    widens what the read fetches; the post-await lookup decides what is kept. A
    caller that left during the read must not get its member back from `selves`.
    On Redis a room larger than K is a random sample per subscribe — accepted by
    the maintainer 2026-09-14, not a bug to "stabilise" with an index key (every
    existing room would need backfilling into it — a migration step `0.4.0` does
    not have). The memory driver's O(limit) walk cannot be pinned by a mutant (a
    full copy is equivalent); keep it by review.
    `tests/mutations/presence_read_bound_341.ts`. 2026-09-15.
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

- **`handlerHooks` passes `onMessage` through untouched, and that is a
  DECISION**
  ([#329](https://github.com/locknessland/lockness-monorepo/issues/329)).
  `onOpen` and `onClose` are composed; `onMessage` is not. Do not wrap it to add
  a rate limit, and do not add a churn budget to `ChannelManager` — the
  docstring carries the marker `VERB RATE IS THE APPLICATION'S` and
  `churn_cost_329.test.ts` fails if it is removed. Two reasons, and the second
  is the one that bites. The framework has no charge target a reconnect does not
  rotate: `Connection.id` is minted per socket, so any burst large enough for a
  legitimate re-issue is handed back free on reconnect. And a budget covering
  the whole cycle has to sit on `unsubscribe`, which **six paths reach and only
  one of them is a client** — socket close, a local `evict`, an evict arriving
  from another instance, the durable reconcile, and direct calls. A refusal
  there charges six and means one: a client that spends its budget makes its own
  eviction leave permanent roster ghosts, because `disconnect` re-throws,
  `revokeLocal` catches and warns, and only a ghost sweep of a **dead** instance
  reclaims them. Verb-rate policy is the application's `onMessage`, keyed on a
  stable string derived from `connection.identity`.
- **Every `buildEvents` hook goes through `guard()`, and the sink never throws
  past itself**
  ([#369](https://github.com/locknessland/lockness-monorepo/issues/369)). Each
  event handler `void`s its promise, because Hono does not await it, and the
  package registers no `unhandledrejection` listener. On Deno an unhandled
  rejection **terminates the process**, which takes down every socket and every
  HTTP request with it. A new hook call written as `void hooks.x?.(…)` instead
  of `void guard(conn, () => hooks.x?.(…))` is the defect. `onClose` was written
  that way until #369. It looks harmless because a close "has nothing left to
  do", but `handlerHooks().onClose` awaits `disconnect`, and `disconnect`
  re-throws its first teardown failure by contract. The same holds for
  `reportError()`: its `onError` call is wrapped, and a failing hook falls back
  to one line whose marker `(the onError hook failed too)` sits in the fixed
  prefix. It must stay there, because an error message can be client-controlled
  and a marker placed after it can be forged. Removing that `try` because "the
  app's handler is the app's problem" reopens the crash on all four paths.
  Witness: `websocket_close_guard_369.test.ts`, battery
  `tests/mutations/websocket_close_guard_369.ts`.
- **A control-frame revocation is applied through `#dispatchRevocation`, never a
  bare `void this.#applyRevocation(…)`**
  ([#376](https://github.com/locknessland/lockness-monorepo/issues/376)).
  `#applyRevocation` contains every failure but its own WARN: a `console.warn`
  that throws inside its catch rejects the apply, and the `evict` /
  `revoke-channel` switch has no caller to receive that. Any peer publishing one
  of those frames could then terminate the process. The dispatch ends the chain
  in the #369 shape, with one `REVOCATION_APPLY_LOG_FAILED` ERROR line and no
  re-throw. Witness: `apply_revocation_376.test.ts`, battery
  `tests/mutations/apply_revocation_376.ts`.
- **A marked fallback line is written with `writeMarkedFallback`, never an
  inline `console.error`**
  ([#391](https://github.com/locknessland/lockness-monorepo/issues/391)). The
  last line of a chain that has no caller left must not throw either. A log sink
  that refuses the ERROR as well would otherwise turn the fallback into the very
  unhandled rejection (or uncaught timer exception) it exists to stop. Every
  such sink goes through the helper in `marked_fallback.ts` — among them
  `reportError`'s #369 line and its default line, the Redis revocation and sweep
  chains' last `.catch`, `#warnPassSample`, the deadline's `#write`,
  `#dispatchRevocation` and `#warnFloor` (#380); `git grep writeMarkedFallback`
  is the count, not this list. The helper tries `console.error` first, so a
  patched app console is respected. Then it writes the same bytes and a newline
  to `Deno.stderr.writeSync`. Its final catch drops the line on purpose: no
  channel is left, and a re-throw would kill the process. A new sink of this
  kind calls the helper and gets a row in the witness table. Writing
  `try { console.warn } catch { console.error(MARKER …) }` inline is the defect.
  Witness: `marked_fallback_sinks_391.test.ts` (one row per sink) and
  `marked_fallback_391.test.ts`, battery
  `tests/mutations/marked_fallback_391.ts`.

## Tests

<!-- generated:tests -->

112 test files for 28 source files:

- `packages/realtime/tests/announce_swept_guard_419.test.ts`
- `packages/realtime/tests/apply_revocation_376.test.ts`
- `packages/realtime/tests/authorize_denial_331.test.ts`
- `packages/realtime/tests/authorize_result_347.test.ts`
- `packages/realtime/tests/authorize_result_357.test.ts`
- `packages/realtime/tests/authorize_result_websocket_352.test.ts`
- `packages/realtime/tests/broadcaster.test.ts`
- `packages/realtime/tests/channel_name_boundary.test.ts`
- `packages/realtime/tests/channel_revoke_332.test.ts`
- `packages/realtime/tests/channel_watch_295.test.ts`
- `packages/realtime/tests/channels.test.ts`
- `packages/realtime/tests/churn_cost_329.test.ts`
- `packages/realtime/tests/client.test.ts`
- `packages/realtime/tests/close_drain_368.test.ts`
- `packages/realtime/tests/connection_id_charset.test.ts`
- `packages/realtime/tests/control_auth.test.ts`
- `packages/realtime/tests/control_mac_coverage.test.ts`
- `packages/realtime/tests/control_plane.test.ts`
- `packages/realtime/tests/control_refusal.test.ts`
- `packages/realtime/tests/control_replay.test.ts`
- `packages/realtime/tests/control_replay_window.test.ts`
- `packages/realtime/tests/deliver_local_reauth.test.ts`
- `packages/realtime/tests/deprecate_disconnect_id_392.test.ts`
- `packages/realtime/tests/disconnect_admission_361.test.ts`
- `packages/realtime/tests/disconnect_propagation.test.ts`
- `packages/realtime/tests/driver_contract.test.ts`
- `packages/realtime/tests/driver_redis.test.ts`
- `packages/realtime/tests/driver_redis_live.test.ts`
- `packages/realtime/tests/emit_isolation_323.test.ts`
- `packages/realtime/tests/emit_presence_warn_395.test.ts`
- `packages/realtime/tests/escape_watcher.test.ts`
- `packages/realtime/tests/escaping_sinks_395.test.ts`
- `packages/realtime/tests/events_bridge.test.ts`
- `packages/realtime/tests/eviction_control.test.ts`
- `packages/realtime/tests/eviction_durable.test.ts`
- `packages/realtime/tests/eviction_reconnect.test.ts`
- `packages/realtime/tests/fake_redis_conformance.test.ts`
- `packages/realtime/tests/handler.test.ts`
- `packages/realtime/tests/heartbeat_ceiling_381.test.ts`
- `packages/realtime/tests/identity.test.ts`
- `packages/realtime/tests/join_reclaim_373.test.ts`
- `packages/realtime/tests/joined_teardown_393.test.ts`
- `packages/realtime/tests/lapse_rehold_349.test.ts`
- `packages/realtime/tests/lapse_run_349.test.ts`
- `packages/realtime/tests/leave_outcome_332.test.ts`
- `packages/realtime/tests/live_fake_conformance.test.ts`
- `packages/realtime/tests/log_encoding_291.test.ts`
- `packages/realtime/tests/manager.test.ts`
- `packages/realtime/tests/manager_debt_353.test.ts`
- `packages/realtime/tests/marked_fallback_391.test.ts`
- `packages/realtime/tests/marked_fallback_sinks_391.test.ts`
- `packages/realtime/tests/member_info_bound_326.test.ts`
- `packages/realtime/tests/memory_driver.test.ts`
- `packages/realtime/tests/mixed_fleet_332.test.ts`
- `packages/realtime/tests/onclose_pairing_404.test.ts`
- `packages/realtime/tests/origin.test.ts`
- `packages/realtime/tests/owed_release_371.test.ts`
- `packages/realtime/tests/pass_sample_360.test.ts`
- `packages/realtime/tests/prefix_anchoring.test.ts`
- `packages/realtime/tests/presence.test.ts`
- `packages/realtime/tests/presence_authoritative.test.ts`
- `packages/realtime/tests/presence_cap_concurrency_323.test.ts`
- `packages/realtime/tests/presence_eviction_334.test.ts`
- `packages/realtime/tests/presence_join_compensation_323.test.ts`
- `packages/realtime/tests/presence_join_rosterless_342.test.ts`
- `packages/realtime/tests/presence_local_member_343.test.ts`
- `packages/realtime/tests/presence_member_admission_350.test.ts`
- `packages/realtime/tests/presence_member_frozen_354.test.ts`
- `packages/realtime/tests/presence_member_id.test.ts`
- `packages/realtime/tests/presence_member_id_type_346.test.ts`
- `packages/realtime/tests/presence_member_transitions_344.test.ts`
- `packages/realtime/tests/presence_read_bound_341.test.ts`
- `packages/realtime/tests/presence_rejoin_327.test.ts`
- `packages/realtime/tests/presence_roster_guard.test.ts`
- `packages/realtime/tests/presence_roster_read_333.test.ts`
- `packages/realtime/tests/presence_roster_wrong_type_414.test.ts`
- `packages/realtime/tests/presence_snapshot_bound_339.test.ts`
- `packages/realtime/tests/presence_snapshot_unit_339.test.ts`
- `packages/realtime/tests/presence_sweep.test.ts`
- `packages/realtime/tests/presence_sweep_departure_348.test.ts`
- `packages/realtime/tests/protocol.test.ts`
- `packages/realtime/tests/recheck_revocation_warn_395.test.ts`
- `packages/realtime/tests/reconcile_single_pass_355.test.ts`
- `packages/realtime/tests/redis_broker_integration.test.ts`
- `packages/realtime/tests/redis_warn_trace_418.test.ts`
- `packages/realtime/tests/register_only_admission_370.test.ts`
- `packages/realtime/tests/revocation_atomicity.test.ts`
- `packages/realtime/tests/revocation_clear_race_337.test.ts`
- `packages/realtime/tests/revocation_durability_warn_395.test.ts`
- `packages/realtime/tests/revocation_encoding_332.test.ts`
- `packages/realtime/tests/revocation_floor_wrong_type_405.test.ts`
- `packages/realtime/tests/revocation_index_wrong_type_411.test.ts`
- `packages/realtime/tests/revocation_lastreadat_383.test.ts`
- `packages/realtime/tests/revocation_paging_359.test.ts`
- `packages/realtime/tests/revocation_pass_bound_362.test.ts`
- `packages/realtime/tests/revocation_retry.test.ts`
- `packages/realtime/tests/revocation_seam_332.test.ts`
- `packages/realtime/tests/revocation_tally_384.test.ts`
- `packages/realtime/tests/revocation_ttl_floor_380.test.ts`
- `packages/realtime/tests/revoke_channel_idless_340.test.ts`
- `packages/realtime/tests/roster_atomicity_323.test.ts`
- `packages/realtime/tests/roster_control_atomicity.test.ts`
- `packages/realtime/tests/roster_holders_345.test.ts`
- `packages/realtime/tests/roster_maintenance_run_371.test.ts`
- `packages/realtime/tests/roster_read_barrier_333.test.ts`
- `packages/realtime/tests/roster_slot_key_408.test.ts`
- `packages/realtime/tests/roster_window_341.test.ts`
- `packages/realtime/tests/subscribe_unsubscribe_race_330.test.ts`
- `packages/realtime/tests/sweep_paging_358.test.ts`
- `packages/realtime/tests/teardown_channel_warn_395.test.ts`
- `packages/realtime/tests/websocket.test.ts`
- `packages/realtime/tests/websocket_close_guard_369.test.ts`

59 mutation batteries — **`deno test` does not run these.** Each is an
executable that mutates a source file and re-runs the suites that should notice.
Run them with `deno task mutate` (all of them, one at a time) or
`deno task mutate <name>` (one); nightly CI runs the full sweep. See
[testing.md](../../docs/testing.md#mutation-batteries).

- `packages/realtime/tests/mutations/announce_swept_guard_419.ts`
- `packages/realtime/tests/mutations/apply_revocation_376.ts`
- `packages/realtime/tests/mutations/authorize_result_347.ts`
- `packages/realtime/tests/mutations/authorize_result_357.ts`
- `packages/realtime/tests/mutations/channel_name_314.ts`
- `packages/realtime/tests/mutations/channel_revoke_332.ts`
- `packages/realtime/tests/mutations/close_drain_368.ts`
- `packages/realtime/tests/mutations/connection_id_304.ts`
- `packages/realtime/tests/mutations/deprecate_disconnect_id_392.ts`
- `packages/realtime/tests/mutations/disconnect_admission_361.ts`
- `packages/realtime/tests/mutations/escaping_sinks_395.ts`
- `packages/realtime/tests/mutations/fake_redis_280.ts`
- `packages/realtime/tests/mutations/heartbeat_ceiling_381.ts`
- `packages/realtime/tests/mutations/join_reclaim_373.ts`
- `packages/realtime/tests/mutations/joined_teardown_393.ts`
- `packages/realtime/tests/mutations/lapse_rehold_349.ts`
- `packages/realtime/tests/mutations/live_conformance_285.ts`
- `packages/realtime/tests/mutations/log_encoding_291.ts`
- `packages/realtime/tests/mutations/manager_debt_353.ts`
- `packages/realtime/tests/mutations/marked_fallback_391.ts`
- `packages/realtime/tests/mutations/onclose_pairing_404.ts`
- `packages/realtime/tests/mutations/owed_release_371.ts`
- `packages/realtime/tests/mutations/pass_sample_360.ts`
- `packages/realtime/tests/mutations/prefix_288.ts`
- `packages/realtime/tests/mutations/presence_eviction_334.ts`
- `packages/realtime/tests/mutations/presence_join_323.ts`
- `packages/realtime/tests/mutations/presence_key_guards_414.ts`
- `packages/realtime/tests/mutations/presence_local_member_343.ts`
- `packages/realtime/tests/mutations/presence_member_306.ts`
- `packages/realtime/tests/mutations/presence_member_admission_350.ts`
- `packages/realtime/tests/mutations/presence_member_frozen_354.ts`
- `packages/realtime/tests/mutations/presence_member_holds_345.ts`
- `packages/realtime/tests/mutations/presence_member_transitions_344.ts`
- `packages/realtime/tests/mutations/presence_member_type_346.ts`
- `packages/realtime/tests/mutations/presence_read_bound_341.ts`
- `packages/realtime/tests/mutations/presence_snapshot_339.ts`
- `packages/realtime/tests/mutations/presence_sweep_departure_348.ts`
- `packages/realtime/tests/mutations/reconcile_single_pass_355.ts`
- `packages/realtime/tests/mutations/redis_warn_trace_418.ts`
- `packages/realtime/tests/mutations/register_only_admission_370.ts`
- `packages/realtime/tests/mutations/revocation_durability_warn_395.ts`
- `packages/realtime/tests/mutations/revocation_floor_wrong_type_405.ts`
- `packages/realtime/tests/mutations/revocation_index_wrong_type_411.ts`
- `packages/realtime/tests/mutations/revocation_paging_359.ts`
- `packages/realtime/tests/mutations/revocation_pass_bound_362.ts`
- `packages/realtime/tests/mutations/revocation_retry_308.ts`
- `packages/realtime/tests/mutations/revocation_tally_384.ts`
- `packages/realtime/tests/mutations/revocation_ttl_floor_380.ts`
- `packages/realtime/tests/mutations/revoke_channel_idless_340.ts`
- `packages/realtime/tests/mutations/roster_read_barrier_333.ts`
- `packages/realtime/tests/mutations/roster_sync_330.ts`
- `packages/realtime/tests/mutations/security_review_escape_395.ts`
- `packages/realtime/tests/mutations/self_skip_310.ts`
- `packages/realtime/tests/mutations/subscription_identity_315.ts`
- `packages/realtime/tests/mutations/sweep_paging_358.ts`
- `packages/realtime/tests/mutations/sweep_parse_316.ts`
- `packages/realtime/tests/mutations/timer_suppression_narrowing_415.ts`
- `packages/realtime/tests/mutations/websocket_close_guard_369.ts`
- `packages/realtime/tests/mutations/websocket_error_routing_352.ts`

<!-- /generated:tests -->

## Before you call it done

<!-- generated:gate -->

The framework-wide gate, from the repository root:

```bash
deno task gate             # the full gate, as the pre-push hook runs it
deno task agents:brief     # refresh this file's generated blocks
```

Then, specific to this package: run its 112 test files directly —

```bash
deno test -A packages/realtime/
```

<!-- /generated:gate -->

---

_Framework-wide rules live in the root [AGENTS.md](../../AGENTS.md). The
dependency contract, public surface, tests and closing gate are generated by
`deno task agents:brief` from the code itself — fix the code, not those blocks.
Everything else is hand-written and preserved._
