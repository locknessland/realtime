/**
 * @fileoverview The Redis broadcast driver — cross-process fan-out, the
 * authoritative presence roster, and the authenticated control plane.
 *
 * Publishing a channel event is a normal `PUBLISH` command (args are RESP bulk
 * strings via the client — no inline construction, no RESP injection). Receiving
 * push messages needs a **subscribe-mode connection**, which `@lockness/redis`'s
 * serialized-command `RedisClient` does not provide; that connection is a
 * {@link RedisSubscriber} port.
 *
 * There are two ways to obtain a driver:
 *
 * - **Production (FR-012).** {@link RedisBroadcastDriver.fromConfig} constructs
 *   both ends INTERNALLY from one Redis connection config — a lazily-connecting
 *   `RedisClient` for `PUBLISH` and a dedicated `RedisSubscribeConnection` for
 *   the pub/sub socket — mirroring `@lockness/queue`'s `RedisClient`
 *   construction (`packages/queue/manager.ts`). This is the decision-table home
 *   for "queue-mirror construction"; the `realtime → redis` edge is already
 *   granted and this is what makes the declaration used.
 * - **Tests.** The public constructor still takes the {@link RedisCommandClient}
 *   and {@link RedisSubscriber} ports so a fake bus can be injected — the
 *   injection path is preserved, not replaced.
 *
 * This driver is the single home for three decision-table rules (#268 §5):
 *
 * - **The reserved control-topic name and shape** (evict, presence join/leave):
 *   one {@link RedisBroadcastDriver.controlTopic} + one encode/decode pair,
 *   delivered on the DISTINCT {@link RedisBroadcastDriver.onControl} seam —
 *   never through {@link RedisBroadcastDriver.onMessage}'s channel-event path.
 * - **Whether a control / presence-identity message is authentic**: the FR-015
 *   HMAC over the payload, keyed by the per-deployment secret, attached on
 *   publish and verified on ingest BEFORE the message is actioned; an absent or
 *   failed MAC is dropped with a WARN and never obeyed. The reserved `prefix`
 *   bounds OUTBOUND routing only and is not an inbound boundary — see
 *   `RedisBroadcastDriverOptions.prefix`, which is the single home for what it
 *   does and does not guarantee.
 * - **Who is authoritatively "here"** and **how a member is identified for the
 *   sweep**: the per-presence-channel Redis roster keyed by member id, each
 *   entry tagged with the owning-instance id (internal, FR-018), plus the
 *   instance-scoped ghost sweep (Q1/FR-008).
 *
 * It performs NO authorization — local re-authorization is
 * `ChannelManager.deliverLocal`'s single home (S6).
 *
 * @module @lockness/realtime/drivers/redis
 */

import {
    type BroadcastDriver,
    type BroadcastMessage,
    type ChannelRevocation,
    type ControlMessage,
    type ControlRefusal,
    MAX_ROSTER_READ_SELF_IDS,
    type Revocation,
    type RevocationTally,
    type RosterDeparture,
    type RosterHold,
    type RosterRelease,
    type RosterWindow,
} from '../driver.ts'
import {
    isPresenceMemberIdValue,
    isPresenceMemberInfoValue,
    isPresenceMemberWire,
    isValidName,
} from '../protocol.ts'
import { sameMemberId } from '../presence_snapshot.ts'
import { freezePresenceMember } from '../presence_member.ts'
import { ControlReplayWindow } from '../control_replay_window.ts'
import {
    type MarkedFallbackMarker,
    markedFallbackMarker,
    writeMarkedFallback,
} from '../marked_fallback.ts'
import { LapseRun } from './lapse_run.ts'
import { RosterMaintenanceRun } from './roster_maintenance_run.ts'
import { awaitCloseDrain, type CloseDrainPending } from './close_drain.ts'
import {
    EnforcementDeadline,
    REVOCATION_LOG_FAILED,
} from './enforcement_deadline.ts'
import { type PresenceMember, typeLabel } from '../channel.ts'
import type { RealtimeControlConfig } from '../types.ts'
import { renderError, safeForLog } from '@lockness/contract'
import {
    hmacSha256Hex,
    RedisClient,
    type RedisClientConfig,
    RedisSubscribeConnection,
    type RedisSubscribeConnectionConfig,
} from '@lockness/redis'

/**
 * Extra seconds on the index key's own TTL, beyond the longest revocation it can
 * hold. It only has to outlive the newest member, and it is refreshed on every
 * write; the slack keeps a key that is still being written from expiring under
 * a member (#276).
 */
const INDEX_TTL_SLACK_SECONDS = 60

/**
 * Self-heal a wrong-typed revocation INDEX key, inside the same atomic `EVAL`
 * as the caller's own body (#411) — the `FLOOR_WRITE`-shaped heal (#405)
 * generalised to the index, spliced into both write sites that touch it with
 * a type-sensitive command: {@link REAP_REVOKED_SCRIPT} (before its
 * `ZREMRANGEBYSCORE`) and {@link MARK_REVOKED_SCRIPT} (before its `ZADD`).
 *
 * `local indexKind = redis.call('TYPE', index)['ok']` reads the key's current
 * Redis type — `['ok']` because a live broker wraps `TYPE`'s reply as a Lua
 * table, `{ ok = '<kind>' }`, never a bare string (measured for #405, not
 * assumed). Five independently-gated blocks, one per type other than
 * `zset`/`none`, then `DEL` the key before the caller's own body runs.
 *
 * **Why `DEL` is safe here, for a DIFFERENT reason than the floor's.** The
 * floor's heal is safe because the floor is fully re-derivable — every entry
 * is rewritten by the next reap or announce. The index holds primary,
 * non-derived revocation records, and DEL is safe here for a narrower, harder
 * reason: **any command able to change a key's TYPE has already discarded the
 * prior value, unconditionally, before Redis ever raises `WRONGTYPE`** — a
 * `SET`/`RENAME`/`COPY REPLACE`/`RESTORE REPLACE` overwrites the value first,
 * and a WRONGTYPE-refusing write (`SADD`/`HSET`/`LPUSH`) never gets far enough
 * to reach this script at all. So the former zset is already gone, at the
 * Redis layer, the instant `TYPE` disagrees — before any remedy here runs.
 *
 * **That claim is about the value that is GONE, not the value that is
 * THERE.** A `RENAME`/`COPY … REPLACE`/`RESTORE … REPLACE`, an inbound
 * `MIGRATE` or a `SWAPDB` can land another key's live data under this name,
 * and the `DEL` then destroys that data too — and if what lands is itself a
 * zset, `TYPE` reads `zset`, no heal runs, and revocation records mix into it
 * undetected. Both need a client that can already write this keyspace, and no
 * Lua script can defend against that from the inside. The control is the one
 * `docs/realtime.md` already makes a condition ("Security posture: the bus is
 * trusted"): a Redis ACL under which no credential but the app's own reaches
 * `~<prefix>__*`.
 *
 * Quarantining the key (renaming it aside) would therefore preserve nothing
 * recoverable: no forensic value beyond the WARN's own "prior type" word, at
 * the cost of an unbounded, un-TTL'd key an attacker can keep spawning under
 * repeated corruption. Rebuilding from per-record keys is inapplicable, not
 * merely rejected: every revocation is a member of this ONE sorted set, no
 * per-record key is ever written, and manufacturing one now would double
 * every mark's round trips.
 *
 * Runs over the bound local `index`, never `KEYS`/`ARGV` directly — the same
 * discipline as `FLOOR_WRITE`'s `floor`, because the two callers bind it from
 * different `KEYS` positions (the reap's `KEYS[1]`, the mark's own `KEYS[1]`).
 * **Never `pcall`, `else`, `~=` or reassignment**: `packages/redis/tests/lua_eval.ts`
 * (the shared Lua subset three other packages also depend on) proves all four
 * unsupported. Both callers' replies carry `indexKind`, so their caller can
 * WARN once on a heal through {@link RedisBroadcastDriver.#warnIfIndexHealed}.
 */
const INDEX_HEAL: string = [
    "local indexKind = redis.call('TYPE', index)['ok']",
    "if indexKind == 'string' then",
    "    redis.call('DEL', index)",
    'end',
    "if indexKind == 'list' then",
    "    redis.call('DEL', index)",
    'end',
    "if indexKind == 'set' then",
    "    redis.call('DEL', index)",
    'end',
    "if indexKind == 'hash' then",
    "    redis.call('DEL', index)",
    'end',
    "if indexKind == 'stream' then",
    "    redis.call('DEL', index)",
    'end',
].join('\n')

/**
 * Record a revocation: ONE operation, expiring at a Redis-decided instant.
 *
 * `TIME` is read inside the script, so the expiry is set from Redis's clock
 * and no instance's wall clock takes part in the decision (#276 FR-012) —
 * the property #271's monotonicity argument rests on.
 *
 * **Every** write here is extend-only, and it takes THREE calls to be so.
 *
 * - `ZADD … GT` protects one member's score, so a re-eviction from an instance
 *   configured with a shorter `revocationTtlSeconds` cannot pull that member's
 *   expiry back in.
 * - `EXPIRE … NX` **arms** the key's own TTL, and only when it has none.
 * - `EXPIRE … GT` **extends** it, and only upward — so the same shorter-TTL
 *   instance cannot shrink the whole key and take every live revocation in it
 *   down, which would undo at key granularity what the `ZADD` guarantees at
 *   member granularity.
 *
 * `NX` and `GT` cannot be combined in one `EXPIRE`, and `GT` alone is inert:
 * Redis treats a key with **no** TTL as having an *infinite* one, so `GT` always
 * refuses it and the key would simply never expire. That is a real trap — it
 * looks like a working guard and silently bounds nothing (#276 review cycle 2).
 *
 * **`ARGV[1]` is the effective TTL** (#380), not necessarily the writer's own:
 * the mark reads the revocation floor first and passes the largest of its own
 * TTL and every floor entry, or the maximum TTL when the floor cannot be read.
 * The script itself does not know the floor exists.
 *
 * **Self-heals a wrong-typed index key, inside the same atomic `EVAL`**
 * (#411): {@link INDEX_HEAL} runs first, over the bound local `index`, and its
 * `DEL` — when it fires — lands before this script's own `ZADD` ever runs, so
 * the heal and the mark it was about to perform land in ONE round trip. The
 * reply widens to `{indexKind}`, decoded by {@link decodeMarkReply}.
 *
 * `KEYS[1]` index key · `ARGV[1]` ttl seconds · `ARGV[2]` connection id ·
 * `ARGV[3]` the index key's own TTL.
 */
const MARK_REVOKED_SCRIPT: string = [
    'local index = KEYS[1]',
    "local t = redis.call('TIME')[1]",
    INDEX_HEAL,
    "redis.call('ZADD', index, 'GT', t + ARGV[1], ARGV[2])",
    "redis.call('EXPIRE', index, ARGV[3], 'NX')",
    "redis.call('EXPIRE', index, ARGV[3], 'GT')",
    'return {indexKind}',
].join('\n')

/**
 * Write one instance's entry in the revocation floor (#380) — **the only Lua
 * that writes the floor**, a fragment spliced into exactly two scripts:
 * {@link REAP_REVOKED_SCRIPT} (every revocation pass refreshes the entry) and
 * {@link ANNOUNCE_FLOOR_SCRIPT} (the first registration writes it before any
 * pass has run). Nothing else writes the floor: not the mark, not
 * `close()`, not the heartbeat, not the ghost sweep.
 *
 * The floor is a sorted set whose member is a TTL in seconds and whose score
 * is the broker second that entry lapses. A mark reads every member and
 * scores its record at the largest one, so a record outlives the longest TTL
 * among the instances still passing.
 *
 * It runs over **bound locals**, never `KEYS`/`ARGV`, because its two callers
 * bind them from different positions — the reap carries the index key first,
 * the announce carries only the floor (A2). Each caller binds `t` (the
 * broker's `TIME`), `floor` (the key), `ttl` (the caller's own
 * `revocationTtlSeconds`) and `keyTtl` (the key's own TTL, `ttl` plus the
 * index's slack).
 *
 * **Self-heals a wrong-typed floor key, inside the same atomic `EVAL`**
 * (#405). A key on the shared bus is not this driver's alone to trust: a
 * `string`, `list`, `set`, `hash` or `stream` at `floor` used to raise
 * `WRONGTYPE` on the very first call below and abort the whole reap or
 * announce — silently halting revocation enforcement fleet-wide for as long
 * as the key stayed wrong-typed. `local kind = redis.call('TYPE', floor)['ok']`
 * reads the key's current Redis type. **`['ok']` is measured, not assumed**: a
 * live broker wraps `TYPE`'s reply as a Lua table, `{ ok = '<kind>' }`, never a
 * bare string — confirmed against Redis 7 before this landed, which the
 * architect-expert disposition this self-heal otherwise follows exactly got
 * wrong (it read `fake_redis.ts`'s own flattening of a status reply to a bare
 * string as if it were the broker's behaviour, and called `kind` indexable
 * with no `[...]` at all). Comparing the un-indexed table against a string
 * literal is never an error in Lua — it is simply always `false` — so the
 * heal would have silently never fired against any real broker while every
 * fake-broker witness stayed green. `lua_eval.ts` gained ONE narrow addition
 * for it: `redis.call(...)['ok']` reads the same flattened string this bridge
 * already returns for a status reply — no general string-keyed table, and
 * still no `pcall`, `else`, `~=` or reassignment. Five independently-gated
 * blocks, one per type other than `zset`/`none`, then `DEL` the key before the
 * write below ever runs. **Never `pcall`, `else`, `~=` or reassignment**:
 * `packages/redis/tests/lua_eval.ts` (the shared Lua subset three other
 * packages also depend on) proves all four unsupported, and extending it
 * further for one driver's edge case would be a second, weaker home for the
 * same reasoning ADR013 already used to reject extending it for the floor's
 * own decode. Both callers now return `kind`, so their caller can WARN once on
 * a heal — {@link REAP_REVOKED_SCRIPT}'s triple and
 * {@link ANNOUNCE_FLOOR_SCRIPT}'s bare reply. **The `DEL` never touches
 * anything but the floor key itself — never the revocation index.** The
 * index gets the SAME heal, but never through this fragment: it is a
 * different structure with a different write pattern (#411), and
 * {@link INDEX_HEAL} is its own, separate fragment for exactly that reason —
 * generalising this `DEL`'s reach "while we're in there" would let a floor
 * heal destroy live revocations instead of leaving that to the index's own,
 * narrower fragment.
 *
 * - `ZADD … GT` writes the entry at `t + ttl`, and never pulls an existing
 *   entry for the same TTL back in (a broker clock that steps back).
 * - `ZREMRANGEBYSCORE … -inf t` prunes every entry that has lapsed, so an
 *   instance that stopped passing drops out of the floor one TTL after its
 *   last write, and records return to their writer's own TTL.
 * - `EXPIRE … NX` then `EXPIRE … GT` arm and extend the key's own TTL, so a
 *   fleet that stops leaves no floor behind. Why it takes both calls is
 *   {@link MARK_REVOKED_SCRIPT}'s JSDoc.
 */
const FLOOR_WRITE: string = [
    "local kind = redis.call('TYPE', floor)['ok']",
    "if kind == 'string' then",
    "    redis.call('DEL', floor)",
    'end',
    "if kind == 'list' then",
    "    redis.call('DEL', floor)",
    'end',
    "if kind == 'set' then",
    "    redis.call('DEL', floor)",
    'end',
    "if kind == 'hash' then",
    "    redis.call('DEL', floor)",
    'end',
    "if kind == 'stream' then",
    "    redis.call('DEL', floor)",
    'end',
    "redis.call('ZADD', floor, 'GT', t + ttl, ttl)",
    "redis.call('ZREMRANGEBYSCORE', floor, '-inf', t)",
    "redis.call('EXPIRE', floor, keyTtl, 'NX')",
    "redis.call('EXPIRE', floor, keyTtl, 'GT')",
].join('\n')

/**
 * Reap expired revocations and answer the `now` it reaped against — the
 * revocation pass's ONLY delete, and its one `now` (#359).
 *
 * It reads `TIME` inside the script, removes every member whose score is at
 * or below that second, and returns the second exactly as Redis gave it: one
 * integer's worth of reply, whatever the size of the index. The pass then
 * reads the index in `ZSCAN` pages and keeps only members scored strictly
 * above this `t`.
 *
 * **Why splitting the reap from the read does not re-open #276.** #276's race
 * was a read in one round trip acted on by a DELETE in a later one. Here the
 * delete is still one script bounded by its own `TIME`, so a live revocation
 * (score above `t`) cannot be removed by it; the read deletes nothing; and
 * `t` is carried to every page, never re-read — so a page read later cannot
 * judge liveness against a later clock than the reap did. A member that
 * another instance's reap removes mid-pass had expired at that reap's `now`.
 *
 * The `ZREMRANGEBYSCORE` is a bare call statement, not a returned value.
 *
 * **The floor write rides on the reap** (#380): after the index delete, the
 * script refreshes this instance's entry in the revocation floor through
 * {@link FLOOR_WRITE}, against the same `t`, so a reader that keeps passing
 * keeps its TTL on the floor with no extra round trip. The reap is still the
 * pass's only delete of the INDEX; the floor write prunes only floor entries.
 *
 * **The index self-heals here too, before the delete above** (#411):
 * {@link INDEX_HEAL} runs first, over the bound local `index`, so a
 * wrong-typed index no longer aborts the whole pass with `WRONGTYPE` — the
 * SAME gap #405 closed for the floor, generalised to the index by the same
 * disposition. Unlike the mark, this script never `ZADD`s the index, so a
 * heal here leaves it ABSENT (`none`) rather than a fresh `zset` — "usable
 * again", not "a zset again": the next mark or a raw write succeeds against
 * it with nothing left to work around.
 *
 * **The reply is `{t, indexKind, floorKind}` since #411** (`{t, kind}` since
 * #405, `kind` renamed `floorKind` on decode): `indexKind` is
 * {@link INDEX_HEAL}'s `TYPE` read and `floorKind` is {@link FLOOR_WRITE}'s,
 * so the caller can WARN once for each key it healed (a kind outside
 * `zset`/`none`) — decoded by {@link decodeReapReply}, its one caller
 * `listRevocations`.
 *
 * Called as `EVAL <script> 2 <index> <floor> <ttl> <ttl + slack>`:
 * `KEYS[1]` index key · `KEYS[2]` floor key · `ARGV[1]` this instance's
 * `revocationTtlSeconds` · `ARGV[2]` the floor key's own TTL.
 */
const REAP_REVOKED_SCRIPT: string = [
    'local index = KEYS[1]',
    "local t = redis.call('TIME')[1]",
    INDEX_HEAL,
    "redis.call('ZREMRANGEBYSCORE', index, '-inf', t)",
    'local floor = KEYS[2]',
    'local ttl = ARGV[1]',
    'local keyTtl = ARGV[2]',
    FLOOR_WRITE,
    'return {t, indexKind, kind}',
].join('\n')

/**
 * Announce one instance's entry in the revocation floor at its first
 * registration (#380), before any revocation pass has run — the second and
 * last caller of {@link FLOOR_WRITE}. The reap refreshes the entry from then
 * on; a mark written in the interval before a new reader's first reap is
 * therefore already scored at that reader's TTL.
 *
 * **It never carries the index key** (A2): called as `EVAL <script> 1 <floor>
 * <ttl> <ttl + slack>`, it differs from the reap in numkeys and in shape, so
 * nothing that picks out the reap can pick up the announce. **It returns
 * `kind` since #405** — {@link FLOOR_WRITE}'s `TYPE` read, so
 * {@link RedisBroadcastDriver.#announceFloor} can WARN once when it healed a
 * wrong-typed floor key — where it used to return nothing. `KEYS[1]` floor
 * key · `ARGV[1]` this instance's `revocationTtlSeconds` · `ARGV[2]` the
 * floor key's own TTL.
 */
const ANNOUNCE_FLOOR_SCRIPT: string = [
    "local t = redis.call('TIME')[1]",
    'local floor = KEYS[1]',
    'local ttl = ARGV[1]',
    'local keyTtl = ARGV[2]',
    FLOOR_WRITE,
    'return kind',
].join('\n')

/**
 * The delimiter between the names inside one index member: a channel-scoped
 * record is `"<target> <channel> <id>"` (#332, #337), a whole-connection one is
 * the bare `target`.
 *
 * **A space, and it must stay outside `NAME_RE`** (`/^[A-Za-z0-9:._-]+$/`).
 * That is what makes the composite decidable in both directions, and what makes
 * an instance running an older release *inert* rather than confused when it
 * meets one: a connection id that passed the manager's charset assertion can
 * never contain a space, so `connections.has('c1 private-orders')` is
 * structurally false and the record is skipped rather than acted on.
 *
 * Change it to a `:` or a `.` "for readability" and a composite can collide
 * with a real connection id — at which point an older reader applies a
 * room-scoped revocation as a **whole-socket** kill. Every same-version test
 * still passes; only the mixed-fleet witness fails, which is why the mutation
 * battery carries a row for exactly this substitution.
 */
const REVOCATION_SCOPE_SEPARATOR = ' '

/**
 * The `COUNT` of every revocation-index page the revocation pass reads (#359)
 * — the single home of that page size, and not configurable: no option, no
 * environment variable. Exported for the test suite only; `mod.ts` does not
 * re-export it.
 *
 * **The bound it buys.** A member is at most 602 bytes (a channel-scoped
 * record: three 200-byte names and two separators) and its score about 12, so
 * with RESP framing a page of about `COUNT` pairs plus the rest of the last
 * bucket visited stays under about 130 KB of wire at maximum member length
 * (typically about 12 KB) — a few MiB of heap at the roughly 20× amplification
 * `resp.ts` measured — against the command client's 32 MiB reply cap. An
 * instance holds one page of other instances' records at a time, plus its own
 * matches until the pass ends.
 *
 * **It holds only while the broker honours `COUNT`.** A listpack-encoded
 * sorted set is answered whole whatever `COUNT` says (by default at most 128
 * entries of at most 64 bytes, so harmless); an operator who raises
 * `zset-max-listpack-*`, or a Redis-compatible server that answers `ZSCAN`
 * whole, reopens a large reply on that deployment.
 *
 * **Why 100 and not 1,000**: the pass runs on every instance on every tick,
 * so the per-page heap is paid fleet-wide, continuously; the round trips a
 * smaller page adds are one per hundred records.
 */
export const REVOCATION_SCAN_COUNT = 100

/**
 * Self-heal a wrong-typed OWNED-SET key, inside the same atomic `EVAL` as the
 * caller's own body (#414) — the {@link INDEX_HEAL}-shaped heal generalised
 * from a sorted set to a plain SET. Spliced into {@link HOLD_MEMBER_SCRIPT}
 * and {@link RELEASE_MEMBER_SCRIPT}, over the bound local `owned`, before
 * each one's own `SADD`/`SREM`.
 *
 * **Self-heal is safe here for a narrower reason than the revocation
 * index's.** The owned set is scoped to ONE writer — the instance whose id
 * names it — and feeds no `arrived`/`gone` decision the manager announces
 * from; it is bookkeeping the sweep enumerates, never roster membership
 * itself. Its prior members are already destroyed at the Redis layer the
 * instant `TYPE` disagrees, the same argument {@link INDEX_HEAL}'s own
 * docstring makes for the index — a `DEL` here changes nothing about that
 * loss, it only lets THIS hold or release proceed rather than aborting the
 * whole `EVAL`. The architect-expert disposition (issue #414) is binding;
 * ADR 016 records the standing rule this and {@link INSTANCES_HEAL} follow.
 *
 * `local ownedKind = redis.call('TYPE', owned)['ok']`, then five
 * independently-gated blocks, one per type other than `set`/`none`, then
 * `DEL` before the caller's own write runs. **Never `pcall`, `else`, `~=` or
 * reassignment**: `packages/redis/tests/lua_eval.ts` proves all four
 * unsupported, the same discipline {@link INDEX_HEAL} and `FLOOR_WRITE`
 * keep. Both callers' replies widen to carry `ownedKind`, decoded by
 * {@link decodeHoldReply} / {@link decodeReleaseReply} and WARN'd once
 * through {@link RedisBroadcastDriver.#warnFloor}'s sink via
 * {@link RedisBroadcastDriver.#warnIfOwnedHealed} — never a second sink.
 */
const OWNED_HEAL: string = [
    "local ownedKind = redis.call('TYPE', owned)['ok']",
    "if ownedKind == 'string' then",
    "    redis.call('DEL', owned)",
    'end',
    "if ownedKind == 'list' then",
    "    redis.call('DEL', owned)",
    'end',
    "if ownedKind == 'hash' then",
    "    redis.call('DEL', owned)",
    'end',
    "if ownedKind == 'zset' then",
    "    redis.call('DEL', owned)",
    'end',
    "if ownedKind == 'stream' then",
    "    redis.call('DEL', owned)",
    'end',
].join('\n')

/**
 * Self-heal a wrong-typed INSTANCES-SET key (#414), the same shape as
 * {@link OWNED_HEAL} generalised to the one fleet-wide instances set.
 * Spliced into {@link HOLD_MEMBER_SCRIPT} and
 * {@link DEREGISTER_INSTANCE_SCRIPT}, over the bound local `instances`,
 * before each one's own `SADD`/`SREM`.
 *
 * **Self-heal is safe here for a DIFFERENT reason than the owned set's**:
 * the instances set is fully re-derivable rather than single-writer-scoped —
 * every live instance's heartbeat unconditionally re-`SADD`s itself every
 * `heartbeatIntervalMs` (#349), so a wipe self-repairs fleet-wide within one
 * interval, the same re-derivability `FLOOR_WRITE` already rests on for the
 * revocation floor. It feeds no `arrived`/`gone` decision either. ADR 016 is
 * the standing rule both fragments follow.
 *
 * `local instancesKind = redis.call('TYPE', instances)['ok']`, then five
 * independently-gated blocks, one per type other than `set`/`none`, then
 * `DEL` before the caller's own write runs. Both callers' replies widen to
 * carry `instancesKind`, decoded by {@link decodeHoldReply} /
 * {@link decodeDeregisterReply} and WARN'd once through
 * {@link RedisBroadcastDriver.#warnIfInstancesHealed} — the same sink,
 * never a sibling.
 */
const INSTANCES_HEAL: string = [
    "local instancesKind = redis.call('TYPE', instances)['ok']",
    "if instancesKind == 'string' then",
    "    redis.call('DEL', instances)",
    'end',
    "if instancesKind == 'list' then",
    "    redis.call('DEL', instances)",
    'end',
    "if instancesKind == 'hash' then",
    "    redis.call('DEL', instances)",
    'end',
    "if instancesKind == 'zset' then",
    "    redis.call('DEL', instances)",
    'end',
    "if instancesKind == 'stream' then",
    "    redis.call('DEL', instances)",
    'end',
].join('\n')

/**
 * Hold a roster slot for one instance — ONE operation, four structures (#345).
 *
 * A roster slot is a field in the channel's presence hash; **who holds it** is
 * the slot's holders hash, `instanceId → that instance's latest entry JSON`. A
 * hold writes the instance's entry into both, records the slot in the
 * instance's owned set (what the ghost sweep enumerates), and registers the
 * instance itself — so no hold can exist on an instance the sweep does not know
 * of (S1b). Two structures encoding one fact must not be writable into
 * disagreement (#276, #323): a field with no owned entry is unreclaimable by
 * any instance, forever.
 *
 * **`arrived` is decided HERE and nowhere else**: 1 iff the holders `HSET`
 * added a field and the hash then has exactly one — this instance filled an
 * empty slot. Inferring it from the presence `HSET` reply would report a slot
 * another instance already fills as an arrival; reading `HLEN` from TypeScript
 * would race another instance's hold between the two round-trips.
 *
 * **No `EXPIRE`, deliberately.** A holders hash that expired alone would bring
 * #345 back — a release would see no holders and delete a slot another
 * instance still holds — so the realtime Redis must not evict these keys.
 *
 * **Four keys, and they hash to different slots**: a cross-slot `EVAL` is
 * refused on Redis Cluster, which this package does not target.
 *
 * **The presence hash is guarded, read-only, before ANY write (#414).** The
 * very first statement is a bare `HGET` of the presence field this hold is
 * about to write — its value is discarded; it runs only so that a
 * wrong-typed presence key raises `WRONGTYPE` here, before the holders
 * `HSET` below ever commits. Before this fix the holders write ran FIRST and
 * the presence write SECOND, so a corrupt presence key let the holders
 * `HSET` commit and then aborted on the presence `HSET` — a holder entry
 * with no matching presence field, the exact orphan shape ADR 004 §5 closed
 * for a different cause. **This guard never heals**: the presence hash holds
 * live membership state, and a heal-`DEL` here would silently erase every
 * OTHER member's shown entry with zero `left` frames (ADR 016). It fails the
 * whole `EVAL` closed instead, so nothing commits.
 *
 * **The owned set and the instances set self-heal (#414)**, `KEYS[3]` through
 * {@link OWNED_HEAL} and `KEYS[4]` through {@link INSTANCES_HEAL}, each just
 * before its own `SADD` — neither feeds an `arrived`/`gone` decision, and ADR
 * 016 is the standing rule for why these two may heal while presence and
 * holders may not. The reply widens to `{arrived, ownedKind, instancesKind}`,
 * decoded by {@link decodeHoldReply}, which WARNs once for each key it healed
 * through {@link RedisBroadcastDriver.#warnIfOwnedHealed} /
 * {@link RedisBroadcastDriver.#warnIfInstancesHealed}.
 *
 * `KEYS[1]` presence hash · `KEYS[2]` holders hash · `KEYS[3]` owned set ·
 * `KEYS[4]` instances set ·
 * `ARGV[1]` field · `ARGV[2]` instance id · `ARGV[3]` entry JSON ·
 * `ARGV[4]` owned entry.
 */
const HOLD_MEMBER_SCRIPT: string = [
    "redis.call('HGET', KEYS[1], ARGV[1])",
    "local added = redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])",
    "redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])",
    'local owned = KEYS[3]',
    OWNED_HEAL,
    "redis.call('SADD', owned, ARGV[4])",
    'local instances = KEYS[4]',
    INSTANCES_HEAL,
    "redis.call('SADD', instances, ARGV[2])",
    "local n = redis.call('HLEN', KEYS[2])",
    'if added == 1 then',
    '  if n == 1 then',
    '    return {1, ownedKind, instancesKind}',
    '  end',
    'end',
    'return {0, ownedKind, instancesKind}',
].join('\n')

/**
 * The *kept* reply code (#355): the release dropped a hold the releaser had,
 * and other holders keep the slot. Also what {@link DEREGISTER_INSTANCE_SCRIPT}
 * answers when the instance still owns a hold.
 *
 * **The single spelling of the code**: interpolated into both scripts and
 * read by both decoders. Never `1` — that is a pre-#348 release reply, pinned
 * as one that must throw — and never negative.
 *
 * Exported for the test suite only, so no test spells the literal; `mod.ts`
 * does not re-export it, and it is not part of the package's API.
 */
export const KEPT = 2

/**
 * The *refused* reply code (#355): the write was asked on another process's
 * behalf, and that process's liveness key exists — it is alive, so nothing
 * was written. Shared by both scripts, with the same meaning in each. Never
 * `1`, never negative; exported for the test suite only — see {@link KEPT}.
 */
export const REFUSED = 3

/**
 * Release one instance's hold on a roster slot — ONE operation, three
 * structures (#345). The leave AND the ghost sweep run it; there is no second
 * release path.
 *
 * **A sweep writes only while its target is dead, and that is decided HERE,
 * inside the write (#355, ADR 006).** With `ARGV[4] == '1'` — the release is on
 * another process's behalf — its first statements read the releaser's
 * liveness key, `KEYS[4]`, and answer {@link REFUSED} before any read or
 * write when it exists. A TypeScript `EXISTS` before the `EVAL` would race:
 * the instance can renew between the two. A leave passes `'0'` and is never
 * refused.
 *
 * It drops the releaser's holders entry and owned entry, then: if no holder is
 * left, deletes the presence field and returns **the releaser's own entry**
 * **only if the releaser held it** (the script's `mine == false` test answers
 * 0) — a non-holder emptying a legacy or already-emptied slot is not a
 * departure. If holders remain, the field stays; and when the shown entry was
 * the releaser's (`shown == mine`), one remaining holder's entry is copied in,
 * so the shown `info` never belongs to a departed holder. Releasing a holder
 * whose entry is not shown leaves the field unchanged. The reply is then
 * {@link KEPT} if the releaser held the slot, else 0 — tested AFTER the
 * promotion, so a non-holder's release still restores a missing shown field.
 *
 * **Whether the slot is gone, and which entry left, is decided HERE and
 * nowhere else (#348).** The entry IS the gone bit: a leave reads only that it
 * is present, and the ghost sweep hands it to the departure handler as the
 * member to announce. Reading it with an `HGET` before or after this `EVAL`
 * would race another instance's hold; and because the read and the delete of
 * the releaser's holders entry are one atomic step, of two sweeps of one dead
 * instance only the first gets the entry — exactly one announcement, with no
 * lock and no leader.
 *
 * **This is the direction that could delete somebody else's live member**, and
 * the only one that touches the presence field on a leave: no raw `HDEL` on the
 * presence hash exists anywhere else in this driver.
 *
 * `KEYS[1]` presence hash · `KEYS[2]` holders hash · `KEYS[3]` the releaser's
 * owned set · `KEYS[4]` the releaser's liveness key · `ARGV[1]` field ·
 * `ARGV[2]` releaser instance id · `ARGV[3]` owned entry · `ARGV[4]` `'1'`
 * to ask for the liveness check, `'0'` otherwise.
 *
 * **Presence and holders stay fail-closed here too, with no new guard
 * needed (#414).** `shown`, the presence `HGET`, already runs before the
 * first write below (`HDEL` on holders) — a wrong-typed presence key raises
 * `WRONGTYPE` on that read, aborting before anything commits, the same
 * property {@link HOLD_MEMBER_SCRIPT}'s explicit guard restores there for a
 * different ordering. `mine`, the holders `HGET`, runs even earlier. Neither
 * heals: an `RELEASE`-side `DEL` of either would erase a live member (ADR
 * 016).
 *
 * **The owned set self-heals (#414)**, `KEYS[3]` through {@link OWNED_HEAL},
 * bound to the local `owned` just before the unconditional `SREM` below — it
 * runs whenever the liveness gate above did not already refuse. The reply
 * widens to carry `ownedKind` alongside every existing outcome, decoded by
 * {@link decodeReleaseReply}, which WARNs once through
 * {@link RedisBroadcastDriver.#warnIfOwnedHealed} when it is healed.
 *
 * Four reply SHAPES since #414, each a `{value, ownedKind}` pair: the
 * released entry as a non-empty bulk string (**emptied**), {@link KEPT}
 * (**kept**), 0 (**absent** — the releaser held nothing there) or
 * {@link REFUSED} (**refused**). A refusal is answered by the liveness gate
 * before `owned` is ever read, so its kind is always `'none'` —
 * `{REFUSED, 'none'}`. Decoded by {@link decodeReleaseReply} and nowhere else.
 */
const RELEASE_MEMBER_SCRIPT: string = [
    "if ARGV[4] == '1' then",
    "  local alive = redis.call('EXISTS', KEYS[4])",
    '  if alive == 1 then',
    `    return {${REFUSED}, 'none'}`,
    '  end',
    'end',
    "local mine = redis.call('HGET', KEYS[2], ARGV[2])",
    "local shown = redis.call('HGET', KEYS[1], ARGV[1])",
    "redis.call('HDEL', KEYS[2], ARGV[2])",
    'local owned = KEYS[3]',
    OWNED_HEAL,
    "redis.call('SREM', owned, ARGV[3])",
    "local n = redis.call('HLEN', KEYS[2])",
    'if n == 0 then',
    "  redis.call('HDEL', KEYS[1], ARGV[1])",
    '  if mine == false then',
    '    return {0, ownedKind}',
    '  end',
    '  return {mine, ownedKind}',
    'end',
    'if shown == mine then',
    "  local promoted = redis.call('HRANDFIELD', KEYS[2], 1, 'WITHVALUES')",
    "  redis.call('HSET', KEYS[1], ARGV[1], promoted[2])",
    'end',
    'if mine == false then',
    '  return {0, ownedKind}',
    'end',
    `return {${KEPT}, ownedKind}`,
].join('\n')

/**
 * Deregister a swept instance — only while it is **dead and owns nothing**
 * (#355, audit A4, ADR 006). The ghost sweep's last write; there is no raw `SREM` of
 * the instances set anywhere in this driver.
 *
 * Its liveness key existing means the instance renewed while being swept (a
 * lapse, not a crash): the reply is {@link REFUSED} and it stays registered.
 * Its owned set existing means the sweep's scan left an entry behind — a late
 * hold that landed behind the scan's cursor (#358), or an entry the sweep
 * cannot parse: the reply is {@link KEPT}, it stays registered (the sweep ends
 * `kept`), and the next pass resumes it. This script, not the scan's control flow, is what keeps a
 * half-swept instance registered. Otherwise it leaves the instances set and
 * the reply is 0. `EXISTS`, not `SCARD`: Redis deletes an emptied set.
 *
 * Both checks and the write are one step, so neither can go stale between a
 * read and the `SREM` — which is what orphaned a late hold before (ADR 004
 * §5).
 *
 * **The instances set self-heals (#414)**, through {@link INSTANCES_HEAL}
 * bound to the local `instances`, spliced just before the gated `SREM` —
 * the only write this script makes, and the only one that ever touches the
 * instances set outside {@link HOLD_MEMBER_SCRIPT}. `EXISTS` on `KEYS[2]`
 * and `KEYS[3]` never heals and never needs to: `EXISTS` is never
 * type-sensitive, the same documented exemption the liveness key carries
 * everywhere else it is read this way (ADR 016). The reply widens to carry
 * `instancesKind` — `'none'` for the two branches that never touch the set —
 * decoded by {@link decodeDeregisterReply}, which WARNs once through
 * {@link RedisBroadcastDriver.#warnIfInstancesHealed} when it is healed.
 *
 * `KEYS[1]` instances set · `KEYS[2]` the instance's liveness key ·
 * `KEYS[3]` its owned set · `ARGV[1]` its instance id. Decoded by
 * {@link decodeDeregisterReply}.
 */
const DEREGISTER_INSTANCE_SCRIPT: string = [
    "local alive = redis.call('EXISTS', KEYS[2])",
    'if alive == 1 then',
    `  return {${REFUSED}, 'none'}`,
    'end',
    "local owns = redis.call('EXISTS', KEYS[3])",
    'if owns == 0 then',
    '  local instances = KEYS[1]',
    INSTANCES_HEAL,
    "  redis.call('SREM', instances, ARGV[1])",
    '  return {0, instancesKind}',
    'end',
    `return {${KEPT}, 'none'}`,
].join('\n')

/**
 * Read a bounded window of the roster, its size, and the callers' own entries —
 * ONE operation, ONE instant (#341).
 *
 * `HLEN`, `HRANDFIELD … WITHVALUES` and `HMGET` in one `EVAL`, so `total`, the
 * window and the selves cannot disagree under a concurrent join or leave. Split
 * into separate commands, a reply could count a member it no longer lists, or
 * list one it did not count.
 *
 * **Bounded by construction.** `HRANDFIELD` with a positive count returns at
 * most `count` DISTINCT pairs, and at or above the hash size the whole hash in
 * the hash's own iteration order — the order the previous whole-room read
 * returned — so a small room is unchanged and a large one costs `limit`
 * entries, whatever its size. A negative count would repeat pairs, which is one
 * reason the caller asserts `limit` before ever reaching this script.
 *
 * **`ARGV[2]` is always `''`**, and the self ids follow from `ARGV[3]`. `HMGET`
 * with no field is an arity error on a real broker, and a read with no self id
 * is legitimate (a superseded join reaches it holding no member, A1). No member
 * id can be `''` — ids are 1–200 characters (#306) — so the padding field never
 * matches and costs one nil. A Lua `if` would be a second code path; an empty
 * `HMGET` would be a production-only failure.
 *
 * **No loop, and no id in the script text.** Ids reach it through `ARGV` only.
 * The reply is parsed in TypeScript, where a self is accepted only when the
 * entry under its own field carries that same id (S3).
 *
 * **Fails closed, and never heals (#414).** Every command here reads the
 * presence hash; a wrong-typed key raises `WRONGTYPE` on the first one
 * (`HLEN`) and the whole read throws, never a silent empty roster. A
 * heal-`DEL` here would erase every member's shown entry with zero `left`
 * frames — ADR 016 is the standing rule this script and
 * {@link HOLD_MEMBER_SCRIPT}'s presence guard both follow.
 *
 * Returns `{ HLEN, [field, value, …], [value-or-nil, …] }`.
 *
 * `KEYS[1]` presence hash · `ARGV[1]` limit · `ARGV[2]` `''` ·
 * `ARGV[3…]` self ids.
 */
const READ_ROSTER_SCRIPT: string = [
    "local total = redis.call('HLEN', KEYS[1])",
    "local sample = redis.call('HRANDFIELD', KEYS[1], ARGV[1], 'WITHVALUES')",
    "local selves = redis.call('HMGET', KEYS[1], unpack(ARGV, 2))",
    'return {total, sample, selves}',
].join('\n')

/** A resource the driver owns and must release on {@link RedisBroadcastDriver.close}. */
interface Closeable {
    /** Release the resource (idempotent). */
    close(): void | Promise<void>
}

/**
 * The minimal command surface used for publishing and roster state.
 * `@lockness/redis`'s `RedisClient` satisfies it; a test passes a fake. Every
 * op is an ordinary serialized command — the serialized-command client handles
 * it. The reply is a `@lockness/redis` `RespReply` (`{ type, value }`), narrowed
 * here through the {@link asArray}/{@link asBulk}/{@link asInteger} guards.
 *
 * **Contract: one exchange in flight at a time, in call order** (#348 A1). A
 * command issued while another is outstanding is sent only once that one has
 * settled — `RedisClient` chains every command on one tail. The ghost sweep
 * depends on it: a hold of a slot, issued while the sweep's release of that
 * slot is outstanding, commits after the release, and the sweep reports the
 * departure before the hold's reply can announce the arrival — so the room
 * hears `left`, then `joined`. A pipelining client, or a second client for the
 * sweep, would let the two replies race.
 *
 * **Contract: every command settles** (#362) — resolves or rejects — within a
 * bound the port owns. `RedisClient` meets it through its read timeout
 * (`READ_TIMEOUT_MS` in `@lockness/redis`). Because commands are serialised, a
 * command that never settles stalls every command queued behind it: the
 * revocation re-check, the ghost sweep and the heartbeat. The driver does not
 * cancel a command; a revocation pass stalled this way is reported once by
 * the enforcement deadline, and is not recovered.
 *
 * **`close()` no longer stalls on it** (#368). It still waits for its
 * ghost-sweep pass, then its lapse run, then its owed-release drain (#371),
 * but bounded at one liveness TTL through {@link awaitCloseDrain} — a port
 * that violates this contract makes `close()` log one WARN and carry on with
 * its teardown, never hang. A command left stalled on such a port is still
 * never cancelled; it settles, if ever, on the port's own terms, after
 * `close()` has already returned.
 */
export interface RedisCommandClient {
    /**
     * Run a Redis command; args are sent as RESP bulk strings.
     *
     * @param args - The command and its arguments.
     * @returns The reply (a `RespReply`-shaped value, narrowed by the caller).
     */
    command(...args: string[]): Promise<unknown>
}

/**
 * A subscribe-mode connection that pushes messages for a topic pattern. A test
 * passes a fake bus; production supplies a real pub/sub connection
 * (`@lockness/redis`'s `RedisSubscribeConnection` — the serialized client cannot
 * subscribe).
 */
export interface RedisSubscriber {
    /**
     * Subscribe to a topic pattern and receive each published payload.
     *
     * @param pattern - The topic glob (e.g. `lockness:realtime:*`).
     * @param handler - Called with `(topic, payload)` for each message.
     */
    psubscribe(
        pattern: string,
        handler: (topic: string, payload: string) => void,
    ): void
    /**
     * OPTIONAL (#271/FR-004). Register a handler invoked after a fault-triggered
     * reconnect has re-issued every active subscription.
     *
     * Optional on the type so a subscriber that predates the seam — or a test
     * double that has no socket to lose — still satisfies this port. When it is
     * absent the driver falls back to its periodic revocation reconcile alone,
     * which is exactly #268's shipped behaviour.
     *
     * @param handler - Called with no arguments after each successful reconnect.
     */
    onReconnect?(handler: () => void | Promise<void>): void
    /**
     * OPTIONAL (#295). Subscribe to ONE pattern and resolve once its frame has
     * reached the socket.
     *
     * The awaitable counterpart to {@link psubscribe}, and what lets
     * {@link RedisBroadcastDriver.watchChannel} promise anything at all:
     * `psubscribe` is `void` by contract and returns in the same turn, so an
     * `async watchChannel` wrapping it would resolve having awaited nothing.
     *
     * The guarantee is the **write leg** — the frame reached the socket — never
     * that the broker answered, and never that delivery has started. A
     * rejection means the frame did not land; the connection is expected to
     * schedule its own retry regardless, so the caller keeps its membership.
     *
     * @param pattern - The topic glob or exact topic.
     * @param handler - Called with `(topic, payload)` for each message.
     * @returns Resolves once the frame is on the wire.
     */
    subscribeOne?(
        pattern: string,
        handler: (topic: string, payload: string) => void,
        options?: {
            /**
             * Put this pattern on the wire BEFORE any other on a re-issue, and
             * fire the reconnect seam once its write has landed.
             *
             * For the one subscription whose absence is a security fact rather
             * than a latency one — here, the control plane.
             */
            priority?: boolean
        },
    ): void | Promise<void>
    /**
     * OPTIONAL (#295). Stop receiving one pattern, on the wire **and** in
     * whatever set the connection re-issues after a reconnect.
     *
     * Both halves, or the fan-out win decays silently: a pattern unsubscribed
     * on the wire and left in the re-issue set comes back on the next fault,
     * and a fault is the worst moment to discover it.
     *
     * A rejection is expected to discard and reconnect the underlying socket
     * (#372) — the implementation owns that recovery, never the caller.
     *
     * @param pattern - The pattern to stop receiving.
     * @returns Resolves once the frame is on the wire.
     */
    unsubscribeOne?(pattern: string): void | Promise<void>
}

/**
 * A {@link RedisSubscriber} narrowed to one that can subscribe and unsubscribe
 * per pattern — obtained by testing both members together, never one at a time.
 */
export interface PerChannelSubscriber extends RedisSubscriber {
    /** Subscribe to one pattern, resolving once its frame is on the wire. */
    subscribeOne(
        pattern: string,
        handler: (topic: string, payload: string) => void,
        options?: { priority?: boolean },
    ): void | Promise<void>
    /** Stop receiving one pattern, on the wire and on reconnect. */
    unsubscribeOne(pattern: string): void | Promise<void>
}

/**
 * Narrow a subscriber to one that subscribes and unsubscribes per pattern.
 *
 * Exported so a test double can assert which path it will take rather than
 * inferring it from behaviour.
 *
 * @param subscriber - The subscriber to probe.
 * @returns The narrowed subscriber, or `undefined` when either member is absent.
 */
export function perChannelSubscriber(
    subscriber: RedisSubscriber,
): PerChannelSubscriber | undefined {
    return typeof subscriber.subscribeOne === 'function' &&
            typeof subscriber.unsubscribeOne === 'function'
        ? subscriber as PerChannelSubscriber
        : undefined
}

/**
 * Tuning for the instance-scoped ghost-member sweep (Q1/FR-008). One liveness
 * key per instance is refreshed on the heartbeat interval; a reconcile pass
 * sweeps the roster members of any instance whose liveness key has expired.
 */
export interface RedisPresenceOptions {
    /**
     * The instance liveness key TTL, in seconds. An instance that stops
     * heartbeating (crash) is considered dead once this elapses.
     * @default 15
     */
    livenessTtlSeconds?: number
    /**
     * How often (ms) this instance refreshes its own liveness key. Must be well
     * under `livenessTtlSeconds * 1000`: the constructor refuses an interval
     * above half of it (#293), and one above 2 147 483 647 ms, the longest
     * delay a single timer can hold (#381) — a longer one fires after 1 ms.
     * @default 5000
     */
    heartbeatIntervalMs?: number
    /**
     * How often (ms) this instance reconciles the roster, sweeping the members
     * of any dead instance.
     * @default 10000
     */
    reconcileIntervalMs?: number
}

/**
 * Redis glob metacharacters. A prefix carrying one of these is refused (#282).
 *
 * The prefix reaches `PSUBSCRIBE` at two sites — the event pattern and the
 * control topic — and both are **pattern** contexts, not literal ones. A `*` in
 * the prefix therefore widens the subscription to traffic the deployment does
 * not own, and it does so while remaining trivially "anchored" under any
 * `startsWith` check, so a containment test alone will not catch it.
 *
 * `packages/redis/tests/live_broker.ts:157-172` already applies this discipline
 * to the test harness's own namespace, with the reasoning written out. The
 * driver did not apply it to the operator's prefix until now.
 *
 * **All five, and the fifth is the nastiest.** A first version listed four and
 * omitted `\\`. A prefix of `app\\` yields `PSUBSCRIBE app\\:*`, which Redis
 * reads as the literal `app:*` — so that deployment subscribes to another one's
 * entire event stream **while its own traffic stays invisible to that
 * deployment**, i.e. the asymmetry hides it from whoever would notice. It also
 * corrupts the #273 reaper's `SCAN MATCH app\\*` into a literal, so its keys are
 * never reaped. (That reaper lives in the test harness —
 * `tests/live_realtime.ts` — not in shipped code; this file runs no `SCAN`.)
 */
const PREFIX_GLOB_CHARS: readonly string[] = ['*', '?', '[', ']', '\\']

/**
 * The two-character sequence a prefix may never contain, and which every
 * reserved separator must begin with (#288).
 *
 * **This constant is the isolation guarantee**, and the rule is one decision
 * with two halves that must be read together:
 *
 * 1. `assertUsablePrefix` refuses any prefix containing `__`.
 * 2. **Every reserved separator this driver introduces MUST begin with it** —
 *    `__event:`, `__control`, `__presence:` and the rest all do.
 *
 * Together they make cross-prefix reach impossible rather than filtered. The
 * proof is positional and never mentions the channel charset. For accepted
 * prefixes `P ≠ Q` and any channel `C`, `Q__event:*` cannot match `P__event:C`:
 *
 * - `|Q| ≥ |P| + 2` — then `Q` spans the topic's own `__`, so `Q` contains the
 *   refused sequence and was never accepted.
 * - `|Q| = |P| + 1` — then `Q = P + "_"`, and the pattern's literal part reads
 *   `P___event:` against a topic reading `P__event:C`. They diverge at offset
 *   `|P| + 2`, `_` against `e`.
 * - `|Q| = |P|` with `P ≠ Q` — they diverge inside the prefix.
 *
 * The same argument covers event-pattern-against-control-topic, and every pair
 * of key families, which is why FR-012 could anchor the keys as well. It is
 * also why the channel needs no part in it: the channel sits entirely to the
 * right of every pattern's literal part, so even a wholly unvalidated channel
 * cannot cross into another accepted prefix.
 *
 * **A separator that does not begin with this sequence breaks the proof and
 * passes every test in the suite.** `#presence:` would look reasonable and
 * would silently reopen #288 one level down. That is the whole reason this is
 * a named constant with the proof attached rather than a literal at each site.
 */
const RESERVED_SEPARATOR_LEAD = '__'

/**
 * The charset a prefix may use — a positive allowlist, not a denylist.
 *
 * Deliberately the same alphabet as `isValidName`'s `NAME_RE`, so the isolation
 * proof holds over ONE charset rather than two.
 *
 * The prefix defines the isolation boundary and reaches `PSUBSCRIBE` at two
 * pattern contexts, yet until now it was bounded only by
 * {@link PREFIX_GLOB_CHARS} — a five-item denylist — while the *less* trusted
 * channel had an allowlist and a length cap. Nothing exploitable followed from
 * that (UTF-8 is self-synchronising, so no multi-byte sequence smuggles one of
 * those five bytes past an `includes` check), and having to reason that out is
 * exactly what an allowlist removes. It was one line to add before any operator
 * had a prefix in production config, and a breaking configuration change with
 * no migration afterwards.
 */
const PREFIX_RE = /^[A-Za-z0-9:._-]{1,64}$/

/**
 * Refuse a prefix that would widen a subscription or break the isolation proof.
 *
 * Five checks, in order of what they protect: the emptiness check, the glob
 * scan that names the specific character when one gets through, the separator
 * check that protects {@link RESERVED_SEPARATOR_LEAD}'s guarantee, the
 * trailing-underscore check that protects the ACL boundary that separator
 * implies, and the allowlist that bounds everything else.
 *
 * @param prefix - The configured prefix.
 * @throws {Error} If it is empty, contains a Redis glob metacharacter, contains
 *   the reserved separator lead-in `__`, ends with `_`, or is outside
 *   {@link PREFIX_RE} (charset or the 64-character cap).
 */
function assertUsablePrefix(prefix: string): void {
    if (prefix.length === 0) {
        throw new Error(
            'RedisBroadcastDriver: prefix must not be empty — every key and ' +
                'topic is derived from it',
        )
    }
    // BEFORE the allowlist, deliberately. Every one of these five characters
    // is already outside PREFIX_RE, so running the allowlist first would make
    // this loop unreachable — a guard that cannot execute is not defence in
    // depth, it is dead code that reads as protection. Ordered most-specific
    // first, it stays live and it is the only check that names WHICH character
    // is at fault, which is the message that made the `\\` case diagnosable.
    for (const char of PREFIX_GLOB_CHARS) {
        if (prefix.includes(char)) {
            throw new Error(
                `RedisBroadcastDriver: prefix must not contain the Redis glob ` +
                    `character "${char}" — it is interpolated into PSUBSCRIBE ` +
                    `patterns, where it would widen the subscription to traffic ` +
                    `this deployment does not own`,
            )
        }
    }
    if (prefix.includes(RESERVED_SEPARATOR_LEAD)) {
        throw new Error(
            `RedisBroadcastDriver: prefix must not contain ` +
                `"${RESERVED_SEPARATOR_LEAD}" — it is the lead-in every ` +
                'reserved separator begins with, and a prefix carrying it can ' +
                "reach another deployment's topics and keys (#288)",
        )
    }
    // A TRAILING underscore, which #288 left open and #278 closes.
    //
    // `app` and `app_` are both accepted, and they derive different keys — no
    // collision. What they do NOT get is ACL isolation: the recommended grant
    // for `app` is `~app__*`, and `app_`'s own names begin `app___`, which that
    // glob matches. So the `app` credential reaches every key of the `app_`
    // deployment while its own traffic looks perfectly ordinary.
    //
    // `__` is already refused above, so the only shape that can do this is a
    // prefix ending in exactly one `_`. Refusing it makes the containment
    // argument exact rather than conditional: for any two accepted prefixes,
    // neither `${a}__` nor `${b}__` is a prefix of the other's derived names.
    if (prefix.endsWith('_')) {
        throw new Error(
            'RedisBroadcastDriver: prefix must not end with "_" — the ' +
                `recommended ACL grant "~${prefix.slice(0, -1)}__*" would ` +
                `match this deployment's own keys, so the two would share a ` +
                'credential boundary without sharing a prefix (#278)',
        )
    }
    // Last: the catch-all. The four checks above each name a specific,
    // actionable fault; this one bounds everything else — spaces, control
    // characters, bidi marks, an unbounded length.
    if (!PREFIX_RE.test(prefix)) {
        throw new Error(
            'RedisBroadcastDriver: prefix must match ' +
                `${PREFIX_RE.source} — the same charset channel names use, ` +
                'and at most 64 characters. Every key and topic is derived ' +
                `from it. Got ${prefix.length} character(s).`,
        )
    }
}

/** Options for the Redis broadcast driver. */
export interface RedisBroadcastDriverOptions {
    /**
     * Reserved name prefix for every key and topic this driver derives.
     *
     * **This docstring is the single home for what the prefix guarantees**
     * (#288). Every other statement of it — this file's header,
     * `docs/realtime.md`, the package README and AGENTS.md — points here.
     *
     * The prefix bounds this driver's **outbound routing**: no deployment
     * receives another deployment's frames. It is **not** an inbound boundary —
     * any client on the broker can publish into, and read from, these topics
     * and keys. Use Redis ACLs for that.
     *
     * Outbound isolation is structural, not conventional. Every derived name
     * sits behind {@link RESERVED_SEPARATOR_LEAD}, which no accepted prefix may
     * contain, so no pattern one deployment subscribes can match any topic or
     * key another derives — nested prefixes included. Until #288 the event
     * topic used a plain `:` separator and a deployment at `app` received the
     * events of one at `app:eu`. Two legacy revocation key names were the one
     * documented exception; **#278 removed them, so there is no exception
     * left** — the anchoring check has no exemption list to add a name to.
     *
     * It said "multi-tenant isolation" until #282, which was wrong in both
     * directions and is the wording that led operators to nest prefixes in the
     * first place. The sentence above is deliberately narrower than that:
     * outbound only, and named as such.
     *
     * **Five refusals at construction, and this is the full statement** — the
     * one every other mention points at:
     *
     * | Refused | Why |
     * | :--- | :--- |
     * | empty | every key and topic is derived from it |
     * | a Redis glob metacharacter (`*` `?` `[` `]` `\`) | it is interpolated into `PSUBSCRIBE` patterns, where it widens the subscription to traffic the deployment does not own. `app\` is the worst: Redis reads `app\:*` as the literal `app:*`, so that deployment reads another's whole stream while its own traffic stays invisible to the deployment it is reading |
     * | containing `__` | it is the lead-in every reserved separator begins with, so a prefix carrying it reaches another deployment's names (#288) |
     * | ending in `_` | the ACL grant documented for `app` is `~app__*`, and every name `app_` derives begins `app___`, which that glob matches. The two collide on nothing and cross-subscribe to nothing, and one credential still reads the other's whole keyspace (#278) |
     * | outside `[A-Za-z0-9:._-]{1,64}` | the catch-all: spaces, control characters, bidi marks, unbounded length |
     *
     * The last three each protect a different property, and only the middle one
     * is about what this driver itself subscribes.
     *
     * @default "lockness:realtime"
     */
    prefix?: string
    /**
     * The FR-015 control-plane authenticity secret. Required for the control /
     * presence-identity path (`onControl` / `publishControl`): without it, a
     * control message can neither be signed on publish nor verified on ingest,
     * so both are refused with a WARN.
     */
    control?: RealtimeControlConfig
    /** Ghost-member sweep tuning (Q1/FR-008). */
    presence?: RedisPresenceOptions
    /**
     * The TTL (seconds) of a durable revocation marker (FR-014). A marker
     * lingers **at least** this long so a socket that reconnects within the
     * window is still revoked; after it, the marker self-expires so the set
     * never grows without bound. Since #380 a marker lives up to the longest
     * TTL among the instances still passing — see "Revocation timing" in
     * `docs/realtime.md`.
     * @default 300
     */
    revocationTtlSeconds?: number
}

/**
 * A fresh control-frame nonce: 16 CSPRNG bytes, hex-encoded to a fixed width.
 *
 * A counter would be cheaper and is the wrong choice twice over: it collides
 * across senders (two instances both start at 1), and it collides with itself
 * after a restart (back to 1, inside a live window). Unpredictability is not
 * what the anti-replay property requires — an attacker cannot forge a MAC over
 * a nonce of their choosing — it is simply how uniqueness is obtained across
 * processes without coordination.
 *
 * @returns A 32-character lowercase hex string.
 */
function newControlNonce(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(16))
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

/**
 * Whether a control frame's `member` is exactly a presence member: `id` and
 * `info` and nothing else (#350).
 *
 * `member` was the one field the ingest shape gate never checked, and it is the
 * one an attacker can make arbitrarily large — which matters because everything
 * downstream of the gate re-serialises it and hashes it synchronously
 * (FR-011). `undefined` is valid: an `evict` frame carries no member. What a
 * member must be is {@link isPresenceMemberWire}'s rule, shared with the
 * join's admission and the manager's departure handler (#348, #350) — never
 * a copy here. Before #350 it bounded the key COUNT, so a signed
 * `{ id, smuggled }` was admitted and re-emitted to every subscriber.
 *
 * @param value - The candidate, straight off the wire.
 * @returns Whether it is safe to canonicalise.
 */
function isPlainMember(value: unknown): boolean {
    return value === undefined || isPresenceMemberWire(value)
}

/** Narrow an unknown `RespReply` to its array elements, or `undefined`. */
function asArray(reply: unknown): readonly unknown[] | undefined {
    return typeof reply === 'object' && reply !== null &&
            (reply as { type?: unknown }).type === 'array'
        ? (reply as { value: readonly unknown[] }).value
        : undefined
}

/** Narrow an unknown `RespReply` to its bulk-string value, or `undefined`. */
function asBulk(reply: unknown): string | undefined {
    return typeof reply === 'object' && reply !== null &&
            (reply as { type?: unknown }).type === 'bulk'
        ? (reply as { value: string }).value
        : undefined
}

/** Narrow an unknown `RespReply` to its integer value, or `undefined`. */
function asInteger(reply: unknown): number | undefined {
    return typeof reply === 'object' && reply !== null &&
            (reply as { type?: unknown }).type === 'integer'
        ? (reply as { value: number }).value
        : undefined
}

/**
 * The one message {@link decodeHoldReply} throws (#345 FR-004a, widened
 * #414). It names the shape it expected and never carries the reply.
 * Exported for the test suite only.
 */
export const HOLD_REPLY_REFUSED =
    'realtime: the hold script answered something other than 0 or 1 for ' +
    'arrived, or a {arrived, ownedKind, instancesKind} triple with a ' +
    'missing kind'

/**
 * The hold's decoded reply (#345, widened #414): whether the hold filled an
 * empty slot, and the Redis type {@link OWNED_HEAL} / {@link INSTANCES_HEAL}
 * each found at their key, before either healed anything.
 */
export interface HoldReply {
    /** Whether no instance held the slot before this hold. */
    readonly arrived: boolean
    /** The owned set's prior Redis type — `'set'`/`'none'` iff no heal ran. */
    readonly ownedKind: string
    /** The instances set's prior Redis type — `'set'`/`'none'` iff no heal ran. */
    readonly instancesKind: string
}

/**
 * Decode a {@link HOLD_MEMBER_SCRIPT} reply (#345 FR-004a, widened #414): a
 * three-element array whose first element is the integer 1 (`arrived: true`)
 * or 0 (`arrived: false`), and whose second and third are bulk strings
 * naming the owned set's and the instances set's prior Redis type.
 *
 * Truthiness would read an error string or an unexpected array as an arrival —
 * and the manager announces a `joined` from this bit.
 *
 * @param reply - The `EVAL` reply.
 * @returns Whether the hold filled an empty slot, and each key's prior kind.
 * @throws {Error} {@link HOLD_REPLY_REFUSED}, for any other reply.
 */
function decodeHoldReply(reply: unknown): HoldReply {
    const items = asArray(reply)
    const arrived = asInteger(items?.[0])
    const ownedKind = items ? asBulk(items[1]) : undefined
    const instancesKind = items ? asBulk(items[2]) : undefined
    if (
        arrived === 1 && ownedKind !== undefined && instancesKind !== undefined
    ) {
        return { arrived: true, ownedKind, instancesKind }
    }
    if (
        arrived === 0 && ownedKind !== undefined && instancesKind !== undefined
    ) {
        return { arrived: false, ownedKind, instancesKind }
    }
    throw new Error(HOLD_REPLY_REFUSED)
}

/**
 * What one {@link RELEASE_MEMBER_SCRIPT} run did (#355, widened #414).
 * Internal: a leave's public answer is still only `gone`
 * ({@link RosterRelease}).
 *
 * - `emptied` — the releaser held the slot and was its last holder; `entry`
 *   is its entry, the member that left; `ownedKind` is the owned set's prior
 *   Redis type.
 * - `kept` — the releaser held the slot; other holders keep it; `ownedKind`
 *   as above.
 * - `absent` — the releaser held nothing there (already released);
 *   `ownedKind` as above.
 * - `refused` — asked on another process's behalf while that process is
 *   alive; nothing was written, {@link OWNED_HEAL} never ran, and there is no
 *   `ownedKind` to report.
 *
 * `ownedKind` is optional on the three heal-carrying variants rather than
 * required: the decoder always supplies it from a genuine script reply, but
 * a test double that builds one of these literals directly (a driver
 * fixture standing in for the sweep's own decode) is not obliged to.
 */
type ReleaseOutcome =
    | {
        readonly kind: 'emptied'
        readonly entry: string
        readonly ownedKind?: string
    }
    | { readonly kind: 'kept'; readonly ownedKind?: string }
    | { readonly kind: 'absent'; readonly ownedKind?: string }
    | { readonly kind: 'refused' }

/**
 * The one message {@link decodeReleaseReply} throws (#348 FR-004a, #355,
 * widened #414). It names what is accepted and never the reply, its type or
 * its length — the reply's bytes come from the broker and the message
 * reaches a log line (S4). Exported for the test suite only.
 */
export const RELEASE_REPLY_REFUSED =
    'realtime: the release script answered none of its four replies — a ' +
    `{value, ownedKind} pair whose value is a released entry, 0 (absent) or ` +
    `${KEPT} (kept), or ${REFUSED} paired with 'none' (refused)`

/**
 * Decode a {@link RELEASE_MEMBER_SCRIPT} reply into its {@link ReleaseOutcome}
 * (#348 FR-004a, #355, widened #414): a two-element array whose first
 * element is a non-empty bulk string (**emptied**, the released holder's
 * entry), {@link KEPT} (**kept**) or 0 (**absent**) — each paired with the
 * owned set's prior Redis type as its second element — or {@link REFUSED}
 * ALONE (**refused**: the liveness gate answered before `owned` was ever
 * read, so there is no kind to pair it with). Anything else throws.
 *
 * **The single home of what a release reply means.** An integer 1 is a
 * pre-#348 script, a nil or an array of the wrong length is not this script
 * at all, and an empty bulk is an entry no hold ever writes. Truthiness
 * would read any of them as a departure, and a leave announces its `left` —
 * and a sweep announces the entry itself — from this value.
 *
 * @param reply - The `EVAL` reply.
 * @returns What the release did.
 * @throws {Error} {@link RELEASE_REPLY_REFUSED}, for any other reply.
 */
function decodeReleaseReply(reply: unknown): ReleaseOutcome {
    const pair = asArray(reply)
    if (pair === undefined || pair.length !== 2) {
        throw new Error(RELEASE_REPLY_REFUSED)
    }
    const code = asInteger(pair[0])
    if (code === REFUSED) return { kind: 'refused' }
    const ownedKind = asBulk(pair[1])
    if (ownedKind === undefined) throw new Error(RELEASE_REPLY_REFUSED)
    if (code === 0) return { kind: 'absent', ownedKind }
    if (code === KEPT) return { kind: 'kept', ownedKind }
    const entry = asBulk(pair[0])
    if (entry) return { kind: 'emptied', entry, ownedKind }
    throw new Error(RELEASE_REPLY_REFUSED)
}

/** What one {@link DEREGISTER_INSTANCE_SCRIPT} run did (#355). Internal. */
type DeregisterOutcome = 'deregistered' | 'renewed' | 'kept'

/**
 * The deregistration's decoded reply (#355, widened #414): what the
 * deregistration did, and the Redis type {@link INSTANCES_HEAL} found at the
 * instances set — `'none'` for `renewed`/`kept`, which never touch it.
 */
interface DeregisterReply {
    /** What the deregistration did. */
    readonly outcome: DeregisterOutcome
    /** The instances set's prior Redis type — `'set'`/`'none'` iff no heal ran. */
    readonly instancesKind: string
}

/**
 * The one message {@link decodeDeregisterReply} throws (#355, widened
 * #414). Its wording is constant, like {@link decodeReleaseReply}'s: it
 * never carries the reply. Exported for the test suite only.
 */
export const DEREGISTER_REPLY_REFUSED =
    'realtime: the deregistration script answered none of its three ' +
    `replies — a {code, instancesKind} pair whose code is 0 ` +
    `(deregistered), ${REFUSED} (renewed) or ${KEPT} (kept)`

/**
 * Decode a {@link DEREGISTER_INSTANCE_SCRIPT} reply (#355, widened #414): a
 * two-element array whose first element is 0 (**deregistered**),
 * {@link REFUSED} (**renewed** — the instance is alive again) or
 * {@link KEPT} (**kept** — it owns a late hold, left for the next pass), and
 * whose second is a bulk string naming the instances set's prior Redis type.
 * Anything else throws.
 *
 * **The single home of what a deregistration reply means.**
 *
 * @param reply - The `EVAL` reply.
 * @returns What the deregistration did, and the instances set's prior kind.
 * @throws {Error} {@link DEREGISTER_REPLY_REFUSED}, for any other reply.
 */
function decodeDeregisterReply(reply: unknown): DeregisterReply {
    const pair = asArray(reply)
    if (pair === undefined || pair.length !== 2) {
        throw new Error(DEREGISTER_REPLY_REFUSED)
    }
    const code = asInteger(pair[0])
    const instancesKind = asBulk(pair[1])
    if (instancesKind === undefined) throw new Error(DEREGISTER_REPLY_REFUSED)
    if (code === 0) return { outcome: 'deregistered', instancesKind }
    if (code === REFUSED) return { outcome: 'renewed', instancesKind }
    if (code === KEPT) return { outcome: 'kept', instancesKind }
    throw new Error(DEREGISTER_REPLY_REFUSED)
}

/**
 * What the revocation re-check handler resolved to, decoded (#384) — **the
 * one place that decision is made**, and it never throws. Exactly one of:
 *
 * - `undefined` — **no tally**: `undefined`, or any value that is not
 *   tally-shaped (not a non-null object carrying an `attempted` or a `failed`
 *   property). Today's behaviour, and silent: a handler that compiles as
 *   `() => void` can still resolve a stray value, and that is not a breach;
 * - `'malformed'` — a tally-shaped value whose counts are bad: either one
 *   missing, not a safe integer, negative, or `failed` above `attempted`, or
 *   a read that threw (a getter, a proxy trap);
 * - a {@link RevocationTally} — a fresh frozen copy of the two counts.
 *
 * Every read sits inside this function's own `try`, so a hostile value can
 * neither fail the pass nor reach its caller's catch.
 *
 * @param value - What the handler resolved to.
 * @returns The tally, `'malformed'`, or `undefined` for no tally.
 * @example
 * ```ts
 * decodeRevocationTally({ attempted: 4, failed: 1 }) // { attempted: 4, failed: 1 }
 * decodeRevocationTally({ attempted: 1, failed: 2 }) // 'malformed'
 * decodeRevocationTally('x') // undefined
 * ```
 */
function decodeRevocationTally(
    value: unknown,
): RevocationTally | 'malformed' | undefined {
    if (typeof value !== 'object' || value === null) return undefined
    try {
        if (!('attempted' in value) && !('failed' in value)) return undefined
        const { attempted, failed } = value as {
            attempted?: unknown
            failed?: unknown
        }
        if (!Number.isSafeInteger(attempted)) return 'malformed'
        if (!Number.isSafeInteger(failed)) return 'malformed'
        const counts = {
            attempted: attempted as number,
            failed: failed as number,
        }
        if (counts.failed < 0 || counts.failed > counts.attempted) {
            return 'malformed'
        }
        return Object.freeze(counts)
    } catch {
        // Not silent: `'malformed'` is what the caller WARNs on, once per pass.
        return 'malformed'
    }
}

/**
 * A canonical SCAN-family cursor (#358 S1): `0`, or at most 20 decimal digits
 * with no leading zero. Kept as a string — a cursor is opaque and may exceed
 * 2^53.
 */
const SCAN_CURSOR = /^(0|[1-9][0-9]{0,19})$/

/**
 * The one message {@link decodeScanReply} throws (#358 FR-003, S2). It
 * describes the shape it expected and names no command and no key; it never
 * carries the reply, its type or its length — those bytes come from the broker
 * and the message reaches a log line. Exported for the test suite only.
 */
export const SCAN_REPLY_REFUSED =
    'realtime: a scan reply is not [cursor, array] — a canonical decimal ' +
    'cursor of at most 20 digits followed by an array of items'

/** One decoded page of a SCAN-family reply (#358). Internal. */
export interface ScanPage {
    /** The next cursor: `'0'` once the iteration is complete. */
    readonly cursor: string
    /** The page's items, still raw replies, each parsed by its caller. */
    readonly items: readonly unknown[]
}

/**
 * Decode a SCAN-family reply envelope (#358 FR-003, A4): a two-element array
 * of a canonical cursor (a bulk string, `0` or up to 20 digits without a
 * leading zero) and an array of items. Anything else throws
 * {@link SCAN_REPLY_REFUSED}.
 *
 * **The single home of what a SCAN-family reply envelope means**, shaped
 * command-neutral: the owned-set `SSCAN` is its first caller, and a later
 * paged read reuses it rather than decoding its own. **Strict on purpose**: a
 * missing cursor read as `undefined` never equals `'0'`, so the pass would
 * never end; defaulted to `'0'`, it would end after one page. The cursor is
 * never parsed as a number.
 *
 * Exported for the test suite only (like {@link decodeBeatReply}); `mod.ts`
 * does not re-export it.
 *
 * @param reply - The `SSCAN` (or other SCAN-family) reply.
 * @returns The next cursor and the page's raw items.
 * @throws {Error} {@link SCAN_REPLY_REFUSED}, for any other shape.
 * @example
 * ```ts
 * decodeScanReply({
 *     type: 'array',
 *     value: [{ type: 'bulk', value: '0' }, { type: 'array', value: [] }],
 * }) // { cursor: '0', items: [] }
 * ```
 */
export function decodeScanReply(reply: unknown): ScanPage {
    const parts = asArray(reply)
    if (parts?.length === 2) {
        const cursor = asBulk(parts[0])
        const items = asArray(parts[1])
        if (
            typeof cursor === 'string' && SCAN_CURSOR.test(cursor) &&
            items !== undefined
        ) {
            return { cursor, items }
        }
    }
    throw new Error(SCAN_REPLY_REFUSED)
}

/**
 * The one message {@link decodeMembersReply} throws (#360 S2). It names the
 * shape it expected and never carries the reply. Exported for the test suite
 * only.
 */
export const MEMBERS_REPLY_REFUSED =
    'realtime: the instance-set read did not answer an array of members'

/**
 * Decode the ghost sweep's `SMEMBERS <instances key>` reply (#360 S2): an
 * array of raw members, each parsed by the caller. Anything else throws
 * {@link MEMBERS_REPLY_REFUSED}.
 *
 * **Strict on purpose**: read leniently, a reply that is not an array was an
 * empty instance set, so the sweep silently swept nothing, pass after pass.
 * A throw fails the pass instead, with one WARN.
 *
 * Exported for the test suite only; `mod.ts` does not re-export it.
 *
 * @param reply - The `SMEMBERS` reply.
 * @returns The raw members.
 * @throws {Error} {@link MEMBERS_REPLY_REFUSED}, for any other shape.
 * @example
 * ```ts
 * decodeMembersReply({ type: 'array', value: [{ type: 'bulk', value: 'a' }] })
 * // [{ type: 'bulk', value: 'a' }]
 * ```
 */
export function decodeMembersReply(reply: unknown): readonly unknown[] {
    const members = asArray(reply)
    if (members !== undefined) return members
    throw new Error(MEMBERS_REPLY_REFUSED)
}

/**
 * The one message {@link decodeExistsReply} throws (#360 S2). It names the
 * shape it expected and never carries the reply. Exported for the test suite
 * only.
 */
export const EXISTS_REPLY_REFUSED =
    'realtime: a liveness probe did not answer the integer 0 or 1'

/**
 * Decode a liveness probe's `EXISTS <alive key>` reply (#360 S2): the integer
 * `0` (the instance is dead) or `1` (it is alive). Anything else throws
 * {@link EXISTS_REPLY_REFUSED}.
 *
 * **Strict on purpose**: read leniently, a reply that is not an integer was
 * "alive", so a dead instance was never swept. A throw fails the pass
 * instead, with one WARN.
 *
 * Exported for the test suite only; `mod.ts` does not re-export it.
 *
 * @param reply - The `EXISTS` reply.
 * @returns `0` or `1`.
 * @throws {Error} {@link EXISTS_REPLY_REFUSED}, for any other reply.
 * @example
 * ```ts
 * decodeExistsReply({ type: 'integer', value: 0 }) // 0
 * ```
 */
export function decodeExistsReply(reply: unknown): 0 | 1 {
    const alive = asInteger(reply)
    if (alive === 0 || alive === 1) return alive
    throw new Error(EXISTS_REPLY_REFUSED)
}

/**
 * What epoch seconds look like on the wire (#359 FR-002, A12) — the ONE
 * grammar for both the reap's `t` and every revocation score: `0`, or at most
 * 15 decimal digits with no leading zero, so `Number` reads it exactly. Not
 * `Number.isFinite(Number(s))`, which accepts `1e9`, `1.5` and `' 1'`.
 * Exported for the test suite only.
 */
export const EPOCH_SECONDS = /^(0|[1-9][0-9]{0,14})$/

/**
 * The one message {@link decodeReapReply} throws (#359, widened #405, widened
 * again #411). It names the shape it expected and never carries the reply,
 * its type or its length. Exported for the test suite only.
 */
export const REAP_REPLY_REFUSED =
    'realtime: the revocation reap did not answer a {t, indexKind, floorKind} ' +
    'triple — t a bulk string of at most 15 decimal digits with no leading ' +
    'zero, indexKind and floorKind bulk strings naming the index and floor ' +
    "keys' Redis types"

/**
 * The reap's decoded reply (#405, widened #411): the pass's one `now`, the
 * Redis type {@link INDEX_HEAL} found at the index key, and the Redis type
 * {@link FLOOR_WRITE} found at the floor key — each read before it healed
 * anything.
 */
export interface ReapReply {
    /** The Redis second the reap used. */
    readonly t: number
    /** The index key's prior Redis type — `'zset'`/`'none'` iff no heal ran. */
    readonly indexKind: string
    /** The floor key's prior Redis type — `'zset'`/`'none'` iff no heal ran. */
    readonly floorKind: string
}

/**
 * Decode the reply of {@link REAP_REVOKED_SCRIPT} (#359 FR-002, widened by
 * #405, widened again by #411): a three-element array whose first element is
 * a bulk string matching {@link EPOCH_SECONDS}, read as the pass's one `now`,
 * and whose second and third are bulk strings naming the index key's and the
 * floor key's prior Redis type, respectively.
 *
 * **The single home of what a reap reply means.** Anything else — an array of
 * the wrong length, an integer reply for any element, a nil, or a
 * non-canonical or over-long `t` — throws {@link REAP_REPLY_REFUSED}, so a
 * pass never judges liveness against a `now` it guessed (`?? 0` would read
 * every record as live, forever). Exported for the test suite only; `mod.ts`
 * does not re-export it.
 *
 * @param reply - The reap's `EVAL` reply.
 * @returns The Redis second the reap used, and each key's prior kind.
 * @throws {Error} {@link REAP_REPLY_REFUSED}, for any other reply.
 * @example
 * ```ts
 * decodeReapReply({
 *     type: 'array',
 *     value: [
 *         { type: 'bulk', value: '1790157600' },
 *         { type: 'bulk', value: 'zset' },
 *         { type: 'bulk', value: 'zset' },
 *     ],
 * }) // { t: 1790157600, indexKind: 'zset', floorKind: 'zset' }
 * ```
 */
export function decodeReapReply(reply: unknown): ReapReply {
    const items = asArray(reply)
    if (items === undefined || items.length !== 3) {
        throw new Error(REAP_REPLY_REFUSED)
    }
    const t = asBulk(items[0])
    const indexKind = asBulk(items[1])
    const floorKind = asBulk(items[2])
    if (
        t === undefined || !EPOCH_SECONDS.test(t) ||
        indexKind === undefined || floorKind === undefined
    ) {
        throw new Error(REAP_REPLY_REFUSED)
    }
    return { t: Number(t), indexKind, floorKind }
}

/**
 * The one message {@link decodeMarkReply} throws (#411). It names the shape
 * it expected and never carries the reply, its type or its length. Exported
 * for the test suite only.
 */
export const MARK_REPLY_REFUSED =
    'realtime: the revocation mark did not answer a {indexKind} singleton — ' +
    "a bulk string naming the index key's Redis type"

/**
 * The mark's decoded reply (#411): the Redis type {@link INDEX_HEAL} found at
 * the index key before it healed anything.
 */
export interface MarkReply {
    /** The index key's prior Redis type — `'zset'`/`'none'` iff no heal ran. */
    readonly kind: string
}

/**
 * Decode the reply of {@link MARK_REVOKED_SCRIPT} (#411): a one-element array
 * whose element is a bulk string naming the index key's prior Redis type.
 *
 * **The single home of what a mark reply means.** Anything else — an array of
 * the wrong length, an integer reply, or a nil — throws
 * {@link MARK_REPLY_REFUSED}, the same discipline {@link REAP_REPLY_REFUSED}
 * keeps. Exported for the test suite only; `mod.ts` does not re-export it.
 *
 * @param reply - The mark's `EVAL` reply.
 * @returns The index key's prior kind.
 * @throws {Error} {@link MARK_REPLY_REFUSED}, for any other reply.
 * @example
 * ```ts
 * decodeMarkReply({
 *     type: 'array',
 *     value: [{ type: 'bulk', value: 'zset' }],
 * }) // { kind: 'zset' }
 * ```
 */
export function decodeMarkReply(reply: unknown): MarkReply {
    const items = asArray(reply)
    if (items === undefined || items.length !== 1) {
        throw new Error(MARK_REPLY_REFUSED)
    }
    const kind = asBulk(items[0])
    if (kind === undefined) throw new Error(MARK_REPLY_REFUSED)
    return { kind }
}

/**
 * The one message {@link decodeAnnounceReply} throws (#411 folded LOW). It
 * names the shape it expected and never carries the reply, its type or its
 * length. Exported for the test suite only.
 */
export const ANNOUNCE_REPLY_REFUSED =
    'realtime: the floor announce did not answer a bulk string naming the ' +
    "floor key's Redis type"

/**
 * Decode the reply of {@link ANNOUNCE_FLOOR_SCRIPT} (#405, made strict by
 * #411): a bulk string naming the floor key's prior Redis type.
 *
 * **Strict on purpose, mirroring {@link decodeReapReply}.** The bare
 * `asBulk(reply)` this replaced read a non-bulk reply as `undefined` and
 * silently skipped the heal check — no throw, no WARN, and
 * {@link RedisBroadcastDriver.#announceFloor} could not tell a healthy
 * announce from a decode it could not trust. Throwing instead lands the
 * failure in `#announceFloor`'s own `catch`, which already WARNs and retries
 * for a rejected command — a decode failure now gets the same treatment
 * rather than a silent no-op.
 *
 * @param reply - The announce's `EVAL` reply.
 * @returns The floor key's prior kind.
 * @throws {Error} {@link ANNOUNCE_REPLY_REFUSED}, for any other reply.
 * @example
 * ```ts
 * decodeAnnounceReply({ type: 'bulk', value: 'zset' }) // 'zset'
 * ```
 */
export function decodeAnnounceReply(reply: unknown): string {
    const kind = asBulk(reply)
    if (kind === undefined) throw new Error(ANNOUNCE_REPLY_REFUSED)
    return kind
}

/**
 * The one message {@link decodeRevocationPage} throws for an odd item list
 * (#359): from its first misalignment on, every score would be read as a
 * member. Constant; it never carries the reply. Exported for the test suite
 * only.
 */
export const REVOCATION_PAGE_REFUSED =
    'realtime: a revocation index page does not hold member and score ' +
    'pairs — its item list has an odd length'

/** One decoded revocation-index page (#359). Internal. */
export interface RevocationPage {
    /** The next cursor: `'0'` once the iteration is complete. */
    readonly cursor: string
    /** The page's well-formed pairs, member as stored, score as a number. */
    readonly entries: readonly { member: string; score: number }[]
    /** How many malformed pairs inside this well-formed page were skipped. */
    readonly skipped: number
}

/**
 * Decode one `ZSCAN` page of the revocation index (#359 FR-005, A12).
 *
 * The envelope goes through {@link decodeScanReply} — the one SCAN-envelope
 * decoder, so every envelope refusal is {@link SCAN_REPLY_REFUSED}. This is
 * the one `ZSCAN`-specific step after it, and **the single home of what a
 * well-formed pair is**:
 * - an **odd-length** item list throws {@link REVOCATION_PAGE_REFUSED}: the
 *   page cannot be paired at all;
 * - inside a well-formed page, a pair whose member is not a bulk string, or
 *   whose score is not a bulk string matching {@link EPOCH_SECONDS} (`inf`,
 *   `+inf`, a decimal point, an exponent), is **skipped and counted** in
 *   `skipped`, never thrown — a planted `+inf` member is never reaped, and a
 *   throw would fail every pass for as long as it stays.
 *
 * Exported for the test suite only; `mod.ts` does not re-export it.
 *
 * @param reply - The `ZSCAN` reply.
 * @returns The next cursor, the well-formed pairs and the skip count.
 * @throws {Error} {@link SCAN_REPLY_REFUSED} for a bad envelope,
 *   {@link REVOCATION_PAGE_REFUSED} for an odd item list.
 * @example
 * ```ts
 * decodeRevocationPage({
 *     type: 'array',
 *     value: [
 *         { type: 'bulk', value: '0' },
 *         { type: 'array', value: [
 *             { type: 'bulk', value: 'c1' },
 *             { type: 'bulk', value: '1790157900' },
 *         ] },
 *     ],
 * }) // { cursor: '0', entries: [{ member: 'c1', score: 1790157900 }], skipped: 0 }
 * ```
 */
export function decodeRevocationPage(reply: unknown): RevocationPage {
    const { cursor, items } = decodeScanReply(reply)
    if (items.length % 2 !== 0) throw new Error(REVOCATION_PAGE_REFUSED)
    const entries: { member: string; score: number }[] = []
    let skipped = 0
    for (let i = 0; i < items.length; i += 2) {
        const member = asBulk(items[i])
        const score = asBulk(items[i + 1])
        if (
            typeof member !== 'string' || typeof score !== 'string' ||
            !EPOCH_SECONDS.test(score)
        ) {
            skipped++
            continue
        }
        entries.push({ member, score: Number(score) })
    }
    return { cursor, entries, skipped }
}

/**
 * The one message {@link decodeRevocationFloor} throws (#380): the floor read
 * did not answer an array of bulk strings. Constant; it never carries the
 * reply. Exported for the test suite only.
 */
export const REVOCATION_FLOOR_REFUSED =
    'realtime: the revocation floor read did not answer an array of bulk ' +
    'strings'

/**
 * Decode the revocation floor read (#380) into the TTL a mark writes its
 * record at: the largest of `ownTtl` and every floor member.
 *
 * **The single home of what a floor reply means.**
 * - A reply that is not an array of bulk strings throws
 *   {@link REVOCATION_FLOOR_REFUSED}, which never carries the reply. The mark
 *   treats that as an unreadable floor and fails closed.
 * - A member that does not match {@link EPOCH_SECONDS} (`1e3`, `0x10`, ` 5`,
 *   `5.5`, `inf`, the empty string, `012`) is **skipped and counted**, never
 *   thrown: something other than this driver may have written it, and a throw
 *   would degrade every mark for as long as it stays.
 * - A member that matches is clamped to `[1, MAX_REVOCATION_TTL_SECONDS]`, so
 *   no floor content can score a record past the longest lifetime a driver
 *   accepts.
 *
 * No clock is consulted: an entry that has lapsed but was not yet pruned only
 * lengthens a record, which fails closed. Pure — it writes no WARN; the mark
 * reports the skip count after its write. Exported for the test suite only;
 * `mod.ts` does not re-export it.
 *
 * @param reply - The `ZRANGEBYSCORE <floor> -inf +inf` reply.
 * @param ownTtl - This driver's own `revocationTtlSeconds`.
 * @returns `ttl`, the effective TTL, and `skipped`, the members that are not a
 *   TTL in seconds.
 * @throws {Error} {@link REVOCATION_FLOOR_REFUSED}, for a reply that is not an
 *   array of bulk strings.
 * @example
 * ```ts
 * decodeRevocationFloor({
 *     type: 'array',
 *     value: [{ type: 'bulk', value: '300' }, { type: 'bulk', value: '1e3' }],
 * }, 10) // { ttl: 300, skipped: 1 }
 * ```
 */
export function decodeRevocationFloor(
    reply: unknown,
    ownTtl: number,
): { ttl: number; skipped: number } {
    const members = asArray(reply)
    if (members === undefined) throw new Error(REVOCATION_FLOOR_REFUSED)
    let ttl = ownTtl
    let skipped = 0
    for (const item of members) {
        const member = asBulk(item)
        if (member === undefined) throw new Error(REVOCATION_FLOOR_REFUSED)
        if (!EPOCH_SECONDS.test(member)) {
            skipped++
            continue
        }
        const clamped = Math.min(
            Math.max(Number(member), 1),
            MAX_REVOCATION_TTL_SECONDS,
        )
        ttl = Math.max(ttl, clamped)
    }
    return { ttl, skipped }
}

/**
 * What a revocation pass throws once {@link RedisBroadcastDriver.close} has
 * begun (#359): it stops before its next reap or page read, and a closing
 * pass never answers `[]`, which would read as "nobody is revoked". Exported
 * for the test suite only.
 */
export const REVOCATION_PASS_CLOSING =
    'realtime: the revocation pass stopped before its next read — the ' +
    'driver is closing'

/**
 * The words of the one WARN a revocation pass logs, after its last page, when
 * well-formed pages carried malformed pairs (#359 FR-006a, S1) — followed by
 * the count, never by a member or a score. Exported for the test suite only.
 */
export const REVOCATION_PAIRS_SKIPPED =
    'realtime: the revocation index returned pairs that are not revocations ' +
    '(a member that is not a string, or a score that is not canonical epoch ' +
    'seconds) and they were skipped — a broker that formats scores ' +
    'differently would leave every revocation unenforced. Pairs skipped this ' +
    'pass:'

/**
 * The words that start the one WARN a revocation pass writes when its
 * re-check handler resolved a value that claims to be a
 * {@link RevocationTally} and is not one (#384): a count missing, not a safe
 * integer, negative, or `failed` above `attempted`, or a count that threw
 * while being read. Followed by the pass's trigger and the contract — never
 * by the value.
 *
 * At most one per pass, written where the value is decoded and nowhere else.
 * The pass stays `ok` (the enumeration completed), reports no counts, and is
 * **not clean**, so it does not re-arm the enforcement deadline: a handler
 * reporting counts no one can trust must not keep the guarantee looking kept.
 * A value that is not tally-shaped at all is no tally, and writes nothing.
 * Exported for the test suite only.
 */
export const REVOCATION_TALLY_MALFORMED =
    'realtime: the revocation re-check resolved a malformed tally (#384):'

/**
 * The words that start the one WARN written when an
 * {@link RedisBroadcastDriver.onPassComplete} handler throws, or the promise
 * it returned rejects (#360) — followed by the rendered failure. The pass
 * itself is unaffected. Exported for the test suite only.
 */
export const PASS_SAMPLE_FAILED =
    'realtime: an onPassComplete handler failed (#360); the pass itself is ' +
    'unaffected:'

/**
 * The marker that starts the one ERROR line written when the
 * {@link PASS_SAMPLE_FAILED} WARN could not be, because `console.warn` threw
 * (#360). The line carries both halves, the handler's failure and the sink's,
 * each rendered; the marker is the fixed prefix, so an error text cannot forge
 * it (#369). Exported for the test suite only.
 */
export const PASS_SAMPLE_LOG_FAILED = markedFallbackMarker(
    'realtime: a pass-sample failure could not be logged (#360):',
)

/**
 * The marker that starts the one ERROR line written when a rejection reaches
 * the end of the ghost-sweep chain (#360): a log sink threw inside the sweep,
 * so the pass rejected. The rejection is rendered after it; the marker is the
 * fixed prefix, so an error text cannot forge it (#369). Exported for the test
 * suite only.
 */
export const SWEEP_LOG_FAILED = markedFallbackMarker(
    'realtime: a ghost-sweep log line could not be written (#360):',
)

/**
 * The WARN {@link RedisBroadcastDriver.close} writes once when its bounded
 * drain expires (#368): it waited one liveness TTL for the ghost-sweep pass,
 * then the lapse run, and at least one was still pending. Followed by which
 * one(s), the sweep pass's age where known, and the budget — never a member,
 * channel or instance id. Exported for the test suite only.
 */
export const CLOSE_DRAIN_EXPIRED =
    "realtime: close()'s drain EXPIRED (#368): the bounded wait for work " +
    'already in flight ran out and teardown continued anyway'

/**
 * The marker that starts the one ERROR line written when the
 * {@link CLOSE_DRAIN_EXPIRED} WARN could not be, because `console.warn`
 * threw (#369 shape, #391): a marked `console.error` line instead, through
 * {@link writeMarkedFallback}, which never throws. Exported for the test
 * suite only.
 */
export const CLOSE_LOG_FAILED = markedFallbackMarker(
    "realtime: close()'s drain WARN could not be logged (#368):",
)

/**
 * The marker that starts the one ERROR line written when the control
 * subscription's WARN could not be, because `console.warn` threw (#395). The
 * subscription's promise is `void`ed, so that throw would reach the runtime as
 * an unhandled rejection. The line carries the subscription's failure and the
 * sink's, each rendered. Exported for the test suite only.
 */
export const CONTROL_SUBSCRIBE_LOG_FAILED = markedFallbackMarker(
    'realtime: a control-subscription failure could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when a failed heartbeat's
 * WARN could not be, because `console.warn` threw (#395). The interval
 * discards the heartbeat's promise, so that throw would reach the runtime as
 * an unhandled rejection. The line carries the beat's failure and the sink's,
 * each rendered. Exported for the test suite only.
 */
export const HEARTBEAT_LOG_FAILED = markedFallbackMarker(
    'realtime: a heartbeat failure could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when the malformed-message
 * WARN in {@link RedisBroadcastDriver.onMessage}'s `#deliver` could not be,
 * because `console.warn` threw (#418). `#deliver` is registered directly as
 * the subscriber's message handler — nothing in this class wraps that call in
 * a `try`, so an unguarded throw here would reach whatever the `RedisSubscriber`
 * port's concrete implementation does with a handler fault, not this driver's
 * own containment (the port makes no promise either way — see its JSDoc — and
 * this package's own `FakeRedis` test double dispatches synchronously with no
 * containment of its own). Exported for the test suite only.
 */
export const MESSAGE_DECODE_LOG_FAILED = markedFallbackMarker(
    'realtime: a malformed-message WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when the malformed-control
 * WARN in {@link RedisBroadcastDriver.onControl}'s `#verifyAndDecode` could not
 * be, because `console.warn` threw (#418) — the control-topic twin of
 * {@link MESSAGE_DECODE_LOG_FAILED}: `onControl`'s `deliver` closure calls
 * `#verifyAndDecode` directly as the subscriber's handler, with the same
 * unwrapped call site. Exported for the test suite only.
 */
export const CONTROL_DECODE_LOG_FAILED = markedFallbackMarker(
    'realtime: a malformed-control WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when the roster-departure
 * WARN in {@link RedisBroadcastDriver}'s `#announceSwept` could not be,
 * because `console.warn` threw (#418). That WARN is written from inside a
 * `catch` around the departure handler's own call, in the middle of
 * `#sweepPage`'s loop over one dead instance's owned slots — nothing there
 * wraps `#announceSwept`'s own await, so an unguarded throw would reject
 * `#sweepPage`, skipping every remaining slot on the page (and every later
 * page for the SAME dead instance this pass), exactly the "escapes a loop and
 * skips the rest of it" shape #395 named. `#sweepInstance`'s own `try` around
 * `#sweepOwned` turns an ordinary rejection there into one WARN and a normal
 * return, so a departure HANDLER that merely throws never travels past this
 * site, or past `#sweepInstance`. `#sweepInstance`'s own "sweep … failed"
 * line ({@link SWEEP_INSTANCE_LOG_FAILED}) and `#reconcile`'s own catch
 * ({@link RECONCILE_LOG_FAILED}) are guarded too (security review, same
 * issue) — before that, when the SINK itself was down, an escape from THIS
 * site climbed past both of theirs in turn, all the way to the top of the
 * whole reconcile chain ({@link SWEEP_LOG_FAILED}), costing every dead
 * instance still left in `ids` its sweep this pass, not only this one's
 * remaining slots (#355 A3). With all three guarded, one dead instance's
 * failure — sweep, sweep-report or reconcile-report — ends only that
 * instance's sweep, never a peer's. Exported for the test suite only.
 */
export const SWEEP_DEPARTURE_LOG_FAILED = markedFallbackMarker(
    'realtime: a roster-departure WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when
 * `RedisBroadcastDriver`'s `#announceSwept` drops a swept slot's own WARN —
 * "was not announced as left" — because `console.warn` threw (#419, the
 * security review of #418 that found two more bare sites). Unlike
 * {@link SWEEP_DEPARTURE_LOG_FAILED}, which guards the departure HANDLER's
 * own failure, this guards `#announceSwept`'s own drop of an entry the
 * handler never even sees: an owned entry whose channel fails
 * {@link isValidName}, or whose decoded member id does not match the slot it
 * was released from ({@link sameMemberId}). Both checks run BEFORE the
 * handler is called, inside `#sweepPage`'s loop over one dead instance's
 * owned slots, with nothing between here and there that catches a throw —
 * so an unguarded `console.warn` failing here used to reject
 * `#announceSwept` and abort that loop, skipping every remaining slot on the
 * page (and any later page) for the SAME dead instance this pass, the
 * identical #395 shape {@link SWEEP_DEPARTURE_LOG_FAILED} already guards one
 * exit later. Exported for the test suite only.
 */
export const SWEEP_DROPPED_LOG_FAILED = markedFallbackMarker(
    'realtime: a swept-member-dropped WARN could not be logged (#419):',
)

/**
 * The marker that starts the one ERROR line written when
 * `RedisBroadcastDriver`'s `#parseRosterValue` drops a malformed roster
 * entry's own "skipped a malformed roster entry" WARN, because
 * `console.warn` threw (#419). **The one decode of a roster entry** (#348),
 * so this ONE guard covers all three of its call sites: `readRoster`'s
 * sample loop, its selves loop, and `#announceSwept`'s own decode of a swept
 * entry. None of the three wraps the call in a `try`, so an unguarded
 * `console.warn` failing here escaped whichever of them called it —
 * synchronously out of `readRoster`'s `for` loop, aborting it before a later
 * sample or self is ever decoded, or out of `#announceSwept`, into
 * `#sweepPage`'s loop with nothing there to catch it either, the same #395
 * shape {@link SWEEP_DROPPED_LOG_FAILED} guards one call up. Exported for
 * the test suite only.
 */
export const ROSTER_ENTRY_LOG_FAILED = markedFallbackMarker(
    'realtime: a malformed-roster-entry WARN could not be logged (#419):',
)

/**
 * The marker that starts the one ERROR line written when `#sweepInstance`'s
 * "sweep of dead instance … failed" WARN could not be, because `console.warn`
 * threw (#418, security review of the same issue). `#sweepInstance`'s own
 * `try` around `#sweepOwned` already turns an ordinary sweep failure into this
 * one WARN and a normal return — the invariant #355 A3 states ("one
 * instance's failure ends only that instance's sweep"). Before this site was
 * guarded, that invariant held for an ordinary failure but not for a BROKEN
 * SINK: the WARN's own throw escaped `#sweepInstance`, reached `#reconcile`'s
 * `for` loop with no per-iteration `try` to stop it, and skipped every
 * instance still left in `ids` — the opposite of what A3 promises. Exported
 * for the test suite only.
 */
export const SWEEP_INSTANCE_LOG_FAILED = markedFallbackMarker(
    'realtime: a sweep-failure WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when `#sweepInstance`'s
 * "instance … renewed its liveness" WARN could not be, because `console.warn`
 * threw (#418, second security review of the same issue). This is a SUCCESS
 * exit — the instance turned out to be alive, not a crash — so nothing wraps
 * it in a `try` the way the failure branch's own WARN is wrapped: an
 * unguarded throw here escaped `#sweepInstance` exactly as {@link
 * SWEEP_INSTANCE_LOG_FAILED}'s did, reaching `#reconcile`'s `for` loop and
 * skipping every instance still left in `ids` — #355 A3 broken by a
 * SUCCESSFUL sweep's own report, not a failed one. Exported for the test
 * suite only.
 */
export const SWEEP_INSTANCE_RENEWED_LOG_FAILED = markedFallbackMarker(
    'realtime: a sweep-renewed WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when `#sweepInstance`'s
 * "released N hold(s) of dead instance …" WARN could not be, because
 * `console.warn` threw (#418, second security review of the same issue). The
 * control-flow twin of {@link SWEEP_INSTANCE_RENEWED_LOG_FAILED}: also a
 * SUCCESS exit (every owned entry was released, `completed`/`kept`/`closed`),
 * also unwrapped by any `try`, and also capable of skipping every instance
 * still left in `ids` when its own sink is what is down. Exported for the
 * test suite only.
 */
export const SWEEP_INSTANCE_RELEASED_LOG_FAILED = markedFallbackMarker(
    'realtime: a sweep-released WARN could not be logged (#418):',
)

/**
 * The marker that starts the one ERROR line written when `#reconcile`'s own
 * catch — the "roster reconcile failed" WARN — could not be, because
 * `console.warn` threw (#418, security review of the same issue). That catch
 * wraps the whole pass: the `SMEMBERS` read, and the `for` loop's `EXISTS`
 * checks and `#sweepInstance` calls. `#sweepInstance` itself never rejects
 * once its own WARN is guarded ({@link SWEEP_INSTANCE_LOG_FAILED}), so this
 * catch now fires only for a failure in the pass's OWN housekeeping — a
 * broker round trip or a decode failure on `SMEMBERS`/`EXISTS` — never for a
 * single dead instance's own sweep. Guarding it stops that housekeeping
 * failure's own WARN from being the thing that turns a resolved `'failed'`
 * pass into a rejected one: `#armReconcile`'s `.finally` re-arms the next
 * pass either way, but a rejection here used to erase this WARN's specific
 * text behind the generic {@link SWEEP_LOG_FAILED} fallback instead. Exported
 * for the test suite only.
 */
export const RECONCILE_LOG_FAILED = markedFallbackMarker(
    'realtime: a reconcile-failure WARN could not be logged (#418):',
)

/**
 * The words that start the one WARN written when the floor announce failed
 * (#380) — followed by the rendered failure. The announce is retried; the
 * revocation pass writes the same entry on its next reap. Exported for the
 * test suite only.
 */
export const REVOCATION_FLOOR_ANNOUNCE_FAILED =
    'realtime: the revocation floor announce failed (#380); it is retried, ' +
    'and the next revocation pass writes the entry anyway:'

/**
 * The words of the one WARN a mark writes, after its `EVAL`, when the floor
 * held members that are not a TTL in seconds (#380) — followed by the count,
 * never by a member. Exported for the test suite only.
 */
export const REVOCATION_FLOOR_SKIPPED =
    'realtime: the revocation floor held members that are not a TTL in ' +
    'seconds and they were skipped; the record was written anyway. Members ' +
    'skipped:'

/**
 * The words that start the one WARN a mark writes, after its `EVAL`, when the
 * floor could not be read (#380) — followed by the rendered failure. The
 * record was written at the maximum TTL instead (fail closed). Exported for
 * the test suite only.
 */
export const REVOCATION_FLOOR_READ_FAILED =
    'realtime: the revocation floor could not be read (#380); the record was ' +
    'written at the maximum TTL instead:'

/**
 * The marker that starts the one ERROR line written when a floor WARN could
 * not be, because `console.warn` threw (#380). The line carries the WARN and
 * the sink's failure, each rendered; the marker is the fixed prefix, so an
 * error text cannot forge it (#369). Exported for the test suite only.
 */
export const REVOCATION_FLOOR_LOG_FAILED = markedFallbackMarker(
    'realtime: a revocation floor WARN could not be logged (#380):',
)

/**
 * The words of the one WARN written when {@link FLOOR_WRITE} healed a
 * wrong-typed revocation floor key (#405) — followed by the prior Redis type
 * (`string`, `list`, `set`, `hash` or `stream`), never a member, channel or
 * instance id. Written once per healed pass, from `listRevocations` when the
 * reap healed it or from {@link RedisBroadcastDriver.#announceFloor} when the
 * announce did — every occurrence WARNs, including a repeatedly-recorrupted
 * key. The pass or announce completed normally: the heal ran inside the same
 * atomic `EVAL` that already writes the floor, so nothing here means the
 * write failed. Exported for the test suite only.
 */
export const REVOCATION_FLOOR_WRONG_TYPE =
    'realtime: the revocation floor key held the wrong Redis type and was ' +
    'healed (#405); the pass completed normally. Prior type:'

/**
 * The words of the one WARN written when {@link INDEX_HEAL} healed a
 * wrong-typed revocation INDEX key (#411) — followed by the prior Redis type
 * (`string`, `list`, `set`, `hash` or `stream`), never a member, channel or
 * instance id — wording twin to {@link REVOCATION_FLOOR_WRONG_TYPE}. Written
 * once per healed pass, from `listRevocations` when the reap healed it or
 * from `markRevocation` when the mark did — every occurrence WARNs, including
 * a repeatedly-recorrupted key. The pass or mark completed normally: the heal
 * ran inside the same atomic `EVAL` that already touches the index, so
 * nothing here means the write failed. Exported for the test suite only.
 */
export const REVOCATION_INDEX_WRONG_TYPE =
    'realtime: the revocation index key held the wrong Redis type and was ' +
    'healed (#411); the pass completed normally. Prior type:'

/**
 * The words of the one WARN written when {@link OWNED_HEAL} healed a
 * wrong-typed owned-set key (#414) — followed by the prior Redis type
 * (`string`, `list`, `hash`, `zset` or `stream`), never a member, channel or
 * instance id. Written once per healed hold or release — the write completed
 * normally: the heal ran inside the same atomic `EVAL`. Exported for the test
 * suite only.
 */
export const OWNED_SET_WRONG_TYPE =
    'realtime: the owned set key held the wrong Redis type and was healed ' +
    '(#414); the write completed normally. Prior type:'

/**
 * The words of the one WARN written when {@link INSTANCES_HEAL} healed a
 * wrong-typed instances-set key (#414) — followed by the prior Redis type,
 * never a member, channel or instance id — wording twin to
 * {@link OWNED_SET_WRONG_TYPE}. Written once per healed hold or
 * deregistration. Exported for the test suite only.
 */
export const INSTANCES_SET_WRONG_TYPE =
    'realtime: the instances set key held the wrong Redis type and was ' +
    'healed (#414); the write completed normally. Prior type:'

/**
 * What one heartbeat's `SET <alive key> 1 EX <ttl> GET` reported (#349):
 * `continuous` — the key existed, so this renewal extended it; `lapsed` — the
 * key had expired or was deleted, so this write re-created it, and a peer may
 * have swept this instance's holds meanwhile.
 */
export type BeatOutcome = 'lapsed' | 'continuous'

/**
 * Decode a heartbeat liveness-write reply (#349 FR-002): a nil is
 * **lapsed**, any bulk string (the previous value, whatever it is) is
 * **continuous**, and anything else throws.
 *
 * **The single home of what a beat reply means**, and called only inside
 * {@link RedisBroadcastDriver}'s heartbeat, inside the `try` around its `SET`,
 * so a throw here is one failed beat and never escapes. A simple `OK` is a
 * `SET` without `GET` — not this command — and reading it (or a `null`, or
 * truthiness) as continuous would hide every lapse.
 *
 * **The error message is constant**: it names what is accepted and never the
 * reply, its type or its length (#355 S4). Exported for the tests, not from
 * `mod.ts`.
 *
 * @param reply - The `SET … GET` reply.
 * @returns Whether the key was re-created by this write.
 * @throws {Error} If the reply is neither a nil nor a bulk string.
 * @example
 * ```ts
 * decodeBeatReply({ type: 'nil' }) // 'lapsed'
 * ```
 */
export function decodeBeatReply(reply: unknown): BeatOutcome {
    if (
        typeof reply === 'object' && reply !== null &&
        (reply as { type?: unknown }).type === 'nil'
    ) {
        return 'lapsed'
    }
    if (asBulk(reply) !== undefined) return 'continuous'
    throw new Error(
        'realtime: the liveness write answered neither a nil (the key was ' +
            're-created) nor a bulk string (its previous value) — is SET … GET ' +
            'supported by this broker?',
    )
}

/**
 * How one instance's sweep stopped without throwing (#355) — which line the
 * Redis driver's sweep logs:
 * - `completed` — one full scan of the owned set, then deregistered;
 * - `kept` — one full scan, then the deregistration answered *kept*: the
 *   owned set still holds an entry (a hold that landed behind the scan's
 *   cursor, or one that does not parse), so the instance stays registered
 *   and a later pass resumes it (#358);
 * - `closed` — cut short by `close()`;
 * - `renewed` — a release or the deregistration was refused.
 *
 * `kept` and `closed` leave work behind: their "released" line carries the
 * *unfinished* suffix.
 */
type SweepStop = 'completed' | 'kept' | 'closed' | 'renewed'

/** How one instance's sweep ended: a {@link SweepStop}, or the error it threw. */
type SweepEnd = SweepStop | { readonly failed: unknown }

/** What one instance's sweep has removed so far: the N and E of its line. */
interface SweepCount {
    /** Holds removed — emptied + kept (N). */
    released: number
    /** Slots emptied, each a departure announced (E). */
    emptied: number
}

/** A stored roster entry: the client-visible member + its internal owner (FR-018). */
interface RosterEntry {
    /** The client-visible member (the only field that enters snapshots/frames). */
    readonly member: PresenceMember
    /**
     * The id of the instance that wrote this entry — internal, never
     * client-visible. Who HOLDS a slot is the holders hash's keys (#345), not
     * this field; nothing decides a hold or a sweep by reading it.
     *
     * **It is still load-bearing: it makes each holder's stored value
     * distinct.** Two instances holding one member with identical `info` would
     * otherwise store byte-identical entries, and the release script's
     * `shown == mine` — which decides whether the shown entry must be replaced —
     * would be a value comparison that a departing holder could win for an
     * entry another instance wrote. With the owner in the bytes it is an
     * identity check.
     */
    readonly owner: string
}

/** The wire shape of a control message: the manager-facing fields + `origin`. */
interface ControlWire {
    kind: ControlMessage['kind']
    target: string
    channel?: string
    member?: PresenceMember
    /**
     * `revoke-channel` only: the id of the record the frame announces (#337).
     * Inside the MAC, and appended LAST in `#canonical` — absent, it is left
     * out of the canonical bytes, so every other kind signs exactly what a
     * `0.3.0` peer signs.
     */
    revocationId?: string
    origin: string
    /**
     * Epoch milliseconds at issue (#272). Inside the MAC — outside it, an
     * attacker could re-date a captured frame and the window would be
     * decorative.
     */
    ts: number
    /**
     * A per-frame CSPRNG value (#272). Inside the MAC, for the same reason.
     * Unpredictability is not what the anti-replay property needs — an attacker
     * cannot forge a MAC over a nonce of their choosing — but a CSPRNG is how
     * uniqueness survives a restart and holds across instances without
     * coordination. A counter would collide across senders and again after
     * every restart.
     */
    nonce: string
    mac?: string
}

const DEFAULT_LIVENESS_TTL_SECONDS = 15
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000
const DEFAULT_RECONCILE_INTERVAL_MS = 10_000
/**
 * How long after a FAILED seam-triggered reconcile the single retry runs
 * (#308).
 *
 * An order of magnitude under `DEFAULT_RECONCILE_INTERVAL_MS`, so the retry is
 * still a fast path and not a second timer — and not instant, because the
 * failure it answers is usually a broker that just refused a command.
 */
const RECONCILE_RETRY_MS = 1_000
/**
 * The first backoff step of a failed floor announce's retry, in milliseconds
 * (#380 S1). It doubles on each failure, capped at `reconcileIntervalMs`. It
 * is below 2 s so that a reader whose first announce failed is back on the
 * floor within seconds, not after its first pass.
 *
 * Exported (#415) so `driver_redis_live.test.ts` can match this exact value
 * instead of duplicating the literal — `RECONCILE_RETRY_MS` above arms the
 * same 1000 ms for an unrelated retry, so a test that only checked the value
 * would suppress both.
 */
export const FLOOR_ANNOUNCE_RETRY_MS = 1_000
const DEFAULT_REVOCATION_TTL_SECONDS = 300
/**
 * What one completed background pass of the Redis driver reports to the
 * handler registered through {@link RedisBroadcastDriver.onPassComplete}
 * (#360): which pass it was, what started it, how it ended, how long it took
 * and how many pages it read. Handed over frozen, once per completed pass.
 *
 * **The one home of what each field means.** Documentation elsewhere links
 * here rather than restating it. The instrument names an application records
 * these under live in `docs/observability-and-crypto.md` § Framework
 * instruments.
 *
 * A pass that never completes — a command that never settles — reports
 * nothing, because the sample is taken at the pass's end. For the revocation
 * pass, the enforcement deadline (#362) is what reports a stall.
 */
export interface PassSample {
    /**
     * Which background pass: `sweep` — the ghost sweep, which releases the
     * presence holds of instances whose liveness lapsed; `revocation` — the
     * revocation re-check, which applies revocations whose control frame was
     * lost.
     */
    readonly pass: 'sweep' | 'revocation'
    /**
     * What started the pass: `timer` — the periodic timer; `reconnect` — the
     * subscribe socket's reconnect seam; `reconnect-retry` — the one retry a
     * failed reconnect pass gets. Always `timer` for the sweep.
     */
    readonly trigger: 'timer' | 'reconnect' | 'reconnect-retry'
    /**
     * How the pass ended.
     *
     * `ok` means **the enumeration completed — not that every record was
     * applied**. A failure confined to one unit (one dead instance's sweep,
     * one revocation's apply) is reported as its own named WARN and counted in
     * {@link failures}, and the pass is still `ok`.
     *
     * `failed` means the pass itself stopped. For the sweep: the instance-set
     * read or a liveness probe threw, or answered a reply that does not
     * decode. For the revocation pass: the re-check handler threw — a reap or
     * a page read failed or did not decode. A pass that rejected before its
     * outcome was recorded (a log sink threw inside it) is `failed` too.
     */
    readonly outcome: 'ok' | 'failed'
    /**
     * How long the pass took, in milliseconds, on the driver's monotonic pass
     * clock — never the wall clock — and never negative.
     *
     * It is **the whole pass**: every round trip, and for the revocation pass
     * also the apply and any wait on the manager's serial tail. It is
     * therefore at least, and not comparable to, the round-trip-only `P` the
     * revocation enforcement bound is stated in — see the bound's one home,
     * {@link RedisBroadcastDriver.onRevocationReconcile}.
     */
    readonly durationMs: number
    /**
     * How many pages the pass read: each one a decoded reply of a paged read.
     * A page that failed to decode is not counted.
     *
     * - `sweep`: the `SSCAN` pages of the owned sets of every dead instance
     *   swept in this pass, summed. The unpaged instance-set read (`SMEMBERS`)
     *   and each liveness probe (`EXISTS`) are timed but not counted. `0` when
     *   no instance was dead.
     * - `revocation`: the `ZSCAN` pages of the revocation index that
     *   {@link RedisBroadcastDriver.listRevocations} read while this pass was
     *   in flight — including a call that ran on the manager's serial tail
     *   ahead of this pass's own handler, whose time is in `durationMs` too.
     */
    readonly pages: number
    /**
     * How many units the pass attempted (#384). **The one home of what the
     * counts mean, per pass**; documentation elsewhere links here.
     *
     * - `sweep`: one dead instance the sweep was called for — an instance
     *   whose liveness key was gone when probed. A live instance is not
     *   attempted. `0` when none was dead.
     * - `revocation`: one apply, as {@link RevocationTally.attempted}
     *   defines it — a connection revocation or a channel pair, for a socket
     *   this instance owns.
     *
     * **Present on every sweep sample** — a failed sweep reports what it
     * reached — and **on a revocation sample whose handler resolved a valid
     * tally**. Absent (no key at all, never `undefined`) otherwise: a handler
     * that resolved nothing or a value that is not tally-shaped, a malformed
     * tally ({@link REVOCATION_TALLY_MALFORMED}), or a handler that threw.
     * `attempts` and {@link failures} are always present together.
     */
    readonly attempts?: number
    /**
     * How many of the {@link attempts} failed (#384); present exactly when
     * `attempts` is.
     *
     * - `sweep`: one dead instance whose sweep threw — its one "failed" WARN.
     *   An instance that renewed its liveness while being swept, or whose
     *   sweep was cut short by `close()` or left unfinished, is attempted and
     *   not failed.
     * - `revocation`: {@link RevocationTally.failed} — an apply that threw.
     *
     * A failure means the unit threw, not that the socket stayed subscribed.
     * **A revocation pass with a failure is not clean**: it does not re-arm
     * the enforcement deadline ({@link RedisBroadcastDriver.onRevocationReconcile}).
     */
    readonly failures?: number
}
/**
 * One revocation pass in flight, as {@link RedisBroadcastDriver} records it
 * (#359, #360, #362, #384): its trigger and start, then what it learns while
 * it runs — its pages, and what its handler resolved to (the counts of a
 * valid tally, or the mark of a malformed one). Built once at the pass's
 * start; read by its end site from that closure.
 */
interface RevocationPassRecord {
    readonly trigger: 'timer' | 'reconnect' | 'reconnect-retry'
    readonly startedAt: number
    /** The `ZSCAN` pages read for this pass so far. */
    pages: number
    /**
     * The reap time of THIS pass's own completed enumeration, in broker
     * seconds (#383): what {@link EnforcementDeadline.passSucceeded} compares
     * pass-to-pass for `SKEWED`, scoped to the pass that reports it rather
     * than to whichever pass last happened to enumerate. `undefined` when
     * this pass never called {@link RedisBroadcastDriver.listRevocations}.
     * `#lastReadAt` remains {@link RedisBroadcastDriver.#announceFloor}'s own
     * sticky "has any pass ever completed an enumeration" flag; the two no
     * longer share a reader.
     */
    readAt?: number
    /** A valid tally's `attempted`, once the handler resolved one. */
    attempts?: number
    /** A valid tally's `failed`, once the handler resolved one. */
    failures?: number
    /** Set when the handler resolved a malformed tally: the pass is not clean. */
    malformed?: true
}
/**
 * How one background pass ended (#362; widened to the ghost sweep by #360):
 * the public {@link PassSample} outcome, plus `closed`, which never leaves
 * the driver and is never reported.
 *
 * - The revocation pass, as `#runRevocationReconcile` declares it: `ok` —
 *   the handler resolved, so the enumeration completed (not that every
 *   record was applied); `failed` — it threw; `closed` — no handler was
 *   registered, which happens only after `close()` dropped it.
 * - The ghost sweep, as `#reconcile` declares it: `ok` — every registered
 *   instance was read, a failure confined to one instance's sweep included;
 *   `failed` — the instance-set read or a liveness probe threw, or answered
 *   a reply that does not decode; `closed` — the loop stopped at an
 *   instance because `close()` had begun.
 *
 * **Unrecorded means `failed`, for both passes** — this is the one home of
 * that rule. Each start site declares its outcome `failed` before the pass
 * runs, and only a pass that settles records another, so a pass that
 * rejects (a log sink threw inside it, #349) is reported as failed.
 */
type PassOutcome = PassSample['outcome'] | 'closed'
/**
 * The largest delay one `setTimeout` can hold, in milliseconds (#362): the
 * largest signed 32-bit integer.
 *
 * **A longer delay does not wait longer; it fires almost at once.** Measured
 * on Deno 2.9.6: a delay of 2^31 ms or more is replaced by 1 ms, with a
 * `TimeoutOverflowWarning`. A timing past this ceiling is therefore the hot
 * loop again, not a slow timer, so the constructor refuses a revocation TTL
 * whose deadline could not fit one timer, rather than chunking the wait — and
 * a heartbeat interval above it (#381), which the #293 relation alone admits
 * whenever the liveness TTL is large.
 */
const MAX_TIMER_MS = 2 ** 31 - 1
/**
 * The largest `revocationTtlSeconds` a driver accepts, and the ceiling of every
 * revocation lifetime it writes (#362, #380): the whole seconds one
 * `setTimeout` can hold ({@link MAX_TIMER_MS}), about 24.8 days.
 *
 * **The one home of that ceiling.** The constructor's range guard refuses a TTL
 * above it; {@link decodeRevocationFloor} clamps every floor member to it; and
 * a mark whose floor read fails writes its record at exactly this lifetime
 * (fail closed). Exported for the test suite only; `mod.ts` does not re-export
 * it.
 */
export const MAX_REVOCATION_TTL_SECONDS: number = Math.floor(
    MAX_TIMER_MS / 1000,
)
/**
 * How long after issue a control frame may still be obeyed (#272). See
 * `RealtimeControlConfig.windowMs` for why 30s and what widening it costs.
 */
const DEFAULT_CONTROL_WINDOW_MS = 30_000
/**
 * The byte ceiling on a control payload, checked BEFORE `JSON.parse` and before
 * any MAC computation.
 *
 * A control frame is a kind, two names and a small member — kilobytes at the
 * outside. Without this bound an unauthenticated PUBLISH costs every instance
 * in the fleet a parse, a re-serialise and a *synchronous, pure-JS* SHA-256
 * (`hmacSha256Hex`) over attacker-chosen bytes, on the event loop, before the
 * MAC has had a chance to reject it. The RESP reader already caps a frame at
 * 10MB, so this is an amplifier rather than an unbounded one — but 10MB of
 * blocking hash per packet, multiplied by instance count, is not a cost the MAC
 * check contains.
 */
const DEFAULT_MAX_CONTROL_PAYLOAD_BYTES = 8 * 1024
/** The exact width of a hex-encoded 16-byte nonce. */
const CONTROL_NONCE_HEX_LENGTH = 32
/**
 * The minimum control-secret length, in bytes. The FR-015 MAC is only as strong
 * as its key: a short, guessable secret lets a peer forge an authentic-looking
 * control frame, so a secret below this floor is rejected at construction.
 */
const MIN_CONTROL_SECRET_BYTES = 32
/**
 * Field separator inside an owned-member set entry — `channel memberId`, joined
 * by a single space. Unambiguous because the first space always marks the
 * channel/member boundary: a channel name cannot contain one, and a member id
 * after it may.
 *
 * **That was an unenforced claim until #314.** `isValidName` ran only on the
 * WebSocket wire (`decodeClientMessage`), never on `ChannelManager.subscribe`'s
 * public path — so a channel with a space could be created programmatically,
 * and `#sweepInstance` then split `presence-my room u1` into channel
 * `presence-my` and field `room u1`, issuing `HDEL` against a key that does not
 * exist and leaving the members unreclaimed forever. Only the death-recovery
 * path broke, because the leave path re-joins the full string, which is why it
 * went unnoticed. `ChannelManager`'s `#assertUsableChannel` is the enforcement
 * point this docstring now depends on rather than assumes.
 *
 * The other half — that a member id after the first space may contain more — is
 * proven by the US5/FR-008 live-broker scenario since #316, whose ghost carries
 * a two-space id. Before that its id was `2`, so `indexOf` and `lastIndexOf`
 * agreed on every entry and the sweep could have parsed on the LAST space
 * undetected: the line ran on every pass and no fixture could observe it.
 *
 * **The holders key joins channel and member id with this separator too**
 * (#345, `holdersKey`), so the same no-space-in-a-channel rule is what keeps
 * two slots from sharing one holders hash.
 */
const OWNED_SEP = ' '

/**
 * The `COUNT` of every owned-set page the ghost sweep reads (#358) — the
 * single home of the page size, and not configurable: no option, no
 * environment variable. Exported for the test suite only; `mod.ts` does not
 * re-export it.
 *
 * **The bound it buys.** An owned entry is at most a 200-byte channel, a space
 * and a 600-byte member id (200 characters of UTF-8): about 812 wire bytes
 * with its RESP framing. A hashtable-encoded set answers about `COUNT`
 * members per call, plus the rest of the last bucket it visited, so one page
 * stays around 100 KB of wire at maximum entry length (typically about 5 KB)
 * against the command client's 32 MiB reply cap — and a survivor holds one
 * page of a dead instance's owned set at a time.
 *
 * **It holds only while the broker honours `COUNT`** (S4). A listpack-encoded
 * set is answered whole whatever `COUNT` says (by default at most 128 entries
 * of at most 64 bytes, so harmless); an operator who raises
 * `set-max-listpack-entries` into the tens of thousands, or a Redis-compatible
 * server that answers `SSCAN` whole, reopens a large reply on that deployment.
 * The #285 live conformance case asserts the broker pages.
 */
export const OWNED_SCAN_COUNT = 100

/**
 * What an undecodable swept entry costs (#348), appended to the decoder's
 * one WARN: the departure was never reported, so the room did not hear it.
 * Words only — never the entry.
 */
const SWEPT_ENTRY_NOT_ANNOUNCED =
    'the member swept from it was not announced as left. The release is ' +
    'committed; clients heal on resubscribe.'

/**
 * Constant-time-ish comparison of two lowercase-hex MAC strings. Compares every
 * character regardless of the first mismatch so verification does not leak where
 * a forged MAC first diverges.
 */
function timingSafeEqualHex(a: string, b: string): boolean {
    if (a.length !== b.length) return false
    let diff = 0
    for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i)
    return diff === 0
}

/**
 * What {@link RedisBroadcastDriver.fromConfig} accepts: a Redis client config,
 * plus the subscribe socket's liveness and retry cadences.
 *
 * The cadences are here because they were otherwise **unreachable**. `fromConfig`
 * builds the `RedisSubscribeConnection` itself, so an application had no way to
 * pass one — and because every field is optional, a literal carrying
 * `keepaliveMs` was rejected as an excess property rather than silently ignored.
 * `packages/redis/README.md` documents these four by name, and a documented knob
 * nobody can set is a README that lies.
 */
export type RedisBroadcastConnectionConfig =
    & RedisClientConfig
    & Pick<
        RedisSubscribeConnectionConfig,
        'keepaliveMs' | 'livenessMs' | 'retryBaseMs' | 'retryMaxMs'
    >

/**
 * A cross-process broadcast driver over Redis pub/sub.
 *
 * @example
 * ```ts
 * const driver = RedisBroadcastDriver.fromConfig(
 *   { hostname: 'localhost' },
 *   { prefix: 'myapp', control: { secret: Deno.env.get('REALTIME_SECRET')! } },
 * )
 * ```
 */
export class RedisBroadcastDriver implements BroadcastDriver {
    private readonly prefix: string
    /**
     * The per-pattern seams when the injected subscriber offers BOTH, else
     * `undefined` — the single guard (#295).
     */
    readonly #perChannel: PerChannelSubscriber | undefined
    /**
     * The delivery decoder built by {@link onMessage}, installed per hosted
     * channel by {@link watchChannel}.
     *
     * One closure for every subscription, deliberately: a per-channel closure
     * could capture the channel and pass it to the handler, which would make
     * the topic-derived attribution below dead code.
     */
    #deliver: ((topic: string, payload: string) => void) | undefined
    /** The per-deployment MAC secret bytes, or `undefined` when unconfigured. */
    private readonly secret: Uint8Array<ArrayBuffer> | undefined
    /** This instance's identity — tags roster entries and control-message origin. */
    private readonly instanceId: string = crypto.randomUUID()
    private readonly livenessTtlSeconds: number
    private readonly heartbeatIntervalMs: number
    private readonly reconcileIntervalMs: number
    private readonly revocationTtlSeconds: number
    /**
     * The anti-replay window (#272) — the single home for whether a control
     * frame is fresh, whether it has been seen, and what makes two frames the
     * same frame. Absent when no control secret is configured, because the
     * control plane is then refused at both ends anyway.
     */
    private readonly replayWindow: ControlReplayWindow | undefined
    /** The control-payload byte ceiling, enforced on BOTH publish and ingest. */
    private readonly maxControlPayloadBytes: number
    /** #318 — notified whenever a control frame is declined. */
    private controlRefusedHandler?: (refusal: ControlRefusal) => void
    private heartbeatTimer?: ReturnType<typeof setInterval>
    /**
     * The ONE pending ghost-sweep timer (#355): a one-shot timeout armed by
     * {@link #armReconcile} and nowhere else, never an interval.
     */
    private reconcileTimer?: ReturnType<typeof setTimeout>
    /**
     * The ghost-sweep pass in flight, if any (#355). Stored and cleared by
     * {@link #armReconcile} alone; {@link close} awaits it.
     */
    #reconcilePass?: Promise<void>
    /**
     * Set by {@link close}, never cleared (#355). Read synchronously by
     * {@link #armReconcile} and {@link #ensureSweepStarted} before arming a
     * timer.
     */
    #closing = false
    /**
     * The ONE pending revocation timer (#359): a one-shot timeout armed by
     * {@link #armRevocationReconcile} and nowhere else, never an interval.
     */
    private revocationTimer?: ReturnType<typeof setTimeout>
    /**
     * The revocation pass in flight, if any (#359): its trigger, its start on
     * {@link #passClock} (#362), and the `ZSCAN` pages
     * {@link listRevocations} has read for it so far (#360) — `pages` is its
     * one mutable member. Stored and cleared by {@link #startRevocationPass}
     * alone, which builds it once, at the start. No promise is kept, because
     * nothing awaits a pass; the enforcement deadline reads this record when
     * it expires, to name the pass that has not settled.
     */
    #revocationPass?: RevocationPassRecord
    /**
     * The one trailing pass a reconnect or retry recorded while a pass was in
     * flight (#359): one slot, not a queue, and `reconnect` wins over
     * `reconnect-retry`. Consumed by the pass's `finally`; cleared by
     * {@link close}.
     */
    #revocationRerun?: 'reconnect' | 'reconnect-retry'
    /**
     * The ONE retry a failed seam-triggered reconcile gets (#308).
     *
     * At most one exists: the retry itself never retries, so a broker that
     * keeps failing costs one extra round-trip per outage rather than a loop.
     */
    private revocationRetryTimer?: ReturnType<typeof setTimeout>
    /**
     * The ONE pending retry of a failed floor announce (#380), armed by
     * {@link #announceFloor} alone and cleared by {@link close}. Unref'd.
     */
    #announceRetry?: ReturnType<typeof setTimeout>
    private sweepStarted = false
    /**
     * The owning instance's revocation re-check (S1/FR-014). Registered by the
     * manager via {@link onRevocationReconcile}; absent until then, so a driver
     * used without a manager reconciles nothing. Its cadence is the DEDICATED
     * {@link revocationTimer} — deliberately independent of the presence
     * ghost-sweep, which only starts once this instance hosts a presence member,
     * so a presence-free deployment still recovers a lost evict (FR-014).
     * What it resolves to is decoded by {@link decodeRevocationTally} (#384).
     */
    private revocationHandler?: () =>
        | RevocationTally
        | void
        | Promise<RevocationTally | void>
    /**
     * The revocation enforcement deadline (#362): says so, once per episode,
     * when no pass has completed within `revocationTtlSeconds` of the last
     * success's start. Moved at three sites only — see
     * {@link #startRevocationPass}.
     */
    readonly #deadline: EnforcementDeadline
    /**
     * The broker's reap time `t`, in seconds, of the last COMPLETED
     * revocation enumeration (#362 S1). Its only writer is
     * {@link listRevocations}; the pass's end site hands it to the deadline's
     * broker-clock check. A pass that throws mid-enumeration leaves it alone.
     */
    #lastReadAt?: number
    /**
     * The one handler completed passes report to (#360), registered through
     * {@link onPassComplete}. Re-registration replaces it. It is read when a
     * pass ENDS, never captured at its start, and {@link close} drops it —
     * though what decides that a closing driver reports nothing is the
     * `#closing` gate, not this drop.
     */
    #passCompleteHandler?: (sample: PassSample) => void
    /**
     * The ghost sweep in flight, if any (#360): its start on
     * {@link #passClock}, and the `SSCAN` pages {@link #sweepOwned} has read
     * for it so far across every dead instance. A Temporary Field — set only
     * while a sweep is in flight — kept for parity with
     * {@link #revocationPass}, so both passes count pages the same way.
     * Stored and cleared by {@link #armReconcile} alone. Since #384 it also
     * counts the dead instances {@link #sweepInstance} was called for, and
     * how many of those sweeps failed.
     */
    #sweepPass?: {
        readonly startedAt: number
        pages: number
        /** Dead instances swept so far (#384). */
        attempts: number
        /** Of those, the sweeps that threw (#384). */
        failures: number
    }
    /**
     * The owning instance's departure announcer (#348), registered by the
     * manager via {@link onRosterDeparture}. ONE handler: re-registration
     * replaces it and {@link close} drops it. Only {@link #announceSwept}
     * calls it.
     */
    #departureHandler?: (departure: RosterDeparture) => void | Promise<void>
    /**
     * When the owner's lapse handler runs, and how it stops (#349): the
     * handler registered through {@link onRosterLapse}, one run in flight, one
     * trailing run, the abort. A failed run marks the lapse suspected, so the
     * next successful heartbeat runs it again. Closed by {@link close}.
     */
    #lapse = new LapseRun(() => {
        this.#lapseSuspected = true
    })
    /**
     * Set once this driver has issued a hold's `EVAL` (#349), never cleared.
     * Written only by {@link holdMember}, read only by {@link #heartbeat}'s
     * tail: before any hold, a nil or a failed beat carries no lapse, because
     * nothing of this instance's can have been swept.
     */
    #holdIssued = false
    /**
     * A beat failed, or a lapse run failed, after a hold was issued (#349): a
     * lapse may have gone unseen, so the next successful beat runs the lapse
     * handler whatever its reply says. Written by {@link #heartbeat}'s tail
     * and by {@link #lapse}'s failure callback; consumed only by the tail.
     */
    #lapseSuspected = false
    /**
     * When the owed-release drain runs, and how it stops (#371): the handler
     * registered through {@link onRosterMaintenance}, one run in flight, one
     * trailing run, none once {@link close} has begun. Unlike {@link #lapse}
     * it takes no `AbortSignal` — its handler's contract is "no arguments" —
     * so `close()` only refuses a new run and waits for one already in
     * flight.
     */
    #rosterMaintenance = new RosterMaintenanceRun()
    /**
     * Resources this driver constructed itself (via {@link fromConfig}) and is
     * therefore responsible for closing. Empty when the ports were injected — a
     * test owns and closes its own fakes, so {@link close} then only stops the
     * sweep timers.
     */
    private owned: readonly Closeable[] = []

    /**
     * @param command - The command client used to `PUBLISH` and hold roster state.
     * @param subscriber - The subscribe-mode connection pushing messages.
     * @param options - The reserved prefix, control secret, and sweep tuning.
     * @throws {Error} When a control secret is supplied but is shorter than
     *   {@link MIN_CONTROL_SECRET_BYTES} bytes — a weak key would let a peer
     *   forge an authentic-looking control frame (FR-015).
     * @throws {Error} When `control.windowMs` or `control.maxPayloadBytes` is
     *   not a positive, finite value, or `control.maxEntries` is not a positive
     *   INTEGER — each bounds a cost paid on every ingest, and a zero, negative
     *   or fractional bound is a misconfiguration that would disable the check
     *   rather than tighten it.
     * @throws {Error} When `prefix` is empty, or contains a Redis glob
     *   metacharacter (`*`, `?`, `[`, `]`, or a backslash) — an empty prefix
     *   namespaces nothing, and a glob one is `startsWith`-anchored while its
     *   subscribe pattern reaches into other deployments (#282).
     * @throws {Error} When `presence.reconcileIntervalMs` is not a finite
     *   number of at least 1 ms, or `revocationTtlSeconds` is not a whole
     *   number of seconds whose deadline fits one timer — the message says
     *   `out of range` (#362).
     * @throws {Error} When `presence.reconcileIntervalMs` is more than HALF of
     *   `revocationTtlSeconds`: a lost revocation's record could expire before
     *   a pass applies it (#362). The configuration paragraph of
     *   `docs/realtime.md` states the relation.
     */
    constructor(
        private readonly command: RedisCommandClient,
        private readonly subscriber: RedisSubscriber,
        options: RedisBroadcastDriverOptions = {},
    ) {
        this.prefix = options.prefix ?? 'lockness:realtime'
        assertUsablePrefix(this.prefix)
        // ONE feature-detect, at construction, for the PAIR (#295). Detecting
        // the two members at their call sites is how a subscriber that can
        // subscribe per pattern but not unsubscribe ends up accumulating one
        // permanent subscription per channel ever hosted — monotonic over the
        // process lifetime, strictly worse than the single glob it replaces,
        // and invisible, because delivery stays correct.
        this.#perChannel = typeof this.subscriber.subscribeOne === 'function' &&
                typeof this.subscriber.unsubscribeOne === 'function'
            ? this.subscriber as PerChannelSubscriber
            : undefined
        if (options.control?.secret !== undefined) {
            const bytes = new TextEncoder().encode(options.control.secret)
            if (bytes.length < MIN_CONTROL_SECRET_BYTES) {
                throw new Error(
                    'realtime: the control secret must be at least ' +
                        `${MIN_CONTROL_SECRET_BYTES} bytes (FR-015) — got ` +
                        `${bytes.length}. Use a high-entropy value, e.g. ` +
                        `Deno.env.get('REALTIME_SECRET').`,
                )
            }
            this.secret = bytes
        } else {
            this.secret = undefined
        }
        this.livenessTtlSeconds = options.presence?.livenessTtlSeconds ??
            DEFAULT_LIVENESS_TTL_SECONDS
        this.heartbeatIntervalMs = options.presence?.heartbeatIntervalMs ??
            DEFAULT_HEARTBEAT_INTERVAL_MS
        // THE TWO ARE NOT INDEPENDENT (#293). The heartbeat is what keeps this
        // instance's own `{prefix}:alive:<id>` key alive, and that key's TTL is
        // `livenessTtlSeconds`. Beat slower than the TTL and a HEALTHY, running
        // instance lets its own key lapse between beats: every peer's
        // `#reconcile` then reads `EXISTS` 0 for it and sweeps its presence
        // members out of the roster — repeatedly, for as long as it runs.
        // Connected users vanish from every presence channel while their
        // sockets stay open, and nothing in the log looks wrong.
        //
        // TWO beats per window, not one. A beat landing exactly at the boundary
        // races the expiry, and it loses whenever the round-trip is slower than
        // the slack — which is exactly when the broker is under load.
        //
        // Finiteness first, for the reason `control.windowMs` gives below:
        // every comparison against NaN is false, so a
        // `Number(Deno.env.get('...'))` on an unset variable would slip past
        // the relation and disable this guard silently on a fresh process.
        if (
            !Number.isFinite(this.heartbeatIntervalMs) ||
            this.heartbeatIntervalMs <= 0 ||
            !Number.isFinite(this.livenessTtlSeconds) ||
            this.livenessTtlSeconds <= 0
        ) {
            throw new Error(
                'realtime: presence.heartbeatIntervalMs and ' +
                    'presence.livenessTtlSeconds must both be positive, finite ' +
                    `numbers (#293) — got heartbeatIntervalMs=${this.heartbeatIntervalMs} ` +
                    `and livenessTtlSeconds=${this.livenessTtlSeconds}.`,
            )
        }
        if (this.heartbeatIntervalMs * 2 > this.livenessTtlSeconds * 1000) {
            throw new Error(
                'realtime: presence.heartbeatIntervalMs must be at most HALF ' +
                    'of presence.livenessTtlSeconds, so a running instance ' +
                    `beats at least twice per TTL window (#293) — got ` +
                    `heartbeatIntervalMs=${this.heartbeatIntervalMs}ms and ` +
                    `livenessTtlSeconds=${this.livenessTtlSeconds}s ` +
                    `(${this.livenessTtlSeconds * 1000}ms). At one beat per ` +
                    'window a healthy instance races its own liveness-key ' +
                    'expiry, and its peers sweep its presence members out of ' +
                    'every roster while it is still serving those sockets.',
            )
        }
        // THE RELATION IS NOT A CEILING (#381). `livenessTtlSeconds` has no
        // upper bound, so a large TTL admits an interval no timer can hold, and
        // Deno replaces such a delay with 1 ms: the heartbeat then renews the
        // liveness key every millisecond, the opposite of what a long interval
        // asked for. `> MAX_TIMER_MS`, not `>=`: the ceiling itself still waits.
        if (this.heartbeatIntervalMs > MAX_TIMER_MS) {
            throw new Error(
                'realtime: presence.heartbeatIntervalMs is above the timer ' +
                    `ceiling of ${MAX_TIMER_MS}ms (#381) — got ` +
                    `heartbeatIntervalMs=${this.heartbeatIntervalMs}ms and ` +
                    `livenessTtlSeconds=${this.livenessTtlSeconds}s. A longer ` +
                    'delay does not wait longer: it fires after 1 ms, so the ' +
                    'heartbeat would renew the liveness key back to back ' +
                    'against the broker. Lower the interval.',
            )
        }
        this.reconcileIntervalMs = options.presence?.reconcileIntervalMs ??
            DEFAULT_RECONCILE_INTERVAL_MS
        this.revocationTtlSeconds = options.revocationTtlSeconds ??
            DEFAULT_REVOCATION_TTL_SECONDS
        // Ranges first, the relation second — the #293 shape above. A NaN,
        // zero, negative or infinite interval reaches `setTimeout` as 0 ms and
        // re-arms both the revocation pass and the ghost sweep back to back,
        // and a NaN TTL passes every comparison the relation makes. The TTL is
        // a whole number of seconds whose deadline fits ONE timer
        // (`MAX_TIMER_MS`); the interval's own ceiling follows from the
        // relation. A fractional interval stays legal.
        if (
            !Number.isFinite(this.reconcileIntervalMs) ||
            this.reconcileIntervalMs < 1 ||
            !Number.isSafeInteger(this.revocationTtlSeconds) ||
            this.revocationTtlSeconds < 1 ||
            this.revocationTtlSeconds > MAX_REVOCATION_TTL_SECONDS
        ) {
            throw new Error(
                'realtime: presence.reconcileIntervalMs or ' +
                    'revocationTtlSeconds is out of range (#362) — got ' +
                    `presence.reconcileIntervalMs=${this.reconcileIntervalMs}ms ` +
                    `and revocationTtlSeconds=${this.revocationTtlSeconds}s. ` +
                    'The interval must be a finite number of at least 1 ms, ' +
                    'and the TTL a whole number of seconds from 1 to ' +
                    `${MAX_REVOCATION_TTL_SECONDS}. An interval out of range ` +
                    'fires at once, so both the revocation pass and the ghost ' +
                    'sweep re-arm back to back against the broker.',
            )
        }
        // TWO passes per record lifetime (#362): at any wider interval, one
        // failed pass lets a lost revocation's record expire before the next
        // pass can apply it. It also leaves the enforcement deadline at least
        // half a TTL of headroom over healthy passes — the bound itself lives
        // on `onRevocationReconcile`, below.
        if (this.reconcileIntervalMs * 2 > this.revocationTtlSeconds * 1000) {
            throw new Error(
                'realtime: presence.reconcileIntervalMs must be at most HALF ' +
                    'of revocationTtlSeconds (#362) — got ' +
                    `presence.reconcileIntervalMs=${this.reconcileIntervalMs}ms ` +
                    `and revocationTtlSeconds=${this.revocationTtlSeconds}s ` +
                    `(${this.revocationTtlSeconds * 1000}ms). A revocation ` +
                    'whose control frame was lost could expire before a ' +
                    'revocation pass applies it. Lower the interval or raise ' +
                    'the TTL. See https://github.com/locknessland/lockness-monorepo/issues/362 ' +
                    'and the enforcement bound on onRevocationReconcile in ' +
                    'packages/realtime/drivers/redis.ts.',
            )
        }
        // The window is built only when a control secret exists: without one
        // the control plane refuses to publish and refuses to verify, so there
        // is nothing to remember. The clock is supplied HERE, once — the class
        // requires it rather than defaulting, so production and tests share one
        // path through the seam.
        const windowMs = options.control?.windowMs ?? DEFAULT_CONTROL_WINDOW_MS
        // Validated at boot, like the secret above it. `NaN` is the dangerous
        // one and it is easy to produce — `Number(Deno.env.get('...'))` on an
        // unset variable — because `Math.abs(x) > NaN` is false for every
        // frame, which silently disables the freshness check and quietly
        // restores the pre-#272 posture on a fresh process. Zero or negative
        // does the opposite and drops every frame.
        if (!Number.isFinite(windowMs) || windowMs <= 0) {
            throw new Error(
                'realtime: control.windowMs must be a positive, finite number ' +
                    `of milliseconds (#272) — got ${windowMs}. A NaN here ` +
                    'disables the anti-replay freshness check silently.',
            )
        }
        const maxPayloadBytes = options.control?.maxPayloadBytes ??
            DEFAULT_MAX_CONTROL_PAYLOAD_BYTES
        if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
            throw new Error(
                'realtime: control.maxPayloadBytes must be a positive, finite ' +
                    `byte count (#272) — got ${maxPayloadBytes}.`,
            )
        }
        this.maxControlPayloadBytes = maxPayloadBytes
        const maxEntries = options.control?.maxEntries
        if (
            maxEntries !== undefined &&
            (!Number.isInteger(maxEntries) || maxEntries < 1)
        ) {
            throw new Error(
                'realtime: control.maxEntries must be a positive integer ' +
                    `entry count (#283) — got ${maxEntries}.`,
            )
        }
        this.replayWindow = this.secret === undefined
            ? undefined
            : new ControlReplayWindow({
                windowMs,
                now: () => this.now(),
                maxEntries,
            })
        // Built only once both timing checks above have passed, so a refused
        // configuration never holds a timer.
        this.#deadline = new EnforcementDeadline({
            ttlMs: this.revocationTtlSeconds * 1000,
            now: () => this.#passClock(),
            inFlight: () => this.#revocationPass,
        })
    }

    /**
     * This instance's clock, in epoch milliseconds — the single home for the
     * time a control frame is stamped with and checked against. Two direct
     * `Date.now()` calls, one on publish and one on verify, would be two clocks
     * that must agree.
     */
    private now(): number {
        return Date.now()
    }

    /**
     * The pass clock (#362): the ONE monotonic reading intervals are measured
     * on — a revocation pass's start and end, a ghost sweep's start and end
     * (#360), and the enforcement deadline's `now`. Not {@link now}: that is
     * the control-frame stamp clock, an epoch reading, and a wall-clock step
     * would corrupt an interval measured on it.
     */
    #passClock(): number {
        return performance.now()
    }

    /**
     * Construct a driver whose command client and subscribe-mode connection are
     * built INTERNALLY from one Redis connection config (FR-012).
     *
     * This is the production path and the decision-table home for "queue-mirror
     * construction": it mirrors `@lockness/queue`'s `new RedisClient(config)` in
     * `packages/queue/manager.ts`. Both connections are lazy — the `RedisClient`
     * dials on its first command and the `RedisSubscribeConnection` on its first
     * `psubscribe` — so this opens no socket. Call {@link close} to release both.
     *
     * @param config - The Redis connection settings (`hostname` required).
     * @param options - The reserved prefix, control secret, and sweep tuning.
     * @returns A driver that owns and will close the two connections it built.
     * @throws {Error} When a control secret is supplied but is shorter than
     *   {@link MIN_CONTROL_SECRET_BYTES} bytes (FR-015).
     * @example
     * ```ts
     * const driver = RedisBroadcastDriver.fromConfig(
     *   { hostname: 'localhost', port: 6379 },
     *   { prefix: 'myapp', control: { secret: Deno.env.get('REALTIME_SECRET')! } },
     * )
     * // …later
     * await driver.close()
     * ```
     */
    static fromConfig(
        config: RedisBroadcastConnectionConfig,
        options: RedisBroadcastDriverOptions = {},
    ): RedisBroadcastDriver {
        const command = new RedisClient(config)
        const subscriber = new RedisSubscribeConnection(config)
        const driver = new RedisBroadcastDriver(command, subscriber, options)
        // Close the subscribe socket before the command socket: stop draining
        // pushes, then drain the command queue's QUIT.
        driver.owned = [subscriber, command]
        return driver
    }

    /**
     * Everything an event topic has before the channel — the **single
     * production home** of the `__event:` separator (#288).
     *
     * All three consumers read it: {@link topic} builds a `PUBLISH` argument
     * from it, {@link onMessage} builds its `PSUBSCRIBE` pattern by appending
     * `*`, and the same method strips exactly `this.eventTopicPrefix.length`
     * characters to recover the channel.
     *
     * **A prefix, not a topic, and that is the point.** Homing the decision in
     * `topic(channel)` would leave `onMessage` needing two values that method
     * does not return — the pattern and the strip length — reachable only via
     * `topic('*')` or a recomputed length. `topic('*')` is the shape to avoid
     * for a second reason: `PUBLISH` is a **literal** context and `PSUBSCRIBE`
     * a **pattern** context, so one builder serving both means any future
     * escaping inside `topic()` silently corrupts the subscription instead of
     * failing loudly.
     */
    private get eventTopicPrefix(): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}event:`
    }

    /** The reserved topic for a channel's events — a PUBLISH argument. */
    private topic(channel: string): string {
        return `${this.eventTopicPrefix}${channel}`
    }

    /**
     * The reserved topic for a channel's events, as a **PSUBSCRIBE pattern**.
     *
     * The same bytes as {@link topic} today, and a separate method on purpose.
     * `PUBLISH` is a literal context and `PSUBSCRIBE` a pattern one — the
     * distinction {@link eventTopicPrefix}'s docstring records — so one builder
     * serving both means the first escaping ever added to `topic()` silently
     * corrupts the subscription instead of failing loudly. Calling `topic()`
     * here is the shortcut to refuse: it returns the right string today, which
     * is precisely what makes it invisible later.
     *
     * It is glob-safe because a channel reaching this point has passed
     * `ChannelManager`'s `#assertUsableChannel` (#314), and `NAME_RE` excludes
     * every Redis glob metacharacter. That guarantee lives in another package
     * and nothing here re-checks it — see the `watchChannel` docstring.
     */
    private eventPattern(channel: string): string {
        return `${this.eventTopicPrefix}${channel}`
    }

    /**
     * The reserved control topic — the single home for the control-topic name
     * (#268 §5).
     *
     * Its separator begins with {@link RESERVED_SEPARATOR_LEAD}, which is what
     * keeps it unreachable from any event pattern — this deployment's own
     * included — so a control frame is delivered only via {@link onControl},
     * never through {@link onMessage}, and the MAC check cannot be skipped by
     * routing.
     *
     * Until #288 the stated reason was "uses `__control` WITHOUT the `:`
     * separator so it never matches the `${prefix}:*` event pattern". That was
     * true of THIS topic and false as a general rule: the event pattern's own
     * `:` separator let it reach a nested deployment's control topic. The rule
     * now lives at {@link RESERVED_SEPARATOR_LEAD} and covers both.
     */
    private get controlTopic(): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}control`
    }

    private presenceKey(channel: string): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}presence:${channel}`
    }

    /**
     * The holders hash of one roster slot: `instanceId → entry JSON` (#345).
     *
     * **Unambiguous only because a channel carries no space.** The key joins
     * channel and member id with {@link OWNED_SEP}, the separator the owned set
     * already splits on; `ChannelManager`'s `#assertUsableChannel` refuses a
     * channel containing one, so `a b` + `c` and `a` + `b c` cannot share a
     * key. Weaken that check and two slots share one holders hash.
     *
     * @param channel - The presence channel.
     * @param id - The member id naming the slot.
     * @returns The holders hash key.
     */
    private holdersKey(channel: string, id: string | number): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}holders:${channel}${OWNED_SEP}${
            String(id)
        }`
    }

    private ownedKey(instanceId: string): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}owned:${instanceId}`
    }

    private aliveKey(instanceId: string): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}alive:${instanceId}`
    }

    private get instancesKey(): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}instances`
    }

    /**
     * The revocation index: a sorted set, member = connection id, **score = the
     * epoch second the revocation expires** (#276).
     *
     * **A new key name, and the reason is now historical.** #276 could not
     * reuse the pre-existing `{prefix}:revoked`, which was a SET: an old
     * instance's `SADD` against a sorted set raises `WRONGTYPE` inside
     * `evict()`, whose first await is untried, so the error would reach the
     * caller and the local revoke would never run. #278 removed the last read
     * of that SET, so nothing here addresses it any more — the name stays
     * because renaming a live key buys nothing.
     */
    private get revocationIndexKey(): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}revocations`
    }

    /**
     * The revocation floor (#380): a sorted set, member = one live reader's
     * `revocationTtlSeconds`, **score = the epoch second (broker `TIME`) that
     * entry lapses**. Written only through {@link FLOOR_WRITE} — by every reap
     * and by the first registration's announce — and read by every mark.
     *
     * **The key carries its own TTL**: its longest entry's TTL plus the index's
     * slack, armed and extended by the same two-call discipline as the index.
     * A fleet that stops leaves no floor behind, and a lapsed entry can never
     * inflate a later mark for longer than one of those TTLs.
     */
    private get revocationFloorKey(): string {
        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}revocation-floor`
    }

    /**
     * Publish a message to the channel's Redis topic.
     *
     * @param message - The message to broadcast.
     */
    async publish(message: BroadcastMessage): Promise<void> {
        await this.command.command(
            'PUBLISH',
            this.topic(message.channel),
            JSON.stringify({ event: message.event, data: message.data }),
        )
    }

    /**
     * Register the delivery handler and start the pattern subscription. Each
     * received payload is decoded back into a {@link BroadcastMessage} whose
     * channel is the topic with the reserved prefix stripped.
     *
     * @param handler - Called with each received message.
     */
    onMessage(handler: (message: BroadcastMessage) => void): void {
        const marker = this.eventTopicPrefix
        // THE DECODER IS BUILT ONCE and stored, because under per-channel
        // subscribe (#295) it is installed by `watchChannel` rather than here,
        // once per hosted channel. Building it per channel is what would let a
        // `watchChannel` closure capture `channel` and hand it to the handler —
        // and that implementation looks correct, right up to the point where
        // the `startsWith` check below becomes dead code and a later tidy-up
        // removes it. §5 row 11: the delivered TOPIC decides, always.
        this.#deliver = (topic: string, payload: string) => {
            // DENY BY DEFAULT. This used to fall back to `channel = topic` on a
            // shape mismatch, which turns a routing fault into a plausible,
            // charset-valid channel name that passes every check below and
            // reaches local fan-out under a name nobody chose. Unreachable from
            // a correct broker under a literal-anchored pattern — which is
            // exactly why it must not be the branch that decides anything.
            if (!topic.startsWith(marker)) {
                console.warn(
                    'realtime: dropped a Redis message whose topic does not ' +
                        "match this deployment's event marker — the " +
                        'subscription and the delivery disagree, which no ' +
                        'correct broker does (#288)',
                )
                return
            }
            // A FIXED-OFFSET SLICE, never `replace` or `split`. A channel name
            // may legally contain the separator (`NAME_RE` permits `_` and
            // `:`), so `replace(marker, '')` would corrupt a channel called
            // `__event:x` and `split(marker)[1]` would truncate it.
            const channel = topic.slice(marker.length)
            let parsed: { event?: unknown; data?: unknown }
            try {
                parsed = JSON.parse(payload)
            } catch {
                // #418: `#deliver` IS the subscriber's message handler — no
                // caller here wraps it in a `try`, so a throwing sink would
                // escape into whatever the port's concrete implementation
                // does with a handler fault, never this driver's own
                // containment. One marked line, through #guardedWarn's shared
                // #369 shape.
                this.#guardedWarn(
                    MESSAGE_DECODE_LOG_FAILED,
                    'realtime: dropped a malformed Redis payload',
                )
                return // a malformed payload is dropped, never a throw
            }
            // Re-validate names on ingest — a peer (or a poisoned topic) must
            // not inject an out-of-charset channel/event name into local fan-out.
            //
            // This is also what used to discard a nested deployment's CONTROL
            // frames, which arrive with no `event` field. It is now unreachable
            // for them — no event pattern can match any accepted prefix's
            // control topic, this deployment's own included — and that is the
            // point rather than a reason to remove it. Authenticity is decided
            // in ONE place, `#verifyAndDecode`; this check only ever asked.
            if (
                typeof parsed.event !== 'string' ||
                !isValidName(parsed.event) || !isValidName(channel)
            ) {
                console.warn(
                    'realtime: dropped a Redis message with an invalid name',
                )
                return
            }
            handler({ channel, event: parsed.event, data: parsed.data })
        }
        // NOTHING IS SUBSCRIBED HERE when the subscriber can do it per pattern
        // (FR-004). The manager's `watchChannel` calls create the
        // subscriptions, one per hosted channel; a driver whose subscriber
        // cannot unsubscribe keeps the prefix-wide glob, because watch-without-
        // unwatch is strictly worse than the glob it would replace.
        if (this.#perChannel) return
        this.subscriber.psubscribe(`${marker}*`, this.#deliver)
    }

    /**
     * Begin receiving `channel`'s events (#295) — one exact-topic subscription.
     *
     * @param channel - The channel this instance has begun hosting.
     * @returns Resolves once the subscribe frame is on the wire.
     * @throws {Error} If no delivery handler has been registered yet.
     */
    watchChannel(channel: string): void | Promise<void> {
        const sub = this.#perChannel
        if (!sub) return
        // FR-004 made "subscribed with no handler" REACHABLE, where it is
        // structurally impossible while `onMessage` does the subscribing. The
        // manager happens to call `onMessage` in its constructor and
        // `watchChannel` later, which is an ordering, not a contract — and a
        // subscription whose frames have nowhere to go is exactly the wasted
        // fan-out this feature exists to remove.
        const deliver = this.#deliver
        if (!deliver) {
            throw new Error(
                'realtime: watchChannel was called before onMessage, so this ' +
                    'subscription would have no handler. Register the delivery ' +
                    'handler first.',
            )
        }
        return sub.subscribeOne(this.eventPattern(channel), deliver)
    }

    /**
     * Stop receiving `channel`'s events (#295).
     *
     * A rejection here discards and reconnects the socket; the orphan clears
     * on that reconnect's re-issue (#372).
     *
     * @param channel - The channel this instance has stopped hosting.
     * @returns Resolves once the unsubscribe frame is on the wire.
     */
    unwatchChannel(channel: string): void | Promise<void> {
        return this.#perChannel?.unsubscribeOne(this.eventPattern(channel))
    }

    /**
     * Register the control-message handler and subscribe the reserved control
     * topic. Each received frame is decoded, its FR-015 MAC verified, and its
     * routing names re-validated BEFORE the handler is invoked; a frame that
     * fails any check — or that this instance published itself (self-loopback) —
     * is dropped and never reaches the handler.
     *
     * @param handler - Called with each **authenticated** control message.
     */
    onControl(handler: (control: ControlMessage) => void): void {
        const deliver = (_topic: string, payload: string) => {
            const control = this.#verifyAndDecode(payload)
            if (control) handler(control)
        }
        // DECLARED PRIORITY (#295/FR-023), where the subscriber supports it.
        //
        // A re-issue that throws half way subscribes a prefix of its set, and
        // which subscription lands first used to be decided by the order this
        // class happened to register its seams in — nothing stated it and no
        // test pinned it. This is the one that must survive: an evict frame
        // reaches this deployment only here, and #271/#308's revocation fast
        // path waits on this subscription and no other. Event delivery
        // resuming late is a latency cost; enforcement resuming late is not.
        const sub = this.#perChannel
        if (sub) {
            void Promise.resolve(
                sub.subscribeOne(this.controlTopic, deliver, {
                    priority: true,
                }),
            ).catch((error: unknown) => {
                // #395: this promise is `void`ed, so a throwing sink would
                // escape as an unhandled rejection. One marked line, through
                // #guardedWarn's shared #369 shape.
                this.#guardedWarn(
                    CONTROL_SUBSCRIBE_LOG_FAILED,
                    'realtime: the control subscription could not be ' +
                        "issued — the driver's own retry is what restores " +
                        `it: ${renderError(error)}`,
                    error,
                )
            })
            return
        }
        this.subscriber.psubscribe(this.controlTopic, deliver)
    }

    /**
     * Publish a control message to every instance's {@link onControl} seam,
     * attaching the FR-015 authenticity MAC. Refused with a WARN when no control
     * secret is configured — an unauthenticated control frame would be dropped by
     * every peer's ingest check anyway, so it is never emitted.
     *
     * @param control - The control message to broadcast (its `mac` is set here).
     */
    onControlRefused(handler: (refusal: ControlRefusal) => void): void {
        this.controlRefusedHandler = handler
    }

    /**
     * WARN, then notify the seam. Both, always — the log is what an operator
     * reading one instance finds, the seam is what anything aggregating across
     * instances can act on, and neither replaces the other.
     *
     * The handler is application code on a path whose whole point is that a
     * failure here is already being swallowed, so a throw from it is contained
     * and logged rather than propagated: turning an observability callback into
     * the caller's exception would make publishing MORE fragile than before the
     * seam existed.
     */
    #refuseControl(refusal: ControlRefusal, message: string): void {
        console.warn(message)
        try {
            this.controlRefusedHandler?.(refusal)
        } catch (error) {
            console.warn(
                'realtime: an onControlRefused handler threw; the refusal ' +
                    `itself is unaffected: ${renderError(error)}`,
            )
        }
    }

    /**
     * Register the handler each completed background pass reports to (#360):
     * one frozen {@link PassSample} per ghost sweep and per revocation
     * re-check, taken at the pass's end — the value an application forwards
     * to its metrics backend. The driver records it and never judges it: no
     * threshold, no slow-pass log.
     *
     * **One handler**: registering again replaces it. The handler registered
     * when a pass ENDS is the one called, so a handler registered mid-pass
     * receives that pass's sample. {@link close} drops it, and nothing is
     * delivered once `close()` has begun. Samples are not buffered: a pass
     * that ends with no handler registered builds nothing and reports to no
     * one.
     *
     * **It is never awaited, and it cannot break a pass.** A throw, or a
     * returned promise that rejects, is one WARN ({@link PASS_SAMPLE_FAILED});
     * a returned promise that never settles holds nothing. The next pass runs
     * either way.
     *
     * Redis-only: the memory driver runs no background pass, so this is not a
     * {@link BroadcastDriver} member.
     *
     * @param handler - Called once per completed pass, with its sample.
     * @example
     * ```ts
     * const failed = { sweep: 0, revocation: 0 }
     * const unitFailures = { sweep: 0, revocation: 0 }
     * driver.onPassComplete((sample) => {
     *     if (sample.outcome === 'failed') failed[sample.pass]++
     *     unitFailures[sample.pass] += sample.failures ?? 0
     * })
     * ```
     */
    onPassComplete(handler: (sample: PassSample) => void): void {
        this.#passCompleteHandler = handler
    }

    /**
     * Build one {@link PassSample} and hand it to the registered handler —
     * **the one place a sample is made** (#360).
     *
     * **One sample per completed pass, taken at the pass's one end site and
     * nowhere else.** This method has exactly two callers, and each is its
     * pass's one end site: the revocation pass's `finally` in
     * {@link #startRevocationPass}, after the enforcement-deadline call, and
     * the ghost sweep's `finally` in {@link #armReconcile}, after the re-arm.
     * **Every argument comes from the start site's closure** — never from
     * {@link #revocationPass} or {@link #sweepPass}: the revocation end site
     * starts a trailing pass before it samples, and the field then holds the
     * trailing pass, not the one that ended.
     *
     * In order: nothing once {@link close} has begun — the one asker of that
     * rule for samples; nothing for a `closed` pass, or with no handler
     * registered, before anything is built; then one frozen sample, its
     * duration clamped at 0, handed to the handler **without awaiting it**.
     * The call and the adoption of what it returned share one `try`: a
     * synchronous throw (including one from adopting a hostile returned
     * promise) and a rejection are each one WARN ({@link #warnPassSample}). A
     * returned thenable is adopted once, through `Promise.resolve`, so it
     * rejects at most once; one that never settles holds nothing.
     *
     * **The counts ride the same rule** (#384): `attempts` and `failures` come
     * from the start site's record, never from a field, and the sample
     * carries both keys only when both are defined — otherwise neither, not
     * `undefined` values. What they mean is {@link PassSample}'s to say.
     *
     * @param pass - Which pass ended.
     * @param trigger - What started it.
     * @param outcome - How it ended.
     * @param startedAt - Its start, on {@link #passClock}.
     * @param endedAt - Its end, on {@link #passClock}.
     * @param pages - The pages it read.
     * @param attempts - The units it attempted, when known.
     * @param failures - How many of those failed, when known.
     */
    #emitPassSample(
        pass: PassSample['pass'],
        trigger: PassSample['trigger'],
        outcome: PassOutcome,
        startedAt: number,
        endedAt: number,
        pages: number,
        attempts?: number,
        failures?: number,
    ): void {
        if (this.#closing) return
        const handler = this.#passCompleteHandler
        if (outcome === 'closed' || handler === undefined) return
        const sample: PassSample = Object.freeze({
            pass,
            trigger,
            outcome,
            durationMs: Math.max(0, endedAt - startedAt),
            pages,
            ...(attempts !== undefined && failures !== undefined
                ? { attempts, failures }
                : {}),
        })
        try {
            const returned: unknown = handler(sample)
            Promise.resolve(returned).then(
                undefined,
                (failure: unknown) => this.#warnPassSample(failure),
            )
        } catch (failure) {
            this.#warnPassSample(failure)
        }
    }

    /**
     * Write the one {@link PASS_SAMPLE_FAILED} WARN for a handler that failed,
     * in the #369 shape, through {@link #guardedWarn}: when `console.warn`
     * itself throws, one {@link PASS_SAMPLE_LOG_FAILED} ERROR line carries
     * both halves instead, so the failure never escapes into a pass — and
     * that line never throws past itself either (#391).
     *
     * @param failure - What the handler threw, or rejected with.
     */
    #warnPassSample(failure: unknown): void {
        this.#guardedWarn(
            PASS_SAMPLE_LOG_FAILED,
            `${PASS_SAMPLE_FAILED} ${renderError(failure)}`,
            failure,
        )
    }

    /**
     * Write one self-guarded WARN, in the #369 shape (#409): `console.warn`
     * first; if the console itself throws, one marked fallback line through
     * {@link writeMarkedFallback} instead, which never throws past itself
     * (#391). **The one home of the pattern every self-guarded WARN in this
     * class shares** — {@link #warnFloor}, {@link #warnPassSample},
     * {@link #warnReconcileFailed}, {@link #warnMalformedTally},
     * {@link #warnCloseDrainExpired}, the control subscription's `.catch` in
     * {@link onControl}, the failure branch of {@link #heartbeat}, `onMessage`'s
     * `#deliver`, {@link #verifyAndDecode}, {@link #announceSwept} (both its
     * handler catch and its own `dropped` drop, #419),
     * {@link #parseRosterValue}'s `skipped` (#419), {@link #sweepInstance} and
     * {@link #reconcile}'s own catch (#418) — so a
     * caller whose promise is `void`ed, whose callback runs on a bare
     * interval, or whose call site is a handler the `RedisSubscriber` port
     * invokes with no `try` of its own, never calls `console.warn` unguarded
     * again.
     *
     * @param marker - The fixed prefix {@link writeMarkedFallback} writes
     *   verbatim when the WARN itself could not be.
     * @param line - The whole WARN line attempted on `console.warn`.
     * @param subject - What the marked fallback line renders in `line`'s
     *   place when the caller's own failure/error value, not the constructed
     *   line, is the right thing to render. Defaults to `line`.
     */
    #guardedWarn(
        marker: MarkedFallbackMarker,
        line: string,
        subject: unknown = line,
    ): void {
        try {
            console.warn(line)
        } catch (sink) {
            writeMarkedFallback(marker, subject, {
                label: 'sink failure',
                error: sink,
            })
        }
    }

    async publishControl(control: ControlMessage): Promise<void> {
        if (!this.secret) {
            this.#refuseControl(
                {
                    reason: 'no-secret',
                    kind: control.kind,
                    channel: control.channel,
                },
                'realtime: refusing to publish an unauthenticated control ' +
                    'message — no control secret configured (FR-015)',
            )
            return
        }
        const wire: ControlWire = {
            kind: control.kind,
            target: control.target,
            channel: control.channel,
            member: control.member,
            revocationId: control.revocationId,
            origin: this.instanceId,
            ts: this.now(),
            nonce: newControlNonce(),
        }
        wire.mac = this.#sign(wire)
        const payload = JSON.stringify(wire)
        // Enforced on PUBLISH as well as on ingest, and this half is the one
        // that matters operationally. Every receiver rejects an oversized frame
        // — so without this check an app whose `PresenceMember.info` grew past
        // the ceiling would publish happily, update the roster, and have every
        // remote instance silently drop the frame. The WARN would appear on the
        // instances that cannot fix it, and never on the one that can.
        if (payload.length > this.maxControlPayloadBytes) {
            this.#refuseControl(
                {
                    reason: 'oversize',
                    kind: control.kind,
                    channel: control.channel,
                    bytes: payload.length,
                    limit: this.maxControlPayloadBytes,
                },
                'realtime: refusing to publish an oversized control message ' +
                    `(${payload.length} bytes > ` +
                    `${this.maxControlPayloadBytes}). Every peer would drop ` +
                    'it, so this instance drops it here where the cause is ' +
                    'visible. Shrink the presence member, or raise ' +
                    'control.maxPayloadBytes on EVERY instance.',
            )
            // THROWS, where it used to `return` (#326). Refusing and then
            // reporting success is a lie to every caller: `evict` awaits this
            // and had no way to learn its frame was never sent. The refusal is
            // recorded first — `#refuseControl` logs and runs the
            // `onControlRefused` handler — so the drop is still observable to
            // an operator whichever way the caller handles this.
            //
            // Both presence announcements catch it and warn on purpose, in
            // `ChannelManager`'s `#announcePresence` (#344): a throw there
            // would reject a queued roster write. It is reachable there only
            // for a member admitted before this bound existed, because
            // `ChannelManager` now refuses an oversized member at admission
            // (#326).
            throw new Error(
                `realtime: control message of ${payload.length} bytes ` +
                    `exceeds control.maxPayloadBytes ` +
                    `(${this.maxControlPayloadBytes}); it was not published.`,
            )
        }
        await this.command.command('PUBLISH', this.controlTopic, payload)
    }

    /**
     * OPTIONAL (FR-005, #345). Hold the channel's roster slot for THIS
     * instance, with `member` as its entry, and start the instance-liveness
     * heartbeat if it is not already running. See {@link HOLD_MEMBER_SCRIPT}.
     *
     * **It arms lapse detection** (#349): just before its `EVAL`, after the
     * boot beat, it records that a hold was issued. From then on a nil or a
     * failed heartbeat may mean this instance's holds were swept; before it,
     * neither carries a lapse, because nothing could have been swept.
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member this instance holds the slot as.
     * @returns `arrived: true` iff no instance held the slot before.
     * @throws {Error} If the broker fails, or the script's reply is not 0 or 1.
     */
    async holdMember(
        channel: string,
        member: PresenceMember,
    ): Promise<RosterHold> {
        await this.#ensureSweepStarted()
        const entry: RosterEntry = { member, owner: this.instanceId }
        const field = String(member.id)
        // Set synchronously, just before the EVAL and after the boot beat
        // (#349 FR-003): from here on, a nil or a failed beat may mean this
        // hold was swept. Never at the method's entry — the boot beat's nil
        // would then count, before anything could have been swept.
        this.#holdIssued = true
        // ONE operation (#323, #345). The sweep start above is deliberately
        // outside it — it is this instance's liveness, not this member's hold.
        const reply = await this.command.command(
            'EVAL',
            HOLD_MEMBER_SCRIPT,
            '4',
            this.presenceKey(channel),
            this.holdersKey(channel, field),
            this.ownedKey(this.instanceId),
            this.instancesKey,
            field,
            this.instanceId,
            JSON.stringify(entry),
            `${channel}${OWNED_SEP}${field}`,
        )
        const decoded = decodeHoldReply(reply)
        this.#warnIfOwnedHealed(decoded.ownedKind)
        this.#warnIfInstancesHealed(decoded.instancesKind)
        return { arrived: decoded.arrived }
    }

    /**
     * OPTIONAL (FR-005, #345). Drop THIS instance's hold on the channel's
     * roster slot. The slot leaves the roster only with its last holder. See
     * {@link RELEASE_MEMBER_SCRIPT}.
     *
     * **Never reports a departure** through {@link onRosterDeparture} (#348):
     * its caller announces `gone` itself, and a report here would be a second
     * `left`.
     *
     * **The public answer is `gone` only** (#355): `emptied` is `true`,
     * `kept` and `absent` are `false`. A leave never asks for the liveness
     * check, so a `refused` reply is a defect and throws rather than reading
     * as "not gone".
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member whose slot this instance releases.
     * @returns `gone: true` iff this instance held the slot and none is left.
     * @throws {Error} If the broker fails, the script's reply is none of its
     *   four, or it is `refused`.
     */
    async releaseMember(
        channel: string,
        memberId: string | number,
    ): Promise<RosterRelease> {
        const outcome = await this.#release(
            channel,
            String(memberId),
            this.instanceId,
            false,
        )
        if (outcome.kind === 'refused') {
            throw new Error(
                'realtime: a leave was refused by the release script, but a ' +
                    'leave never asks for the liveness check',
            )
        }
        return { gone: outcome.kind === 'emptied' }
    }

    /**
     * Run {@link RELEASE_MEMBER_SCRIPT} for one slot on behalf of `releaserId`
     * — this instance on a leave, a dead instance on a sweep. One path, so a
     * leave and a sweep cannot disagree about what releasing means.
     *
     * The liveness check is asked for EXPLICITLY (#355): only
     * {@link #sweepOwned} passes `true`. It is never derived from
     * `releaserId !== this.instanceId`, and the key it checks is always
     * `releaserId`'s own, built here and nowhere else — the sweeper's key
     * would refuse every sweep release forever.
     *
     * @param channel - The presence channel.
     * @param field - The slot's member id, as a string.
     * @param releaserId - The instance whose hold is dropped.
     * @param onBehalf - `true` for a release on another process's behalf,
     *   which the script refuses while that process is alive.
     * @returns What the release did (see {@link decodeReleaseReply}).
     * @throws {Error} If the broker fails, or the reply is none of the four.
     */
    async #release(
        channel: string,
        field: string,
        releaserId: string,
        onBehalf: boolean,
    ): Promise<ReleaseOutcome> {
        const reply = await this.command.command(
            'EVAL',
            RELEASE_MEMBER_SCRIPT,
            '4',
            this.presenceKey(channel),
            this.holdersKey(channel, field),
            this.ownedKey(releaserId),
            this.aliveKey(releaserId),
            field,
            releaserId,
            `${channel}${OWNED_SEP}${field}`,
            onBehalf ? '1' : '0',
        )
        const outcome = decodeReleaseReply(reply)
        if (outcome.kind !== 'refused' && outcome.ownedKind !== undefined) {
            this.#warnIfOwnedHealed(outcome.ownedKind)
        }
        return outcome
    }

    /**
     * OPTIONAL (FR-005, #341). Read a bounded window of the channel's
     * authoritative roster: at most `limit` members, the hash's size, and the
     * entries of `selfIds` — in ONE `EVAL` (see {@link READ_ROSTER_SCRIPT}).
     *
     * On a room larger than `limit` the window is a random sample, a different
     * one per call; at or below `limit` it is the whole hash in its own order.
     * Only the client-visible member leaves this method — the entry's `owner`
     * stays internal (FR-018, S4). A self is returned only when its stored entry
     * names the id that was asked for (S3). An unparseable entry is skipped with
     * a WARN, so `members` can be shorter than `min(limit, total)`.
     *
     * @param channel - The presence channel.
     * @param limit - The most members to return; a positive integer.
     * @param selfIds - The member ids to return in `selves` when held; at most
     *   {@link MAX_ROSTER_READ_SELF_IDS}.
     * @returns The window, the population and the selves.
     * @throws {Error} Before any command, if `limit` is not a positive integer
     *   or `selfIds` is longer than {@link MAX_ROSTER_READ_SELF_IDS}; or if the
     *   broker's reply is not the script's shape.
     *
     * @example
     * ```ts
     * const { members, total, selves } = await driver.readRoster(
     *     'presence-room',
     *     100,
     *     [7],
     * )
     * ```
     */
    async readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): Promise<RosterWindow> {
        // Before any command (S2). A negative `HRANDFIELD` count returns
        // |count| pairs WITH repeats, and a non-integer is a broker error — the
        // manager validates its own call, but this seam is exported.
        if (!Number.isInteger(limit) || limit < 1) {
            throw new Error(
                `realtime: readRoster limit must be a positive integer, got ${limit}`,
            )
        }
        if (selfIds.length > MAX_ROSTER_READ_SELF_IDS) {
            throw new Error(
                `realtime: readRoster accepts at most ${MAX_ROSTER_READ_SELF_IDS} ` +
                    `self ids, got ${selfIds.length}`,
            )
        }
        const wanted = selfIds.map(String)
        const reply = asArray(
            await this.command.command(
                'EVAL',
                READ_ROSTER_SCRIPT,
                '1',
                this.presenceKey(channel),
                String(limit),
                '',
                ...wanted,
            ),
        )
        const total = asInteger(reply?.[0])
        const sample = asArray(reply?.[1])
        const stored = asArray(reply?.[2])
        if (total === undefined || !sample || !stored) {
            throw new Error(
                `realtime: the roster read for ${
                    safeForLog(channel)
                } returned an unexpected reply shape`,
            )
        }
        const members: PresenceMember[] = []
        // HRANDFIELD … WITHVALUES returns [field1, value1, field2, value2, …].
        for (let i = 1; i < sample.length; i += 2) {
            const member = this.#parseRosterValue(channel, asBulk(sample[i]))
            if (member) members.push(member)
        }
        // `stored[0]` answers the `''` padding field and is always nil. Slot i
        // answers the FIELD `wanted[i - 1]` — `HMGET` replies in field order,
        // by protocol — and is accepted only if the entry it holds CARRIES
        // that same id (FR-001, S3). Both must agree: an entry naming another
        // id is never somebody's self, not even the self of another id this
        // read also asked for, since only its own field could prove it is
        // theirs. A repeated id is returned once, as `RosterWindow` promises.
        const selves: PresenceMember[] = []
        const seen = new Set<string>()
        for (let i = 1; i < stored.length; i++) {
            const member = this.#parseRosterValue(channel, asBulk(stored[i]))
            if (!member) continue
            const id = String(member.id)
            if (id !== wanted[i - 1] || seen.has(id)) continue
            seen.add(id)
            selves.push(member)
        }
        return { members, total, selves }
    }

    /**
     * Parse one stored roster value to its client-visible member, or
     * `undefined` for an absent or malformed entry (logged at WARN).
     *
     * A fresh `{ id, info }` object is built rather than returning
     * `entry.member` as parsed, so nothing else stored beside the member — the
     * `owner`, or a field a future write adds — can reach a snapshot (S4).
     * It is deep-frozen before it is returned (#354): this is one of the
     * package's three mint sites, and the member is shared by every caller
     * of one barrier read.
     *
     * **The one decode of a roster entry**, for the roster read and for the
     * member a swept departure announces (#348). Its WARN names the channel
     * and a fixed reason only — never the entry's bytes — and is the ONLY
     * line a skipped entry logs, so a caller says what the skip cost through
     * `consequence` rather than with a WARN of its own.
     *
     * **Never throws** (#419): the WARN goes through {@link #guardedWarn}
     * ({@link ROSTER_ENTRY_LOG_FAILED}), so a throwing sink cannot escape
     * this method and abort whichever caller's loop reached it —
     * `readRoster`'s two loops, or `#announceSwept`'s single decode.
     *
     * @param channel - The channel the entry was read from, for the WARN.
     * @param value - The stored entry, or `undefined` when there was none.
     * @param consequence - What skipping the entry costs this caller, appended
     *   to the WARN (the sweep: the departure was not announced).
     */
    #parseRosterValue(
        channel: string,
        value: string | undefined,
        consequence?: string,
    ): PresenceMember | undefined {
        if (value === undefined) return undefined
        const skipped = (reason: string) =>
            // #419: `readRoster`'s two loops and `#announceSwept` all call
            // this method with no `try` of their own — so an unguarded
            // `console.warn` failing here escaped whichever of them called
            // it, aborting a `for` loop (`readRoster`) or `#sweepPage`'s
            // (through `#announceSwept`, the #418 T3 shape) before its
            // remaining entries or slots were ever reached. One marked
            // line, through #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                ROSTER_ENTRY_LOG_FAILED,
                `realtime: skipped a malformed roster entry on ${
                    safeForLog(channel)
                }: ${reason}${consequence ? ` — ${consequence}` : ''}`,
            )
        try {
            const entry = JSON.parse(value) as Partial<RosterEntry> | null
            const member = entry && typeof entry === 'object'
                ? entry.member
                : undefined
            // The join boundary's predicate (#346), so a roster entry is read
            // back exactly when its id could have been admitted. Entries a
            // 0.3.0 app wrote with a malformed id are skipped here, and
            // removed when their owner leaves.
            if (
                !member || typeof member !== 'object' ||
                !isPresenceMemberIdValue(member.id)
            ) {
                skipped('no member id')
                return undefined
            }
            // The join's `info` rule (#350), so an entry is read back only
            // with an `info` a join could have admitted. The TYPE label only,
            // never the value — `info` is application data. The key rule
            // is NOT applied: the reduction below drops extra keys, and a
            // skip would hide a 0.3.0 member for its whole session.
            if (!isPresenceMemberInfoValue(member.info)) {
                skipped(
                    `info of type ${typeLabel(member.info)} is not an object`,
                )
                return undefined
            }
            // Deep-frozen where it is minted (#354): one read is shared by
            // every caller of the #333 barrier, and a departure is handed
            // to the application's encoder.
            return freezePresenceMember(
                member.info === undefined
                    ? { id: member.id }
                    : { id: member.id, info: member.info },
            )
        } catch {
            // A FIXED reason, never the parser's message (#348 S2): V8's
            // `SyntaxError` quotes the input, and the input is the entry —
            // a member id such as an email address, and application `info`.
            skipped('not valid JSON')
            return undefined
        }
    }

    /**
     * Encode a {@link Revocation} as one index member.
     *
     * **The seam carries the domain fact; this method owns the bytes.** Nothing
     * outside this file may parse or construct one — the manager receives
     * `Revocation` records and never sees a member string, which is what keeps
     * the encoding from becoming normative on the public driver interface.
     *
     * **The id is part of the member, and that is the whole of #337's fix.**
     * Two revocations of one pair are two members, so `ZREM` of one exact
     * member is already a compare-and-delete: a clear can only name an id its
     * caller saw, and a newer write has an id nobody has seen yet. `ZADD GT`
     * always adds a new member — `GT` only governs an update — so the mark
     * script needs no change.
     *
     * Every part is re-asserted here even though the manager already asserted
     * them, because this is the last point at which an undecodable member can
     * be prevented rather than merely detected. A composite whose channel
     * carried a space would decode to a different channel — or to nothing.
     *
     * @param revocation - The revocation to encode.
     * @returns The sorted-set member.
     * @throws {Error} If any part is outside the supported charset.
     */
    #encodeRevocation(revocation: Revocation): string {
        const { target } = revocation
        if (!isValidName(target)) {
            throw new Error(
                `realtime: refusing to record a revocation for ${
                    safeForLog(target)
                } — the id is outside the supported charset, so the record ` +
                    'could never be matched to a live socket.',
            )
        }
        if (revocation.channel === undefined) return target
        const { channel, id } = revocation
        if (!isValidName(channel)) {
            throw new Error(
                `realtime: refusing to record a revocation on ${
                    safeForLog(channel)
                } — the channel is outside the supported charset, so the ` +
                    'composite record would not decode back to this channel.',
            )
        }
        if (!isValidName(id)) {
            throw new Error(
                `realtime: refusing to record a revocation with id ${
                    safeForLog(id)
                } — the id is outside the supported charset, so the ` +
                    'composite record would not decode back to this id.',
            )
        }
        return [target, channel, id].join(REVOCATION_SCOPE_SEPARATOR)
    }

    /**
     * Decode one index member back to a {@link Revocation}, or drop it.
     *
     * **This fails CLOSED, and it is the boundary rather than belt-and-braces.**
     * The revocation index is the only cross-instance write channel in this
     * package with no authenticity tag — control frames carry a MAC, this does
     * not — so a writer with bus access can put anything in it, and what comes
     * back here is handed almost directly to a revocation.
     *
     * The failure that matters is not "an unknown member is applied"; it is
     * **a channel-scoped member returned without its channel**, which the
     * manager then applies as a whole-connection revocation: hard-close 4403,
     * every other room gone. So a member that does not fully decode is
     * discarded, never returned with a partial scope, and never widened.
     *
     * **Exactly one part, or exactly three.** The two-part `"<target>
     * <channel>"` form was only ever written by unreleased builds of `main`
     * (#332 before #337); it carries no id, so it names no record a clear
     * could remove, and it is dropped like any other malformed member. It
     * expires on its score.
     *
     * @param member - The raw sorted-set member.
     * @returns The revocation, or `undefined` when the member does not fully
     *   decode.
     */
    #decodeRevocation(member: string): Revocation | undefined {
        const parts = member.split(REVOCATION_SCOPE_SEPARATOR)
        // EVERY part is charset-checked. An empty part (a doubled separator)
        // fails it, so a member is never re-split into a different record.
        if (!parts.every((part) => isValidName(part))) return undefined
        if (parts.length === 1) return { target: parts[0] }
        if (parts.length !== 3) return undefined
        const [target, channel, id] = parts
        return { target, channel, id }
    }

    /**
     * OPTIONAL (S1/FR-014). Durably record a revocation.
     *
     * The record is **one** sorted-set member whose score is the second it
     * expires (#276) — not a marker key plus a separate index entry, which were
     * two structures encoding one fact and could be made to disagree. It is
     * written by {@link MARK_REVOKED_SCRIPT} in a single operation, so there is
     * no window in which the connection is enumerable but not yet revoked.
     * Decision-table home: "whether a revoked connection stays revoked across a
     * reconnect".
     *
     * A **channel**-scoped record is the same member string with its channel
     * and its revocation id appended, space-separated (#332, #337) — the same
     * index, the same script, the same extend-only discipline. No new key, no
     * migration, no dual-write. Two revocations of one pair are two members.
     *
     * **Two round trips since #380, one write.** The mark first reads the
     * revocation floor and scores the record at the largest of its own
     * `revocationTtlSeconds` and every floor entry
     * ({@link decodeRevocationFloor}), so a record outlives the longest TTL
     * among the instances still passing. A floor that cannot be read fails
     * **closed**: the record is written at {@link MAX_REVOCATION_TTL_SECONDS}.
     * Any floor WARN is written after the record, through a helper that never
     * throws.
     *
     * **The index self-heals here too** (#411): {@link INDEX_HEAL} runs inside
     * the same `EVAL`, before the `ZADD` below, so a wrong-typed index no
     * longer fails the mark with `WRONGTYPE` — heal and write land in one
     * round trip. `decodeMarkReply` reads which prior kind it found;
     * `#warnIfIndexHealed` WARNs once when that is outside `zset`/`none`.
     *
     * @param revocation - What is revoked: a whole connection, or a connection
     *   in one channel.
     * @throws {Error} If the record's `EVAL` fails, or if either half of the
     *   record is outside the supported charset — never for the floor read.
     *   `ChannelManager` applies the revocation anyway and re-throws, so the
     *   caller learns durability was lost.
     */
    async markRevocation(revocation: Revocation): Promise<void> {
        const member = this.#encodeRevocation(revocation)
        let eff = MAX_REVOCATION_TTL_SECONDS
        let skipped = 0
        let unreadable: { error: unknown } | undefined
        try {
            const floor = decodeRevocationFloor(
                await this.command.command(
                    'ZRANGEBYSCORE',
                    this.revocationFloorKey,
                    '-inf',
                    '+inf',
                ),
                this.revocationTtlSeconds,
            )
            eff = floor.ttl
            skipped = floor.skipped
        } catch (error) {
            // Fail CLOSED (#380 S3): a floor that cannot be read never fails
            // the mark and never shortens the record — it is kept for the
            // longest lifetime instead, and one WARN says so after the write.
            unreadable = { error }
        }
        const reply = await this.command.command(
            'EVAL',
            MARK_REVOKED_SCRIPT,
            '1',
            this.revocationIndexKey,
            String(eff),
            member,
            String(eff + INDEX_TTL_SLACK_SECONDS),
        )
        this.#warnIfIndexHealed(decodeMarkReply(reply).kind)
        if (unreadable !== undefined) {
            this.#warnFloor(
                `${REVOCATION_FLOOR_READ_FAILED} ${
                    renderError(unreadable.error)
                }`,
            )
        }
        if (skipped > 0) {
            this.#warnFloor(`${REVOCATION_FLOOR_SKIPPED} ${skipped}`)
        }
    }

    /**
     * Write one revocation-floor WARN (#380), in the #391 shape, through
     * {@link #guardedWarn}: when `console.warn` itself throws, one
     * {@link REVOCATION_FLOOR_LOG_FAILED} ERROR line carries both halves
     * instead. It never throws, so a log sink can never fail a mark whose
     * record is already written, nor escape the announce, which no caller
     * awaits.
     *
     * @param line - The whole WARN line, constant first.
     */
    #warnFloor(line: string): void {
        this.#guardedWarn(REVOCATION_FLOOR_LOG_FAILED, line)
    }

    /**
     * WARN once when {@link FLOOR_WRITE} healed a wrong-typed floor key
     * (#405): `kind` is its `TYPE` read, taken BEFORE the heal's `DEL`. A
     * `zset` (the floor's own shape) or `none` (no key yet) means nothing was
     * healed, so nothing WARNs on the hot path — every other kind is this
     * key's Redis type just before this call deleted it.
     *
     * @param kind - The floor key's Redis type, as {@link FLOOR_WRITE} read it.
     */
    #warnIfFloorHealed(kind: string): void {
        if (kind === 'zset' || kind === 'none') return
        this.#warnFloor(`${REVOCATION_FLOOR_WRONG_TYPE} ${kind}`)
    }

    /**
     * WARN once when {@link INDEX_HEAL} healed a wrong-typed index key
     * (#411): `kind` is its `TYPE` read, taken BEFORE the heal's `DEL`. A
     * `zset` (the index's own shape) or `none` (no key yet) means nothing was
     * healed, so nothing WARNs on the hot path — every other kind is this
     * key's Redis type just before this call deleted it. Reuses
     * {@link #warnFloor}'s sink rather than a sibling: the #391 marked
     * fallback and its own log-failure line are shared machinery, not
     * floor-specific.
     *
     * @param kind - The index key's Redis type, as {@link INDEX_HEAL} read it.
     */
    #warnIfIndexHealed(kind: string): void {
        if (kind === 'zset' || kind === 'none') return
        this.#warnFloor(`${REVOCATION_INDEX_WRONG_TYPE} ${kind}`)
    }

    /**
     * WARN once when {@link OWNED_HEAL} healed a wrong-typed owned-set key
     * (#414): `kind` is its `TYPE` read, taken BEFORE the heal's `DEL`. A
     * `set` (the owned set's own shape) or `none` (no key yet) means nothing
     * was healed, so nothing WARNs on the hot path. Reuses
     * {@link #warnFloor}'s sink, the same #391 marked-fallback discipline —
     * never a sibling.
     *
     * @param kind - The owned set's Redis type, as {@link OWNED_HEAL} read it.
     */
    #warnIfOwnedHealed(kind: string): void {
        if (kind === 'set' || kind === 'none') return
        this.#warnFloor(`${OWNED_SET_WRONG_TYPE} ${kind}`)
    }

    /**
     * WARN once when {@link INSTANCES_HEAL} healed a wrong-typed instances-set
     * key (#414): `kind` is its `TYPE` read, taken BEFORE the heal's `DEL`. A
     * `set` (the instances set's own shape) or `none` (no key yet) means
     * nothing was healed, so nothing WARNs on the hot path. Reuses
     * {@link #warnFloor}'s sink — never a sibling.
     *
     * @param kind - The instances set's Redis type, as
     *   {@link INSTANCES_HEAL} read it.
     */
    #warnIfInstancesHealed(kind: string): void {
        if (kind === 'set' || kind === 'none') return
        this.#warnFloor(`${INSTANCES_SET_WRONG_TYPE} ${kind}`)
    }

    /**
     * OPTIONAL (S1/FR-014). One revocation pass's read half (#359): reap the
     * expired records, then read the index in bounded pages and return the
     * live, decodable records `owns` keeps. The contract an implementation
     * owes is {@link BroadcastDriver.listRevocations}'s; this is the Redis
     * shape of it.
     *
     * **One reap, one `now`, then pages.** {@link REAP_REVOKED_SCRIPT} is the
     * pass's only delete and answers the Redis second `t` it reaped against;
     * the index is then read with `ZSCAN <index> <cursor> COUNT`
     * {@link REVOCATION_SCAN_COUNT} — no other option — from cursor `'0'`
     * until the cursor comes back `'0'`: one full iteration, no budget, no
     * resume state. A record is live iff its score is strictly above the
     * carried `t`, never a re-read `TIME` and never this instance's clock.
     * No reply grows with the index.
     *
     * **The decode filter is deliberately non-destructive, and the read path
     * never deletes.** An instance running an older release meets a
     * channel-scoped member, cannot decode it, and skips it — but it also
     * cannot delete it, so the record survives for the instance that owns the
     * socket and can act on it. Making the reap drop members it fails to parse
     * would silently delete live revocations during a rolling deploy.
     *
     * **A failed, malformed or closing pass throws**, never answers `[]`
     * (which reads as "nobody is revoked"): a reap reply or page that does not
     * decode, and a driver whose `close()` has begun, before its next reap or
     * page read ({@link REVOCATION_PASS_CLOSING}). A malformed PAIR inside a
     * well-formed page is skipped and counted; after the last page, a nonzero
     * count is ONE WARN ({@link REVOCATION_PAIRS_SKIPPED} and the number).
     *
     * **A wrong-typed floor key no longer stops this pass** (#405): the reap
     * self-heals it inside its own `EVAL` ({@link FLOOR_WRITE}), so this
     * method still reaps the index and reads every page. `#warnIfFloorHealed`
     * WARNs once, right after the reap's reply decodes, when that happened.
     *
     * **Neither does a wrong-typed INDEX key** (#411): the reap self-heals it
     * too, inside the same `EVAL` ({@link INDEX_HEAL}), before the delete
     * above. Unlike the floor, the reap never `ZADD`s the index, so a healed
     * index is ABSENT rather than a fresh `zset` — the next page read simply
     * sees an empty index, and the next mark or a raw write finds it usable.
     * `#warnIfIndexHealed` WARNs once, right after the reap's reply decodes.
     *
     * **A completed enumeration records its reap time** on the pass in
     * flight, if there is one — {@link RevocationPassRecord.readAt} (#383) —
     * and in {@link #lastReadAt} (#362 S1), its only writer; a pass that
     * throws records neither. The enforcement deadline compares consecutive
     * PASSES' reap times to notice the broker's clock stepping a full TTL, so
     * a handler that enumerates only on some passes never compares a fresh
     * reading against a stale one left by an earlier pass; `#lastReadAt`
     * keeps its own, separate reader — {@link #announceFloor}'s retry-stop.
     *
     * **Each decoded page counts into the revocation pass in flight**, if
     * there is one (#360), and nowhere else: a call made while no pass runs
     * counts nowhere. {@link PassSample}'s `pages` says what a page is.
     *
     * @param owns - Which targets to keep, asked once per decoded record,
     *   synchronously; a throw fails the pass. Omitted, every record is kept.
     * @returns The live revocations `owns` keeps, each member once.
     * @throws {Error} When a round trip fails, a reply does not decode, or the
     *   driver is closing.
     * @example
     * ```ts
     * const local = await driver.listRevocations((id) => sockets.has(id))
     * ```
     */
    async listRevocations(
        owns?: (target: string) => boolean,
    ): Promise<Revocation[]> {
        if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)
        const { t, indexKind, floorKind } = decodeReapReply(
            await this.command.command(
                'EVAL',
                REAP_REVOKED_SCRIPT,
                '2',
                this.revocationIndexKey,
                this.revocationFloorKey,
                String(this.revocationTtlSeconds),
                String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),
            ),
        )
        this.#warnIfIndexHealed(indexKind)
        this.#warnIfFloorHealed(floorKind)
        const live = new Map<string, Revocation>()
        let skipped = 0
        let cursor = '0'
        do {
            if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)
            const page = decodeRevocationPage(
                await this.command.command(
                    'ZSCAN',
                    this.revocationIndexKey,
                    cursor,
                    'COUNT',
                    String(REVOCATION_SCAN_COUNT),
                ),
            )
            if (this.#revocationPass) this.#revocationPass.pages++
            skipped += page.skipped
            for (const entry of page.entries) {
                if (!(entry.score > t)) continue
                // Filtered, matching what the control-plane ingest has always
                // done to `wire.target`. Both return paths are broker-sourced:
                // a writer with bus access could put anything in the index,
                // and reconcile hands what it finds straight to a revocation.
                // The asymmetry between the two paths was the finding, not the
                // reach.
                //
                // ONE filter, and it is the boundary rather than
                // belt-and-braces. #304's battery recorded an equivalent
                // mutant here on the grounds that the real guard had moved
                // inside `#legacyRevoked`, which built a Redis key from an
                // unfiltered member before the caller ever saw it. That method
                // is gone (#278) and with it the second path, so this is the
                // only thing standing between a broker-sourced member and a
                // revocation — the mutation that removes it is a kill, not an
                // equivalence.
                //
                // #332 made it strictly more load-bearing rather than less:
                // the filter now also decides SCOPE, and a decode that
                // degraded to `{ target }` on a malformed member would turn a
                // room revocation into a socket kill. Failing closed is that
                // decision. A member that does not decode is NOT counted as a
                // skipped pair: during a rolling deploy it is expected state.
                const revocation = this.#decodeRevocation(entry.member)
                if (revocation === undefined) continue
                if (owns !== undefined && !owns(revocation.target)) continue
                live.set(entry.member, revocation)
            }
            cursor = page.cursor
        } while (cursor !== '0')
        if (skipped > 0) console.warn(`${REVOCATION_PAIRS_SKIPPED} ${skipped}`)
        if (this.#revocationPass) this.#revocationPass.readAt = t
        this.#lastReadAt = t
        return [...live.values()]
    }

    /**
     * OPTIONAL (S1/FR-014). Forget a revocation the owning instance has applied.
     *
     * **Only a channel-scoped record is ever cleared.** A connection-scoped one
     * becomes moot the instant the socket dies, so `evict` leaves it to the TTL;
     * a channel-scoped one has a live socket to act on for the whole TTL, so an
     * uncleared record would re-apply the leave at every reconcile tick and kick
     * a client that has legitimately re-subscribed. Clearing on apply makes a
     * record mean exactly one thing: *a revocation the owning instance has not
     * applied yet.*
     *
     * **One exact member, by its id** (#337). `ZREM` deletes one member in one
     * atomic command, so a record for the same pair with another id — one
     * written after the revocation being cleared — survives. No script: the
     * semantics are the broker's own, identical on a live broker and the fake.
     *
     * @param revocation - The channel revocation that has been applied.
     * @throws {Error} If the write fails. `ChannelManager` reports it to a
     *   caller where one exists and warns where none does.
     */
    async clearRevocation(revocation: ChannelRevocation): Promise<void> {
        await this.command.command(
            'ZREM',
            this.revocationIndexKey,
            this.#encodeRevocation(revocation),
        )
    }

    /**
     * OPTIONAL (S1/FR-014). Register the owning instance's revocation re-check
     * and start its periodic pass so a missed evict is recovered rather than
     * lost. The re-check itself (which local socket to revoke) lives in the
     * manager; the marker and its cadence live here (decision-table home).
     *
     * The pass runs on a DEDICATED timer started here, UNCONDITIONALLY — it is
     * not coupled to the presence ghost-sweep (which only starts once this
     * instance hosts a presence member). A deployment that serves only private /
     * public channels therefore still reconciles revocations, with the bound
     * below, for EVERY deployment class (closing the FR-014 gap the
     * presence-coupled cadence left open). The timer is cleared by
     * {@link close}.
     *
     * **The timer rule** (#359 A2): one `setTimeout`, armed by
     * {@link #armRevocationReconcile} alone, **from the end of the pass that
     * consumed it** — never a `setInterval`, so a slow pass is never overlapped
     * by the next one. An edge-triggered pass (a reconnect, the #308 retry)
     * never moves a pending timer, and at most one revocation pass runs per
     * driver at a time ({@link #startRevocationPass}).
     *
     * **The enforcement bound — the one home of it** (#359 S2). A revocation
     * whose one-shot control frame was lost is applied within
     * **`reconcileIntervalMs + 2P`**, where **P** is the duration of one pass:
     * 1 + max(1, ⌈N / {@link REVOCATION_SCAN_COUNT}⌉) round trips made one at
     * a time — the reap, then one `ZSCAN` per page of an index of N members,
     * and always at least one page — each capped at the command client's read
     * timeout ({@link RedisCommandClient}'s "every command settles" contract). One P is the pass in flight
     * when the record is written, which may miss it behind its cursor; the
     * interval is armed from that pass's end; the second P is the next pass,
     * which applies it. **A failed pass restarts the clock**: it applies
     * nothing, and the bound runs again from its end. **It holds only while the
     * broker honours `COUNT`**: a broker that answers `ZSCAN` whole turns a
     * page into one unbounded reply. **P counts round trips only**: the
     * pass's apply (the manager's leaves, roster writes and clears) and any
     * wait on the manager's serial re-check tail (a lapse re-check queued
     * ahead of it) add to the bound. And **P grows with the index size N**,
     * which counts every instance's live records, not only this one's. **Since
     * #362 the bound is checked**: statically at boot, where the constructor
     * refuses an interval above half of `revocationTtlSeconds`, and at runtime,
     * where the enforcement deadline says so when no pass completes within one
     * TTL of the last clean pass's start. Other documentation links here rather
     * than restating it. **A pass with a failure is not a success** (#384):
     * which passes re-arm the deadline is decided at the pass's end site, in
     * {@link #startRevocationPass}.
     *
     * **The first registration also announces the floor entry** (#380): one
     * {@link #announceFloor}, under the same gate as the deadline arm, retried
     * until it lands, a pass completes, or {@link close} begins.
     *
     * @param handler - Called with no arguments on each revocation pass;
     *   resolves to the re-check's {@link RevocationTally}, or to nothing
     *   ({@link BroadcastDriver.onRevocationReconcile}).
     */
    onRevocationReconcile(
        handler: () =>
            | RevocationTally
            | void
            | Promise<RevocationTally | void>,
    ): void {
        const first = this.revocationHandler === undefined
        this.revocationHandler = handler
        // Re-registration replaces the pending timer rather than stacking one;
        // the one arming site then arms it afresh.
        if (this.revocationTimer !== undefined) {
            clearTimeout(this.revocationTimer)
            this.revocationTimer = undefined
        }
        this.#armRevocationReconcile()
        // The enforcement deadline (#362): only the FIRST registration arms
        // it, one TTL from now, and never once close() has begun. A later
        // registration leaves a pending or fired deadline alone, so
        // re-registering during a failure run cannot postpone a due WARN.
        if (first && !this.#closing) {
            this.#deadline.arm(this.revocationTtlSeconds * 1000)
            // The floor announce (#380): the same gate, so a re-registration
            // or a registration after close() announces nothing.
            void this.#announceFloor(FLOOR_ANNOUNCE_RETRY_MS)
        }
        // The SECOND trigger (#271): the subscribe socket coming back is the
        // routine moment an `evict` frame was lost, so re-check immediately
        // rather than waiting up to `reconcileIntervalMs`. Registered HERE, in
        // the same method as the timer — "when the revocation re-check runs" has
        // one home, and a future non-revocation consumer of the reconnect signal
        // does not belong in it. Routed through `#startRevocationPass` (not
        // the raw handler) so every trigger runs one pass at a time and shares
        // `#runRevocationReconcile`'s contextual WARN, the only log line naming
        // WHICH control failed. It hands back nothing (#359 A6).
        this.subscriber.onReconnect?.(() =>
            this.#startRevocationPass('reconnect')
        )
    }

    /**
     * Write this instance's floor entry once, before its first revocation
     * pass (#380 FR-005) — {@link ANNOUNCE_FLOOR_SCRIPT}, which never carries
     * the index key.
     *
     * **`async`, and it never rejects** (S5): the command is awaited inside
     * the `try`, so a port that throws synchronously lands in the same
     * `catch` as one that rejects, and {@link onRevocationReconcile} — which
     * `void`s it — still registers its reconnect trigger.
     *
     * **A failure is retried** (S1): one WARN through {@link #warnFloor} —
     * for every failed attempt, retries included — then one unref'd timer
     * ({@link #announceRetry}) re-sends the idempotent announce after
     * `backoffMs`, doubling each time and capped at `reconcileIntervalMs`.
     * The retry stops at the first successful announce, once a revocation
     * pass has completed its whole enumeration ({@link #lastReadAt} is set;
     * its reap wrote the entry, while a pass that fails after its reap does
     * not stop the retry), or once {@link close} has begun — asked before
     * every re-arm and again when the timer fires (the #355 gate).
     *
     * **A wrong-typed floor key no longer fails this announce** (#405): the
     * heal runs inside the same `EVAL` ({@link FLOOR_WRITE}), and
     * `#warnIfFloorHealed` inspects the reply for it once the write succeeds.
     *
     * **The reply is strictly decoded** (#411 folded LOW): `decodeAnnounceReply`
     * throws on anything but a bulk string, instead of the bare `asBulk` this
     * used to read directly — which silently skipped the heal check on a
     * non-bulk reply, no throw and no WARN. A decode failure now lands in the
     * `catch` below, exactly like a rejected command: one WARN, and the same
     * retry.
     *
     * @param backoffMs - The delay before the retry a failure arms.
     * @returns Resolves once this attempt has settled; never rejects.
     */
    async #announceFloor(backoffMs: number): Promise<void> {
        try {
            const reply = await this.command.command(
                'EVAL',
                ANNOUNCE_FLOOR_SCRIPT,
                '1',
                this.revocationFloorKey,
                String(this.revocationTtlSeconds),
                String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),
            )
            this.#warnIfFloorHealed(decodeAnnounceReply(reply))
        } catch (error) {
            this.#warnFloor(
                `${REVOCATION_FLOOR_ANNOUNCE_FAILED} ${renderError(error)}`,
            )
            if (this.#closing || this.#lastReadAt !== undefined) return
            const delay = Math.min(backoffMs, this.reconcileIntervalMs)
            const id = setTimeout(() => {
                this.#announceRetry = undefined
                if (this.#closing || this.#lastReadAt !== undefined) return
                void this.#announceFloor(delay * 2)
            }, delay)
            Deno.unrefTimer(id)
            this.#announceRetry = id
        }
    }

    /**
     * Arm the next timer-triggered revocation pass — **the single arming site
     * of the revocation timer** (#359 FR-010, A2).
     *
     * One `setTimeout`, never a `setInterval`: **the timer is armed from the
     * end of the pass that consumed it, and an edge-triggered pass never
     * moves a pending timer.** Its callback clears the field and asks
     * {@link #startRevocationPass} for a `timer` pass; that pass's end arms
     * the next one. A reconnect or retry pass that ends while a timer is
     * pending leaves it where it is — at most one extra pass per reconnect,
     * never a later timer.
     *
     * Returns without arming while {@link close} is in progress (the one
     * gate for timers), and while a timer is already pending (so at most one
     * exists).
     *
     * A FakeTime `tickAsync(k × interval)` fires the callback once and does
     * not await its pass, so it runs ONE pass, not k.
     */
    #armRevocationReconcile(): void {
        if (this.#closing) return
        if (this.revocationTimer !== undefined) return
        this.revocationTimer = setTimeout(() => {
            this.revocationTimer = undefined
            this.#startRevocationPass('timer')
        }, this.reconcileIntervalMs)
    }

    /**
     * Start one revocation pass — **the single entry point for all three
     * triggers** (the timer, the reconnect seam and the #308 retry), and the
     * reason a driver never runs two revocation passes at once (#359 FR-011).
     *
     * While a pass is in flight a `timer` trigger does nothing (the pass's
     * end arms the next timer), and a `reconnect` or `reconnect-retry` is
     * recorded in {@link #revocationRerun} — one slot, where `reconnect` wins
     * — so however many arrive, ONE trailing pass follows. Otherwise the pass
     * is stored in {@link #revocationPass}; its `finally` clears the slot,
     * takes the recorded rerun and starts it, or else arms the timer.
     *
     * **It returns nothing** (A6): a promise handed to a coalesced caller
     * would resolve before that caller's trailing pass had run.
     *
     * **No closing check of its own** (D3): once {@link close} has begun,
     * the dropped handler makes {@link #runRevocationReconcile} run nothing,
     * and {@link #armRevocationReconcile} arms nothing.
     *
     * **Where the enforcement deadline moves — three sites and `close()`,
     * nowhere else** (#362). {@link onRevocationReconcile}'s FIRST
     * registration arms it one TTL out; here, the start records the pass
     * `{ trigger, startedAt }` on {@link #passClock}, and the end — which
     * stays a `finally` and never logs — reads `endedAt`, frees the slot,
     * takes the rerun or arms the timer, and LAST, while {@link close} has
     * not begun, tells the deadline how the pass ended.
     *
     * **Which pass is clean is decided here, and nowhere else** (#384): an
     * `ok` pass whose record carries no malformed tally and no failure — a
     * pass with no tally counts as none. A clean pass calls
     * `passSucceeded(startedAt, endedAt, pass.readAt)` (#383: THIS pass's own
     * reap time, never {@link #lastReadAt}, so a handler that skips
     * enumeration on some passes never compares a fresh reading against a
     * stale one), which re-arms; **every other settled pass** — `failed`, or
     * `ok` with a
     * failure or a malformed tally — calls `passEnded()`, which re-arms
     * nothing and makes an expiry after it `MISSED`. A `closed` pass, or any
     * pass once `close()` began, calls neither. {@link close} clears it.
     * **The deadline never frees the slot**: a pass whose command never
     * settles holds it forever, and is reported, not abandoned. An outcome that is never recorded (the pass rejected)
     * counts as failed, and the chain's last handler writes one marked ERROR
     * line instead of letting the rejection escape (#369).
     *
     * **The end site's final step is the pass sample** (#360), after the
     * deadline call: {@link #emitPassSample} says where a sample is taken and
     * what it is built from. The start's record also carries the pass's page
     * count, which {@link listRevocations} increments.
     *
     * @param trigger - What asked for the pass.
     */
    #startRevocationPass(
        trigger: 'timer' | 'reconnect' | 'reconnect-retry',
    ): void {
        if (this.#revocationPass !== undefined) {
            if (trigger === 'timer') return
            if (this.#revocationRerun !== 'reconnect') {
                this.#revocationRerun = trigger
            }
            return
        }
        const startedAt = this.#passClock()
        const pass: RevocationPassRecord = { trigger, startedAt, pages: 0 }
        this.#revocationPass = pass
        let outcome: PassOutcome = 'failed'
        this.#runRevocationReconcile(trigger, pass)
            .then((ended) => void (outcome = ended))
            .finally(() => {
                const endedAt = this.#passClock()
                this.#revocationPass = undefined
                const rerun = this.#revocationRerun
                this.#revocationRerun = undefined
                if (rerun !== undefined) this.#startRevocationPass(rerun)
                else this.#armRevocationReconcile()
                const clean = outcome === 'ok' &&
                    pass.malformed !== true &&
                    (pass.failures ?? 0) === 0
                if (clean && !this.#closing) {
                    this.#deadline.passSucceeded(
                        startedAt,
                        endedAt,
                        pass.readAt,
                    )
                } else if (outcome !== 'closed' && !this.#closing) {
                    this.#deadline.passEnded()
                }
                this.#emitPassSample(
                    'revocation',
                    trigger,
                    outcome,
                    startedAt,
                    endedAt,
                    pass.pages,
                    pass.attempts,
                    pass.failures,
                )
            })
            .catch((error: unknown) => {
                // #369: nothing escapes the pass chain. A rejection reaches
                // here only when a log sink itself threw (#349); the marker
                // is the fixed prefix, the rejection is rendered, and the
                // line never throws past itself either (#391).
                writeMarkedFallback(REVOCATION_LOG_FAILED, error)
            })
    }

    /**
     * Run the registered revocation re-check once. A failure is logged at WARN
     * (in the #391 shape, through {@link #warnReconcileFailed} — self-guarded
     * like its three siblings, #383) and never swallowed silently; the next
     * pass is armed from this one's end, so exposure stays bounded (see
     * {@link onRevocationReconcile}).
     *
     * **The trigger is named in the log, and it decides whether a failure is
     * retried (#308).** The two triggers are not equivalent on failure. The
     * timer's next pass is armed from this one's end whatever happened, so a
     * failed timer pass costs nothing but latency. The SEAM fires once per
     * outage and its intent is consumed by the activation that fired it, so a
     * failed seam pass is retried by nothing at all — enforcement silently
     * reverts to the periodic timer, which is exactly the pre-#271 exposure
     * the seam exists to remove. The condition is broker-controllable: heal
     * the subscribe socket while stalling the command socket's `EVAL`.
     *
     * Naming the trigger is half the fix on its own. Both lines read
     * identically before this, so an operator watching a WARN stream could not
     * tell that the fast path had been lost rather than a routine pass having
     * failed — and those want different responses.
     *
     * @param trigger - What ran this pass. `reconnect` is the only one that
     *   earns a retry, and `reconnect-retry` is that retry, which does not
     *   retry itself.
     * @param pass - This pass's record, from {@link #startRevocationPass}'s
     *   closure: where a valid tally's counts, or a malformed tally's mark,
     *   are written (#384). Handed in rather than read from
     *   {@link #revocationPass}, which by the time the handler resolves is
     *   the right record only by the end site's ordering.
     * @returns How the pass ended (#362): `ok` once the handler resolved,
     *   `failed` on every path out of its failure, and `closed` when no
     *   handler is registered — only after {@link close} dropped it.
     */
    async #runRevocationReconcile(
        trigger: 'timer' | 'reconnect' | 'reconnect-retry' = 'timer',
        pass?: RevocationPassRecord,
    ): Promise<PassOutcome> {
        if (!this.revocationHandler) return 'closed'
        try {
            const tally = decodeRevocationTally(await this.revocationHandler())
            if (tally === 'malformed') {
                if (pass) pass.malformed = true
                this.#warnMalformedTally(trigger)
            } else if (tally !== undefined && pass) {
                pass.attempts = tally.attempted
                pass.failures = tally.failed
            }
            return 'ok'
        } catch (error) {
            this.#warnReconcileFailed(trigger, error)
            if (trigger !== 'reconnect') return 'failed'
            // Nor once close() has begun (#355): a run already in flight when
            // close() started would arm a timer that outlives the driver.
            if (this.#closing) return 'failed'
            // ONE retry, and only one. Chaining would turn a broker that keeps
            // failing into a hot loop against the command socket, which is the
            // opposite of what a bounded enforcement window needs.
            if (this.revocationRetryTimer !== undefined) {
                clearTimeout(this.revocationRetryTimer)
            }
            const id = setTimeout(
                () => this.#startRevocationPass('reconnect-retry'),
                RECONCILE_RETRY_MS,
            )
            // Unref'd: this must never be the reason a process stays alive.
            Deno.unrefTimer(id)
            this.revocationRetryTimer = id
            return 'failed'
        }
    }

    /**
     * Write the "revocation reconcile failed" WARN of a pass whose handler
     * rejected — **its one write site**, called only by
     * {@link #runRevocationReconcile}'s catch, before the trigger decides
     * whether a retry follows. In the #391 shape, through {@link
     * #guardedWarn}, the one helper its siblings ({@link #warnFloor},
     * {@link #warnMalformedTally}, {@link #warnPassSample}) share (#409): a
     * `console.warn` that throws becomes one marked
     * {@link REVOCATION_LOG_FAILED} line instead (#383).
     *
     * **Self-guarding here, not only at the tail `.catch`, restores the #308
     * retry.** Before #383 this WARN was a bare `console.warn`: a throwing
     * sink escaped the catch block itself, so `#runRevocationReconcile`
     * rejected before ever reaching the retry-arming code below, and a
     * `reconnect` pass silently lost its one-shot retry whenever the log sink
     * was down — degrading the fast reconnect path to the periodic timer
     * cadence, the exact pre-#271 exposure the seam exists to close. The tail
     * `.catch` on the pass chain stays as defence in depth, for a throw this
     * method does not model.
     *
     * @param trigger - What started the pass.
     * @param error - What the handler rejected with.
     */
    #warnReconcileFailed(
        trigger: PassSample['trigger'],
        error: unknown,
    ): void {
        const line = `realtime: revocation reconcile failed (${trigger}): ${
            renderError(error)
        }`
        this.#guardedWarn(REVOCATION_LOG_FAILED, line)
    }

    /**
     * Write the one {@link REVOCATION_TALLY_MALFORMED} WARN of a pass whose
     * handler resolved a malformed tally (#384) — **its one write site**,
     * called only by {@link #runRevocationReconcile}, where the value is
     * decoded, and so before the pass's end site starts any trailing pass. It
     * names the trigger and the contract, never the value. In the #391 shape,
     * through {@link #guardedWarn}: a `console.warn` that throws becomes one
     * marked {@link REVOCATION_LOG_FAILED} line, which never throws past
     * itself, so a refusing sink can neither fail the pass nor escape it.
     *
     * @param trigger - What started the pass.
     */
    #warnMalformedTally(trigger: PassSample['trigger']): void {
        const line = `${REVOCATION_TALLY_MALFORMED} the ${trigger} pass ` +
            'reports no counts and does not re-arm the enforcement deadline. ' +
            'A handler resolving { attempted, failed } owes two safe ' +
            'integers with 0 <= failed <= attempted (RevocationTally in ' +
            'packages/realtime/driver.ts).'
        this.#guardedWarn(REVOCATION_LOG_FAILED, line)
    }

    /** Compute the FR-015 MAC over a control message's canonical payload. */
    #sign(wire: ControlWire): string {
        if (!this.secret) return ''
        return hmacSha256Hex(this.secret, this.#canonical(wire))
    }

    /**
     * The canonical bytes a control MAC covers: the semantic fields in a fixed
     * key order (the `mac` field itself excluded). `JSON.stringify` omits
     * `undefined` values, so `evict` (no channel/member) and a presence frame
     * canonicalise deterministically.
     */
    #canonical(wire: ControlWire): Uint8Array<ArrayBuffer> {
        return new TextEncoder().encode(JSON.stringify({
            kind: wire.kind,
            target: wire.target,
            channel: wire.channel,
            member: wire.member,
            origin: wire.origin,
            // #272: both inside the MAC. A field on the wire but absent here
            // ships UNAUTHENTICATED, and no test in this package could detect
            // that before FR-013 — see tests/control_mac_coverage.test.ts.
            ts: wire.ts,
            nonce: wire.nonce,
            // #337: LAST, and only ever present on `revoke-channel`. An
            // `undefined` value is omitted by `JSON.stringify`, so every other
            // kind canonicalises to the exact bytes a 0.3.0 peer computes. A
            // 0.3.0 peer cannot cover it, so it drops `revoke-channel` as an
            // invalid MAC — a kind it never acted on, so a WARN, not a loss.
            revocationId: wire.revocationId,
        }))
    }

    /**
     * Decode a control-topic payload, verify its authenticity MAC and routing
     * names, and drop self-loopback. Returns the manager-facing
     * {@link ControlMessage} only when every check passes, its `member`
     * deep-frozen (#354, a mint site); otherwise `undefined` (logged at WARN —
     * never obeyed, never thrown).
     */
    #verifyAndDecode(payload: string): ControlMessage | undefined {
        if (!this.secret) {
            console.warn(
                'realtime: dropped a control message — no control secret ' +
                    'configured to verify it (FR-015)',
            )
            return undefined
        }
        // COST GATE, before `JSON.parse` and before any hashing (#272/FR-011).
        // `hmacSha256Hex` is a synchronous, pure-JS SHA-256 that allocates
        // twice the message length, so without this bound one unauthenticated
        // PUBLISH costs every instance in the fleet a parse, a re-serialise and
        // a blocking hash over attacker-chosen bytes. The RESP reader caps a
        // frame at 10MB; that is an amplifier, not a containment.
        if (payload.length > this.maxControlPayloadBytes) {
            console.warn(
                'realtime: dropped an oversized control payload ' +
                    `(${payload.length} bytes > ${this.maxControlPayloadBytes})`,
            )
            return undefined
        }
        let wire: ControlWire
        try {
            wire = JSON.parse(payload) as ControlWire
        } catch {
            // #418: `onControl`'s `deliver` closure calls this method
            // directly as the subscriber's handler — no caller here wraps it
            // in a `try`, so a throwing sink would escape the same way
            // `onMessage`'s `#deliver` does. One marked line, through
            // #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                CONTROL_DECODE_LOG_FAILED,
                'realtime: dropped a malformed control payload',
            )
            return undefined
        }
        if (
            typeof wire !== 'object' || wire === null ||
            typeof wire.kind !== 'string' || typeof wire.target !== 'string' ||
            typeof wire.origin !== 'string' || typeof wire.mac !== 'string' ||
            // #272/FR-012. `Number.isInteger` rather than `typeof === 'number'`:
            // `1e400` parses to `Infinity`, and `JSON.stringify` collapses
            // `Infinity`, `-Infinity` and `null` to the same bytes — three
            // distinct wire values sharing one MAC. Not reachable today, and
            // one predicate away from never being reachable.
            !Number.isInteger(wire.ts) ||
            // An object nonce would be compared by identity in the replay
            // store, so every replay would be a fresh key: duplicate detection
            // fails silently while the store grows.
            typeof wire.nonce !== 'string' ||
            wire.nonce.length !== CONTROL_NONCE_HEX_LENGTH ||
            // The one field the shape gate never checked, and the one an
            // attacker can make arbitrarily large (FR-011).
            !isPlainMember(wire.member) ||
            // #337: a string when present. A non-string would be compared by
            // the manager against a record id that can never equal it.
            (wire.revocationId !== undefined &&
                typeof wire.revocationId !== 'string')
        ) {
            console.warn('realtime: dropped a control message of invalid shape')
            return undefined
        }
        // Our own publish loops back; we already applied it locally. Skip before
        // the MAC check — skipping is never "obeying", so it is always safe.
        if (wire.origin === this.instanceId) return undefined
        // FR-015: verify authenticity BEFORE any further action.
        const expected = this.#sign({ ...wire, mac: undefined })
        if (!timingSafeEqualHex(expected, wire.mac)) {
            console.warn(
                'realtime: dropped a control message with an absent/invalid ' +
                    'MAC — never obeyed (FR-015)',
            )
            return undefined
        }
        // FR-019: re-validate the routing names on ingest. `origin` joins them
        // (#272): it is always a `crypto.randomUUID()` from a legitimate
        // signer, so this rejects nothing real.
        //
        // The replay WARNs below still run `origin` through `safeForLog`
        // (#277). This guard already constrains the charset, so the encoder
        // rejects nothing either — that is the point. An allowlist upstream and
        // an encoder at the sink are independent controls, and the encoder is
        // the one that survives a future caller reaching those WARNs down a
        // path that does not pass through here.
        if (
            !isValidName(wire.target) || !isValidName(wire.origin) ||
            (wire.channel !== undefined && !isValidName(wire.channel)) ||
            (wire.revocationId !== undefined && !isValidName(wire.revocationId))
        ) {
            console.warn(
                'realtime: dropped a control message with an invalid name',
            )
            return undefined
        }
        // #272: anti-replay, LAST — strictly after the MAC. Admitting an
        // unauthenticated frame would let anyone with bus PUBLISH write into
        // the replay store, trading one weakness for a worse one. The verdict
        // is mapped to a message here rather than logged by the window itself,
        // so every drop reason has one home (this guard chain).
        const verdict = this.replayWindow?.admit(
            wire.origin,
            wire.nonce,
            wire.ts,
        )
        if (verdict === 'stale') {
            const skewMs = this.now() - wire.ts
            console.warn(
                'realtime: dropped a STALE control message — never obeyed ' +
                    `(#272). Issued ${skewMs}ms ago by origin ` +
                    `${safeForLog(wire.origin)}; a large or negative value ` +
                    'here is clock skew between instances, not a dead bus.',
            )
            return undefined
        }
        if (verdict === 'duplicate') {
            console.warn(
                'realtime: dropped a DUPLICATE control message — never ' +
                    `obeyed (#272). Origin ${
                        safeForLog(wire.origin)
                    } already ` +
                    'delivered this exact frame inside the freshness window.',
            )
            return undefined
        }
        return {
            kind: wire.kind,
            target: wire.target,
            channel: wire.channel,
            // Frozen only now (#354), after the MAC, the replay window and
            // the shape gate: a dropped frame costs no walk, and the MAC's
            // canonical bytes never depended on it.
            member: wire.member && freezePresenceMember(wire.member),
            revocationId: wire.revocationId,
        }
    }

    /**
     * Start the instance-liveness heartbeat and the ghost-sweep reconcile pass
     * once, the first time this instance touches the roster. Idempotent; the
     * timers are cleared by {@link close}.
     *
     * **Neither timer is armed once {@link close} has begun** (#355): the boot
     * heartbeat is awaited, and `close()` can run during it. The heartbeat's
     * check sits here; the sweep's sits only inside {@link #armReconcile}, its
     * one arming site, which this calls unconditionally.
     *
     * **The heartbeat stays an unguarded `setInterval`** (#355 disposition):
     * a guard or a re-arm would turn one slow renewal into a missed one — a
     * lapse — while an overlapping beat is a harmless repeat renewal.
     */
    async #ensureSweepStarted(): Promise<void> {
        if (this.sweepStarted) return
        this.sweepStarted = true
        await this.#heartbeat()
        // The heartbeat's closing check. The sweep has none here: it is armed
        // only through #armReconcile, whose own check is its one home.
        if (!this.#closing) {
            // Refresh our own liveness key so a live instance is never swept.
            // A FakeTime `tickAsync` fires this callback and does NOT await
            // the promise it returns — a test drains the round trip itself.
            // Its error handling swallows nothing (it logs at WARN).
            this.heartbeatTimer = setInterval(
                () => this.#heartbeat(),
                this.heartbeatIntervalMs,
            )
        }
        // Sweep the members of any instance whose liveness key has expired.
        this.#armReconcile()
    }

    /**
     * Arm the next ghost-sweep pass — **the single arming site of the sweep
     * timer, and the reason a driver never has two passes in flight** (#355,
     * ADR 006).
     *
     * One `setTimeout`, never a `setInterval`: its callback stores the pass in
     * `#reconcilePass`, runs {@link #reconcile}, and re-arms from the pass's
     * `finally`, so the next interval starts when this pass ends, whether it
     * succeeded or not. A pass slower than the interval therefore delays the
     * next one instead of running beside it. The ghost sweep is
     * level-triggered — every pass re-reads the instance set and each liveness
     * key — so a pass that did not run loses nothing, and no trailing pass is
     * queued.
     *
     * Returns without arming while {@link close} is in progress: this check,
     * here and not at a call site, is what keeps a pass finishing during
     * `close()` from arming another.
     *
     * A FakeTime `tickAsync(k × interval)` fires the callback once and does not
     * await its promise, so it runs ONE pass, not k.
     *
     * **The callback is also the sweep's one start site and one end site for
     * its pass sample** (#360). The start reads {@link #passClock} and stores
     * the pass's record in {@link #sweepPass}; the end reads the pass clock
     * FIRST, frees the pass, re-arms, clears the record, and LAST hands the
     * sample to {@link #emitPassSample}, which says where a sample is taken and
     * what it is built from. An outcome {@link #reconcile} never recorded (the
     * pass rejected) counts as failed, and the chain's last handler writes one
     * marked ERROR line ({@link SWEEP_LOG_FAILED}) instead of letting the
     * rejection escape (#369) — so `#reconcilePass`, which {@link close}
     * awaits, never rejects.
     */
    #armReconcile(): void {
        if (this.#closing) return
        this.reconcileTimer = setTimeout(() => {
            this.reconcileTimer = undefined
            const pass = {
                startedAt: this.#passClock(),
                pages: 0,
                attempts: 0,
                failures: 0,
            }
            this.#sweepPass = pass
            let outcome: PassOutcome = 'failed'
            this.#reconcilePass = this.#reconcile()
                .then((ended) => void (outcome = ended))
                .finally(() => {
                    const endedAt = this.#passClock()
                    this.#reconcilePass = undefined
                    this.#armReconcile()
                    this.#sweepPass = undefined
                    this.#emitPassSample(
                        'sweep',
                        'timer',
                        outcome,
                        pass.startedAt,
                        endedAt,
                        pass.pages,
                        pass.attempts,
                        pass.failures,
                    )
                })
                .catch((error: unknown) => {
                    // #360 A1, the #369 rule: nothing escapes the sweep chain.
                    // A rejection reaches here only when a log sink threw
                    // inside the pass; the marker is the fixed prefix, and
                    // the line never throws past itself either (#391).
                    writeMarkedFallback(SWEEP_LOG_FAILED, error)
                })
        }, this.reconcileIntervalMs)
    }

    /**
     * Refresh this instance's liveness key, then register it (TTL heartbeat).
     *
     * **The liveness key is written FIRST** (#355, A4): registered with no
     * liveness key, this instance is exactly what a peer's sweep takes for
     * dead. The registration is still attempted when that write failed (#310:
     * an instance whose `SET` fails stays registered, and never sweeps
     * itself). One WARN per failed beat, however many of its writes failed.
     *
     * **It also detects a lapse** (#349). The write is `SET … EX … GET`, whose
     * nil reply says the key had expired and this write re-created it — a
     * peer may have swept this instance's holds meanwhile. Once a hold has
     * been issued, a lapsed reply (or any successful beat after a failed one)
     * triggers the lapse handler registered through {@link onRosterLapse},
     * without awaiting it. The reply is decoded by {@link decodeBeatReply}
     * inside the write's `try`.
     */
    async #heartbeat(): Promise<void> {
        let failure: { readonly error: unknown } | undefined
        let outcome: BeatOutcome | undefined
        try {
            const reply = await this.command.command(
                'SET',
                this.aliveKey(this.instanceId),
                '1',
                'EX',
                String(this.livenessTtlSeconds),
                'GET',
            )
            // Decoded INSIDE the try (#349 S1): a reply it refuses is one
            // failed beat, never a rejection escaping an interval callback.
            outcome = decodeBeatReply(reply)
        } catch (error) {
            failure = { error }
        }
        try {
            await this.command.command(
                'SADD',
                this.instancesKey,
                this.instanceId,
            )
        } catch (error) {
            failure ??= { error }
        }
        if (failure) {
            // #395: the interval discards this promise, so a throwing sink
            // would escape as an unhandled rejection — and would skip the
            // lapse decision below. One marked line, then on, through
            // #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                HEARTBEAT_LOG_FAILED,
                `realtime: instance-liveness heartbeat failed: ${
                    renderError(failure.error)
                }`,
                failure.error,
            )
        }
        // The owed-release drain (#371): fired after every beat whose OWN
        // liveness `SET` succeeded and decoded — never on a beat that just
        // proved the connection broken, no point writing into it again — and
        // never gated on `#holdIssued`: an owed release can be queued from a
        // channel this instance no longer holds anything on, so the drain
        // must run whether or not a hold currently exists. Never awaited,
        // exactly like the lapse trigger below.
        if (outcome !== undefined) {
            this.#rosterMaintenance.trigger()
        }
        // The lapse decision, once, reading `#holdIssued` NOW (#349 FR-004,
        // A4): read when the beat was issued, it would miss a hold that
        // overtook this beat's SET on a port that does not serialize. A failed
        // SADD decides nothing — only the liveness write's outcome does.
        if (!this.#holdIssued) return
        if (outcome === undefined) {
            this.#lapseSuspected = true
        } else if (outcome === 'lapsed' || this.#lapseSuspected) {
            this.#lapseSuspected = false
            // Never awaited: the heartbeat must not wait behind K slot writes.
            this.#lapse.trigger()
        }
    }

    /**
     * Release the roster holds of every instance whose liveness key has expired
     * (Q1/FR-008, #345), so a crashed instance leaves no permanent ghost members
     * and never removes a member a live instance still holds.
     *
     * Its one caller is the callback {@link #armReconcile} arms (#355). The
     * `EXISTS` here only SELECTS candidates; it never authorises a write —
     * each sweep write re-checks liveness inside its own script.
     *
     * **It stops at the top of an instance once {@link close} has begun**,
     * before that instance's `EXISTS`: no further instance is read. Its catch
     * covers the instance-set read and the `EXISTS` only — a reply either one
     * answers that does not decode ({@link decodeMembersReply},
     * {@link decodeExistsReply}) is a throw there too (#360 S2), never read as
     * "no instances" or as "alive"; one instance's sweep
     * failing is contained in {@link #sweepInstance}, so it never stops the
     * others.
     *
     * @returns How the pass ended (#360, {@link PassOutcome}): `failed` from
     *   its catch — the one WARN is written first; `closed` from the loop's
     *   closing check; `ok` once every instance was read, a failure contained
     *   in {@link #sweepInstance} included.
     */
    async #reconcile(): Promise<PassOutcome> {
        try {
            const reply = await this.command.command(
                'SMEMBERS',
                this.instancesKey,
            )
            const ids = decodeMembersReply(reply)
            for (const raw of ids) {
                if (this.#closing) return 'closed'
                const id = asBulk(raw)
                if (!id || id === this.instanceId) continue
                const alive = decodeExistsReply(
                    await this.command.command('EXISTS', this.aliveKey(id)),
                )
                if (alive === 0) await this.#sweepInstance(id)
            }
        } catch (error) {
            // #418 (security review): this catch wraps the whole pass, with
            // no per-id try inside the loop — so an unguarded WARN here that
            // ALSO throws would escape `#reconcile()` itself, skipping every
            // id still left in `ids` this pass (#355 A3). One marked line,
            // through #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                RECONCILE_LOG_FAILED,
                `realtime: roster reconcile failed: ${renderError(error)}`,
            )
            return 'failed'
        }
        // The durable revocation re-check runs on its OWN dedicated timer
        // (see {@link onRevocationReconcile}), NOT here — it must fire for a
        // presence-free deployment that never starts this ghost-sweep pass.
        return 'ok'
    }

    /**
     * OPTIONAL (#348). Register the handler the ghost sweep calls for every
     * roster slot it empties while releasing a dead instance's hold — the
     * member nobody else would ever announce as `left`. See
     * {@link BroadcastDriver.onRosterDeparture} for the contract.
     *
     * One handler: registering again replaces it, and {@link close} drops it —
     * the {@link onRevocationReconcile} precedent. {@link releaseMember} never
     * calls it.
     *
     * @param handler - Called with each swept departure, awaited one at a
     *   time; a throw is contained as one WARN.
     *
     * @example
     * ```ts
     * driver.onRosterDeparture(({ channel, member }) =>
     *     console.log(`swept ${member.id} out of ${channel}`)
     * )
     * ```
     */
    onRosterDeparture(
        handler: (departure: RosterDeparture) => void | Promise<void>,
    ): void {
        this.#departureHandler = handler
    }

    /**
     * OPTIONAL (#349). Register the handler this driver calls when its
     * heartbeat finds that this instance's liveness key had lapsed — its holds
     * may have been swept on its behalf, so the owner should write them again
     * through its normal write path. See
     * {@link BroadcastDriver.onRosterLapse} for the contract.
     *
     * One handler: registering again replaces it, and {@link close} aborts its
     * signal, waits for a run in flight, then drops it. Only after this
     * instance has issued a hold, never awaited by the heartbeat, one run at a
     * time plus one trailing run, and a failed run is one WARN and retried by
     * the next successful heartbeat.
     *
     * @param handler - Called with a signal aborted by {@link close}.
     *
     * @example
     * ```ts
     * // Re-hold through the owner's OWN write path — the one its joins and
     * // leaves use, which reads the desired state when the write runs and
     * // orders it with a concurrent leave. Calling `holdMember` over a list of
     * // slots would re-hold a member who left during the run.
     * driver.onRosterLapse(async (signal) => {
     *     for (const slot of localSlots()) {
     *         if (signal.aborted) return
     *         await writeSlot(slot) // the owner's serialized slot writer
     *     }
     * })
     * ```
     */
    onRosterLapse(
        handler: (signal: AbortSignal) => void | Promise<void>,
    ): void {
        this.#lapse.register(handler)
    }

    /**
     * OPTIONAL (#371). Register the handler this driver calls after every
     * heartbeat whose own liveness `SET` succeeded — the owed-release drain.
     * See {@link BroadcastDriver.onRosterMaintenance} for the contract.
     *
     * One handler: registering again replaces it, and {@link close} waits
     * for a run in flight, then drops it. Fired unconditionally, unlike
     * {@link onRosterLapse}, and never gated on {@link #holdIssued}: an owed
     * release can be queued from a channel this instance no longer hosts
     * anything on, so the drain must run whether or not a hold currently
     * exists.
     *
     * @param handler - Called with no arguments after each qualifying beat.
     *
     * @example
     * ```ts
     * driver.onRosterMaintenance(() => drainOwedReleases())
     * ```
     */
    onRosterMaintenance(handler: () => void | Promise<void>): void {
        this.#rosterMaintenance.register(handler)
    }

    /**
     * Sweep one dead instance: release every hold it owns, report each slot a
     * release emptied, then forget the instance (#345, #348, #355) — and log
     * what that did, in ONE place.
     *
     * The writes are {@link #sweepOwned}'s; this method owns the two things
     * that must hold however those writes end:
     *
     * **One instance's failure ends only that instance's sweep** (#355 A3).
     * The catch below contains a throw from any of its writes: one "failed"
     * line, no deregistration (the instance is retried next pass), and the
     * pass goes on to the next instance.
     *
     * **It logs one line per instance, or none** (#355), counting from the
     * decoded outcomes: N = emptied + kept (holds actually removed), E =
     * emptied (departures announced); an *absent* release removed nothing.
     * - `completed`, `kept` or `closed` (cut short by {@link close}), N > 0:
     *   `released N hold(s) of dead instance <id> (E emptied their slot)` —
     *   nothing at N = 0, so the count covers every hold removed. On `kept`
     *   and `closed`, which leave the instance registered with work behind,
     *   the line ends `— unfinished: it stays registered and a later pass
     *   resumes it` (#358); it gives no count of what remains;
     * - `renewed`: `instance <id> renewed its liveness while being swept …`;
     * - thrown: `sweep of dead instance <id> failed after N hold(s) released
     *   (E emptied): <error>`.
     *
     * **Every exit reaches the one log site below**: {@link #sweepOwned}
     * returns how the sweep ended — its return type makes a bare `return`
     * a compile error — and a throw becomes the `failed` end here, so an exit
     * added later can neither skip the line nor write a second one.
     *
     * **It counts the sweep's attempts and failures, and nothing else does**
     * (#384): one attempt as its first statement, and one failure as the
     * first statement of the failed branch, before that branch's WARN. What
     * the two counts mean is {@link PassSample}'s to say.
     *
     * **Every exit's WARN goes through {@link #guardedWarn}, so this method
     * never throws** (#418, second security review): a `#reconcile` that
     * called it inside a per-id `try` would either sit dead — since nothing
     * here can reject it any more — or, worse, relabel a genuine bug as a
     * routine sweep failure the first time one is added without the same
     * guard. A WARN added to a future exit follows this rule too.
     *
     * @param deadId - The instance whose liveness lapsed.
     */
    async #sweepInstance(deadId: string): Promise<void> {
        if (this.#sweepPass) this.#sweepPass.attempts++
        const count: SweepCount = { released: 0, emptied: 0 }
        let end: SweepEnd
        try {
            end = await this.#sweepOwned(deadId, count)
        } catch (error) {
            end = { failed: error }
        }
        const id = safeForLog(deadId)
        const { released, emptied } = count
        if (typeof end === 'object') {
            // Counted BEFORE the WARN, so a sink that throws cannot skip it.
            if (this.#sweepPass) this.#sweepPass.failures++
            // #418 (security review): nothing between here and #reconcile's
            // `for` loop catches a throw — an unguarded WARN that ALSO threw
            // used to escape this method and abort the loop, skipping every
            // id still left in `ids` (#355 A3). One marked line, through
            // #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                SWEEP_INSTANCE_LOG_FAILED,
                `realtime: sweep of dead instance ${id} failed after ` +
                    `${released} hold(s) released (${emptied} emptied): ` +
                    renderError(end.failed),
            )
        } else if (end === 'renewed') {
            // #418 (second security review): a SUCCESS exit — the instance
            // renewed, not crashed — but nothing wraps this WARN in a `try`
            // either, so its own throw would escape this method exactly like
            // the failed branch's used to. One marked line, through
            // #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                SWEEP_INSTANCE_RENEWED_LOG_FAILED,
                `realtime: instance ${id} renewed its liveness while being ` +
                    `swept — a lapse, not a crash; ${released} hold(s) ` +
                    `released (${emptied} emptied) before it did`,
            )
        } else if (released > 0) {
            const unfinished = end === 'kept' || end === 'closed'
                ? ' — unfinished: it stays registered and a later pass ' +
                    'resumes it'
                : ''
            // #418 (second security review): the other SUCCESS exit, the
            // same unwrapped shape as `renewed` above.
            this.#guardedWarn(
                SWEEP_INSTANCE_RELEASED_LOG_FAILED,
                `realtime: released ${released} hold(s) of dead instance ` +
                    `${id} (${emptied} emptied their slot)${unfinished}`,
            )
        }
    }

    /**
     * The writes of one instance's sweep: one release per owned entry, then
     * the deregistration (#345, #348, #355). It counts into `count` as each
     * reply is decoded, so a throw mid-sweep leaves the count of what was
     * already removed for {@link #sweepInstance}'s "failed" line.
     *
     * **The owned set is read here and nowhere else, one page at a time**
     * (#358): `SSCAN <owned key> <cursor> COUNT` {@link OWNED_SCAN_COUNT}, no
     * other option, each page released by {@link #sweepPage} before the next
     * is read. The scan is **one full iteration per pass**, ending only when
     * the cursor comes back `'0'` — no budget, no resume state: the owned set
     * shrinks under its own releases, and what is left is the next pass's
     * work. No sweep reply grows with the owned set, and a survivor holds one
     * page of it at a time. Each decoded page counts into the sweep in flight
     * ({@link #sweepPass}, #360); {@link PassSample}'s `pages` says what a page
     * is. The one SCAN guarantee relied on: a member present
     * for the whole iteration is returned at least once. A duplicate, or an
     * entry another survivor already released, is an *absent* release; a
     * member added mid-iteration may be missed, and the deregistration then
     * answers *kept*.
     *
     * **A sweep is a leave on the dead instance's behalf**: one
     * {@link RELEASE_MEMBER_SCRIPT} per owned entry, with `deadId` as the
     * releaser, so a slot another live instance still holds stays in the
     * roster. **When the release empties the slot, its reply is the dead
     * holder's entry**, handed to {@link #announceSwept}. Of two instances
     * sweeping the same dead one, only the first release gets the entry, so
     * the room hears it once.
     *
     * **The owned set is never `DEL`eted.** Each release already removes its
     * own entry; a hold that lands behind the scan's cursor stays in the set,
     * sweepable next time (S1c) — and keeps the instance registered, because
     * {@link DEREGISTER_INSTANCE_SCRIPT} only deregisters an instance that
     * owns nothing (#355 A4). That script decides; the control flow only
     * asks.
     *
     * **Every write asks whether the instance is still dead, inside the
     * write** (#355). A *refused* release or deregistration means it renewed
     * mid-sweep — a lapse, not a crash: the sweep stops there, with no further
     * release and no deregistration. What it released before stays released
     * (re-holding it is #349).
     *
     * **Once {@link close} has begun it issues nothing more**: it checks
     * before each page read, before each release and before the
     * deregistration — never between a release reply and the announcement, so
     * an in-flight release's departure is still announced. The check before a
     * page read is what bounds `close()` through a run of pages that release
     * nothing (empty pages, unparsable entries).
     *
     * Its exits, each one an end {@link #sweepInstance} logs from:
     * - `closed` — `close()` began: checked before each page read, before
     *   each release ({@link #sweepPage}) and before the deregistration;
     * - `renewed` — a release or the deregistration was refused; no further
     *   page is read;
     * - `completed` — one full scan, then deregistered;
     * - `kept` — one full scan, then kept by a late or unparsable entry
     *   (next pass);
     * - a throw — a round trip or a decoder failed.
     *
     * @param deadId - The instance whose liveness lapsed.
     * @param count - Incremented per hold removed (N) and per slot emptied (E).
     * @returns How the sweep ended, when it did not throw.
     * @throws {Error} When a broker round trip fails or a reply does not
     *   decode.
     */
    async #sweepOwned(
        deadId: string,
        count: SweepCount,
    ): Promise<SweepStop> {
        let cursor = '0'
        do {
            if (this.#closing) return 'closed'
            const page = decodeScanReply(
                await this.command.command(
                    'SSCAN',
                    this.ownedKey(deadId),
                    cursor,
                    'COUNT',
                    String(OWNED_SCAN_COUNT),
                ),
            )
            if (this.#sweepPass) this.#sweepPass.pages++
            const end = await this.#sweepPage(deadId, page.items, count)
            if (end !== 'swept') return end
            cursor = page.cursor
        } while (cursor !== '0')
        if (this.#closing) return 'closed'
        const deregistration = decodeDeregisterReply(
            await this.command.command(
                'EVAL',
                DEREGISTER_INSTANCE_SCRIPT,
                '3',
                this.instancesKey,
                this.aliveKey(deadId),
                this.ownedKey(deadId),
                deadId,
            ),
        )
        this.#warnIfInstancesHealed(deregistration.instancesKind)
        return deregistration.outcome === 'deregistered'
            ? 'completed'
            : deregistration.outcome
    }

    /**
     * Release one page of a dead instance's owned set (#358): the per-entry
     * body of the sweep, moved verbatim out of {@link #sweepOwned} so the page
     * loop stays flat. One {@link RELEASE_MEMBER_SCRIPT} per parsable entry,
     * counted into `count`, and each emptied slot handed to
     * {@link #announceSwept} straight from its release reply.
     *
     * **A page read never sits between a release reply and its
     * announcement** (#348 A1): the next page is read only after this
     * returns, after the page's last announcement.
     *
     * Its declared return type makes a bare `return` a compile error (the
     * #355 rule), so every exit says how the page ended:
     * - `swept` — every entry of the page handled; the scan goes on;
     * - `closed` — {@link close} began, checked before each release;
     * - `renewed` — a release was refused: the instance is alive again, and
     *   the scan stops at the first refusal.
     *
     * @param deadId - The instance whose liveness lapsed.
     * @param owned - The page's raw owned entries, from
     *   {@link decodeScanReply}.
     * @param count - Incremented per hold removed (N) and per slot emptied (E).
     * @returns How the page ended.
     * @throws {Error} When a release round trip fails or its reply does not
     *   decode.
     */
    async #sweepPage(
        deadId: string,
        owned: readonly unknown[],
        count: SweepCount,
    ): Promise<'swept' | 'closed' | 'renewed'> {
        for (const raw of owned) {
            const entry = asBulk(raw)
            if (!entry) continue
            const sep = entry.indexOf(OWNED_SEP)
            if (sep < 0) continue
            const channel = entry.slice(0, sep)
            const field = entry.slice(sep + 1)
            if (this.#closing) return 'closed'
            const outcome = await this.#release(
                channel,
                field,
                deadId,
                true,
            )
            if (outcome.kind === 'refused') return 'renewed'
            if (outcome.kind === 'absent') continue
            count.released++
            if (outcome.kind === 'kept') continue
            count.emptied++
            await this.#announceSwept(channel, field, outcome.entry)
        }
        return 'swept'
    }

    /**
     * Report one slot a sweep release emptied to the departure handler
     * ({@link onRosterDeparture}) — its only caller is {@link #sweepPage},
     * which makes this the only path to the handler (#348).
     *
     * **What it reports is checked here, where the slot is known** (#348 A2,
     * S1). An entry is dropped — one WARN naming the channel only, no report,
     * the release still committed — if its owned entry's channel is not a
     * valid name, if the entry does not decode, or if its member id is not the
     * slot it was released from. The entry's bytes come from the broker, and
     * the report becomes a MAC-signed `presence-leave`. A throwing handler is
     * the same one WARN, and the sweep goes on.
     *
     * **The handler is called before this method's first await** (#348 A1),
     * and its caller invokes this straight from the release reply: no I/O
     * await separates the reply from the handler call. The command client runs
     * one exchange at a time, so a hold of this slot issued mid-sweep has not
     * had its reply yet, and the `left` goes out before its `joined`. An await
     * here would hand that order to the race.
     *
     * Never throws: every failure is one WARN, and since #419 every one of
     * those WARNs — the drop, the decoder's own, and the handler-failure
     * report — goes through {@link #guardedWarn}, so a throwing sink cannot
     * escape here either.
     *
     * @param channel - The channel of the emptied slot, as its owned entry
     *   names it.
     * @param field - The slot's member id, as its owned entry names it.
     * @param entry - The release reply: the dead holder's roster entry.
     */
    async #announceSwept(
        channel: string,
        field: string,
        entry: string,
    ): Promise<void> {
        const handler = this.#departureHandler
        if (!handler) return
        const dropped = () =>
            // #419: called BEFORE the departure handler ever runs, still
            // inside #sweepPage's loop over one dead instance's owned slots
            // with nothing between here and there that catches a throw — so
            // an unguarded `console.warn` failing here would reject THIS
            // call the same way an unguarded handler-catch WARN used to
            // (#418 T3), skipping every remaining slot on the page (and
            // every later page) for the SAME dead instance this pass. One
            // marked line, through #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                SWEEP_DROPPED_LOG_FAILED,
                `realtime: a member swept from ${
                    safeForLog(channel)
                } was not announced as left — its roster entry is ` +
                    'not a departure this sweep can report. The ' +
                    'release is committed; clients heal on ' +
                    'resubscribe.',
            )
        if (!isValidName(channel)) {
            dropped()
            return
        }
        // An entry that does not decode is logged by its decoder, told what
        // the skip cost here: still one WARN per dropped entry.
        const member = this.#parseRosterValue(
            channel,
            entry,
            SWEPT_ENTRY_NOT_ANNOUNCED,
        )
        if (!member) return
        if (!sameMemberId(member.id, field)) {
            dropped()
            return
        }
        try {
            await handler({ channel, member })
        } catch {
            // DELIBERATELY drops the error (#348 plan §11, S2): a handler's
            // message may carry the entry, and the entry is application data
            // — so neither the member nor the error.
            //
            // #418: this `await` sits inside #sweepPage's loop over one dead
            // instance's owned slots, with nothing between here and there
            // that catches a throw — so an unguarded `console.warn` failing
            // would reject THIS call, skipping every remaining slot on the
            // page (and every later page), not just losing this one line. One
            // marked line, through #guardedWarn's shared #369 shape.
            this.#guardedWarn(
                SWEEP_DEPARTURE_LOG_FAILED,
                `realtime: the roster departure handler failed for a ` +
                    `member swept from ${
                        safeForLog(channel)
                    } — the release is committed and the sweep goes on`,
            )
        }
    }

    /**
     * Release the connections this driver constructed itself (via
     * {@link fromConfig}) — the subscribe socket first (stops the push read
     * loop), then the command client (drains its QUIT) — and stop the sweep
     * timers. Does NOT proactively release this instance's roster holds: a real
     * crash cannot, so its liveness key simply expires and a surviving instance
     * sweeps it (that is what {@link close} models in the sweep tests).
     * Idempotent; for an injected-port driver it stops the timers and drops the
     * revocation handler, so a later reconnect on the app-owned subscriber
     * revokes nothing (FR-007). It also drops the departure handler
     * ({@link onRosterDeparture}), on both construction paths.
     *
     * **It waits for the ghost-sweep pass, then the lapse run** (#355, #349),
     * in this order: mark the driver closing, clear every timer, drop the
     * revocation handler — synchronously, so a reconnect during the wait runs
     * nothing and arms no retry — then {@link awaitCloseDrain} both against
     * ONE shared expiry, then drop the departure handler, then close the
     * owned connections.
     *
     * **The wait is bounded at one liveness TTL** (#368):
     * `Math.min(livenessTtlSeconds * 1000, MAX_TIMER_MS)`. Past that, this
     * instance's own heartbeat has stopped, so a peer's sweep is already
     * entitled to treat it as dead — a stalled wait behaves exactly like a
     * crash, which is the failure the sweep exists to survive. A command on
     * the port is never cancelled and the sweep slot is never freed early;
     * past the budget this writes ONE WARN
     * ({@link CLOSE_DRAIN_EXPIRED}) naming what was still pending, and
     * teardown carries on regardless. The pass stops at its next write, so a
     * release that settles within the budget still has its departure
     * announced; one that settles later finds the departure handler already
     * dropped and announces nothing. Once this resolves the driver issues no
     * further sweep command.
     *
     * **It also stops the lapse run** (#349): right after the timers it
     * closes {@link onRosterLapse}'s run and aborts its signal, synchronously,
     * so a beat reply arriving later starts nothing; it is then awaited by
     * the same bounded drain, after the sweep pass. That wait is at most one
     * slot write plus whatever is queued ahead of it on that slot, or the
     * revocation re-check in flight (the run's first step, which the signal
     * cannot cut short) — unless the shared budget runs out first. That
     * re-check stops before its next reap or page read (#359), so it adds at
     * most the one command in flight — **but** when that command is the LAST
     * page, the read returns normally and the manager's apply phase (leaves,
     * roster writes, clears) runs while this waits. It then drops the
     * departure handler and the {@link onControlRefused} handler, and closes
     * the owned connections — whether or not the drain expired.
     *
     * **It also stops the owed-release drain** (#371), the same shape right
     * beside the lapse run: marked closed synchronously, so no new drain
     * starts once `close()` has begun, then awaited by the same bounded drain
     * as the sweep pass and the lapse run, last of the three. It has no
     * signal to abort — a drain in flight simply finishes, or is reported
     * still pending at the shared budget like the other two.
     *
     * **It does not await a revocation pass** (#359 FR-013). A timer- or
     * reconnect-triggered pass in flight completes the command in flight,
     * then stops before its next reap or page read and logs one WARN
     * ({@link REVOCATION_PASS_CLOSING}); a trailing pass recorded for it runs
     * nothing (the handler is dropped), and no timer or retry is armed after
     * it. **Unless the command in flight is the pass's LAST page**: there is
     * no next read to refuse, so the pass finishes with no WARN, and its apply
     * (the manager's leaves, roster writes and clears) runs while this closes
     * the owned connections, not awaited. Every such action only removes
     * access, so a pass cut off there grants nothing.
     *
     * @returns Resolves once the bounded drain has settled — the sweep pass,
     *   the lapse run and the owed-release drain each either finished or gave
     *   up at the TTL budget — and every owned connection is closed.
     * @example
     * ```ts
     * const driver = RedisBroadcastDriver.fromConfig({ hostname: 'localhost' })
     * await driver.close()
     * ```
     */
    /**
     * Write the one WARN {@link close}'s bounded drain logs when it expires
     * (#368), through {@link #guardedWarn}'s #369 shape: `console.warn` first,
     * a marked `console.error` line through {@link writeMarkedFallback} when
     * that throws. Names what was still pending — the sweep pass, with its age
     * read from {@link #sweepPass} when known, and/or the lapse run, and/or
     * the owed-release drain (#371) — and the budget. Points at the
     * {@link RedisCommandClient} contract and at the handlers this instance
     * drops regardless. Carries no member, channel or instance id.
     *
     * @param pending - Which of the three the drain reports as still
     *   pending.
     * @param budgetMs - The budget the drain was armed with.
     */
    #warnCloseDrainExpired(
        pending: CloseDrainPending,
        budgetMs: number,
    ): void {
        const pieces: string[] = []
        if (pending.sweepPass) {
            pieces.push(
                this.#sweepPass === undefined
                    ? 'the ghost sweep pass'
                    : `the ghost sweep pass (age ${
                        this.#passClock() - this.#sweepPass.startedAt
                    }ms)`,
            )
        }
        if (pending.lapseRun) pieces.push('the lapse run')
        if (pending.maintenanceDrain) pieces.push('the owed-release drain')
        const text = `${CLOSE_DRAIN_EXPIRED} — still pending: ` +
            `${pieces.join(' and ')}. Budget ${budgetMs}ms ` +
            '(livenessTtlSeconds). A command on the port never settled: ' +
            'every command settles is the RedisCommandClient contract, and ' +
            'the driver does not cancel one. The departure, lapse-run and ' +
            'roster-maintenance handlers are dropped, and the owned ' +
            'connections are still closed, regardless.'
        this.#guardedWarn(CLOSE_LOG_FAILED, text)
    }

    async close(): Promise<void> {
        this.#closing = true
        if (this.heartbeatTimer !== undefined) {
            clearInterval(this.heartbeatTimer)
            this.heartbeatTimer = undefined
        }
        if (this.reconcileTimer !== undefined) {
            clearTimeout(this.reconcileTimer)
            this.reconcileTimer = undefined
        }
        if (this.revocationTimer !== undefined) {
            clearTimeout(this.revocationTimer)
            this.revocationTimer = undefined
        }
        // A trailing pass recorded before close() runs nothing either way (the
        // handler is dropped below); cleared so no stale intent survives.
        this.#revocationRerun = undefined
        if (this.revocationRetryTimer !== undefined) {
            clearTimeout(this.revocationRetryTimer)
            this.revocationRetryTimer = undefined
        }
        // Nor a floor announce retry (#380).
        if (this.#announceRetry !== undefined) {
            clearTimeout(this.#announceRetry)
            this.#announceRetry = undefined
        }
        // The enforcement deadline goes with the other timers (#362); a pass
        // that ends after this re-arms nothing, because its end site asks
        // `#closing` first.
        this.#deadline.close()
        // Closed and aborted synchronously (#349): a beat reply arriving from
        // here on starts no run, and a re-assert in flight stops before its
        // next slot. Awaited only after the sweep pass, on its own line.
        const stopped = this.#lapse.close()
        // Same shape, no signal to abort: the drain has nothing to cut short
        // mid-run, so this only refuses a NEW run and waits for one already
        // in flight (#371).
        const maintenanceStopped = this.#rosterMaintenance.close()
        // Clearing the timer is not enough for the RECONNECT trigger (#271): on
        // the injected-port path `owned` is empty, so the subscriber outlives
        // this driver and can still fire. Dropping the handler makes
        // `#runRevocationReconcile`'s existing guard the ONE gate that quiesces
        // both triggers on both construction paths. BEFORE the wait below
        // (#355 A1): a reconnect while the pass finishes must run nothing.
        this.revocationHandler = undefined
        // The pass stops at its next write once `#closing` is set; a release
        // already in flight still reports its departure, so the handler is
        // dropped only AFTER the drain below. Bounded at one liveness TTL
        // (#368): past that, this instance's own heartbeat has stopped, so a
        // peer's sweep is already entitled to treat it as dead, and a wait
        // that is still stalled is reported once and teardown goes on
        // regardless — never a second timer, and never a freed sweep slot.
        // The owed-release drain (#371) shares that same one budget.
        const budgetMs = Math.min(
            this.livenessTtlSeconds * 1000,
            MAX_TIMER_MS,
        )
        const pending = await awaitCloseDrain(
            budgetMs,
            this.#reconcilePass,
            stopped,
            maintenanceStopped,
        )
        if (pending.sweepPass || pending.lapseRun || pending.maintenanceDrain) {
            this.#warnCloseDrainExpired(pending, budgetMs)
        }
        // A closed driver reports no departure either (#348) — dropped here
        // whether or not the drain above expired.
        this.#departureHandler = undefined
        // Nor a refusal (#349 FR-006a): every hook this driver holds is
        // dropped by its own shutdown.
        this.controlRefusedHandler = undefined
        // Nor a pass sample (#360) — hygiene: the `#closing` gate in
        // `#emitPassSample` is what decides.
        this.#passCompleteHandler = undefined
        for (const resource of this.owned) {
            await resource.close()
        }
    }
}
