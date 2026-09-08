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

import type {
    BroadcastDriver,
    BroadcastMessage,
    ControlMessage,
    ControlRefusal,
} from '../driver.ts'
import { isValidName } from '../protocol.ts'
import { ControlReplayWindow } from '../control_replay_window.ts'
import type { PresenceMember } from '../channel.ts'
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
 * `KEYS[1]` index key · `ARGV[1]` ttl seconds · `ARGV[2]` connection id ·
 * `ARGV[3]` the index key's own TTL.
 */
const MARK_REVOKED_SCRIPT: string = [
    "local t = redis.call('TIME')[1]",
    "redis.call('ZADD', KEYS[1], 'GT', t + ARGV[1], ARGV[2])",
    "redis.call('EXPIRE', KEYS[1], ARGV[3], 'NX')",
    "redis.call('EXPIRE', KEYS[1], ARGV[3], 'GT')",
].join('\n')

/**
 * Reap expired revocations and return the live ones — ONE operation, ONE
 * `now`.
 *
 * Both halves are bounded by the same `t`, so every member the enumeration
 * returns has a score strictly greater than the bound the reap just used: a
 * live revocation cannot be removed, whatever else is happening concurrently
 * (#276 FR-001). Nothing is read in an earlier round-trip and acted on in a
 * later one, which is the shape the previous `EXISTS`-then-`SREM` had.
 *
 * `KEYS[1]` index key.
 */
const LIST_REVOKED_SCRIPT: string = [
    "local t = redis.call('TIME')[1]",
    "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', t)",
    "return redis.call('ZRANGEBYSCORE', KEYS[1], t, '+inf')",
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
     * under `livenessTtlSeconds * 1000`.
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
     * lingers this long so a socket that reconnects within the window is still
     * revoked; after it, the marker self-expires so the set never grows without
     * bound.
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
 * Whether a control frame's `member` is a plain, small presence member.
 *
 * `member` was the one field the ingest shape gate never checked, and it is the
 * one an attacker can make arbitrarily large — which matters because everything
 * downstream of the gate re-serialises it and hashes it synchronously
 * (FR-011). `undefined` is valid: an `evict` frame carries no member.
 *
 * @param value - The candidate, straight off the wire.
 * @returns Whether it is safe to canonicalise.
 */
function isPlainMember(value: unknown): boolean {
    if (value === undefined) return true
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
        return false
    }
    const member = value as { id?: unknown; info?: unknown }
    const idOk = typeof member.id === 'string' || typeof member.id === 'number'
    const infoOk = member.info === undefined ||
        (typeof member.info === 'object' && member.info !== null &&
            !Array.isArray(member.info))
    return idOk && infoOk && Object.keys(member).length <= 2
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

/** A stored roster entry: the client-visible member + its internal owner (FR-018). */
interface RosterEntry {
    /** The client-visible member (the only field that enters snapshots/frames). */
    readonly member: PresenceMember
    /** The owning-instance id — internal sweep metadata, never client-visible. */
    readonly owner: string
}

/** The wire shape of a control message: the manager-facing fields + `origin`. */
interface ControlWire {
    kind: ControlMessage['kind']
    target: string
    channel?: string
    member?: PresenceMember
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
const DEFAULT_REVOCATION_TTL_SECONDS = 300
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
 * path broke, because `removeMember` re-joins the full string, which is why it
 * went unnoticed. `ChannelManager`'s `#assertUsableChannel` is the enforcement
 * point this docstring now depends on rather than assumes.
 *
 * The other half — that a member id after the first space may contain more — is
 * proven by the US5/FR-008 live-broker scenario since #316, whose ghost carries
 * a two-space id. Before that its id was `2`, so `indexOf` and `lastIndexOf`
 * agreed on every entry and the sweep could have parsed on the LAST space
 * undetected: the line ran on every pass and no fixture could observe it.
 */
const OWNED_SEP = ' '

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
    private reconcileTimer?: ReturnType<typeof setInterval>
    private revocationTimer?: ReturnType<typeof setInterval>
    /**
     * The ONE retry a failed seam-triggered reconcile gets (#308).
     *
     * At most one exists: the retry itself never retries, so a broker that
     * keeps failing costs one extra round-trip per outage rather than a loop.
     */
    private revocationRetryTimer?: ReturnType<typeof setTimeout>
    private sweepStarted = false
    /**
     * The owning instance's revocation re-check (S1/FR-014). Registered by the
     * manager via {@link onRevocationReconcile}; absent until then, so a driver
     * used without a manager reconciles nothing. Its cadence is the DEDICATED
     * {@link revocationTimer} — deliberately independent of the presence
     * ghost-sweep, which only starts once this instance hosts a presence member,
     * so a presence-free deployment still recovers a lost evict (FR-014).
     */
    private revocationHandler?: () => void | Promise<void>
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
        this.reconcileIntervalMs = options.presence?.reconcileIntervalMs ??
            DEFAULT_RECONCILE_INTERVAL_MS
        this.revocationTtlSeconds = options.revocationTtlSeconds ??
            DEFAULT_REVOCATION_TTL_SECONDS
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
                console.warn('realtime: dropped a malformed Redis payload')
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
            ).catch((error) =>
                console.warn(
                    'realtime: the control subscription could not be issued ' +
                        `— the driver's own retry is what restores it: ${
                            renderError(error)
                        }`,
                )
            )
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
            return
        }
        await this.command.command('PUBLISH', this.controlTopic, payload)
    }

    /**
     * OPTIONAL (FR-005). Add a member to the channel's authoritative Redis
     * roster, tagged with this instance's owning id for the ghost sweep (FR-008),
     * and start the instance-liveness heartbeat if it is not already running.
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member to add.
     */
    async addMember(channel: string, member: PresenceMember): Promise<void> {
        await this.#ensureSweepStarted()
        const entry: RosterEntry = { member, owner: this.instanceId }
        const field = String(member.id)
        await this.command.command(
            'HSET',
            this.presenceKey(channel),
            field,
            JSON.stringify(entry),
        )
        await this.command.command(
            'SADD',
            this.ownedKey(this.instanceId),
            `${channel}${OWNED_SEP}${field}`,
        )
    }

    /**
     * OPTIONAL (FR-005). Remove a member from the channel's authoritative roster.
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member to remove.
     */
    async removeMember(
        channel: string,
        memberId: string | number,
    ): Promise<void> {
        const field = String(memberId)
        await this.command.command('HDEL', this.presenceKey(channel), field)
        await this.command.command(
            'SREM',
            this.ownedKey(this.instanceId),
            `${channel}${OWNED_SEP}${field}`,
        )
    }

    /**
     * OPTIONAL (FR-005). List the channel's authoritative roster — every
     * instance's members. Only the client-visible member is returned; the
     * owning-instance metadata stays internal (FR-018).
     *
     * @param channel - The presence channel.
     * @returns The current members ("here").
     */
    async listMembers(channel: string): Promise<PresenceMember[]> {
        const reply = await this.command.command(
            'HGETALL',
            this.presenceKey(channel),
        )
        const flat = asArray(reply)
        if (!flat) return []
        const members: PresenceMember[] = []
        // HGETALL returns [field1, value1, field2, value2, …] as bulk strings.
        for (let i = 1; i < flat.length; i += 2) {
            const value = asBulk(flat[i])
            if (!value) continue
            try {
                const entry = JSON.parse(value) as RosterEntry
                if (entry && typeof entry === 'object' && entry.member) {
                    members.push(entry.member)
                }
            } catch (error) {
                console.warn(
                    `realtime: skipped a malformed roster entry on ${
                        safeForLog(channel)
                    }: ${renderError(error)}`,
                )
            }
        }
        return members
    }

    /**
     * OPTIONAL (S1/FR-014). Durably record that a connection is revoked.
     *
     * The record is **one** sorted-set member whose score is the second it
     * expires (#276) — not a marker key plus a separate index entry, which were
     * two structures encoding one fact and could be made to disagree. It is
     * written by {@link MARK_REVOKED_SCRIPT} in a single operation, so there is
     * no window in which the connection is enumerable but not yet revoked.
     * Decision-table home: "whether a revoked connection stays revoked across a
     * reconnect".
     *
     * @param target - The revoked connection id.
     * @throws {Error} If the write fails — `ChannelManager.evict` revokes the
     *   socket anyway and re-throws, so the caller learns durability was lost.
     */
    async markRevoked(target: string): Promise<void> {
        await this.command.command(
            'EVAL',
            MARK_REVOKED_SCRIPT,
            '1',
            this.revocationIndexKey,
            String(this.revocationTtlSeconds),
            target,
            String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),
        )
    }

    /**
     * OPTIONAL (S1/FR-014). The connection ids whose revocation is live now,
     * reaping expired entries so the index stays bounded (#276 FR-002/FR-003).
     *
     * Reap and enumeration happen inside ONE script, against ONE `now` read from
     * Redis — so every surviving member's score is strictly greater than the
     * bound the reap just used, and a live revocation is unremovable. There is
     * no earlier round-trip whose result could go stale before it is acted on.
     *
     * During rollout it also reads the legacy structure (FR-009) so a revocation
     * written by a not-yet-upgraded instance is still enumerated. Only the new
     * index is reaped; legacy markers expire on their own TTL.
     *
     * @returns The currently-revoked connection ids.
     * @example
     * ```ts
     * for (const id of await driver.listRevoked()) { /* revoke if local *\/ }
     * ```
     */
    async listRevoked(): Promise<string[]> {
        const reply = await this.command.command(
            'EVAL',
            LIST_REVOKED_SCRIPT,
            '1',
            this.revocationIndexKey,
        )
        const members = asArray(reply)
        if (members === undefined) {
            // "Nobody is revoked" and "the reply was not the shape we expect"
            // must not look the same to a caller: the first is routine, the
            // second means every revocation this instance owns goes unenforced.
            console.warn(
                'realtime: the revocation index returned an unexpected reply ' +
                    'shape — treating it as empty, so no revocation will be ' +
                    'recovered on this pass',
            )
        }
        const live = new Set<string>()
        for (const raw of members ?? []) {
            const id = asBulk(raw)
            // Filtered, matching what the control-plane ingest has always done
            // to `wire.target`. Both return paths are broker-sourced: a writer
            // with bus access could put anything in the index, and reconcile
            // hands what it finds straight to `revokeLocal`. The asymmetry
            // between the two paths was the finding, not the reach.
            //
            // ONE filter, and it is the boundary rather than belt-and-braces.
            // #304's battery recorded an equivalent mutant here on the grounds
            // that the real guard had moved inside `#legacyRevoked`, which
            // built a Redis key from an unfiltered member before the caller
            // ever saw it. That method is gone (#278) and with it the second
            // path, so this line is the only thing standing between a
            // broker-sourced member and `revokeLocal` — the mutation that
            // removes it is a kill, not an equivalence.
            if (id && isValidName(id)) live.add(id)
        }
        return [...live]
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
     * public channels therefore still reconciles revocations, bounding exposure
     * to a lost evict at ~`reconcileIntervalMs` for EVERY deployment class
     * (closing the FR-014 gap the presence-coupled cadence left open). The timer
     * is cleared by {@link close}.
     *
     * @param handler - Called with no arguments on each reconcile tick.
     */
    onRevocationReconcile(handler: () => void | Promise<void>): void {
        this.revocationHandler = handler
        // Re-registration replaces the previous timer rather than stacking one.
        if (this.revocationTimer !== undefined) {
            clearInterval(this.revocationTimer)
        }
        // The callback RETURNS its promise so a FakeTime `tickAsync` awaits the
        // full re-check round-trip (the same discipline as the sweep timers).
        this.revocationTimer = setInterval(
            () => this.#runRevocationReconcile(),
            this.reconcileIntervalMs,
        )
        // The SECOND trigger (#271): the subscribe socket coming back is the
        // routine moment an `evict` frame was lost, so re-check immediately
        // rather than waiting up to `reconcileIntervalMs`. Registered HERE, in
        // the same method as the timer — "when the revocation re-check runs" has
        // one home, and a future non-revocation consumer of the reconnect signal
        // does not belong in it. Routed through `#runRevocationReconcile` (not
        // the raw handler) so both triggers share its contextual WARN, the only
        // log line naming WHICH control failed.
        this.subscriber.onReconnect?.(() =>
            this.#runRevocationReconcile('reconnect')
        )
    }

    /**
     * Run the registered revocation re-check once. A failure is logged at WARN
     * and never swallowed silently; the timer keeps running so the next pass
     * still bounds exposure to ~`reconcileIntervalMs`.
     *
     * **The trigger is named in the log, and it decides whether a failure is
     * retried (#308).** The two triggers are not equivalent on failure. The
     * timer's next pass is already scheduled, so a failed timer pass costs
     * nothing but latency. The SEAM fires once per outage and its intent is
     * consumed by the activation that fired it, so a failed seam pass is
     * retried by nothing at all — enforcement silently reverts to the periodic
     * timer, which is exactly the pre-#271 exposure the seam exists to remove.
     * The condition is broker-controllable: heal the subscribe socket while
     * stalling the command socket's `EVAL`.
     *
     * Naming the trigger is half the fix on its own. Both lines read
     * identically before this, so an operator watching a WARN stream could not
     * tell that the fast path had been lost rather than a routine pass having
     * failed — and those want different responses.
     *
     * @param trigger - What ran this pass. `reconnect` is the only one that
     *   earns a retry, and `reconnect-retry` is that retry, which does not
     *   retry itself.
     */
    async #runRevocationReconcile(
        trigger: 'timer' | 'reconnect' | 'reconnect-retry' = 'timer',
    ): Promise<void> {
        if (!this.revocationHandler) return
        try {
            await this.revocationHandler()
        } catch (error) {
            console.warn(
                `realtime: revocation reconcile failed (${trigger}): ${
                    renderError(error)
                }`,
            )
            if (trigger !== 'reconnect') return
            // ONE retry, and only one. Chaining would turn a broker that keeps
            // failing into a hot loop against the command socket, which is the
            // opposite of what a bounded enforcement window needs.
            if (this.revocationRetryTimer !== undefined) {
                clearTimeout(this.revocationRetryTimer)
            }
            const id = setTimeout(
                () => void this.#runRevocationReconcile('reconnect-retry'),
                RECONCILE_RETRY_MS,
            )
            // Unref'd: this must never be the reason a process stays alive.
            Deno.unrefTimer(id)
            this.revocationRetryTimer = id
        }
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
        }))
    }

    /**
     * Decode a control-topic payload, verify its authenticity MAC and routing
     * names, and drop self-loopback. Returns the manager-facing
     * {@link ControlMessage} only when every check passes; otherwise `undefined`
     * (logged at WARN — never obeyed, never thrown).
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
            console.warn('realtime: dropped a malformed control payload')
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
            !isPlainMember(wire.member)
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
            (wire.channel !== undefined && !isValidName(wire.channel))
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
            member: wire.member,
        }
    }

    /**
     * Start the instance-liveness heartbeat and the ghost-sweep reconcile pass
     * once, the first time this instance touches the roster. Idempotent; the
     * timers are cleared by {@link close}.
     */
    async #ensureSweepStarted(): Promise<void> {
        if (this.sweepStarted) return
        this.sweepStarted = true
        await this.#heartbeat()
        // Refresh our own liveness key so a live instance is never swept. The
        // callbacks RETURN their promise (rather than voiding it) so a FakeTime
        // `tickAsync` awaits the full round-trip — the sweep's own error handling
        // still swallows nothing (both log at WARN).
        this.heartbeatTimer = setInterval(
            () => this.#heartbeat(),
            this.heartbeatIntervalMs,
        )
        // Sweep the members of any instance whose liveness key has expired.
        this.reconcileTimer = setInterval(
            () => this.#reconcile(),
            this.reconcileIntervalMs,
        )
    }

    /** Register this instance and refresh its liveness key (TTL heartbeat). */
    async #heartbeat(): Promise<void> {
        try {
            await this.command.command(
                'SADD',
                this.instancesKey,
                this.instanceId,
            )
            await this.command.command(
                'SET',
                this.aliveKey(this.instanceId),
                '1',
                'EX',
                String(this.livenessTtlSeconds),
            )
        } catch (error) {
            console.warn(
                `realtime: instance-liveness heartbeat failed: ${
                    renderError(error)
                }`,
            )
        }
    }

    /**
     * Sweep the roster members of every instance whose liveness key has expired
     * (Q1/FR-008), so a crashed instance leaves no permanent ghost members.
     */
    async #reconcile(): Promise<void> {
        try {
            const reply = await this.command.command(
                'SMEMBERS',
                this.instancesKey,
            )
            const ids = asArray(reply) ?? []
            for (const raw of ids) {
                const id = asBulk(raw)
                if (!id || id === this.instanceId) continue
                const alive = asInteger(
                    await this.command.command('EXISTS', this.aliveKey(id)),
                )
                if (alive === 0) await this.#sweepInstance(id)
            }
        } catch (error) {
            console.warn(
                `realtime: roster reconcile failed: ${renderError(error)}`,
            )
        }
        // The durable revocation re-check runs on its OWN dedicated timer
        // (see {@link onRevocationReconcile}), NOT here — it must fire for a
        // presence-free deployment that never starts this ghost-sweep pass.
    }

    /** Remove every roster member owned by a dead instance, then forget it. */
    async #sweepInstance(deadId: string): Promise<void> {
        const reply = await this.command.command(
            'SMEMBERS',
            this.ownedKey(deadId),
        )
        const owned = asArray(reply) ?? []
        let swept = 0
        for (const raw of owned) {
            const entry = asBulk(raw)
            if (!entry) continue
            const sep = entry.indexOf(OWNED_SEP)
            if (sep < 0) continue
            const channel = entry.slice(0, sep)
            const field = entry.slice(sep + 1)
            await this.command.command('HDEL', this.presenceKey(channel), field)
            swept++
        }
        await this.command.command('DEL', this.ownedKey(deadId))
        await this.command.command('SREM', this.instancesKey, deadId)
        console.warn(
            `realtime: swept ${swept} ghost member(s) of dead instance ${
                safeForLog(deadId)
            }`,
        )
    }

    /**
     * Release the connections this driver constructed itself (via
     * {@link fromConfig}) — the subscribe socket first (stops the push read
     * loop), then the command client (drains its QUIT) — and stop the sweep
     * timers. Does NOT proactively drop this instance's roster members: a real
     * crash cannot, so its liveness key simply expires and a surviving instance
     * sweeps it (that is what {@link close} models in the sweep tests).
     * Idempotent; for an injected-port driver it stops the timers and drops the
     * revocation handler, so a later reconnect on the app-owned subscriber
     * revokes nothing (FR-007).
     *
     * @returns Resolves once every owned connection is closed.
     * @example
     * ```ts
     * const driver = RedisBroadcastDriver.fromConfig({ hostname: 'localhost' })
     * await driver.close()
     * ```
     */
    async close(): Promise<void> {
        if (this.heartbeatTimer !== undefined) {
            clearInterval(this.heartbeatTimer)
            this.heartbeatTimer = undefined
        }
        if (this.reconcileTimer !== undefined) {
            clearInterval(this.reconcileTimer)
            this.reconcileTimer = undefined
        }
        if (this.revocationTimer !== undefined) {
            clearInterval(this.revocationTimer)
            this.revocationTimer = undefined
        }
        if (this.revocationRetryTimer !== undefined) {
            clearTimeout(this.revocationRetryTimer)
            this.revocationRetryTimer = undefined
        }
        // Clearing the timer is not enough for the RECONNECT trigger (#271): on
        // the injected-port path `owned` is empty, so the subscriber outlives
        // this driver and can still fire. Dropping the handler makes
        // `#runRevocationReconcile`'s existing guard the ONE gate that quiesces
        // both triggers on both construction paths.
        this.revocationHandler = undefined
        for (const resource of this.owned) {
            await resource.close()
        }
    }
}
