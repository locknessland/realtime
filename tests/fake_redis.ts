/**
 * @fileoverview An in-memory fake Redis for the realtime driver unit tests.
 *
 * It models exactly the command surface the {@link RedisBroadcastDriver} uses —
 * `PUBLISH`, the roster hash (`HSET`/`HDEL`/`HGETALL`, `HLEN`/`HMGET`/
 * `HRANDFIELD … WITHVALUES` for the bounded read, #341, and `HGET` for the
 * roster release, #345), the owned/instances sets
 * (`SADD`/`SREM`/`SMEMBERS`/`DEL`, and `SSCAN … COUNT` for the sweep's paged
 * owned-set read, #358), the revocation index (`ZADD`/`ZREM`,
 * `ZREMRANGEBYSCORE` inside the reap script, and `ZSCAN … COUNT` for the
 * revocation pass's paged read, #359) and the liveness string
 * (`SET … EX`/`EXISTS`) — returning `RespReply`-shaped values so the driver's
 * real reply-narrowing runs unchanged. Pub/sub fan-out is synchronous, like
 * the existing `driver_redis.test.ts` fake bus. `ZRANGEBYSCORE` is still
 * modelled, but since #359 only tests issue it — raw reads of the index; the
 * driver never does.
 *
 * **The scan model** (#358). `SSCAN key cursor COUNT n` is answered by one
 * private scan core (member list, cursor and `COUNT` in, one page out), and
 * `ZSCAN key cursor COUNT n` (#359) is its second caller rather than a walk of
 * its own: it hands the core the members of the LIVE sorted set (key expiry
 * honoured) and answers `[cursor, [member, score, …]]` in the core's order,
 * each score written as Redis writes it — an integral score as its plain
 * digits, a fractional one refused rather than guessed:
 * - **Refused, never ignored**: a missing `COUNT`, any option but `COUNT`
 *   (`MATCH` among them — it filters the page, so ignoring it hands the caller
 *   members a real broker would not), a cursor that is not canonical decimal
 *   (`/^(0|[1-9][0-9]{0,19})$/`), a cursor past the slot table (never one this
 *   fake issued), and a `COUNT` that is not a positive integer.
 * - **A set of at most `COUNT` members, asked at cursor `0`, is answered whole
 *   with cursor `0`.** Every sweep test whose owned set fits one page still
 *   issues exactly one owned-set read. **Deliberate divergence**: Redis answers
 *   whole only for a listpack-encoded set (by default at most 128 entries of at
 *   most 64 bytes), so this fake pages sets of 101–128 short entries that Redis
 *   would answer whole at `COUNT 100`. The fake is the stricter of the two.
 * - **A larger set is walked over a fixed virtual table of
 *   {@link SCAN_TABLE_SLOTS} slots.** A member's slot is a hash of the member
 *   ({@link FakeRedis.scanSlot}), so a removal never shifts another member;
 *   each call visits `COUNT` slots from the cursor and returns their members;
 *   the next cursor is the next unvisited slot, or `0` past the end. Empty
 *   pages with a non-zero cursor therefore happen, and a member present for
 *   the whole iteration is returned **exactly once**. A test places a member
 *   ahead of or behind a cursor with `FakeRedis.scanSlot`, and predicts the
 *   walk's order with `FakeRedis.scanOrder` — the one ordering the core sorts
 *   by, slot then byte order.
 * - **Not modelled**: duplicates (a real broker may return a member twice
 *   across a rehash) and the rehash itself — the table never resizes. #358's
 *   W3 proves duplicates are safe with two sweepers instead; the revocation
 *   pass absorbs one by keying its result on the member. `ZSCAN`'s
 *   `NOSCORES` (Redis 8) is refused like `MATCH`.
 * - **A call ceiling, per key and cumulative**: more than
 *   {@link SCAN_CALLS_PER_KEY} scan calls on one key, or
 *   {@link SCAN_CALLS_TOTAL} on one fake, is a ledger rejection that throws,
 *   so a loop that stops advancing fails instead of hanging. A per-iteration
 *   ceiling could not catch a caller that restarts at cursor `0` every call.
 *   Sizing: the longest legitimate iteration is the #285 coverage case at
 *   `COUNT 10`, `ceil(1024 / 10)` = 103 calls, and the busiest witness
 *   (#358 W6) runs a few iterations of `ceil(1024 / 100)` = 11 — both far
 *   under 1,000 per key, and no test file sweeps ten such keys. **The
 *   revocation index is scanned for the whole test** (#359): every driver on
 *   the fake reads it once per revocation pass, on one shared key. Measured
 *   on `15997dd5` by counting the index reads per fake across the realtime
 *   suite, the busiest test runs fewer than 20 passes in all; an index that
 *   fits one page costs one call per pass, and the largest witness index
 *   (#359 R1, 307 records) costs `ceil(1024 / 100)` = 11 per pass. Tens to a
 *   few hundred calls per key, so the ceiling stands as it is.
 *
 * **One clock and one expiry registry** (#280). Every arm reads `#now()`, which
 * {@link FakeRedis.setTime} overrides, and every TTL lives in `#keyExpiry` in
 * epoch seconds regardless of the value's type. Before that there were two of
 * each: `EXPIRE` and the sorted-set TTL honoured `setTime()` while `SET … EX`
 * and the string liveness check read the wall clock, so advancing the clock
 * expired sorted sets and never expired string keys — and either half could be
 * the one a test believed.
 *
 * **An option this fake does not model is REFUSED, never ignored.** #276
 * shipped a security fix that was wrong twice with a green suite because an
 * ignored option token made an inert guard look like a working one. Refusals go
 * through a ledger, so a driver that catches and warns — which it does around
 * its heartbeat, its reconcile and its revocation pass — cannot turn a
 * modelled divergence back into silence: call
 * {@link FakeRedis.assertNoRejections} in a teardown.
 *
 * **Not everything is modelled, and that is stated rather than implied.** The
 * arms cover the commands the driver issues; anything else throws from the
 * `default:`. Within a modelled arm, an unread argument is a bug — see the
 * pitfall in `packages/realtime/AGENTS.md`.
 *
 * **Every hash command now refuses a key some OTHER type already holds
 * (#414)**: `HSET`/`HGET`/`HDEL`/`HLEN`/`HMGET`/`HRANDFIELD`/`HGETALL` each
 * call `#assertHashKey` first, raising the same `WRONGTYPE` text a real
 * broker would. Before this every hash arm wrote into its own `#hashes` map
 * unconditionally, so a presence or holders key corrupted by a raw
 * `SET`/`SADD`/`ZADD` read as merely ABSENT to a later `HGET` — indistinguishable
 * from a key that never existed — and a fail-closed witness had no throw to
 * observe. `TYPE` (#405) already answers the same cross-type question for a
 * read-only probe; this is the write/read family's own version of it.
 *
 * **`setTime()` now reaches the liveness key.** Unifying the clock means a
 * test that moves the fake clock by hundreds of seconds expires the driver's
 * `SET … EX` alive key too, where before it never expired inside a test.
 * The reconcile pass runs on a self-re-arming `setTimeout` (#355), armed again
 * only when a pass settles, so it does not fire inside a sub-millisecond test
 * on the real clock — but a test that drives it with `FakeTime` and also moves
 * the fake Redis clock will see instances swept that used to read as alive.
 * Stated here rather than left to be discovered.
 *
 * A test helper — never imported by production code.
 *
 * @module @lockness/realtime/tests/fake_redis
 */

import { evalLua, type LuaValue } from '../../redis/tests/lua_eval.ts'

/** A push-message handler for a subscribed pattern. */
type Handler = (topic: string, payload: string) => void

/** A `RespReply`-shaped value (the subset the driver narrows). */
type Reply =
    | { type: 'simple'; value: string }
    | { type: 'integer'; value: number }
    | { type: 'bulk'; value: string }
    | { type: 'array'; value: Reply[] }
    | { type: 'nil' }

/**
 * A reply as a script sees it, by Redis's reply-to-Lua conversion (#341).
 *
 * An integer stays a number, and a nil reply becomes `false` — top-level
 * (`HGET` of an absent field, #345) or an ELEMENT of a multi-bulk. Redis never
 * hands Lua a `nil` for a reply: a table keeps its length, and a script's
 * `mine == false` is how it asks "was it there". The earliest bridge turned
 * both into strings (`'3'`, `''`), which let a returned table carry a shape no
 * real broker sends; the next mapped a top-level nil to `undefined`, which
 * would have made `mine == false` false on the fake and true on Redis.
 */
function toLua(reply: Reply): LuaValue {
    switch (reply.type) {
        case 'array':
            return reply.value.map((r) => r.type === 'nil' ? false : toLua(r))
        case 'integer':
            return reply.value
        case 'nil':
            return false
        default:
            return reply.value
    }
}

/**
 * What a script returns, as the client receives it: a number is an integer
 * reply, `false` a nil bulk, a table an array — recursively.
 */
function fromLua(value: Exclude<LuaValue, undefined>): Reply {
    if (Array.isArray(value)) {
        return {
            type: 'array',
            value: (value as readonly Exclude<LuaValue, undefined>[]).map(
                fromLua,
            ),
        }
    }
    if (value === false) return { type: 'nil' }
    if (typeof value === 'number') {
        return { type: 'integer', value: Math.trunc(value) }
    }
    return { type: 'bulk', value: value as string }
}

/** Convert a Redis glob (`*`, `?`) to an anchored RegExp, escaping the rest. */
function globToRegExp(glob: string): RegExp {
    let out = '^'
    for (const ch of glob) {
        if (ch === '*') out += '.*'
        else if (ch === '?') out += '.'
        else out += ch.replace(/[.+^${}()|[\]\\]/g, '\\$&')
    }
    return new RegExp(out + '$')
}

/**
 * The slots of the scan core's fixed virtual table (#358): a power of two, as
 * a real hash table's size is, and large enough that a 307-member owned set
 * (#358 W1) spans many `COUNT 100` pages. It never resizes, so there is no
 * rehash to model.
 */
const SCAN_TABLE_SLOTS = 1024

/**
 * Scan calls one key may receive before the fake refuses the next (#358). See
 * the header's scan model for the sizing.
 */
const SCAN_CALLS_PER_KEY = 1_000

/** Scan calls one fake may receive, across every key, before it refuses. */
const SCAN_CALLS_TOTAL = 10_000

/** A canonical scan cursor: `0`, or up to 20 digits with no leading zero. */
const SCAN_CURSOR = /^(0|[1-9][0-9]{0,19})$/

/**
 * An in-memory Redis double: shared by several driver instances in one test so
 * their rosters and control bus are genuinely cross-instance.
 */
export class FakeRedis {
    readonly #hashes = new Map<string, Map<string, string>>()
    readonly #sets = new Map<string, Set<string>>()
    /** Sorted sets: key → (member → score). Backs the revocation index (#276). */
    readonly #zsets = new Map<string, Map<string, number>>()
    /**
     * Key-level expiry in epoch seconds, as `EXPIRE` sets it.
     *
     * Modelled rather than stubbed: a no-op `EXPIRE` arm would hide a script
     * that shortens a whole key's lifetime under live members — which is
     * precisely the defect this had to catch (#276 review HIGH-1).
     */
    readonly #keyExpiry = new Map<string, number>()
    /**
     * Every command this double executes, script-internal ones included.
     *
     * A test that wraps the `command` function sees only what the driver issues
     * directly — the commands a script runs reach `#exec` through the evaluator
     * and bypass that wrapper entirely, which made one assertion structurally
     * unable to fail (#276 review cycle 2).
     */
    readonly #commandLog: string[][] = []
    /**
     * The seconds this instance's `TIME` reports.
     *
     * Settable, and that is the point: the real defect #276 removes is a
     * liveness decision made on an INSTANCE's clock, so a test has to be able to
     * give two drivers different clocks and show the outcome does not move. The
     * fake's shared `Date.now()` could never express that.
     */
    #timeSeconds: number | undefined
    readonly #strings = new Map<string, string>()

    /**
     * Every option this fake refused, kept so a swallowed throw still fails.
     *
     * The driver `console.warn`s around its heartbeat, its reconcile and its
     * revocation pass, so a throw from here goes SILENT on exactly the paths
     * these guards were added for — the #276 shape, one layer down, where the
     * suite stays green because nothing asserted the effect. A consumer suite
     * calls {@link assertNoRejections} in a teardown and the run fails anyway.
     */
    readonly #rejections: string[] = []
    readonly #subs: Array<{ re: RegExp; handler: Handler }> = []
    /** Scan calls per key, for the per-key ceiling (#358). */
    readonly #scanCalls = new Map<string, number>()
    /** Scan calls across every key, for the cumulative ceiling (#358). */
    #scanTotal = 0

    /**
     * The slot of the scan core's virtual table that `member` lives in (#358):
     * FNV-1a over its UTF-8 bytes, modulo the table size. Deterministic and
     * member-derived, so removing one member never moves another, and a test
     * can place a member ahead of a cursor (`scanSlot(m) >= cursor`) or behind
     * it. Command-neutral: every scan arm walks the same slots.
     *
     * @param member - The member, byte for byte as stored.
     * @returns Its slot, in `[0, table size)`.
     * @example
     * ```typescript
     * const behind = FakeRedis.scanSlot(`presence-room 7`) < Number(cursor)
     * ```
     */
    static scanSlot(member: string): number {
        let hash = 0x811c9dc5
        for (const byte of new TextEncoder().encode(member)) {
            hash ^= byte
            hash = Math.imul(hash, 0x01000193)
        }
        return (hash >>> 0) % SCAN_TABLE_SLOTS
    }

    /**
     * The order the scan core visits members in (#358): by
     * {@link FakeRedis.scanSlot}, then byte order within a slot. The scan core
     * sorts every page with it, so a test that predicts the walk sorts with
     * this rather than restating the tie-break — the two cannot drift.
     *
     * @param a - A member, byte for byte as stored.
     * @param b - Another member.
     * @returns Negative when `a` is visited first, positive when `b` is, 0
     *   when they are the same member.
     * @example
     * ```typescript
     * const walk = [...members].sort(FakeRedis.scanOrder)
     * ```
     */
    static scanOrder(a: string, b: string): number {
        return FakeRedis.scanSlot(a) - FakeRedis.scanSlot(b) ||
            (a < b ? -1 : a > b ? 1 : 0)
    }

    /**
     * The private scan core (#358): one page of `members` from `cursor`,
     * visiting `count` slots. Every scan arm is a caller; none walks its own.
     *
     * At cursor `0` a collection of at most `count` members is answered whole
     * with cursor `0` (the listpack shape, stricter than Redis — see the
     * header). Otherwise the page is the members whose slot lies in
     * `[cursor, cursor + count)`, in slot order, and the next cursor is
     * `cursor + count`, or `0` once that passes the table.
     *
     * @param key - The key scanned, for the per-key ceiling.
     * @param members - The collection's members at the time of the call.
     * @param cursor - A canonical cursor, already checked by the arm.
     * @param count - Slots to visit, a positive integer checked by the arm.
     * @returns The next cursor and the page's members.
     * @throws Through the ledger, past either call ceiling or for a cursor
     *   past the table.
     */
    #scan(
        key: string,
        members: readonly string[],
        cursor: string,
        count: number,
    ): { cursor: string; members: string[] } {
        const calls = (this.#scanCalls.get(key) ?? 0) + 1
        this.#scanCalls.set(key, calls)
        this.#scanTotal++
        if (calls > SCAN_CALLS_PER_KEY || this.#scanTotal > SCAN_CALLS_TOTAL) {
            this.#reject(
                `FakeRedis: scan call ceiling passed (${calls} on this key, ` +
                    `${this.#scanTotal} in all) — a scan that never ` +
                    'advances its cursor',
            )
        }
        const from = Number(cursor)
        if (from >= SCAN_TABLE_SLOTS) {
            this.#reject(
                `FakeRedis: scan cursor ${cursor} lies past the ` +
                    `${SCAN_TABLE_SLOTS}-slot table — never one this fake issued`,
            )
        }
        if (from === 0 && members.length <= count) {
            return { cursor: '0', members: [...members] }
        }
        const to = from + count
        const page = members
            .map((member) => ({ member, slot: FakeRedis.scanSlot(member) }))
            .filter(({ slot }) => slot >= from && slot < to)
            .map(({ member }) => member)
            .sort(FakeRedis.scanOrder)
        return {
            cursor: to >= SCAN_TABLE_SLOTS ? '0' : String(to),
            members: page,
        }
    }

    /** The command client each driver publishes and stores state through. */
    readonly command = (...args: string[]): Promise<unknown> =>
        Promise.resolve(this.#exec(args))

    /**
     * A fresh subscriber whose `psubscribe` registrations share this instance.
     *
     * It also implements the optional reconnect seam (#271), so a test can
     * simulate a subscribe-socket reconnect with `fireReconnect()` — the real
     * `RedisSubscribeConnection` fires it after a fault-triggered re-`PSUBSCRIBE`.
     * `driver_redis.test.ts` keeps its own fake WITHOUT the seam on purpose: it
     * is the standing proof that an unaware subscriber still works (FR-004).
     */
    subscriberFor(): {
        psubscribe(pattern: string, handler: Handler): void
        onReconnect(handler: () => void | Promise<void>): void
        /**
         * Test-only: simulate a reconnect. It awaits whatever the registered
         * handler returns — and since #359 the Redis driver's handler starts a
         * revocation pass and returns NOTHING (a coalesced trigger would
         * otherwise read another pass's end as its own). So awaiting this does
         * not wait for the pass: drain with `time.runMicrotasks()` after it,
         * as for a timer-fired pass.
         */
        fireReconnect(): Promise<void>
    } {
        let onReconnect: (() => void | Promise<void>) | undefined
        return {
            psubscribe: (pattern, handler) =>
                void this.#subs.push({ re: globToRegExp(pattern), handler }),
            onReconnect: (handler) => void (onReconnect = handler),
            fireReconnect: async () => {
                await onReconnect?.()
            },
        }
    }

    /**
     * Refuse a command shape, recording it so a swallowed throw still counts.
     *
     * @param message - What was refused and why.
     * @throws Always.
     */
    #reject(message: string): never {
        this.#rejections.push(message)
        throw new Error(message)
    }

    /**
     * Throw if any command was refused during the run.
     *
     * Call it in a teardown. A driver that catches and warns turns a modelled
     * divergence back into the silence this fake exists to break.
     *
     * @throws If any command was rejected.
     * @example
     * ```typescript
     * } finally {
     *     redis.assertNoRejections()
     * }
     * ```
     */
    assertNoRejections(): void {
        if (this.#rejections.length === 0) return
        throw new Error(
            `FakeRedis refused ${this.#rejections.length} command(s), and the ` +
                `caller swallowed each one:\n  ${
                    this.#rejections.join('\n  ')
                }`,
        )
    }

    /**
     * Whether `key`'s key-level TTL has passed.
     *
     * `#keyExpiry` is authoritative for EVERY type. It used to be consulted
     * only through `#liveZset`, so `EXPIRE` on a string, a set or a hash was
     * inert for existence and `DEL` over four logically-expired keys returned
     * 3 where Redis returns 0.
     */
    #expired(key: string): boolean {
        const at = this.#keyExpiry.get(key)
        return at !== undefined && this.#now() >= at
    }

    /**
     * Whether `key` holds anything at all, of any type.
     *
     * Redis's keyspace is one namespace across every type; this fake keeps a
     * map per type, so "does it exist" has to ask all of them. `EXISTS` asked
     * only `#strings`, which made it answer 0 for every set, hash and sorted
     * set in the store — including the revocation index.
     */
    #keyExists(key: string): boolean {
        if (this.#expired(key)) {
            this.#dropKey(key)
            return false
        }
        return this.#strings.has(key) || this.#sets.has(key) ||
            this.#hashes.has(key) || this.#zsets.has(key)
    }

    /**
     * Delete `key` from every store, reporting whether anything went.
     *
     * The key-level TTL goes with it: leaving an entry in `#keyExpiry` for a
     * deleted key would make a later re-creation inherit an expiry it never
     * asked for.
     */
    /**
     * Drop `key` if the collection at it is now empty.
     *
     * Redis removes a key when its last element goes; this fake did not, so
     * `SREM`, `HDEL` and `ZREMRANGEBYSCORE` each left an empty container
     * behind. That was invisible while `EXISTS` consulted only `#strings` —
     * making `EXISTS` see every type made it observable, and wrong on the #276
     * revocation index itself, which the reap drives to empty by design.
     */
    #dropIfEmpty(key: string): void {
        if (
            this.#sets.get(key)?.size === 0 ||
            this.#hashes.get(key)?.size === 0 ||
            this.#zsets.get(key)?.size === 0
        ) {
            this.#dropKey(key)
        }
    }

    #dropKey(key: string): boolean {
        const existed = this.#strings.has(key) || this.#sets.has(key) ||
            this.#hashes.has(key) || this.#zsets.has(key)
        this.#strings.delete(key)
        this.#sets.delete(key)
        this.#hashes.delete(key)
        this.#zsets.delete(key)
        this.#keyExpiry.delete(key)
        return existed
    }

    /**
     * Override what `TIME` reports for this double, in whole seconds.
     *
     * @param seconds - The epoch seconds to report, or `undefined` to track the
     *   real clock again.
     * @example
     * ```typescript
     * redis.setTime(1_800_000_000)
     * ```
     */
    setTime(seconds: number | undefined): void {
        this.#timeSeconds = seconds
    }

    /**
     * How many members a sorted set holds, expired ones included.
     *
     * Test-only, and load-bearing: a reap that does nothing is invisible to any
     * assertion that reads `listRevocations()`'s return value, because the caller
     * filters by score anyway. Only stored cardinality can see it (#276).
     *
     * @param key - The sorted-set key.
     * @returns The number of stored members.
     * @example
     * ```typescript
     * assertEquals(redis.zcard('app:rt:revocations'), 0)
     * ```
     */
    /**
     * Every command executed, in order — including those issued from inside a
     * script. Test-only.
     *
     * @returns A copy of the command log.
     * @example
     * ```typescript
     * assert(!redis.commandLog().some(([c, k]) => c === 'SADD' && k === legacy))
     * ```
     */
    commandLog(): string[][] {
        return this.#commandLog.map((c) => [...c])
    }

    /**
     * The key-level expiry recorded for `key`, in epoch seconds, or `undefined`
     * when it has none. Test-only.
     *
     * @param key - The key to inspect.
     * @returns The expiry, or `undefined` for a persistent key.
     */
    expiryOf(key: string): number | undefined {
        return this.#keyExpiry.get(key)
    }

    zcard(key: string): number {
        return this.#liveZset(key)?.size ?? 0
    }

    /** The epoch seconds `TIME` reports — overridden by {@link setTime}. */
    #now(): number {
        return this.#timeSeconds ?? Math.floor(Date.now() / 1000)
    }

    /**
     * The sorted set at `key`, or `undefined` — dropping it first if its
     * key-level TTL has passed, as Redis would.
     */
    #liveZset(key: string): Map<string, number> | undefined {
        if (this.#expired(key)) this.#dropKey(key)
        return this.#zsets.get(key)
    }

    /**
     * The hash at `key`, or `undefined` — dropping it first if its key-level
     * TTL has passed, as Redis would.
     */
    #liveHash(key: string): Map<string, string> | undefined {
        if (this.#expired(key)) this.#dropKey(key)
        return this.#hashes.get(key)
    }

    /**
     * Refuse a hash command against a key some OTHER type already holds
     * (#414): every hash arm below wrote into `#hashes` unconditionally,
     * never checking `#strings`/`#sets`/`#zsets` first, so a key corrupted by
     * a raw `SET`/`SADD`/`ZADD` before a hash command reached it read as
     * simply absent — `#liveHash(key)?.get(...)` returns `undefined` for a
     * key that is not a hash exactly as it does for one that does not exist
     * at all, so no WRONGTYPE ever surfaced and a fail-closed witness had
     * nothing to observe. Real Redis raises `WRONGTYPE Operation against a
     * key holding the wrong kind of value` for every hash command against a
     * non-hash key, reads included — the same message `SET … GET` already
     * raises here for a foreign key at the string arm. Checked BEFORE the
     * command's own work, exactly where a real broker's type check runs.
     *
     * @param key - The key a hash command is about to touch.
     * @throws When `key` is expired-and-dropped, this is a no-op; otherwise,
     *   when it holds a string, a set or a sorted set, it throws the same
     *   `WRONGTYPE` text a real broker would.
     */
    #assertHashKey(key: string): void {
        if (this.#expired(key)) this.#dropKey(key)
        if (this.#hashes.has(key)) return
        if (
            this.#strings.has(key) || this.#sets.has(key) ||
            this.#zsets.has(key)
        ) {
            this.#reject(
                'WRONGTYPE Operation against a key holding the wrong kind ' +
                    'of value',
            )
        }
    }

    /**
     * Refuse a set command against a key some OTHER type already holds
     * (#414), the same guard as {@link FakeRedis.#assertHashKey} for the SET
     * family: `SADD`/`SREM` wrote into `#sets` unconditionally, so a
     * hash/string/zset-typed owned or instances key silently absorbed a raw
     * `SADD` — including the boot heartbeat's OWN unconditional
     * `SADD instances <id>`, issued outside any script, which would
     * otherwise "heal" a corrupted instances key by accident before
     * `INSTANCES_HEAL`'s own `TYPE` read ever saw the corruption.
     *
     * @param key - The key a set command is about to touch.
     * @throws When `key` is expired-and-dropped, this is a no-op; otherwise,
     *   when it holds a string, a hash or a sorted set, it throws the same
     *   `WRONGTYPE` text a real broker would.
     */
    #assertSetKey(key: string): void {
        if (this.#expired(key)) this.#dropKey(key)
        if (this.#sets.has(key)) return
        if (
            this.#strings.has(key) || this.#hashes.has(key) ||
            this.#zsets.has(key)
        ) {
            this.#reject(
                'WRONGTYPE Operation against a key holding the wrong kind ' +
                    'of value',
            )
        }
    }

    /**
     * A score as Redis writes it into a reply (#359): an integral score of at
     * most 2^52 in magnitude is its plain digits — no decimal point, no
     * exponent — which is every score the driver writes (`TIME` seconds plus a
     * TTL). Redis switches to a shortest-float form past that bound and for a
     * fractional score, and the exact digits of that form are not modelled
     * here: such a score is REFUSED rather than formatted the way JavaScript
     * happens to.
     */
    #formatScore(score: number): string {
        if (!Number.isInteger(score) || Math.abs(score) > 2 ** 52) {
            this.#reject(
                `FakeRedis: unmodelled score format for ${score} — only ` +
                    'integral scores up to 2^52 are written as Redis does',
            )
        }
        return String(score)
    }

    /** Parse a `ZRANGEBYSCORE` bound, honouring `-inf` / `+inf` and `(` exclusivity. */
    #bound(raw: string): { value: number; exclusive: boolean } {
        const exclusive = raw.startsWith('(')
        const body = exclusive ? raw.slice(1) : raw
        if (body === '-inf') return { value: -Infinity, exclusive }
        if (body === '+inf') return { value: Infinity, exclusive }
        return { value: Number(body), exclusive }
    }

    #exec(args: string[]): Reply {
        this.#commandLog.push([...args])
        const [cmd, ...rest] = args
        switch (cmd.toUpperCase()) {
            case 'TIME':
                return {
                    type: 'array',
                    value: [
                        { type: 'bulk', value: String(this.#now()) },
                        { type: 'bulk', value: '0' },
                    ],
                }
            case 'ZADD': {
                // ZADD key [GT] score member — GT is MODELLED, not ignored: an
                // ignored option token is exactly the silent-no-op class FR-008
                // exists to stop, and #276 depends on GT to guarantee a
                // re-eviction can only extend a live revocation.
                const [key, ...tail] = rest
                // Flags are read from the LEADING position only. The old scan
                // ran `filter` over the whole tail, so a MEMBER named `gt` was
                // silently accepted as a flag and one named `nx` raised a
                // spurious rejection — the argument list decided by content
                // rather than by position.
                let cursor = 0
                let gt = false
                while (cursor < tail.length) {
                    const token = tail[cursor].toUpperCase()
                    if (!['GT', 'LT', 'NX', 'XX', 'CH'].includes(token)) break
                    if (token !== 'GT') {
                        this.#reject(
                            `FakeRedis: unmodelled ZADD option ${tail[cursor]}`,
                        )
                    }
                    gt = true
                    cursor++
                }
                const pairs = tail.slice(cursor)
                if (pairs.length === 0 || pairs.length % 2 !== 0) {
                    this.#reject(
                        'FakeRedis: ZADD needs score/member pairs, got ' +
                            `${pairs.length} argument(s)`,
                    )
                }
                const zset = this.#liveZset(key) ?? new Map<string, number>()
                this.#zsets.set(key, zset)
                // VARIADIC. It read one pair and dropped the rest — the same
                // "a second argument silently dropped" class DEL, EXISTS, HSET
                // and HDEL were all fixed for, left in the one arm #276
                // actually depends on.
                let added = 0
                for (let i = 0; i < pairs.length; i += 2) {
                    const score = Number(pairs[i])
                    if (!Number.isFinite(score)) {
                        this.#reject(
                            `FakeRedis: ZADD score must be a number, got '${
                                pairs[i]
                            }'`,
                        )
                    }
                    const member = pairs[i + 1]
                    const existing = zset.get(member)
                    if (existing === undefined) added++
                    if (existing === undefined || !gt || score > existing) {
                        zset.set(member, score)
                    }
                }
                return { type: 'integer', value: added }
            }
            case 'ZREM': {
                // ZREM key member [member ...] — every member IS read, so
                // there is no unmodelled argument to refuse (#280). The reply
                // is the number actually removed, which is what lets a caller
                // tell "cleared" from "it was already gone".
                const [key, ...members] = rest
                if (members.length === 0) {
                    throw new Error('FakeRedis: ZREM needs at least one member')
                }
                const zset = this.#liveZset(key)
                if (!zset) return { type: 'integer', value: 0 }
                let removed = 0
                for (const member of members) {
                    if (zset.delete(member)) removed++
                }
                // Redis drops a key when its last element goes. The revocation
                // index is driven to empty by design — by the reap, and now by
                // clear-on-apply — so a fake that kept an empty zset here would
                // make `EXISTS` disagree with the real broker on exactly the
                // key this feature clears most often.
                this.#dropIfEmpty(key)
                return { type: 'integer', value: removed }
            }
            case 'ZREMRANGEBYSCORE': {
                const [key, rawMin, rawMax] = rest
                const min = this.#bound(rawMin)
                const max = this.#bound(rawMax)
                const zset = this.#liveZset(key)
                if (!zset) return { type: 'integer', value: 0 }
                let removed = 0
                for (const [member, score] of [...zset]) {
                    const aboveMin = min.exclusive
                        ? score > min.value
                        : score >= min.value
                    const belowMax = max.exclusive
                        ? score < max.value
                        : score <= max.value
                    if (aboveMin && belowMax) {
                        zset.delete(member)
                        removed++
                    }
                }
                this.#dropIfEmpty(key)
                return { type: 'integer', value: removed }
            }
            case 'ZRANGEBYSCORE': {
                // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] —
                // neither option is modelled, and both change the REPLY SHAPE,
                // so ignoring one hands the caller a different array than Redis
                // would and nothing says so.
                const [key, rawMin, rawMax, ...opts] = rest
                if (opts.length > 0) {
                    this.#reject(
                        `FakeRedis: unmodelled ZRANGEBYSCORE option(s) ${
                            opts.join(' ')
                        }`,
                    )
                }
                const min = this.#bound(rawMin)
                const max = this.#bound(rawMax)
                const zset = this.#liveZset(key)
                if (!zset) return { type: 'array', value: [] }
                const members = [...zset]
                    .filter(([, score]) => {
                        const aboveMin = min.exclusive
                            ? score > min.value
                            : score >= min.value
                        const belowMax = max.exclusive
                            ? score < max.value
                            : score <= max.value
                        return aboveMin && belowMax
                    })
                    // Score first, then MEMBER lexicographically — Redis's own
                    // tie-break. A stable sort left ties in insertion order,
                    // and revocation scores are whole seconds from `TIME`, so
                    // two evictions in one second tie routinely.
                    .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : 1))
                    .map(([member]): Reply => ({ type: 'bulk', value: member }))
                return { type: 'array', value: members }
            }
            case 'EXPIRE': {
                // EXPIRE key seconds [GT] — GT is MODELLED: it is what stops a
                // shorter-TTL instance pulling in the whole key's lifetime and
                // taking live members down with it.
                const [key, rawSeconds, ...flags] = rest
                const known = ['NX', 'GT']
                const unsupported = flags.filter((f) =>
                    !known.includes(f.toUpperCase())
                )
                if (unsupported.length > 0) {
                    this.#reject(
                        `FakeRedis: unmodelled EXPIRE option(s) ${
                            unsupported.join(', ')
                        }`,
                    )
                }
                // Redis returns 0 and records NOTHING for a key that is not
                // there. Writing the expiry anyway let a later create inherit a
                // TTL it never asked for — the hazard `#dropKey` exists to
                // prevent, closed on DEL and left open here.
                if (!this.#keyExists(key)) return { type: 'integer', value: 0 }
                const nx = flags.some((f) => f.toUpperCase() === 'NX')
                const gt = flags.some((f) => f.toUpperCase() === 'GT')
                const at = this.#now() + Number(rawSeconds)
                const current = this.#keyExpiry.get(key)
                // NX: only when the key currently has NO expiry.
                if (nx && current !== undefined) {
                    return { type: 'integer', value: 0 }
                }
                // GT: only when the new expiry is greater than the current one.
                // A key with no TTL counts as an INFINITE one, so GT refuses it —
                // modelling this the other way round is what let an inert
                // `EXPIRE … GT` look like a working guard (#276 review cycle 2).
                if (gt && (current === undefined || at <= current)) {
                    return { type: 'integer', value: 0 }
                }
                this.#keyExpiry.set(key, at)
                return { type: 'integer', value: 1 }
            }
            case 'EVAL': {
                // EVAL script numkeys k1..kN a1..aM — split by the DECLARED
                // numkeys, then evaluate the script itself. Never dispatch on
                // script text.
                const [script, rawNumKeys, ...operands] = rest
                const numKeys = Number(rawNumKeys)
                const keys = operands.slice(0, numKeys)
                const argv = operands.slice(numKeys)
                const result = evalLua(
                    script,
                    keys,
                    argv,
                    // The ONE place a reply becomes a Lua value, nil included:
                    // Redis hands a script `false` for a nil reply (`HGET` of
                    // an absent field), and the #345 release script's
                    // `mine == false` reads exactly that. No arm answers
                    // `false` itself.
                    (command, cargs) => toLua(this.#exec([command, ...cargs])),
                )
                if (result === undefined) return { type: 'nil' }
                return fromLua(result)
            }
            case 'PUBLISH': {
                if (rest.length !== 2) {
                    this.#reject(
                        `FakeRedis: PUBLISH takes topic and payload, got ${rest.length}`,
                    )
                }
                const [topic, payload] = rest
                let n = 0
                for (const s of this.#subs) {
                    if (s.re.test(topic)) {
                        s.handler(topic, payload)
                        n++
                    }
                }
                return { type: 'integer', value: n }
            }
            case 'HSET': {
                // HSET key field value [field value ...] -> fields ADDED, not
                // fields written. A trailing field with no value is an error in
                // Redis, and half-applying it here would leave a store no real
                // sequence of commands could produce.
                const [key, ...pairs] = rest
                this.#assertHashKey(key)
                if (pairs.length === 0 || pairs.length % 2 !== 0) {
                    this.#reject(
                        'FakeRedis: HSET needs field/value pairs, got ' +
                            `${pairs.length} argument(s) after the key`,
                    )
                }
                let h = this.#hashes.get(key)
                if (!h) this.#hashes.set(key, h = new Map())
                let added = 0
                for (let i = 0; i < pairs.length; i += 2) {
                    if (!h.has(pairs[i])) added++
                    h.set(pairs[i], pairs[i + 1])
                }
                return { type: 'integer', value: added }
            }
            case 'HDEL': {
                // HDEL key field [field ...] -> the count actually removed.
                const [key, ...fields] = rest
                this.#assertHashKey(key)
                const h = this.#hashes.get(key)
                let removed = 0
                for (const field of fields) if (h?.delete(field)) removed++
                this.#dropIfEmpty(key)
                return { type: 'integer', value: removed }
            }
            case 'HGETALL': {
                if (rest.length !== 1) {
                    this.#reject(
                        `FakeRedis: HGETALL takes one key, got ${rest.length}`,
                    )
                }
                this.#assertHashKey(rest[0])
                const h = this.#hashes.get(rest[0])
                const flat: Reply[] = []
                for (const [field, value] of h ?? []) {
                    flat.push({ type: 'bulk', value: field })
                    flat.push({ type: 'bulk', value })
                }
                return { type: 'array', value: flat }
            }
            case 'HGET': {
                // HGET key field -> the value, or a nil reply for an absent
                // field or key. Nil, never `false`: turning it into Lua's
                // `false` is the EVAL bridge's job, once, for every command.
                if (rest.length !== 2) {
                    this.#reject(
                        `FakeRedis: HGET takes key and field, got ${rest.length}`,
                    )
                }
                this.#assertHashKey(rest[0])
                const value = this.#liveHash(rest[0])?.get(rest[1])
                return value === undefined
                    ? { type: 'nil' }
                    : { type: 'bulk', value }
            }
            case 'HLEN': {
                // HLEN key -> the number of fields; 0 for an absent key.
                if (rest.length !== 1) {
                    this.#reject(
                        `FakeRedis: HLEN takes one key, got ${rest.length}`,
                    )
                }
                this.#assertHashKey(rest[0])
                return {
                    type: 'integer',
                    value: this.#liveHash(rest[0])?.size ?? 0,
                }
            }
            case 'HMGET': {
                // HMGET key field [field ...] -> one reply per field, nil for
                // an absent one. ZERO fields is an arity error on a real
                // broker; answering `[]` here is exactly what would hide a
                // script issuing HMGET with no self id (#341 A1).
                const [key, ...fields] = rest
                if (key === undefined || fields.length === 0) {
                    this.#reject('FakeRedis: HMGET needs at least one field')
                }
                this.#assertHashKey(key)
                const h = this.#liveHash(key)
                return {
                    type: 'array',
                    value: fields.map((field): Reply => {
                        const value = h?.get(field)
                        return value === undefined
                            ? { type: 'nil' }
                            : { type: 'bulk', value }
                    }),
                }
            }
            case 'HRANDFIELD': {
                // HRANDFIELD key count WITHVALUES, count a positive integer —
                // the one form the driver issues, and the only one modelled.
                // A negative count REPEATS pairs on a real broker and the other
                // forms change the reply shape, so each is refused.
                //
                // count >= size: the whole hash, in HGETALL order — what a
                // real broker returns. count < size: the FIRST `count` in
                // insertion order, which is one outcome a real broker's random
                // pick can produce, and nothing a test may rely on beyond
                // "count distinct pairs".
                const [key, rawCount, withValues, ...extra] = rest
                const count = Number(rawCount)
                if (
                    key === undefined || rawCount === undefined ||
                    !/^\d+$/.test(rawCount) || !Number.isSafeInteger(count) ||
                    withValues?.toUpperCase() !== 'WITHVALUES' ||
                    extra.length > 0
                ) {
                    this.#reject(
                        `FakeRedis: HRANDFIELD models only 'key count WITHVALUES' ` +
                            `with a non-negative integer count, got '${
                                rest.join(' ')
                            }'`,
                    )
                }
                this.#assertHashKey(key)
                const flat: Reply[] = []
                for (const [field, value] of this.#liveHash(key) ?? []) {
                    if (flat.length === count * 2) break
                    flat.push({ type: 'bulk', value: field })
                    flat.push({ type: 'bulk', value })
                }
                return { type: 'array', value: flat }
            }
            case 'SADD': {
                const [key, ...members] = rest
                this.#assertSetKey(key)
                if (members.length === 0) {
                    this.#reject('FakeRedis: SADD needs at least one member')
                }
                let set = this.#sets.get(key)
                if (!set) this.#sets.set(key, set = new Set())
                let added = 0
                for (const m of members) {
                    if (!set.has(m)) added++
                    set.add(m)
                }
                return { type: 'integer', value: added }
            }
            case 'SREM': {
                const [key, ...members] = rest
                this.#assertSetKey(key)
                const set = this.#sets.get(key)
                let removed = 0
                for (const m of members) if (set?.delete(m)) removed++
                this.#dropIfEmpty(key)
                return { type: 'integer', value: removed }
            }
            case 'SMEMBERS': {
                if (rest.length !== 1) {
                    this.#reject(
                        `FakeRedis: SMEMBERS takes one key, got ${rest.length}`,
                    )
                }
                const set = this.#sets.get(rest[0])
                return {
                    type: 'array',
                    value: [...(set ?? [])].map((m) => ({
                        type: 'bulk' as const,
                        value: m,
                    })),
                }
            }
            case 'SSCAN': {
                // SSCAN key cursor COUNT n (#358) — the one form the sweep
                // issues. The scan core does the walking; this arm only
                // refuses what it does not model. MATCH filters the page and
                // NOVALUES is not a set option: ignoring either hands the
                // caller a page a real broker would not send.
                const [key, cursor, ...opts] = rest
                if (key === undefined || cursor === undefined) {
                    this.#reject(
                        `FakeRedis: SSCAN takes key and cursor, got ${rest.length}`,
                    )
                }
                if (!SCAN_CURSOR.test(cursor)) {
                    this.#reject(
                        `FakeRedis: SSCAN cursor '${cursor}' is not canonical ` +
                            'decimal',
                    )
                }
                let count: number | undefined
                for (let i = 0; i < opts.length; i += 2) {
                    const option = opts[i].toUpperCase()
                    const value = opts[i + 1]
                    if (option !== 'COUNT') {
                        this.#reject(
                            `FakeRedis: unmodelled SSCAN option '${opts[i]}'`,
                        )
                    }
                    if (count !== undefined) {
                        this.#reject('FakeRedis: SSCAN given COUNT twice')
                    }
                    if (value === undefined || !/^[1-9][0-9]*$/.test(value)) {
                        this.#reject(
                            'FakeRedis: SSCAN COUNT must be a positive ' +
                                `integer, got '${value}'`,
                        )
                    }
                    count = Number(value)
                }
                if (count === undefined) {
                    this.#reject(
                        'FakeRedis: SSCAN without COUNT — every scan this ' +
                            'fake models bounds its page',
                    )
                }
                const page = this.#scan(
                    key,
                    [...(this.#sets.get(key) ?? [])],
                    cursor,
                    count,
                )
                return {
                    type: 'array',
                    value: [
                        { type: 'bulk', value: page.cursor },
                        {
                            type: 'array',
                            value: page.members.map((m): Reply => ({
                                type: 'bulk',
                                value: m,
                            })),
                        },
                    ],
                }
            }
            case 'ZSCAN': {
                // ZSCAN key cursor COUNT n (#359) — the one form the
                // revocation pass issues, walked by the same scan core as
                // SSCAN over the LIVE sorted set. MATCH filters the page and
                // NOSCORES (Redis 8) drops the scores: ignoring either hands
                // the caller a page a real broker would not send.
                const [key, cursor, ...opts] = rest
                if (key === undefined || cursor === undefined) {
                    this.#reject(
                        `FakeRedis: ZSCAN takes key and cursor, got ${rest.length}`,
                    )
                }
                if (!SCAN_CURSOR.test(cursor)) {
                    this.#reject(
                        `FakeRedis: ZSCAN cursor '${cursor}' is not canonical ` +
                            'decimal',
                    )
                }
                let pageSize: number | undefined
                for (let i = 0; i < opts.length; i += 2) {
                    const flag = opts[i].toUpperCase()
                    if (flag !== 'COUNT') {
                        this.#reject(
                            `FakeRedis: unmodelled ZSCAN option '${opts[i]}'`,
                        )
                    }
                    if (pageSize !== undefined) {
                        this.#reject('FakeRedis: ZSCAN given COUNT twice')
                    }
                    const n = opts[i + 1]
                    if (n === undefined || !/^[1-9][0-9]*$/.test(n)) {
                        this.#reject(
                            'FakeRedis: ZSCAN COUNT must be a positive ' +
                                `integer, got '${n}'`,
                        )
                    }
                    pageSize = Number(n)
                }
                if (pageSize === undefined) {
                    this.#reject(
                        'FakeRedis: ZSCAN without COUNT — every scan this ' +
                            'fake models bounds its page',
                    )
                }
                const zset = this.#liveZset(key) ?? new Map<string, number>()
                const page = this.#scan(key, [...zset.keys()], cursor, pageSize)
                return {
                    type: 'array',
                    value: [
                        { type: 'bulk', value: page.cursor },
                        {
                            type: 'array',
                            value: page.members.flatMap((m): Reply[] => [
                                { type: 'bulk', value: m },
                                {
                                    type: 'bulk',
                                    value: this.#formatScore(zset.get(m)!),
                                },
                            ]),
                        },
                    ],
                }
            }
            case 'DEL': {
                // DEL key [key ...] -> the count actually removed. This read
                // `rest[0]` alone and never touched `#zsets`, so deleting the
                // revocation index — a ZSET — returned 0 and left every member
                // in place: a cleanup that silently did nothing.
                let removed = 0
                for (const key of rest) if (this.#dropKey(key)) removed++
                return { type: 'integer', value: removed }
            }
            case 'SET': {
                // SET key value [EX seconds] [GET] — every other option is
                // REJECTED. Silently ignoring NX is the shape that made an
                // inert guard look like a working one: the caller believes it
                // wrote only if the key was absent, and the fake wrote
                // unconditionally.
                const [key, value, ...opts] = rest
                let expireAt: number | undefined
                let get = false
                for (let i = 0; i < opts.length; i++) {
                    // GET (#349): the reply becomes the value the key held
                    // before this write, or nil — the heartbeat's lapse bit.
                    // Once, in any position.
                    if (opts[i].toUpperCase() === 'GET') {
                        if (get) this.#reject('FakeRedis: SET given GET twice')
                        get = true
                        continue
                    }
                    if (opts[i].toUpperCase() !== 'EX') {
                        this.#reject(
                            `FakeRedis: unmodelled SET option '${opts[i]}' — ` +
                                'model it rather than letting it silently no-op',
                        )
                    }
                    if (expireAt !== undefined) {
                        this.#reject('FakeRedis: SET given EX twice')
                    }
                    // The argument is CHECKED. `opts[++i]` consumed it blindly,
                    // so `SET k v EX` and `SET k v EX abc` both returned OK
                    // holding NaN — and `NaN >= x` is false forever, making the
                    // key IMMORTAL. A silent no-op in the very arm rewritten to
                    // stop silent no-ops.
                    const raw = opts[++i]
                    const seconds = Number(raw)
                    if (
                        raw === undefined || raw === '' ||
                        !Number.isFinite(seconds)
                    ) {
                        this.#reject(
                            `FakeRedis: SET EX needs an integer, got '${raw}'`,
                        )
                    }
                    expireAt = this.#now() + seconds
                }
                // The previous value is read AFTER the expiry check and BEFORE
                // the write: an expired key answers nil, as on the broker. A
                // key of another type is refused (WRONGTYPE) and, unlike a
                // plain SET, left as it was.
                let previous: Reply | undefined
                if (get) {
                    const exists = this.#keyExists(key)
                    if (exists && !this.#strings.has(key)) {
                        this.#reject(
                            'WRONGTYPE Operation against a key holding the ' +
                                'wrong kind of value',
                        )
                    }
                    const held = exists ? this.#strings.get(key) : undefined
                    previous = held === undefined
                        ? { type: 'nil' }
                        : { type: 'bulk', value: held }
                }
                // A plain (no GET) SET overwrites UNCONDITIONALLY, whatever
                // type held the key before (#414) — unlike HSET/SADD/ZADD,
                // which each refuse a foreign type instead. Real Redis
                // destroys the prior value first; leaving it in `#sets` /
                // `#hashes` / `#zsets` made the key exist under two types at
                // once, so a later same-family command (`HGET` after a `SET`
                // over an old hash) read the STALE data back rather than
                // seeing the string `TYPE`/`#assertHashKey` would report.
                this.#sets.delete(key)
                this.#hashes.delete(key)
                this.#zsets.delete(key)
                this.#strings.set(key, value)
                // A plain SET CLEARS any existing TTL, as Redis does. With one
                // registry that has to be said rather than falling out of an
                // `expireAt: undefined` overwrite.
                if (expireAt === undefined) this.#keyExpiry.delete(key)
                else this.#keyExpiry.set(key, expireAt)
                return previous ?? { type: 'simple', value: 'OK' }
            }
            case 'TYPE': {
                // TYPE key -> a status reply naming the key's Redis type, or
                // 'none' for an absent (or lapsed) key (#405). This fake never
                // models a `list` or a `stream`, so those two kinds are never
                // produced here — only on a live broker, which is why #405's
                // list/stream rows are live-only.
                const [key] = rest
                if (this.#expired(key)) this.#dropKey(key)
                const kind = this.#strings.has(key)
                    ? 'string'
                    : this.#sets.has(key)
                    ? 'set'
                    : this.#hashes.has(key)
                    ? 'hash'
                    : this.#zsets.has(key)
                    ? 'zset'
                    : 'none'
                return { type: 'simple', value: kind }
            }
            case 'EXISTS': {
                // EXISTS key [key ...] -> a COUNT, and across every type. It
                // read `rest[0]` and consulted only `#strings`, so it answered
                // 0 for every set, hash and sorted set in the store.
                let found = 0
                for (const key of rest) if (this.#keyExists(key)) found++
                return { type: 'integer', value: found }
            }
            default:
                // A `nil` here would be indistinguishable from a legitimate miss,
                // so an unmodelled command would make the driver a silent no-op
                // and every test green. Fail loudly instead (#276 FR-008).
                this.#reject(
                    `FakeRedis: unmodelled command '${cmd}' — model it in ` +
                        `#exec rather than letting it silently no-op`,
                )
        }
    }
}

/** A command function, the shape of the driver's command port. */
export type CommandFn = (...args: string[]) => Promise<unknown>

/** A command matcher, over the arguments the driver issued. */
export type CommandMatch = (args: readonly string[]) => boolean

/** A one-shot gate holding ONE command's reply in flight. */
export interface CommandGate {
    /** Resolves once the gated command has executed and its reply is held. */
    readonly reached: Promise<void>
    /** Deliver the held reply, letting the queue behind it move. */
    release(): void
}

/** A {@link CommandFn} with one exchange in flight at a time, plus gates. */
export interface SerializedCommands {
    /** The serialized command function to hand to a driver. */
    readonly command: CommandFn
    /**
     * Hold the reply of the NEXT command matching `match` until the returned
     * gate is released. The command has already executed at the fake broker
     * when `reached` resolves — in flight the way a written command whose
     * reply has not been read is — and every command issued after it waits.
     *
     * @param match - Which command to hold.
     * @returns The gate.
     */
    hold(match: CommandMatch): CommandGate
    /**
     * Resolve once a command matching `match` has been ISSUED — enqueued on
     * the tail, not necessarily executed.
     *
     * @param match - Which command to wait for.
     * @returns Resolves on the first matching issue from now on.
     */
    whenIssued(match: CommandMatch): Promise<void>
}

/**
 * Chain every command on ONE tail, exactly as `@lockness/redis`'s `RedisClient`
 * does with its `commandTail` (`packages/redis/client.ts`, `command`), so a
 * command does not start until the one before it has settled (#348 A1).
 *
 * `FakeRedis.command` settles each command on its own, instantly, so it cannot
 * show an ordering that depends on the production client running one exchange
 * at a time — which is what keeps a swept `left` ahead of a hold committed
 * right behind the sweep's release. This wrapper restores that property, and
 * its gate holds one command's reply in flight so a test can issue a second
 * command while the first is outstanding.
 *
 * @param inner - The command function each exchange runs, usually
 *   `redis.command`.
 * @returns The serialized command function and its gates.
 *
 * @example
 * ```typescript
 * const serial = serializedCommands(redis.command)
 * const gate = serial.hold((args) => args[0] === 'EVAL')
 * const driver = new RedisBroadcastDriver({ command: serial.command }, sub)
 * ```
 */
export function serializedCommands(inner: CommandFn): SerializedCommands {
    let tail: Promise<unknown> = Promise.resolve()
    const gates: Array<{
        match: CommandMatch
        reach: () => void
        released: Promise<void>
    }> = []
    const watchers: Array<{ match: CommandMatch; issued: () => void }> = []
    const command: CommandFn = (...args) => {
        const watcher = watchers.findIndex((w) => w.match(args))
        if (watcher >= 0) watchers.splice(watcher, 1)[0].issued()
        const run = tail.then(async () => {
            const index = gates.findIndex((g) => g.match(args))
            if (index < 0) return await inner(...args)
            const gate = gates.splice(index, 1)[0]
            // Settled to a value first: a rejection held behind the gate must
            // not surface as unhandled before its reply is delivered.
            const outcome = inner(...args).then(
                (value) => ({ ok: true as const, value }),
                (error: unknown) => ({ ok: false as const, error }),
            )
            gate.reach()
            await gate.released
            const settled = await outcome
            if (!settled.ok) throw settled.error
            return settled.value
        })
        // The tail must always settle so the next command runs; `run` still
        // rejects to its caller, as the production client's does.
        tail = run.catch(() => {})
        return run
    }
    return {
        command,
        hold(match) {
            let reach!: () => void
            let release!: () => void
            const reached = new Promise<void>((resolve) => (reach = resolve))
            const released = new Promise<void>((resolve) => (release = resolve))
            gates.push({ match, reach, released })
            return { reached, release }
        },
        whenIssued(match) {
            return new Promise<void>((issued) =>
                void watchers.push({ match, issued })
            )
        },
    }
}
