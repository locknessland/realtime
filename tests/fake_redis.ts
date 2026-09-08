/**
 * @fileoverview An in-memory fake Redis for the realtime driver unit tests.
 *
 * It models exactly the command surface the {@link RedisBroadcastDriver} uses —
 * `PUBLISH`, the roster hash (`HSET`/`HDEL`/`HGETALL`), the owned/instances sets
 * (`SADD`/`SREM`/`SMEMBERS`/`DEL`) and the liveness string (`SET … EX`/`EXISTS`)
 * — returning `RespReply`-shaped values so the driver's real reply-narrowing
 * runs unchanged. Pub/sub fan-out is synchronous, like the existing
 * `driver_redis.test.ts` fake bus.
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
 * **`setTime()` now reaches the liveness key.** Unifying the clock means a
 * test that moves the fake clock by hundreds of seconds expires the driver's
 * `SET … EX` alive key too, where before it never expired inside a test.
 * Nothing depends on that today — `#reconcile` runs on a real `setInterval`
 * that does not fire inside a sub-millisecond test — but a test that later
 * gains a reconcile tick will see instances swept that used to read as alive.
 * Stated here rather than left to be discovered.
 *
 * A test helper — never imported by production code.
 *
 * @module @lockness/realtime/tests/fake_redis
 */

import { evalLua } from '../../redis/tests/lua_eval.ts'

/** A push-message handler for a subscribed pattern. */
type Handler = (topic: string, payload: string) => void

/** A `RespReply`-shaped value (the subset the driver narrows). */
type Reply =
    | { type: 'simple'; value: string }
    | { type: 'integer'; value: number }
    | { type: 'bulk'; value: string }
    | { type: 'array'; value: Reply[] }
    | { type: 'nil' }

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
        /** Test-only: simulate a reconnect, awaiting the handler's round-trip. */
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

    /** Whether a string key is present and unexpired (lazy-expiring on read). */
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
     * assertion that reads `listRevoked()`'s return value, because the caller
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
                const result = evalLua(script, keys, argv, (command, cargs) => {
                    const reply = this.#exec([command, ...cargs])
                    if (reply.type === 'array') {
                        return reply.value.map((r) =>
                            r.type === 'bulk' ? r.value : ''
                        )
                    }
                    if (reply.type === 'nil') return undefined
                    return String(reply.value)
                })
                if (result === undefined) return { type: 'nil' }
                if (Array.isArray(result)) {
                    return {
                        type: 'array',
                        value: result.map((v): Reply => ({
                            type: 'bulk',
                            value: v,
                        })),
                    }
                }
                return { type: 'bulk', value: result as string }
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
                const h = this.#hashes.get(rest[0])
                const flat: Reply[] = []
                for (const [field, value] of h ?? []) {
                    flat.push({ type: 'bulk', value: field })
                    flat.push({ type: 'bulk', value })
                }
                return { type: 'array', value: flat }
            }
            case 'SADD': {
                const [key, ...members] = rest
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
                // SET key value [EX seconds] — every other option is REJECTED.
                // Silently ignoring NX is the shape that made an inert guard
                // look like a working one: the caller believes it wrote only if
                // the key was absent, and the fake wrote unconditionally.
                const [key, value, ...opts] = rest
                let expireAt: number | undefined
                for (let i = 0; i < opts.length; i++) {
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
                this.#strings.set(key, value)
                // A plain SET CLEARS any existing TTL, as Redis does. With one
                // registry that has to be said rather than falling out of an
                // `expireAt: undefined` overwrite.
                if (expireAt === undefined) this.#keyExpiry.delete(key)
                else this.#keyExpiry.set(key, expireAt)
                return { type: 'simple', value: 'OK' }
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
