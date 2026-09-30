/**
 * @fileoverview #359 — the revocation re-check reads the index in bounded
 * `ZSCAN` pages, one pass at a time, and applies nothing before the
 * enumeration ends.
 *
 * Before #359 `listRevocations` ran one script that reaped expired records
 * and returned EVERY live one in a single reply, on the instance's SHARED
 * command client. Past `MAX_REPLY_BYTES` of wire the reply was refused, the
 * client dropped its socket and opened its refusal window, and the re-check
 * enforced nothing — on every instance, on every tick.
 *
 * Every FakeRedis witness runs a real `RedisBroadcastDriver` "B" (and its
 * `ChannelManager`) behind #348's `serializedCommands` — one exchange in
 * flight, as on the production client — so a page or an apply can be HELD
 * with the wrapper's gate while the test fires timers, reconnects, a lapse or
 * `close()`. Records are planted in the index exactly as
 * `MARK_REVOKED_SCRIPT` writes them: one member per record, scored at its
 * expiry second. The fake pages by member-derived slots
 * (`FakeRedis.scanSlot`), so a test puts a record ahead of or behind the
 * cursor by its slot.
 *
 * **Waiting for a pass** (plan FR-015, the FakeTime-drain pitfall in
 * `packages/realtime/AGENTS.md`): after the tick or the reconnect that starts
 * it, `await time.runMicrotasks()`, which runs a real macrotask, so the whole
 * pass has drained when it resolves unless a gate holds a reply. A witness
 * that holds a page or an apply waits on the gate's `reached` — bounded by
 * {@link reachedNow}, so a driver that never issues the held command fails
 * instead of hanging. `close()` only where `close()` is the subject (R10).
 *
 * Red on `4b2c40fc` (the driver before #359, with this branch's fake): R1 and
 * R12 failed to compile (`REVOCATION_SCAN_COUNT`, `decodeReapReply`,
 * `decodeRevocationPage` and the message constants did not exist), and R8,
 * R9, R10 and R14 on their behaviour; R2 on a live broker ("RESP reply
 * exceeds", then the refusal window). The rest are guards, each paired with a
 * mutant in `mutations/revocation_paging_359.ts`.
 *
 * @module @lockness/realtime/tests/revocation_paging_359
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    decodeReapReply,
    decodeRevocationPage,
    REAP_REPLY_REFUSED,
    RedisBroadcastDriver,
    REVOCATION_PAGE_REFUSED,
    REVOCATION_PAIRS_SKIPPED,
    REVOCATION_PASS_CLOSING,
    REVOCATION_SCAN_COUNT,
    SCAN_REPLY_REFUSED,
} from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import type {
    BroadcastMessage,
    ChannelRevocation,
    Revocation,
    RevocationStoreDriver,
} from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { RedisClient } from '../../redis/mod.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
    waitFor,
} from '../../redis/tests/live_broker.ts'
import {
    type CommandFn,
    type CommandGate,
    type CommandMatch,
    FakeRedis,
    serializedCommands,
} from './fake_redis.ts'
import { isReap as isReapOf } from './revocation_wire.ts'

const START = new Date('2026-09-23T10:00:00Z')
/** The fake broker's `TIME`, pinned: every planted score is relative to it. */
const NOW = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const SECRET = 'a-deployment-secret-with-more-than-enough-entropy'
const INTERVAL = 1_000
const ROOM = 'private-room'
const OTHER = 'private-other'
const PRESENCE = 'presence-room'

/** The words of the one line a failed revocation pass logs. */
const PASS_FAILED = 'revocation reconcile failed'

interface User {
    id: number
    name?: string
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
    readonly closed: number[]
}

function conn(id: string, userId = 1): Recording {
    const received: Record<string, unknown>[] = []
    const closed: number[] = []
    return {
        id,
        identity: { id: userId, name: `user-${userId}` },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: (code?: number) => void closed.push(code ?? 0),
        received,
        closed,
    } as Recording
}

/** Whether `c` was told it left `channel` — a channel revocation applied. */
const unsubscribedFrom = (c: Recording, channel: string) =>
    c.received.filter((f) => f.type === 'unsubscribed' && f.channel === channel)
        .length

/** Whether `c`'s socket was hard-closed 4403 — a connection revocation. */
const evicted = (c: Recording) => c.closed.includes(4403)

const authorize = (user: User | null): PresenceMember | false =>
    user ? { id: user.id, info: { name: user.name ?? '' } } : false

/** Plant one record, as `MARK_REVOKED_SCRIPT` writes it. */
async function plant(
    redis: FakeRedis,
    member: string,
    score = NOW + 300,
): Promise<void> {
    await redis.command('ZADD', INDEX, String(score), member)
}

/** A channel-scoped record's member. */
const scoped = (target: string, channel: string, id: string) =>
    `${target} ${channel} ${id}`

/** The reap, as `revocation_wire.ts` defines it, bound to this index. */
const isReap: CommandMatch = (args) => isReapOf(args, INDEX)

/** A page read of the index. */
const isPageRead: CommandMatch = (args) =>
    args[0] === 'ZSCAN' && args[1] === INDEX

/** A clear of one applied channel record. */
const isClear: CommandMatch = (args) => args[0] === 'ZREM' && args[1] === INDEX

/**
 * A command port serialized as the production client is, recording every
 * command issued through it and every reply it answered, in issue order.
 */
function serialPort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const sent: string[][] = []
    const replies: { args: string[]; reply: unknown }[] = []
    const failNext: CommandMatch[] = []
    const command: CommandFn = (...args) => {
        sent.push(args)
        const fail = failNext.findIndex((m) => m(args))
        if (fail >= 0) {
            failNext.splice(fail, 1)
            return Promise.reject(new Error('injected: refused (#359)'))
        }
        return serial.command(...args).then((reply) => {
            replies.push({ args, reply })
            return reply
        })
    }
    return {
        serial,
        sent,
        replies,
        command,
        /** Refuse the NEXT command matching `match`, once. */
        failOnce: (match: CommandMatch) => void failNext.push(match),
        reaps: () => sent.filter(isReap).length,
        pageReads: () => sent.filter(isPageRead),
    }
}

/**
 * One instance: a Redis driver over `command` and its manager, with the
 * reconnect seam exposed and the lapse handler the manager registered
 * captured (so a witness can fire A's re-assert itself).
 */
function instance(
    redis: FakeRedis,
    command: CommandFn,
    options: { interval?: number } = {},
) {
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver({ command }, subscriber, {
        prefix: PREFIX,
        control: { secret: SECRET },
        revocationTtlSeconds: 300,
        presence: {
            livenessTtlSeconds: 2,
            heartbeatIntervalMs: 500,
            reconcileIntervalMs: options.interval ?? INTERVAL,
        },
    })
    let lapse: ((signal: AbortSignal) => void | Promise<void>) | undefined
    const register = driver.onRosterLapse.bind(driver)
    driver.onRosterLapse = (handler) => {
        lapse = handler
        register(handler)
    }
    const manager = new ChannelManager<User>({ driver, authorize })
    return {
        driver,
        manager,
        /** Fire the subscribe socket's reconnect seam; it returns nothing. */
        reconnect: () => void subscriber.fireReconnect(),
        /** Run the manager's re-assert, as a lapse would. */
        lapse: () => lapse?.(new AbortController().signal),
    }
}

/**
 * Whether `gate` has been reached once the microtask queue has drained — a
 * bounded wait, so a driver that never issues the held command fails on an
 * assertion instead of hanging the run.
 */
async function reachedNow(
    gate: CommandGate,
    time: FakeTime,
): Promise<boolean> {
    let reached = false
    void gate.reached.then(() => void (reached = true))
    await time.runMicrotasks()
    return reached
}

/** Collect every `console.warn` line until `restore()`. */
function captureWarnings() {
    const warn = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => void lines.push(parts.join(' '))
    return {
        lines,
        having: (words: string) => lines.filter((l) => l.includes(words)),
        restore: () => void (console.warn = warn),
    }
}

/** Fire the revocation timer once and wait for the pass it starts (FR-015). */
async function runOnePass(time: FakeTime, ms = INTERVAL): Promise<void> {
    await time.tickAsync(ms)
    await time.runMicrotasks()
}

/**
 * `count` foreign channel-scoped records — no socket on any instance here —
 * enough to span several pages.
 */
async function plantForeign(redis: FakeRedis, count: number): Promise<void> {
    for (let i = 0; i < count; i++) {
        await plant(
            redis,
            scoped(`x${i}`, `private-foreign-${i % 13}`, `f${i}`),
        )
    }
}

// --- US1 + US2: a large index is fully enforced, no reply over a page --------

Deno.test('#359 R1 307 live records, five for B: one drained pass applies all five through one reap and ZSCAN pages from cursor 0 to cursor 0, and nothing else reads the index', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c = ['c1', 'c2', 'c3', 'c4', 'c5'].map((id) => conn(id))
        for (const x of c) {
            b.manager.register(x)
            assert((await b.manager.subscribe(x, ROOM)).ok)
            assert((await b.manager.subscribe(x, OTHER)).ok)
        }
        await plantForeign(redis, 302)
        await plant(redis, 'c1')
        await plant(redis, 'c2')
        await plant(redis, scoped('c3', ROOM, 'r3'))
        await plant(redis, scoped('c4', OTHER, 'r4'))
        await plant(redis, scoped('c5', ROOM, 'r5'))
        assertEquals(redis.zcard(INDEX), 307)
        port.sent.length = 0
        port.replies.length = 0

        await runOnePass(time)

        assert(evicted(c[0]) && evicted(c[1]), 'both connection records')
        assertEquals(unsubscribedFrom(c[2], ROOM), 1)
        assertEquals(unsubscribedFrom(c[3], OTHER), 1)
        assertEquals(unsubscribedFrom(c[4], ROOM), 1)
        assertEquals(unsubscribedFrom(c[2], OTHER), 0, 'only its own room')

        const reads = port.sent.filter((args) =>
            args.includes(INDEX) && !isClear(args)
        )
        assert(isReap(reads[0]), `the pass starts with the reap: ${reads[0]}`)
        assert(
            !reads[0][1].includes('ZRANGE'),
            'the reap reads nothing back: no ZRANGE in its script',
        )
        const pages = reads.slice(1)
        assert(
            pages.length >= 4,
            `a 307-record index is read in pages, got ${pages.length}`,
        )
        for (const page of pages) {
            assertEquals(page.length, 5, page.join(' '))
            assert(isPageRead(page), `only ZSCAN pages follow: ${page}`)
            assertEquals(page[3], 'COUNT')
            assertEquals(page[4], String(REVOCATION_SCAN_COUNT))
        }
        assertEquals(pages[0][2], '0', 'the first page is at cursor 0')
        const answered = port.replies.filter((r) => isPageRead(r.args))
        assertEquals(answered.length, pages.length)
        for (let i = 1; i < pages.length; i++) {
            const previous = decodeRevocationPage(answered[i - 1].reply)
            assertEquals(pages[i][2], previous.cursor, 'the cursor advances')
        }
        assertEquals(
            decodeRevocationPage(answered.at(-1)!.reply).cursor,
            '0',
            'the last page answered cursor 0',
        )
        assert(
            !redis.commandLog().some(([cmd]) => cmd === 'ZRANGEBYSCORE'),
            'no ZRANGEBYSCORE ran, inside a script or out',
        )
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

/**
 * `count` members `make(i)`, counting up from `from`, whose scan slot lies in
 * `[lo, hi)` — how a witness places a record on a chosen page.
 */
function inSlots(
    make: (i: number) => string,
    lo: number,
    hi: number,
    count: number,
    from = 0,
): string[] {
    const out: string[] = []
    for (let i = from; out.length < count; i++) {
        if (i > from + 1_000_000) throw new Error(`no member in [${lo}, ${hi})`)
        const slot = FakeRedis.scanSlot(make(i))
        if (slot >= lo && slot < hi) out.push(make(i))
    }
    return out
}

/** Foreign records whose slots all lie in `[lo, hi)`. */
async function plantForeignIn(
    redis: FakeRedis,
    lo: number,
    hi: number,
    count: number,
): Promise<void> {
    for (
        const m of inSlots((i) => scoped(`x${i}`, ROOM, `f${i}`), lo, hi, count)
    ) {
        await plant(redis, m)
    }
}

Deno.test('#359 R3 (a, b) the pass judges every page against the reap’s one now: a record whose expiry passes mid-pass is applied, one scored at t is not', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        const c2 = conn('c2')
        b.manager.register(c2)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        assert((await b.manager.subscribe(c2, ROOM)).ok)
        await plant(redis, 'c1', NOW + 10)
        // The REAP's reply is held: it has run at `t = NOW`, and the first
        // page is read only after the release — so what is planted meanwhile
        // is on that page. (Holding the page instead would hold a reply
        // already read, and neither record below would reach the filter.)
        const gate = port.serial.hold(isReap)
        await time.tickAsync(INTERVAL)
        assert(await reachedNow(gate, time), 'held between reap and page')
        // (a) The broker's clock moves past c1's expiry mid-pass. (b) A
        // record scored exactly at the reap's `t` lands behind the reap.
        redis.setTime(NOW + 20)
        await plant(redis, 'c2', NOW)
        gate.release()
        await time.runMicrotasks()
        assert(evicted(c1), '(a) live at the pass’s one now: applied')
        assert(!evicted(c2), '(b) scored at t: not live, not applied')
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

for (const skewMs of [3_600_000, -3_600_000]) {
    Deno.test(`#359 R3 (c) the instance clock skewed ${skewMs > 0 ? '+' : '-'}1 h from the broker's changes nothing — liveness is never Date.now()`, async () => {
        const redis = new FakeRedis()
        redis.setTime(NOW)
        const time = new FakeTime(START.getTime() + skewMs)
        const port = serialPort(redis)
        const b = instance(redis, port.command)
        const warnings = captureWarnings()
        try {
            const c1 = conn('c1')
            b.manager.register(c1)
            const c2 = conn('c2')
            b.manager.register(c2)
            assert((await b.manager.subscribe(c1, ROOM)).ok)
            assert((await b.manager.subscribe(c2, ROOM)).ok)
            await plant(redis, 'c1', NOW + 10)
            // The reap's reply is held, so c2 — planted behind the reap —
            // is on the first page and meets the filter (see R3 (a, b)).
            const gate = port.serial.hold(isReap)
            await time.tickAsync(INTERVAL)
            assert(await reachedNow(gate, time), 'held between reap and page')
            await plant(redis, 'c2', NOW - 100)
            gate.release()
            await time.runMicrotasks()
            assert(evicted(c1), 'live on the broker: applied')
            assert(!evicted(c2), 'expired on the broker: not applied')
            redis.assertNoRejections()
        } finally {
            warnings.restore()
            await b.driver.close()
            time.restore()
        }
    })
}

Deno.test('#359 R4 undecodable members across pages survive every pass and are never applied or counted; expired members are reaped', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        const undecodable = [
            ...inSlots((i) => `c1 ${ROOM}-${i}`, 0, 100, 3),
            ...inSlots((i) => `c1 ${ROOM} r${i} extra`, 400, 500, 3),
            ...inSlots((i) => `c1!${i}`, 900, 1024, 3),
        ]
        for (const m of undecodable) await plant(redis, m)
        await plantForeign(redis, 150)
        await plant(redis, 'expired-1', NOW - 5)
        await plant(redis, scoped('expired-2', ROOM, 'e2'), NOW)
        const before = redis.zcard(INDEX)

        await runOnePass(time)
        assertEquals(redis.zcard(INDEX), before - 2, 'exactly the two expired')
        const raw = await redis.command(
            'ZRANGEBYSCORE',
            INDEX,
            '-inf',
            '+inf',
        ) as { value: { value: string }[] }
        const left = raw.value.map((m) => m.value)
        assert(!left.includes('expired-1'))
        assert(!left.includes(scoped('expired-2', ROOM, 'e2')))
        for (const m of undecodable) assert(left.includes(m), `${m} survives`)
        assert(!evicted(c1), 'no undecodable member is applied')
        assertEquals(unsubscribedFrom(c1, ROOM), 0)
        assertEquals(warnings.having(REVOCATION_PAIRS_SKIPPED), [])
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R5 two records of one local pair on different pages: one leave, one unsubscribed, both cleared — and a re-subscribe is not kicked next tick', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        const [early] = inSlots((i) => scoped('c1', ROOM, `a${i}`), 0, 100, 1)
        const [late] = inSlots((i) => scoped('c1', ROOM, `b${i}`), 600, 700, 1)
        await plant(redis, early)
        await plant(redis, late)
        await plantForeign(redis, 150)
        const before = redis.zcard(INDEX)

        await runOnePass(time)
        assertEquals(unsubscribedFrom(c1, ROOM), 1, 'the pair leaves once')
        assertEquals(redis.zcard(INDEX), before - 2, 'both ids cleared')
        assert(port.pageReads().length > 1, 'the two records were paged')

        assert((await b.manager.subscribe(c1, ROOM)).ok)
        await runOnePass(time)
        assertEquals(
            unsubscribedFrom(c1, ROOM),
            1,
            'a legitimate re-subscribe is not kicked',
        )
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R6 a record written between two pages: ahead of the cursor, applied this pass; behind it, applied by the next', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        const c2 = conn('c2')
        b.manager.register(c2)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        assert((await b.manager.subscribe(c2, ROOM)).ok)
        await plantForeign(redis, 150)
        // The second page visits slots [100, 200): once it has executed, a
        // slot at 200 or past lies ahead of the cursor, one under 100 behind.
        const gate = port.serial.hold((args) =>
            isPageRead(args) && args[2] === '100'
        )
        await time.tickAsync(INTERVAL)
        assert(await reachedNow(gate, time), 'held on the second page')
        const [ahead] = inSlots(
            (i) => scoped('c1', ROOM, `a${i}`),
            300,
            1024,
            1,
        )
        const [behind] = inSlots((i) => scoped('c2', ROOM, `b${i}`), 0, 100, 1)
        await plant(redis, ahead)
        await plant(redis, behind)
        gate.release()
        await time.runMicrotasks()
        assertEquals(unsubscribedFrom(c1, ROOM), 1, '(a) ahead: this pass')
        assertEquals(unsubscribedFrom(c2, ROOM), 0, '(b) behind: not yet')
        await runOnePass(time)
        assertEquals(unsubscribedFrom(c2, ROOM), 1, '(b) the next pass')
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R7 an empty page with a non-zero cursor does not end the pass', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        await plantForeignIn(redis, 0, 100, 150)
        const [last] = inSlots((i) => scoped('c1', ROOM, `z${i}`), 950, 1024, 1)
        await plant(redis, last)

        await runOnePass(time)
        const empty = port.replies.filter((r) => {
            if (!isPageRead(r.args)) return false
            const page = decodeRevocationPage(r.reply)
            return page.entries.length === 0 && page.cursor !== '0'
        })
        assert(empty.length > 0, 'precondition: an empty mid-iteration page')
        assertEquals(unsubscribedFrom(c1, ROOM), 1, 'the last page was read')
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R11 a page read that fails applies NOTHING, page 1 included; the WARN names the trigger; the next pass applies everything', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        const c2 = conn('c2')
        b.manager.register(c2)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        assert((await b.manager.subscribe(c2, ROOM)).ok)
        await plantForeign(redis, 150)
        const [first] = inSlots((i) => scoped('c1', ROOM, `a${i}`), 0, 100, 1)
        await plant(redis, first)
        await plant(redis, 'c2')
        port.failOnce((args) => isPageRead(args) && args[2] !== '0')

        await runOnePass(time)
        assertEquals(
            unsubscribedFrom(c1, ROOM),
            0,
            "page 1's match not applied",
        )
        assert(!evicted(c2))
        assertEquals(warnings.having(`${PASS_FAILED} (timer)`).length, 1)

        await runOnePass(time)
        assertEquals(unsubscribedFrom(c1, ROOM), 1)
        assert(evicted(c2))
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

/**
 * A revocation store written by hand: it records every `owns` it is handed
 * and, when `ignoreOwns` is set, returns every record regardless — the
 * third-party driver the port allows.
 */
class RecordingRevocationDriver implements RevocationStoreDriver {
    readonly #handlers: Array<(message: BroadcastMessage) => void> = []
    revocations: Revocation[] = []
    owns: ((target: string) => boolean)[] = []
    ignoreOwns = false
    reconcile?: () => void | Promise<void>

    publish(message: BroadcastMessage): void {
        for (const handler of this.#handlers) handler(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.#handlers.push(handler)
    }

    markRevocation(revocation: Revocation): void {
        this.revocations.push(revocation)
    }

    /** What each `owns` answered for {@link probes}, at the call itself. */
    answers: boolean[][] = []
    probes: string[] = []

    listRevocations(owns?: (target: string) => boolean): Revocation[] {
        if (owns) {
            this.owns.push(owns)
            this.answers.push(this.probes.map((t) => owns(t)))
        }
        return this.revocations.filter((r) =>
            this.ignoreOwns || owns === undefined || owns(r.target)
        )
    }

    clearRevocation(revocation: ChannelRevocation): void {
        this.revocations = this.revocations.filter((r) =>
            r.id !== revocation.id
        )
    }

    onRevocationReconcile(handler: () => void | Promise<void>): void {
        this.reconcile = handler
    }
}

Deno.test('#359 R13 (a) the Redis driver keeps only what owns accepts, asking once per decoded record; without owns it returns every record', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        { psubscribe: () => {} },
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        await plant(redis, 'c1')
        await plant(redis, scoped('c2', ROOM, 'r2'))
        await plant(redis, 'x3')
        await plant(redis, 'bad!member')
        const asked: string[] = []
        const kept = await driver.listRevocations((target) => {
            asked.push(target)
            return target.startsWith('c')
        })
        assertEquals(kept.map((r) => r.target).sort(), ['c1', 'c2'])
        assertEquals(
            asked.sort(),
            ['c1', 'c2', 'x3'],
            'once per decoded record',
        )
        const all = await driver.listRevocations()
        assertEquals(all.map((r) => r.target).sort(), ['c1', 'c2', 'x3'])
        redis.assertNoRejections()
    } finally {
        await driver.close()
    }
})

Deno.test('#359 R13 (e) a throw from owns fails listRevocations: the call rejects with that error, returns nothing to apply, and deletes nothing', async () => {
    // The port contract (driver.ts): `owns` is called synchronously and a
    // throw from it fails the call. A `try` around it that swallowed the
    // throw would turn a broken predicate into "not mine" — or "mine" —
    // and hand the caller a partial answer it reads as complete.
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        { psubscribe: () => {} },
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        await plant(redis, 'c1')
        await plant(redis, scoped('c2', ROOM, 'r2'))
        await plant(redis, 'x3')
        const before = redis.zcard(INDEX)
        const broken = new Error('owns is broken')
        const asked: string[] = []
        const outcome = await driver.listRevocations((target) => {
            asked.push(target)
            if (target === 'x3') throw broken
            return true
        }).then(
            (kept) => ({ kept }),
            (error: unknown) => ({ error }),
        )
        assert(asked.includes('x3'), 'the predicate was asked about x3')
        assertEquals(
            outcome,
            { error: broken },
            'the call rejects with the predicate’s own error, nothing kept',
        )
        assertEquals(
            redis.zcard(INDEX),
            before,
            'nothing deleted: the records stay for the next pass',
        )
        redis.assertNoRejections()
    } finally {
        await driver.close()
    }
})

Deno.test('#359 R13 (b, c) the manager asks for exactly its local ids, and applies only local records even from a driver that ignores the question', async () => {
    const driver = new RecordingRevocationDriver()
    const manager = new ChannelManager<User>({ driver, authorize })
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        manager.register(c1)
        const c2 = conn('c2')
        manager.register(c2)
        assert((await manager.subscribe(c1, ROOM)).ok)
        assert((await manager.subscribe(c2, ROOM)).ok)
        driver.revocations = [
            { target: 'c1' },
            { target: 'c2', channel: ROOM, id: 'r2' },
            { target: 'x9' },
            { target: 'x8', channel: ROOM, id: 'r8' },
        ]
        driver.ignoreOwns = true
        driver.probes = ['c1', 'c2', 'x8', 'x9', `c1 ${ROOM}`]
        await driver.reconcile?.()
        // (b) The predicate the manager handed over accepted exactly its
        // local ids when the pass asked.
        assertEquals(driver.owns.length, 1)
        assertEquals(driver.answers, [[true, true, false, false, false]])
        // (c) Given everything, it still applied only what is local.
        assert(evicted(c1))
        assertEquals(unsubscribedFrom(c2, ROOM), 1)
        assertEquals(
            driver.revocations.map((r) => r.target).sort(),
            ['c1', 'x8', 'x9'],
            "only the local channel record was cleared; the foreign one is another instance's",
        )
        assertEquals(warnings.lines, [])
    } finally {
        warnings.restore()
    }
})

// R13 (d) retired by #361: its precondition, a membership naming an id absent from `connections`, is no longer reachable. See `disconnect_admission_361.test.ts` W1.

Deno.test('#359 R15 a re-check run that rejects never stops the tail: its caller sees the rejection, and the next run reaps and applies', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command, { interval: 60_000 })
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        await plant(redis, 'c1')
        port.failOnce(isReap)
        b.reconnect()
        await time.runMicrotasks()
        assertEquals(
            warnings.having(`${PASS_FAILED} (reconnect)`).length,
            1,
            'the rejection reached its caller: the #308 WARN',
        )
        assert(!evicted(c1))
        const reaps = port.reaps()
        // The #308 retry is the next run through the same tail.
        await time.tickAsync(1_000)
        await time.runMicrotasks()
        assertEquals(port.reaps(), reaps + 1, 'the next run issued its reap')
        assert(evicted(c1), 'and applied the record')
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

// --- US3: a slow pass never runs beside another ------------------------------

Deno.test('#359 R8 a page held across three intervals: one reap until it is released, and the next pass one interval after the held one ends', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const gate = port.serial.hold(isPageRead)
        await time.tickAsync(INTERVAL)
        assert(await reachedNow(gate, time), 'the first page read is held')
        await time.tickAsync(2.5 * INTERVAL) // 3.5×
        assertEquals(port.reaps(), 1, 'no second pass beside the held one')
        gate.release()
        await time.runMicrotasks()
        await time.tickAsync(0.9 * INTERVAL) // 4.4×
        await time.runMicrotasks()
        assertEquals(
            port.reaps(),
            1,
            'no pass in (3.5×, 4.5×): an interval would have fired at 4×',
        )
        await runOnePass(time, 0.1 * INTERVAL) // 4.5×
        assertEquals(port.reaps(), 2, 'the next pass, one interval later')
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R9 (a) one reconnect during a held pass → exactly one trailing pass, after it', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command, { interval: 10_000 })
    const warnings = captureWarnings()
    try {
        const gate = port.serial.hold(isPageRead)
        await time.tickAsync(10_000)
        assert(await reachedNow(gate, time), 'the timer pass is held')
        b.reconnect()
        await time.runMicrotasks()
        assertEquals(port.reaps(), 1, 'nothing runs beside the held pass')
        gate.release()
        await time.runMicrotasks()
        assertEquals(port.reaps(), 2, 'one trailing pass')
        await time.tickAsync(5_000)
        await time.runMicrotasks()
        assertEquals(port.reaps(), 2, 'and only one')
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

Deno.test('#359 R9 (b) two reconnects during a held pass → still one trailing pass', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command, { interval: 10_000 })
    const warnings = captureWarnings()
    try {
        const gate = port.serial.hold(isPageRead)
        await time.tickAsync(10_000)
        assert(await reachedNow(gate, time), 'the timer pass is held')
        b.reconnect()
        b.reconnect()
        await time.runMicrotasks()
        gate.release()
        await time.runMicrotasks()
        await time.tickAsync(5_000)
        await time.runMicrotasks()
        assertEquals(port.reaps(), 2, 'the held pass and ONE trailing pass')
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

/**
 * R9 (c), in one arrival order: a reconnect pass fails and arms the #308
 * retry; a second reconnect pass is held; during the hold the retry fires and
 * another reconnect arrives, in `order`. `'reconnect'` must win either way —
 * one order alone cannot tell "reconnect wins" from "the last one wins".
 */
async function retryAndReconnectDuringHold(
    order: 'retry-then-reconnect' | 'reconnect-then-retry',
): Promise<void> {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command, { interval: 60_000 })
    const warnings = captureWarnings()
    try {
        // A failed reconnect pass arms the one retry, 1 s out.
        port.failOnce(isReap)
        b.reconnect()
        await time.runMicrotasks()
        assertEquals(warnings.having(`${PASS_FAILED} (reconnect)`).length, 1)
        // A second reconnect pass starts, and its page read is held.
        const gate = port.serial.hold(isPageRead)
        b.reconnect()
        assert(await reachedNow(gate, time), 'the reconnect pass is held')
        // The retry fires during the hold, and another reconnect arrives —
        // in the order under test.
        if (order === 'retry-then-reconnect') {
            await time.tickAsync(1_500)
            b.reconnect()
        } else {
            b.reconnect()
            await time.tickAsync(1_500)
        }
        await time.runMicrotasks()
        assertEquals(port.reaps(), 2, 'neither runs beside the held pass')
        // The trailing pass fails: its WARN names 'reconnect', not the retry.
        port.failOnce(isReap)
        gate.release()
        await time.runMicrotasks()
        assertEquals(port.reaps(), 3, 'ONE trailing pass')
        assertEquals(warnings.having(`${PASS_FAILED} (reconnect)`).length, 2)
        assertEquals(
            warnings.having(`${PASS_FAILED} (reconnect-retry)`),
            [],
            "'reconnect' won over the retry",
        )
        // Its failure armed the one retry, which runs and succeeds.
        await time.tickAsync(1_000)
        await time.runMicrotasks()
        assertEquals(port.reaps(), 4, 'the #308 retry of the trailing pass')
        await time.tickAsync(5_000)
        await time.runMicrotasks()
        assertEquals(port.reaps(), 4, 'and nothing after it')
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
}

Deno.test("#359 R9 (c) a retry, then a reconnect, during a held pass → one trailing pass whose trigger is 'reconnect', and its failure arms the one #308 retry", () =>
    retryAndReconnectDuringHold('retry-then-reconnect'))

Deno.test("#359 R9 (c) a reconnect, then a retry, during a held pass → still 'reconnect': the retry arriving last does not overwrite it", () =>
    retryAndReconnectDuringHold('reconnect-then-retry'))

// --- US5: shutting down mid-pass stops reading -------------------------------

Deno.test('#359 R10 close() during a held page read: no page read after it but the one in flight, one closing WARN, no trailing pass, no timer, no retry', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const b = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        await plantForeign(redis, 307) // several pages
        const gate = port.serial.hold(isPageRead)
        await time.tickAsync(INTERVAL)
        assert(await reachedNow(gate, time), 'the first page read is held')
        b.reconnect() // a trailing pass is recorded
        await time.runMicrotasks()
        const reapsBefore = port.reaps()
        const pagesBefore = port.pageReads().length
        const closing = b.driver.close()
        gate.release()
        await time.runMicrotasks()
        await closing
        await time.tickAsync(5 * INTERVAL)
        await time.runMicrotasks()

        assertEquals(
            port.pageReads().length,
            pagesBefore,
            'no page read after close() began',
        )
        assertEquals(
            port.reaps(),
            reapsBefore,
            'no reap: the rerun ran nothing',
        )
        assertEquals(
            (b.driver as unknown as { revocationTimer?: number })
                .revocationTimer,
            undefined,
            'no revocation timer outlives the driver',
        )
        const closingWarn = warnings.having(REVOCATION_PASS_CLOSING)
        assertEquals(closingWarn.length, 1, 'one WARN, naming the close')
        assert(closingWarn[0].includes(`${PASS_FAILED} (timer)`))
        assertEquals(
            (b.driver as unknown as { revocationRetryTimer?: number })
                .revocationRetryTimer,
            undefined,
            'no retry',
        )
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        time.restore()
    }
})

// --- US4: re-checks never overlap (the manager's serial tail) ----------------

Deno.test('#359 R14 a lapse during a held apply queues its re-check behind the pass: no second reap until release, a record written meanwhile applied, a re-subscribed pair not kicked', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    const port = serialPort(redis)
    const a = instance(redis, port.command)
    const warnings = captureWarnings()
    try {
        const cx = conn('cx', 7)
        a.manager.register(cx)
        const cy = conn('cy', 8)
        a.manager.register(cy)
        assert((await a.manager.subscribe(cx, PRESENCE)).ok)
        assert((await a.manager.subscribe(cy, PRESENCE)).ok)
        await time.runMicrotasks()
        await plant(redis, scoped('cy', PRESENCE, 'r1'))
        port.sent.length = 0

        // The driver-triggered pass leaves cy, then its clear is held.
        const gate = port.serial.hold(isClear)
        await time.tickAsync(INTERVAL)
        assert(await reachedNow(gate, time), "the pass's clear is held")
        assertEquals(unsubscribedFrom(cy, PRESENCE), 1)
        assertEquals(port.reaps(), 1)

        // During the hold: A's lapse fires its re-assert, a revocation for
        // cx is written, and cy legitimately re-subscribes.
        const reassert = a.lapse()
        await plant(redis, scoped('cx', PRESENCE, 'r2'))
        const resubscribed = a.manager.subscribe(cy, PRESENCE)
        await time.runMicrotasks()
        assertEquals(
            port.reaps(),
            1,
            "the lapse's re-check waits for the pass: no second reap",
        )

        gate.release()
        await time.runMicrotasks()
        assert((await resubscribed).ok)
        await reassert
        await time.runMicrotasks()

        assertEquals(port.reaps(), 2, 'the queued re-check read afresh')
        assertEquals(
            unsubscribedFrom(cx, PRESENCE),
            1,
            'the record written during the hold is applied before any re-hold',
        )
        assertEquals(
            unsubscribedFrom(cy, PRESENCE),
            1,
            're-subscribed cy is not kicked by a stale snapshot',
        )
        assertEquals(redis.zcard(INDEX), 0, 'both records cleared')
        assertEquals(warnings.having(PASS_FAILED), [])
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
    }
})

// --- R12: the decoders and the counted skip ----------------------------------

const bulk = (value: string) => ({ type: 'bulk', value })
const int = (value: number) => ({ type: 'integer', value })
const arr = (...value: unknown[]) => ({ type: 'array', value })
const MARKER = 'MARKER-359-z9'

Deno.test('#359/#405/#411 R12 decodeReapReply accepts only a {t, indexKind, floorKind} triple — t a canonical bulk of at most 15 digits, indexKind and floorKind bulk strings — and refuses anything else with one constant message carrying no reply bytes', () => {
    assertEquals(
        decodeReapReply(arr(bulk('1790157600'), bulk('zset'), bulk('zset'))),
        { t: 1790157600, indexKind: 'zset', floorKind: 'zset' },
    )
    assertEquals(
        decodeReapReply(arr(bulk('0'), bulk('none'), bulk('none'))),
        { t: 0, indexKind: 'none', floorKind: 'none' },
    )
    assertEquals(
        decodeReapReply(
            arr(bulk('9'.repeat(15)), bulk('string'), bulk('hash')),
        ),
        { t: 999_999_999_999_999, indexKind: 'string', floorKind: 'hash' },
    )
    const refused: unknown[] = [
        int(1790157600),
        bulk('1790157600'),
        arr(bulk('01'), bulk('zset'), bulk('zset')),
        arr(bulk(''), bulk('zset'), bulk('zset')),
        arr(bulk('1'.repeat(16)), bulk('zset'), bulk('zset')),
        arr(bulk('1.5'), bulk('zset'), bulk('zset')),
        arr(bulk('1e9'), bulk('zset'), bulk('zset')),
        arr(bulk(' 1'), bulk('zset'), bulk('zset')),
        arr(bulk(`x${MARKER}`), bulk('zset'), bulk('zset')),
        arr(bulk('1790157600')),
        arr(bulk('1790157600'), bulk('zset')),
        arr(bulk('1790157600'), int(1), bulk('zset')),
        arr(bulk('1790157600'), bulk('zset'), int(1)),
        arr(bulk('1790157600'), { type: 'nil' }, bulk('zset')),
        arr(bulk('1790157600'), bulk('zset'), bulk('zset'), bulk('extra')),
        { type: 'nil' },
        undefined,
        null,
    ]
    for (const reply of refused) {
        const error = assertThrows(() => decodeReapReply(reply), Error)
        assertEquals(error.message, REAP_REPLY_REFUSED, JSON.stringify(reply))
        assert(!error.message.includes(MARKER))
    }
})

Deno.test('#359 R12 decodeRevocationPage: a bad envelope throws SCAN_REPLY_REFUSED, an odd body REVOCATION_PAGE_REFUSED, neither carrying reply bytes', () => {
    const envelope: unknown[] = [
        { type: 'nil' },
        arr(bulk('0')),
        arr(bulk('0'), arr(), arr()),
        arr(undefined, arr()),
        arr(bulk('01'), arr()),
        arr(bulk(MARKER), arr()),
        arr(bulk('0'), bulk(MARKER)),
    ]
    for (const reply of envelope) {
        const error = assertThrows(() => decodeRevocationPage(reply), Error)
        assertEquals(error.message, SCAN_REPLY_REFUSED, JSON.stringify(reply))
    }
    const odd = assertThrows(
        () =>
            decodeRevocationPage(
                arr(bulk('0'), arr(bulk('c1'), bulk('100'), bulk(MARKER))),
            ),
        Error,
    )
    assertEquals(odd.message, REVOCATION_PAGE_REFUSED)
    assert(!odd.message.includes(MARKER))
})

Deno.test('#359 R12 decodeRevocationPage: inside a well-formed page each malformed pair is skipped and counted, and only that pair', () => {
    const bad: [unknown, unknown][] = [
        [int(1), bulk('100')],
        [bulk('m1'), int(100)],
        [bulk('m2'), bulk('inf')],
        [bulk('m3'), bulk('+inf')],
        [bulk('m4'), bulk('1.5')],
        [bulk('m5'), bulk('1e9')],
        [bulk('m6'), bulk('01')],
        [bulk('m7'), bulk('1'.repeat(16))],
    ]
    const page = decodeRevocationPage(
        arr(
            bulk('42'),
            arr(
                bulk('good-1'),
                bulk('1790157900'),
                ...bad.flat(),
                bulk('good-2'),
                bulk('7'),
            ),
        ),
    )
    assertEquals(page.cursor, '42')
    assertEquals(page.entries, [
        { member: 'good-1', score: 1790157900 },
        { member: 'good-2', score: 7 },
    ])
    assertEquals(page.skipped, bad.length)
    const empty = decodeRevocationPage(arr(bulk('0'), arr()))
    assertEquals(empty, { cursor: '0', entries: [], skipped: 0 })
})

Deno.test('#359 R12 through listRevocations: malformed pairs never stop the pass — the rest applied, ONE skip WARN per pass with the exact count and no broker bytes; a clean pass logs none', async () => {
    const redis = new FakeRedis()
    redis.setTime(NOW)
    const time = new FakeTime(START)
    // Replies are rewritten on the way back, as a broker formatting scores
    // differently would send them: pairs appended to each page read.
    let inject = true
    const planted = [
        [MARKER, 'inf'],
        [`${MARKER}-2`, '+inf'],
        [`${MARKER}-3`, '1.5'],
        [`${MARKER}-4`, '1e9'],
        [`${MARKER}-5`, '01'],
    ]
    const command: CommandFn = async (...args) => {
        const reply = await redis.command(...args)
        if (!inject || !isPageRead(args)) return reply
        const [cursor, items] = (reply as { value: unknown[] }).value as [
            unknown,
            { value: unknown[] },
        ]
        return arr(
            cursor,
            arr(
                ...items.value,
                ...planted.flatMap(([m, s]) => [bulk(m), bulk(s)]),
                int(5),
                bulk(String(NOW + 300)),
            ),
        )
    }
    const b = instance(redis, command)
    const warnings = captureWarnings()
    try {
        const c1 = conn('c1')
        b.manager.register(c1)
        const c2 = conn('c2')
        b.manager.register(c2)
        assert((await b.manager.subscribe(c1, ROOM)).ok)
        assert((await b.manager.subscribe(c2, ROOM)).ok)
        await plant(redis, 'c1')
        await plant(redis, scoped('c2', ROOM, 'r2'))
        // A 16-digit score, written through ZADD: well formed, out of grammar.
        await plant(redis, `${MARKER}-6`, 1_000_000_000_000_000)

        await runOnePass(time)
        assert(evicted(c1), 'the connection record is applied')
        assertEquals(unsubscribedFrom(c2, ROOM), 1, 'and the channel record')
        const skip = warnings.having(REVOCATION_PAIRS_SKIPPED)
        assertEquals(skip.length, 1, 'exactly one skip WARN for the pass')
        assertEquals(skip[0], `${REVOCATION_PAIRS_SKIPPED} 7`)
        assert(!warnings.lines.some((l) => l.includes(MARKER)))
        assertEquals(warnings.having(PASS_FAILED), [], 'the pass did not fail')

        await runOnePass(time)
        assertEquals(
            warnings.having(REVOCATION_PAIRS_SKIPPED).length,
            2,
            'the next pass reports it again',
        )

        inject = false
        await redis.command('ZREM', INDEX, `${MARKER}-6`)
        await runOnePass(time)
        assertEquals(
            warnings.having(REVOCATION_PAIRS_SKIPPED).length,
            2,
            'a clean pass logs no skip WARN',
        )
        redis.assertNoRejections()
    } finally {
        warnings.restore()
        await b.driver.close()
        time.restore()
    }
})

// --- R2: the reply cap itself, on a live broker ------------------------------

/**
 * Seed `ARGV[1]` channel-scoped records of maximum length (200-byte target,
 * channel and id), scored at the broker's `TIME + 300`, in ONE script, and
 * answer how many.
 */
const SEED_SCRIPT = [
    "local t = tonumber(redis.call('TIME')[1])",
    'local pad = ARGV[2]',
    'for i = 1, tonumber(ARGV[1]) do',
    "  local s = string.format('%08d', i)",
    "  redis.call('ZADD', KEYS[1], t + 300, 'x' .. s .. string.sub(pad, 1, 191) .. ' private-' .. s .. string.sub(pad, 1, 184) .. ' id' .. s .. string.sub(pad, 1, 190))",
    'end',
    'return tonumber(ARGV[1])',
].join('\n')

Deno.test({
    name:
        '#359 R2 an index past the 32 MiB reply cap: one pass on B applies its own three, with no failure line, and B’s client keeps working',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const ns = runNamespace()
        const prefix = `${ns}:rt`
        const index = `${prefix}__revocations`
        let fire: (() => void | Promise<void>) | undefined
        const subscriber = {
            psubscribe: () => {},
            onReconnect: (handler: () => void | Promise<void>) =>
                void (fire = handler),
        }
        const driver = new RedisBroadcastDriver(live, subscriber, {
            prefix,
            control: { secret: SECRET },
            revocationTtlSeconds: 300,
        })
        const manager = new ChannelManager<User>({ driver, authorize })
        const warnings = captureWarnings()
        try {
            const seeded = await live.command(
                'EVAL',
                SEED_SCRIPT,
                '1',
                index,
                '60000',
                'a'.repeat(200),
            )
            assertEquals(seeded, { type: 'integer', value: 60_000 })
            const c1 = conn('c1')
            const c2 = conn('c2')
            const c3 = conn('c3')
            for (const c of [c1, c2, c3]) {
                manager.register(c)
                assert((await manager.subscribe(c, ROOM)).ok)
            }
            const time = await live.command('TIME') as unknown as {
                value: { value: string }[]
            }
            const expiry = String(Number(time.value[0].value) + 300)
            await live.command('ZADD', index, expiry, 'c1')
            await live.command('ZADD', index, expiry, scoped('c2', ROOM, 'r2'))
            await live.command('ZADD', index, expiry, scoped('c3', ROOM, 'r3'))

            void fire?.()
            await waitFor(
                () =>
                    evicted(c1) && unsubscribedFrom(c2, ROOM) === 1 &&
                    unsubscribedFrom(c3, ROOM) === 1,
                'B applied its three revocations',
                60_000,
            )
            assertEquals(warnings.having(PASS_FAILED), [])
            assertEquals(
                await live.command('PING'),
                { type: 'simple', value: 'PONG' },
                "B's client still answers",
            )
        } finally {
            warnings.restore()
            await driver.close()
            await teardown(live, ns)
            await live.close()
        }
    },
})
