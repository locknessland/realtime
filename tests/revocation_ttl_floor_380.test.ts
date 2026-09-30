/**
 * @fileoverview #380 — a fleet-wide revocation TTL floor: every durable
 * revocation record outlives the longest live reader's `revocationTtlSeconds`,
 * so a peer configured with a shorter TTL no longer lets a record expire
 * before a longer-interval reader's next pass applies it.
 *
 * Every witness drives real `RedisBroadcastDriver`s over one `FakeRedis`:
 *
 * - the broker clock is pinned with `FakeRedis.setTime`, and moved only by
 *   the witness that means to move it;
 * - timers run on FakeTime, advanced in short steps with the microtask queue
 *   drained after each ({@link advance}), and `performance.now` follows it;
 * - `console.warn` and `console.error` are captured into one {@link Journal}
 *   beside every command the recording port issued, so a witness can say a
 *   WARN was written AFTER the mark's `EVAL`, not merely that it was written;
 * - "no escaped rejection" is the #395 escape watcher's word, never a guess.
 *
 * The floor and index are read raw, by the test only (`ZSCAN`, which carries
 * scores, and `FakeRedis.expiryOf` for a key's own TTL).
 *
 * **Red before the fix** (on the #380 skeleton over `32baca7b`): F1, F3,
 * F4 (i, iv, v), F5 (i–iv), F6, F7, F8 (through its precondition), F9 (i, ii),
 * F11, F12, F13 (i), F14 (i–iii) and F15. **Pins, green before and after:**
 * F2 and F10. **Vacuously green before the fix**, meaningful only once the
 * announce exists: F4 (ii), F4 (iii) and F13 (ii) — each asserts that NO
 * announce is issued; their mutants (N15, N16, N23) prove their power.
 *
 * Test names start `#380 F<n> ` with a trailing space, so `F1 ` is not a
 * prefix of `F10`–`F15`.
 *
 * @module @lockness/realtime/tests/revocation_ttl_floor_380
 */

import { assert, assertEquals, assertRejects, assertThrows } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager } from '../manager.ts'
import {
    decodeRevocationFloor,
    MAX_REVOCATION_TTL_SECONDS,
    RedisBroadcastDriver,
    type RedisSubscriber,
    REVOCATION_FLOOR_ANNOUNCE_FAILED,
    REVOCATION_FLOOR_LOG_FAILED,
    REVOCATION_FLOOR_READ_FAILED,
    REVOCATION_FLOOR_REFUSED,
    REVOCATION_FLOOR_SKIPPED,
} from '../drivers/redis.ts'
import { type CommandFn, type CommandMatch, FakeRedis } from './fake_redis.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'
import { isAnnounce, isReap } from './revocation_wire.ts'

const START = new Date('2026-09-25T10:00:00Z')
/** The fake broker's `TIME` at the start of every witness, pinned. */
const T0 = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
/** The revocation index key: a pin of the name, never an oracle for it. */
const INDEX = `${PREFIX}__revocations`
/** The revocation floor key: a pin of the name, never an oracle for it. */
const FLOOR = `${PREFIX}__revocation-floor`
/** Extra seconds on a key's own TTL beyond the longest member (#276): a pin. */
const SLACK = 60

/**
 * The OLD release's mark and reap, frozen from `32baca7b` (F10).
 *
 * **Pins of the old release's behaviour, not oracles for the new code** (A6):
 * they are what an instance that has not yet upgraded still sends, so a mixed
 * fleet is driven with the exact bytes it would meet on the broker. They must
 * never be updated to follow `drivers/redis.ts`.
 */
const OLD_MARK_SCRIPT = [
    "local t = redis.call('TIME')[1]",
    "redis.call('ZADD', KEYS[1], 'GT', t + ARGV[1], ARGV[2])",
    "redis.call('EXPIRE', KEYS[1], ARGV[3], 'NX')",
    "redis.call('EXPIRE', KEYS[1], ARGV[3], 'GT')",
].join('\n')
/** The old reap, frozen from `32baca7b` (see {@link OLD_MARK_SCRIPT}). */
const OLD_REAP_SCRIPT = [
    "local t = redis.call('TIME')[1]",
    "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', t)",
    'return t',
].join('\n')

// ---------------------------------------------------------------------------
// Matchers — the reap and the announce through their one test-side home
// ---------------------------------------------------------------------------

const announce: CommandMatch = (args) => isAnnounce(args, FLOOR)
const reap: CommandMatch = (args) => isReap(args, INDEX)
/** The mark's floor read. */
const floorRead: CommandMatch = (args) =>
    args[0] === 'ZRANGEBYSCORE' && args[1] === FLOOR
/** The mark's `EVAL`: one key, and that key is the index. */
const markEval: CommandMatch = (args) =>
    args[0] === 'EVAL' && args[2] === '1' && args[3] === INDEX

// ---------------------------------------------------------------------------
// The journal: commands and log lines, in the order they happened
// ---------------------------------------------------------------------------

/** One thing that happened: a command issued, or a log line attempted. */
type Entry =
    | { readonly kind: 'command'; readonly args: readonly string[] }
    | { readonly kind: 'warn' | 'error'; readonly line: string }

type Journal = Entry[]

/** Captured log lines, and a `console.warn` that can be made to throw. */
function captureLogs(journal: Journal) {
    const warn = console.warn
    const error = console.error
    let warnFails = false
    console.warn = (...parts: unknown[]) => {
        journal.push({ kind: 'warn', line: parts.map(String).join(' ') })
        if (warnFails) throw new Error('warn sink down (#380)')
    }
    console.error = (...parts: unknown[]) =>
        void journal.push({ kind: 'error', line: parts.map(String).join(' ') })
    const lines = (kind: 'warn' | 'error', prefix: string) =>
        journal.filter((e): e is Extract<Entry, { line: string }> =>
            e.kind === kind && e.line.startsWith(prefix)
        ).map((e) => e.line)
    return {
        /** WARN lines (attempted) starting with `prefix`. */
        warns: (prefix: string) => lines('warn', prefix),
        /** ERROR lines starting with `prefix` and a space. */
        marked: (prefix: string) => lines('error', `${prefix} `),
        /** Make `console.warn` throw, or stop it throwing. */
        failWarn: (on: boolean) => void (warnFails = on),
        restore: () => {
            console.warn = warn
            console.error = error
        },
    }
}

type Logs = ReturnType<typeof captureLogs>

/**
 * A command port over the fake that journals every command AT ISSUE, and can
 * refuse — once, or from now on — a command matched by a predicate.
 */
function recordingPort(redis: FakeRedis, journal: Journal) {
    const sent: string[][] = []
    let once: { match: CommandMatch; error: Error } | undefined
    let from: { match: CommandMatch; error: Error } | undefined
    let answer: { match: CommandMatch; reply: unknown } | undefined
    const command: CommandFn = (...args) => {
        sent.push(args)
        journal.push({ kind: 'command', args })
        if (once?.match(args)) {
            const { error } = once
            once = undefined
            return Promise.reject(error)
        }
        if (from?.match(args)) return Promise.reject(from.error)
        if (answer?.match(args)) {
            const { reply } = answer
            answer = undefined
            return Promise.resolve(reply)
        }
        return redis.command(...args)
    }
    return {
        command,
        /** How many commands matching `match` were issued. */
        issued: (match: CommandMatch) => sent.filter(match).length,
        /** Every issued command matching `match`, in order. */
        all: (match: CommandMatch) => sent.filter(match),
        /** Refuse the NEXT command matching `match`. */
        refuseOnce: (match: CommandMatch, error: Error) =>
            void (once = { match, error }),
        /** Refuse every command matching `match` from now on. */
        refuseFrom: (match: CommandMatch, error: Error) =>
            void (from = { match, error }),
        /** Answer the NEXT command matching `match` with `reply`, unexecuted. */
        answerOnce: (match: CommandMatch, reply: unknown) =>
            void (answer = { match, reply }),
    }
}

type Port = ReturnType<typeof recordingPort>

/** Everything one witness shares. */
interface Fixture {
    time: FakeTime
    redis: FakeRedis
    journal: Journal
    logs: Logs
    /** A recording port over the shared fake. */
    port(): Port
    /** A raw driver over the shared fake, closed when the witness ends. */
    driver(options: {
        interval: number
        ttl: number
        command?: CommandFn
        subscriber?: RedisSubscriber
    }): RedisBroadcastDriver
}

/**
 * Run `body` under FakeTime at `START`, the broker clock pinned at `T0`,
 * `performance.now` on the fake clock and the console captured. Every driver
 * built is closed, and everything restored, whatever happens.
 */
async function withFixture(body: (f: Fixture) => Promise<void>) {
    const time = new FakeTime(START)
    const passClock = performance.now
    performance.now = () => time.now
    const journal: Journal = []
    const logs = captureLogs(journal)
    const redis = new FakeRedis()
    redis.setTime(T0)
    const drivers: RedisBroadcastDriver[] = []
    try {
        await body({
            time,
            redis,
            journal,
            logs,
            port: () => recordingPort(redis, journal),
            driver: ({ interval, ttl, command, subscriber }) => {
                const driver = new RedisBroadcastDriver(
                    { command: command ?? redis.command },
                    subscriber ?? redis.subscriberFor(),
                    {
                        prefix: PREFIX,
                        revocationTtlSeconds: ttl,
                        presence: { reconcileIntervalMs: interval },
                    },
                )
                drivers.push(driver)
                return driver
            },
        })
        redis.assertNoRejections()
    } finally {
        try {
            for (const driver of drivers) await driver.close()
        } finally {
            logs.restore()
            performance.now = passClock
            time.restore()
        }
    }
}

/**
 * Advance fake time by `ms` in steps of at most 250 ms, draining the
 * microtask queue after each, so every timer runs at its own instant.
 */
async function advance(time: FakeTime, ms: number): Promise<void> {
    let left = ms
    while (left > 0) {
        const by = Math.min(250, left)
        await time.tickAsync(by)
        await time.runMicrotasks()
        left -= by
    }
}

/** A sorted set's members and scores, read raw by the test (`ZSCAN`). */
async function entries(
    redis: FakeRedis,
    key: string,
): Promise<Record<string, number>> {
    const reply = await redis.command('ZSCAN', key, '0', 'COUNT', '1000') as {
        value: [{ value: string }, { value: { value: string }[] }]
    }
    assertEquals(reply.value[0].value, '0', `${key} read in one page`)
    const items = reply.value[1].value
    const out: Record<string, number> = {}
    for (let i = 0; i < items.length; i += 2) {
        out[items[i].value] = Number(items[i + 1].value)
    }
    return out
}

/** A key's own remaining TTL, in seconds, against broker time `now`. */
function keyTtl(redis: FakeRedis, key: string, now: number): number {
    const at = redis.expiryOf(key)
    assert(at !== undefined, `${key} carries its own TTL`)
    return at - now
}

/** Seed the floor raw, as something other than this driver could. */
async function seedFloor(redis: FakeRedis, members: readonly string[]) {
    for (const member of members) {
        await redis.command('ZADD', FLOOR, String(T0 + 100), member)
    }
}

/** The journal position of the first entry matching `test`, or -1. */
function position(journal: Journal, test: (e: Entry) => boolean): number {
    return journal.findIndex(test)
}

/** The floor members F6 seeds: one valid TTL, then seven off-grammar. */
const CORRUPT_FLOOR = ['300', '1e3', '0x10', ' 5', '5.5', 'inf', '', '012']

// ---------------------------------------------------------------------------
// US1 — a short-TTL peer's revocation still reaches a long-interval reader
// ---------------------------------------------------------------------------

Deno.test("#380 F1 a TTL-10 peer's revocation reaches a TTL-300 reader whose next pass is 59 s later", async () => {
    await withFixture(async (f) => {
        const w = f.driver({ interval: 5_000, ttl: 10 })
        const r = f.driver({ interval: 60_000, ttl: 300 })
        const passes: string[][] = []
        r.onRevocationReconcile(async () => {
            const found = await r.listRevocations((target) => target === 'c1')
            passes.push(found.map((revocation) => revocation.target))
        })
        // One R reap.
        await r.listRevocations()
        await advance(f.time, 1_000)
        f.redis.setTime(T0 + 1)
        // W marks; no control frame is ever sent.
        await w.markRevocation({ target: 'c1' })
        // R's next pass, 59 s after the mark, on the broker clock too.
        f.redis.setTime(T0 + 60)
        await advance(f.time, 59_000)
        assertEquals(passes, [['c1']], "R's pass applies W's revocation")
    })
})

Deno.test('#380 F2 a uniform fleet: a mark scores t + 300 exactly, the index TTL in [300, 360]', async () => {
    await withFixture(async (f) => {
        const a = f.driver({ interval: 60_000, ttl: 300 })
        const b = f.driver({ interval: 60_000, ttl: 300 })
        await a.listRevocations()
        await b.listRevocations()
        await a.markRevocation({ target: 'c1' })
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 300)
        const ttl = keyTtl(f.redis, INDEX, T0)
        assert(ttl >= 300 && ttl <= 360, `index key TTL ${ttl}`)
    })
})

Deno.test('#380 F3 one reap by a TTL-300 driver writes exactly { 300: t + 300 }, key TTL 360', async () => {
    await withFixture(async (f) => {
        const r = f.driver({ interval: 60_000, ttl: 300 })
        await r.listRevocations()
        assertEquals(await entries(f.redis, FLOOR), { '300': T0 + 300 })
        assertEquals(keyTtl(f.redis, FLOOR, T0), 300 + SLACK)
    })
})

Deno.test('#380 F7 a floor member above the range is clamped: the record and the index TTL use the maximum', async () => {
    await withFixture(async (f) => {
        await seedFloor(f.redis, ['9999999'])
        const w = f.driver({ interval: 5_000, ttl: 10 })
        await w.markRevocation({ target: 'c1' })
        assertEquals(
            (await entries(f.redis, INDEX)).c1,
            T0 + MAX_REVOCATION_TTL_SECONDS,
        )
        assertEquals(
            keyTtl(f.redis, INDEX, T0),
            MAX_REVOCATION_TTL_SECONDS + SLACK,
        )
    })
})

Deno.test('#380 F11 an absent floor: the record scores t + own TTL, and the mark is one floor read then one EVAL', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        const before = f.journal.length
        await w.markRevocation({ target: 'c1' })
        const issued = f.journal.slice(before)
            .filter((e) => e.kind === 'command')
            .map((e) => (e as { args: readonly string[] }).args)
        assertEquals(issued.length, 2, 'two round trips')
        assertEquals(issued[0], ['ZRANGEBYSCORE', FLOOR, '-inf', '+inf'])
        assert(markEval(issued[1]), `then the mark: ${issued[1]}`)
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 10)
    })
})

Deno.test('#380 F12 decodeRevocationFloor refuses a reply that is not an array of bulk strings, skips and clamps members', () => {
    const bulk = (value: string) => ({ type: 'bulk', value })
    for (
        const reply of [
            bulk('300'),
            { type: 'array', value: [{ type: 'integer', value: 300 }] },
            { type: 'array', value: [bulk('300'), { type: 'nil' }] },
            { type: 'nil' },
            null,
        ]
    ) {
        const error = assertThrows(
            () => decodeRevocationFloor(reply, 10),
            Error,
        )
        assertEquals(error.message, REVOCATION_FLOOR_REFUSED, 'no reply text')
    }
    assertEquals(
        decodeRevocationFloor({ type: 'array', value: [] }, 10),
        { ttl: 10, skipped: 0 },
    )
    assertEquals(
        decodeRevocationFloor(
            { type: 'array', value: CORRUPT_FLOOR.map(bulk) },
            10,
        ),
        { ttl: 300, skipped: 7 },
    )
    assertEquals(
        decodeRevocationFloor(
            { type: 'array', value: [bulk('9999999'), bulk('0')] },
            10,
        ),
        { ttl: MAX_REVOCATION_TTL_SECONDS, skipped: 0 },
    )
    // The own TTL wins over a smaller member.
    assertEquals(
        decodeRevocationFloor({ type: 'array', value: [bulk('5')] }, 10),
        { ttl: 10, skipped: 0 },
    )
})

// ---------------------------------------------------------------------------
// US3 — a stopped long-TTL reader stops lengthening records
// ---------------------------------------------------------------------------

Deno.test("#380 F8 301 s after a TTL-300 reader's last reap, a TTL-10 reap prunes its entry and marks score t + 10", async () => {
    await withFixture(async (f) => {
        const r = f.driver({ interval: 60_000, ttl: 300 })
        const w = f.driver({ interval: 5_000, ttl: 10 })
        await r.listRevocations()
        assertEquals(
            await entries(f.redis, FLOOR),
            { '300': T0 + 300 },
            "precondition: R's reap wrote its entry",
        )
        f.redis.setTime(T0 + 301)
        await w.listRevocations()
        const floor = await entries(f.redis, FLOOR)
        assert(!('300' in floor), `R's entry is gone: ${JSON.stringify(floor)}`)
        await w.markRevocation({ target: 'c1' })
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 301 + 10)
    })
})

Deno.test('#380 F9 (i) a shorter-TTL write never pulls a floor entry or the key back in', async () => {
    await withFixture(async (f) => {
        const long = f.driver({ interval: 60_000, ttl: 300 })
        const short = f.driver({ interval: 5_000, ttl: 10 })
        await long.listRevocations()
        f.redis.setTime(T0 + 5)
        await short.listRevocations()
        assertEquals((await entries(f.redis, FLOOR))['300'], T0 + 300)
        assert(keyTtl(f.redis, FLOOR, T0 + 5) >= 355, 'key TTL kept')
        // The broker clock steps back: the same TTL written again at an
        // earlier second would pull the entry in, and `GT` refuses it.
        f.redis.setTime(T0 - 5)
        await long.listRevocations()
        assertEquals((await entries(f.redis, FLOOR))['300'], T0 + 300)
    })
})

Deno.test('#380 F9 (ii) a longer-TTL write extends the key a shorter one armed', async () => {
    await withFixture(async (f) => {
        const short = f.driver({ interval: 5_000, ttl: 10 })
        const long = f.driver({ interval: 60_000, ttl: 300 })
        await short.listRevocations()
        assertEquals(keyTtl(f.redis, FLOOR, T0), 10 + SLACK)
        await long.listRevocations()
        assertEquals(keyTtl(f.redis, FLOOR, T0), 300 + SLACK)
    })
})

// ---------------------------------------------------------------------------
// US4 — a corrupt or unreadable floor never blocks a revocation
// ---------------------------------------------------------------------------

Deno.test('#380 F6 a corrupt floor: the record scores t + 300, and one count-only WARN follows the EVAL', async () => {
    await withFixture(async (f) => {
        await seedFloor(f.redis, CORRUPT_FLOOR)
        const port = f.port()
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        await w.markRevocation({ target: 'c1' })
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 300)
        assertEquals(f.logs.warns(REVOCATION_FLOOR_SKIPPED), [
            `${REVOCATION_FLOOR_SKIPPED} 7`,
        ])
        const evalAt = position(
            f.journal,
            (e) => e.kind === 'command' && markEval(e.args),
        )
        const warnAt = position(
            f.journal,
            (e) =>
                e.kind === 'warn' &&
                e.line.startsWith(REVOCATION_FLOOR_SKIPPED),
        )
        assert(evalAt >= 0 && warnAt > evalAt, 'the WARN follows the EVAL')
    })
})

Deno.test('#380 F14 (i) a WRONGTYPE floor read: the mark resolves at the maximum TTL, one WARN after the EVAL', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        // The broker's error reply to a ZRANGEBYSCORE on a string key, as the
        // command client raises it: FakeRedis does not type-check a zset read.
        port.refuseOnce(
            floorRead,
            new Error(
                'WRONGTYPE Operation against a key holding the wrong kind of value',
            ),
        )
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        await w.markRevocation({ target: 'c1' })
        assertEquals(
            (await entries(f.redis, INDEX)).c1,
            T0 + MAX_REVOCATION_TTL_SECONDS,
        )
        const warns = f.logs.warns(REVOCATION_FLOOR_READ_FAILED)
        assertEquals(warns.length, 1)
        assert(warns[0].includes('WRONGTYPE'), warns[0])
        const evalAt = position(
            f.journal,
            (e) => e.kind === 'command' && markEval(e.args),
        )
        const warnAt = position(
            f.journal,
            (e) =>
                e.kind === 'warn' &&
                e.line.startsWith(REVOCATION_FLOOR_READ_FAILED),
        )
        assert(evalAt >= 0 && warnAt > evalAt, 'the WARN follows the EVAL')
    })
})

Deno.test('#380 F14 (ii) a transport error or a refused reply on the floor read: the mark resolves at the maximum TTL', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        port.refuseOnce(floorRead, new Error('connection reset (#380)'))
        await w.markRevocation({ target: 'c1' })
        port.answerOnce(floorRead, { type: 'nil' })
        await w.markRevocation({ target: 'c2' })
        const index = await entries(f.redis, INDEX)
        assertEquals(index.c1, T0 + MAX_REVOCATION_TTL_SECONDS)
        assertEquals(index.c2, T0 + MAX_REVOCATION_TTL_SECONDS)
        const warns = f.logs.warns(REVOCATION_FLOOR_READ_FAILED)
        assertEquals(warns.length, 2, 'one WARN per mark')
        assert(warns[0].includes('connection reset'), warns[0])
        assert(warns[1].includes(REVOCATION_FLOOR_REFUSED), warns[1])
    })
})

Deno.test('#380 F14 (iii) the EVAL itself rejecting still fails the mark', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        port.refuseOnce(markEval, new Error('EVAL refused (#380)'))
        await assertRejects(
            () => w.markRevocation({ target: 'c1' }),
            Error,
            'EVAL refused (#380)',
        )
        assertEquals(port.issued(floorRead), 1, 'the floor was read first')
    })
})

Deno.test('#380 F15 a throwing console.warn never fails a mark: the record is written, one marked line per WARN', async () => {
    await withFixture(async (f) => {
        await seedFloor(f.redis, ['1e3'])
        const port = f.port()
        const w = f.driver({ interval: 5_000, ttl: 10, command: port.command })
        f.logs.failWarn(true)
        await w.markRevocation({ target: 'c1' })
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 10)
        assertEquals(f.logs.marked(REVOCATION_FLOOR_LOG_FAILED).length, 1)
        port.refuseOnce(floorRead, new Error('connection reset (#380)'))
        await w.markRevocation({ target: 'c2' })
        assertEquals(
            (await entries(f.redis, INDEX)).c2,
            T0 + MAX_REVOCATION_TTL_SECONDS,
        )
        const marked = f.logs.marked(REVOCATION_FLOOR_LOG_FAILED)
        assertEquals(marked.length, 2)
        assert(marked[1].includes('sink failure'), marked[1])
        assert(marked[1].includes('warn sink down'), marked[1])
    })
})

// ---------------------------------------------------------------------------
// US5 — the announce, and a reader whose first announce fails
// ---------------------------------------------------------------------------

Deno.test('#380 F4 (i) the first registration issues exactly one announce, 1 <floor> 300 360, before any reap', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        r.onRevocationReconcile(() => {})
        await f.time.runMicrotasks()
        const sent = port.all(announce)
        assertEquals(sent.length, 1, 'one announce')
        assertEquals(sent[0].slice(2), ['1', FLOOR, '300', '360'])
        assertEquals(port.issued(reap), 0, 'no reap yet')
        assertEquals(await entries(f.redis, FLOOR), { '300': T0 + 300 })
    })
})

Deno.test('#380 F4 (ii) a re-registration announces nothing', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        r.onRevocationReconcile(() => {})
        const first = port.issued(announce)
        r.onRevocationReconcile(() => {})
        r.onRevocationReconcile(() => {})
        await f.time.runMicrotasks()
        assertEquals(port.issued(announce), first, 'no second announce')
    })
})

Deno.test('#380 F4 (iii) a registration after close() announces nothing', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        await r.close()
        r.onRevocationReconcile(() => {})
        await f.time.runMicrotasks()
        assertEquals(port.issued(announce), 0)
    })
})

Deno.test('#380 F4 (iv) a ChannelManager over the driver announces exactly once', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        new ChannelManager({ driver: r })
        await f.time.runMicrotasks()
        assertEquals(port.issued(announce), 1)
    })
})

Deno.test('#380 F4 (v) the announce is not a reap on the wire: no reap until the first pass, then one', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        r.onRevocationReconcile(async () => void await r.listRevocations())
        await f.time.runMicrotasks()
        assertEquals(port.issued(announce), 1, 'the announce was issued')
        assertEquals(port.issued(reap), 0, 'and it is not a reap')
        const [sent] = port.all(announce)
        assert(!isReap(sent, INDEX), 'isReap does not match the announce')
        assert(!sent.includes(INDEX), 'the announce never carries the index')
        await advance(f.time, 60_000)
        assertEquals(port.issued(reap), 1, 'the first pass reaps once')
        assertEquals(port.issued(announce), 1, 'and announces nothing')
        const [reaped] = port.all(reap)
        assert(!isAnnounce(reaped, FLOOR), 'isAnnounce does not match a reap')
    })
})

Deno.test('#380 F5 (i) a rejected announce writes one WARN and nothing escapes', async () => {
    await watchingEscapes(async (escaped) => {
        await withFixture(async (f) => {
            const port = f.port()
            port.refuseOnce(announce, new Error('announce refused (#380)'))
            const r = f.driver({
                interval: 60_000,
                ttl: 300,
                command: port.command,
            })
            r.onRevocationReconcile(() => {})
            await settle()
            // The escape first: a removed `catch` is red HERE, through the
            // rejection itself, not merely through the WARN it no longer
            // writes.
            assertEquals(escaped, [], 'no rejection reaches the runtime')
            const warns = f.logs.warns(REVOCATION_FLOOR_ANNOUNCE_FAILED)
            assertEquals(warns.length, 1, 'one WARN')
            assert(warns[0].includes('announce refused (#380)'), warns[0])
        })
        await settle()
        assertEquals(escaped, [], 'no rejection reaches the runtime')
    })
})

Deno.test('#380 F5 (ii) a rejected announce with console.warn throwing: one marked line carrying both halves', async () => {
    await watchingEscapes(async (escaped) => {
        await withFixture(async (f) => {
            const port = f.port()
            port.refuseOnce(announce, new Error('announce refused (#380)'))
            const r = f.driver({
                interval: 60_000,
                ttl: 300,
                command: port.command,
            })
            f.logs.failWarn(true)
            r.onRevocationReconcile(() => {})
            await settle()
            const marked = f.logs.marked(REVOCATION_FLOOR_LOG_FAILED)
            assertEquals(marked.length, 1, 'one marked line')
            assert(marked[0].includes('announce refused (#380)'), marked[0])
            assert(marked[0].includes('sink failure'), marked[0])
            assert(marked[0].includes('warn sink down'), marked[0])
        })
        await settle()
        assertEquals(escaped, [], 'no rejection reaches the runtime')
    })
})

Deno.test('#380 F5 (iii) after a rejected announce, the first completed reap writes the entry and stops the retry', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        port.refuseFrom(announce, new Error('announce refused (#380)'))
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        r.onRevocationReconcile(() => {})
        await f.time.runMicrotasks()
        assertEquals(port.issued(announce), 1)
        await r.listRevocations()
        assertEquals(await entries(f.redis, FLOOR), { '300': T0 + 300 })
        await advance(f.time, 120_000)
        assertEquals(port.issued(announce), 1, 'no retry after the reap')
        assertEquals(
            f.logs.warns(REVOCATION_FLOOR_ANNOUNCE_FAILED).length,
            1,
            'one WARN',
        )
    })
})

Deno.test('#380 F5 (iv) a port whose command() throws synchronously: registration completes, one WARN, nothing escapes', async () => {
    await watchingEscapes(async (escaped) => {
        await withFixture(async (f) => {
            let reconnects = 0
            const subscriber: RedisSubscriber = {
                psubscribe: () => {},
                onReconnect: () => void reconnects++,
            }
            const r = f.driver({
                interval: 60_000,
                ttl: 300,
                subscriber,
                command: (...args) => {
                    if (announce(args)) throw new Error('sync throw (#380)')
                    return f.redis.command(...args)
                },
            })
            r.onRevocationReconcile(() => {})
            assertEquals(reconnects, 1, 'onReconnect is still registered')
            await settle()
            const warns = f.logs.warns(REVOCATION_FLOOR_ANNOUNCE_FAILED)
            assertEquals(warns.length, 1, 'one WARN')
            assert(warns[0].includes('sync throw (#380)'), warns[0])
        })
        await settle()
        assertEquals(escaped, [], 'no rejection reaches the runtime')
    })
})

Deno.test('#380 F13 (i) an announce rejected once is retried within 2 s, so a TTL-10 mark at +2 s scores t + 300', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        port.refuseOnce(announce, new Error('announce refused (#380)'))
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        const w = f.driver({ interval: 5_000, ttl: 10 })
        r.onRevocationReconcile(() => {})
        f.redis.setTime(T0 + 1)
        await advance(f.time, 1_999)
        assertEquals(
            port.issued(announce),
            2,
            'the first backoff step is below 2 s',
        )
        f.redis.setTime(T0 + 2)
        await advance(f.time, 1)
        await w.markRevocation({ target: 'c1' })
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 2 + 300)
    })
})

Deno.test('#380 F13 (ii) close() stops the announce retry: no announce after it, no timer left pending', async () => {
    await withFixture(async (f) => {
        const port = f.port()
        port.refuseFrom(announce, new Error('announce refused (#380)'))
        const r = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        r.onRevocationReconcile(() => {})
        await advance(f.time, 1_500)
        await r.close()
        const atClose = port.issued(announce)
        assertEquals(await f.time.nextAsync(), false, 'no timer left pending')
        await advance(f.time, 120_000)
        assertEquals(port.issued(announce), atClose, 'no announce after close')
        // close() while the first announce is still in flight: its failure
        // arms no retry.
        const again = f.driver({
            interval: 60_000,
            ttl: 300,
            command: port.command,
        })
        again.onRevocationReconcile(() => {})
        const inFlight = port.issued(announce)
        await again.close()
        await settle()
        assertEquals(await f.time.nextAsync(), false, 'no retry was armed')
        await advance(f.time, 120_000)
        assertEquals(port.issued(announce), inFlight, 'no announce after close')
    })
})

Deno.test('#380 F16 a failing announce is retried 1 s, 2 s, then doubling until reconcileIntervalMs caps the gap', async () => {
    await withFixture(async (f) => {
        // Every announce is refused; the issue instant of each is recorded on
        // the fake clock, so the gaps ARE the backoff schedule.
        const issuedAt: number[] = []
        const r = f.driver({
            interval: 5_000,
            ttl: 300,
            command: (...args) => {
                if (announce(args)) {
                    issuedAt.push(Date.now())
                    return Promise.reject(new Error('announce refused (#380)'))
                }
                return f.redis.command(...args)
            },
        })
        // A handler that never reads, so no completed pass stops the retry.
        r.onRevocationReconcile(() => {})
        await advance(f.time, 20_000)
        const gaps = issuedAt.slice(1).map((at, i) => at - issuedAt[i])
        assertEquals(
            gaps,
            [1_000, 2_000, 4_000, 5_000, 5_000],
            'first step 1 s, doubling, capped at reconcileIntervalMs (5 s)',
        )
        assertEquals(
            f.logs.warns(REVOCATION_FLOOR_ANNOUNCE_FAILED).length,
            issuedAt.length,
            'one WARN per failed attempt, retries included',
        )
    })
})

// ---------------------------------------------------------------------------
// US6 — an upgrade in progress is no worse than today
// ---------------------------------------------------------------------------

Deno.test('#380 F10 an old-release writer and reap beside a new TTL-300 reader: nothing shortened, nothing reaped early', async () => {
    await withFixture(async (f) => {
        const r = f.driver({ interval: 60_000, ttl: 300 })
        await r.listRevocations()
        const floorBefore = await entries(f.redis, FLOOR)
        const floorExpiry = f.redis.expiryOf(FLOOR)
        // The old writer at TTL 10: its own call form, frozen.
        await f.redis.command(
            'EVAL',
            OLD_MARK_SCRIPT,
            '1',
            INDEX,
            '10',
            'c1',
            String(10 + SLACK),
        )
        assertEquals((await entries(f.redis, INDEX)).c1, T0 + 10, 'as today')
        // The old reap, 5 s later.
        f.redis.setTime(T0 + 5)
        await f.redis.command('EVAL', OLD_REAP_SCRIPT, '1', INDEX)
        assertEquals(
            (await entries(f.redis, INDEX)).c1,
            T0 + 10,
            'nothing live was reaped',
        )
        assertEquals(await entries(f.redis, FLOOR), floorBefore)
        assertEquals(f.redis.expiryOf(FLOOR), floorExpiry, 'floor untouched')
    })
})
