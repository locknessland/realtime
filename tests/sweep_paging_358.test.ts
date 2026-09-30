/**
 * @fileoverview #358 — the ghost sweep reads a dead instance's owned set in
 * bounded `SSCAN` pages: one full iteration per instance per pass, no budget,
 * no resume state.
 *
 * Before #358 `#sweepOwned` read the whole owned set with one `SMEMBERS`, on
 * the survivor's SHARED command client. Past `MAX_REPLY_BYTES` of wire the
 * reply was refused, the sweep never finished, and the client discarded its
 * socket and opened its refusal window on every pass.
 *
 * Every FakeRedis witness runs a real `RedisBroadcastDriver` "B" behind #348's
 * `serializedCommands` (one exchange in flight, as on the production client),
 * sweeping an instance whose holds are planted exactly as
 * `HOLD_MEMBER_SCRIPT` writes them and which never set a liveness key — dead
 * from the start. The fake pages by stable member-derived slots
 * (`FakeRedis.scanSlot`): the first page visits slots `[0, OWNED_SCAN_COUNT)`,
 * so a test places a member ahead of or behind the cursor by its slot.
 *
 * **Waiting for a pass** (plan FR-012): `runOnePass` fires the reconcile timer
 * with one `tickAsync` and drains with FakeTime's `runMicrotasks()`, which runs
 * real macrotasks, so a 307-entry sweep (about 310 round trips, all
 * microtasks) finishes inside it. Never a fixed microtask count — that reads
 * a half-finished pass as finished — and `close()` only where `close()` is the
 * subject (W4, W10): it sets `#closing` first and truncates the pass.
 *
 * Red on `1986b2c9` (the driver before #358, with this branch's fake): W1 on
 * the read itself (an `SMEMBERS`, no `SSCAN`), W4, W6 and W7 on the
 * *unfinished* suffix, W2 on a live broker ("RESP reply exceeds"); W1 and W9
 * also failed to compile until `OWNED_SCAN_COUNT`, `decodeScanReply` and
 * `SCAN_REPLY_REFUSED` existed. W3, W5, W8, W10 and W11 are guards, each paired
 * with a mutant in `mutations/sweep_paging_358.ts`.
 *
 * @module @lockness/realtime/tests/sweep_paging_358
 */

import { assert, assertEquals, assertRejects, assertThrows } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    decodeScanReply,
    OWNED_SCAN_COUNT,
    RedisBroadcastDriver,
    SCAN_REPLY_REFUSED,
} from '../drivers/redis.ts'
import type { RosterDeparture } from '../driver.ts'
import { RedisClient } from '../../redis/mod.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
} from '../../redis/tests/live_broker.ts'
import { type CommandFn, FakeRedis, serializedCommands } from './fake_redis.ts'

const START = new Date('2026-09-23T10:00:00Z')
const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string) =>
    `${PREFIX}__holders:${channel} ${id}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const ALIVE_KEY = (instanceId: string) => `${PREFIX}__alive:${instanceId}`
const INSTANCES_KEY = `${PREFIX}__instances`

const DEAD = 'instance-dead'

/** The words of the one line a sweep that removed something logs. */
const RELEASED = 'hold(s) of dead instance'
/** The words of the one line a sweep cut short by a renewal logs. */
const RENEWED = 'renewed its liveness while being swept'
/** The words of the one line a sweep that threw logs. */
const FAILED = 'sweep of dead instance'
/** The suffix of a "released" line whose sweep left work behind. */
const UNFINISHED =
    ' — unfinished: it stays registered and a later pass resumes it'

/** The exact "released" line of a sweep of {@link DEAD}. */
const releasedLine = (n: number, unfinished = false) =>
    `realtime: released ${n} hold(s) of dead instance ${DEAD} ` +
    `(${n} emptied their slot)` + (unfinished ? UNFINISHED : '')

const entry = (field: string, owner: string) =>
    JSON.stringify({ member: { id: Number(field) }, owner })

/** The owned entry of `field` in {@link CHANNEL}. */
const ownedEntry = (field: string) => `${CHANNEL} ${field}`

/** The fake's scan slot of `field`'s owned entry. */
const slotOf = (field: string) => FakeRedis.scanSlot(ownedEntry(field))

/** Fields `'1'` … `'n'`. */
const numbered = (n: number) => Array.from({ length: n }, (_, i) => `${i + 1}`)

/** Numeric order, for comparing sets of fields. */
const sorted = (fields: readonly string[]) =>
    [...fields].sort((a, b) => Number(a) - Number(b))

/**
 * `fields` in the order the fake's scan visits their owned entries — the
 * fake's own ordering, not a restatement of it.
 */
const inScanOrder = (fields: readonly string[]) =>
    [...fields].sort((a, b) =>
        FakeRedis.scanOrder(ownedEntry(a), ownedEntry(b))
    )

/**
 * `count` numeric fields, counting up from `from`, whose owned entry lies in
 * the scan slots `[lo, hi)`.
 */
function fieldsInSlots(
    lo: number,
    hi: number,
    count: number,
    from = 1,
): string[] {
    const out: string[] = []
    for (let i = from; out.length < count; i++) {
        if (i > from + 1_000_000) {
            throw new Error(`no ${count} fields in slots [${lo}, ${hi})`)
        }
        const slot = slotOf(`${i}`)
        if (slot >= lo && slot < hi) out.push(`${i}`)
    }
    return out
}

/** Write `owner`'s hold of `field` exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<void> {
    const value = entry(field, owner)
    await redis.command('HSET', HOLDERS_KEY(CHANNEL, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(CHANNEL), field, value)
    await redis.command('SADD', OWNED_KEY(owner), ownedEntry(field))
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/**
 * Leave the store exactly as `RELEASE_MEMBER_SCRIPT` does after another
 * survivor released `owner`'s sole hold of `field`: its holders entry, the
 * presence field and its owned entry are gone.
 */
async function releasedByAnotherSurvivor(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<void> {
    await redis.command('HDEL', HOLDERS_KEY(CHANNEL, field), owner)
    await redis.command('HDEL', PRESENCE_KEY(CHANNEL), field)
    await redis.command('SREM', OWNED_KEY(owner), ownedEntry(field))
}

async function registered(redis: FakeRedis, id: string): Promise<boolean> {
    const reply = await redis.command('SMEMBERS', INSTANCES_KEY) as {
        value: { value: string }[]
    }
    return reply.value.some((m) => m.value === id)
}

/** The owned set of `owner`, read directly (a test read, not the sweep's). */
async function ownedOf(redis: FakeRedis, owner = DEAD): Promise<string[]> {
    const reply = await redis.command('SMEMBERS', OWNED_KEY(owner)) as {
        value: { value: string }[]
    }
    return reply.value.map((m) => m.value)
}

/** A sweep release of one of `owner`'s holds (it names a holders hash). */
const isRelease = (args: readonly string[], owner = DEAD) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(owner)) &&
    args.some((a) => a.startsWith(`${PREFIX}__holders:`))

/** The deregistration of `owner` (it names the instances set). */
const isDeregistration = (args: readonly string[], owner = DEAD) =>
    args[0] === 'EVAL' && args.includes(INSTANCES_KEY) &&
    args.includes(ALIVE_KEY(owner))

/** A page read of `owner`'s owned set. */
const isPageRead = (args: readonly string[], owner = DEAD) =>
    args[0] === 'SSCAN' && args[1] === OWNED_KEY(owner)

/** A page read of `owner`'s owned set past the first page. */
const isLaterPageRead = (args: readonly string[], owner = DEAD) =>
    isPageRead(args, owner) && args[2] !== '0'

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

/**
 * A command port serialized as the production client is, recording every
 * command issued through it, in issue order.
 */
function serialPort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const sent: string[][] = []
    const command: CommandFn = (...args) => {
        sent.push(args)
        return serial.command(...args)
    }
    return { serial, sent, command }
}

/**
 * A survivor over `command`, recording each member of {@link CHANNEL} its
 * sweep announces as left.
 */
function sweeper(redis: FakeRedis, command: CommandFn) {
    const b = new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
    const departures: string[] = []
    b.onRosterDeparture(({ channel, member }: RosterDeparture) => {
        if (channel === CHANNEL) departures.push(String(member.id))
    })
    return { b, departures }
}

/**
 * Fire the reconcile timer once and wait for the pass it starts (FR-012):
 * `runMicrotasks()` runs a real macrotask, so the whole microtask chain of
 * the pass — however many round trips — has drained when it resolves, unless
 * a gate holds a reply.
 */
async function runOnePass(time: FakeTime): Promise<void> {
    await time.tickAsync(1_000)
    await time.runMicrotasks()
}

// --- US1 + US2: one pass over any size, no reply over a page ------------------

Deno.test('#358 W1 a 307-slot dead instance is swept in ONE pass through bounded SSCAN pages: every slot gone, one left each, deregistered, no SMEMBERS of its owned set', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        const fields = numbered(307)
        for (const field of fields) await plantHold(redis, field)
        // B holds something, so its reconcile pass runs.
        await b.holdMember(OTHER, { id: 9 })
        await runOnePass(time)

        assertEquals(
            sent.filter((a) => a[0] === 'SMEMBERS' && a[1] === INSTANCES_KEY)
                .length,
            1,
            'one pass',
        )
        assertEquals(sorted(departures), fields, 'one left per slot')
        const roster = await b.readRoster(CHANNEL, 1_000, [])
        assertEquals(roster.total, 0, 'every slot is gone from the roster')
        assertEquals(roster.members, [])
        assertEquals(await registered(redis, DEAD), false, 'deregistered')
        assertEquals(
            sent.filter((a) => a[0] === 'SMEMBERS' && a[1] === OWNED_KEY(DEAD)),
            [],
            'no whole-set read of the owned set',
        )
        const pages = sent.filter((a) => isPageRead(a))
        assert(
            pages.length >= 4,
            `307 entries take at least 4 pages of ${OWNED_SCAN_COUNT}, ` +
                `saw ${pages.length}`,
        )
        for (const page of pages) {
            assertEquals(
                page.slice(3),
                ['COUNT', String(OWNED_SCAN_COUNT)],
                'every page is bounded by OWNED_SCAN_COUNT, and nothing else',
            )
        }
        assertEquals(warnings.having(RELEASED), [releasedLine(307)])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

/** The dead instance of W2, whose owned set is seeded past the reply cap. */
const W2_DEAD = 'instance-w2-huge'

/**
 * Owned entries seeded for W2. Each is a 200-byte channel, a space and a
 * 600-byte member id — 801 bytes, 809 on the wire as a bulk element — so
 * 42,000 of them are ≈ 34.0 MB of `SMEMBERS` reply, past the 32 MiB
 * (33,554,432-byte) `MAX_REPLY_BYTES`. The test proves the crossing itself
 * rather than restating the cap: a whole-set read on a throwaway client is
 * refused before the sweep starts.
 */
const W2_ENTRIES = 42_000

/**
 * How often W2's watchdog samples the owned set's size. The sweep is ≈ 42,000
 * release `EVAL`s plus 420 pages; sampling every 250 ms adds a few hundred
 * `SCARD`s, where a 5 ms poll would add one round trip per ≈ 2 releases.
 */
const W2_POLL_MS = 250

/**
 * How long the owned set may stay the same size before W2 calls the sweep
 * stalled. A working sweep removes one entry per release — hundreds a second
 * on a local broker — so 30 s without a single removal is a sweep that stopped,
 * not a host under load. It also covers the wait for the first pass (armed
 * every 100 ms) and the one deregistration after the last release.
 */
const W2_STALL_MS = 30_000

/**
 * W2's overall ceiling, a backstop to the stall watchdog rather than a
 * deadline: ≈ 42,000 releases at 0.5 ms a round trip is ≈ 21 s, so 10 min
 * fails only a sweep that keeps progressing at under ~70 releases a second.
 */
const W2_CEILING_MS = 600_000

/** Seed `KEYS[1]` with `ARGV[1]` maximal owned entries and register `ARGV[2]`. */
const SEED_OWNED_SCRIPT = [
    "local channel = 'presence-' .. string.rep('c', 191)",
    "local tail = string.rep('€', 198)",
    'for i = 1, tonumber(ARGV[1]) do',
    "  redis.call('SADD', KEYS[1], channel .. ' ' .. string.format('%06d', i) .. tail)",
    'end',
    "redis.call('SADD', KEYS[2], ARGV[2])",
    "return redis.call('SCARD', KEYS[1])",
].join('\n')

Deno.test({
    name:
        '#358 W2 an owned set past the reply cap is swept and deregistered on a real broker, and the survivor’s client keeps working',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const ns = runNamespace()
        const prefix = `${ns}:rt`
        const owned = `${prefix}__owned:${W2_DEAD}`
        const instances = `${prefix}__instances`
        const live = new RedisClient(config)
        const reader = new RedisClient(config)
        const warnings = captureWarnings()
        let b: RedisBroadcastDriver | undefined
        try {
            assertEquals(
                await live.command(
                    'EVAL',
                    SEED_OWNED_SCRIPT,
                    '2',
                    owned,
                    instances,
                    String(W2_ENTRIES),
                    W2_DEAD,
                ),
                { type: 'integer', value: W2_ENTRIES },
            )
            // The seed really crosses the cap: the read #358 replaces is
            // refused. On a throwaway client, whose socket the refusal costs.
            const probe = new RedisClient(config)
            try {
                await assertRejects(
                    () => probe.command('SMEMBERS', owned),
                    Error,
                    'RESP reply exceeds',
                )
            } finally {
                await probe.close()
            }

            b = new RedisBroadcastDriver(live, { psubscribe: () => {} }, {
                prefix,
                presence: {
                    livenessTtlSeconds: 30,
                    heartbeatIntervalMs: 1_000,
                    reconcileIntervalMs: 100,
                },
            })
            await b.holdMember(OTHER, { id: 9 })
            const isRegistered = async () => {
                const reply = await reader.command(
                    'SISMEMBER',
                    instances,
                    W2_DEAD,
                )
                return reply.type === 'integer' && reply.value === 1
            }
            // A progress watchdog, not a wall-clock deadline: under host load
            // the sweep slows, and only a sweep that STOPS removing entries
            // fails. A "failed" line ends the wait early.
            const size = async () => {
                const reply = await reader.command('SCARD', owned)
                return reply.type === 'integer' ? reply.value : NaN
            }
            const start = Date.now()
            let smallest = await size()
            let progressAt = start
            while (
                warnings.having(FAILED).length === 0 && await isRegistered()
            ) {
                const now = Date.now()
                if (now - progressAt > W2_STALL_MS) {
                    throw new Error(
                        `the sweep of ${W2_DEAD} stalled: its owned set stayed ` +
                            `at ${smallest} entries for ${W2_STALL_MS} ms`,
                    )
                }
                if (now - start > W2_CEILING_MS) {
                    throw new Error(
                        `the sweep of ${W2_DEAD} passed the ` +
                            `${W2_CEILING_MS} ms ceiling at ${smallest} entries`,
                    )
                }
                await new Promise((resolve) => setTimeout(resolve, W2_POLL_MS))
                const current = await size()
                if (current < smallest) {
                    smallest = current
                    progressAt = Date.now()
                }
            }

            assertEquals(warnings.having(FAILED), [], 'no "failed" line')
            assertEquals(await isRegistered(), false, 'deregistered')
            assertEquals(
                await reader.command('EXISTS', owned),
                { type: 'integer', value: 0 },
                'the owned set is gone',
            )
            // The survivor's own client never discarded its socket: a command
            // right after the sweep meets no refusal window.
            assertEquals(await live.command('PING'), {
                type: 'simple',
                value: 'PONG',
            })
        } finally {
            warnings.restore()
            await b?.close()
            await teardown(reader, ns)
            await reader.close()
            await live.close()
        }
    },
})

Deno.test('#358 W9 decodeScanReply accepts only [canonical cursor, array] and refuses everything else with one constant message that carries nothing of the reply', () => {
    const MARKER = 'ada@ex.com'
    const bulk = (value: string) => ({ type: 'bulk', value })
    const array = (...value: unknown[]) => ({ type: 'array', value })

    assertEquals(decodeScanReply(array(bulk('0'), array())), {
        cursor: '0',
        items: [],
    })
    const twentyDigits = '1' + '8'.repeat(19)
    assertEquals(
        decodeScanReply(
            array(bulk(twentyDigits), array(bulk('a 1'), bulk('b'))),
        ),
        { cursor: twentyDigits, items: [bulk('a 1'), bulk('b')] },
        'the cursor stays a string, past 2^53; the items stay raw replies',
    )

    const refused: [string, unknown][] = [
        ['nil', { type: 'nil' }],
        ['null', null],
        ['a bulk, not an array', bulk('0')],
        ['one element', array(bulk('0'))],
        ['three elements', array(bulk('0'), array(), bulk(MARKER))],
        ['a missing cursor', array(array(), array())],
        ['an integer cursor', array({ type: 'integer', value: 0 }, array())],
        ['an empty cursor', array(bulk(''), array())],
        ['a non-digit cursor', array(bulk('abc'), array())],
        ["the cursor '00'", array(bulk('00'), array())],
        ["the cursor '01'", array(bulk('01'), array())],
        ['a 21-digit cursor', array(bulk('1'.repeat(21)), array())],
        ['a marker cursor', array(bulk(`x${MARKER}`), array())],
        ['items that are not an array', array(bulk('0'), bulk(MARKER))],
        ['nil items', array(bulk('0'), { type: 'nil' })],
    ]
    for (const [what, reply] of refused) {
        const error = assertThrows(
            () => decodeScanReply(reply),
            Error,
            undefined,
            what,
        )
        assertEquals(error.message, SCAN_REPLY_REFUSED, what)
        assert(!error.message.includes(MARKER), `${what}: no reply bytes`)
    }
    assert(
        !/SSCAN|ZSCAN|owned/i.test(SCAN_REPLY_REFUSED),
        'the message names no command and no key',
    )
})

Deno.test('#358 W8 a page emptied by another survivor, with a non-zero cursor, does not end the scan', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial, sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    try {
        const C = OWNED_SCAN_COUNT
        const first = fieldsInSlots(0, C, 40)
        const second = fieldsInSlots(C, 2 * C, 40)
        const rest = fieldsInSlots(2 * C, Infinity, 40)
        for (const field of [...first, ...second, ...rest]) {
            await plantHold(redis, field)
        }
        await b.holdMember(OTHER, { id: 9 })
        // B has read its first page; another survivor releases every member
        // of the second before B reads it.
        const firstPage = serial.hold((a) => isPageRead(a) && a[2] === '0')
        await time.tickAsync(1_000)
        await firstPage.reached
        for (const field of second) {
            await releasedByAnotherSurvivor(redis, field)
        }
        firstPage.release()
        await time.runMicrotasks()

        const cursors = sent.filter((a) => isPageRead(a)).map((a) => a[2])
        assert(
            cursors.includes(String(C)),
            `precondition: B read the emptied page (cursors ${cursors})`,
        )
        assert(
            cursors.includes(String(2 * C)),
            `B went past the empty page (cursors ${cursors})`,
        )
        assertEquals(sorted(departures), sorted([...first, ...rest]))
        assertEquals(await ownedOf(redis), [])
        assertEquals(await registered(redis, DEAD), false)
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#358 W5 a page read that fails after the first page: one "failed" line with that page’s N, still registered, and the next pass finishes', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial } = serialPort(redis)
    let pageReads = 0
    const flaky: CommandFn = (...args) => {
        if (isPageRead(args) && ++pageReads === 2) {
            return Promise.reject(new Error('connection reset'))
        }
        return serial.command(...args)
    }
    const { b, departures } = sweeper(redis, flaky)
    const warnings = captureWarnings()
    try {
        const fields = numbered(307)
        for (const field of fields) await plantHold(redis, field)
        const firstPage = fields.filter((f) => slotOf(f) < OWNED_SCAN_COUNT)
        assert(
            firstPage.length > 0,
            'precondition: the first page is not empty',
        )
        await b.holdMember(OTHER, { id: 9 })
        await runOnePass(time)

        const failed = warnings.having(FAILED)
        assertEquals(failed.length, 1, failed.join('\n'))
        const n = firstPage.length
        assert(
            failed[0].startsWith(
                `realtime: sweep of dead instance ${DEAD} failed after ${n} ` +
                    `hold(s) released (${n} emptied): `,
            ),
            failed[0],
        )
        assert(failed[0].includes('connection reset'), failed[0])
        assertEquals(sorted(departures), sorted(firstPage))
        assertEquals(await registered(redis, DEAD), true, 'retried next pass')

        await runOnePass(time)
        assertEquals(sorted(departures), fields, 'each member announced once')
        assertEquals(await registered(redis, DEAD), false)
        assertEquals(warnings.having(RELEASED), [releasedLine(307 - n)])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US3: shutting down mid-scan leaves the rest to the fleet -----------------

Deno.test('#358 W4 close() after the first page: no release after the one in flight, the rest stays owned and registered, no command after close() resolves, the line says unfinished — and the next survivor finishes, each member announced once', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const portB = serialPort(redis)
    const B = sweeper(redis, portB.command)
    let C: ReturnType<typeof sweeper> | undefined
    const warnings = captureWarnings()
    try {
        const fields = numbered(307)
        for (const field of fields) await plantHold(redis, field)
        await B.b.holdMember(OTHER, { id: 9 })
        // close() while the release of the SECOND page's first member is in
        // flight: its departure is still announced, and nothing follows it.
        const [opening] = inScanOrder(
            fields.filter((f) => slotOf(f) >= OWNED_SCAN_COUNT),
        )
        const inFlight = portB.serial.hold((a) =>
            isRelease(a) && a.includes(HOLDERS_KEY(CHANNEL, opening))
        )
        await time.tickAsync(1_000)
        await inFlight.reached
        const closeBegan = portB.sent.length
        const closing = B.b.close()
        inFlight.release()
        await closing
        const resolvedAt = portB.sent.length

        // The check before each release: the one in flight completes, and
        // no release of the rest of its page follows it.
        assertEquals(
            portB.sent.slice(closeBegan).filter((a) => isRelease(a)),
            [],
            'no release after close() began but the one in flight',
        )

        const n = B.departures.length
        assert(
            n > 1 && n < fields.length,
            `precondition: B released its first page and one more (${n})`,
        )
        assertEquals(await registered(redis, DEAD), true, 'still registered')
        assertEquals(
            (await ownedOf(redis)).length,
            fields.length - n,
            'the rest is still owned',
        )
        assertEquals(warnings.having(RELEASED), [releasedLine(n, true)])

        await time.tickAsync(1_000)
        await time.runMicrotasks()
        assertEquals(
            portB.sent.slice(resolvedAt),
            [],
            'no command once close() resolved',
        )

        const portC = serialPort(redis)
        C = sweeper(redis, portC.command)
        await C.b.holdMember(OTHER, { id: 10 })
        await runOnePass(time)
        assertEquals(
            sorted([...B.departures, ...C.departures]),
            fields,
            'each member announced once overall',
        )
        assertEquals(await registered(redis, DEAD), false)
        assertEquals(
            warnings.having(RELEASED).filter((l) => l.includes(DEAD)),
            [releasedLine(n, true), releasedLine(fields.length - n)],
        )
    } finally {
        warnings.restore()
        await B.b.close()
        await C?.b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#358 W10 close() during a run of pages that release nothing: no page read after close() begins but the one in flight', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial, sent, command } = serialPort(redis)
    const { b } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        // Only unparsable entries: every page yields no release, so the
        // per-release check never runs — only the check before a page read
        // can stop this scan.
        for (let i = 0; i < 150; i++) {
            await redis.command('SADD', OWNED_KEY(DEAD), `garbage-${i}`)
        }
        await redis.command('SADD', INSTANCES_KEY, DEAD)
        await b.holdMember(OTHER, { id: 9 })
        const secondPage = serial.hold((a) => isLaterPageRead(a))
        await time.tickAsync(1_000)
        await secondPage.reached
        const closeBegan = sent.length
        const closing = b.close()
        secondPage.release()
        await closing

        assertEquals(
            sent.slice(closeBegan).filter((a) => isPageRead(a)),
            [],
            'no SSCAN after close() began',
        )
        assertEquals(
            sent.filter((a) => isDeregistration(a)),
            [],
            'no deregistration either',
        )
        assertEquals(await registered(redis, DEAD), true)
        assertEquals(warnings.having(RELEASED), [], 'N = 0 logs nothing')
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#358 W6 (i) a hold of the still-lapsed instance landing AHEAD of the cursor mid-scan is released in this pass', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        const fields = numbered(150)
        for (const field of fields) await plantHold(redis, field)
        const [late] = fieldsInSlots(OWNED_SCAN_COUNT, Infinity, 1, 1_000)
        await b.holdMember(OTHER, { id: 9 })
        // The first page is read and its first release is in flight when
        // the late hold lands, ahead of the cursor.
        const inFlight = serial.hold((a) => isRelease(a))
        await time.tickAsync(1_000)
        await inFlight.reached
        await plantHold(redis, late)
        inFlight.release()
        await time.runMicrotasks()

        assertEquals(sorted(departures), sorted([...fields, late]))
        assertEquals(await registered(redis, DEAD), false, 'deregistered')
        assertEquals(warnings.having(RELEASED), [releasedLine(151)])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#358 W6 (ii) a hold landing BEHIND the cursor mid-scan is missed: the deregistration keeps the instance, the line says unfinished, and the next pass releases it', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        const fields = numbered(150)
        for (const field of fields) await plantHold(redis, field)
        const [late] = fieldsInSlots(0, OWNED_SCAN_COUNT, 1, 1_000)
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((a) => isRelease(a))
        await time.tickAsync(1_000)
        await inFlight.reached
        await plantHold(redis, late)
        inFlight.release()
        await time.runMicrotasks()

        assertEquals(sorted(departures), fields, 'the late hold was missed')
        assertEquals(await ownedOf(redis), [ownedEntry(late)])
        assertEquals(await registered(redis, DEAD), true, 'kept, not orphaned')
        assertEquals(warnings.having(RELEASED), [releasedLine(150, true)])

        await runOnePass(time)
        assertEquals(sorted(departures), sorted([...fields, late]))
        assertEquals(await registered(redis, DEAD), false)
        assertEquals(warnings.having(RELEASED), [
            releasedLine(150, true),
            releasedLine(1),
        ])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#358 W7 150 unparsable entries ahead of 5 parsable ones: one pass releases all 5 and says unfinished; the next pass logs nothing and the instance stays registered', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        // The 5 parsable entries sit at the END of the scan order, and 150
        // unparsable ones — never removed (ADR 006 §5) — before them.
        const parsable = inScanOrder(numbered(2_000)).slice(-5)
        const floor = Math.min(...parsable.map(slotOf))
        const unparsable: string[] = []
        for (let i = 0; unparsable.length < 150; i++) {
            const garbage = `garbage-${i}`
            if (FakeRedis.scanSlot(garbage) < floor) unparsable.push(garbage)
        }
        for (const field of parsable) await plantHold(redis, field)
        await redis.command('SADD', OWNED_KEY(DEAD), ...unparsable)
        await b.holdMember(OTHER, { id: 9 })
        await runOnePass(time)

        assertEquals(sorted(departures), sorted(parsable), 'all 5 released')
        assertEquals(warnings.having(RELEASED), [releasedLine(5, true)])
        assertEquals(await registered(redis, DEAD), true)

        await runOnePass(time)
        assertEquals(warnings.having(RELEASED).length, 1, 'N = 0 is silent')
        assertEquals(await registered(redis, DEAD), true)
        assertEquals(sorted(departures), sorted(parsable))
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US4: a renewal mid-scan stops the scan -----------------------------------

Deno.test('#358 W11 a renewal after the first page: no release after the refused one, no further page read, no deregistration, one "renewed" line', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const { serial, sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        const fields = numbered(307)
        for (const field of fields) await plantHold(redis, field)
        const firstPage = fields.filter((f) => slotOf(f) < OWNED_SCAN_COUNT)
        assert(
            fields.some((f) =>
                slotOf(f) >= OWNED_SCAN_COUNT &&
                slotOf(f) < 2 * OWNED_SCAN_COUNT
            ),
            'precondition: the second page is not empty',
        )
        await b.holdMember(OTHER, { id: 9 })
        const secondPage = serial.hold((a) => isLaterPageRead(a))
        await time.tickAsync(1_000)
        await secondPage.reached
        await redis.command('SET', ALIVE_KEY(DEAD), '1', 'EX', '30')
        secondPage.release()
        await time.runMicrotasks()

        const after = sent.slice(sent.findIndex((a) => isLaterPageRead(a)) + 1)
        assertEquals(
            after.filter((a) => isRelease(a)).length,
            1,
            'the refused release is the last one',
        )
        assertEquals(after.filter((a) => isPageRead(a)), [], 'no page after')
        assertEquals(sent.filter((a) => isDeregistration(a)), [])
        assertEquals(sorted(departures), sorted(firstPage))
        const n = firstPage.length
        assertEquals(warnings.having(RENEWED), [
            `realtime: instance ${DEAD} renewed its liveness while being ` +
            `swept — a lapse, not a crash; ${n} hold(s) released (${n} ` +
            'emptied) before it did',
        ])
        assertEquals(await registered(redis, DEAD), true)
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US5: two survivors sweep the same instance -------------------------------

Deno.test('#358 W3 two survivors page through the same dead instance at once: one left per member overall, and N_B + N_C = 307', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const order: string[] = []
    const portFor = (who: string) => {
        const serial = serializedCommands(redis.command)
        const command: CommandFn = (...args) => {
            if (isPageRead(args)) order.push(who)
            return serial.command(...args)
        }
        return command
    }
    const B = sweeper(redis, portFor('B'))
    const C = sweeper(redis, portFor('C'))
    const warnings = captureWarnings()
    try {
        const fields = numbered(307)
        for (const field of fields) await plantHold(redis, field)
        await B.b.holdMember(OTHER, { id: 9 })
        await C.b.holdMember(OTHER, { id: 10 })
        await runOnePass(time)

        const firstB = order.indexOf('B')
        const lastB = order.lastIndexOf('B')
        assert(
            order.slice(firstB, lastB).includes('C'),
            `precondition: the page reads interleaved (${order.join('')})`,
        )
        assertEquals(
            sorted([...B.departures, ...C.departures]),
            fields,
            'one left per member across both',
        )
        const removed = warnings.having(RELEASED)
            .filter((l) => l.includes(DEAD))
            .map((l) => Number(/released (\d+) hold/.exec(l)?.[1]))
        assertEquals(
            removed.reduce((sum, n) => sum + n, 0),
            307,
            `N_B + N_C (${removed.join(' + ')})`,
        )
        assertEquals(await registered(redis, DEAD), false)
    } finally {
        warnings.restore()
        await B.b.close()
        await C.b.close()
        time.restore()
        redis.assertNoRejections()
    }
})
