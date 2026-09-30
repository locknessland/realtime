/**
 * @fileoverview #348 — a crashed instance's swept presence members are
 * announced as left.
 *
 * When a Redis-backed instance crashes, a surviving instance's reconcile pass
 * releases the dead instance's roster holds. Before #348 the sweep threw the
 * release script's answer away, so the roster became correct while nobody told
 * the room: a client that builds its member list from `joined` / `left` kept a
 * ghost for every member only the dead instance held.
 *
 * The disposition: the release script replies with the released holder's
 * entry when it empties the slot, the sweep hands that entry to the driver's
 * one departure handler (`onRosterDeparture`), and the manager announces it
 * through the one announcement home, exactly as a leave.
 *
 * W1, W3 and W5 were red before the fix (the sweep announced nothing), as was
 * the `readRoster` S2 row (a V8 `SyntaxError` quotes the entry's bytes). W2 and
 * W6 were green before and after. W4, W7, W8, the A6 handler-lifecycle row and
 * the manager-side drop pin the seam's contract, so they could not run before
 * the seam existed; each is paired with a mutant in
 * `mutations/presence_sweep_departure_348.ts`.
 *
 * Every witness runs real `RedisBroadcastDriver`s over one `FakeRedis`, and the
 * ghost sweep is driven by `FakeTime`, as `roster_holders_345.test.ts` does.
 *
 * @module @lockness/realtime/tests/presence_sweep_departure_348
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import type {
    BroadcastDriver,
    ControlMessage,
    RosterDeparture,
} from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    type CommandFn,
    FakeRedis,
    type SerializedCommands,
    serializedCommands,
} from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const CONTROL_TOPIC = `${PREFIX}__control`
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string | number) =>
    `${PREFIX}__holders:${channel} ${String(id)}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const INSTANCES_KEY = `${PREFIX}__instances`

/**
 * Bytes that must never reach a log line (S2). Short on purpose: V8 quotes a
 * short unparseable input WHOLE in its `SyntaxError` message, which is exactly
 * the leak the fixed-reason WARN closes.
 */
const SENTINEL = 'ada@ex.com'

interface User {
    id: number
    name: string
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

function conn(id: string, userId: number, name = `user-${userId}`): Recording {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId, name },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Recording
}

const authorize = (user: User | null): PresenceMember | false =>
    user ? { id: user.id, info: { name: user.name } } : false

/** The `presence` actions a connection received for member `id`, in order. */
const actions = (c: Recording, id: number) =>
    c.received
        .filter((f) =>
            f.type === 'presence' && f.channel === CHANNEL &&
            (f.member as PresenceMember | undefined)?.id === id
        )
        .map((f) => f.action)

/** The `presence` frames of `action` a connection received for member `id`. */
const frames = (c: Recording, action: string, id: number) =>
    c.received.filter((f) =>
        f.type === 'presence' && f.channel === CHANNEL &&
        f.action === action &&
        (f.member as PresenceMember | undefined)?.id === id
    )

/** Run the microtask queue out. */
async function settle(times = 100): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

/** Advance past a stopped driver's liveness TTL so its live peers sweep it. */
async function lapse(time: FakeTime): Promise<void> {
    await time.tickAsync(3_500)
    await settle()
}

/** A control publish, and which instance asked for it. */
type Published = ControlMessage & { readonly from: string }

/**
 * One instance: a Redis driver over the shared fake, its manager, and every
 * control frame it publishes recorded in `log`.
 *
 * @param options.prepare - Runs on the driver BEFORE the manager is built, so
 *   a test can shadow or spy on the seam the manager registers with.
 */
function instance(
    redis: FakeRedis,
    tag: string,
    log: Published[],
    options: {
        command?: CommandFn
        prepare?: (driver: BroadcastDriver) => void
    } = {},
) {
    const driver = redisDriver(redis, options.command)
    const publish = driver.publishControl.bind(driver)
    driver.publishControl = (control) => {
        log.push({ ...control, from: tag })
        return publish(control)
    }
    options.prepare?.(driver)
    const manager = new ChannelManager<User>({ driver, authorize })
    return { driver, manager, id: idOf(driver) }
}

function redisDriver(redis: FakeRedis, command: CommandFn = redis.command) {
    return new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            control: { secret: 'deployment-secret-with-enough-entropy' },
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
}

/** The instance id a driver tags its holds with. Test-only read of a private. */
const idOf = (d: RedisBroadcastDriver): string => d['instanceId']

/** The presence control frames actually PUBLISHed on the bus, in bus order. */
function busPresence(redis: FakeRedis) {
    return redis.commandLog()
        .filter(([cmd, topic]) => cmd === 'PUBLISH' && topic === CONTROL_TOPIC)
        .map(([, , payload]) =>
            JSON.parse(payload) as {
                kind: string
                target: string
                member?: PresenceMember
            }
        )
        .filter((wire) => wire.kind.startsWith('presence-'))
}

/**
 * Wrap the driver's departure registration so every departure it reports is
 * recorded before the registered handler runs.
 */
function spyDepartures(calls: RosterDeparture[]) {
    return (driver: BroadcastDriver) => {
        const register = driver.onRosterDeparture?.bind(driver)
        driver.onRosterDeparture = (handler) =>
            register?.((departure) => {
                calls.push(departure)
                return handler(departure)
            })
    }
}

/** Collect every `console.warn` line until `restore()`. */
function captureWarnings() {
    const warn = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => void lines.push(parts.join(' '))
    return {
        lines,
        restore: () => void (console.warn = warn),
    }
}

/**
 * A command port that records every command it forwards to `redis`, so one
 * instance's traffic can be told apart from its peers'.
 */
function recording(redis: FakeRedis) {
    const sent: string[][] = []
    const command: CommandFn = (...args) => {
        sent.push(args)
        return redis.command(...args)
    }
    return { sent, command }
}

/** A WARN other than the sweep's own per-instance summary line. */
const notSummary = (line: string) => !line.includes('hold(s) of dead instance')

// --- US1 / US2 / US3: the crash is announced exactly once ---------------------

Deno.test('#348 W1 7 held only on A, A crashes, B sweeps — the observer on B receives exactly one left', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log)
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        assertEquals(actions(observer, 7), ['joined'], 'precondition')

        // A crashes: its timers stop, its holds stay behind.
        await a.driver.close()
        await lapse(time)

        const left = frames(observer, 'left', 7)
        assertEquals(left.length, 1, 'exactly one left for the swept member')
        assertEquals(
            left[0].member,
            { id: 7, info: { name: 'Ada' } },
            "the left carries A's last stored entry",
        )
        const leaves = log.filter((c) => c.kind === 'presence-leave')
        assertEquals(
            leaves.map((c) => [c.from, c.target, c.channel, c.member?.id]),
            [['B', CHANNEL, CHANNEL, 7]],
            'the sweeper publishes exactly one presence-leave, its target the ' +
                'channel name (no connection announced it)',
        )
        assertEquals(
            (await b.driver.readRoster(CHANNEL, 100, [])).members.map((m) =>
                m.id
            ),
            [1],
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#348 W2 7 held on A and B, A is swept — no left anywhere', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log)
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const b7 = conn('b7', 7, 'Ada')
        b.manager.register(b7)
        await b.manager.subscribe(b7, CHANNEL)
        await settle()
        const holderOf = (id: string) =>
            redis.command('HGET', HOLDERS_KEY(CHANNEL, 7), id)
        assert(
            (await holderOf(a.id) as { type: string }).type === 'bulk',
            'precondition: A holds 7',
        )

        await a.driver.close()
        await lapse(time)

        // The sweep RAN — otherwise "no left" below is silence, not a verdict.
        assertEquals(
            await holderOf(a.id),
            { type: 'nil' },
            "the sweep released A's hold of 7",
        )
        assert(
            (await holderOf(b.id) as { type: string }).type === 'bulk',
            "B's hold of 7 survives the sweep",
        )
        const instances = await redis.command('SMEMBERS', INSTANCES_KEY) as {
            value: { value: string }[]
        }
        assert(
            !instances.value.some((m) => m.value === a.id),
            'and forgot A: it left the instances set',
        )
        assertEquals(
            actions(observer, 7),
            ['joined'],
            'B still holds 7, so sweeping A is not a departure',
        )
        assertEquals(log.filter((c) => c.kind === 'presence-leave'), [])
        assertEquals(
            (await b.driver.readRoster(CHANNEL, 100, [])).members
                .map((m) => m.id)
                .sort(),
            [1, 7],
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#348 W3 B and C both sweep A — each observer receives exactly one left', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    // Each survivor's commands on a wire of its own, so "both swept" is
    // checked per sweeper rather than inferred from a shared log.
    const sentByB = recording(redis)
    const sentByC = recording(redis)
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log, { command: sentByB.command })
    const c = instance(redis, 'C', log, { command: sentByC.command })
    try {
        const onB = conn('b-observer', 1)
        b.manager.register(onB)
        const onC = conn('c-observer', 2)
        c.manager.register(onC)
        await b.manager.subscribe(onB, CHANNEL)
        await c.manager.subscribe(onC, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()

        await a.driver.close()
        await lapse(time)

        // A release of A's hold of 7 names A's owned set and 7's holders; no
        // other script either survivor runs names A's owned set.
        const releasesOfA7 = (sent: readonly string[][]) =>
            sent.filter((args) =>
                args[0] === 'EVAL' && args.includes(OWNED_KEY(a.id)) &&
                args.includes(HOLDERS_KEY(CHANNEL, 7))
            ).length
        for (const [who, sent] of [['B', sentByB], ['C', sentByC]] as const) {
            assert(
                releasesOfA7(sent.sent) >= 1,
                `precondition: ${who} sent the release of A's hold of 7 — ` +
                    'otherwise this witnesses one sweep, not two',
            )
        }
        assertEquals(frames(onB, 'left', 7).length, 1, 'observer on B')
        assertEquals(frames(onC, 'left', 7).length, 1, 'observer on C')
        assertEquals(
            log.filter((m) => m.kind === 'presence-leave').length,
            1,
            'the script hands the entry to ONE sweeper: exactly one announcement',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        await c.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W4: the seam is optional ------------------------------------------------

Deno.test('#348 W4 a driver without onRosterDeparture still builds, and its sweep stays silent', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log, {
        prepare: (driver) => void (driver.onRosterDeparture = undefined),
    })
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()

        await a.driver.close()
        await lapse(time)

        assertEquals(actions(observer, 7), ['joined'])
        assertEquals(log.filter((m) => m.kind === 'presence-leave'), [])
        assertEquals(
            (await b.driver.readRoster(CHANNEL, 100, [])).members.map((m) =>
                m.id
            ),
            [1],
            'the sweep itself still ran',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#348 W4 a manager registers for departures only when its driver has a roster', () => {
    let rosterless = 0
    new ChannelManager<User>({
        driver: {
            publish: () => {},
            onMessage: () => {},
            onRosterDeparture: () => void rosterless++,
        },
        authorize,
    })
    assertEquals(rosterless, 0, 'no roster, nothing it could announce from')

    let withRoster = 0
    new ChannelManager<User>({
        driver: {
            publish: () => {},
            onMessage: () => {},
            holdMember: () => ({ arrived: true }),
            releaseMember: () => ({ gone: true }),
            readRoster: (_channel, limit, selfIds) =>
                asWindow([], limit, selfIds),
            onRosterDeparture: () => void withRoster++,
        },
        authorize,
    })
    assertEquals(withRoster, 1, 'the spy is live: a roster driver registers')
})

// --- US4: a lapsed-but-alive instance -----------------------------------------

Deno.test('#348 W5 A lapses while alive — one left, then nothing on its release, then one joined on its next hold', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log)
    const fromA = () =>
        log.filter((m) => m.from === 'A' && m.member?.id === 7).map((m) =>
            m.kind
        )
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()

        // A stops beating but its process, its manager and its sockets stay
        // up — a lapsed liveness key, not a crash. `close()` also stops A's
        // lapse re-assert (#349), so nothing puts 7 back here. a7 itself hears
        // neither the swept `left` nor the `joined` below: no connection
        // receives a frame about its own member id (#349).
        await a.driver.close()
        await lapse(time)
        assertEquals(
            actions(observer, 7),
            ['joined', 'left'],
            'the sweep announces the departure',
        )

        assertEquals(await a.manager.unsubscribe('a7', CHANNEL), 'left')
        await settle()
        assertEquals(
            actions(observer, 7),
            ['joined', 'left'],
            "A's release finds its hold already swept: nothing to announce",
        )
        assertEquals(fromA(), ['presence-join'])

        const a7Again = conn('a7-again', 7, 'Ada')
        a.manager.register(a7Again)
        await a.manager.subscribe(a7Again, CHANNEL)
        await settle()
        assertEquals(
            actions(observer, 7),
            ['joined', 'left', 'joined'],
            "A's next hold is an arrival, and its joined follows a real left",
        )
        assertEquals(fromA(), ['presence-join', 'presence-join'])
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W6: an ordinary leave never reaches the departure handler -----------------

Deno.test('#348 W6 an ordinary leave announces one left and never calls the departure handler', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    const departures: RosterDeparture[] = []
    const a = instance(redis, 'A', log, {
        prepare: spyDepartures(departures),
    })
    const b = instance(redis, 'B', log, {
        prepare: spyDepartures(departures),
    })
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()

        assertEquals(await a.manager.unsubscribe('a7', CHANNEL), 'left')
        await settle()

        assertEquals(actions(observer, 7), ['joined', 'left'])
        assertEquals(
            log.filter((m) => m.kind === 'presence-leave').map((m) => [
                m.from,
                m.target,
            ]),
            [['A', 'a7']],
            'the leave is announced by its own queued write, as the connection',
        )
        assertEquals(
            departures,
            [],
            'releaseMember never reports a departure — a second left',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W7: what the sweep drops, and what it never logs --------------------------

const DEAD = 'instance-dead'

/** Write a dead instance's hold exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(
    redis: FakeRedis,
    channel: string,
    field: string,
    value: string,
    owner = DEAD,
): Promise<void> {
    await redis.command('HSET', HOLDERS_KEY(channel, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(channel), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${channel} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

const entry = (id: string | number, info?: Record<string, unknown>) =>
    JSON.stringify({ member: info ? { id, info } : { id }, owner: DEAD })

interface DropCase {
    readonly label: string
    readonly channel: string
    readonly field: string
    readonly value: string
    readonly throwFor?: string | number
    /** What the one WARN must say happened to the departure. */
    readonly says: string
}

/** The drop WARN's words for a departure the sweep never reported. */
const NOT_ANNOUNCED = 'was not announced as left'

const DROP_CASES: readonly DropCase[] = [
    {
        label: 'a throwing handler',
        channel: CHANNEL,
        // The sentinel IS the member id here, so a WARN that named the member
        // it failed for would be caught by the no-sentinel check below.
        field: SENTINEL,
        value: entry(SENTINEL, { email: SENTINEL }),
        throwFor: SENTINEL,
        says: 'departure handler failed',
    },
    {
        label: 'an unparseable entry',
        channel: CHANNEL,
        field: '7',
        value: SENTINEL,
        says: NOT_ANNOUNCED,
    },
    {
        label: 'an entry with no member id',
        channel: CHANNEL,
        field: '7',
        value: JSON.stringify({ member: { name: SENTINEL }, owner: DEAD }),
        says: NOT_ANNOUNCED,
    },
    {
        label: 'an entry whose member id is not its slot (A2/S1)',
        channel: CHANNEL,
        field: '7',
        value: entry(SENTINEL),
        says: NOT_ANNOUNCED,
    },
    {
        label: 'an owned entry whose channel fails isValidName (S1)',
        channel: 'presence-b@d',
        field: '7',
        value: entry(7, { email: SENTINEL }),
        says: NOT_ANNOUNCED,
    },
]

for (const drop of DROP_CASES) {
    Deno.test(`#348 W7 ${drop.label}: one WARN naming the channel only, no departure, the sweep goes on`, async () => {
        const redis = new FakeRedis()
        const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
        const b = redisDriver(redis)
        const reported: (string | number)[] = []
        const seam: BroadcastDriver = b
        seam.onRosterDeparture?.(({ member }) => {
            if (member.id === drop.throwFor) {
                throw new Error(`handler refused ${SENTINEL} ${member.id}`)
            }
            reported.push(member.id)
        })
        const warnings = captureWarnings()
        try {
            // The bad entry first, so "the sweep goes on" is proven by the
            // good one after it.
            await plantHold(redis, drop.channel, drop.field, drop.value)
            await plantHold(redis, CHANNEL, '8', entry(8, { name: 'Bo' }))
            // B holds something, so its reconcile pass runs.
            await b.holdMember(OTHER, { id: 9 })
            await lapse(time)

            assertEquals(
                reported,
                [8],
                'only the well-formed departure is reported, and the sweep ' +
                    'reaches it',
            )
            const relevant = warnings.lines.filter(notSummary)
            assertEquals(relevant.length, 1, relevant.join('\n'))
            assert(
                relevant[0].includes(drop.channel),
                `the WARN names the channel: ${relevant[0]}`,
            )
            assert(
                relevant[0].includes(drop.says),
                `the WARN says "${drop.says}": ${relevant[0]}`,
            )
            for (const line of warnings.lines) {
                assert(
                    !line.includes(SENTINEL),
                    `no WARN carries the entry bytes or the member id: ${line}`,
                )
            }
            assertEquals(
                await redis.command(
                    'HGET',
                    PRESENCE_KEY(drop.channel),
                    drop.field,
                ),
                { type: 'nil' },
                'the release stays committed',
            )
            assertEquals(
                await redis.command(
                    'EXISTS',
                    HOLDERS_KEY(drop.channel, drop.field),
                ),
                { type: 'integer', value: 0 },
            )
        } finally {
            warnings.restore()
            await b.close()
            time.restore()
            redis.assertNoRejections()
        }
    })
}

Deno.test('#348 S2 readRoster: a malformed entry is skipped with a WARN that never quotes its bytes', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const b = redisDriver(redis)
    const warnings = captureWarnings()
    try {
        await redis.command('HSET', PRESENCE_KEY(CHANNEL), '7', SENTINEL)
        const window = await b.readRoster(CHANNEL, 100, [7])
        assertEquals(window.members, [])
        assert(warnings.lines.length >= 1, 'precondition: the skip is logged')
        for (const line of warnings.lines) {
            assert(!line.includes(SENTINEL), `entry bytes in a WARN: ${line}`)
        }
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- A6: the departure handler's lifecycle -------------------------------------

Deno.test('#348 A6 the departure handler: a second registration replaces the first, and a closed driver reports no departure', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const b = redisDriver(redis)
    const seam: BroadcastDriver = b
    const calls: string[] = []
    seam.onRosterDeparture?.(({ member }) => void calls.push(`h1 ${member.id}`))
    seam.onRosterDeparture?.(({ member }) => void calls.push(`h2 ${member.id}`))
    try {
        await plantHold(redis, CHANNEL, '7', entry(7))
        // B holds something, so its reconcile pass runs.
        await b.holdMember(OTHER, { id: 9 })
        await lapse(time)
        assertEquals(
            calls,
            ['h2 7'],
            'ONE handler: the second registration replaced the first',
        )

        // A closed driver reports no departure. Whether close() DROPS the
        // handler is not observable here, and the title does not claim it:
        // no pass runs after close() resolves, so the drop is equivalent
        // (battery M12, expectSurvival). The mid-sweep case — a
        // release in flight when close() lands — is #355's W4 (i): close()
        // now waits for that release, and its departure IS announced.
        await b.close()
        await plantHold(redis, CHANNEL, '8', entry(8), 'instance-dead-2')
        await lapse(time)
        assertEquals(calls, ['h2 7'], 'a closed driver reports no departure')
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W8: `left` precedes a hold committed right behind the sweep (A1) ---------

Deno.test('#348 W8 a hold committed right behind the sweep release — left, then joined, on the bus and locally', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const log: Published[] = []
    // B runs one exchange at a time, as the production client does.
    const serial: SerializedCommands = serializedCommands(redis.command)
    const a = instance(redis, 'A', log)
    const b = instance(redis, 'B', log, { command: serial.command })
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        await a.driver.close()

        // The sweep's release of A's 7 names A's owned set; B's own hold of 7
        // is the only script naming B's owned set and 7's holders. Keyed on
        // the owned set, not on the key count: since #355 the release
        // declares four keys too.
        const release = serial.hold((args) =>
            args[0] === 'EVAL' && args.includes(OWNED_KEY(a.id))
        )
        const holdIssued = serial.whenIssued((args) =>
            args[0] === 'EVAL' && args.includes(OWNED_KEY(b.id)) &&
            args.includes(HOLDERS_KEY(CHANNEL, 7))
        )
        const lapsed = lapse(time)
        await release.reached
        // The sweep's release has committed; its reply is not back yet.
        const b7 = conn('b7', 7, 'Ada')
        b.manager.register(b7)
        const join = b.manager.subscribe(b7, CHANNEL)
        await holdIssued
        // The margin here is microtask ordering on the serialized wrapper,
        // not wall-clock time. The queued hold cannot start until this reply
        // is delivered, and its `joined` needs a reply of its own on top; the
        // sweep reaches the handler with no further exchange. Nothing waits
        // on a timer, so the order cannot drift with load. A regression that
        // adds an exchange before the handler (M9) or chains the departure on
        // the slot tail (M8) can only move the `left` behind the `joined`: it
        // turns this row red, never falsely green.
        release.release()
        await lapsed
        await join
        await settle()

        assertEquals(
            actions(observer, 7),
            ['joined', 'left', 'joined'],
            'the swept left reaches the room before the arrival that followed it',
        )
        assertEquals(
            busPresence(redis)
                .filter((wire) => wire.member?.id === 7)
                .map((wire) => wire.kind),
            ['presence-join', 'presence-leave', 'presence-join'],
            'and in that order on the bus',
        )
        // 7's OWN connection on B, claimed while the sweep's release was in
        // flight, hears neither the swept `left` nor the `joined`: no
        // connection receives a frame about its own member id (#349 W3b).
        // Its `here` is correct.
        assertEquals(actions(b7, 7), [], "7's own tab hears nothing about 7")
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- S3: the manager checks what a driver reports -----------------------------

Deno.test('#348 S3 a malformed departure from a driver is dropped by the manager: one WARN, no emit, no publish', async () => {
    let handler: ((d: RosterDeparture) => void | Promise<void>) | undefined
    const published: ControlMessage[] = []
    const roster = new Map<string, PresenceMember>()
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl: (control) => void published.push(control),
        holdMember: (_channel, member) => {
            const arrived = !roster.has(String(member.id))
            roster.set(String(member.id), member)
            return { arrived }
        },
        releaseMember: (_channel, id) => ({ gone: roster.delete(String(id)) }),
        readRoster: (_channel, limit, selfIds) =>
            asWindow([...roster.values()], limit, selfIds),
        onRosterDeparture: (registered) => void (handler = registered),
    }
    const manager = new ChannelManager<User>({ driver, authorize })
    const observer = conn('observer', 1)
    manager.register(observer)
    await manager.subscribe(observer, CHANNEL)
    assert(handler, 'precondition: a manager with a roster registers')
    const seen = observer.received.length
    published.length = 0

    // Each malformed departure carries the sentinel in its member, so a WARN
    // that quoted the member would be caught: the line names the channel, or
    // the channel's type when it is not a string, and nothing else.
    const bad: ReadonlyArray<
        readonly [label: string, departure: RosterDeparture, names: string]
    > = [
        ['a channel outside isValidName', {
            channel: 'presence-b@d',
            member: { id: SENTINEL },
        }, 'presence-b@d'],
        ['a non-string channel', {
            channel: 42 as unknown as string,
            member: { id: SENTINEL },
        }, 'a number channel'],
        ['a null member id', {
            channel: CHANNEL,
            member: {
                id: null,
                info: { email: SENTINEL },
            } as unknown as PresenceMember,
        }, CHANNEL],
        ['an array info', {
            channel: CHANNEL,
            member: { id: 7, info: [SENTINEL] } as unknown as PresenceMember,
        }, CHANNEL],
        // The wire-member rule's key bound: every peer's ingest refuses a
        // member with more than `id` and `info`, so this instance must too.
        ['a member with a third key', {
            channel: CHANNEL,
            member: {
                id: 7,
                info: {},
                extra: SENTINEL,
            } as unknown as PresenceMember,
        }, CHANNEL],
        // The key rule is an ALLOW-LIST, not a count (#350): two keys, but
        // the second is not `info`. A count bound of two admits it, and the
        // room would hear `smuggled` in a `left` frame.
        ['a two-key member whose second key is not info', {
            channel: CHANNEL,
            member: {
                id: 7,
                smuggled: SENTINEL,
            } as unknown as PresenceMember,
        }, CHANNEL],
    ]
    for (const [label, departure, names] of bad) {
        const warnings = captureWarnings()
        try {
            await handler(departure)
            assertEquals(warnings.lines.length, 1, label)
        } finally {
            warnings.restore()
        }
        const [line] = warnings.lines
        assert(
            line.startsWith(
                `realtime: dropped a roster departure the driver reported on ${names} — `,
            ),
            `${label}: the WARN names the channel only: ${line}`,
        )
        assert(
            !line.includes(SENTINEL),
            `${label}: member in the WARN: ${line}`,
        )
        assertEquals(observer.received.length, seen, `${label}: no emit`)
        assertEquals(published, [], `${label}: no publish`)
    }

    // The control: a well-formed departure is announced, as the channel.
    await handler({ channel: CHANNEL, member: { id: 7, info: { name: 'A' } } })
    assertEquals(frames(observer, 'left', 7).length, 1)
    assertEquals(
        published.map((c) => [c.kind, c.target, c.channel, c.member?.id]),
        [['presence-leave', CHANNEL, CHANNEL, 7]],
    )
})
