/**
 * @fileoverview #349 — a lapsed-but-alive instance re-holds its presence slots,
 * and no connection hears presence about itself.
 *
 * An instance can stay up while its liveness key lapses: a stalled event loop,
 * a long GC pause, a partition to Redis. A peer's ghost sweep then treats it as
 * crashed. Since #355 the sweep stops as soon as the instance renews, but every
 * hold it released before that renewal stayed released, and nothing put it
 * back: a member who stays connected and does nothing was shown as gone
 * indefinitely, and its own tabs heard themselves leave.
 *
 * The fix (plan `.specnaut/specs/263-lapse-rehold/plan.md`): the heartbeat's
 * liveness write becomes `SET … EX … GET`, whose nil reply says the key was
 * re-created; once a hold has been issued, that (or a failed beat) triggers
 * the driver's optional `onRosterLapse` hook; the manager re-checks durable
 * revocations, then writes every local slot again through `#syncRosterMember`,
 * one at a time, announcing only what the hold says arrived. Separately, the
 * maintainer's decision (2026-09-23): no presence frame ever reaches a
 * connection about its own member id, `left` included.
 *
 * Every witness runs real `RedisBroadcastDriver`s over one `FakeRedis`, with
 * `FakeTime` driving the heartbeat and the sweep in 50 ms steps. The lapse is
 * injected by a command wrapper that refuses the liveness `SET` — the instance
 * is otherwise healthy — exactly what `withFaultyInstance` does on a live
 * broker.
 *
 * Committed red first: W1, W2, W3, W3b, W7 (ii) and (iii), W15; and W8, W9,
 * W11b, W13, W15b and WS1, which need code that did not exist. WD and WL
 * landed with the code they import (a module that does not resolve cannot
 * pass the pre-commit type check); both were run red against the pre-fix
 * driver first. W4, W7 (i) and
 * W10 are guards, green before and after. W5 and W6 guard the frames (no
 * `joined` from a re-assert of a slot still held) and are red before only on
 * the half that needs a re-assert at all. The `LapseRun` unit witnesses live
 * in `lapse_run_349.test.ts`.
 *
 * Added by the review cycle, each with the mutant it kills: W1b (the
 * stalled-loop nil path, M1), W3c (the exclusion by member id, M28), W4b
 * (joins during a re-assert, M29), W8b (a failed SADD sets nothing, M30), W8c
 * (the suspicion cleared, M31), W11b (ii) (the abort before the sweep pass's
 * await, M25) and W15c (one revocation that throws, M27).
 *
 * @module @lockness/realtime/tests/lapse_rehold_349
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { decodeBeatReply, RedisBroadcastDriver } from '../drivers/redis.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { ChannelManager } from '../manager.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ChannelRevocation,
    Revocation,
    RevocationStoreDriver,
    RosterHold,
    RosterRelease,
    RosterWindow,
} from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    type CommandFn,
    type CommandMatch,
    FakeRedis,
    type SerializedCommands,
    serializedCommands,
} from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'
import { watchingEscapes } from './escape_watcher.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const CONTROL_TOPIC = `${PREFIX}__control`
const HOLDERS_KEY = (channel: string, id: string | number) =>
    `${PREFIX}__holders:${channel} ${String(id)}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const ALIVE_PREFIX = `${PREFIX}__alive:`
const INSTANCES_KEY = `${PREFIX}__instances`
const REVOCATIONS_KEY = `${PREFIX}__revocations`

/** The words of the one WARN a failed lapse run logs (`LapseRun`). */
const RUN_FAILED = "re-asserting this instance's presence holds after a " +
    'liveness lapse failed'
/** The words of the one WARN a failed pre-re-assert revocation re-check logs. */
const RECHECK_FAILED = 'before re-asserting'
/** The words of the WARN for one revocation a reconcile could not apply. */
const REVOCATION_CONTAINED = 'a durable revocation could not be applied'
/** The words of the heartbeat's one WARN per failed beat. */
const BEAT_FAILED = 'instance-liveness heartbeat failed'

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

/** Every `joined` / `left` a connection received, whoever it names. */
const transitions = (c: Recording) =>
    c.received.filter((f) =>
        f.type === 'presence' && (f.action === 'joined' || f.action === 'left')
    )

/** Whether a connection was told it left `channel` (a revocation). */
const toldUnsubscribed = (c: Recording, channel: string) =>
    c.received.some((f) => f.type === 'unsubscribed' && f.channel === channel)

/** Run the microtask queue out. */
async function settle(times = 300): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

/**
 * Advance FakeTime by `ms` in 50 ms steps, draining microtasks after each —
 * so a heartbeat's or a sweep's round trips complete at the instant they were
 * issued, as they would on a fast broker.
 */
async function advance(time: FakeTime, ms: number): Promise<void> {
    for (let t = 0; t < ms; t += 50) {
        await time.tickAsync(50)
        await settle()
    }
}

/**
 * Step FakeTime until `cond` holds, for at most `limitMs`. Never hangs: a
 * witness waiting on behaviour the code does not have yet fails on its
 * assertion instead.
 *
 * @returns Whether `cond` held within the limit.
 */
async function until(
    time: FakeTime,
    cond: () => boolean | Promise<boolean>,
    limitMs: number,
): Promise<boolean> {
    for (let t = 0; t <= limitMs; t += 50) {
        if (await cond()) return true
        await time.tickAsync(50)
        await settle()
    }
    return await cond()
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

/** A liveness write: `SET <prefix>__alive:<id> …`. */
const isAliveSet: CommandMatch = (args) =>
    args[0] === 'SET' && (args[1]?.startsWith(ALIVE_PREFIX) ?? false)

/**
 * A hold `EVAL` issued by `instanceId` for member `id` — the only script that
 * names both the holder's owned set and the instances set (a release names the
 * releaser's liveness key instead, a deregistration no holders hash).
 */
const isHold = (instanceId: string, id: number): CommandMatch => (args) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(instanceId)) &&
    args.includes(INSTANCES_KEY) && args.includes(HOLDERS_KEY(CHANNEL, id))

/**
 * A `listRevocations` pass, by its first command: the reap, the one `EVAL`
 * naming the revocation index (#359 — the pages that follow are `ZSCAN`s).
 */
const isListRevocations: CommandMatch = (args) =>
    args[0] === 'EVAL' && args.includes(REVOCATIONS_KEY)

/** How a one-shot fault rule answers the command it matches. */
type Effect =
    | { readonly kind: 'reject' }
    | { readonly kind: 'lose-reply' }
    | { readonly kind: 'reply'; readonly reply: unknown }

/**
 * A command port over the shared fake whose faults a witness switches: the
 * liveness `SET` refused (a lapse with the instance otherwise healthy), every
 * command refused (a partition), a reply replaced, or one matching command
 * refused or its reply lost after it committed.
 */
function faulty(inner: CommandFn) {
    let aliveBroken = false
    let partitioned = false
    let aliveReply: unknown
    const rules: Array<{ match: CommandMatch; effect: Effect }> = []
    const command: CommandFn = (...args) => {
        if (partitioned) {
            return Promise.reject(new Error('injected: partitioned (#349)'))
        }
        if (aliveBroken && isAliveSet(args)) {
            return Promise.reject(
                new Error('injected: liveness write refused (#349)'),
            )
        }
        if (aliveReply !== undefined && isAliveSet(args)) {
            const reply = aliveReply
            return inner(...args).then(() => reply)
        }
        const index = rules.findIndex((rule) => rule.match(args))
        if (index >= 0) {
            const { effect } = rules.splice(index, 1)[0]
            if (effect.kind === 'reject') {
                return Promise.reject(new Error('injected: refused (#349)'))
            }
            if (effect.kind === 'lose-reply') {
                return inner(...args).then(() => {
                    throw new Error('injected: reply lost (#349)')
                })
            }
            return inner(...args).then(() => effect.reply)
        }
        return inner(...args)
    }
    return {
        command,
        breakAlive: () => void (aliveBroken = true),
        healAlive: () => void (aliveBroken = false),
        partition: (on: boolean) => void (partitioned = on),
        /** Replace every liveness `SET`'s reply (it still writes), or stop. */
        answerAliveWith: (reply: unknown) => void (aliveReply = reply),
        /** Apply `effect` to the NEXT command matching `match`, once. */
        once: (match: CommandMatch, effect: Effect) =>
            void rules.push({ match, effect }),
    }
}

/** A subscriber over the shared fake that can drop every delivery. */
function droppable(redis: FakeRedis) {
    const inner = redis.subscriberFor()
    let dropping = false
    return {
        subscriber: {
            psubscribe: (
                pattern: string,
                handler: (topic: string, payload: string) => void,
            ) => inner.psubscribe(pattern, (topic, payload) => {
                if (!dropping) handler(topic, payload)
            }),
            onReconnect: inner.onReconnect,
        },
        drop: (on: boolean) => void (dropping = on),
    }
}

function redisDriver(
    redis: FakeRedis,
    command: CommandFn = redis.command,
    subscriber: {
        psubscribe(
            pattern: string,
            handler: (topic: string, payload: string) => void,
        ): void
    } = redis.subscriberFor(),
) {
    return new RedisBroadcastDriver(
        { command },
        subscriber,
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

/** One instance: a Redis driver over the shared fake, and its manager. */
function instance(
    redis: FakeRedis,
    options: {
        command?: CommandFn
        subscriber?: {
            psubscribe(
                pattern: string,
                handler: (topic: string, payload: string) => void,
            ): void
        }
    } = {},
) {
    const driver = redisDriver(redis, options.command, options.subscriber)
    const manager = new ChannelManager<User>({ driver, authorize })
    return { driver, manager, id: idOf(driver) }
}

/** The member ids of `channel`'s authoritative roster, as `driver` reads it. */
async function rosterIds(driver: RedisBroadcastDriver): Promise<number[]> {
    const window = await driver.readRoster(CHANNEL, 100, [])
    return window.members.map((m) => m.id as number).sort((x, y) => x - y)
}

/** How many hold `EVAL`s `instanceId` has issued for member `id`. */
const holdCount = (redis: FakeRedis, instanceId: string, id: number) =>
    redis.commandLog().filter(isHold(instanceId, id)).length

/** The presence control frames actually PUBLISHed on the bus, in bus order. */
function busPresence(redis: FakeRedis) {
    return redis.commandLog()
        .filter(([cmd, topic]) => cmd === 'PUBLISH' && topic === CONTROL_TOPIC)
        .map(([, , payload]) =>
            JSON.parse(payload) as { kind: string; member?: PresenceMember }
        )
        .filter((wire) => wire.kind.startsWith('presence-'))
}

/**
 * The FakeTime origin. On a whole second, so a liveness `SET … EX 2` issued at
 * t0 expires at exactly t0 + 2 s: the sweep pass armed at t0 + 1 s finds the
 * instance alive, and the next one, at t0 + 2 s, finds it dead.
 */
const T0 = new Date('2026-09-23T10:00:00Z')

/** Long enough for a broken instance's key to lapse and a peer to sweep it. */
const LAPSE_AND_SWEEP_MS = 3_000
/** One heartbeat interval plus the re-assert's round trips, plus slack. */
const ONE_BEAT_MS = 600

// --- US1: a lapsed instance puts its members back ----------------------------

Deno.test('#349 W1 7 held only on A, A lapses and is swept — 7 is back in B’s roster and here within one heartbeat interval', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        assertEquals(await rosterIds(b.driver), [1, 7], 'precondition')

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        assertEquals(await rosterIds(b.driver), [1], 'B swept A')

        fa.healAlive()
        const back = await until(
            time,
            async () => (await rosterIds(b.driver)).includes(7),
            ONE_BEAT_MS,
        )
        assert(back, '7 is back in the authoritative roster within one beat')
        const bLate = conn('b-late', 2)
        b.manager.register(bLate)
        const here = await b.manager.subscribe(bLate, CHANNEL)
        assert(
            here.here?.members.some((m) => m.id === 7),
            "7 is back in a new subscriber's here",
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W1b a stalled loop — no beat fails, the next SET … GET answers nil, and 7 is back within one heartbeat interval', async () => {
    // US1's other path, and the one W1 cannot reach: W1 lapses A by REFUSING
    // its liveness writes, so every beat fails, the lapse is suspected, and
    // the healed beat re-asserts on the suspicion alone. Here nothing fails.
    // A's commands simply stop reaching the broker — its one command client
    // holds the liveness SET's reply, and every command queues behind it — as
    // a stalled event loop or a long GC pause would. The key lapses, B sweeps
    // A, and when the queue drains the next SET … GET answers nil: the lapse
    // bit alone is what must bring 7 back.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const serial = serializedCommands(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        assertEquals(await rosterIds(b.driver), [1, 7], 'precondition')

        const stalled = serial.hold(isAliveSet)
        assert(
            await until(
                time,
                async () => !(await rosterIds(b.driver)).includes(7),
                LAPSE_AND_SWEEP_MS,
            ),
            'B swept A while its commands were stalled',
        )

        stalled.release()
        const back = await until(
            time,
            async () => (await rosterIds(b.driver)).includes(7),
            ONE_BEAT_MS,
        )
        assert(back, '7 is back in the authoritative roster within one beat')
        assertEquals(
            warnings.having(BEAT_FAILED),
            [],
            'no beat failed: the nil alone carried the lapse',
        )
        const bLate = conn('b-late', 2)
        b.manager.register(bLate)
        const here = await b.manager.subscribe(bLate, CHANNEL)
        assert(
            here.here?.members.some((m) => m.id === 7),
            "7 is back in a new subscriber's here",
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W2 observers on B and C each receive exactly left, then joined, for 7', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const c = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
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

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        fa.healAlive()
        await advance(time, ONE_BEAT_MS)
        // Well past the return: nothing more is announced.
        await advance(time, 2_000)

        assertEquals(actions(onB, 7), ['joined', 'left', 'joined'])
        assertEquals(actions(onC, 7), ['joined', 'left', 'joined'])
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        await c.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US2: a member never hears presence about itself -------------------------

Deno.test('#349 W3 7’s own tab on A hears nothing about 7; 8’s tab on A hears left, then joined', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a7, CHANNEL)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        fa.healAlive()
        await advance(time, ONE_BEAT_MS)

        assertEquals(
            actions(a7, 7),
            [],
            '7 never hears its own departure or return',
        )
        assertEquals(actions(a8, 8), [], 'nor does 8 hear its own')
        assertEquals(
            actions(a8, 7),
            ['left', 'joined'],
            '8 hears 7 leave (the sweeper’s left) and come back',
        )
        assertEquals(actions(a7, 8), ['joined', 'left', 'joined'])
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W3b #348 W8’s race on the sweeper — 7’s new tab on B receives no left for 7', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    // B runs one exchange at a time, as the production client does.
    const serial: SerializedCommands = serializedCommands(redis.command)
    const a = instance(redis)
    const b = instance(redis, { command: serial.command })
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        await a.driver.close()

        const release = serial.hold((args) =>
            args[0] === 'EVAL' && args.includes(OWNED_KEY(a.id))
        )
        const holdIssued = serial.whenIssued(isHold(b.id, 7))
        const lapsed = time.tickAsync(3_500).then(() => settle())
        await release.reached
        // The sweep's release has committed; its reply is not back yet.
        const b7 = conn('b7', 7, 'Ada')
        b.manager.register(b7)
        const join = b.manager.subscribe(b7, CHANNEL)
        await holdIssued
        release.release()
        await lapsed
        await join
        await settle()

        assertEquals(
            actions(observer, 7),
            ['joined', 'left', 'joined'],
            'the observer still hears the departure and the return',
        )
        assertEquals(
            actions(b7, 7),
            [],
            '7’s own new tab hears neither its swept left nor its joined',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W3c the exclusion is by member id, not by connection — 7’s second tab hears nothing about 7, 8’s tab hears both', async () => {
    // W3 has one tab per member, where "the connection announced" and "every
    // connection of that member" exclude the same socket. Here 7 has two tabs
    // on A with two distinct entries: the re-assert's local `joined` for 7
    // carries the FIRST tab's entry (the one `#localRoster` kept), so an
    // exclusion keyed on the connection — or on the entry object — would
    // still send it to the second tab. The sweeper's remote `left` carries no
    // local entry at all.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        const a7b = conn('a7-second-tab', 7, 'Ada')
        a.manager.register(a7b)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a7, CHANNEL)
        await a.manager.subscribe(a7b, CHANNEL)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        fa.healAlive()
        await advance(time, ONE_BEAT_MS)

        assertEquals(actions(a7, 7), [], 'the first tab hears nothing about 7')
        assertEquals(
            actions(a7b, 7),
            [],
            'nor does the second: the exclusion is the member id',
        )
        assertEquals(
            actions(a8, 7),
            ['left', 'joined'],
            'a different member on the channel is not excluded',
        )
        assertEquals(actions(a7b, 8), ['joined', 'left', 'joined'])
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- Guards: the re-assert writes through the slot, and announces nothing held

Deno.test('#349 W4 8’s only tab closes while the re-assert is mid-flight — 8 stays absent, and no joined for 8', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const serial = serializedCommands(fa.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        const gate = serial.hold(isHold(a.id, 7))
        let reached = false
        void gate.reached.then(() => void (reached = true))
        fa.healAlive()
        await until(time, () => reached, ONE_BEAT_MS)
        // With a re-assert, its first hold is in flight and 8's slot write is
        // still ahead of it. Without one (before #349), the leave simply runs:
        // either way, the leave must win.
        const left = a.manager.unsubscribe('a8', CHANNEL)
        await settle()
        gate.release()
        await left
        await advance(time, 2_000)

        assert(!(await rosterIds(b.driver)).includes(8), '8 stays absent')
        assertEquals(
            actions(observer, 8).filter((x) => x === 'joined').length,
            1,
            'no joined for 8 beyond its first',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W4b joins during the re-assert — each announces itself once, and the re-assert writes only the slots it snapshotted', async () => {
    // The plan's other edge case: a new tab's own join holds its slot. 8's
    // second tab joins a slot the re-assert has yet to reach, so the join's
    // write fills it (one `joined`) and the re-assert's, queued behind it on
    // the same tail, finds it held (no frame). 9 is a member the re-assert
    // never saw: its slots were fixed before it began, so it writes 9 not at
    // all. A walk over the LIVE presence map would write every join that
    // lands during the run too — and, under steady joins, never finish.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const serial = serializedCommands(fa.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        const gate = serial.hold(isHold(a.id, 7))
        let reached = false
        void gate.reached.then(() => void (reached = true))
        fa.healAlive()
        assert(
            await until(time, () => reached, ONE_BEAT_MS),
            'the re-assert reached its first slot',
        )

        const a8b = conn('a8-second-tab', 8, 'Boris')
        a.manager.register(a8b)
        let joins = 0
        void a.manager.subscribe(a8b, CHANNEL).then(() => void joins++)
        const a9 = conn('a9', 9, 'Cleo')
        a.manager.register(a9)
        void a.manager.subscribe(a9, CHANNEL)
            .then(() => void joins++)
        await settle()
        gate.release()
        assert(
            await until(time, () => joins === 2, 1_000),
            'both joins settled',
        )
        await advance(time, 1_000)

        assertEquals(
            actions(observer, 8),
            ['joined', 'left', 'joined'],
            'one joined for 8 after the sweep: the join’s, not the re-assert’s',
        )
        assertEquals(actions(observer, 9), ['joined'], 'one joined for 9')
        assertEquals(actions(a8, 8), [], '8’s first tab hears nothing of 8')
        assertEquals(actions(a8b, 8), [], 'nor does its new one')
        assertEquals(await rosterIds(b.driver), [1, 7, 8, 9])
        assertEquals(
            holdCount(redis, a.id, 9),
            1,
            'the re-assert never wrote 9, which joined after it began',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W5 7 held on A and D; A swept → no frame; A re-asserts → A a holder again, still no frame', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const d = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const d7 = conn('d7', 7, 'Ada')
        d.manager.register(d7)
        await d.manager.subscribe(d7, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        assertEquals(
            actions(observer, 7),
            ['joined'],
            'the sweep kept the slot',
        )
        fa.healAlive()
        await advance(time, ONE_BEAT_MS + 1_000)

        assertEquals(actions(observer, 7), ['joined'], 'and the re-assert too')
        const holder = await redis.command(
            'HGET',
            HOLDERS_KEY(CHANNEL, 7),
            a.id,
        ) as { type: string }
        assert(holder.type === 'bulk', 'A holds 7 again')
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        await d.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W6 A lapses and nobody sweeps it — the re-assert re-holds each slot, and no frame goes out', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('a-observer', 1)
        a.manager.register(observer)
        await a.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        const framesBefore = transitions(observer).length
        const publishedBefore = busPresence(redis).length

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        fa.healAlive()
        await advance(time, ONE_BEAT_MS + 1_000)

        assertEquals(holdCount(redis, a.id, 1), 2, 'slot 1 re-held once')
        assertEquals(holdCount(redis, a.id, 7), 2, 'slot 7 re-held once')
        assertEquals(transitions(observer).length, framesBefore, 'no frame')
        assertEquals(busPresence(redis).length, publishedBefore, 'no publish')
        assertEquals(await rosterIds(a.driver), [1, 7], 'roster unchanged')
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W7: a nil counts only once a hold was issued ----------------------------

Deno.test('#349 W7 (i) the boot beat before any hold is not a lapse — one hold EVAL, no re-assert', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const a = instance(redis)
    try {
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await advance(time, 2_000)
        assertEquals(holdCount(redis, a.id, 7), 1)
    } finally {
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W7 (ii) a hold that commits before a gated boot SET, and is swept, is re-asserted', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    // NOT serializing: the boot SET is held back BEFORE it executes, and a
    // second hold overtakes it on the wire.
    let openSet!: () => void
    const setGate = new Promise<void>((resolve) => (openSet = resolve))
    let gateArmed = true
    const command: CommandFn = async (...args) => {
        if (gateArmed && isAliveSet(args)) {
            gateArmed = false
            await setGate
        }
        return await redis.command(...args)
    }
    const b = instance(redis)
    const a = instance(redis, { command })
    const warnings = captureWarnings()
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        // 7's join starts the boot beat, whose SET waits at the gate.
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        const join7 = a.manager.subscribe(a7, CHANNEL)
        await settle()
        // 8's hold does not wait for the boot beat: it commits, and registers
        // A, while A has no liveness key.
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()
        assertEquals(await rosterIds(b.driver), [1, 8], 'precondition')

        // B's pass finds A registered and dead, and sweeps 8.
        await advance(time, 1_100)
        assertEquals(await rosterIds(b.driver), [1], 'B swept 8')

        openSet()
        // Raced against a bounded wait, as in (iii): a boot beat that awaited
        // the run it starts would deadlock on 7's own slot.
        let joined7 = false
        void join7.then(() => void (joined7 = true))
        assert(await until(time, () => joined7, 1_000), '7’s join settled')
        await advance(time, ONE_BEAT_MS)

        assertEquals(
            await rosterIds(b.driver),
            [1, 7, 8],
            'the boot beat reported the lapse, and 8 was re-asserted',
        )
        assertEquals(actions(observer, 8), ['joined', 'left', 'joined'])
    } finally {
        warnings.restore()
        openSet()
        let closed = false
        void a.driver.close().then(() => void (closed = true))
        await until(time, () => closed, 1_000)
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W7 (iii) two holds racing the boot beat on a serialized client — exactly one re-assert run, no frame', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const serial = serializedCommands(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        // The boot beat's SET is in flight when the second hold is issued, so
        // that hold is queued ahead of the beat's SADD — and the beat's tail
        // sees a hold issued (A4). Its nil counts: one harmless re-assert.
        const bootSet = serial.hold(isAliveSet)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        const join7 = a.manager.subscribe(a7, CHANNEL)
        await bootSet.reached
        const hold8 = serial.whenIssued(isHold(a.id, 8))
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        const join8 = a.manager.subscribe(a8, CHANNEL)
        await hold8
        bootSet.release()
        // RACED against a FakeTime-bounded wait, never awaited bare: a beat
        // that awaited its run would deadlock here — the run re-writes slots
        // whose tails hold these very joins, which wait on that boot beat —
        // and a bare `await` would leave Deno's deadlock detection to kill
        // the test instead of this assertion.
        let joinsSettled = false
        void Promise.all([join7, join8]).then(() => void (joinsSettled = true))
        assert(
            await until(time, () => joinsSettled, 1_000),
            'both joins settled: the boot beat never waits on the run it starts',
        )
        await advance(time, 1_000)

        assertEquals(holdCount(redis, a.id, 7), 2, 'one re-assert of 7')
        assertEquals(holdCount(redis, a.id, 8), 2, 'one re-assert of 8')
        assertEquals(actions(observer, 7), ['joined'], 'no frame for 7')
        assertEquals(actions(observer, 8), ['joined'], 'no frame for 8')
    } finally {
        // Bounded too: after that deadlock `close()` would wait forever for
        // the run in flight.
        let closed = false
        void a.driver.close().then(() => void (closed = true))
        await until(time, () => closed, 1_000)
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- Suspicion: a failed beat is a lapse the next successful beat repairs ----

Deno.test('#349 W8 a beat whose reply is lost after it committed starts no run; the next successful beat re-asserts', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('a-observer', 1)
        a.manager.register(observer)
        await a.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        const framesBefore = transitions(observer).length

        fa.once(isAliveSet, { kind: 'lose-reply' })
        await advance(time, 500)
        assertEquals(warnings.having(BEAT_FAILED).length, 1, 'the beat failed')
        assertEquals(holdCount(redis, a.id, 7), 1, 'no run on a failed beat')

        await advance(time, 500)
        assertEquals(
            holdCount(redis, a.id, 7),
            2,
            'the next successful beat re-asserts, although its reply is a bulk',
        )
        assertEquals(transitions(observer).length, framesBefore, 'no frame')
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

/** The beat's registration: `SADD <prefix>__instances <id>`. */
const isInstancesSadd: CommandMatch = (args) =>
    args[0] === 'SADD' && args[1] === INSTANCES_KEY

Deno.test('#349 W8b a failed SADD sets nothing — one WARN, and neither that beat nor the next starts a run', async () => {
    // FR-004: only the liveness write's outcome decides. The registration
    // failing says nothing about whether this instance's holds were swept —
    // its SET renewed a key that was there — so it must not make the lapse
    // suspected, or every broker hiccup on SADD would cost K re-holds.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const observer = conn('a-observer', 1)
        a.manager.register(observer)
        await a.manager.subscribe(observer, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()
        const framesBefore = transitions(observer).length

        fa.once(isInstancesSadd, { kind: 'reject' })
        await advance(time, 500)
        assertEquals(
            warnings.having(BEAT_FAILED).length,
            1,
            'the beat logged its failed SADD',
        )
        await advance(time, 1_000)
        assertEquals(
            holdCount(redis, a.id, 7),
            1,
            'no run: the SET answered a bulk, and nothing was suspected',
        )
        assertEquals(transitions(observer).length, framesBefore, 'no frame')
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W8c the suspicion is cleared when the run is triggered — the beats after the repair start no run', async () => {
    // A failed beat makes the lapse suspected, and the next successful beat
    // re-asserts — once. Left set, the suspicion would re-assert every slot on
    // every beat after it, for good.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        await settle()

        fa.once(isAliveSet, { kind: 'reject' })
        await advance(time, 1_000)
        assertEquals(
            holdCount(redis, a.id, 7),
            2,
            'the successful beat after the failed one re-asserted',
        )
        await advance(time, 2_000)
        assertEquals(
            holdCount(redis, a.id, 7),
            2,
            'and the four beats after it re-asserted nothing',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W9 one of three holds fails during a re-assert — the other two land, one WARN, and the next beat restores the third', async () => {
    // The failing member carries an id and an `info` no WARN could contain by
    // accident, so "names no member" is a check a leak would actually fail.
    const ID = 80_808_080
    const INFO = 'info-sentinel-w9'
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const aSentinel = conn('a-sentinel', ID, INFO)
        a.manager.register(aSentinel)
        await a.manager.subscribe(aSentinel, CHANNEL)
        const a9 = conn('a9', 9, 'Cleo')
        a.manager.register(a9)
        await a.manager.subscribe(a9, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        assertEquals(await rosterIds(b.driver), [1], 'B swept all three')

        fa.once(isHold(a.id, ID), { kind: 'reject' })
        fa.healAlive()
        await advance(time, ONE_BEAT_MS)
        assertEquals(
            await rosterIds(b.driver),
            [1, 7, 9],
            'the slots after the failed one were still written',
        )
        const failed = warnings.having(RUN_FAILED)
        assertEquals(failed.length, 1, 'exactly one WARN')
        assert(
            failed[0].includes('1 presence slot(s) could not be re-held') &&
                failed[0].includes('injected: refused (#349)'),
            `it counts the failed slots and renders the first error: ${
                failed[0]
            }`,
        )
        assert(!failed[0].includes(String(ID)), 'it names no member id')
        assert(!failed[0].includes(INFO), 'nor any info')

        await advance(time, 500)
        assertEquals(
            await rosterIds(b.driver),
            [1, 7, 9, ID],
            'the next successful beat ran the re-assert again',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US6: shutting down during a re-assert -----------------------------------

Deno.test('#349 W11b close() during a re-assert stops it before the next slot, and waits for the slot in flight', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const serial = serializedCommands(fa.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        const gate = serial.hold(isHold(a.id, 7))
        let reached = false
        void gate.reached.then(() => void (reached = true))
        fa.healAlive()
        assert(
            await until(time, () => reached, ONE_BEAT_MS),
            'the re-assert reached its first slot',
        )

        let closed = false
        const closing = a.driver.close().then(() => void (closed = true))
        await settle()
        assertEquals(closed, false, 'close() waits for the slot in flight')
        gate.release()
        await closing
        const holds = () =>
            holdCount(redis, a.id, 7) + holdCount(redis, a.id, 8)
        const holdsAtClose = holds()
        await advance(time, 2_000)

        assertEquals(holdCount(redis, a.id, 8), 1, 'slot 8 was never re-held')
        assertEquals(
            holds(),
            holdsAtClose,
            'no hold is issued once close() has resolved',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

/**
 * A command port that holds the reply of a matching command after it ran —
 * and, unlike `serializedCommands`, holds nothing else: every other command
 * still goes straight through, so two gates can be held at once.
 */
function gated(inner: CommandFn) {
    const gates: Array<{
        match: CommandMatch
        reach: () => void
        released: Promise<void>
    }> = []
    const command: CommandFn = async (...args) => {
        const index = gates.findIndex((g) => g.match(args))
        if (index < 0) return await inner(...args)
        const gate = gates.splice(index, 1)[0]
        const reply = await inner(...args)
        gate.reach()
        await gate.released
        return reply
    }
    return {
        command,
        hold(match: CommandMatch) {
            let reach!: () => void
            let release!: () => void
            const reached = new Promise<void>((resolve) => (reach = resolve))
            const released = new Promise<void>((resolve) => (release = resolve))
            gates.push({ match, reach, released })
            let isReached = false
            void reached.then(() => void (isReached = true))
            return { reached: () => isReached, release }
        },
    }
}

Deno.test('#349 W11b (ii) close() while a sweep pass is in flight stops the re-assert at once — no slot is written during the wait', async () => {
    // close() aborts the lapse run BEFORE it awaits the ghost-sweep pass, and
    // that order is the point: the pass can take a broker round trip or two,
    // and a re-assert still running meanwhile would issue a hold after
    // close() had begun. Here A's pass is held on its SMEMBERS while the
    // re-assert's first hold is held too; releasing the hold lets the
    // re-assert reach its next slot while close() still waits for the pass.
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const port = gated(fa.command)
    const b = instance(redis)
    const a = instance(redis, { command: port.command })
    const warnings = captureWarnings()
    // Released in `finally` too: a failed assertion must not leave close()
    // waiting on a held pass forever.
    const releases: Array<() => void> = []
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        const a8 = conn('a8', 8, 'Boris')
        a.manager.register(a8)
        await a.manager.subscribe(a8, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        const slot7 = port.hold(isHold(a.id, 7))
        const pass = port.hold((args) =>
            args[0] === 'SMEMBERS' && args[1] === INSTANCES_KEY
        )
        releases.push(slot7.release, pass.release)
        fa.healAlive()
        assert(
            await until(time, slot7.reached, ONE_BEAT_MS),
            'the re-assert reached its first slot',
        )
        assert(
            await until(time, pass.reached, 1_000),
            'a sweep pass is in flight',
        )

        let closed = false
        const closing = a.driver.close().then(() => void (closed = true))
        slot7.release()
        await settle()
        assertEquals(closed, false, 'close() waits for the sweep pass')
        assertEquals(
            holdCount(redis, a.id, 8),
            1,
            'no slot was written while close() waited for the pass',
        )
        pass.release()
        await closing
        await advance(time, 1_000)
        assertEquals(holdCount(redis, a.id, 8), 1, 'nor afterwards')
    } finally {
        warnings.restore()
        for (const release of releases) release()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- SC-005: the heartbeat never waits behind the whole re-assert ------------

Deno.test('#349 W13 K = 5 on a serialized client — a beat fired during the first hold commits before the last', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const serial = serializedCommands(fa.command)
    const b = instance(redis)
    const a = instance(redis, { command: serial.command })
    const ids = [11, 12, 13, 14, 15]
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        for (const id of ids) {
            const member = conn(`a${id}`, id)
            a.manager.register(member)
            await a.manager.subscribe(member, CHANNEL)
        }
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS)
        const gate = serial.hold(isHold(a.id, 11))
        let reached = false
        void gate.reached.then(() => void (reached = true))
        fa.healAlive()
        assert(
            await until(time, () => reached, ONE_BEAT_MS),
            'the re-assert reached its first slot',
        )
        const reachedAt = redis.commandLog().length
        // A beat fires while the first hold's reply is held.
        await advance(time, 500)
        gate.release()
        await advance(time, ONE_BEAT_MS)

        const log = redis.commandLog()
        const beat = log.findIndex((args, i) =>
            i >= reachedAt && isAliveSet(args) &&
            args[1] === `${ALIVE_PREFIX}${a.id}`
        )
        const lastHold = log.findLastIndex(isHold(a.id, 15))
        assert(beat >= 0, 'the beat was issued')
        assert(
            beat < lastHold,
            `the beat's SET (${beat}) committed before the last hold (${lastHold})`,
        )
        assertEquals(await rosterIds(b.driver), [1, ...ids])
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US3: a revocation issued during the lapse stays enforced ----------------

Deno.test('#349 W15 a revokeChannel lost during A’s partition is applied before A re-holds — no joined, 7 absent, c7 has left', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const sub = droppable(redis)
    const b = instance(redis)
    const a = instance(redis, {
        command: fa.command,
        subscriber: sub.subscriber,
    })
    const warnings = captureWarnings()
    try {
        const observer = conn('b-observer', 1)
        b.manager.register(observer)
        await b.manager.subscribe(observer, CHANNEL)
        const c7 = conn('c7', 7, 'Ada')
        a.manager.register(c7)
        await a.manager.subscribe(c7, CHANNEL)
        await settle()

        // A is partitioned: every command refused, every delivery dropped.
        fa.partition(true)
        sub.drop(true)
        assertEquals(await b.manager.revokeChannel('c7', CHANNEL), 'not-owned')
        // Healed between A's revocation ticks (t0 + k s) and just after a
        // beat, so the next beat — not the periodic re-check — is what runs
        // first.
        await advance(time, LAPSE_AND_SWEEP_MS + 100)
        assertEquals(await rosterIds(b.driver), [1], 'B swept A')
        assertEquals(toldUnsubscribed(c7, CHANNEL), false, 'the frame was lost')

        fa.partition(false)
        sub.drop(false)
        await advance(time, 500)

        assert(toldUnsubscribed(c7, CHANNEL), 'c7 has left the channel')
        assertEquals(
            actions(observer, 7),
            ['joined', 'left'],
            'no joined for 7',
        )
        assertEquals(await rosterIds(b.driver), [1], '7 stays absent')
        await advance(time, 2_000)
        assertEquals(actions(observer, 7), ['joined', 'left'], 'nor later')
        assertEquals(await rosterIds(b.driver), [1])
        assertEquals(
            busPresence(redis).filter((w) =>
                w.kind === 'presence-join' && w.member?.id === 7
            ).length,
            1,
            'no presence-join for 7 beyond its first, anywhere',
        )
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 W15b the pre-re-assert revocation re-check fails once — one manager WARN, 8 still restored, no extra run', async () => {
    // Sentinels a WARN could not contain by accident: the connection id (the
    // revocation's target), the member id and its `info`.
    const TARGET = 'conn-target-sentinel'
    const ID = 80_808_080
    const INFO = 'info-sentinel-w15b'
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const b = instance(redis)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        const bObserver = conn('b-observer', 1)
        b.manager.register(bObserver)
        await b.manager.subscribe(bObserver, CHANNEL)
        const target = conn(TARGET, ID, INFO)
        a.manager.register(target)
        await a.manager.subscribe(target, CHANNEL)
        await settle()

        fa.breakAlive()
        await advance(time, LAPSE_AND_SWEEP_MS + 100)
        // The next revocation read is the re-assert's: A's periodic one is
        // due only at the next whole second.
        fa.once(isListRevocations, { kind: 'reject' })
        fa.healAlive()
        await advance(time, 500)

        const recheck = warnings.having(RECHECK_FAILED)
        assertEquals(recheck.length, 1, 'exactly one manager WARN')
        assert(
            recheck[0].includes('the holds are re-asserted anyway') &&
                recheck[0].includes('injected: refused (#349)'),
            `it says the re-assert goes on, and renders the error: ${
                recheck[0]
            }`,
        )
        assert(!recheck[0].includes(TARGET), 'it names no target')
        assert(!recheck[0].includes(String(ID)), 'nor any member id')
        assert(!recheck[0].includes(INFO), 'nor any info')
        assertEquals(warnings.having(RUN_FAILED), [], 'the run did not fail')
        assertEquals(
            await rosterIds(b.driver),
            [1, ID],
            'the member is restored',
        )
        await advance(time, 1_000)
        assertEquals(holdCount(redis, a.id, ID), 2, 'no extra run')
    } finally {
        warnings.restore()
        await a.driver.close()
        await b.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

/**
 * A revocation store written by hand, whose reconcile a witness runs itself:
 * `reconcile` is the handler the manager registered.
 */
class HandRolledRevocationDriver implements RevocationStoreDriver {
    readonly #handlers: Array<(message: BroadcastMessage) => void> = []
    revocations: Revocation[] = []
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

    listRevocations(): Revocation[] {
        return [...this.revocations]
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

Deno.test('#349 W15c one revocation whose apply throws does not stop the ones after it — one WARN each, naming no target', async () => {
    // The re-assert's precondition runs `reconcileRevocations` (A2 / S2), so a
    // revocation that cannot be applied must not starve every one behind it:
    // before the fix, the first throw ended the pass, and — the socket still
    // open, the record still live — ended every later pass at the same place.
    // `#applyRevocation` contains everything but its own WARN; a log sink
    // that throws on that line is what gets past it.
    const TARGET = 'c1-target-sentinel'
    const INFO = 'info-sentinel-4f1c'
    const driver = new HandRolledRevocationDriver()
    const manager = new ChannelManager<User>({ driver, authorize })
    const poisoned: Recording = {
        ...conn(TARGET, 7_314_159, INFO),
        close: () => {
            throw new Error('the socket is already closing')
        },
    }
    const closed: number[] = []
    const healthy: Recording = {
        ...conn('c2', 8),
        close: (code?: number) => void closed.push(code ?? 0),
    }
    manager.register(poisoned)
    manager.register(healthy)
    await manager.subscribe(poisoned, CHANNEL)
    await manager.subscribe(healthy, CHANNEL)
    driver.revocations = [{ target: TARGET }, { target: 'c2' }]

    const warn = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => {
        const line = parts.join(' ')
        lines.push(line)
        if (line.includes('applying a revocation for')) {
            throw new Error('injected: the log sink refused the line')
        }
    }
    let outcome: unknown = 'resolved'
    try {
        await Promise.resolve(driver.reconcile?.()).catch((error) =>
            void (outcome = error)
        )
    } finally {
        console.warn = warn
    }

    assertEquals(outcome, 'resolved', 'the pass never rejects')
    assertEquals(closed, [4403], 'the revocation after it is still applied')
    const contained = lines.filter((l) => l.includes(REVOCATION_CONTAINED))
    assertEquals(contained.length, 1, 'one WARN for the one that threw')
    assert(
        contained[0].includes('the log sink refused the line'),
        `it renders the error: ${contained[0]}`,
    )
    assert(!contained[0].includes(TARGET), 'it names no target')
    assert(!contained[0].includes('7314159'), 'nor the member id')
    assert(!contained[0].includes(INFO), 'nor its info')
})

// --- S1: nothing escapes from a strict decode --------------------------------

Deno.test('#349 WS1 a SET answered OK, then an integer — one WARN per beat, no run, nothing escapes', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    const beats = () => redis.commandLog().filter(isAliveSet).length
    try {
        await watchingEscapes(async (escaped) => {
            fa.answerAliveWith({ type: 'simple', value: 'OK' })
            const a7 = conn('a7', 7, 'Ada')
            a.manager.register(a7)
            await a.manager.subscribe(a7, CHANNEL)
            await advance(time, 1_000)
            fa.answerAliveWith({ type: 'integer', value: 1 })
            await advance(time, 1_000)

            assertEquals(beats(), 5, 'the boot beat and four interval beats')
            assertEquals(
                warnings.having(BEAT_FAILED).length,
                beats(),
                'one WARN per beat',
            )
            assertEquals(
                holdCount(redis, a.id, 7),
                1,
                'no run on an undecodable beat',
            )
            assertEquals(escaped, [], 'no rejection escaped')

            // The beats after the hold made the lapse suspected: the first
            // decodable beat re-asserts.
            fa.answerAliveWith(undefined)
            await advance(time, 500)
            assertEquals(holdCount(redis, a.id, 7), 2)
        })
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#349 WS1 a failed boot beat before any hold makes nothing suspected', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const fa = faulty(redis.command)
    const a = instance(redis, { command: fa.command })
    const warnings = captureWarnings()
    try {
        fa.answerAliveWith({ type: 'simple', value: 'OK' })
        const a7 = conn('a7', 7, 'Ada')
        a.manager.register(a7)
        await a.manager.subscribe(a7, CHANNEL)
        fa.answerAliveWith(undefined)
        await advance(time, 1_000)
        assertEquals(holdCount(redis, a.id, 7), 1, 'no run')
    } finally {
        warnings.restore()
        await a.driver.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- WD: what a beat reply means ---------------------------------------------

Deno.test('#349 WD decodeBeatReply: nil is lapsed, any bulk continuous, anything else a constant throw', () => {
    assertEquals(decodeBeatReply({ type: 'nil' }), 'lapsed')
    assertEquals(decodeBeatReply({ type: 'bulk', value: '1' }), 'continuous')
    assertEquals(decodeBeatReply({ type: 'bulk', value: '' }), 'continuous')
    const MARKER = 'marker-ada@ex.com'
    const messages: string[] = []
    for (
        const reply of [
            { type: 'integer', value: 1 },
            { type: 'array', value: [{ type: 'bulk', value: MARKER }] },
            { type: 'simple', value: 'OK' },
            null,
        ]
    ) {
        const error = assertThrows(() => decodeBeatReply(reply), Error)
        messages.push(error.message)
    }
    assertEquals(new Set(messages).size, 1, 'the message is constant')
    assert(!messages[0].includes(MARKER), 'it never quotes the reply')
    assert(messages[0].includes('nil'), 'it names what is accepted')
})

// --- WL: the hook's lifecycle on the Redis driver ----------------------------

Deno.test('#349 WL onRosterLapse re-registration replaces; close() drops the lapse and the refusal handlers', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(T0)
    const serial = serializedCommands(redis.command)
    const driver = redisDriver(redis, serial.command)
    const unsigned = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX },
    )
    const warnings = captureWarnings()
    try {
        const called: string[] = []
        driver.onRosterLapse((_signal) => void called.push('first'))
        driver.onRosterLapse((_signal) => void called.push('second'))
        await driver.holdMember(CHANNEL, { id: 7 })
        // The key is gone: the next beat's SET re-creates it and answers nil.
        await redis.command('DEL', `${ALIVE_PREFIX}${idOf(driver)}`)
        await advance(time, 500)
        assertEquals(called, ['second'], 'the second registration replaced')

        // A beat reply that arrives after close() began starts nothing.
        await redis.command('DEL', `${ALIVE_PREFIX}${idOf(driver)}`)
        const beat = serial.hold(isAliveSet)
        let reached = false
        void beat.reached.then(() => void (reached = true))
        await until(time, () => reached, 600)
        assert(reached, 'a beat is in flight')
        const closing = driver.close()
        beat.release()
        await closing
        await settle()
        assertEquals(called, ['second'], 'no run after close()')

        // The refusal handler is dropped by close() too (FR-006a).
        const refusals: string[] = []
        unsigned.onControlRefused((refusal) =>
            void refusals.push(refusal.reason)
        )
        await unsigned.publishControl({ kind: 'evict', target: 'c1' })
        assertEquals(refusals, ['no-secret'], 'precondition: it is called')
        await unsigned.close()
        await unsigned.publishControl({ kind: 'evict', target: 'c1' })
        assertEquals(refusals, ['no-secret'], 'a closed driver calls nothing')
    } finally {
        warnings.restore()
        await driver.close()
        await unsigned.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US4: nothing changes for a consistent roster ----------------------------

/** A roster driver written by hand, with no `onRosterLapse`. */
class HandRolledRosterDriver implements BroadcastDriver {
    readonly #handlers: Array<(message: BroadcastMessage) => void> = []
    readonly #roster = new Map<string, Map<string, PresenceMember>>()

    publish(message: BroadcastMessage): void {
        for (const handler of this.#handlers) handler(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.#handlers.push(handler)
    }

    holdMember(channel: string, member: PresenceMember): RosterHold {
        let slots = this.#roster.get(channel)
        if (!slots) this.#roster.set(channel, slots = new Map())
        const arrived = !slots.has(String(member.id))
        slots.set(String(member.id), member)
        return { arrived }
    }

    releaseMember(channel: string, id: string | number): RosterRelease {
        return { gone: this.#roster.get(channel)?.delete(String(id)) ?? false }
    }

    readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow | Promise<RosterWindow> {
        return asWindow(
            [...(this.#roster.get(channel)?.values() ?? [])],
            limit,
            selfIds,
        )
    }
}

/** A driver with no roster and no control plane. */
class RosterlessDriver implements BroadcastDriver {
    readonly #handlers: Array<(message: BroadcastMessage) => void> = []

    publish(message: BroadcastMessage): void {
        for (const handler of this.#handlers) handler(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.#handlers.push(handler)
    }
}

for (
    const [label, make] of [
        ['the memory driver', () => new MemoryBroadcastDriver()],
        ['a roster-less driver', () => new RosterlessDriver()],
        ['a hand-rolled roster driver', () => new HandRolledRosterDriver()],
    ] as const
) {
    Deno.test(`#349 W10 ${label} without onRosterLapse builds, and a join/leave round is unchanged`, async () => {
        const driver: BroadcastDriver = make()
        assertEquals('onRosterLapse' in driver, false, 'no hook')
        const manager = new ChannelManager<User>({ driver, authorize })
        const observer = conn('observer', 1)
        manager.register(observer)
        const tab = conn('tab', 7)
        manager.register(tab)
        const second = conn('tab-2', 7)
        manager.register(second)
        await manager.subscribe(observer, CHANNEL)
        await manager.subscribe(tab, CHANNEL)
        await manager.subscribe(second, CHANNEL)
        await manager.unsubscribe('tab', CHANNEL)
        await manager.unsubscribe('tab-2', CHANNEL)
        await settle()
        assertEquals(actions(observer, 7), ['joined', 'left'])
        assertEquals(actions(tab, 7), [], 'no self-frame')
        assertEquals(actions(second, 7), [], 'no self-frame')
    })
}
