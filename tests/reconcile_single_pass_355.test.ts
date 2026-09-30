/**
 * @fileoverview #355 — one Redis reconcile pass at a time, a sweep that writes
 * only while its target is dead, and a count of what it actually removed.
 *
 * ## The sweep clock under the self-re-arming timer (T001, plan A6)
 *
 * The ghost sweep used to run on a `setInterval`; since #355 it re-arms one
 * `setTimeout` from the end of each pass (`#armReconcile`). A FakeTime
 * `tickAsync(ms)` drains microtasks, THEN advances the clock synchronously,
 * firing every due timer callback without awaiting the promise it returns.
 * So one `tickAsync(k × interval)` used to fire k overlapping passes and now
 * fires ONE: the next timer is armed only when that pass settles, after the
 * tick has returned.
 *
 * Every pass's round trips run after the tick returns, so they read the
 * broker at the ADVANCED clock: the one pass fired at the first interval
 * already sees a liveness key that lapsed later in the tick. That is why a
 * `lapse()` of `tickAsync(3_500)` still sweeps with one pass.
 *
 * The 12 files that drive the sweep clock, and what the change did to each
 * (baseline: all 12 green before, all 12 green after, no file edited):
 *
 * | File | Relies on | Changes? |
 * | :--- | :--- | :--- |
 * | `driver_redis.test.ts` | `tickAsync(3_500)` counting three REVOCATION ticks | no at #355 — the revocation timer was still an interval; since #359 it is a one-shot re-armed from each pass's end too, and the test counts its three ticks over three drained `tickAsync(1_000)` steps |
 * | `eviction_durable.test.ts` | `tickAsync(1_200)`, one revocation tick (the sweep fires once in both designs) | no |
 * | `eviction_reconnect.test.ts` | the reconnect trigger and one revocation tick | no |
 * | `live_fake_conformance.test.ts` | real clock; its holder pair only holds and releases, never waits on a sweep | no |
 * | `live_realtime.ts` | a helper passing `reconcileIntervalMs` through (default 60 s) | no |
 * | `presence_member_frozen_354.test.ts` | `untilFaked`, 100 ms steps until a condition — never a pass count | no |
 * | `presence_sweep_departure_348.test.ts` | `lapse()` = `tickAsync(3_500)`: ONE pass suffices (it reads at 3.5 s); A6's `tickAsync(1_000)` fires the re-armed pass | no — A6's second half changes later for `close()`, not for the clock (FR-015) |
 * | `presence_sweep.test.ts` | `tickAsync(3_500)` + a microtask flush: one pass suffices | no |
 * | `redis_broker_integration.test.ts` | real clock, fixed waits of 4–5 intervals; the cadence is now interval + one pass, milliseconds on a local broker | no |
 * | `revocation_retry.test.ts` | the revocation timer and its one retry only | no |
 * | `roster_atomicity_323.test.ts` | `tickAsync(3_500)` + a flush: one pass suffices | no |
 * | `roster_holders_345.test.ts` | `lapse()` = `tickAsync(3_500)`: one pass suffices | no |
 *
 * No test relied on more than one sweep pass per tick, nor on two passes
 * overlapping — the overlap was a side effect the suite never asserted.
 *
 * ## The witnesses (plan §4)
 *
 * Every witness runs a real `RedisBroadcastDriver` "B" over one `FakeRedis`,
 * sweeping instances whose holds are planted exactly as `HOLD_MEMBER_SCRIPT`
 * writes them and which never set a liveness key — dead from the start. Most
 * run B behind #348's `serializedCommands`, so one exchange is in flight at a
 * time, as on the production client, and a gate can hold one reply.
 *
 * A gate holds a reply AFTER its command executed at the broker. "Renew
 * before the second release" is therefore staged as: gate the FIRST release,
 * renew while its reply is held, then deliver it — the sweep issues nothing in
 * between, so the renewal lands before the next write.
 *
 * W1, W2, W4, W5, W6, W6b and W7 were committed red; W3 is a guard, green
 * before and after. W1 and W4 (v) were red on 77465d86 and already green on
 * the scheduler commit, which is what closes them. W4 (vii) — the plan's
 * 2026-09-23 amendment: a sweep cut short by `close()` still logs what it
 * removed — was committed red on 2bf46541. W5 also asserts that no
 * deregistration is asked after the refused release: without it, a decoder
 * reading *refused* as *absent* (battery M16) swept on and still logged the
 * same "renewed" line, from the deregistration's own refusal.
 *
 * The review added four. WF (a sweep failing after N > 0 releases), W8 (the
 * heartbeat's write order, and its registration after a failed `SET`) and W9
 * (the heartbeat stays a plain `setInterval`) pin decisions that were already
 * right and had no default-gate witness: green on arrival, each proven by its
 * battery rows. WR — a revocation reconcile already failing when `close()`
 * starts must not arm its retry after `close()` resolves — was committed red.
 *
 * @module @lockness/realtime/tests/reconcile_single_pass_355
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { PresenceMember } from '../channel.ts'
import { type CommandFn, FakeRedis, serializedCommands } from './fake_redis.ts'

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
const DEAD2 = 'instance-dead-2'

/** The words of the one summary line a completed sweep logs. */
const RELEASED = 'hold(s) of dead instance'
/** The words of the one line a sweep cut short by a renewal logs. */
const RENEWED = 'renewed its liveness while being swept'
/** The words of the one line a sweep that threw logs. */
const FAILED = 'sweep of dead instance'

function driverB(redis: FakeRedis, command: CommandFn = redis.command) {
    return new RedisBroadcastDriver(
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
}

/** The instance id a driver tags its holds with. Test-only read of a private. */
const idOf = (d: RedisBroadcastDriver): string => d['instanceId']

const entry = (id: string, owner: string) =>
    JSON.stringify({ member: { id: Number(id) }, owner })

/** Write `owner`'s hold of `field` exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<void> {
    const value = entry(field, owner)
    await redis.command('HSET', HOLDERS_KEY(CHANNEL, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(CHANNEL), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${CHANNEL} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/**
 * An owned entry whose hold is already gone — released by another sweeper
 * between this one's owned-set read and its release: an *absent* release.
 */
async function plantReleased(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<void> {
    await redis.command('SADD', OWNED_KEY(owner), `${CHANNEL} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/** Renew `owner`'s liveness key — the lapsed instance is back. */
async function renew(redis: FakeRedis, owner = DEAD): Promise<void> {
    await redis.command('SET', ALIVE_KEY(owner), '1', 'EX', '30')
}

async function registered(redis: FakeRedis, id: string): Promise<boolean> {
    const reply = await redis.command('SMEMBERS', INSTANCES_KEY) as {
        value: { value: string }[]
    }
    return reply.value.some((m) => m.value === id)
}

/** Whether `owner` still holds `field`, and the slot is still shown. */
async function holds(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<boolean> {
    const holder = await redis.command(
        'HGET',
        HOLDERS_KEY(CHANNEL, field),
        owner,
    ) as { type: string }
    const shown = await redis.command(
        'HGET',
        PRESENCE_KEY(CHANNEL),
        field,
    ) as { type: string }
    return holder.type !== 'nil' && shown.type !== 'nil'
}

/** A sweep release of one of `owner`'s holds (it names a holders hash). */
const isRelease = (args: readonly string[], owner = DEAD) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(owner)) &&
    args.some((a) => a.startsWith(`${PREFIX}__holders:`))

/** The deregistration of `owner` (it names the instances set). */
const isDeregistration = (args: readonly string[], owner = DEAD) =>
    args[0] === 'EVAL' && args.includes(INSTANCES_KEY) &&
    args.includes(ALIVE_KEY(owner))

/** Run the microtask queue out. */
async function settle(times = 200): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
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

/**
 * B's command port: serialized as the production client is, and recording
 * every command B ISSUES, in issue order.
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

/** B with its departure handler recording each swept member id. */
function sweeper(redis: FakeRedis, command: CommandFn = redis.command) {
    const b = driverB(redis, command)
    const departures: string[] = []
    b.onRosterDeparture(({ member }: { member: PresenceMember }) =>
        void departures.push(String(member.id))
    )
    return { b, departures }
}

// --- US1: one pass at a time --------------------------------------------------

Deno.test('#355 W1 a slow broker gets one pass, not a pile-up: one owned-set read, no second pass, one departure and one line', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        // B holds something, so its reconcile pass runs.
        await b.holdMember(OTHER, { id: 9 })
        const slow = serial.hold((args) =>
            args[0] === 'SSCAN' && args[1] === OWNED_KEY(DEAD)
        )
        await time.tickAsync(1_000)
        await slow.reached
        // Two more reconcile intervals elapse while the pass waits on the
        // broker.
        await time.tickAsync(1_000)
        await time.tickAsync(1_000)
        await settle()

        const issued = (cmd: string, key: string) =>
            sent.filter((a) => a[0] === cmd && a[1] === key).length
        assertEquals(issued('SSCAN', OWNED_KEY(DEAD)), 1)
        assertEquals(
            issued('SMEMBERS', INSTANCES_KEY),
            1,
            'no second pass started beside the one in flight',
        )

        slow.release()
        await settle()
        assertEquals(departures, ['7'], 'one departure')
        assertEquals(warnings.having(RELEASED).length, 1, 'one line')
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US2: the log counts what was removed -------------------------------------

Deno.test('#355 W2 the line counts emptied + kept, never absent — and an all-absent sweep logs nothing', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { b, departures } = sweeper(redis)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7') // emptied: DEAD holds it alone
        await plantHold(redis, '8') // kept: B, live, holds it too
        await plantReleased(redis, '6') // absent: already released
        await plantReleased(redis, '5', DEAD2) // DEAD2: nothing to remove
        await b.holdMember(CHANNEL, { id: 8 })
        await time.tickAsync(1_000)
        await settle()

        assertEquals(warnings.having(RELEASED), [
            `realtime: released 2 hold(s) of dead instance ${DEAD} ` +
            '(1 emptied their slot)',
        ])
        assertEquals(departures, ['7'])
        assert(await holds(redis, '8', idOf(b)), 'the kept slot stays B’s')
        assertEquals(await registered(redis, DEAD), false)
        assertEquals(
            await registered(redis, DEAD2),
            false,
            'an instance with nothing left to remove is still deregistered',
        )
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- W3: a failed pass is followed by the next one (guard) ---------------------

Deno.test('#355 W3 the instance-set read rejects: one WARN, and the next interval’s pass sweeps', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    let refused = 0
    const flaky: CommandFn = (...args) => {
        if (
            args[0] === 'SMEMBERS' && args[1] === INSTANCES_KEY && refused === 0
        ) {
            refused++
            return Promise.reject(new Error('connection reset'))
        }
        return redis.command(...args)
    }
    const { b, departures } = sweeper(redis, flaky)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        await time.tickAsync(1_000)
        await settle()
        assertEquals(refused, 1, 'precondition: the first pass failed')
        assertEquals(warnings.having('roster reconcile failed').length, 1)
        assert(await holds(redis, '7'), 'nothing swept yet')

        await time.tickAsync(1_000)
        await settle()
        assertEquals(departures, ['7'], 'the next pass swept')
        assertEquals(await registered(redis, DEAD), false)
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US4: shutting down mid-sweep ---------------------------------------------

Deno.test('#355 W4 (i) close() mid-release waits for it, announces its departure, and issues no further release', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    try {
        await plantHold(redis, '7')
        await plantHold(redis, '8')
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await inFlight.reached

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await settle()
        assertEquals(closed, false, 'close() waits for the release in flight')
        inFlight.release()
        await closing
        await settle()

        assertEquals(
            departures.length,
            1,
            'the in-flight release’s departure is still announced',
        )
        assertEquals(
            sent.filter((a) => isRelease(a)).length,
            1,
            'no release is issued once close() has begun',
        )
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (ii) close() during the last release: no deregistration', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await inFlight.reached
        const closing = b.close()
        inFlight.release()
        await closing
        await settle()

        assertEquals(
            await registered(redis, DEAD),
            true,
            'a closing driver does not deregister',
        )
        assertEquals(departures, ['7'], 'the release completed')
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (iii) close() mid-sweep: a second dead instance is never read', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b } = sweeper(redis, command)
    try {
        // DEAD is registered first, so it is swept first.
        await plantHold(redis, '7')
        await plantHold(redis, '8', DEAD2)
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await inFlight.reached
        const closing = b.close()
        inFlight.release()
        await closing
        await settle()

        assertEquals(
            sent.filter((a) =>
                (a[0] === 'EXISTS' && a[1] === ALIVE_KEY(DEAD2)) ||
                (a[0] === 'SSCAN' && a[1] === OWNED_KEY(DEAD2))
            ),
            [],
            'no EXISTS and no owned-set read for the next instance',
        )
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (iv) once close() resolves, two intervals issue no command', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b } = sweeper(redis, command)
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await inFlight.reached
        let resolvedAt = -1
        const closing = b.close().then(() => void (resolvedAt = sent.length))
        inFlight.release()
        await closing
        await settle()
        await time.tickAsync(1_000)
        await time.tickAsync(1_000)
        await settle()

        assert(resolvedAt >= 0, 'precondition: close() resolved')
        assertEquals(
            sent.slice(resolvedAt),
            [],
            'no command once close() has resolved — no release, no ' +
                'deregistration, no re-armed pass',
        )
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (v) close() during the boot heartbeat: no heartbeat and no pass are armed after it', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b } = sweeper(redis, command)
    try {
        await plantHold(redis, '7')
        const alive = ALIVE_KEY(idOf(b))
        const boot = serial.hold((args) =>
            (args[0] === 'SET' && args[1] === alive) ||
            (args[0] === 'SADD' && args[1] === INSTANCES_KEY)
        )
        const holding = b.holdMember(OTHER, { id: 9 })
        await boot.reached
        const closing = b.close()
        boot.release()
        await closing
        // The hold itself and the rest of the beat already under way are
        // the caller's; what matters is what the clock arms after them.
        await holding
        await settle()
        const mark = sent.length
        await time.tickAsync(1_000)
        await time.tickAsync(1_000)
        await settle()

        assertEquals(sent.slice(mark), [], 'no heartbeat and no pass')
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (vi) a reconnect while close() awaits the pass runs no revocation handler and leaves no retry', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, command } = serialPort(redis)
    const subscriber = redis.subscriberFor()
    const b = new RedisBroadcastDriver({ command }, subscriber, {
        prefix: PREFIX,
        presence: {
            livenessTtlSeconds: 2,
            heartbeatIntervalMs: 500,
            reconcileIntervalMs: 1000,
        },
    })
    let ran = 0
    // Failing, so a reconnect-triggered run would arm its one retry.
    b.onRevocationReconcile(() => {
        ran++
        throw new Error('revocation store down')
    })
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const inFlight = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await inFlight.reached
        const before = ran

        const closing = b.close()
        await subscriber.fireReconnect()
        inFlight.release()
        await closing
        await settle()

        assertEquals(ran, before, 'the revocation handler did not run')
        assertEquals(
            b['revocationRetryTimer'],
            undefined,
            'no retry timer is pending after close()',
        )
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W4 (vii) a sweep cut short by close() still counts what it removed: one "released" line at N = 1, none at N = 0', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, command } = serialPort(redis)
    const { b } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await plantHold(redis, '8')
        await b.holdMember(OTHER, { id: 9 })
        // Cut short after ONE removal: close() while the first of two
        // releases is in flight — the loop's check stops the second.
        const first = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await first.reached
        const closing = b.close()
        first.release()
        await closing
        await settle()

        // A `closed` end at N = 1 leaves work behind: unfinished (#358).
        assertEquals(warnings.having(RELEASED), [
            `realtime: released 1 hold(s) of dead instance ${DEAD} ` +
            '(1 emptied their slot) — unfinished: it stays registered and a ' +
            'later pass resumes it',
        ])
        assertEquals(await registered(redis, DEAD), true, 'not deregistered')
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }

    // And cut short at the deregistration: close() during the LAST release.
    const redis2 = new FakeRedis()
    const time2 = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const port2 = serialPort(redis2)
    const b2 = sweeper(redis2, port2.command).b
    const warnings2 = captureWarnings()
    try {
        await plantHold(redis2, '7')
        await b2.holdMember(OTHER, { id: 9 })
        const last = port2.serial.hold((args) => isRelease(args))
        await time2.tickAsync(1_000)
        await last.reached
        const closing = b2.close()
        last.release()
        await closing
        await settle()

        // A `closed` end at N = 1 leaves work behind: unfinished (#358).
        assertEquals(warnings2.having(RELEASED), [
            `realtime: released 1 hold(s) of dead instance ${DEAD} ` +
            '(1 emptied their slot) — unfinished: it stays registered and a ' +
            'later pass resumes it',
        ])
    } finally {
        warnings2.restore()
        await b2.close()
        time2.restore()
        redis2.assertNoRejections()
    }

    // N = 0: close() before the first release — nothing removed, no line.
    const redis3 = new FakeRedis()
    const time3 = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const port3 = serialPort(redis3)
    const b3 = sweeper(redis3, port3.command).b
    const warnings3 = captureWarnings()
    try {
        await plantHold(redis3, '7')
        await b3.holdMember(OTHER, { id: 9 })
        const read = port3.serial.hold((args) =>
            args[0] === 'SSCAN' && args[1] === OWNED_KEY(DEAD)
        )
        await time3.tickAsync(1_000)
        await read.reached
        const closing = b3.close()
        read.release()
        await closing
        await settle()

        assert(await holds(redis3, '7'), 'precondition: nothing released')
        assertEquals(warnings3.having(RELEASED), [], 'no line at N = 0')
    } finally {
        warnings3.restore()
        await b3.close()
        time3.restore()
        redis3.assertNoRejections()
    }
})

Deno.test('#355 WR a revocation reconcile already failing when close() starts arms no retry after it', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const subscriber = redis.subscriberFor()
    const b = new RedisBroadcastDriver({ command: redis.command }, subscriber, {
        prefix: PREFIX,
        presence: { reconcileIntervalMs: 1000 },
    })
    let fail!: (error: Error) => void
    let ran = 0
    b.onRevocationReconcile(() => {
        ran++
        return new Promise<void>((_, reject) => (fail = reject))
    })
    const warnings = captureWarnings()
    try {
        // The reconnect trigger — the one that earns a retry — is in flight.
        const reconnect = subscriber.fireReconnect()
        await settle()
        assertEquals(ran, 1, 'precondition: the reconnect run started')

        await b.close()
        fail(new Error('revocation store down'))
        await reconnect
        await settle()

        assertEquals(
            warnings.having('revocation reconcile failed (reconnect)').length,
            1,
            'the failure is still logged',
        )
        assertEquals(
            b['revocationRetryTimer'],
            undefined,
            'no retry is armed once close() has resolved',
        )
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US3: a lapsed instance that renews stops being swept ---------------------

Deno.test('#355 W5 a renewal mid-sweep refuses the next release: B stops, the slot stays, one left, one "renewed" line', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await plantHold(redis, '8')
        await b.holdMember(OTHER, { id: 9 })
        const first = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await first.reached
        // A renews while the first release's reply is in flight — before the
        // sweep can issue its second.
        await renew(redis)
        first.release()
        await settle()

        assertEquals(departures.length, 1, 'one left in total')
        const kept = departures[0] === '7' ? '8' : '7'
        assert(
            await holds(redis, kept),
            `the slot not yet released (${kept}) is still A’s`,
        )
        assertEquals(await registered(redis, DEAD), true, 'A stays registered')
        assertEquals(
            sent.filter((a) => isDeregistration(a)),
            [],
            'the refused release stops the sweep: no deregistration is asked',
        )
        assertEquals(warnings.having(RENEWED), [
            `realtime: instance ${DEAD} renewed its liveness while being ` +
            'swept — a lapse, not a crash; 1 hold(s) released (1 emptied) ' +
            'before it did',
        ])
        assertEquals(warnings.having(RELEASED), [], 'no "released" line')
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W6 a renewal after the last release: the deregistration is refused, A stays registered', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const last = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await last.reached
        // Every release is done; A renews before the deregistration.
        await renew(redis)
        last.release()
        await settle()

        assertEquals(departures, ['7'], 'precondition: the release completed')
        assertEquals(await registered(redis, DEAD), true, 'A stays registered')
        assertEquals(warnings.having(RENEWED), [
            `realtime: instance ${DEAD} renewed its liveness while being ` +
            'swept — a lapse, not a crash; 1 hold(s) released (1 emptied) ' +
            'before it did',
        ])
        assertEquals(warnings.having(RELEASED), [])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W6b a late hold of a still-lapsed A keeps it registered; the next pass releases it and deregisters A', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, command } = serialPort(redis)
    const { b, departures } = sweeper(redis, command)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const last = serial.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await last.reached
        // A, still lapsed, holds 8 after the sweep read its owned set.
        await plantHold(redis, '8')
        last.release()
        await settle()

        assertEquals(
            await registered(redis, DEAD),
            true,
            'A owns a hold, so it is not deregistered',
        )
        assert(await holds(redis, '8'), 'the late hold is untouched')
        assertEquals(warnings.having(RENEWED), [], 'a late hold is no renewal')

        await time.tickAsync(1_000)
        await settle()
        assertEquals(departures, ['7', '8'], 'the next pass released it')
        assertEquals(await registered(redis, DEAD), false)
        assertEquals(warnings.having(RENEWED), [])
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- US5: one bad instance does not starve the others -------------------------

Deno.test('#355 W7 the first dead instance’s owned set cannot be read: one "failed" line, and the second is swept in the same pass', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    let refused = 0
    const flaky: CommandFn = (...args) => {
        if (
            args[0] === 'SSCAN' && args[1] === OWNED_KEY(DEAD) &&
            refused === 0
        ) {
            refused++
            return Promise.reject(new Error('connection reset'))
        }
        return redis.command(...args)
    }
    const { b, departures } = sweeper(redis, flaky)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await plantHold(redis, '8', DEAD2)
        await b.holdMember(OTHER, { id: 9 })
        await time.tickAsync(1_000)
        await settle()

        assertEquals(refused, 1, 'precondition: the owned-set read failed')
        const failed = warnings.having(FAILED)
        assertEquals(failed.length, 1, failed.join('\n'))
        assert(
            failed[0].startsWith(
                `realtime: sweep of dead instance ${DEAD} failed after 0 ` +
                    'hold(s) released (0 emptied): ',
            ),
            failed[0],
        )
        assert(failed[0].includes('connection reset'), failed[0])
        assertEquals(warnings.having('roster reconcile failed'), [])
        assertEquals(await registered(redis, DEAD), true, 'retried next pass')
        assert(await holds(redis, '7'))
        assertEquals(departures, ['8'], 'the second instance was swept')
        assertEquals(await registered(redis, DEAD2), false)
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 WF a sweep failing after one release: one "failed" line counting it, no further release and no deregistration', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const sent: string[][] = []
    let releases = 0
    const port: CommandFn = async (...args) => {
        sent.push(args)
        const reply = await redis.command(...args)
        // The second release runs at the broker; its reply does not decode.
        if (isRelease(args) && ++releases === 2) {
            return { type: 'array', value: [] }
        }
        return reply
    }
    const { b, departures } = sweeper(redis, port)
    const warnings = captureWarnings()
    try {
        await plantHold(redis, '7')
        await plantHold(redis, '8')
        await plantHold(redis, '6')
        await b.holdMember(OTHER, { id: 9 })
        await time.tickAsync(1_000)
        await settle()

        const failed = warnings.having(FAILED)
        assertEquals(failed.length, 1, failed.join('\n'))
        assert(
            failed[0].startsWith(
                `realtime: sweep of dead instance ${DEAD} failed after 1 ` +
                    'hold(s) released (1 emptied): ',
            ),
            failed[0],
        )
        assert(failed[0].includes('none of its four replies'), failed[0])
        assertEquals(warnings.having(RELEASED), [])
        assertEquals(warnings.having(RENEWED), [])
        assertEquals(
            sent.filter((a) => isRelease(a)).length,
            2,
            'no release after the one that failed',
        )
        assertEquals(sent.filter((a) => isDeregistration(a)), [])
        assertEquals(await registered(redis, DEAD), true, 'retried next pass')
        assertEquals(departures.length, 1, 'the first release was announced')
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- the heartbeat -------------------------------------------------------------

Deno.test('#355 W8 the heartbeat writes the liveness key before it registers, and registers even when that write failed', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const beat: string[] = []
    let alive = ''
    let refused = 0
    const port: CommandFn = (...args) => {
        if (args[0] === 'SET' && args[1] === alive) {
            beat.push('SET')
            if (refused++ === 0) {
                return Promise.reject(new Error('connection reset'))
            }
        }
        if (args[0] === 'SADD' && args[1] === INSTANCES_KEY) beat.push('SADD')
        return redis.command(...args)
    }
    const b = driverB(redis, port)
    alive = ALIVE_KEY(idOf(b))
    const warnings = captureWarnings()
    try {
        await b.holdMember(OTHER, { id: 9 })
        assertEquals(
            beat,
            ['SET', 'SADD'],
            'the boot beat: the liveness key first, then the registration, ' +
                'attempted although the first write failed',
        )
        assertEquals(warnings.having('heartbeat failed').length, 1)

        await time.tickAsync(500)
        await settle()
        assertEquals(beat, ['SET', 'SADD', 'SET', 'SADD'], 'the next beat')
        assertEquals(warnings.having('heartbeat failed').length, 1)
    } finally {
        warnings.restore()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 W9 the heartbeat stays a plain setInterval: a beat stuck on the broker does not hold back the next one', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const { serial, sent, command } = serialPort(redis)
    const b = driverB(redis, command)
    const alive = ALIVE_KEY(idOf(b))
    const beats = () =>
        sent.filter((a) => a[0] === 'SET' && a[1] === alive).length
    let unstick = () => {}
    try {
        await b.holdMember(OTHER, { id: 9 })
        const stuck = serial.hold((args) =>
            args[0] === 'SET' && args[1] === alive
        )
        unstick = stuck.release
        await time.tickAsync(500)
        await stuck.reached
        const issued = beats()

        await time.tickAsync(500)
        await settle()
        assertEquals(
            beats(),
            issued + 1,
            'the next beat is issued while the last one waits on the broker — ' +
                'an overlapping beat is a harmless repeat renewal, a skipped ' +
                'one a lapse',
        )
    } finally {
        unstick()
        await settle()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})
