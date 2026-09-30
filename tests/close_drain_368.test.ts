/**
 * @fileoverview #368 — `close()` waits at most one liveness TTL for the work
 * it has in flight (the ghost-sweep pass, then the lapse run, then the
 * roster-maintenance drain — #349's and #371's order), then logs one WARN and
 * carries on with the rest of its teardown, whether or not any of the three
 * has settled.
 *
 * W1–W3, W7 and W9 drive one `RedisBroadcastDriver` over a `FakeRedis` behind
 * a serialised command port — one exchange in flight, as on the production
 * client — with FakeTime. A gate held and never released is a command that
 * never settles: none of these witnesses awaits a stalled `close()` without
 * first bounding the wait with FakeTime ticks, so a mutant that removes the
 * budget cannot hang the battery — the assertion after a bounded number of
 * ticks simply reads `closed` as still `false`.
 *
 * W4 exercises `awaitCloseDrain` directly, a fast unit check of its own
 * contract (the timer is cleared, the result names nothing pending) that a
 * driver-level test cannot isolate from the driver's OTHER timers. W8 spawns
 * `fixtures/close_drain_ref_probe.ts` as its own process, because ref and
 * unref only diverge when nothing else keeps the event loop alive AND
 * nothing at the top level is still awaiting the call — measured, not
 * assumed: a same-process test that awaits `awaitCloseDrain` directly
 * settles a ref'd OR an unref'd timer alike, so it cannot tell them apart.
 *
 * @module @lockness/realtime/tests/close_drain_368
 */

import { assert, assertEquals, assertStringIncludes } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    CLOSE_DRAIN_EXPIRED,
    CLOSE_LOG_FAILED,
    RedisBroadcastDriver,
} from '../drivers/redis.ts'
import { awaitCloseDrain } from '../drivers/close_drain.ts'
import type { PresenceMember } from '../channel.ts'
import { type CommandFn, FakeRedis, serializedCommands } from './fake_redis.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string) =>
    `${PREFIX}__holders:${channel} ${id}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const INSTANCES_KEY = `${PREFIX}__instances`

const DEAD = 'instance-dead'

const START = new Date('2026-09-25T10:00:00Z')

/** Build a driver over `command`, with a small TTL so a witness stays fast. */
function driverC(
    redis: FakeRedis,
    command: CommandFn = redis.command,
    overrides: {
        livenessTtlSeconds?: number
        heartbeatIntervalMs?: number
        reconcileIntervalMs?: number
    } = {},
): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            presence: {
                livenessTtlSeconds: overrides.livenessTtlSeconds ?? 3,
                heartbeatIntervalMs: overrides.heartbeatIntervalMs ?? 1_000,
                reconcileIntervalMs: overrides.reconcileIntervalMs ?? 1_000,
            },
        },
    )
}

/** Write `owner`'s hold of `field` exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(
    redis: FakeRedis,
    field: string,
    owner = DEAD,
): Promise<void> {
    const value = JSON.stringify({ member: { id: Number(field) }, owner })
    await redis.command('HSET', HOLDERS_KEY(CHANNEL, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(CHANNEL), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${CHANNEL} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/** A sweep release of `owner`'s hold (it names a holders hash). */
const isRelease = (args: readonly string[], owner = DEAD) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(owner)) &&
    args.some((a) => a.startsWith(`${PREFIX}__holders:`))

/** The one owned-set-of-live-instances read the ghost sweep issues first. */
const isSmembers = (args: readonly string[]) =>
    args[0] === 'SMEMBERS' && args[1] === INSTANCES_KEY

/** The heartbeat's liveness write. */
const isBeat = (args: readonly string[]) =>
    args[0] === 'SET' && args.includes('GET')

/** Run the microtask queue out. */
async function settle(times = 200): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

/**
 * A serialising command port (one exchange in flight, as on the production
 * client) that can also force the NEXT heartbeat write to answer `nil` —
 * simulating a lapse without touching the fake broker's real TTLs.
 */
function lapsingPort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    let forceLapseNext = false
    const command: CommandFn = (...args) => {
        if (forceLapseNext && isBeat(args)) {
            forceLapseNext = false
            return serial.command(...args).then(() => ({ type: 'nil' }))
        }
        return serial.command(...args)
    }
    return {
        command,
        hold: serial.hold,
        forceLapse: () => void (forceLapseNext = true),
    }
}

/** Collect `console.warn` / `console.error` lines; `warn` can be made to throw. */
function captureLogs() {
    const warn = console.warn
    const error = console.error
    const warns: string[] = []
    const errors: string[] = []
    let warnFails = false
    console.warn = (...parts: unknown[]) => {
        if (warnFails) throw new Error('warn sink down (#368)')
        warns.push(parts.join(' '))
    }
    console.error = (...parts: unknown[]) => void errors.push(parts.join(' '))
    return {
        warns,
        errors,
        failWarn: (on: boolean) => void (warnFails = on),
        restore: () => {
            console.warn = warn
            console.error = error
        },
    }
}

// ---------------------------------------------------------------------------
// W1 — the sweep pass alone is stalled
// ---------------------------------------------------------------------------

Deno.test('#368 W1 a stalled sweep release: close() waits until the TTL, logs one WARN naming the sweep pass, and the late reply announces nothing', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    const departures: string[] = []
    b.onRosterDeparture(({ member }: { member: PresenceMember }) =>
        void departures.push(String(member.id))
    )
    const logs = captureLogs()
    // Released in `finally` too, unconditionally: an assertion failure below
    // must not leave this gate held forever, which a SECOND close() call
    // would then wait a full budget on all over again.
    let release: ReturnType<typeof port.hold> | undefined
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        release = port.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await release.reached

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(2_999)
        await settle()
        assertEquals(closed, false, 'close() still waits at TTL - 1 ms')
        await time.tickAsync(1)
        await settle()
        assertEquals(closed, true, 'close() resolves at the TTL')
        await closing

        assertEquals(logs.warns.length, 1, 'exactly one WARN')
        assertStringIncludes(logs.warns[0], CLOSE_DRAIN_EXPIRED)
        assertStringIncludes(logs.warns[0], 'the ghost sweep pass')
        assertStringIncludes(
            logs.warns[0],
            '(age ',
            'the sweep pass age, read from the still-live #sweepPass record',
        )
        assert(
            !logs.warns[0].includes('the lapse run'),
            'the lapse run was healthy — not named',
        )

        release.release()
        await settle()
        assertEquals(
            departures,
            [],
            'the departure handler was already dropped',
        )
    } finally {
        // Not a second close(): the pass may still be stalled if an
        // assertion above threw before the release, and close() would then
        // arm a second full budget waiting for it.
        logs.restore()
        release?.release()
        await settle()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#368 W1b no further sweep command is issued, even after the reconcile interval elapses', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const sent: string[][] = []
    const recording: CommandFn = (...args) => {
        sent.push(args)
        return port.command(...args)
    }
    const b = driverC(redis, recording, { livenessTtlSeconds: 3 })
    const logs = captureLogs()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const release = port.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await release.reached

        const closing = b.close()
        await time.tickAsync(3_000)
        await settle()
        await closing
        const sentAtClose = sent.length

        await time.tickAsync(1_000)
        await settle()
        assertEquals(
            sent.slice(sentAtClose),
            [],
            'no command once close() has resolved',
        )
        // Never truly settles: nothing may await this again once close() has
        // already returned. Released only so the fake broker holds nothing
        // dangling once the test ends.
        release.release()
        await settle()
    } finally {
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W2 — the lapse run alone is stalled
// ---------------------------------------------------------------------------

Deno.test('#368 W2 a stalled lapse run: close() waits until the TTL and logs one WARN naming the lapse run', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    b.onRosterLapse(() => new Promise<void>(() => {}))
    const logs = captureLogs()
    try {
        await b.holdMember(OTHER, { id: 9 })
        port.forceLapse()
        await time.tickAsync(1_000)
        await settle()

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(2_999)
        await settle()
        assertEquals(closed, false, 'close() still waits at TTL - 1 ms')
        await time.tickAsync(1)
        await settle()
        assertEquals(closed, true, 'close() resolves at the TTL')
        await closing

        assertEquals(logs.warns.length, 1, 'exactly one WARN')
        assertStringIncludes(logs.warns[0], CLOSE_DRAIN_EXPIRED)
        assertStringIncludes(logs.warns[0], 'the lapse run')
        assert(
            !logs.warns[0].includes('the ghost sweep pass'),
            'the sweep pass was healthy — not named',
        )
    } finally {
        // Not closed a second time: the lapse handler above never settles,
        // and close() would arm a second full budget waiting for it.
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W3 — both are stalled: one budget, not two
// ---------------------------------------------------------------------------

Deno.test('#368 W3 both the sweep pass and the lapse run stalled: close() resolves at the TTL, not twice it', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    // Reconcile fires strictly AFTER the heartbeat's own tick (never in the
    // same `tickAsync`), so the sweep's SMEMBERS is never queued ahead of the
    // heartbeat's SET on the one shared serial tail — which would starve the
    // heartbeat behind a gate this test holds forever.
    const b = driverC(redis, port.command, {
        livenessTtlSeconds: 3,
        reconcileIntervalMs: 2_000,
    })
    b.onRosterLapse(() => new Promise<void>(() => {}))
    const logs = captureLogs()
    const releases: Array<() => void> = []
    try {
        await b.holdMember(OTHER, { id: 9 })
        port.forceLapse()
        await time.tickAsync(1_000)
        await settle()

        const smembers = port.hold((args) => isSmembers(args))
        releases.push(smembers.release)
        await time.tickAsync(1_000)
        await settle()
        await smembers.reached

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(2_999)
        await settle()
        assertEquals(closed, false, 'still pending at TTL - 1 ms')
        await time.tickAsync(1)
        await settle()
        assertEquals(closed, true, 'resolves at ONE TTL, not two')
        await closing

        assertEquals(logs.warns.length, 1, 'exactly one WARN')
        assertStringIncludes(logs.warns[0], 'the ghost sweep pass')
        assertStringIncludes(logs.warns[0], 'the lapse run')
    } finally {
        // Not closed a second time: the lapse handler above never settles,
        // and close() would arm a second full budget waiting for it.
        logs.restore()
        for (const release of releases) release()
        await settle()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W4 — awaitCloseDrain's own contract, isolated from the driver's timers
// ---------------------------------------------------------------------------

Deno.test('#368 W4 (i) a healthy drain resolves with nothing pending and clears the timer it armed', async () => {
    const realSetTimeout = globalThis.setTimeout
    const realClearTimeout = globalThis.clearTimeout
    const created: ReturnType<typeof setTimeout>[] = []
    const cleared: Parameters<typeof clearTimeout>[0][] = []
    globalThis.setTimeout = ((...args: Parameters<typeof setTimeout>) => {
        const id = realSetTimeout(...args)
        created.push(id)
        return id
    }) as typeof setTimeout
    globalThis.clearTimeout = (
        (...args: Parameters<typeof clearTimeout>) => {
            cleared.push(args[0])
            return realClearTimeout(...args)
        }
    ) as typeof clearTimeout
    try {
        const pending = await awaitCloseDrain(
            5_000,
            Promise.resolve(),
            Promise.resolve(),
            Promise.resolve(),
        )
        assertEquals(pending, {
            sweepPass: false,
            lapseRun: false,
            maintenanceDrain: false,
        })
        assertEquals(created.length, 1, 'one timer armed')
        assertEquals(cleared, created, 'the same timer was cleared')
    } finally {
        globalThis.setTimeout = realSetTimeout
        globalThis.clearTimeout = realClearTimeout
    }
})

Deno.test('#368 W4 (ii) a healthy close() over a real driver writes no WARN', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    const logs = captureLogs()
    try {
        await b.holdMember(OTHER, { id: 9 })
        await b.close()
        assertEquals(logs.warns, [], 'no WARN from a clean close()')
    } finally {
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W5 — the owned connections still close after expiry
// ---------------------------------------------------------------------------

Deno.test('#368 W5 the owned connections are closed even though the sweep pass never settled', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    const logs = captureLogs()
    let closedCount = 0
    Object.assign(b, {
        owned: [
            { close: () => void closedCount++ },
            { close: () => void closedCount++ },
        ],
    })
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const release = port.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await release.reached

        const closing = b.close()
        await time.tickAsync(3_000)
        await settle()
        await closing

        assertEquals(closedCount, 2, 'both owned resources were closed')
        release.release()
        await settle()
    } finally {
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W6 — a throwing console.warn falls back to the marked line
// ---------------------------------------------------------------------------

Deno.test('#368 W6 a throwing console.warn produces one CLOSE_LOG_FAILED line, and teardown still finishes', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    const logs = captureLogs()
    let closedCount = 0
    Object.assign(b, { owned: [{ close: () => void closedCount++ }] })
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const release = port.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await release.reached

        logs.failWarn(true)
        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(3_000)
        await settle()
        logs.failWarn(false)
        assertEquals(closed, true, 'close() still resolves')
        await closing

        assertEquals(logs.warns, [], 'the WARN sink refused it')
        const marked = logs.errors.filter((l) => l.startsWith(CLOSE_LOG_FAILED))
        assertEquals(marked.length, 1, 'one marked fallback line')
        assertStringIncludes(marked[0], 'warn sink down')
        assertEquals(closedCount, 1, 'the owned connection still closed')

        release.release()
        await settle()
    } finally {
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W7 — the budget follows the configured liveness TTL
// ---------------------------------------------------------------------------

Deno.test('#368 W7 a liveness TTL of 2 s gives a budget of 2 s, not the default 3 s', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, {
        livenessTtlSeconds: 2,
        heartbeatIntervalMs: 500,
    })
    const logs = captureLogs()
    try {
        await plantHold(redis, '7')
        await b.holdMember(OTHER, { id: 9 })
        const release = port.hold((args) => isRelease(args))
        await time.tickAsync(1_000)
        await release.reached

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(1_999)
        await settle()
        assertEquals(closed, false, 'still pending at 1999 ms')
        await time.tickAsync(1)
        await settle()
        assertEquals(closed, true, 'resolves at 2000 ms — the 2 s TTL')
        await closing
        assertStringIncludes(logs.warns[0], '2000ms')

        release.release()
        await settle()
    } finally {
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})

// ---------------------------------------------------------------------------
// W8 — real timers: the drain's timer is ref'd, or awaitCloseDrain never
// returns
// ---------------------------------------------------------------------------

Deno.test("#368 W8 the drain's timer is ref'd: a caller that does not await close() still sees it settle", async () => {
    // Measured (#368 grooming): a directly `await`ed call settles a ref'd OR
    // an unref'd timer alike, because the top-level module has not finished
    // either way — a FakeTime test, and a same-process real-timer test that
    // awaits the call directly, cannot tell them apart. The divergence needs
    // the shape a caller that does not hold the process open on `close()`
    // actually has, so this spawns the fixture as its OWN process: nothing
    // else runs there, and `run()` inside it is deliberately not awaited. A
    // ref'd timer still lets Deno finish it and print `settled`; an unref'd
    // one leaves Deno nothing to wait for once the top level returns, and it
    // never prints.
    const probe = new URL(
        './fixtures/close_drain_ref_probe.ts',
        import.meta.url,
    )
    const run = await new Deno.Command(Deno.execPath(), {
        args: ['run', '--allow-all', probe.pathname],
        stdout: 'piped',
        stderr: 'piped',
    }).output()
    const out = new TextDecoder().decode(run.stdout)
    const err = new TextDecoder().decode(run.stderr)
    assertEquals(run.code, 0, `the probe process exited clean: ${err}`)
    assertStringIncludes(out, 'main returned')
    assertStringIncludes(
        out,
        'settled',
        "the ref'd timer let the unawaited call finish and print",
    )
})

// ---------------------------------------------------------------------------
// W9 — the roster-maintenance drain alone is stalled (#371)
// ---------------------------------------------------------------------------

Deno.test('#368 W9 a stalled roster-maintenance drain: close() waits until the TTL and logs one WARN naming the drain', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(START)
    const port = lapsingPort(redis)
    const b = driverC(redis, port.command, { livenessTtlSeconds: 3 })
    b.onRosterMaintenance(() => new Promise<void>(() => {}))
    const logs = captureLogs()
    try {
        // The boot beat already fires the maintenance trigger (#371's own
        // heartbeat tail, unconditional on any successful decode) — unlike
        // the lapse run, no forced nil reply is needed.
        await b.holdMember(OTHER, { id: 9 })

        let closed = false
        const closing = b.close().then(() => void (closed = true))
        await time.tickAsync(2_999)
        await settle()
        assertEquals(closed, false, 'close() still waits at TTL - 1 ms')
        await time.tickAsync(1)
        await settle()
        assertEquals(closed, true, 'close() resolves at the TTL')
        await closing

        assertEquals(logs.warns.length, 1, 'exactly one WARN')
        assertStringIncludes(logs.warns[0], CLOSE_DRAIN_EXPIRED)
        assertStringIncludes(logs.warns[0], 'the owed-release drain')
        assert(
            !logs.warns[0].includes('the ghost sweep pass'),
            'no sweep pass ever started — not named',
        )
        assert(
            !logs.warns[0].includes('the lapse run'),
            'the lapse run was healthy — not named',
        )
    } finally {
        // Not closed a second time: the maintenance handler above never
        // settles, and close() would arm a second full budget waiting for
        // it.
        logs.restore()
        time.restore()
        redis.assertNoRejections()
    }
})
