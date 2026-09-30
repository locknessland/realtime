/**
 * @fileoverview #362 — the Redis revocation pass is bounded: a timing the
 * driver cannot enforce refuses to boot, and a broken enforcement guarantee
 * is never silent.
 *
 * **B witnesses** construct a driver and read WHICH refusal fired: the range
 * guard (`out of range`) or the relation (`at most HALF`). Several inputs are
 * refused by both (an infinite interval breaks the relation too), so a
 * witness that only asserted "it throws" would let the guard's own clauses be
 * dropped unseen (plan D5).
 *
 * **D witnesses** drive one `RedisBroadcastDriver` over a FakeRedis behind a
 * serialising command port — one exchange in flight, as on the production
 * client — with the revocation re-check registered DIRECTLY on the driver
 * (`driver.listRevocations()`), not through a manager: behind a manager a
 * second pass would queue on the manager's serial tail and never reach the
 * port, which would hide a freed pass slot (plan D10). The port records every
 * command AT ISSUE, before its serial queue, so a second pass's reap is
 * visible while it waits behind a held one.
 *
 * - `performance.now` is pointed at FakeTime's clock, because FakeTime does
 *   not fake it, and restored afterwards;
 * - the fake broker's `TIME` is pinned with `FakeRedis.setTime`, so only the
 *   witness that means to move it does (D6, D7);
 * - `console.warn` and `console.error` are captured, and a line counts when it
 *   STARTS with one of the deadline constants or the marked fallback prefix.
 *
 * FakeTime fires every timer due within one `tickAsync` synchronously, with
 * no microtask between them, so time is advanced in short steps with the
 * microtask queue drained after each ({@link advance}): a pass armed from the
 * end of the previous one then runs at its own instant.
 *
 * @module @lockness/realtime/tests/revocation_pass_bound_362
 */

import {
    assert,
    assertEquals,
    assertStringIncludes,
    assertThrows,
} from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import {
    EnforcementDeadline,
    REVOCATION_DEADLINE_MISSED,
    REVOCATION_DEADLINE_SKEWED,
    REVOCATION_DEADLINE_STALLED,
    REVOCATION_LOG_FAILED,
} from '../drivers/enforcement_deadline.ts'
import {
    type CommandFn,
    type CommandGate,
    type CommandMatch,
    FakeRedis,
    serializedCommands,
} from './fake_redis.ts'
import { isReap as isReapOf } from './revocation_wire.ts'
import { watchingEscapes } from './escape_watcher.ts'

/**
 * The real `setTimeout`, captured before any FakeTime exists. D6 drains on it
 * because FakeTime's `runMicrotasks` goes through `restoreFor`, which resets
 * `globalThis.Date` — and with it the wall-clock step D6 installs.
 */
const REAL_SET_TIMEOUT = globalThis.setTimeout

const START = new Date('2026-09-24T10:00:00Z')
/** The fake broker's `TIME`, pinned. */
const NOW_S = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
/** The revocation TTL of every D witness, in seconds. */
const TTL = 10

/** The guard's distinct fragment (FR-005). */
const GUARD = 'out of range'
/** The relation's distinct fragment (FR-005). */
const RELATION = 'at most HALF'

const DEADLINE = [
    REVOCATION_DEADLINE_STALLED,
    REVOCATION_DEADLINE_MISSED,
    REVOCATION_DEADLINE_SKEWED,
]

// ---------------------------------------------------------------------------
// B — the boot check
// ---------------------------------------------------------------------------

/** A subscriber that never delivers: construction is all a B witness needs. */
const quietSubscriber = { psubscribe: () => {} }

/** Construct a driver with this timing; `undefined` keeps the default. */
function build(timing: { interval?: number; ttl?: number }) {
    return new RedisBroadcastDriver(
        { command: () => Promise.resolve(null) },
        quietSubscriber,
        {
            prefix: PREFIX,
            revocationTtlSeconds: timing.ttl,
            presence: { reconcileIntervalMs: timing.interval },
        },
    )
}

/** The largest TTL one timer can hold, derived here, never a literal. */
const MAX_TTL_SECONDS = Math.floor((2 ** 31 - 1) / 1000)

/**
 * The message of the refusal `timing` raises, after checking it is a PLAIN
 * `Error` (FR-005: no subclass, not a `RangeError`) carrying every part its
 * kind must carry. Fails when the driver constructs.
 */
function refused(
    kind: 'guard' | 'relation',
    timing: { interval?: number; ttl?: number },
): string {
    const error = assertThrows(() => build(timing), Error)
    assertEquals(error.constructor, Error, 'a plain Error')
    const message = error.message
    assertStringIncludes(message, '#362')
    if (kind === 'guard') {
        assertStringIncludes(message, GUARD)
        assertStringIncludes(message, 'at least 1 ms')
        assertStringIncludes(message, `from 1 to ${MAX_TTL_SECONDS}`)
        assertStringIncludes(message, 'back to back')
    } else {
        assertStringIncludes(message, RELATION)
        assert(!message.includes(GUARD), message)
        assertStringIncludes(message, `(${(timing.ttl ?? 0) * 1000}ms)`)
        assertStringIncludes(message, 'Lower the interval or raise the TTL')
    }
    return message
}

Deno.test('#362 B1 an interval above half the revocation TTL is refused by the relation', () => {
    const message = refused('relation', { interval: 10_000, ttl: 15 })
    assertStringIncludes(message, '(15000ms)')
    assertStringIncludes(message, 'presence.reconcileIntervalMs=10000ms')
    assertStringIncludes(message, 'revocationTtlSeconds=15s')
})

Deno.test('#362 B2 an interval of exactly half the revocation TTL constructs (the boundary)', () => {
    build({ interval: 10_000, ttl: 20 })
})

Deno.test('#362 B3 the defaults construct, through the constructor and fromConfig', async () => {
    build({})
    const driver = RedisBroadcastDriver.fromConfig({ hostname: '127.0.0.1' })
    await driver.close()
})

Deno.test('#362 B4 a NaN, zero, negative or infinite interval is refused by the guard', () => {
    for (const interval of [NaN, 0, -1, Infinity]) {
        const message = refused('guard', { interval, ttl: 300 })
        assertStringIncludes(
            message,
            `presence.reconcileIntervalMs=${interval}`,
        )
        assertStringIncludes(message, 'revocationTtlSeconds=300')
    }
})

Deno.test('#362 B5 a NaN, zero, negative or infinite revocation TTL is refused by the guard', () => {
    for (const ttl of [NaN, 0, -1, Infinity]) {
        const message = refused('guard', { interval: 10_000, ttl })
        assertStringIncludes(message, `revocationTtlSeconds=${ttl}`)
        assertStringIncludes(message, 'presence.reconcileIntervalMs=10000')
    }
})

Deno.test('#362 B6 a fractional or timer-overflowing TTL and a sub-millisecond interval are refused by the guard', () => {
    refused('guard', { interval: 1_000, ttl: 7.5 })
    refused('guard', { interval: 1_000, ttl: MAX_TTL_SECONDS + 1 })
    refused('guard', { interval: 0.5, ttl: 300 })
    // Pins: the largest TTL one timer can hold, a fractional interval, and
    // the smallest timing both checks accept.
    build({ interval: 1_000, ttl: MAX_TTL_SECONDS })
    build({ interval: 1.5, ttl: 300 })
    build({ interval: 1, ttl: 1 })
})

// ---------------------------------------------------------------------------
// D — the enforcement deadline
// ---------------------------------------------------------------------------

/** The reap, as `revocation_wire.ts` defines it, bound to this index. */
const isReap: CommandMatch = (args) => isReapOf(args, INDEX)

/** A page read of the index. */
const isPageRead: CommandMatch = (args) =>
    args[0] === 'ZSCAN' && args[1] === INDEX

/**
 * A command port serialised as the production client is (#359's
 * `serialPort`, re-created here): every command is recorded AT ISSUE, before
 * its serial queue; one reply can be held; and a class of command can be made
 * to reject from now on.
 */
function serialPort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const sent: string[][] = []
    let failing: CommandMatch | undefined
    const command: CommandFn = (...args) => {
        sent.push(args)
        if (failing?.(args)) {
            return Promise.reject(new Error('injected: refused (#362)'))
        }
        return serial.command(...args)
    }
    return {
        command,
        /** Hold the reply of the NEXT command matching `match`. */
        hold: (match: CommandMatch): CommandGate => serial.hold(match),
        /** Refuse every command matching `match` from now on. */
        failFrom: (match: CommandMatch) => void (failing = match),
        /** Stop refusing. */
        heal: () => void (failing = undefined),
        /** How many commands matching `match` were issued. */
        issued: (match: CommandMatch) => sent.filter(match).length,
    }
}

/** One driver over a serialising port, the broker clock pinned at `NOW_S`. */
function instance(options: { interval: number }) {
    const redis = new FakeRedis()
    redis.setTime(NOW_S)
    const port = serialPort(redis)
    const subscriber = redis.subscriberFor()
    let completed = 0
    const driver = new RedisBroadcastDriver(
        { command: port.command },
        subscriber,
        {
            prefix: PREFIX,
            revocationTtlSeconds: TTL,
            presence: { reconcileIntervalMs: options.interval },
        },
    )
    return {
        redis,
        port,
        driver,
        /** Fire the subscribe socket's reconnect seam. */
        reconnect: () => subscriber.fireReconnect(),
        /** How many re-checks registered by `listen` have completed. */
        completed: () => completed,
        /** Register the re-check directly on the driver (plan D10). */
        listen: () =>
            driver.onRevocationReconcile(async () => {
                await driver.listRevocations()
                completed++
            }),
    }
}

/** Captured log lines, and a `console.warn` that can be made to throw. */
function captureLogs() {
    const warn = console.warn
    const error = console.error
    const warns: string[] = []
    const errors: string[] = []
    let warnFails = false
    console.warn = (...parts: unknown[]) => {
        if (warnFails) throw new Error('warn sink down (#362)')
        warns.push(parts.join(' '))
    }
    console.error = (...parts: unknown[]) => void errors.push(parts.join(' '))
    return {
        /** WARN lines starting with `prefix`. */
        lines: (prefix: string) => warns.filter((l) => l.startsWith(prefix)),
        /** How many WARN lines start with `prefix`. */
        count: (prefix: string) =>
            warns.filter((l) => l.startsWith(prefix)).length,
        /** Every WARN line starting with any deadline constant. */
        deadlineLines: () =>
            warns.filter((l) => DEADLINE.some((c) => l.startsWith(c))),
        /** Every ERROR line starting with the marked fallback prefix. */
        marked: () => errors.filter((l) => l.startsWith(REVOCATION_LOG_FAILED)),
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
 * Run `body` under FakeTime at `START`, with `performance.now` on the fake
 * clock and the console captured; everything is restored afterwards.
 */
async function withClock(
    body: (time: FakeTime, logs: Logs) => Promise<void>,
): Promise<void> {
    const time = new FakeTime(START)
    const passClock = performance.now
    performance.now = () => time.now
    const logs = captureLogs()
    try {
        await body(time, logs)
    } finally {
        logs.restore()
        performance.now = passClock
        time.restore()
    }
}

/**
 * Advance fake time by `ms` in steps of at most `step`, draining the
 * microtask queue after each, so every pass runs at its own instant.
 */
async function advance(time: FakeTime, ms: number, step = 250): Promise<void> {
    let left = ms
    while (left > 0) {
        const by = Math.min(step, left)
        await time.tickAsync(by)
        await time.runMicrotasks()
        left -= by
    }
}

/** Whether `gate` has been reached once the microtask queue has drained. */
async function reachedNow(gate: CommandGate, time: FakeTime): Promise<boolean> {
    let reached = false
    void gate.reached.then(() => void (reached = true))
    await time.runMicrotasks()
    return reached
}

Deno.test('#362 D1 a command that never settles is reported once as STALLED, and no second pass starts', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        const page = port.hold(isPageRead)
        listen()
        await advance(time, 1_000)
        assert(await reachedNow(page, time), 'the timer pass reads a page')
        await advance(time, 9_000)
        assertEquals(logs.count(REVOCATION_DEADLINE_STALLED), 1)
        const [line] = logs.lines(REVOCATION_DEADLINE_STALLED)
        assertStringIncludes(line, 'trigger timer')
        assertStringIncludes(line, 'age 9000ms')
        assertStringIncludes(line, 'every command settles')
        await advance(time, 30_000)
        assertEquals(logs.deadlineLines().length, 1)
        assertEquals(port.issued(isReap), 1, 'one reap was issued')
        assertEquals(port.issued(isPageRead), 1, 'one page read was issued')
        await driver.close()
    })
})

Deno.test('#362 D2 a failure run is reported once per episode, and a success re-arms', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 3_000 })
        port.failFrom(isReap)
        listen()
        await advance(time, 9_999)
        assertEquals(logs.deadlineLines().length, 0)
        await advance(time, 1)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        // Heal: the pass at s = 12 s succeeds.
        port.heal()
        await advance(time, 2_000)
        port.failFrom(isReap)
        await advance(time, 9_999)
        assertEquals(logs.deadlineLines().length, 1, 'nothing before s + TTL')
        await advance(time, 1)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 2)
        assertEquals(logs.deadlineLines().length, 2)
        await driver.close()
    })
})

Deno.test('#362 D3 healthy passes never write a deadline line', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 30_000)
        assertEquals(logs.deadlineLines(), [])
        assertEquals(logs.marked(), [])
        assert(port.issued(isReap) >= 29, 'the passes ran')
        await driver.close()
    })
})

Deno.test('#362 D4 (a) the deadline is anchored at the START of the last success', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        const reply = port.hold(isReap)
        listen()
        await advance(time, 1_000) // s = 1 s
        assert(await reachedNow(reply, time))
        await advance(time, 4_000) // H = 4 s
        port.failFrom(isReap)
        reply.release()
        await time.runMicrotasks() // the pass succeeds at s + H
        await advance(time, 5_999)
        assertEquals(logs.deadlineLines().length, 0)
        await advance(time, 1) // s + TTL, not s + H + TTL
        assertEquals(logs.deadlineLines().length, 1)
        await driver.close()
    })
})

Deno.test('#362 D4 (b) a pass held past the TTL is STALLED at the last success start + TTL, then MISSED at its end', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 1_000) // s_prev = 1 s succeeds
        const reply = port.hold(isReap)
        await advance(time, 1_000) // s = 2 s, held
        assert(await reachedNow(reply, time))
        await advance(time, 8_999)
        assertEquals(logs.deadlineLines().length, 0)
        await advance(time, 1) // s_prev + TTL
        assertEquals(logs.count(REVOCATION_DEADLINE_STALLED), 1)
        const [line] = logs.lines(REVOCATION_DEADLINE_STALLED)
        assertStringIncludes(line, 'trigger timer')
        assertStringIncludes(line, 'age 9000ms')
        await advance(time, 3_000) // s + 12 s
        reply.release()
        await time.runMicrotasks()
        await time.tickAsync(0)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        assertEquals(logs.deadlineLines().length, 2)
        await driver.close()
    })
})

Deno.test('#362 D4b an overdue arm writes MISSED even when a trailing pass started first', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen, reconnect } = instance({
            interval: 1_000,
        })
        listen()
        await advance(time, 1_000) // s_prev = 1 s succeeds
        const reply = port.hold(isReap)
        await advance(time, 1_000) // s = 2 s, held
        assert(await reachedNow(reply, time))
        await reconnect() // recorded: the end site starts it
        await advance(time, 12_000)
        assertEquals(logs.count(REVOCATION_DEADLINE_STALLED), 1)
        const trailing = port.hold(isReap)
        reply.release()
        assert(await reachedNow(trailing, time), 'the trailing pass runs')
        await time.tickAsync(0)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        assertEquals(
            logs.lines(REVOCATION_DEADLINE_STALLED)
                .filter((l) => l.includes('trigger reconnect')),
            [],
        )
        assertEquals(logs.deadlineLines().length, 2)
        trailing.release()
        await time.runMicrotasks()
        await driver.close()
    })
})

Deno.test('#362 D4c an overdue MISSED carry survives a trailing pass that succeeds before its flush timer (#383)', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen, reconnect } = instance({
            interval: 1_000,
        })
        listen()
        await advance(time, 1_000) // s_prev = 1 s succeeds
        const reply = port.hold(isReap)
        await advance(time, 1_000) // s = 2 s, held
        assert(await reachedNow(reply, time))
        await reconnect() // recorded: the end site starts the trailing pass
        await advance(time, 12_000) // past s_prev + TTL: STALLED, once
        assertEquals(logs.count(REVOCATION_DEADLINE_STALLED), 1)
        // The held pass is clean but overdue (it ran 12 s > TTL): its end
        // site starts the trailing 'reconnect' pass FIRST, then decides
        // MISSED now (arm(delayMs <= 0)) and queues it in #unwritten behind
        // a 0 ms timer. The trailing pass is NOT held this time, so it
        // completes in this SAME microtask drain — racing that 0 ms timer —
        // and its own success calls passSucceeded again, which must carry
        // the still-unwritten MISSED forward rather than lose it.
        reply.release()
        await time.runMicrotasks()
        assertEquals(
            logs.count(REVOCATION_DEADLINE_MISSED),
            0,
            'not flushed yet',
        )
        assertEquals(
            logs.lines(REVOCATION_DEADLINE_STALLED)
                .filter((l) => l.includes('trigger reconnect')),
            [],
        )
        await time.tickAsync(0)
        assertEquals(
            logs.count(REVOCATION_DEADLINE_MISSED),
            1,
            'the carry survives the race',
        )
        assertEquals(logs.deadlineLines().length, 2)
        await driver.close()
    })
})

Deno.test('#362 D5 (i) close() during a pass that then succeeds arms nothing', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        const page = port.hold(isPageRead)
        listen()
        await advance(time, 1_000)
        assert(await reachedNow(page, time))
        const closed = driver.close()
        page.release()
        await time.runMicrotasks()
        await closed
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines(), [])
    })
})

Deno.test('#362 D5 (ii) close() clears a pending deadline', async () => {
    await withClock(async (time, logs) => {
        const { driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 500)
        await driver.close()
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines(), [])
    })
})

Deno.test('#362 D5 (iii) a registration after close() arms nothing', async () => {
    await withClock(async (time, logs) => {
        const { driver, listen } = instance({ interval: 1_000 })
        await driver.close()
        listen()
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines(), [])
    })
})

Deno.test('#362 D5 (iv) a re-registration during a failure run does not postpone the deadline', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        port.failFrom(isReap)
        listen()
        await advance(time, (TTL - 1) * 1000)
        listen()
        await advance(time, 999)
        assertEquals(logs.deadlineLines().length, 0)
        await advance(time, 1) // TTL, not TTL - 1 + TTL
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines().length, 1)
        await driver.close()
    })
})

Deno.test('#362 D6 a wall-clock step during a pass does not move the deadline', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen, completed } = instance({
            interval: 1_000,
        })
        const reply = port.hold(isReap)
        listen()
        await advance(time, 1_000) // s = 1 s
        assert(await reachedNow(reply, time))
        // The wall clock steps one TTL forward; the pass clock does not.
        // Wrapped, never assigned: FakeTime's `Date` is a Proxy whose `set`
        // writes through to the REAL `Date.now`.
        const fakeDate = globalThis.Date
        globalThis.Date = new Proxy(fakeDate, {
            get: (target, prop, receiver) =>
                prop === 'now'
                    ? () => time.now + TTL * 1000
                    : Reflect.get(target, prop, receiver),
        })
        try {
            port.failFrom(isReap)
            reply.release()
            // The pass succeeds; drained on a real macrotask (see
            // REAL_SET_TIMEOUT), so the step is still in place at its end.
            await new Promise<void>((done) => REAL_SET_TIMEOUT(done, 0))
            assertEquals(completed(), 1, 'the pass completed under the step')
            assertEquals(
                Date.now(),
                time.now + TTL * 1000,
                'the wall-clock step is in place at the pass end',
            )
        } finally {
            globalThis.Date = fakeDate
        }
        await advance(time, 9_999)
        assertEquals(logs.deadlineLines().length, 0)
        await advance(time, 1) // s + TTL on the pass clock
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        await driver.close()
    })
})

Deno.test('#362 D7 (i) a broker clock step of exactly one TTL between two successes is SKEWED, once', async () => {
    await withClock(async (time, logs) => {
        const { redis, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 1_000) // read at NOW_S
        redis.setTime(NOW_S + TTL)
        await advance(time, 1_000) // read at NOW_S + TTL
        await advance(time, 250)
        assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 1)
        const [line] = logs.lines(REVOCATION_DEADLINE_SKEWED)
        assertStringIncludes(line, String(NOW_S))
        assertStringIncludes(line, String(NOW_S + TTL))
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines().length, 1)
        await driver.close()
    })
})

Deno.test('#362 D7 (ii) a broker clock step shorter than one TTL is not reported', async () => {
    await withClock(async (time, logs) => {
        const { redis, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 1_000)
        redis.setTime(NOW_S + TTL - 1)
        await advance(time, 2 * TTL * 1000)
        assertEquals(logs.deadlineLines(), [])
        await driver.close()
    })
})

Deno.test('#362 D7 (iii) a broker clock step after the local deadline fired adds nothing', async () => {
    await withClock(async (time, logs) => {
        const { redis, port, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 1_000) // read at NOW_S, deadline at 11 s
        port.failFrom(isReap)
        await advance(time, TTL * 1000)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        redis.setTime(NOW_S + TTL)
        port.heal()
        await advance(time, 1_250)
        assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 0)
        assertEquals(logs.deadlineLines().length, 1)
        await driver.close()
    })
})

Deno.test('#362 D7 (iv) a failure run after a SKEWED success is still reported at s + TTL', async () => {
    await withClock(async (time, logs) => {
        const { redis, port, driver, listen } = instance({ interval: 1_000 })
        listen()
        await advance(time, 1_000) // read at NOW_S
        redis.setTime(NOW_S + TTL)
        await advance(time, 1_000) // s = 2 s, SKEWED
        port.failFrom(isReap)
        await advance(time, 9_999)
        assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 1)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 0)
        await advance(time, 1) // s + TTL
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        await driver.close()
    })
})

Deno.test('#362 D7 (v) a decided SKEWED line survives a rerun that succeeds before its timer', async () => {
    await withClock(async (time, logs) => {
        const { redis, port, driver, listen, reconnect } = instance({
            interval: 1_000,
        })
        listen()
        await advance(time, 1_000) // read at NOW_S
        redis.setTime(NOW_S + TTL)
        const reply = port.hold(isReap)
        await advance(time, 1_000) // s = 2 s, held
        assert(await reachedNow(reply, time))
        await reconnect() // the end site starts the rerun at once
        reply.release()
        // Both the skewed pass and its rerun succeed in this one drain,
        // before the 0 ms timer that writes SKEWED can fire.
        await time.runMicrotasks()
        port.failFrom(isReap)
        await advance(time, 9_999)
        assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 1)
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 0)
        await advance(time, 1) // the rerun's start + TTL
        assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
        assertEquals(logs.deadlineLines().length, 2)
        await driver.close()
    })
})

Deno.test('#362 D7 (vi) an overdue MISSED carry suppresses a coinciding SKEWED: one line, not two (#383 item 5)', async () => {
    await withClock(async (time, logs) => {
        const { redis, port, driver, listen, reconnect } = instance({
            interval: 1_000,
        })
        listen()
        await advance(time, 1_000) // s_prev = 1 s succeeds, readAt = NOW_S
        const reply = port.hold(isReap)
        await advance(time, 1_000) // s = 2 s, held
        assert(await reachedNow(reply, time))
        await reconnect() // recorded: the end site starts the trailing pass
        await advance(time, 12_000) // past s_prev + TTL: STALLED, once
        assertEquals(logs.count(REVOCATION_DEADLINE_STALLED), 1)
        // The broker clock steps a full TTL AFTER the held pass's own reap
        // already ran (it reads NOW_S, unmoved) — so releasing it is clean
        // but overdue (12 s > TTL): its own passSucceeded call carries a
        // MISSED, never a SKEWED of its own (its readAt matches the previous
        // one exactly).
        redis.setTime(NOW_S + TTL)
        const trailing = port.hold(isReap)
        reply.release()
        await time.runMicrotasks()
        assertEquals(
            logs.count(REVOCATION_DEADLINE_MISSED),
            0,
            'not flushed yet',
        )
        // The trailing 'reconnect' pass reads the STEPPED broker clock: on
        // its own, a gap of one full TTL against the held pass's readAt
        // would be SKEWED-eligible. But the carried MISSED is still pending
        // when this pass's passSucceeded runs, so the #383 item-5 gate must
        // suppress it — the overdue MISSED already reports this episode.
        assert(await reachedNow(trailing, time), 'the trailing pass runs')
        trailing.release()
        await time.runMicrotasks()
        await time.tickAsync(0)
        assertEquals(
            logs.count(REVOCATION_DEADLINE_MISSED),
            1,
            'the carry lands',
        )
        assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 0, 'suppressed')
        assertEquals(
            logs.deadlineLines().length,
            2,
            'STALLED once, MISSED once — never a second line for one episode',
        )
        await driver.close()
    })
})

Deno.test("#362 D9 the deadline timer is unref'd, never what keeps the process alive", async () => {
    await withClock(async (time) => {
        const { driver, listen } = instance({ interval: 1_000 })
        const unref = Deno.unrefTimer
        const unrefd: number[] = []
        Object.defineProperty(Deno, 'unrefTimer', {
            value: (id: number) => {
                unrefd.push(id)
                unref(id)
            },
            configurable: true,
            writable: true,
        })
        try {
            listen() // arms the deadline; nothing else unrefs here
            assertEquals(unrefd.length, 1, "the deadline timer is unref'd")
        } finally {
            Object.defineProperty(Deno, 'unrefTimer', {
                value: unref,
                configurable: true,
                writable: true,
            })
        }
        await advance(time, 250)
        await driver.close()
    })
})

Deno.test('#362 D8 (i) a throwing console.warn at the fire becomes one marked ERROR line', async () => {
    await withClock(async (time, logs) => {
        const { port, driver, listen } = instance({ interval: 1_000 })
        const page = port.hold(isPageRead)
        listen()
        await advance(time, 1_000)
        assert(await reachedNow(page, time))
        logs.failWarn(true)
        try {
            await advance(time, 9_000)
        } finally {
            logs.failWarn(false)
        }
        const marked = logs.marked()
        assertEquals(marked.length, 1)
        assertStringIncludes(marked[0], REVOCATION_DEADLINE_STALLED)
        assertStringIncludes(marked[0], 'warn sink down')
        assertEquals(logs.deadlineLines(), [])
        await driver.close()
    })
})

Deno.test('#362 D8 (ii) a rejecting pass chain is one marked ERROR line, and the passes and the deadline go on', async () => {
    await withClock(async (time, logs) => {
        const { driver } = instance({ interval: 1_000 })
        try {
            await watchingEscapes(async (escaped) => {
                let calls = 0
                driver.onRevocationReconcile(() => {
                    calls++
                    return Promise.reject(new Error('handler down (#362)'))
                })
                logs.failWarn(true)
                try {
                    await advance(time, 1_000)
                    await time.runMicrotasks()
                } finally {
                    logs.failWarn(false)
                }
                assertEquals(escaped, [], 'no rejection escapes the pass chain')
                const marked = logs.marked()
                assertEquals(marked.length, 1)
                assertStringIncludes(marked[0], 'warn sink down')
                // #383 item 2: #warnReconcileFailed self-guards, so the
                // "reconcile failed" context is never lost to the rejection
                // that used to carry only the sink's OWN error past it.
                assertStringIncludes(marked[0], 'handler down (#362)')
                await advance(time, 1_000)
                assertEquals(calls, 2, 'the next timer pass runs')
                await advance(time, 7_999)
                assertEquals(logs.deadlineLines().length, 0)
                await advance(time, 1)
                assertEquals(logs.count(REVOCATION_DEADLINE_MISSED), 1)
            })
        } finally {
            await driver.close()
        }
    })
})

Deno.test('#362 D8 (iii) a reconnect pass whose sink also fails still runs the #308 retry', async () => {
    await withClock(async (time, logs) => {
        // A wide interval (the relation allows up to 5 000ms at TTL=10) keeps
        // the ordinary periodic timer pass well outside this witness's
        // window, so only the #308 retry itself can raise `calls` past 1.
        const { driver, reconnect } = instance({ interval: 5_000 })
        try {
            await watchingEscapes(async (escaped) => {
                let calls = 0
                driver.onRevocationReconcile(() => {
                    calls++
                    return Promise.reject(new Error('handler down (#362)'))
                })
                logs.failWarn(true)
                try {
                    await reconnect()
                    await time.runMicrotasks()
                    assertEquals(calls, 1, 'the reconnect pass ran')
                    assertEquals(
                        escaped,
                        [],
                        'no rejection escapes the pass chain',
                    )
                    // #383 item 2: before the self-guard, the throwing
                    // console.warn escaped #runRevocationReconcile's catch
                    // block before it ever reached the retry-arming code
                    // below, silently dropping the #308 one-shot retry too.
                    await advance(time, 1_500)
                    assertEquals(
                        calls,
                        2,
                        'the reconnect-retry pass still fires',
                    )
                } finally {
                    logs.failWarn(false)
                }
            })
        } finally {
            await driver.close()
        }
    })
})

// ---------------------------------------------------------------------------
// #383 item 6 — a direct EnforcementDeadline unit witness (no driver needed)
// ---------------------------------------------------------------------------

Deno.test(
    "#383 (vi) passSucceeded arms ttlMs minus the pass's own elapsed time, not a full TTL",
    async () => {
        const time = new FakeTime(START)
        const warn = console.warn
        const warns: string[] = []
        console.warn = (...parts: unknown[]) => void warns.push(parts.join(' '))
        try {
            const deadline = new EnforcementDeadline({
                ttlMs: 10_000,
                now: () => time.now,
                inFlight: () => undefined,
            })
            deadline.arm(10_000)
            // Under FakeTime the elapsed time between two calls is 0 unless
            // real time is advanced in between — so a witness that calls
            // passSucceeded right after arm() cannot tell a subtraction from
            // no subtraction at all. Here the pass itself took 4 000 ms
            // (startedAt=0, endedAt=4_000): without the subtraction the
            // re-arm would wait a full 10 000 ms from THIS instant, and
            // MISSED would fire 4 000 ms late.
            deadline.passSucceeded(0, 4_000, undefined)
            await time.tickAsync(5_999)
            assertEquals(warns.length, 0, 'not armed for a full TTL')
            await time.tickAsync(1)
            assertEquals(
                warns.length,
                1,
                "armed for ttlMs - the pass's own elapsed time (6 000ms)",
            )
        } finally {
            console.warn = warn
            time.restore()
        }
    },
)
