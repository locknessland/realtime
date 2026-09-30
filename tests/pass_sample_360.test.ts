/**
 * @fileoverview #360 — the Redis driver hands each completed ghost sweep and
 * revocation re-check to one `onPassComplete` handler as a frozen
 * `PassSample`: which pass, what triggered it, how it ended, how long it took
 * on the pass clock, and how many pages it read.
 *
 * Every witness drives one `RedisBroadcastDriver` over a FakeRedis behind a
 * serialising command port (one exchange in flight, as on the production
 * client), with the revocation re-check registered DIRECTLY on the driver
 * (`driver.listRevocations()`), as #362 D1 does. The port records every
 * command at issue, can hold one reply, can refuse a class of command from a
 * given point on, and can replace a reply outright.
 *
 * - `performance.now` is pointed at FakeTime's clock, measured from the
 *   witness's own origin (a real `performance.now` is not an epoch reading),
 *   and can be stepped on its own ({@link stubPerformanceNow});
 * - the fake broker's `TIME` is pinned, so a revocation record never expires
 *   under a witness;
 * - `console.warn` and `console.error` are captured, a line counts when it
 *   STARTS with one of the named markers, and `console.warn` can be made to
 *   throw;
 * - the shared `unhandledrejection` watcher (`watchingEscapes`, #374) records
 *   what escapes, as a backstop to the sanitizer, so an escape is an assertion
 *   rather than a crashed file.
 *
 * **Page counts are read from the fake, never hard-coded** (plan D12): the
 * fake honours `COUNT` on `ZSCAN` and `SSCAN` by walking a fixed 1 024-slot
 * table, so 250 members at `COUNT` 100 take 11 pages, some empty. A witness
 * over a paged set first asserts the fake served more than one page (A6),
 * then compares the sample with what the fake's command log says it served
 * during that pass.
 *
 * Red on `f697c100` (before #360): the file does not compile — `PassSample`,
 * `onPassComplete` and the three markers do not exist. P11 and P13 are pins.
 *
 * **P14 is not in the original FR-015 list** (#386, the #360 review's item 1).
 * It asserts nothing but the sweep's re-arm order, under the same throwing
 * handler and throwing `console.warn` as P8 (iii), because P8 (iii) alone
 * cannot attribute a kill to the order: M12 (the #369 fallback removed) fails
 * P8 (iii) on its own, on a marked-line assertion, whatever the order is — so
 * M18, which bundles M12 with the reorder, was killed by P8 (iii) for a
 * reason the order never had to hold. P14 has one assertion, so a kill there
 * is order and only order.
 *
 * @module @lockness/realtime/tests/pass_sample_360
 */

import { assert, assertEquals, assertStringIncludes } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    PASS_SAMPLE_FAILED,
    PASS_SAMPLE_LOG_FAILED,
    type PassSample,
    RECONCILE_LOG_FAILED,
    RedisBroadcastDriver,
    SWEEP_INSTANCE_LOG_FAILED,
    SWEEP_INSTANCE_RELEASED_LOG_FAILED,
    SWEEP_INSTANCE_RENEWED_LOG_FAILED,
    SWEEP_LOG_FAILED,
} from '../drivers/redis.ts'
import { REVOCATION_LOG_FAILED } from '../drivers/enforcement_deadline.ts'
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
 * The real `setTimeout`, captured before any FakeTime exists: P9 drains on it,
 * because FakeTime's `runMicrotasks` resets `globalThis.Date`, and with it
 * the wall-clock step P9 installs (the #362 D6 precedent).
 */
const REAL_SET_TIMEOUT = globalThis.setTimeout

const START = new Date('2026-09-24T10:00:00Z')
/** The fake broker's `TIME`, pinned. */
const NOW_S = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const INSTANCES_KEY = `${PREFIX}__instances`
const OWNED_KEY = (id: string) => `${PREFIX}__owned:${id}`
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string) =>
    `${PREFIX}__holders:${channel} ${id}`
/** The channel every planted ghost hold lives in. */
const CHANNEL = 'presence-room'
/** The channel the sweeping driver holds its own member in. */
const OTHER = 'presence-other'
/** The one interval both passes run on, in milliseconds. */
const INTERVAL = 1_000
/** The revocation TTL, in seconds: at least twice the interval (#362). */
const TTL = 10
const DEAD = 'instance-dead'
const DEAD_TOO = 'instance-dead-too'
/** The start of the one WARN a failed ghost sweep writes. */
const RECONCILE_FAILED = 'realtime: roster reconcile failed'

// ---------------------------------------------------------------------------
// Matchers
// ---------------------------------------------------------------------------

/** The reap, as `revocation_wire.ts` defines it, bound to this index. */
const isReap: CommandMatch = (args) => isReapOf(args, INDEX)

/** A page read of the revocation index. */
const isZscan: CommandMatch = (args) => args[0] === 'ZSCAN' && args[1] === INDEX

/** A page read of any owned set. */
const isSscan: CommandMatch = (args) => args[0] === 'SSCAN'

/** The sweep's read of the instance set. */
const isInstanceRead: CommandMatch = (args) =>
    args[0] === 'SMEMBERS' && args[1] === INSTANCES_KEY

/** A liveness probe. */
const isExists: CommandMatch = (args) => args[0] === 'EXISTS'

/** A sweep release of one of `owner`'s holds (it names a holders hash). */
const isRelease = (owner: string): CommandMatch => (args) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(owner)) &&
    args.some((a) => a.startsWith(`${PREFIX}__holders:`))

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/**
 * A command port serialised as the production client is: every command is
 * recorded AT ISSUE; one reply can be held; a class of command can be refused
 * from now on; a class of command can be answered with a replaced reply.
 */
function overridePort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const sent: string[][] = []
    const failing: CommandMatch[] = []
    const replaced: { match: CommandMatch; reply: unknown }[] = []
    const command: CommandFn = (...args) => {
        sent.push(args)
        if (failing.some((match) => match(args))) {
            return Promise.reject(new Error('injected: refused (#360)'))
        }
        const replacement = replaced.find((r) => r.match(args))
        if (replacement) return Promise.resolve(replacement.reply)
        return serial.command(...args)
    }
    return {
        command,
        /** Hold the reply of the NEXT command matching `match`. */
        hold: (match: CommandMatch): CommandGate => serial.hold(match),
        /** Refuse every command matching `match` from now on. */
        failFrom: (match: CommandMatch) => void failing.push(match),
        /** Answer every command matching `match` with `reply` from now on. */
        replace: (match: CommandMatch, reply: unknown) =>
            void replaced.push({ match, reply }),
        /** How many commands matching `match` were issued. */
        issued: (match: CommandMatch) => sent.filter(match).length,
    }
}

/**
 * How many commands matching `match` the fake has SERVED so far, read from
 * its command log (D12) — a held command is served when it reaches the fake.
 */
const served = (redis: FakeRedis, match: CommandMatch) =>
    redis.commandLog().filter(match).length

/** One driver over the override port, the broker clock pinned. */
function fleet() {
    const redis = new FakeRedis()
    redis.setTime(NOW_S)
    const port = overridePort(redis)
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver(
        { command: port.command },
        subscriber,
        {
            prefix: PREFIX,
            revocationTtlSeconds: TTL,
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: INTERVAL,
            },
        },
    )
    const samples: PassSample[] = []
    return {
        redis,
        port,
        driver,
        /** Every sample {@link record} received, in order. */
        samples,
        /** The sweep samples received. */
        sweeps: () => samples.filter((s) => s.pass === 'sweep'),
        /** The revocation samples received. */
        revocations: () => samples.filter((s) => s.pass === 'revocation'),
        /** Register the recording handler. */
        record: () => driver.onPassComplete((s) => void samples.push(s)),
        /** Register the re-check directly on the driver (#362 D1). */
        listen: () =>
            driver.onRevocationReconcile(async () => {
                await driver.listRevocations()
            }),
        /** Hold a member, which starts the heartbeat and the sweep timer. */
        startSweep: () => driver.holdMember(OTHER, { id: 9 }),
        /** Fire the subscribe socket's reconnect seam. */
        reconnect: () => subscriber.fireReconnect(),
    }
}

/** A sample without its duration, for comparing what is deterministic. */
const shape = (s: PassSample) => ({
    pass: s.pass,
    trigger: s.trigger,
    outcome: s.outcome,
    pages: s.pages,
})

/** Plant one revocation record, as `MARK_REVOKED_SCRIPT` writes it. */
async function plantRevocation(redis: FakeRedis, member: string) {
    await redis.command('ZADD', INDEX, String(NOW_S + 300), member)
}

/** Write `owner`'s hold of `field` exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(redis: FakeRedis, field: string, owner = DEAD) {
    const value = JSON.stringify({ member: { id: Number(field) }, owner })
    await redis.command('HSET', HOLDERS_KEY(CHANNEL, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(CHANNEL), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${CHANNEL} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/**
 * A dead instance (no liveness key, ever) owning `holds` holds, fields
 * counting up from `from`.
 */
async function plantDead(
    redis: FakeRedis,
    holds: number,
    owner = DEAD,
    from = 1,
) {
    for (let i = 0; i < holds; i++) {
        await plantHold(redis, String(from + i), owner)
    }
}

/**
 * Point `performance.now` at FakeTime's clock, measured from now — a real
 * `performance.now` is a small monotonic reading, never an epoch one — and
 * let a witness step it on its own by a set amount.
 */
function stubPerformanceNow(time: FakeTime) {
    const original = performance.now
    const origin = time.now
    let offset = 0
    performance.now = () => time.now - origin + offset
    return {
        /** Move the pass clock forward by `ms`, and nothing else. */
        step: (ms: number) => void (offset += ms),
        restore: () => void (performance.now = original),
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
        if (warnFails) throw new Error('warn sink down (#360)')
        warns.push(parts.join(' '))
    }
    console.error = (...parts: unknown[]) => void errors.push(parts.join(' '))
    return {
        /** WARN lines starting with `prefix`. */
        warned: (prefix: string) => warns.filter((l) => l.startsWith(prefix)),
        /** ERROR lines starting with `prefix`. */
        errored: (prefix: string) => errors.filter((l) => l.startsWith(prefix)),
        /** Every ERROR line a marked fallback writes. */
        marked: () =>
            errors.filter((l) =>
                [
                    PASS_SAMPLE_LOG_FAILED,
                    SWEEP_LOG_FAILED,
                    SWEEP_INSTANCE_LOG_FAILED,
                    SWEEP_INSTANCE_RENEWED_LOG_FAILED,
                    SWEEP_INSTANCE_RELEASED_LOG_FAILED,
                    RECONCILE_LOG_FAILED,
                    REVOCATION_LOG_FAILED,
                ].some((marker) => l.startsWith(marker))
            ),
        /** Make `console.warn` throw, or stop it throwing. */
        failWarn: (on: boolean) => void (warnFails = on),
        restore: () => {
            console.warn = warn
            console.error = error
        },
    }
}

/** Everything a witness runs under. */
interface Harness {
    readonly time: FakeTime
    readonly clock: ReturnType<typeof stubPerformanceNow>
    readonly logs: ReturnType<typeof captureLogs>
    /** Every rejection that escaped, from the shared watcher (#374). */
    readonly escaped: unknown[]
}

/**
 * Run `body` under FakeTime at `START`, with `performance.now` on the fake
 * clock, the console captured and unhandled rejections recorded by the shared
 * {@link watchingEscapes}; all of it is restored afterwards.
 */
async function withClock(body: (h: Harness) => Promise<void>): Promise<void> {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const clock = stubPerformanceNow(time)
        const logs = captureLogs()
        try {
            await body({ time, clock, logs, escaped })
        } finally {
            logs.restore()
            clock.restore()
            time.restore()
        }
    })
}

/**
 * Advance fake time by `ms` in steps of at most `step`, draining the
 * microtask queue after each, so every pass runs at its own instant (#362).
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

/**
 * One real macrotask: lets a rejection's `unhandledrejection` event be
 * dispatched, and drains without touching FakeTime's `Date`.
 */
const settle = () => new Promise<void>((done) => REAL_SET_TIMEOUT(done, 0))

// ---------------------------------------------------------------------------
// US1 — every completed pass reports its duration and its pages
// ---------------------------------------------------------------------------

Deno.test('#360 P1 one revocation record, one timer pass: exactly one frozen sample with one page', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        await plantRevocation(f.redis, 'c1')
        f.record()
        f.listen()
        await advance(time, INTERVAL)
        assertEquals(f.samples.length, 1, 'exactly one sample')
        const [sample] = f.samples
        assertEquals(shape(sample), {
            pass: 'revocation',
            trigger: 'timer',
            outcome: 'ok',
            pages: 1,
        })
        assert(sample.durationMs >= 0, 'a duration is never negative')
        assert(Object.isFrozen(sample), 'the sample is frozen')
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test('#360 P2 a paged revocation pass counts the ZSCAN pages the fake served, and the next pass counts its own', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        for (let i = 1; i <= 250; i++) await plantRevocation(f.redis, `c${i}`)
        f.record()
        f.listen()
        const before = served(f.redis, isZscan)
        await advance(time, INTERVAL)
        const first = served(f.redis, isZscan) - before
        assert(first > 1, `precondition: the fake paged (served ${first})`)
        assertEquals(f.revocations().length, 1)
        assertEquals(f.revocations()[0].pages, first)
        const mid = served(f.redis, isZscan)
        await advance(time, INTERVAL)
        const second = served(f.redis, isZscan) - mid
        assert(second > 1, `precondition: the fake paged (served ${second})`)
        assertEquals(f.revocations().length, 2)
        assertEquals(
            f.revocations()[1].pages,
            second,
            'its own pages, not the running sum',
        )
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test('#360 P3 (i) a sweep of one dead instance counts the SSCAN pages the fake served', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 250)
        const before = served(f.redis, isSscan)
        await advance(time, INTERVAL)
        const pages = served(f.redis, isSscan) - before
        assert(pages > 1, `precondition: the fake paged (served ${pages})`)
        assertEquals(f.sweeps().map(shape), [{
            pass: 'sweep',
            trigger: 'timer',
            outcome: 'ok',
            pages,
        }])
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test('#360 P3 (ii) a sweep with no dead instance is one sample of zero pages', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().map(shape), [{
            pass: 'sweep',
            trigger: 'timer',
            outcome: 'ok',
            pages: 0,
        }])
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test('#360 P3 (iii) a sweep of two dead instances counts the pages of both', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 150, DEAD, 1)
        await plantDead(f.redis, 150, DEAD_TOO, 1_001)
        const before = served(f.redis, isSscan)
        await advance(time, INTERVAL)
        const pages = served(f.redis, isSscan) - before
        assert(pages > 1, `precondition: the fake paged (served ${pages})`)
        assertEquals(f.sweeps().length, 1)
        assertEquals(
            f.sweeps()[0].pages,
            pages,
            'both instances, not the last one alone',
        )
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test('#360 P9 (revocation) the duration is read on the pass clock, not the wall clock', async () => {
    await withClock(async ({ time, clock }) => {
        const f = fleet()
        f.record()
        f.listen()
        const page = f.port.hold(isZscan)
        await advance(time, INTERVAL)
        assert(await reachedNow(page, time), 'the pass reads a page')
        clock.step(40)
        const fakeDate = globalThis.Date
        // Wrapped, never assigned (#362 D6): FakeTime's `Date` is a Proxy
        // whose `set` writes through to the real `Date.now`.
        globalThis.Date = new Proxy(fakeDate, {
            get: (target, prop, receiver) =>
                prop === 'now'
                    ? () => time.now + 10_000
                    : Reflect.get(target, prop, receiver),
        })
        try {
            page.release()
            await settle()
        } finally {
            globalThis.Date = fakeDate
        }
        assertEquals(f.revocations().length, 1)
        assertEquals(f.revocations()[0].durationMs, 40)
        await f.driver.close()
    })
})

Deno.test('#360 P9 (sweep) the duration is read on the pass clock, not the wall clock', async () => {
    await withClock(async ({ time, clock }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        const read = f.port.hold(isInstanceRead)
        await advance(time, INTERVAL)
        assert(await reachedNow(read, time), 'the sweep reads the instances')
        clock.step(40)
        const fakeDate = globalThis.Date
        globalThis.Date = new Proxy(fakeDate, {
            get: (target, prop, receiver) =>
                prop === 'now'
                    ? () => time.now + 10_000
                    : Reflect.get(target, prop, receiver),
        })
        try {
            read.release()
            await settle()
        } finally {
            globalThis.Date = fakeDate
        }
        assertEquals(f.sweeps().length, 1)
        assertEquals(f.sweeps()[0].durationMs, 40)
        await f.driver.close()
    })
})

Deno.test("#360 P10 (i) a handler registered mid-pass receives that pass's sample", async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.listen()
        const page = f.port.hold(isZscan)
        await advance(time, INTERVAL)
        assert(await reachedNow(page, time))
        f.record()
        page.release()
        await time.runMicrotasks()
        assertEquals(f.samples.map(shape), [{
            pass: 'revocation',
            trigger: 'timer',
            outcome: 'ok',
            pages: 1,
        }])
        await f.driver.close()
    })
})

Deno.test('#360 P10 (ii) registering again replaces the handler', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        const first: PassSample[] = []
        f.driver.onPassComplete((s) => void first.push(s))
        f.listen()
        await advance(time, INTERVAL)
        assertEquals(first.length, 1)
        f.record()
        await advance(time, INTERVAL)
        assertEquals(f.samples.length, 1, 'the second handler is called')
        assertEquals(first.length, 1, 'the first one is not, any more')
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// US2 — a reconnect's catch-up is its own series
// ---------------------------------------------------------------------------

Deno.test('#360 P5 a reconnect pass is tagged reconnect, and its retry reconnect-retry', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        f.listen()
        await f.reconnect()
        await time.runMicrotasks()
        assertEquals(f.samples.map(shape), [{
            pass: 'revocation',
            trigger: 'reconnect',
            outcome: 'ok',
            pages: 1,
        }])
        f.port.failFrom(isReap)
        await f.reconnect()
        await time.runMicrotasks()
        assertEquals(shape(f.samples[1]), {
            pass: 'revocation',
            trigger: 'reconnect',
            outcome: 'failed',
            pages: 0,
        })
        await advance(time, INTERVAL)
        const retries = f.samples.filter((s) => s.trigger === 'reconnect-retry')
        assertEquals(retries.map(shape), [{
            pass: 'revocation',
            trigger: 'reconnect-retry',
            outcome: 'failed',
            pages: 0,
        }])
        await f.driver.close()
    })
})

Deno.test('#360 P6 three reconnects during a held pass: two samples, the first is the ended pass', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        f.listen()
        const page = f.port.hold(isZscan)
        await advance(time, INTERVAL)
        assert(await reachedNow(page, time), 'the timer pass reads its page')
        await f.reconnect()
        await f.reconnect()
        await f.reconnect()
        page.release()
        await time.runMicrotasks()
        assertEquals(f.samples.length, 2, 'the ended pass and one trailing')
        assertEquals(shape(f.samples[0]), {
            pass: 'revocation',
            trigger: 'timer',
            outcome: 'ok',
            pages: 1,
        })
        assertEquals(f.samples[1].trigger, 'reconnect')
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// US3 — a failed pass says so
// ---------------------------------------------------------------------------

Deno.test('#360 P4 (i) a refused reap fails the revocation pass', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.port.failFrom(isReap)
        f.record()
        f.listen()
        await advance(time, INTERVAL)
        assertEquals(f.samples.map(shape), [{
            pass: 'revocation',
            trigger: 'timer',
            outcome: 'failed',
            pages: 0,
        }])
        await f.driver.close()
    })
})

Deno.test('#360 P4 (ii) a refused instance-set read fails the sweep', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        f.port.failFrom(isInstanceRead)
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().map(shape), [{
            pass: 'sweep',
            trigger: 'timer',
            outcome: 'failed',
            pages: 0,
        }])
        await f.driver.close()
    })
})

Deno.test('#360 P4 (iii) one dead instance whose release fails is contained: the sweep is ok', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 1)
        f.port.failFrom(isRelease(DEAD))
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        assertEquals(f.sweeps()[0].outcome, 'ok')
        await f.driver.close()
    })
})

Deno.test('#360 P4 (iv) (a) an instance-set reply that is not an array fails the sweep with one WARN', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 1)
        f.port.replace(isInstanceRead, {
            type: 'set',
            value: [{ type: 'bulk', value: DEAD }],
        })
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        assertEquals(f.sweeps()[0].outcome, 'failed')
        assertEquals(logs.warned(RECONCILE_FAILED).length, 1)
        await f.driver.close()
    })
})

Deno.test('#360 P4 (iv) (b) an EXISTS reply that is not an integer fails the sweep', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 1)
        f.port.replace(isExists, { type: 'bulk', value: '0' })
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        assertEquals(f.sweeps()[0].outcome, 'failed')
        assertEquals(logs.warned(RECONCILE_FAILED).length, 1)
        await f.driver.close()
    })
})

Deno.test('#360 P12 (i) a revocation pass that rejects records failed, and #362 writes its marked line', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.record()
        f.driver.onRevocationReconcile(() => {
            throw new Error('re-check down (#360)')
        })
        logs.failWarn(true)
        await advance(time, INTERVAL)
        logs.failWarn(false)
        await settle()
        assertEquals(escaped, [])
        assertEquals(f.samples.map(shape), [{
            pass: 'revocation',
            trigger: 'timer',
            outcome: 'failed',
            pages: 0,
        }])
        assertEquals(logs.errored(REVOCATION_LOG_FAILED).length, 1)
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// US4 — observability never breaks a pass
// ---------------------------------------------------------------------------

Deno.test('#360 P7 (i) close() during a revocation pass: no sample, then or later', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        f.listen()
        const page = f.port.hold(isZscan)
        await advance(time, INTERVAL)
        assert(await reachedNow(page, time))
        await f.driver.close()
        // Registered AFTER close() began, so what is under test is the
        // `#closing` gate, not close() dropping the handler.
        f.record()
        page.release()
        await time.runMicrotasks()
        await advance(time, 2 * INTERVAL)
        assertEquals(f.samples, [])
    })
})

Deno.test('#360 P7 (ii) close() during a sweep: no sample, then or later', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        // Planted after this instance registered, so the dead one is the
        // LAST the loop reads: the pass ends `ok`, not `closed`.
        await plantDead(f.redis, 1)
        const scan = f.port.hold(isSscan)
        await advance(time, INTERVAL)
        assert(await reachedNow(scan, time), 'the sweep reads a page')
        const closed = f.driver.close()
        scan.release()
        await closed
        await advance(time, 2 * INTERVAL)
        assertEquals(f.samples, [])
    })
})

Deno.test('#360 P8 (i) a throwing handler is one WARN per pass, and both loops go on', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            throw new Error('handler down (#360)')
        })
        await f.startSweep()
        f.listen()
        await advance(time, 3 * INTERVAL)
        await settle()
        assert(f.sweeps().length >= 2, 'the sweep goes on')
        assert(f.revocations().length >= 2, 'the re-check goes on')
        const lines = logs.warned(PASS_SAMPLE_FAILED)
        assertEquals(lines.length, f.samples.length, 'one WARN per pass')
        assertStringIncludes(lines[0], 'handler down')
        assertEquals(logs.marked(), [])
        assertEquals(escaped, [])
        await f.driver.close()
    })
})

Deno.test('#360 P8 (ii) a handler whose promise rejects is one WARN per pass, and nothing escapes', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            return Promise.reject(new Error('handler rejected (#360)'))
        })
        await f.startSweep()
        f.listen()
        await advance(time, 3 * INTERVAL)
        await settle()
        assert(f.sweeps().length >= 2, 'the sweep goes on')
        assert(f.revocations().length >= 2, 'the re-check goes on')
        const lines = logs.warned(PASS_SAMPLE_FAILED)
        assertEquals(lines.length, f.samples.length, 'one WARN per pass')
        assertStringIncludes(lines[0], 'handler rejected')
        assertEquals(logs.marked(), [], 'no rejection reaches a pass chain')
        assertEquals(escaped, [])
        await f.driver.close()
    })
})

Deno.test('#360 P8 (iii) a throwing handler while console.warn throws is one marked ERROR per pass, and the sweep re-arms', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            throw new Error('handler down (#360)')
        })
        await f.startSweep()
        f.listen()
        logs.failWarn(true)
        await advance(time, 3 * INTERVAL)
        logs.failWarn(false)
        await settle()
        assertEquals(escaped, [])
        assert(f.sweeps().length >= 2, 'the sweep loop re-arms')
        const lines = logs.errored(PASS_SAMPLE_LOG_FAILED)
        assertEquals(lines.length, f.samples.length, 'one line per pass')
        assertStringIncludes(lines[0], 'handler down')
        assertStringIncludes(lines[0], 'warn sink down')
        assertEquals(logs.errored(SWEEP_LOG_FAILED), [])
        assertEquals(logs.errored(REVOCATION_LOG_FAILED), [])
        await f.driver.close()
    })
})

Deno.test('#360 P8 (iv) a handler that never settles holds nothing: close() resolves', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            return new Promise<void>(() => {})
        })
        await f.startSweep()
        f.listen()
        await advance(time, 2 * INTERVAL)
        assert(f.sweeps().length >= 1 && f.revocations().length >= 1)
        // A sweep in flight when close() begins, so close() awaits a pass.
        const read = f.port.hold(isInstanceRead)
        await advance(time, INTERVAL)
        assert(await reachedNow(read, time), 'a sweep is in flight')
        let resolved = false
        const closing = f.driver.close().then(() => void (resolved = true))
        read.release()
        await advance(time, 2 * INTERVAL)
        assert(resolved, 'close() resolved within the bounded wait')
        await closing
    })
})

Deno.test('#360 P8 (v) (i) a returned promise whose constructor getter throws is one WARN', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            const hostile = Promise.resolve()
            Object.defineProperty(hostile, 'constructor', {
                get() {
                    throw new Error('hostile constructor (#360)')
                },
            })
            return hostile
        })
        f.listen()
        await advance(time, INTERVAL)
        await settle()
        assertEquals(f.samples.length, 1)
        const lines = logs.warned(PASS_SAMPLE_FAILED)
        assertEquals(lines.length, 1)
        assertStringIncludes(lines[0], 'hostile constructor')
        assertEquals(logs.marked(), [])
        assertEquals(escaped, [])
        await f.driver.close()
    })
})

Deno.test('#360 P8 (v) (ii) a thenable that rejects three times is ONE WARN', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            return {
                then(_: unknown, reject: (reason: unknown) => void) {
                    reject(new Error('first (#360)'))
                    reject(new Error('second (#360)'))
                    reject(new Error('third (#360)'))
                },
            }
        })
        f.listen()
        await advance(time, INTERVAL)
        await settle()
        assertEquals(f.samples.length, 1)
        assertEquals(logs.warned(PASS_SAMPLE_FAILED).length, 1)
        assertEquals(escaped, [])
        await f.driver.close()
    })
})

Deno.test("#360 P14 the sweep's next pass is armed before its sample is taken, even when the handler and console.warn both throw", async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.driver.onPassComplete((s) => {
            f.samples.push(s)
            throw new Error('handler down (#360 P14)')
        })
        await f.startSweep()
        logs.failWarn(true)
        await advance(time, 3 * INTERVAL)
        logs.failWarn(false)
        await settle()
        assert(
            f.sweeps().length >= 2,
            'the sweep loop re-arms even though the sample and its own WARN ' +
                'both threw — proves the re-arm order alone, independent of ' +
                'what the throw is logged as',
        )
        await f.driver.close()
    })
})

Deno.test('#360 P12 (ii) a ghost released while console.warn throws: ok, one marked line at #sweepInstance, and the loop goes on', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantDead(f.redis, 1)
        logs.failWarn(true)
        await advance(time, INTERVAL)
        logs.failWarn(false)
        await settle()
        assertEquals(escaped, [], 'nothing escapes the sweep')
        // #418 (second security review): the "released N hold(s)" WARN is a
        // SUCCESS exit's own report, not a failure — it is now guarded, so its
        // sink's own throw is contained right there and never reaches
        // `#reconcile`'s `for` loop at all. The pass is `ok`: releasing the
        // ghost genuinely succeeded, and only the report of it hit a broken
        // sink.
        assertEquals(f.sweeps().map(shape), [{
            pass: 'sweep',
            trigger: 'timer',
            outcome: 'ok',
            pages: 1,
        }])
        // Neither the reconcile-level nor the top-of-chain fallback fires —
        // this fixture can no longer reach either.
        assertEquals(logs.errored(RECONCILE_LOG_FAILED), [])
        assertEquals(logs.errored(SWEEP_LOG_FAILED), [])
        const lines = logs.errored(SWEEP_INSTANCE_RELEASED_LOG_FAILED)
        assertEquals(lines.length, 1)
        assertStringIncludes(lines[0], 'warn sink down')
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 2, 'the loop re-armed')
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// US5 — nothing wired, nothing new
// ---------------------------------------------------------------------------

Deno.test('#360 P11 no handler: both loops run five passes, and no pass-sample line is written', async () => {
    await withClock(async ({ time, logs, escaped }) => {
        const f = fleet()
        await f.startSweep()
        f.listen()
        await advance(time, 5 * INTERVAL + 500)
        await settle()
        assert(f.port.issued(isReap) >= 5, 'five revocation passes')
        assert(f.port.issued(isInstanceRead) >= 5, 'five sweeps')
        for (
            const marker of [
                PASS_SAMPLE_FAILED,
                PASS_SAMPLE_LOG_FAILED,
                SWEEP_LOG_FAILED,
            ]
        ) {
            assertEquals(logs.warned(marker), [])
            assertEquals(logs.errored(marker), [])
        }
        assertEquals(escaped, [])
        await f.driver.close()
        f.redis.assertNoRejections()
    })
})

Deno.test("#360 P13 #362's suite is still green", async () => {
    const suite = new URL(
        './revocation_pass_bound_362.test.ts',
        import.meta.url,
    )
    const run = await new Deno.Command(Deno.execPath(), {
        args: ['test', '--allow-all', suite.pathname],
        stdout: 'piped',
        stderr: 'piped',
    }).output()
    const out = new TextDecoder().decode(run.stdout) +
        new TextDecoder().decode(run.stderr)
    assertEquals(run.code, 0, out)
})
