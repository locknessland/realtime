/**
 * @fileoverview #383 item 1 — a stale `#lastReadAt` can raise a spurious
 * `SKEWED`. Scoped the `SKEWED` comparison's input to the pass that reports
 * it: `RevocationPassRecord.readAt`, written in `listRevocations()` and fed to
 * `passSucceeded` at `#startRevocationPass`'s end site, in place of the
 * driver-level `#lastReadAt`, which stays `#announceFloor`'s own sticky "has
 * any pass ever completed an enumeration" flag.
 *
 * Three witnesses, driving one `RedisBroadcastDriver` over a `FakeRedis`, the
 * revocation re-check registered DIRECTLY on the driver (never through a
 * manager, per plan D10 of #362):
 *
 * (i) a handler that enumerates every other pass, across a real gap of at
 *     least one TTL, does not raise `SKEWED` — the fix's whole point;
 * (ii) two genuinely consecutive enumerating passes with a gap of at least
 *      one TTL still raise `SKEWED` — the diagnostic is not disabled outright;
 * (iii) `#announceFloor`'s retry-stop, a regression guard for #380: once one
 *       enumeration has stopped it, a later pass that skips enumeration never
 *       resumes it.
 *
 * @module @lockness/realtime/tests/revocation_lastreadat_383
 */

import { assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { REVOCATION_DEADLINE_SKEWED } from '../drivers/enforcement_deadline.ts'
import {
    type CommandFn,
    type CommandMatch,
    FakeRedis,
    serializedCommands,
} from './fake_redis.ts'
import { isAnnounce } from './revocation_wire.ts'

const START = new Date('2026-09-26T10:00:00Z')
/** The fake broker's `TIME`, pinned. */
const NOW_S = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
const FLOOR = `${PREFIX}__revocation-floor`
/** The revocation TTL of every witness, in seconds. */
const TTL = 10

const isAnnounceHere: CommandMatch = (args) => isAnnounce(args, FLOOR)

/**
 * A command port that can refuse commands matching a predicate, from now on.
 * `issued` counts every attempt AT ISSUE, including one refused before it
 * ever reaches the fake broker (so `FakeRedis.commandLog` would miss it) —
 * the same shape as `revocation_pass_bound_362.test.ts`'s own `serialPort`.
 */
function serialPort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const sent: string[][] = []
    let failing: CommandMatch | undefined
    const command: CommandFn = (...args) => {
        sent.push(args)
        if (failing?.(args)) {
            return Promise.reject(new Error('injected: refused (#383)'))
        }
        return serial.command(...args)
    }
    return {
        command,
        /** Refuse every command matching `match` from now on. */
        failFrom: (match: CommandMatch) => void (failing = match),
        /** How many commands matching `match` were issued. */
        issued: (match: CommandMatch) => sent.filter(match).length,
    }
}

/** One driver over a serialising port, the broker clock pinned at `NOW_S`. */
function instance(interval: number) {
    const redis = new FakeRedis()
    redis.setTime(NOW_S)
    const port = serialPort(redis)
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver(
        { command: port.command },
        subscriber,
        {
            prefix: PREFIX,
            revocationTtlSeconds: TTL,
            presence: { reconcileIntervalMs: interval },
        },
    )
    return { redis, port, driver }
}

/** Captured `console.warn` lines. */
function captureLogs() {
    const warn = console.warn
    const warns: string[] = []
    console.warn = (...parts: unknown[]) => void warns.push(parts.join(' '))
    return {
        count: (prefix: string) =>
            warns.filter((l) => l.startsWith(prefix)).length,
        restore: () => void (console.warn = warn),
    }
}

type Logs = ReturnType<typeof captureLogs>

/**
 * Run `body` under FakeTime at `START`, with `performance.now` on the fake
 * clock and `console.warn` captured; everything is restored afterwards.
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

Deno.test(
    '#383 (i) a handler that enumerates every other pass does not raise SKEWED across a real gap >= TTL',
    async () => {
        await withClock(async (time, logs) => {
            const { redis, driver } = instance(1_000)
            let tick = 0
            driver.onRevocationReconcile(async () => {
                tick++
                // Odd ticks enumerate; even ticks skip it entirely — a valid
                // third-party handler, never through the manager (plan D10).
                if (tick % 2 === 1) await driver.listRevocations()
            })
            await advance(time, 1_000) // pass 1 (tick 1): enumerates at NOW_S
            redis.setTime(NOW_S + TTL) // the broker clock steps a full TTL
            await advance(time, 1_000) // pass 2 (tick 2): skips
            await advance(time, 1_000) // pass 3 (tick 3): enumerates at NOW_S+TTL
            assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 0)
            await advance(time, 5_000) // margin: nothing fires later either
            assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 0)
            await driver.close()
        })
    },
)

Deno.test(
    '#383 (ii) two genuinely consecutive enumerating passes with a gap >= TTL still raise SKEWED',
    async () => {
        await withClock(async (time, logs) => {
            const { redis, driver } = instance(1_000)
            driver.onRevocationReconcile(async () => {
                await driver.listRevocations()
            })
            await advance(time, 1_000) // pass 1 enumerates at NOW_S
            redis.setTime(NOW_S + TTL)
            await advance(time, 1_000) // pass 2 enumerates at NOW_S + TTL
            await advance(time, 250) // the carried SKEWED line's flush timer
            assertEquals(logs.count(REVOCATION_DEADLINE_SKEWED), 1)
            await driver.close()
        })
    },
)

Deno.test(
    "#383 (iii) #announceFloor's retry-stop is unchanged by an intervening skip pass",
    async () => {
        await withClock(async (time) => {
            const { port, driver } = instance(1_000)
            port.failFrom(isAnnounceHere)
            // Every periodic pass skips enumeration; the regression is that a
            // skip pass, run AFTER a stand-alone enumeration stopped the
            // retry, must never resume it.
            driver.onRevocationReconcile(() => {})
            await time.runMicrotasks() // the first-registration announce, refused
            assertEquals(
                port.issued(isAnnounceHere),
                1,
                'the first announce ran',
            )
            await advance(time, 1_200) // the retry fires once more, still refused
            assertEquals(port.issued(isAnnounceHere), 2, 'the retry re-armed')
            await driver.listRevocations() // one enumeration, outside any pass
            await advance(time, 5_000) // several skip passes run in this window
            assertEquals(
                port.issued(isAnnounceHere),
                2,
                'no further announce: the retry stayed stopped through the skip passes',
            )
            await driver.close()
        })
    },
)
