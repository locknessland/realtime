/**
 * @fileoverview #381 — the Redis driver's heartbeat interval fits one timer.
 *
 * The heartbeat is a `setInterval` of `presence.heartbeatIntervalMs`. On Deno a
 * delay of 2^31 ms or more is replaced by 1 ms, so an interval past the timer
 * ceiling does not beat slower — it beats every millisecond, one `SET … EX`
 * per millisecond per instance against the broker. The #293 relation bounds
 * the interval only by half the liveness TTL, and that TTL has no upper
 * bound, so a large TTL admitted such an interval. The constructor now
 * refuses it.
 *
 * Every witness pairs the interval with a TTL large enough that the #293
 * relation ADMITS it, so the refusal read here is the ceiling's and no other.
 * The ceiling is derived here, never imported: a mutated constant in the
 * driver must not move the witness with it.
 *
 * @module @lockness/realtime/tests/heartbeat_ceiling_381
 */

import { assertEquals, assertStringIncludes, assertThrows } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'

/** The largest delay one timer can hold, in milliseconds. */
const CEILING_MS = 2 ** 31 - 1

/** The ceiling's distinct fragment in the refusal. */
const REFUSAL = 'timer ceiling'

/** A subscriber that never delivers: construction is all a witness needs. */
const quietSubscriber = { psubscribe: () => {} }

/**
 * The smallest whole-second liveness TTL the #293 relation accepts for
 * `intervalMs`: twice the interval, rounded up to a second.
 */
function admittingTtlSeconds(intervalMs: number): number {
    return Math.ceil((intervalMs * 2) / 1000)
}

/** Construct a driver whose heartbeat is `intervalMs`, the TTL admitting it. */
function build(intervalMs: number, livenessTtlSeconds?: number) {
    return new RedisBroadcastDriver(
        { command: () => Promise.resolve(null) },
        quietSubscriber,
        {
            prefix: 'app:rt',
            presence: {
                heartbeatIntervalMs: intervalMs,
                livenessTtlSeconds: livenessTtlSeconds ??
                    admittingTtlSeconds(intervalMs),
            },
        },
    )
}

Deno.test('#381 H1 an interval one past the timer ceiling is refused at construction', () => {
    // 2^31 is the FIRST delay Deno clamps to 1 ms: a guard one too loose
    // (`> 2 ** 31`) still admits it, so this value is the one that matters.
    const error = assertThrows(() => build(CEILING_MS + 1), Error)
    assertEquals(error.constructor, Error, 'a plain Error')
    assertStringIncludes(error.message, REFUSAL)
    assertStringIncludes(error.message, '#381')
    // Far past the ceiling as well, with the TTL still admitting it.
    assertThrows(() => build(10 * CEILING_MS), Error, REFUSAL)
})

Deno.test('#381 H2 an interval of exactly the timer ceiling is accepted', () => {
    // The ceiling is the largest delay that still waits; the guard is
    // `> ceiling`, not `>= ceiling`. Pinned so a tightening is seen.
    build(CEILING_MS)
})

Deno.test('#381 H3 the refusal names the interval, the liveness TTL and the ceiling', () => {
    const interval = CEILING_MS + 1
    const ttl = admittingTtlSeconds(interval)
    const error = assertThrows(() => build(interval, ttl), Error, REFUSAL)
    assertStringIncludes(error.message, `heartbeatIntervalMs=${interval}ms`)
    assertStringIncludes(error.message, `livenessTtlSeconds=${ttl}s`)
    assertStringIncludes(error.message, `${CEILING_MS}ms`)
})
