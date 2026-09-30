/**
 * @fileoverview #419 — the two catch sites #418's security review named as
 * still calling `console.warn` directly, traced and routed through
 * `#guardedWarn`, continuing #418's own trace (T1-T7 in
 * `redis_warn_trace_418.test.ts`) as T8 and T9:
 *
 * - **T8** `#announceSwept`'s `dropped()` — its OWN drop of an entry that
 *   decoded but does not belong to the slot it was released from
 *   (`sameMemberId` fails), or whose owned entry names a channel outside
 *   `isValidName`'s charset. Unlike T3 (`redis_warn_trace_418.test.ts`),
 *   which guards the departure HANDLER's own failure, this guards
 *   `#announceSwept`'s failure BEFORE the handler is ever called — but the
 *   shape is identical: nothing between here and `#sweepPage`'s loop over
 *   one dead instance's owned slots catches a throw, so an unguarded
 *   `console.warn` failing here used to reject `#announceSwept`, aborting
 *   that loop and skipping every remaining slot on the page (and any later
 *   page) for the SAME dead instance this pass — the #395 "escapes a loop
 *   and skips the rest of it" shape.
 * - **T9** `#parseRosterValue`'s `skipped()` — the one decode of a roster
 *   entry, shared by `readRoster`'s two loops (the sample and the selves)
 *   AND by `#announceSwept`'s own decode of a swept entry (#348). Reached
 *   through `#announceSwept` here, with the identical unwrapped shape as T8:
 *   an unguarded `console.warn` failing inside the `catch` (or either `if`)
 *   branch escapes `#parseRosterValue` itself, and from there whichever of
 *   its three call sites invoked it — `#sweepPage`'s loop here, or
 *   `readRoster`'s own `for` loop when called from there instead, aborting
 *   it before a later sample or self is ever decoded.
 *
 * Both are now routed through `#guardedWarn` (#409's #369 shape, same as
 * #418): a throwing sink writes one marked ERROR line instead of escaping.
 *
 * `#parseRosterValue`'s guard is ONE closure shared by all three call sites
 * (`readRoster`'s sample loop, its selves loop, and `#announceSwept`), so
 * guarding it here also closes the escape on the `readRoster` paths — this
 * file exercises it through the sweep, the shape T3-T7 already established
 * conventions for, and the sharing itself is what makes that sufficient: a
 * mutant reverting the one `skipped` closure to a bare `console.warn` fails
 * regardless of which caller reaches it.
 *
 * Red before this fix: T8 and T9 both fail the same way T3-T7 did — either
 * `no rejection reaches the runtime`, or the "still reported" assertion,
 * whichever the mutant breaks first.
 *
 * @module @lockness/realtime/tests/announce_swept_guard_419
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    RedisBroadcastDriver,
    ROSTER_ENTRY_LOG_FAILED,
    SWEEP_DROPPED_LOG_FAILED,
} from '../drivers/redis.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

const START = new Date('2026-09-26T10:00:00Z')
const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const DEAD = 'instance-dead'
const INSTANCES_KEY = `${PREFIX}__instances`
const HOLDERS_KEY = (channel: string, id: string | number) =>
    `${PREFIX}__holders:${channel} ${String(id)}`
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`

/** A driver with the standard sweep/heartbeat tuning T8/T9 share with T3-T7. */
function redisDriver(
    redis: FakeRedis,
    command: CommandFn = redis.command,
): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1_000,
            },
        },
    )
}

/** A roster entry byte-identical to what `HOLD_MEMBER_SCRIPT` writes. */
const entry = (id: string | number, owner: string) =>
    JSON.stringify({ member: { id }, owner })

/** Write a dead instance's hold directly, as `presence_sweep_departure_348.test.ts` does. */
async function plantHold(
    redis: FakeRedis,
    channel: string,
    field: string,
    value: string,
    owner: string,
): Promise<void> {
    await redis.command('HSET', HOLDERS_KEY(channel, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(channel), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${channel} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

Deno.test("#419 T8 #announceSwept's dropped(): every channel throwing, the sweep still reaches the next slot", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const driver = redisDriver(redis)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(({ member }) => {
            reported.push(member.id)
        })
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            // The slot's owned entry names field 'mismatch', but the roster
            // entry it decodes to carries member id 'someone-else' — fails
            // `sameMemberId`, so `#announceSwept` drops it BEFORE the
            // handler is ever called (#348 A2/S1). Planted FIRST, so "the
            // sweep still reaches the next slot" is proven by the
            // well-formed one released right after it, on the SAME page.
            await plantHold(
                redis,
                CHANNEL,
                'mismatch',
                entry('someone-else', DEAD),
                DEAD,
            )
            await plantHold(redis, CHANNEL, '8', entry(8, DEAD), DEAD)
            // The driver holds something of its own, so its reconcile pass
            // runs at all.
            await driver.holdMember(OTHER, { id: 9 })
            using channels = everyChannelThrows()
            try {
                await time.tickAsync(3_500)
                await settle()
            } catch (error) {
                thrown = error
            }
            lines = [...channels.errorLines()]
        } finally {
            await driver.close()
            time.restore()
            redis.assertNoRejections()
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${SWEEP_DROPPED_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
        assert(
            marked.some((line) =>
                line.split('; sink failure')[0].includes(
                    'was not announced as left',
                )
            ),
            `its subject names the drop: ${JSON.stringify(marked)}`,
        )
        assertEquals(
            reported,
            [8],
            'the well-formed departure right after the dropped one is ' +
                "still reported — the WARN's own sink failure did not " +
                'also cost #sweepPage the rest of its page',
        )
    })
})

Deno.test("#419 T9 #parseRosterValue's skipped(), reached through #announceSwept: every channel throwing, the sweep still reaches the next slot", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const driver = redisDriver(redis)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(({ member }) => {
            reported.push(member.id)
        })
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            // Not valid JSON: `#parseRosterValue`'s `catch` branch calls
            // `skipped('not valid JSON')`. Planted FIRST, so "the sweep
            // still reaches the next slot" is proven by the well-formed one
            // released right after it, on the SAME page.
            await plantHold(redis, CHANNEL, '7', 'not json', DEAD)
            await plantHold(redis, CHANNEL, '8', entry(8, DEAD), DEAD)
            await driver.holdMember(OTHER, { id: 9 })
            using channels = everyChannelThrows()
            try {
                await time.tickAsync(3_500)
                await settle()
            } catch (error) {
                thrown = error
            }
            lines = [...channels.errorLines()]
        } finally {
            await driver.close()
            time.restore()
            redis.assertNoRejections()
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${ROSTER_ENTRY_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
        assert(
            marked.some((line) =>
                line.split('; sink failure')[0].includes(
                    'skipped a malformed roster entry',
                )
            ),
            `its subject names the skip: ${JSON.stringify(marked)}`,
        )
        assertEquals(
            reported,
            [8],
            'the well-formed departure right after the skipped one is ' +
                "still reported — the WARN's own sink failure did not " +
                'also cost #sweepPage the rest of its page',
        )
    })
})
