/**
 * @fileoverview #418 — the seven catch sites in `drivers/redis.ts` the
 * security-review audit named as still calling `console.warn` directly,
 * traced and (all seven) routed through `#guardedWarn`.
 *
 * **Traced call chains** — what calls the enclosing function, and what a
 * throwing sink does to it:
 *
 * - **T1** the malformed-Redis-payload catch in `onMessage`'s `#deliver`.
 *   `#deliver` IS the handler `RedisSubscriber.psubscribe`/`subscribeOne`
 *   calls directly for every delivered message — nothing in this class wraps
 *   that call in a `try`. The port's own JSDoc makes no promise that a
 *   throwing handler is contained (a test passes a fake bus; production
 *   supplies `@lockness/redis`'s `RedisSubscribeConnection`, whose OWN
 *   `#deliver` happens to contain a handler fault, but that is the concrete
 *   adapter's choice, not the port's contract) — and this package's own
 *   `FakeRedis` test double dispatches every subscriber synchronously, inside
 *   its `PUBLISH` fan-out loop, with no containment of its own. An unguarded
 *   throw there escapes that loop, aborting it before a LATER subscriber on
 *   the same topic is ever reached, and surfaces as a synchronous throw out
 *   of the `PUBLISH` round trip itself.
 * - **T2** the malformed-control catch in `#verifyAndDecode` — the
 *   control-topic twin of T1: `onControl`'s `deliver` closure calls
 *   `#verifyAndDecode` directly as the subscriber's handler, the identical
 *   unwrapped call site.
 * - **T3** the ghost-sweep departure-handler catch in `#announceSwept`
 *   (#348 plan §11, S2 — DELIBERATELY drops the handler's own error, never
 *   the WARN's). The `await handler(...)` it wraps sits inside
 *   `#sweepPage`'s loop over one dead instance's owned slots, with no `try`
 *   between here and there — so an unguarded `console.warn` failing rejects
 *   `#announceSwept`, which `#sweepPage` awaits with no `try` of its own,
 *   aborting the loop and skipping every remaining slot on the page (and any
 *   further page) for the SAME dead instance this pass — the #395 "escapes a
 *   loop and skips the rest of it" shape.
 * - **T4** `#sweepInstance`'s own "sweep … failed" WARN (security review of
 *   the same issue): its `try` around `#sweepOwned` already turns an ordinary
 *   sweep failure into one WARN and a normal return, but that WARN itself was
 *   bare — so when the SINK was what was down, its own throw escaped
 *   `#sweepInstance` and reached `#reconcile`'s `for` loop with no
 *   per-iteration `try` to stop it, skipping every OTHER dead instance still
 *   left in `ids` this pass (#355 A3).
 * - **T5** `#reconcile`'s own outer catch — the "roster reconcile failed"
 *   WARN. Once T4's site is guarded, `#sweepInstance` never rejects, so this
 *   catch fires only for a failure in the pass's OWN housekeeping (a broker
 *   round trip or decode failure on `SMEMBERS`/`EXISTS`), never for a single
 *   dead instance's sweep. Guarding it stops that housekeeping failure's WARN
 *   from erasing itself behind the generic top-of-chain fallback
 *   (`SWEEP_LOG_FAILED`) when the sink is what is down.
 * - **T6** `#sweepInstance`'s "instance … renewed its liveness" WARN (second
 *   security review): a SUCCESS exit, not a failure — the instance turned out
 *   to be alive, mid-sweep — but bare all the same, and just as unwrapped by
 *   any `try` as T4's site was.
 * - **T7** `#sweepInstance`'s "released N hold(s)" WARN (second security
 *   review): the other SUCCESS exit, same unwrapped shape.
 *
 * All seven escape, so all seven are now routed through `#guardedWarn`
 * (#409's #369 shape): a throwing sink writes one marked ERROR line instead
 * of escaping. Once every exit of `#sweepInstance` goes through
 * `#guardedWarn`, the method itself can never throw — `#reconcile` does NOT
 * gain a per-id `try` around its call: with nothing left that can reject it,
 * such a `try` would be dead code, or would relabel a real bug (one that
 * reaches `#sweepInstance` from OUTSIDE this file's known exits) as a routine
 * sweep failure.
 *
 * Red before this fix: T1 and T2 fail on `no synchronous throw`; T3 fails on
 * `no rejection reaches the runtime` after the second, well-formed departure
 * never gets reported; T4, T6 and T7 fail the same way after the second dead
 * instance's departure never gets reported; T5 fails because
 * `SWEEP_LOG_FAILED`, not `RECONCILE_LOG_FAILED`, is the only marked line
 * written — proof the escape climbed past this site's own report.
 *
 * @module @lockness/realtime/tests/redis_warn_trace_418
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    CONTROL_DECODE_LOG_FAILED,
    MESSAGE_DECODE_LOG_FAILED,
    RECONCILE_LOG_FAILED,
    RedisBroadcastDriver,
    SWEEP_DEPARTURE_LOG_FAILED,
    SWEEP_INSTANCE_LOG_FAILED,
    SWEEP_INSTANCE_RELEASED_LOG_FAILED,
    SWEEP_INSTANCE_RENEWED_LOG_FAILED,
    SWEEP_LOG_FAILED,
} from '../drivers/redis.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

const START = new Date('2026-09-26T10:00:00Z')
const PREFIX = 'app:rt'
const SECRET = 'deployment-secret-with-enough-entropy'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const INSTANCES_KEY = `${PREFIX}__instances`
const HOLDERS_KEY = (channel: string, id: string | number) =>
    `${PREFIX}__holders:${channel} ${String(id)}`
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const ALIVE_KEY = (instanceId: string) => `${PREFIX}__alive:${instanceId}`

/** A driver with the standard sweep/heartbeat tuning `T3`-`T5` share. */
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

/** A row, armed: what drives it to its sink, and what tears it down. */
interface Armed {
    /** Drive the sink. Runs with every log channel throwing. */
    fire: () => Promise<void>
    /** Tear the fixture down, after the channels are restored. */
    dispose: () => Promise<void>
    /**
     * Whether a LATER subscriber on the same bus still received the frame —
     * the "contained is not the end of the story" half: proof the malformed
     * frame did not abort `FakeRedis`'s `PUBLISH` fan-out before it reached
     * whichever subscriber comes after the traced site's own.
     */
    delivered: () => boolean
}

/** One traced site: its name, and how to build a fixture that reaches it. */
interface TraceRow {
    name: string
    /** The marker the row's ERROR line must start with: its own site's. */
    marker: string
    /** Words the line's subject, before any `; sink failure`, must carry. */
    subject: string
    /** Build the fixture. Runs with the real log channels. */
    arm: () => Promise<Armed> | Armed
}

const ROWS: TraceRow[] = [
    {
        name: "T1 the malformed-Redis-payload catch in onMessage's #deliver",
        marker: MESSAGE_DECODE_LOG_FAILED,
        subject: 'realtime: dropped a malformed Redis payload',
        arm: () => {
            const redis = new FakeRedis()
            const driver = new RedisBroadcastDriver(
                { command: redis.command },
                redis.subscriberFor(),
                { prefix: PREFIX },
            )
            driver.onMessage(() => {})
            const topic = `${PREFIX}__event:news`
            // A second, independent subscriber on the same glob, registered
            // AFTER the driver's own — so it sits LATER in FakeRedis's
            // PUBLISH fan-out loop than `#deliver` does.
            let secondReceived = false
            redis.subscriberFor().psubscribe(`${PREFIX}__event:*`, () => {
                secondReceived = true
            })
            return {
                // Not async, and not awaited: `redis.command(...)` dispatches
                // to every subscriber SYNCHRONOUSLY, inside its own call —
                // before it ever returns a promise. An unguarded throw here
                // is a throw out of calling `fire()` itself, exactly like
                // `escaping_sinks_395.test.ts`'s "the publish throws" row.
                fire: () => {
                    void redis.command('PUBLISH', topic, 'not json')
                    return Promise.resolve()
                },
                dispose: () => driver.close(),
                delivered: () => secondReceived,
            }
        },
    },
    {
        name: 'T2 the malformed-control-payload catch in #verifyAndDecode',
        marker: CONTROL_DECODE_LOG_FAILED,
        subject: 'realtime: dropped a malformed control payload',
        arm: () => {
            const redis = new FakeRedis()
            const driver = new RedisBroadcastDriver(
                { command: redis.command },
                redis.subscriberFor(),
                { prefix: PREFIX, control: { secret: SECRET } },
            )
            driver.onControl(() => {})
            const topic = `${PREFIX}__control`
            let secondReceived = false
            redis.subscriberFor().psubscribe(topic, () => {
                secondReceived = true
            })
            return {
                fire: () => {
                    void redis.command('PUBLISH', topic, 'not json')
                    return Promise.resolve()
                },
                dispose: () => driver.close(),
                delivered: () => secondReceived,
            }
        },
    },
]

for (const row of ROWS) {
    Deno.test(`#418 ${row.name}: every channel throwing, nothing escapes`, async () => {
        await watchingEscapes(async (escaped) => {
            const armed = await row.arm()
            let thrown: unknown = undefined
            let lines: readonly string[] = []
            try {
                using channels = everyChannelThrows()
                try {
                    await armed.fire()
                    await settle()
                } catch (error) {
                    thrown = error
                }
                lines = [...channels.errorLines()]
            } finally {
                await armed.dispose()
            }
            assertEquals(thrown, undefined, 'no synchronous throw')
            assertEquals(escaped, [], 'no rejection reaches the runtime')
            const marked = lines.filter((line) =>
                line.startsWith(`${row.marker} `)
            )
            assert(
                marked.length >= 1,
                `the site's own marked line was attempted: ${
                    JSON.stringify(lines)
                }`,
            )
            assert(
                marked.some((line) =>
                    line.split('; sink failure')[0].includes(row.subject)
                ),
                `its subject carries "${row.subject}": ${
                    JSON.stringify(marked)
                }`,
            )
            assert(
                marked.some((line) => {
                    const [, second] = line.split('; sink failure: ')
                    return second !== undefined &&
                        second.includes('warn sink down')
                }),
                `its second half carries "; sink failure: warn sink down": ${
                    JSON.stringify(marked)
                }`,
            )
            assert(
                armed.delivered(),
                'a later subscriber on the same bus still received the ' +
                    "frame — this site's own sink failure did not also " +
                    "abort FakeRedis's PUBLISH fan-out before reaching it",
            )
        })
    })
}

/**
 * T3 — a separate shape from T1/T2 (an `async` driven fixture over `FakeTime`,
 * not a synchronous `PUBLISH` fan-out), and one extra half T1/T2 have no
 * analogue for: proving the SECOND, well-formed departure on the same page
 * is still reported once the WARN is guarded — the #402 "contained is not
 * the end of the story" check, aimed at #395's own reference shape (a WARN
 * escaping a loop and skipping the rest of it).
 */
Deno.test('#418 T3 the ghost-sweep departure-handler catch in #announceSwept: every channel throwing, the sweep still reaches the next slot', async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const driver = redisDriver(redis)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(({ member }) => {
            if (member.id === 'throws') {
                throw new Error('departure handler refused (#418)')
            }
            reported.push(member.id)
        })
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            // The throwing slot FIRST, so "the sweep still reaches the next
            // slot" is proven by the well-formed one released right after it,
            // on the SAME page of the SAME dead instance's sweep.
            await plantHold(
                redis,
                CHANNEL,
                'throws',
                entry('throws', 'instance-dead'),
                'instance-dead',
            )
            await plantHold(
                redis,
                CHANNEL,
                '8',
                entry(8, 'instance-dead'),
                'instance-dead',
            )
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
            line.startsWith(`${SWEEP_DEPARTURE_LOG_FAILED} `)
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
                    'the roster departure handler failed',
                )
            ),
            `its subject names the departure-handler failure: ${
                JSON.stringify(marked)
            }`,
        )
        assertEquals(
            reported,
            [8],
            'the well-formed departure right after the throwing one is ' +
                "still reported — the WARN's own sink failure did not " +
                'also cost #sweepPage the rest of its page',
        )
    })
})

/**
 * T4 — `#sweepInstance`'s own "sweep … failed" WARN (security review): one
 * dead instance's `#sweepOwned` genuinely fails (its owned set's `SSCAN`
 * round trip is refused), so `#sweepInstance` reports it — under a broken
 * sink, that report used to escape `#sweepInstance` and abort `#reconcile`'s
 * `for` loop before it ever reached the SECOND dead instance. Proves #355
 * A3 ("one instance's failure ends only that instance's sweep") survives
 * even when the failing instance's OWN report cannot be logged.
 */
Deno.test("#418 T4 #sweepInstance's own sweep-failed WARN: every channel throwing, a second dead instance is still swept in the same pass", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const DEAD_A = 'instance-dead-a'
        const DEAD_B = 'instance-dead-b'
        const command: CommandFn = (...args) =>
            args[0] === 'SSCAN' && args[1] === OWNED_KEY(DEAD_A)
                ? Promise.reject(new Error('owned scan refused (#418)'))
                : redis.command(...args)
        const driver = redisDriver(redis, command)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(({ member }) =>
            void reported.push(member.id)
        )
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            // A, whose sweep fails outright — planted FIRST, so it is
            // attempted before B in `#reconcile`'s `for` loop (FakeRedis's
            // SMEMBERS preserves SADD insertion order).
            await plantHold(redis, CHANNEL, '7', entry(7, DEAD_A), DEAD_A)
            // B, an ordinary dead instance right after it.
            await plantHold(redis, CHANNEL, '8', entry(8, DEAD_B), DEAD_B)
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
            // A's owned entry is never released (its SSCAN never succeeds),
            // which is the fixture's point, not a bug this row swallows.
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${SWEEP_INSTANCE_LOG_FAILED} `)
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
                    'sweep of dead instance',
                )
            ),
            `its subject names the sweep failure: ${JSON.stringify(marked)}`,
        )
        assertEquals(
            reported,
            [8],
            "B's departure is still reported in the SAME pass — A's own " +
                'sweep-failure WARN losing its sink did not also cost ' +
                "#reconcile's loop the rest of `ids`",
        )
    })
})

/**
 * T5 — `#reconcile`'s own outer catch: a failure in the pass's OWN
 * housekeeping (here, the `EXISTS` round trip for one instance) is what this
 * catch reports; #395/#409's shared question is whether that report's own
 * sink failure erases itself behind the generic top-of-chain fallback
 * (`SWEEP_LOG_FAILED`) instead of this site's own, more specific marker.
 */
Deno.test("#418 T5 #reconcile's own outer catch: every channel throwing, its OWN marker fires, not the generic top-of-chain fallback", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const DEAD_C = 'instance-dead-c'
        const command: CommandFn = (...args) =>
            args[0] === 'EXISTS' && args[1] === ALIVE_KEY(DEAD_C)
                ? Promise.reject(new Error('exists refused (#418)'))
                : redis.command(...args)
        const driver = redisDriver(redis, command)
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            await redis.command('SADD', INSTANCES_KEY, DEAD_C)
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
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${RECONCILE_LOG_FAILED} `)
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
                    'roster reconcile failed',
                )
            ),
            `its subject names the reconcile failure: ${
                JSON.stringify(marked)
            }`,
        )
        assert(
            !lines.some((line) => line.startsWith(`${SWEEP_LOG_FAILED} `)),
            'the generic top-of-chain fallback never fires — this site ' +
                `contained its own escape: ${JSON.stringify(lines)}`,
        )
    })
})

/**
 * T6 — `#sweepInstance`'s "renewed" WARN (second security review): the
 * dead-turned-alive instance's very first release is refused (the alive key
 * is written right before that one `EVAL` runs, the same technique
 * `reconcile_single_pass_355.test.ts`'s `renew()` uses), so its sweep ends
 * `renewed` at 0 released. A SECOND, ordinary dead instance is planted
 * alongside it, and its departure must still be reported in the same pass.
 */
Deno.test("#418 T6 #sweepInstance's renewed WARN: every channel throwing, a second dead instance is still swept in the same pass", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const DEAD_A = 'instance-dead-a'
        const DEAD_B = 'instance-dead-b'
        let renewedOnce = false
        const command: CommandFn = async (...args) => {
            if (
                !renewedOnce && args[0] === 'EVAL' &&
                args.includes(OWNED_KEY(DEAD_A)) &&
                args.includes(ALIVE_KEY(DEAD_A))
            ) {
                // A renews right before its own release's `EVAL` runs — the
                // release script reads the alive key inside that SAME call,
                // so it sees A alive and refuses.
                renewedOnce = true
                await redis.command('SET', ALIVE_KEY(DEAD_A), '1', 'EX', '30')
            }
            return redis.command(...args)
        }
        const driver = redisDriver(redis, command)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(
            ({ member }) => void reported.push(member.id),
        )
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            await plantHold(redis, CHANNEL, '7', entry(7, DEAD_A), DEAD_A)
            await plantHold(redis, CHANNEL, '8', entry(8, DEAD_B), DEAD_B)
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
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${SWEEP_INSTANCE_RENEWED_LOG_FAILED} `)
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
                    'renewed its liveness',
                )
            ),
            `its subject preserves the original line's text: ${
                JSON.stringify(marked)
            }`,
        )
        assertEquals(
            reported,
            [8],
            "B's departure is still reported in the SAME pass — A's own " +
                'renewed WARN losing its sink did not also cost ' +
                "#reconcile's loop the rest of `ids`",
        )
    })
})

/**
 * T7 — `#sweepInstance`'s "released N hold(s)" WARN (second security
 * review): an ORDINARY successful sweep of one dead instance — no failure,
 * no renewal — still reaches this WARN, and a second dead instance's
 * departure must still be reported in the same pass when this one's sink
 * throws.
 */
Deno.test("#418 T7 #sweepInstance's released WARN: every channel throwing, a second dead instance is still swept in the same pass", async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        const DEAD_A = 'instance-dead-a'
        const DEAD_B = 'instance-dead-b'
        const driver = redisDriver(redis)
        const reported: (string | number)[] = []
        driver.onRosterDeparture?.(
            ({ member }) => void reported.push(member.id),
        )
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            await plantHold(redis, CHANNEL, '7', entry(7, DEAD_A), DEAD_A)
            await plantHold(redis, CHANNEL, '8', entry(8, DEAD_B), DEAD_B)
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
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const marked = lines.filter((line) =>
            line.startsWith(`${SWEEP_INSTANCE_RELEASED_LOG_FAILED} `)
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
                    'released 1 hold(s)',
                )
            ),
            `its subject preserves the original line's text: ${
                JSON.stringify(marked)
            }`,
        )
        assertEquals(
            reported,
            [7, 8],
            "both A's and B's departures are reported in the SAME pass — " +
                "A's own released WARN losing its sink did not also cost " +
                "#reconcile's loop the rest of `ids`",
        )
    })
})
