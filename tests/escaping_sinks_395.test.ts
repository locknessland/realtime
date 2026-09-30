/**
 * @fileoverview #395 — four `console.*` calls that no caller awaits no longer
 * turn a throwing log sink into an unhandled rejection.
 *
 * #391 made seven marked-fallback sinks safe. The #395 audit confirmed four
 * more log lines on a path nobody awaits: when the sink throws there, the
 * throw becomes a rejection with no handler, and on Deno an unhandled
 * rejection terminates the process.
 *
 * - E1 `LapseRun.#invoke`'s WARN: `trigger()` never awaits the run.
 * - E2 the same site, reached end to end: the manager's `#reassertRoster`
 *   WARN throws, so the lapse handler rejects into E1's site. Fixing that
 *   site contains it; the row shows the escape passes through it.
 * - E3, E4 the manager's default `onPublishError`: `broadcast` discards the
 *   publish promise (E3), and calls the sink inline on a synchronous throw
 *   (E4).
 * - E5 the Redis driver's control-subscription `.catch`, whose promise is
 *   `void`ed.
 * - E6 the Redis heartbeat's WARN, run from a `setInterval` callback that
 *   discards the promise.
 * - E7 (#380) the Redis floor announce's WARN: `onRevocationReconcile`
 *   `void`s the announce.
 *
 * **One table, one row per path.** Each row drives a real path with
 * `console.warn`, `console.error` AND `Deno.stderr.writeSync` all throwing,
 * then asserts that nothing throws synchronously, that no rejection reaches
 * the runtime, and that the site's OWN marked line was attempted, so the row
 * reached its fallback rather than passing because nothing failed, or because
 * another site's fallback ran.
 *
 * Red on `8a7600d3` (before #395): all six rows, on `no rejection reaches the
 * runtime`, except E4, which throws out of `broadcast` itself (`no synchronous
 * throw`). E2's escaped rejection was thrown at `lapse_run.ts:102`.
 *
 * @module @lockness/realtime/tests/escaping_sinks_395
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager, PUBLISH_FAILED } from '../manager.ts'
import {
    CONTROL_SUBSCRIBE_LOG_FAILED,
    HEARTBEAT_LOG_FAILED,
    RedisBroadcastDriver,
    REVOCATION_FLOOR_LOG_FAILED,
} from '../drivers/redis.ts'
import { LAPSE_RUN_LOG_FAILED, LapseRun } from '../drivers/lapse_run.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import { isAnnounce } from './revocation_wire.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: number
}

const START = new Date('2026-09-25T10:00:00Z')
const PREFIX = 'app:rt'
const ALIVE_PREFIX = `${PREFIX}__alive:`
const REVOCATIONS_KEY = `${PREFIX}__revocations`
/** The heartbeat interval every Redis row runs on, in milliseconds. */
const BEAT = 500

/** A row, armed: what drives it to its sink, and what tears it down. */
interface Armed {
    /** Drive the sink. Runs with every log channel throwing. */
    fire: () => Promise<void>
    /** Tear the fixture down, after the channels are restored. */
    dispose: () => Promise<void>
}

/** One path to a sink: its name, and how to build a fixture that reaches it. */
interface SinkRow {
    name: string
    /** The marker the row's ERROR line must start with: its own site's. */
    marker: string
    /** Words the line's subject, before any `; sink failure`, must carry. */
    subject?: string
    /**
     * Words the line's SECOND half — after `; sink failure: ` — must carry
     * (#402): the row's own marker and subject alone do not prove the line
     * carries two failures, only the first. Every row that funnels through
     * `#guardedWarn` or an equivalent try/catch on `console.warn` has one;
     * `everyChannelThrows` always throws `'warn sink down'` from
     * `console.warn`, so that text is what every such row's second half
     * carries, deterministically.
     */
    sinkFailure?: string
    /** Build the fixture. Runs with the real log channels. */
    arm: () => Promise<Armed>
}

/** A Redis driver over `command`, beating every {@link BEAT} ms. */
function redisDriver(
    redis: FakeRedis,
    command: CommandFn,
    subscriber: ConstructorParameters<typeof RedisBroadcastDriver>[1] = redis
        .subscriberFor(),
): RedisBroadcastDriver {
    return new RedisBroadcastDriver({ command }, subscriber, {
        prefix: PREFIX,
        // A secret, so a presence frame is published rather than refused
        // with a WARN that would only add noise to the arm.
        control: { secret: 'deployment-secret-with-enough-entropy' },
        revocationTtlSeconds: 10,
        presence: {
            livenessTtlSeconds: 2,
            heartbeatIntervalMs: BEAT,
            reconcileIntervalMs: 1_000,
        },
    })
}

/** Advance FakeTime by one beat in 50 ms steps, draining after each. */
async function oneBeat(time: FakeTime): Promise<void> {
    for (let left = BEAT; left > 0; left -= 50) {
        await time.tickAsync(50)
        await time.runMicrotasks()
    }
}

/** A manager over a driver whose `publish` fails as `publish` says. */
function publishRow(publish: BroadcastDriver['publish']): Promise<Armed> {
    // No onPublishError supplied: the framework's own default is under test.
    const manager = new ChannelManager<User>({
        driver: { publish, onMessage: () => {} },
    })
    return Promise.resolve({
        fire: () => {
            manager.broadcast('news', 'e', {})
            return Promise.resolve()
        },
        dispose: () => Promise.resolve(),
    })
}

/**
 * A manager over a real Redis driver, one presence member held. `fire` makes
 * the next liveness `SET` answer nil (a lapse: the key was re-created) and
 * refuses the next revocation read, so the re-assert's revocation re-check
 * fails and its WARN — `#reassertRoster`'s own — is the one that throws.
 */
async function reassertRow(): Promise<Armed> {
    const time = new FakeTime(START)
    const redis = new FakeRedis()
    let lapseOnce = false
    let refuseReadOnce = false
    const command: CommandFn = (...args) => {
        if (
            refuseReadOnce && args[0] === 'EVAL' &&
            args.includes(REVOCATIONS_KEY)
        ) {
            refuseReadOnce = false
            return Promise.reject(new Error('revocation read refused (#395)'))
        }
        if (
            lapseOnce && args[0] === 'SET' &&
            (args[1]?.startsWith(ALIVE_PREFIX) ?? false)
        ) {
            lapseOnce = false
            return redis.command(...args).then(() => ({ type: 'nil' }))
        }
        return redis.command(...args)
    }
    const driver = redisDriver(redis, command)
    const manager = new ChannelManager<User>({
        driver,
        authorize: (user): PresenceMember | false =>
            user ? { id: user.id, info: {} } : false,
    })
    const member = {
        id: 'c1',
        identity: { id: 7 },
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<User>
    manager.register(member)
    assertEquals((await manager.subscribe(member, 'presence-room')).ok, true)
    await time.runMicrotasks()
    return {
        fire: async () => {
            lapseOnce = true
            refuseReadOnce = true
            await oneBeat(time)
        },
        dispose: async () => {
            try {
                await driver.close()
            } finally {
                time.restore()
            }
        },
    }
}

const SINKS: SinkRow[] = [
    {
        name: "LapseRun.#invoke's WARN (the handler rejects)",
        marker: LAPSE_RUN_LOG_FAILED,
        subject: 're-assert failed (#395)',
        sinkFailure: 'warn sink down',
        arm: () => {
            const lapse = new LapseRun(() => {})
            lapse.register(() =>
                Promise.reject(new Error('re-assert failed (#395)'))
            )
            return Promise.resolve({
                fire: () => {
                    lapse.trigger()
                    return Promise.resolve()
                },
                dispose: () => lapse.close(),
            })
        },
    },
    {
        name: "the manager's #reassertRoster WARN, reaching LapseRun.#invoke",
        marker: LAPSE_RUN_LOG_FAILED,
        // The run failed BECAUSE #reassertRoster's own WARN threw: that throw
        // is the subject the lapse-run line reports.
        subject: 'warn sink down',
        arm: reassertRow,
    },
    {
        name: 'the default onPublishError (the publish rejects)',
        marker: PUBLISH_FAILED,
        arm: () =>
            publishRow(() =>
                Promise.reject(new Error('publish refused (#395)'))
            ),
    },
    {
        name: 'the default onPublishError (the publish throws)',
        marker: PUBLISH_FAILED,
        arm: () =>
            publishRow(() => {
                throw new Error('publish refused (#395)')
            }),
    },
    {
        name: 'the redis control subscription (subscribeOne rejects)',
        marker: CONTROL_SUBSCRIBE_LOG_FAILED,
        subject: 'subscribe refused (#395)',
        sinkFailure: 'warn sink down',
        arm: () => {
            const redis = new FakeRedis()
            const inner = redis.subscriberFor()
            const driver = redisDriver(redis, redis.command, {
                psubscribe: inner.psubscribe,
                subscribeOne: () =>
                    Promise.reject(new Error('subscribe refused (#395)')),
                unsubscribeOne: () => {},
            })
            return Promise.resolve({
                fire: () => {
                    driver.onControl(() => {})
                    return Promise.resolve()
                },
                dispose: () => driver.close(),
            })
        },
    },
    {
        name: "the redis heartbeat's WARN (the liveness write is refused)",
        marker: HEARTBEAT_LOG_FAILED,
        subject: 'liveness refused (#395)',
        sinkFailure: 'warn sink down',
        arm: async () => {
            const time = new FakeTime(START)
            const redis = new FakeRedis()
            let refusing = false
            const command: CommandFn = (...args) =>
                refusing && args[0] === 'SET' &&
                    (args[1]?.startsWith(ALIVE_PREFIX) ?? false)
                    ? Promise.reject(new Error('liveness refused (#395)'))
                    : redis.command(...args)
            const driver = redisDriver(redis, command)
            // A hold starts the heartbeat's interval.
            await driver.holdMember('presence-other', { id: 9 })
            return {
                fire: async () => {
                    refusing = true
                    await oneBeat(time)
                },
                dispose: async () => {
                    try {
                        await driver.close()
                    } finally {
                        time.restore()
                    }
                },
            }
        },
    },
    {
        // #380: the floor announce is `void`ed by `onRevocationReconcile`, so
        // a throw from its WARN would be a rejection nobody handles.
        name: "the redis floor announce's WARN (the announce is refused)",
        marker: REVOCATION_FLOOR_LOG_FAILED,
        subject: 'announce refused (#380)',
        arm: () => {
            const redis = new FakeRedis()
            const command: CommandFn = (...args) =>
                isAnnounce(args, `${PREFIX}__revocation-floor`)
                    ? Promise.reject(new Error('announce refused (#380)'))
                    : redis.command(...args)
            const driver = redisDriver(redis, command)
            return Promise.resolve({
                fire: () => {
                    driver.onRevocationReconcile(() => {})
                    return Promise.resolve()
                },
                dispose: () => driver.close(),
            })
        },
    },
]

for (const [index, row] of SINKS.entries()) {
    Deno.test(`#395 E${index + 1} ${row.name}: every channel throwing, nothing escapes`, async () => {
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
            if (row.subject !== undefined) {
                const subject = row.subject
                assert(
                    marked.some((line) =>
                        line.split('; sink failure')[0].includes(subject)
                    ),
                    `its subject carries "${subject}": ${
                        JSON.stringify(marked)
                    }`,
                )
            }
            if (row.sinkFailure !== undefined) {
                const sinkFailure = row.sinkFailure
                assert(
                    marked.some((line) => {
                        const [, second] = line.split('; sink failure: ')
                        return second !== undefined &&
                            second.includes(sinkFailure)
                    }),
                    `its second half carries "; sink failure: ${sinkFailure}", ` +
                        `not only the marker: ${JSON.stringify(marked)}`,
                )
            }
        })
    })
}

/**
 * #402: a contained WARN is not the end of the story — the work after it
 * still has to run. E1 above only proves the run does not escape; it never
 * checks that `onFailure` — the callback the Redis driver marks a lapse
 * suspected from — still fires once the run's own WARN AND its marked
 * fallback both failed.
 */
Deno.test('#395 LapseRun.#invoke: onFailure still runs after its own WARN is contained', async () => {
    await watchingEscapes(async (escaped) => {
        let onFailureRan = false
        const lapse = new LapseRun(() => {
            onFailureRan = true
        })
        lapse.register(() =>
            Promise.reject(new Error('re-assert failed (#395)'))
        )
        let thrown: unknown = undefined
        try {
            using _channels = everyChannelThrows()
            try {
                lapse.trigger()
                await settle()
            } catch (error) {
                thrown = error
            }
        } finally {
            await lapse.close()
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assert(
            onFailureRan,
            "onFailure ran even though its own run's WARN, and the " +
                'marked fallback behind it, both failed',
        )
    })
})

/**
 * #402: the heartbeat's lapse decision (`#lapseSuspected`, then
 * `#lapse.trigger()` on the next successful beat) runs AFTER the WARN it
 * writes when the liveness `SET` fails — E6 above only proves that WARN's
 * own escape is contained, never that the decision behind it survived.
 */
Deno.test('#395 the redis heartbeat: the lapse decision still runs after its WARN is contained', async () => {
    await watchingEscapes(async (escaped) => {
        const time = new FakeTime(START)
        const redis = new FakeRedis()
        let refusing = false
        const command: CommandFn = (...args) =>
            refusing && args[0] === 'SET' &&
                (args[1]?.startsWith(ALIVE_PREFIX) ?? false)
                ? Promise.reject(new Error('liveness refused (#395)'))
                : redis.command(...args)
        const driver = redisDriver(redis, command)
        let lapseTriggered = false
        driver.onRosterLapse(() => {
            lapseTriggered = true
        })
        // A hold starts the heartbeat's interval, and establishes the
        // liveness key the refused beat below re-creates — the fixture #395
        // E6 already relies on.
        await driver.holdMember('presence-other', { id: 9 })
        let thrown: unknown = undefined
        try {
            using _channels = everyChannelThrows()
            try {
                refusing = true
                await oneBeat(time)
            } catch (error) {
                thrown = error
            }
        } finally {
            // Channels restored here (scope exit); the driver stays open.
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assert(
            !lapseTriggered,
            'not yet: a refused beat only marks the lapse suspected',
        )
        refusing = false
        await oneBeat(time)
        assert(
            lapseTriggered,
            'the lapse decision survived the contained WARN: the next ' +
                'successful beat triggered a re-assert run',
        )
        await driver.close()
        time.restore()
    })
})

/**
 * #402: an offline pin (no throwing channel, no FakeTime interval) on the
 * control-subscription WARN's exact wording — inline at its one call site,
 * never a named constant, so nothing else in this suite locks its text.
 * E5 above only ever observes it through the SINK-FAILURE second half, when
 * `console.warn` itself has already been made to throw.
 */
Deno.test('#395 the redis control subscription: the WARN line is pinned', async () => {
    const redis = new FakeRedis()
    const inner = redis.subscriberFor()
    const driver = redisDriver(redis, redis.command, {
        psubscribe: inner.psubscribe,
        subscribeOne: () =>
            Promise.reject(new Error('subscribe refused (#395)')),
        unsubscribeOne: () => {},
    })
    const lines: string[] = []
    const originalWarn = console.warn
    console.warn = (...args: unknown[]) => {
        lines.push(args.map(String).join(' '))
    }
    try {
        driver.onControl(() => {})
        await settle()
    } finally {
        console.warn = originalWarn
        await driver.close()
    }
    assertEquals(lines.length, 1, 'exactly one console.warn line')
    assertEquals(
        lines[0],
        'realtime: the control subscription could not be issued — the ' +
            "driver's own retry is what restores it: Error: subscribe " +
            'refused (#395)',
    )
})
