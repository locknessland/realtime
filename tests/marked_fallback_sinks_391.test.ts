/**
 * @fileoverview #391 — no marked-fallback sink throws past itself, even when
 * every log channel it has refuses the line.
 *
 * #391 found seven sinks in this package that end a chain with no caller
 * left, and this table has one row for each; #395 and #380 added more, whose
 * rows live in `escaping_sinks_395.test.ts` (`git grep writeMarkedFallback`
 * is the current count). Each is the #369
 * shape, `try { console.warn } catch { console.error(MARKER …) }`, or a
 * chain's last `.catch` writing one marked line. Before #391 none of them
 * guarded its own `console.error`, so a log sink that refused the ERROR as
 * well turned the fallback into the very rejection it exists to stop — and on
 * Deno an unhandled rejection terminates the process.
 *
 * **One table, one row per sink.** Each row drives a real path to its sink
 * with `console.warn`, `console.error` AND `Deno.stderr.writeSync` all
 * throwing, then asserts three things:
 *
 * - nothing throws synchronously out of the drive — the deadline's sink runs
 *   in a timer callback, where a throw is an uncaught exception, not a
 *   rejection, and FakeTime's `tickAsync` surfaces it;
 * - no rejection reaches the runtime ({@link watchingEscapes});
 * - the row's OWN marker was written to `console.error` (#399) — not merely
 *   that `console.error` was called at least once, which passes on any
 *   row's line, including one that escaped from a DIFFERENT sink.
 *
 * A separate test below proves the fallback reaches PAST a refusing console,
 * onto stderr (#399): none of S1–S7 can, because {@link everyChannelThrows}
 * makes stderr throw too, by design — that is what proves H3's "last resort"
 * is reachable at all, not merely that `console.error` was attempted.
 *
 * Each row's path is chosen so that it is the ONLY path to its sink: the
 * pass-sample row drives the handler's REJECTION, not a synchronous throw,
 * because a synchronous one would be caught again by the revocation chain's
 * own last handler and hide a regression at the pass-sample site.
 *
 * Red on `587bb7ca` (before #391): all seven rows.
 *
 * @module @lockness/realtime/tests/marked_fallback_sinks_391
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    buildEvents,
    HOOK_FAILED_TOO,
    UNHANDLED_WEBSOCKET_ERROR,
} from '../websocket.ts'
import { ChannelManager, REVOCATION_APPLY_LOG_FAILED } from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import {
    PASS_SAMPLE_LOG_FAILED,
    RECONCILE_LOG_FAILED,
    RedisBroadcastDriver,
} from '../drivers/redis.ts'
import {
    EnforcementDeadline,
    REVOCATION_LOG_FAILED,
} from '../drivers/enforcement_deadline.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection, WebSocketHooks, WSContext } from '../types.ts'
import type { MarkedFallbackMarker } from '../marked_fallback.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import {
    consoleRefuses,
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: number
}

const START = new Date('2026-09-25T10:00:00Z')
const PREFIX = 'app:rt'
/** The one interval both Redis passes run on, in milliseconds. */
const INTERVAL = 1_000
/** The revocation TTL, in seconds: at least twice the interval (#362). */
const TTL = 10

/** A row, armed: what drives it to its sink, and what tears it down. */
interface Armed {
    /** Drive the sink. Runs with every log channel throwing. */
    fire: () => Promise<void>
    /** Tear the fixture down, after the channels are restored. */
    dispose: () => Promise<void>
}

/** One sink: its name, and how to build a fixture that reaches it. */
interface SinkRow {
    name: string
    /** The marker this row's own line must start with (#399). */
    marker: MarkedFallbackMarker
    /** Build the fixture. Runs with the real log channels. */
    arm: () => Promise<Armed>
}

/** A socket context that accepts every frame and every close. */
const quietSocket = {
    send: () => {},
    close: () => {},
} as unknown as WSContext

/** A `buildEvents` fixture whose application `onMessage` throws. */
function websocketRow(hooks: WebSocketHooks<User>): Promise<Armed> {
    const events = buildEvents<User>({
        ...hooks,
        onMessage: () => {
            throw new Error('app onMessage failed (#391)')
        },
    }, { id: 1 })
    return Promise.resolve({
        fire: () => {
            events.onMessage?.({ data: 'x' } as MessageEvent, quietSocket)
            return Promise.resolve()
        },
        dispose: () => Promise.resolve(),
    })
}

/**
 * A Redis driver under FakeTime, over `command` (the fake's own port unless a
 * row replaces it). Its `fire` advances one interval in short steps, draining
 * the microtask queue after each.
 */
function redisRow(
    wire: (driver: RedisBroadcastDriver) => Promise<void> | void,
    port: (redis: FakeRedis) => CommandFn = (redis) => redis.command,
): Promise<Armed> {
    const time = new FakeTime(START)
    const redis = new FakeRedis()
    const driver = new RedisBroadcastDriver(
        { command: port(redis) },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            revocationTtlSeconds: TTL,
            presence: { reconcileIntervalMs: INTERVAL },
        },
    )
    return Promise.resolve(wire(driver)).then(() => ({
        fire: async () => {
            for (let left = INTERVAL; left > 0; left -= 250) {
                await time.tickAsync(250)
                await time.runMicrotasks()
            }
        },
        dispose: async () => {
            try {
                await driver.close()
            } finally {
                time.restore()
            }
        },
    }))
}

/**
 * A manager owning one subscribed socket, over a driver whose control seam
 * the row holds and whose `unwatchChannel` rejects, so an `evict` has a
 * teardown failure to WARN about (the #376 fixture).
 */
async function managerRow(): Promise<Armed> {
    const memory = new MemoryBroadcastDriver()
    let deliver: ((control: ControlMessage) => void) | undefined
    const driver: BroadcastDriver = {
        publish: (message) => memory.publish(message),
        onMessage: (handler) => memory.onMessage(handler),
        watchChannel: () => Promise.resolve(),
        unwatchChannel: () =>
            Promise.reject(new Error('broker unwatch failed')),
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: () => Promise.reject(new Error('broker clear failed')),
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: () => true,
    })
    const victim = {
        id: 'c1',
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<User>
    manager.register(victim)
    assertEquals((await manager.subscribe(victim, 'private-room')).ok, true)
    assert(deliver !== undefined, 'the control seam was registered')
    const send = deliver
    return {
        fire: () => {
            send({ kind: 'evict', target: 'c1' })
            return Promise.resolve()
        },
        dispose: () => Promise.resolve(),
    }
}

const SINKS: SinkRow[] = [
    {
        name: "websocket reportError's #369 marked line (onError throws)",
        marker: HOOK_FAILED_TOO,
        arm: () =>
            websocketRow({
                onError: () => {
                    throw new Error('app onError failed (#391)')
                },
            }),
    },
    {
        name: "websocket reportError's default line (no onError)",
        marker: UNHANDLED_WEBSOCKET_ERROR,
        arm: () => websocketRow({}),
    },
    {
        name: 'redis revocation-pass chain (REVOCATION_LOG_FAILED)',
        marker: REVOCATION_LOG_FAILED,
        arm: () =>
            redisRow((driver) =>
                driver.onRevocationReconcile(() => {
                    throw new Error('re-check down (#391)')
                })
            ),
    },
    {
        name: 'redis #reconcile catch (RECONCILE_LOG_FAILED)',
        marker: RECONCILE_LOG_FAILED,
        // The instance-set read is refused, so #reconcile's own catch WARNs;
        // that WARN is now guarded (#418, security review), so its throw is
        // contained right there instead of climbing to the chain's generic
        // last handler (SWEEP_LOG_FAILED) — this fixture can no longer reach
        // that marker at all.

        arm: () =>
            redisRow(
                async (driver) => {
                    await driver.holdMember('presence-other', { id: 9 })
                },
                (redis) => (...args) =>
                    args[0] === 'SMEMBERS'
                        ? Promise.reject(new Error('refused (#391)'))
                        : redis.command(...args),
            ),
    },
    {
        name:
            "redis #warnPassSample (PASS_SAMPLE_LOG_FAILED, handler's rejection)",
        marker: PASS_SAMPLE_LOG_FAILED,
        arm: () =>
            redisRow((driver) => {
                driver.onRevocationReconcile(async () => {
                    await driver.listRevocations()
                })
                driver.onPassComplete(() =>
                    Promise.reject(new Error('handler down (#391)'))
                )
            }),
    },
    {
        name:
            "enforcement deadline #write (REVOCATION_LOG_FAILED, the fire's WARN)",
        marker: REVOCATION_LOG_FAILED,
        arm: () => {
            const time = new FakeTime(START)
            const deadline = new EnforcementDeadline({
                ttlMs: TTL * 1000,
                now: () => 0,
                inFlight: () => undefined,
            })
            return Promise.resolve({
                // An overdue arm decides MISSED now and writes it on a 0 ms
                // timer: the sink runs inside that timer's callback.
                fire: async () => {
                    deadline.arm(0)
                    await time.tickAsync(0)
                },
                dispose: () => {
                    deadline.close()
                    time.restore()
                    return Promise.resolve()
                },
            })
        },
    },
    {
        name: 'manager #dispatchRevocation (REVOCATION_APPLY_LOG_FAILED)',
        marker: REVOCATION_APPLY_LOG_FAILED,
        arm: managerRow,
    },
]

for (const [index, row] of SINKS.entries()) {
    Deno.test(`#391 S${index + 1} ${row.name}: every channel throwing, nothing escapes`, async () => {
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
                lines = channels.errorLines()
            } finally {
                await armed.dispose()
            }
            assertEquals(thrown, undefined, 'no synchronous throw')
            assertEquals(escaped, [], 'no rejection reaches the runtime')
            assert(
                lines.some((line) => line.startsWith(`${row.marker} `)),
                `this row's OWN marker was written, not another row's: ${
                    JSON.stringify(lines)
                }`,
            )
        })
    })
}

/**
 * The fallback reaches PAST a refusing console, onto stderr (#399): none of
 * S1–S7 above can prove this — {@link everyChannelThrows} makes stderr throw
 * too, on purpose, so H3's "last resort" catch is reachable at all. One row
 * (S2's fixture: the simplest, no driver, no FakeTime) is driven again here
 * with stderr left working, and the write it received is asserted on
 * directly.
 */
Deno.test('#391 stderr fallback: a refusing console still reaches stderr', async () => {
    await watchingEscapes(async (escaped) => {
        const armed = await websocketRow({})
        let thrown: unknown = undefined
        let lines: readonly string[] = []
        try {
            using channels = consoleRefuses()
            try {
                await armed.fire()
                await settle()
            } catch (error) {
                thrown = error
            }
            lines = channels.stderrLines()
        } finally {
            await armed.dispose()
        }
        assertEquals(thrown, undefined, 'no synchronous throw')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assert(
            lines.some((line) =>
                line.startsWith(`${UNHANDLED_WEBSOCKET_ERROR} `)
            ),
            `stderr carried the default line: ${JSON.stringify(lines)}`,
        )
    })
})
