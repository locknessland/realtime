/**
 * @fileoverview #384 — the revocation re-check and the ghost sweep report how
 * many units each pass attempted and how many failed, and the enforcement
 * deadline re-arms only on a clean pass.
 *
 * `ok` meant **the enumeration completed**, never that every record was
 * applied, and #362's deadline re-armed on every `ok` pass. A revoked socket
 * whose `Connection.close` throws therefore stayed open and owned, failed on
 * every pass, re-armed the deadline every pass, and was reaped at its TTL with
 * nothing but one WARN per pass to show for it. These witnesses pin the
 * counts, the clean-pass rule, and the one line such an episode now writes.
 *
 * **T witnesses** drive one `ChannelManager` over a local driver double that
 * implements the revocation store and the roster, and call the re-check the
 * manager registered, exactly as a driver's pass does. **Every failure goes
 * through a real path** (plan A1): a roster release that rejects, which is how
 * `unsubscribe` and `disconnect` really reject, or a `Connection.close` that
 * throws. Nothing stubs `unsubscribe` or `disconnect` themselves: that would
 * test a path the manager cannot reach.
 *
 * **R, S and E witnesses** drive one `RedisBroadcastDriver` over a FakeRedis,
 * under FakeTime with `performance.now` pointed at its clock (FakeTime does
 * not fake it). R registers the re-check directly on the driver, so a witness
 * chooses exactly what the handler resolves to; E wires a real
 * `ChannelManager` over the driver.
 *
 * FakeTime fires every timer due within one `tickAsync` with no microtask
 * between them, so time is advanced in short steps with the microtask queue
 * drained after each ({@link advance}).
 *
 * @module @lockness/realtime/tests/revocation_tally_384
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver, Revocation, RevocationTally } from '../driver.ts'
import {
    type PassSample,
    RedisBroadcastDriver,
    REVOCATION_TALLY_MALFORMED,
} from '../drivers/redis.ts'
import {
    REVOCATION_DEADLINE_MISSED,
    REVOCATION_DEADLINE_SKEWED,
    REVOCATION_DEADLINE_STALLED,
    REVOCATION_LOG_FAILED,
} from '../drivers/enforcement_deadline.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    type CommandFn,
    type CommandGate,
    type CommandMatch,
    FakeRedis,
    serializedCommands,
} from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

/** The presence channel every T witness joins. */
const ROOM = 'presence-room'

/** The WARN `#applyRevocation` writes when an apply throws. */
const APPLY_FAILED = 'realtime: applying a revocation'

/** A connection that records its frames and closes, and can refuse to close. */
type TestConnection = Connection<User> & {
    readonly closes: number[]
}

/**
 * A connection double. With `closeThrows`, every `close` throws AFTER
 * recording the call: the hard-close is attempted and fails, which is the
 * failure that repeats on every pass (plan §1, A1).
 */
function conn(
    id: string,
    userId: number,
    options: { closeThrows?: boolean } = {},
): TestConnection {
    const closes: number[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: (code?: number) => {
            closes.push(code ?? 1000)
            if (options.closeThrows) {
                throw new Error('injected: the socket refused to close (#384)')
            }
        },
        closes,
    } as TestConnection
}

/**
 * How a stub store keys a record: by its exact id when it has one (#337),
 * so two revocations of one pair stay two records.
 */
const key = (r: Revocation) => r.channel === undefined ? r.target : r.id

/**
 * Captured WARN and ERROR lines. `console.warn` can be made to throw for the
 * lines a predicate selects, which is how a log sink refuses one line and
 * accepts the next.
 */
function captureLogs() {
    const warn = console.warn
    const error = console.error
    const warns: string[] = []
    const errors: string[] = []
    /** Every WARN line and every handler call, in the order they happened. */
    const events: string[] = []
    let refuse: (line: string) => boolean = () => false
    console.warn = (...parts: unknown[]) => {
        const line = parts.join(' ')
        if (refuse(line)) throw new Error('injected: warn sink down (#384)')
        warns.push(line)
        events.push(`warn:${line}`)
    }
    console.error = (...parts: unknown[]) => void errors.push(parts.join(' '))
    return {
        events,
        /** WARN lines starting with `prefix`. */
        warned: (prefix: string) => warns.filter((l) => l.startsWith(prefix)),
        /** ERROR lines starting with `prefix`. */
        errored: (prefix: string) => errors.filter((l) => l.startsWith(prefix)),
        /** Every WARN line starting with a deadline constant. */
        deadlineLines: () =>
            warns.filter((l) =>
                [
                    REVOCATION_DEADLINE_STALLED,
                    REVOCATION_DEADLINE_MISSED,
                    REVOCATION_DEADLINE_SKEWED,
                ].some((c) => l.startsWith(c))
            ),
        /** Make `console.warn` throw for every line `predicate` selects. */
        refuseWarn: (predicate: (line: string) => boolean) =>
            void (refuse = predicate),
        restore: () => {
            console.warn = warn
            console.error = error
        },
    }
}

// ---------------------------------------------------------------------------
// The manager harness
// ---------------------------------------------------------------------------

/**
 * One `ChannelManager` over a local driver double: a revocation index, a
 * roster, and the re-check the manager registers. No control plane — nothing
 * here publishes a frame, and every record is planted in the index directly.
 *
 * The roster release can be made to reject ONCE for one member: that is the
 * real path to a rejecting leave, and so to a rejecting `disconnect`.
 */
function managerHarness() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const index = new Map<string, Revocation>()
    const releaseFailsOnce = new Set<string>()
    let clearFails = false
    let listFails = false
    let reconciler: (() => unknown) | undefined

    const driver: BroadcastDriver = {
        publish() {},
        onMessage() {},
        onRevocationReconcile(handler) {
            reconciler = handler
        },
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            if (releaseFailsOnce.delete(String(memberId))) {
                return Promise.reject(
                    new Error('injected: the roster release failed (#384)'),
                )
            }
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                [...(roster.get(channel)?.values() ?? [])],
                limit,
                selfIds,
            )
        },
        markRevocation(revocation) {
            index.set(key(revocation), revocation)
            return Promise.resolve()
        },
        // Every record, local or not: the manager's own ownership filter is
        // what decides, and T5 asserts that it does.
        listRevocations() {
            if (listFails) {
                return Promise.reject(
                    new Error('injected: the index read failed (#384)'),
                )
            }
            return Promise.resolve([...index.values()])
        },
        clearRevocation(revocation) {
            if (clearFails) {
                return Promise.reject(
                    new Error('injected: the clear failed (#384)'),
                )
            }
            index.delete(key(revocation))
            return Promise.resolve()
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    } as BroadcastDriver
    const manager = new ChannelManager<User>({
        driver,
        authorize: (identity) => identity ? { id: identity.id } : false,
    })
    return {
        manager,
        index,
        /** Register a connection. */
        connect: (id: string, userId: number, closeThrows = false) => {
            const connection = conn(id, userId, { closeThrows })
            manager.register(connection)
            return connection
        },
        /** Join `connection` to the presence room. */
        join: async (connection: TestConnection) => {
            const result = await manager.subscribe(connection, ROOM)
            assert(result.ok, 'precondition: the join is admitted')
        },
        /** Plant one record in the index, as `markRevocation` writes it. */
        plant: (revocation: Revocation) =>
            void index.set(key(revocation), revocation),
        /** The NEXT roster release of `memberId` rejects. */
        failReleaseOnce: (memberId: number) =>
            void releaseFailsOnce.add(String(memberId)),
        /** Every `clearRevocation` rejects from now on. */
        failClear: () => void (clearFails = true),
        /** Every `listRevocations` rejects from now on. */
        failList: () => void (listFails = true),
        /**
         * Run the re-check once, as a driver's pass calls it, and hand back
         * what it resolved to — the handler's public type (FR-002).
         */
        recheck: async (): Promise<RevocationTally | void> => {
            assert(
                reconciler,
                'precondition: the manager registered a re-check',
            )
            return await reconciler() as RevocationTally | void
        },
    }
}

/**
 * Run `body` with the console captured, restored afterwards: every T witness
 * produces WARNs by design.
 */
async function withLogs(
    body: (logs: ReturnType<typeof captureLogs>) => Promise<void>,
): Promise<void> {
    const logs = captureLogs()
    try {
        await body(logs)
    } finally {
        logs.restore()
    }
}

// ---------------------------------------------------------------------------
// US1 — the re-check resolves a tally
// ---------------------------------------------------------------------------

Deno.test('#384 T1 the re-check resolves how many applies it attempted: a pair with two ids is one', async () => {
    await withLogs(async () => {
        const h = managerHarness()
        h.connect('c1', 1)
        h.connect('c2', 2)
        const c3 = h.connect('c3', 3)
        await h.join(c3)
        h.plant({ target: 'c1' })
        h.plant({ target: 'c2' })
        h.plant({ target: 'c3', channel: ROOM, id: 'r1' })
        h.plant({ target: 'c3', channel: ROOM, id: 'r2' })
        assertEquals(await h.recheck(), { attempted: 3, failed: 0 })
    })
})

Deno.test('#384 T2 a pair whose roster release rejects is one failure; the next pass finds it not-subscribed and is clean', async () => {
    await withLogs(async (logs) => {
        const h = managerHarness()
        const c1 = h.connect('c1', 1)
        const c2 = h.connect('c2', 2)
        const c3 = h.connect('c3', 3)
        await h.join(c3)
        h.plant({ target: 'c1' })
        h.plant({ target: 'c2' })
        h.plant({ target: 'c3', channel: ROOM, id: 'r1' })
        h.plant({ target: 'c3', channel: ROOM, id: 'r2' })
        h.failReleaseOnce(3)
        assertEquals(await h.recheck(), { attempted: 3, failed: 1 })
        assertEquals(c1.closes, [4403], 'c1 was revoked')
        assertEquals(c2.closes, [4403], 'c2 was revoked')
        assertEquals(logs.warned(APPLY_FAILED).length, 1)
        // The leave dropped the membership before the release rejected, so the
        // pair is 'not-subscribed' now: applied, and clean. The two connection
        // records are foreign now — their sockets were torn down above — so
        // only the pair is attempted (plan §9: a connection record is never
        // cleared, and a revoked connection is no longer this instance's).
        assertEquals(await h.recheck(), { attempted: 1, failed: 0 })
    })
})

Deno.test('#384 T3 a connection revocation whose disconnect rejects is one failure, the socket is closed anyway, and the next pass has nothing local', async () => {
    await withLogs(async (logs) => {
        const h = managerHarness()
        const c1 = h.connect('c1', 1)
        await h.join(c1)
        h.plant({ target: 'c1' })
        h.failReleaseOnce(1)
        assertEquals(await h.recheck(), { attempted: 1, failed: 1 })
        assertEquals(c1.closes, [4403], 'the hard-close ran first')
        assertEquals(
            logs.warned('realtime: evict teardown for').length,
            1,
            "revokeLocal's own WARN reports the teardown",
        )
        // `disconnect` forgets the connection even when it throws: its record
        // is foreign from now on.
        assertEquals(h.manager.connectionCount, 0)
        assertEquals(await h.recheck(), { attempted: 0, failed: 0 })
    })
})

Deno.test("#384 T4 an apply whose own WARN throws is counted by the wrapper's catch, and the next revocation is still applied", async () => {
    await withLogs(async (logs) => {
        const h = managerHarness()
        h.connect('c1', 1, true)
        const c2 = h.connect('c2', 2)
        h.plant({ target: 'c1' })
        h.plant({ target: 'c2' })
        // The sink refuses #applyRevocation's line only; the wrapper's WARN
        // is accepted.
        logs.refuseWarn((line) => line.startsWith(APPLY_FAILED))
        assertEquals(await h.recheck(), { attempted: 2, failed: 1 })
        assertEquals(c2.closes, [4403], 'the revocation after it was applied')
        assertEquals(
            logs.warned('realtime: a durable revocation could not be applied')
                .length,
            1,
        )
    })
})

Deno.test('#384 T5 a record the ownership filter drops is not attempted', async () => {
    await withLogs(async () => {
        const h = managerHarness()
        h.connect('c1', 1)
        h.plant({ target: 'elsewhere-1' })
        h.plant({ target: 'c1' })
        h.plant({ target: 'elsewhere-2' })
        assertEquals(await h.recheck(), { attempted: 1, failed: 0 })
    })
})

Deno.test('#384 T6 a pair whose leave succeeds and whose clear rejects is applied, not failed', async () => {
    await withLogs(async (logs) => {
        const h = managerHarness()
        const c3 = h.connect('c3', 3)
        await h.join(c3)
        h.plant({ target: 'c3', channel: ROOM, id: 'r1' })
        h.failClear()
        assertEquals(await h.recheck(), { attempted: 1, failed: 0 })
        assert(
            logs.warned('realtime: the revocation record for').length >= 1,
            "precondition: the clear failed, and #clearRevocation's WARN said so",
        )
    })
})

Deno.test('#384 T7 an index read that rejects rejects the re-check: there is no tally', async () => {
    await withLogs(async () => {
        const h = managerHarness()
        h.connect('c1', 1)
        h.plant({ target: 'c1' })
        h.failList()
        await assertRejects(
            () => h.recheck(),
            Error,
            'injected: the index read failed (#384)',
        )
    })
})

Deno.test('#384 T8 a socket that refuses to close fails on every pass, and stays registered', async () => {
    await withLogs(async () => {
        const h = managerHarness()
        const c1 = h.connect('c1', 1, true)
        h.plant({ target: 'c1' })
        for (let pass = 1; pass <= 3; pass++) {
            assertEquals(
                await h.recheck(),
                { attempted: 1, failed: 1 },
                `pass ${pass}`,
            )
        }
        assertEquals(
            c1.closes,
            [4403, 4403, 4403],
            'a close was tried each time',
        )
        assertEquals(h.manager.connectionCount, 1, 'the socket is still owned')
    })
})

// ---------------------------------------------------------------------------
// The Redis harness
// ---------------------------------------------------------------------------

const START = new Date('2026-09-24T10:00:00Z')
/** The fake broker's `TIME`, pinned. */
const NOW_S = Math.floor(START.getTime() / 1000)
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const INSTANCES_KEY = `${PREFIX}__instances`
const OWNED_KEY = (id: string) => `${PREFIX}__owned:${id}`
const ALIVE_KEY = (id: string) => `${PREFIX}__alive:${id}`
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string) =>
    `${PREFIX}__holders:${channel}:${id}`
/** The channel every planted ghost hold lives in. */
const GHOST_CHANNEL = 'presence-ghosts'
/** The channel the sweeping driver holds its own member in. */
const OTHER = 'presence-other'
/** The one interval both passes run on, in milliseconds. */
const INTERVAL = 1_000
/** The revocation TTL, in seconds: at least twice the interval (#362). */
const TTL = 10
const DEAD = 'instance-dead'
const DEAD_TOO = 'instance-dead-too'
/** A peer whose liveness key is present. */
const LIVE = 'instance-live'

/** A liveness probe. */
const isExists: CommandMatch = (args) => args[0] === 'EXISTS'

/** A sweep release of one of `owner`'s holds (it names a holders hash). */
const isRelease = (owner: string): CommandMatch => (args) =>
    args[0] === 'EVAL' && args.includes(OWNED_KEY(owner)) &&
    args.some((a) => a.startsWith(`${PREFIX}__holders:`))

/**
 * A command port serialised as the production client is: one reply can be
 * held, and a class of command can be refused from now on.
 */
function overridePort(redis: FakeRedis) {
    const serial = serializedCommands(redis.command)
    const failing: CommandMatch[] = []
    const command: CommandFn = (...args) => {
        if (failing.some((match) => match(args))) {
            return Promise.reject(new Error('injected: refused (#384)'))
        }
        return serial.command(...args)
    }
    return {
        command,
        /** Hold the reply of the NEXT command matching `match`. */
        hold: (match: CommandMatch): CommandGate => serial.hold(match),
        /** Refuse every command matching `match` from now on. */
        failFrom: (match: CommandMatch) => void failing.push(match),
    }
}

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
        samples,
        /** The sweep samples received. */
        sweeps: () => samples.filter((s) => s.pass === 'sweep'),
        /** The revocation samples received. */
        revocations: () => samples.filter((s) => s.pass === 'revocation'),
        /** Register the recording handler. */
        record: () => driver.onPassComplete((s) => void samples.push(s)),
        /**
         * Register a re-check directly on the driver that resolves to what
         * `resolve` returns. Typed `() => void` on purpose: a handler that
         * compiles as `() => void` can still resolve a stray value (plan A3),
         * which is exactly what R3a and R3b feed the driver.
         */
        listen: (resolve: () => unknown) =>
            driver.onRevocationReconcile(resolve as () => void),
        /** Hold a member, which starts the heartbeat and the sweep timer. */
        startSweep: () => driver.holdMember(OTHER, { id: 9 }),
        /** Fire the subscribe socket's reconnect seam. */
        reconnect: () => subscriber.fireReconnect(),
    }
}

/** Write `owner`'s hold of `field` exactly as `HOLD_MEMBER_SCRIPT` would. */
async function plantHold(redis: FakeRedis, field: string, owner: string) {
    const value = JSON.stringify({ member: { id: Number(field) }, owner })
    await redis.command('HSET', HOLDERS_KEY(GHOST_CHANNEL, field), owner, value)
    await redis.command('HSET', PRESENCE_KEY(GHOST_CHANNEL), field, value)
    await redis.command('SADD', OWNED_KEY(owner), `${GHOST_CHANNEL} ${field}`)
    await redis.command('SADD', INSTANCES_KEY, owner)
}

/**
 * Point `performance.now` at FakeTime's clock, measured from now: a real
 * `performance.now` is a small monotonic reading, never an epoch one.
 */
function stubPerformanceNow(time: FakeTime) {
    const original = performance.now
    const origin = time.now
    performance.now = () => time.now - origin
    return { restore: () => void (performance.now = original) }
}

/** Everything an R, S or E witness runs under. */
interface Harness {
    readonly time: FakeTime
    readonly logs: ReturnType<typeof captureLogs>
}

/**
 * Run `body` under FakeTime at `START`, with `performance.now` on the fake
 * clock and the console captured; all of it is restored afterwards.
 */
async function withClock(body: (h: Harness) => Promise<void>): Promise<void> {
    const time = new FakeTime(START)
    const clock = stubPerformanceNow(time)
    const logs = captureLogs()
    try {
        await body({ time, logs })
    } finally {
        logs.restore()
        clock.restore()
        time.restore()
    }
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

/** Milliseconds since `START` on the fake clock. */
const elapsed = (time: FakeTime) => time.now - START.getTime()

// ---------------------------------------------------------------------------
// US1 — each revocation sample carries the tally
// ---------------------------------------------------------------------------

Deno.test('#384 R1 a handler that resolves a tally puts it on the sample; the outcome stays ok', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        f.listen(() => ({ attempted: 4, failed: 1 }))
        await advance(time, INTERVAL)
        assertEquals(f.revocations().length, 1)
        const [sample] = f.revocations()
        assertEquals(sample.outcome, 'ok')
        assertEquals(sample.attempts, 4)
        assertEquals(sample.failures, 1)
        assert(Object.isFrozen(sample), 'the sample is still frozen')
        await f.driver.close()
    })
})

Deno.test('#384 R2 a handler that resolves nothing puts neither key on the sample', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        f.listen(() => undefined)
        await advance(time, INTERVAL)
        assertEquals(f.revocations().length, 1)
        const [sample] = f.revocations()
        assertEquals(sample.outcome, 'ok')
        assert(!('attempts' in sample), 'no attempts key')
        assert(!('failures' in sample), 'no failures key')
        await f.driver.close()
    })
})

Deno.test('#384 R3a a value that is not tally-shaped is no tally: no counts, no WARN, and the deadline behaves as today', async (t) => {
    const values: [string, unknown][] = [
        ['a string', 'x'],
        ['a number', 42],
        ['null', null],
        ['an array', []],
        ['an empty object', {}],
    ]
    for (const [name, value] of values) {
        await t.step(name, async () => {
            await withClock(async ({ time, logs }) => {
                const f = fleet()
                f.record()
                f.listen(() => value)
                await advance(time, 30_000)
                assert(f.revocations().length >= 25, 'precondition: passes ran')
                for (const sample of f.revocations()) {
                    assertEquals(sample.outcome, 'ok')
                    assert(!('attempts' in sample), 'no attempts key')
                    assert(!('failures' in sample), 'no failures key')
                }
                assertEquals(logs.warned(REVOCATION_TALLY_MALFORMED), [])
                assertEquals(logs.deadlineLines(), [])
                await f.driver.close()
            })
        })
    }
})

Deno.test('#384 R3b a tally-shaped value with bad counts is one WARN per pass, no counts, ok, no retry, and never re-arms', async (t) => {
    const values: [string, () => unknown][] = [
        ['failed above attempted', () => ({ attempted: 1, failed: 2 })],
        ['a negative count', () => ({ attempted: -1, failed: 0 })],
        ['a fractional count', () => ({ attempted: 1.5, failed: 0 })],
        ['a string count', () => ({ attempted: '4', failed: 0 })],
        ['a missing count', () => ({ attempted: 1 })],
        ['failed below zero', () => ({ attempted: 1, failed: -1 })],
        ['a getter that throws', () => ({
            get attempted(): number {
                throw new Error('injected: the getter threw (#384)')
            },
            failed: 0,
        })],
    ]
    for (const [name, make] of values) {
        await t.step(name, async () => {
            await withClock(async ({ time, logs }) => {
                const f = fleet()
                f.record()
                let hold: PromiseWithResolvers<void> | undefined
                f.listen(() => {
                    logs.events.push('call')
                    if (hold === undefined) return make()
                    return hold.promise.then(make)
                })
                // A reconnect pass whose handler is held, and a second
                // reconnect recorded behind it: the end site starts that
                // trailing pass. The malformed WARN is the held pass's own,
                // written before its end site runs (row 5a) — never after the
                // trailing pass started (K22).
                hold = Promise.withResolvers<void>()
                await f.reconnect()
                await f.reconnect()
                const release = hold
                hold = undefined
                release.resolve()
                await time.runMicrotasks()
                await advance(time, 9_999)
                assertEquals(logs.deadlineLines(), [], 'nothing before the TTL')
                await advance(time, 1)
                const samples = f.revocations()
                assert(samples.length >= 10, 'precondition: passes ran')
                assertEquals(
                    logs.warned(REVOCATION_TALLY_MALFORMED).length,
                    samples.length,
                    'one WARN per pass',
                )
                // Row 5a: the line names the trigger and the contract, never
                // the value the handler resolved.
                for (const line of logs.warned(REVOCATION_TALLY_MALFORMED)) {
                    assert(
                        !line.includes('"attempted"'),
                        `value leaked: ${line}`,
                    )
                    assert(!line.includes('"failed"'), `value leaked: ${line}`)
                }
                const order = logs.events
                    .filter((e) =>
                        e === 'call' ||
                        e.startsWith(`warn:${REVOCATION_TALLY_MALFORMED}`)
                    )
                    .map((e) => e === 'call' ? 'call' : 'malformed')
                assertEquals(
                    order,
                    order.map((_, i) => i % 2 === 0 ? 'call' : 'malformed'),
                    'each pass writes its WARN before the next pass is called',
                )
                for (const sample of samples) {
                    assertEquals(sample.outcome, 'ok')
                    assert(!('attempts' in sample), 'no attempts key')
                    assert(!('failures' in sample), 'no failures key')
                }
                assertEquals(
                    samples.filter((s) => s.trigger === 'reconnect-retry'),
                    [],
                    'a malformed tally is not a failed pass: no #308 retry',
                )
                assertEquals(
                    logs.warned('realtime: revocation reconcile failed'),
                    [],
                )
                assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED).length, 1)
                assertEquals(logs.deadlineLines().length, 1)
                await f.driver.close()
            })
        })
    }
})

// ---------------------------------------------------------------------------
// US2 — only a clean pass re-arms the deadline
// ---------------------------------------------------------------------------

Deno.test('#384 R3c a sink that refuses the malformed WARN writes one marked line per pass, and the pass stays ok', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.record()
        f.listen(() => ({ attempted: 1, failed: 2 }))
        // Row 5a's #391 shape: the sink refuses THIS line only.
        logs.refuseWarn((line) => line.startsWith(REVOCATION_TALLY_MALFORMED))
        await advance(time, 3 * INTERVAL)
        const samples = f.revocations()
        assert(samples.length >= 3, 'precondition: passes ran')
        for (const sample of samples) assertEquals(sample.outcome, 'ok')
        const marked = logs.errored(REVOCATION_LOG_FAILED)
        assertEquals(marked.length, samples.length, 'one marked line per pass')
        for (const line of marked) {
            assert(line.includes(REVOCATION_TALLY_MALFORMED), line)
        }
        assertEquals(logs.warned('realtime: revocation reconcile failed'), [])
        await f.driver.close()
    })
})

Deno.test('#384 R4 a pass whose every apply failed never re-arms: one MISSED at the TTL, and one only', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.listen(() => ({ attempted: 1, failed: 1 }))
        await advance(time, 9_999)
        assertEquals(logs.deadlineLines(), [])
        await advance(time, 1)
        assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED).length, 1)
        await advance(time, 20_000)
        assertEquals(logs.deadlineLines().length, 1, 'one line per episode')
        await f.driver.close()
    })
})

Deno.test('#384 R5 a pass with one failure among five never re-arms either', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.listen(() => ({ attempted: 5, failed: 1 }))
        await advance(time, 9_999)
        assertEquals(logs.deadlineLines(), [])
        await advance(time, 1)
        assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED).length, 1)
        await advance(time, 20_000)
        assertEquals(logs.deadlineLines().length, 1)
        await f.driver.close()
    })
})

Deno.test('#384 R6 one pass with a failure among clean ones costs margin, not a line', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        let dirty = 0
        f.listen(() => {
            if (elapsed(time) !== 3_000) return { attempted: 1, failed: 0 }
            dirty++
            return { attempted: 1, failed: 1 }
        })
        await advance(time, 30_000)
        assertEquals(dirty, 1, 'precondition: the pass at 3 s had a failure')
        assertEquals(logs.deadlineLines(), [])
        await f.driver.close()
    })
})

Deno.test('#384 R7 a handler that resolves nothing re-arms on every ok pass, as today', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.listen(() => undefined)
        await advance(time, 30_000)
        assertEquals(logs.deadlineLines(), [])
        await f.driver.close()
    })
})

Deno.test("#384 R8 both deadline lines state the premise: no pass completed without failures since the last clean pass's start", async (t) => {
    await t.step('MISSED', async () => {
        await withClock(async ({ time, logs }) => {
            const f = fleet()
            f.listen(() => ({ attempted: 1, failed: 1 }))
            await advance(time, TTL * 1000)
            const [line] = logs.warned(REVOCATION_DEADLINE_MISSED)
            assert(line, 'precondition: MISSED was written')
            assert(line.includes('without failures'), line)
            assert(line.includes("last clean pass's start"), line)
            await f.driver.close()
        })
    })
    await t.step('STALLED', async () => {
        await withClock(async ({ time, logs }) => {
            const f = fleet()
            // The first pass never settles: no pass has ended since the last
            // clean one (there was none), and one is in flight at the TTL.
            f.listen(() => new Promise<never>(() => {}))
            await advance(time, TTL * 1000)
            const [line] = logs.warned(REVOCATION_DEADLINE_STALLED)
            assert(line, 'precondition: STALLED was written')
            assert(line.includes('without failures'), line)
            assert(line.includes("last clean pass's start"), line)
            await f.driver.close()
        })
    })
})

Deno.test('#384 R9 an expiry during a healthy pass, after passes with failures, is MISSED — never STALLED', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        let calls = 0
        let settled = 0
        // Held 700 ms: passes start at 1.0, 2.7, 4.4, 6.1, 7.8 and 9.5 s, so
        // the one started at 9.5 s is in flight when the deadline fires at
        // 10 s.
        f.listen(async () => {
            calls++
            await new Promise((done) => setTimeout(done, 700))
            settled++
            return { attempted: 1, failed: 1 }
        })
        await advance(time, 9_999, 100)
        assertEquals(logs.deadlineLines(), [])
        await advance(time, 1)
        assertEquals(calls - settled, 1, 'precondition: a pass is in flight')
        assertEquals(logs.warned(REVOCATION_DEADLINE_STALLED), [])
        assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED).length, 1)
        await advance(time, 1_000, 100)
        await f.driver.close()
    })
})

Deno.test('#384 R10 a failing pass, then a clean pass, then a stall: the clean pass forgets the failure, and the stall is STALLED', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        let calls = 0
        // 1 s: a failure (passEnded). 2 s: clean (re-arms to 12 s, and must
        // forget the failure). 3 s: never settles, so it is in flight at 12 s.
        f.listen(() => {
            calls++
            if (calls === 1) return { attempted: 1, failed: 1 }
            if (calls === 2) return { attempted: 1, failed: 0 }
            return new Promise<never>(() => {})
        })
        await advance(time, 2 * INTERVAL + TTL * 1000 - 1)
        assertEquals(logs.deadlineLines(), [])
        await advance(time, 1)
        assertEquals(calls, 3, 'precondition: the third pass is in flight')
        assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED), [])
        assertEquals(logs.warned(REVOCATION_DEADLINE_STALLED).length, 1)
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// US4 — every ghost sweep counts the dead instances it swept
// ---------------------------------------------------------------------------

Deno.test('#384 S1 two dead instances, one of whose release throws, and a live peer: two attempts, one failure, ok', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantHold(f.redis, '1', DEAD)
        await plantHold(f.redis, '2', DEAD_TOO)
        // A live peer: the sweep reads it and probes it, and never sweeps it,
        // so it is not an attempt.
        await f.redis.command('SADD', INSTANCES_KEY, LIVE)
        await f.redis.command('SET', ALIVE_KEY(LIVE), '1', 'EX', '30')
        f.port.failFrom(isRelease(DEAD))
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        const [sample] = f.sweeps()
        assertEquals(sample.outcome, 'ok')
        assertEquals(sample.attempts, 2)
        assertEquals(sample.failures, 1)
        await f.driver.close()
    })
})

Deno.test('#384 S1b the sweep failure is counted before its WARN: a throwing sink cannot skip it', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantHold(f.redis, '1', DEAD)
        f.port.failFrom(isRelease(DEAD))
        logs.refuseWarn(() => true)
        await advance(time, INTERVAL)
        logs.refuseWarn(() => false)
        assertEquals(f.sweeps().length, 1)
        assertEquals(f.sweeps()[0].failures, 1)
        await f.driver.close()
    })
})

Deno.test('#384 S2 a sweep with no dead instance counts zero attempts and zero failures', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        const [sample] = f.sweeps()
        assertEquals(sample.outcome, 'ok')
        assertEquals(sample.attempts, 0)
        assertEquals(sample.failures, 0)
        await f.driver.close()
    })
})

Deno.test('#384 S3 a dead instance that renews itself mid-sweep is attempted, not failed', async () => {
    await withClock(async ({ time, logs }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantHold(f.redis, '1', DEAD)
        await plantHold(f.redis, '2', DEAD)
        const first = f.port.hold(isRelease(DEAD))
        await advance(time, INTERVAL)
        await first.reached
        // DEAD renews while the first release's reply is in flight: the
        // second release is refused, and the sweep ends `renewed`.
        await f.redis.command('SET', ALIVE_KEY(DEAD), '1', 'EX', '30')
        first.release()
        await time.runMicrotasks()
        await advance(time, 250)
        assert(
            logs.warned(`realtime: instance ${DEAD} renewed`).length === 1,
            'precondition: the sweep ended renewed',
        )
        assert(f.sweeps().length >= 1)
        const [sample] = f.sweeps()
        assertEquals(sample.outcome, 'ok')
        assertEquals(sample.attempts, 1)
        assertEquals(sample.failures, 0)
        await f.driver.close()
    })
})

Deno.test('#384 S4 a sweep that fails after sweeping one instance reports the counts it reached', async () => {
    await withClock(async ({ time }) => {
        const f = fleet()
        f.record()
        await f.startSweep()
        await plantHold(f.redis, '1', DEAD)
        await plantHold(f.redis, '2', DEAD_TOO)
        // The SECOND liveness probe throws, whichever instance it names: the
        // first dead instance is swept, then the pass stops.
        let probes = 0
        f.port.failFrom((args) => isExists(args) && ++probes === 2)
        await advance(time, INTERVAL)
        assertEquals(f.sweeps().length, 1)
        const [sample] = f.sweeps()
        assertEquals(sample.outcome, 'failed')
        assertEquals(sample.attempts, 1)
        assertEquals(sample.failures, 0)
        await f.driver.close()
    })
})

// ---------------------------------------------------------------------------
// End to end — a revoked socket that cannot be closed
// ---------------------------------------------------------------------------

Deno.test('#384 E1 a revoked socket that refuses to close fails every pass, stays registered, and is reported once at the TTL', async () => {
    await withClock(async ({ time, logs }) => {
        const redis = new FakeRedis()
        redis.setTime(NOW_S)
        const driver = new RedisBroadcastDriver(
            { command: redis.command },
            redis.subscriberFor(),
            {
                prefix: PREFIX,
                revocationTtlSeconds: TTL,
                presence: { reconcileIntervalMs: INTERVAL },
            },
        )
        const samples: PassSample[] = []
        driver.onPassComplete((s) => void samples.push(s))
        const manager = new ChannelManager<User>({
            driver,
            authorize: (identity) => identity ? { id: identity.id } : false,
        })
        const c1 = conn('c1', 1, { closeThrows: true })
        manager.register(c1)
        // A connection record is its bare target (MARK_REVOKED_SCRIPT), live
        // well past the run on the pinned broker clock.
        await redis.command('ZADD', INDEX, String(NOW_S + 10 * TTL), 'c1')
        await advance(time, TTL * 1000 - 1)
        assertEquals(logs.deadlineLines(), [])
        await advance(time, 1)
        const revocations = samples.filter((s) => s.pass === 'revocation')
        assert(revocations.length >= 9, 'precondition: passes ran')
        for (const sample of revocations) {
            assertEquals(sample.outcome, 'ok')
            assertEquals(sample.attempts, 1)
            assertEquals(sample.failures, 1)
        }
        assertEquals(manager.connectionCount, 1, 'the socket is still owned')
        assert(c1.closes.length >= 9, 'a close was tried on every pass')
        assertEquals(logs.warned(REVOCATION_DEADLINE_MISSED).length, 1)
        assertEquals(logs.deadlineLines().length, 1)
        await driver.close()
    })
})
