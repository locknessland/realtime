/**
 * @fileoverview #371 — a roster release this instance could not commit is
 * retried, rather than left only to the ghost sweep, which never reaches a
 * healthy instance's own holds.
 *
 * `ChannelManager` records a per-slot owed-release ledger
 * (`#owedReleases`), fed from `unsubscribe`'s release catch and
 * `#joinPresence`'s #323/#373 reclaim catch whenever `#syncRosterMember`
 * rejects. The driver's new `onRosterMaintenance` hook drains it, one slot at
 * a time, through `#syncRosterMember` — the roster's one writer — so a slot
 * already released no-ops and one re-claimed by a fresh join is re-derived as
 * held.
 *
 * Every witness here drives the drain directly, by invoking the handler the
 * manager registers on a literal `BroadcastDriver` double's
 * `onRosterMaintenance` — the same "gated fake driver" shape
 * `join_reclaim_373.test.ts` (#371's own prerequisite, #373) already uses, and
 * for the same reason: the ledger and the drain are properties of
 * `ChannelManager` alone, so they are witnessed without a real broker or a
 * real heartbeat timer. `RedisBroadcastDriver`'s own wiring — the trigger
 * fired after a successful heartbeat, and `close()` waiting for a drain in
 * flight — is witnessed on its own, in `roster_maintenance_run_371.test.ts`,
 * on `LapseRun`'s own precedent (`lapse_run_349.test.ts` beside
 * `lapse_rehold_349.test.ts`).
 *
 * @module @lockness/realtime/tests/owed_release_371
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { ChannelManager, MAX_PENDING_ROSTER_RELEASES } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

function conn(id: string, userId: number): Recording {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Recording
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

/** The `presence` actions a connection received for member `id`, in order. */
const actions = (c: Recording, id: number) =>
    c.received
        .filter((f) =>
            f.type === 'presence' && f.channel === CHANNEL &&
            (f.member as PresenceMember | undefined)?.id === id
        )
        .map((f) => f.action)

/** Capture every `console.warn` call, restoring it on scope exit. */
function warnCalls(): { readonly lines: string[] } & Disposable {
    const real = console.warn
    const lines: string[] = []
    console.warn = (...args: unknown[]) =>
        void lines.push(args.map(String).join(' '))
    return {
        lines,
        [Symbol.dispose]: () => {
            console.warn = real
        },
    }
}

/**
 * A presence-capable driver double whose `releaseMember` can be told to
 * reject the next N calls for one `(channel, id)` slot — and which captures
 * whatever handler `ChannelManager` registers on `onRosterMaintenance`, so a
 * witness can invoke it directly to simulate one heartbeat's drain.
 */
function presenceDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const releaseFaults = new Map<string, number>()
    const releaseCalls = new Map<string, number>()
    let maintenance: (() => void | Promise<void>) | undefined
    // Concurrency gauge (W3c): every `releaseMember` yields once on a real
    // microtask before it settles, so two calls issued without awaiting the
    // first would genuinely overlap — a `Promise.all` in `#drainOwedReleases`
    // is observable here, where a fully synchronous double could not show it.
    let inFlight = 0
    let peak = 0
    const slot = (channel: string, id: string | number) =>
        `${channel}\0${String(id)}`
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const field = String(member.id)
            const arrived = !members.has(field)
            members.set(field, member)
            return { arrived }
        },
        async releaseMember(channel, memberId) {
            const field = String(memberId)
            const key = slot(channel, field)
            releaseCalls.set(key, (releaseCalls.get(key) ?? 0) + 1)
            inFlight++
            peak = Math.max(peak, inFlight)
            try {
                await Promise.resolve()
                const remaining = releaseFaults.get(key) ?? 0
                if (remaining > 0) {
                    releaseFaults.set(key, remaining - 1)
                    throw new Error('ROSTER_RELEASE_REFUSED')
                }
                const gone = roster.get(channel)?.delete(field) ?? false
                return { gone }
            } finally {
                inFlight--
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                [...(roster.get(channel)?.values() ?? [])],
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl: () => Promise.resolve(),
        watchChannel: () => {},
        unwatchChannel: () => {},
        onRosterMaintenance(handler) {
            maintenance = handler
        },
    }
    return {
        driver,
        roster,
        /** Make the next `times` releases of this slot reject. */
        failReleaseTimes(channel: string, id: string | number, times = 1) {
            const key = slot(channel, id)
            releaseFaults.set(key, (releaseFaults.get(key) ?? 0) + times)
        },
        /** How many times `releaseMember` has been called for this slot. */
        releaseCallCount(channel: string, id: string | number): number {
            return releaseCalls.get(slot(channel, id)) ?? 0
        },
        /** The most `releaseMember` calls ever in flight at once. */
        peakConcurrency(): number {
            return peak
        },
        resetPeakConcurrency(): void {
            peak = 0
        },
        /** Invoke the captured `onRosterMaintenance` handler once. */
        async drain(): Promise<void> {
            await maintenance?.()
        },
    }
}

/**
 * A driver whose presence JOIN commits behind a lost reply and whose
 * reclaim (the #323/#373 compensation's release) also rejects once — the
 * #371 residue #373 explicitly defers here.
 */
function faultyJoinReclaimDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    let reclaimFaults = 1
    let maintenance: (() => void | Promise<void>) | undefined
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            // The write commits before the reply is lost — #373's own shape.
            members.set(String(member.id), member)
            return Promise.reject(new Error('ROSTER_REPLY_LOST'))
        },
        releaseMember(channel, memberId) {
            if (reclaimFaults > 0) {
                reclaimFaults--
                return Promise.reject(new Error('RECLAIM_REFUSED'))
            }
            const gone = roster.get(channel)?.delete(String(memberId)) ?? false
            return { gone }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                [...(roster.get(channel)?.values() ?? [])],
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl: () => Promise.resolve(),
        watchChannel: () => {},
        unwatchChannel: () =>
            Promise.reject(new Error('BROKER_UNWATCH_FAILED')),
        onRosterMaintenance(handler) {
            maintenance = handler
        },
    }
    return {
        driver,
        roster,
        async drain(): Promise<void> {
            await maintenance?.()
        },
    }
}

Deno.test(
    '#371 W1 (AC) a release that rejects once — disconnect still rejects, ' +
        'and after the next drain the roster no longer holds the member, ' +
        'exactly one left, no extra joined',
    async () => {
        await watchingEscapes(async (escaped) => {
            const driver = presenceDriver()
            const m = new ChannelManager<User>({
                driver: driver.driver,
                authorize,
            })
            const observer = conn('observer', 1)
            m.register(observer)
            await m.subscribe(observer, CHANNEL)
            const c7 = conn('c7', 7)
            m.register(c7)
            await m.subscribe(c7, CHANNEL)
            using warn = warnCalls()

            driver.failReleaseTimes(CHANNEL, 7, 1)
            let rejection: unknown
            try {
                await m.disconnect(c7)
            } catch (error) {
                rejection = error
            }
            await settle()
            assert(
                rejection instanceof Error,
                'disconnect() rejects — the contract is unchanged',
            )
            assertEquals(
                driver.roster.get(CHANNEL)?.has('7'),
                true,
                'the release never committed: the roster still holds 7',
            )

            await driver.drain()

            assertEquals(
                driver.roster.get(CHANNEL)?.has('7'),
                false,
                'the drain released 7 after the (simulated) next heartbeat',
            )
            assertEquals(
                actions(observer, 7),
                ['joined', 'left'],
                'exactly one left, no extra joined',
            )
            assert(
                warn.lines.some((line) => line.includes('queued for retry')),
                `the enqueue WARN named the retry: ${warn.lines}`,
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)

Deno.test(
    '#371 W2 (covers #373 residue) a join reclaim that itself fails is ' +
        'released by the drain, and the ORIGINAL roster error still wins',
    async () => {
        await watchingEscapes(async (escaped) => {
            const { driver, roster, drain } = faultyJoinReclaimDriver()
            const m = new ChannelManager<User>({ driver, authorize })
            const newcomer = conn('c1', 1)
            m.register(newcomer)

            let rejection: unknown
            try {
                await m.subscribe(newcomer, CHANNEL)
            } catch (error) {
                rejection = error
            }
            await settle()

            assert(rejection instanceof Error, 'the subscribe must reject')
            assertEquals(
                (rejection as Error).message,
                'ROSTER_REPLY_LOST',
                'the ORIGINAL roster error still wins (#373 AC1), not the ' +
                    'reclaim failure nor the unwatch failure',
            )
            assertEquals(
                [...(roster.get(CHANNEL)?.keys() ?? [])],
                ['1'],
                'the reclaim itself failed: the committed-but-lost hold is ' +
                    'still there',
            )

            await drain()

            assertEquals(
                [...(roster.get(CHANNEL)?.keys() ?? [])],
                [],
                'the drain finished the reclaim the compensation could not',
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)

Deno.test(
    '#371 W3 the drain itself fails once — succeeds on the following ' +
        'tick, still exactly one left, no duplicate',
    async () => {
        await watchingEscapes(async (escaped) => {
            const driver = presenceDriver()
            const m = new ChannelManager<User>({
                driver: driver.driver,
                authorize,
            })
            const observer = conn('observer', 1)
            m.register(observer)
            await m.subscribe(observer, CHANNEL)
            const c7 = conn('c7', 7)
            m.register(c7)
            await m.subscribe(c7, CHANNEL)

            // The original attempt AND the drain's first retry both fail.
            driver.failReleaseTimes(CHANNEL, 7, 2)
            await assertRejects(() => m.unsubscribe(c7.id, CHANNEL))
            assertEquals(driver.roster.get(CHANNEL)?.has('7'), true)

            await driver.drain()
            assertEquals(
                driver.roster.get(CHANNEL)?.has('7'),
                true,
                'the first drain tick failed too',
            )
            assertEquals(actions(observer, 7), ['joined'], 'no left yet')

            await driver.drain()
            assertEquals(
                driver.roster.get(CHANNEL)?.has('7'),
                false,
                'the second drain tick succeeded',
            )
            assertEquals(
                actions(observer, 7),
                ['joined', 'left'],
                'exactly one left',
            )

            const callsAfterSuccess = driver.releaseCallCount(CHANNEL, 7)
            await driver.drain()
            assertEquals(
                actions(observer, 7),
                ['joined', 'left'],
                'no duplicate on a further tick: the ledger is now empty',
            )
            assertEquals(
                driver.releaseCallCount(CHANNEL, 7),
                callsAfterSuccess,
                'the ledger entry was actually cleared: a further drain ' +
                    'does not retry an already-settled slot',
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)

Deno.test(
    '#371 W3c the drain issues releases sequentially, never concurrently',
    async () => {
        await watchingEscapes(async (escaped) => {
            const driver = presenceDriver()
            const m = new ChannelManager<User>({
                driver: driver.driver,
                authorize,
            })
            const c7 = conn('c7', 7)
            m.register(c7)
            await m.subscribe(c7, CHANNEL)
            const c9 = conn('c9', 9)
            m.register(c9)
            await m.subscribe(c9, CHANNEL)

            driver.failReleaseTimes(CHANNEL, 7, 1)
            await assertRejects(() => m.unsubscribe(c7.id, CHANNEL))
            driver.failReleaseTimes(CHANNEL, 9, 1)
            await assertRejects(() => m.unsubscribe(c9.id, CHANNEL))

            driver.resetPeakConcurrency()
            await driver.drain()

            assertEquals(
                driver.peakConcurrency(),
                1,
                'never Promise.all: the drain issues one release at a time',
            )
            assertEquals(driver.roster.get(CHANNEL)?.has('7'), false)
            assertEquals(driver.roster.get(CHANNEL)?.has('9'), false)
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)

Deno.test(
    '#371 W4 a fresh join by the same member id, while its release is ' +
        'queued — the drain refreshes the hold, never releasing a member ' +
        'who is present',
    async () => {
        await watchingEscapes(async (escaped) => {
            const driver = presenceDriver()
            const m = new ChannelManager<User>({
                driver: driver.driver,
                authorize,
            })
            const observer = conn('observer', 1)
            m.register(observer)
            await m.subscribe(observer, CHANNEL)
            const c1 = conn('c1', 7)
            m.register(c1)
            await m.subscribe(c1, CHANNEL)

            driver.failReleaseTimes(CHANNEL, 7, 1)
            await assertRejects(() => m.unsubscribe(c1.id, CHANNEL))
            assertEquals(driver.roster.get(CHANNEL)?.has('7'), true)

            // A DIFFERENT connection, same member id, re-joins before the
            // drain runs.
            const c2 = conn('c2', 7)
            m.register(c2)
            await m.subscribe(c2, CHANNEL)

            await driver.drain()

            assertEquals(
                driver.roster.get(CHANNEL)?.has('7'),
                true,
                'member 7 is still held: it is present again',
            )
            assertEquals(
                actions(observer, 7),
                ['joined'],
                'no left was ever announced for a member who is present',
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)

Deno.test(
    '#371 W5 the cap: enqueue past MAX_PENDING_ROSTER_RELEASES is refused ' +
        'and logged with the ghost-sweep wording, never silently dropped',
    async () => {
        const driver = presenceDriver()
        const m = new ChannelManager<User>({
            driver: driver.driver,
            authorize,
        })
        using warn = warnCalls()
        for (let i = 0; i < MAX_PENDING_ROSTER_RELEASES; i++) {
            const c = conn(`c${i}`, i)
            m.register(c)
            await m.subscribe(c, CHANNEL)
            driver.failReleaseTimes(CHANNEL, i, 1)
            await assertRejects(() => m.unsubscribe(c.id, CHANNEL))
        }
        const extraId = MAX_PENDING_ROSTER_RELEASES
        const extra = conn('extra', extraId)
        m.register(extra)
        await m.subscribe(extra, CHANNEL)
        driver.failReleaseTimes(CHANNEL, extraId, 1)
        await assertRejects(() => m.unsubscribe(extra.id, CHANNEL))

        assert(
            warn.lines.some((line) =>
                line.includes('pending-release ledger is at its cap') &&
                line.includes('the ghost sweep is the remaining backstop')
            ),
            `the cap refusal is logged, never silent: ${warn.lines}`,
        )

        await driver.drain()

        assertEquals(
            driver.roster.get(CHANNEL)?.has(String(extraId)),
            true,
            'the refused slot was never queued, so the drain never touched ' +
                'it — the ghost sweep is its only backstop, exactly as the ' +
                'WARN said',
        )
        for (let i = 0; i < MAX_PENDING_ROSTER_RELEASES; i++) {
            assertEquals(
                driver.roster.get(CHANNEL)?.has(String(i)),
                false,
                `slot ${i} was queued and the drain released it`,
            )
        }
    },
)
