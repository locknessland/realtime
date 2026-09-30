/**
 * @fileoverview #333 — concurrent presence subscribes share one authoritative
 * roster read, through the manager.
 *
 * The unit test beside this one proves the barrier's rule. This proves the
 * manager is actually wired to it, and — in the last row — that the rule chosen
 * is the *correct* one rather than merely a working one.
 *
 * ## The instrument
 *
 * The driver double's `readRoster` **increments a counter at call time and
 * returns a deferred the test resolves by hand**. Both halves matter:
 *
 *   * Counting at call time is the only way to tell a shared read from two
 *     reads that happened to agree. A count taken from return values cannot.
 *   * The hand-resolved gate is what makes the count exact. #333's issue asked
 *     for "fewer than K reads", and that assertion would flake: a double that
 *     answers synchronously has no round-trip, so the in-flight window is one
 *     microtask and how many of K callers land inside it is a scheduling
 *     accident — somewhere between 2 and K, moving between runs and between
 *     Deno versions. With the gate held closed, the answer is exactly 1, then
 *     exactly 2, on every machine.
 *
 * ## The row that chooses the design
 *
 * `'a joiner sees ITSELF in its own reply'`. It fails under a leading-edge
 * single-flight — the shape the issue originally proposed — and passes under
 * the trailing-edge barrier. Without it the two designs are indistinguishable
 * to this suite, and the wrong one is the smaller diff.
 *
 * @module @lockness/realtime/tests/presence_roster_read_333
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, type SubscribeResult } from '../manager.ts'
import { RosterReadBarrier } from '../roster_read_barrier.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const ROOM = 'presence-room'

function conn(id: string, userId: number): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    } as Connection<User>
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

/**
 * The manager's barrier, read directly.
 *
 * `private` in TypeScript is erased at runtime. See `manager.ts` for why the
 * field is `private` rather than `#`: retention has no behavioural consequence
 * to assert on, so the only honest witness is the structural one.
 */
type BarrierView = { rosterReads: RosterReadBarrier | undefined }
const barrierOf = (m: ChannelManager<User>): RosterReadBarrier => {
    const barrier = (m as unknown as BarrierView).rosterReads
    assert(barrier instanceof RosterReadBarrier, 'the manager holds a barrier')
    return barrier
}

/**
 * A roster driver whose reads are held open until the test releases them.
 *
 * `readRoster` snapshots the store **at issue time** and resolves with that
 * snapshot later — which is what a real round-trip does, and what makes the
 * joiner-sees-itself row meaningful. Resolving with the store's *current*
 * contents instead would hide the very ordering under test.
 */
function gatedRosterDriver() {
    const store = new Map<string, Map<string, PresenceMember>>()
    const releases: (() => void)[] = []
    let reads = 0
    let open = false

    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = store.get(channel)
            if (!members) store.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            return {
                gone: store.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    reads++
                    const snapshot = [...(store.get(channel)?.values() ?? [])]
                    if (open) return snapshot
                    return new Promise<PresenceMember[]>((resolve) => {
                        releases.push(() => resolve(snapshot))
                    })
                })(),
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl: () => Promise.resolve(),
        watchChannel: () => {},
        unwatchChannel: () => {},
    }

    return {
        driver,
        get reads(): number {
            return reads
        },
        /** Release every read issued so far; later reads are held again. */
        release(): void {
            const queued = releases.splice(0)
            for (const fire of queued) fire()
        },
        /** Stop holding reads at all — for the drain at the end of a test. */
        openGate(): void {
            open = true
            this.release()
        },
    }
}

/**
 * The roster a join returned, asserted present.
 *
 * `SubscribeResult.ok` is a plain boolean rather than a discriminant, so it
 * narrows nothing — and `here` is absent for a non-presence channel. Saying
 * so once here beats a non-null assertion at every use, which would also be the
 * one spelling that hides a genuinely missing roster.
 */
function membersOf(result: SubscribeResult): PresenceMember[] {
    assert(result.ok, 'the join succeeded')
    assert(result.here !== undefined, 'a presence join carries a roster')
    return result.here.members
}

/** Let every already-queued microtask and timer run. */
const drain = () => new Promise((r) => setTimeout(r, 0))

Deno.test('#333 K concurrent presence subscribes cost exactly two reads', async () => {
    const gate = gatedRosterDriver()
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    const joins = Array.from({ length: 8 }, (_, i) => {
        const c = conn(`c${i}`, i)
        m.register(c)
        return m.subscribe(c, ROOM)
    })
    await drain()

    assertEquals(
        gate.reads,
        1,
        'eight concurrent joiners, one authoritative read in flight — counted ' +
            'on the driver double at call time, not inferred from replies',
    )

    gate.release()
    await drain()
    assertEquals(
        gate.reads,
        2,
        'and exactly one more, shared by everyone who arrived during the ' +
            'first — not one per caller, and not a number that moves between ' +
            'runs',
    )

    gate.openGate()
    const results = await Promise.all(joins)

    for (const result of results) {
        assert(result.ok, 'every join succeeded')
        assertEquals(
            result.here?.source,
            'authoritative',
            'sharing a read must be invisible in the answer — a shared read ' +
                'that degraded to the local projection would be a different ' +
                'and worse defect',
        )
    }
    await drain()
    assertEquals(
        barrierOf(m).size,
        0,
        'and the barrier holds nothing once the burst settles',
    )
})

Deno.test('#333 a joiner sees ITSELF in its own reply', async () => {
    // THE ROW THAT CHOOSES THE DESIGN, and the whole reason the barrier is
    // trailing-edge rather than a leading-edge single-flight.
    //
    // A join commits its roster write BEFORE it reads, so a joiner has always
    // appeared in its own `here`. Share a read that was issued before a
    // joiner's own write committed and that stops being true: the client
    // subscribes to a room and renders a roster it is not in. Nothing throws,
    // nothing is logged, and no type changes.
    const gate = gatedRosterDriver()
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    // A commits, then issues read #0 — which snapshots the store holding only A.
    const c1 = conn('c1', 1)
    m.register(c1)
    const first = m.subscribe(c1, ROOM)
    await drain()
    assertEquals(gate.reads, 1, 'A issued the only read so far')

    // B commits while read #0 is in flight. Under a leading-edge single-flight
    // B would be handed read #0's answer, which predates B's own write.
    const c2 = conn('c2', 2)
    m.register(c2)
    const second = m.subscribe(c2, ROOM)
    await drain()
    assertEquals(gate.reads, 1, 'B did not issue its own read — it is sharing')

    gate.release()
    await drain()
    assertEquals(gate.reads, 2, "B's read was issued once A's settled")
    gate.openGate()

    const a = await first
    const b = await second
    assert(a.ok && b.ok)

    assertEquals(
        membersOf(a).map((member) => member.id).sort(),
        [1],
        'A read before B existed, and that is correct — A asked first',
    )
    assertEquals(
        membersOf(b).map((member) => member.id).sort(),
        [1, 2],
        'B MUST contain itself. Its roster write committed before it asked, ' +
            'so the read that answers it has to have been issued after that ' +
            'write — which is the whole difference between a read barrier ' +
            'and a single-flight',
    )
})

Deno.test('#333 an unconcurrent join still reads at its own ask-time', async () => {
    // The bound is "one in flight", never "one per interval". Nothing here is
    // shared, so nothing here is staler than before the change — which is what
    // keeps `churn_cost_329.test.ts`'s published cost table accurate: a
    // sequential join is still exactly one roster read.
    const gate = gatedRosterDriver()
    gate.openGate()
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    assertEquals(gate.reads, 1)
    const rejoin = await m.subscribe(c1, ROOM)
    assertEquals(gate.reads, 2, 'a re-join is its own read, as #327 leaves it')
    assertEquals(membersOf(rejoin).map((member) => member.id), [1])
})

Deno.test('#333 distinct channels do not share a read, and leave nothing behind', async () => {
    // The bound is per channel — R1b in the disposition — and the map is keyed
    // by channel names clients choose. What bounds it is reads IN FLIGHT, never
    // names ever seen; miss that and this remedy is #334 in a different map.
    const gate = gatedRosterDriver()
    gate.openGate()
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    for (let i = 0; i < 30; i++) {
        const cN = conn(`c${i}`, i)
        m.register(cN)
        await m.subscribe(cN, `presence-room-${i}`)
    }
    assertEquals(gate.reads, 30, 'thirty rooms, thirty reads — none shared')
    await drain()
    assertEquals(
        barrierOf(m).size,
        0,
        'and thirty distinct names left nothing behind',
    )
})

Deno.test('#333 every caller of a shared read gets its OWN array', async () => {
    // R1g. The barrier hands one array to everyone sharing a read;
    // `rosterSnapshot`'s spread is what keeps that from being observable, and
    // that spread stopped being defensive the day the barrier arrived.
    const gate = gatedRosterDriver()
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    const c1 = conn('c1', 1)
    m.register(c1)
    const c2 = conn('c2', 2)
    m.register(c2)
    const c3 = conn('c3', 3)
    m.register(c3)
    const joins = [
        m.subscribe(c1, ROOM),
        m.subscribe(c2, ROOM),
        m.subscribe(c3, ROOM),
    ]
    await drain()
    gate.release()
    await drain()
    gate.openGate()
    const results = await Promise.all(joins)
    const shared = results.map(membersOf).filter((list) => list.length > 1)
    assert(shared.length >= 2, 'at least two callers shared one read')
    const [one, two] = shared
    assert(
        one !== two,
        'two callers of one read must not hold the same mutable array',
    )
    one.length = 0
    assert(
        two.length > 0,
        "and emptying one caller's list must not empty another's",
    )
})
