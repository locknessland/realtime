/**
 * @fileoverview #333 — the read barrier, tested alone.
 *
 * The unit exists so the rule can be stated without a manager, a driver or a
 * socket in the way. Everything here is a deferred the test resolves by hand:
 * **no timing, no sleeps, no deadlines.** A concurrency rule gated on elapsed
 * milliseconds is the defect class this repository has fixed four times.
 *
 * The two rows that carry the design are:
 *
 *   * `'a caller that arrived during a read is answered by the NEXT one'` —
 *     the trailing edge. A leading-edge single-flight passes every other test
 *     in this file and fails this one, which is the only reason the two designs
 *     are distinguishable at all.
 *   * `'a rejected read still releases the channel'` — the trap. A continuation
 *     registered only on fulfilment strands every queued caller forever the
 *     first time a driver rejects, and nothing else in the suite would notice.
 *
 * @module @lockness/realtime/tests/roster_read_barrier_333
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { RosterReadBarrier } from '../roster_read_barrier.ts'
import type { PresenceMember } from '../channel.ts'
import type { RosterWindow } from '../driver.ts'

/** A promise plus the handles to settle it from the test body. */
function deferred(): {
    promise: Promise<RosterWindow>
    resolve: (window: RosterWindow) => void
    reject: (error: unknown) => void
} {
    let resolve!: (window: RosterWindow) => void
    let reject!: (error: unknown) => void
    const promise = new Promise<RosterWindow>((res, rej) => {
        resolve = res
        reject = rej
    })
    return { promise, resolve, reject }
}

/**
 * A read the test drives. `calls` counts issues **at call time**, which is the
 * instrument the acceptance criteria name — a count taken from return values
 * cannot tell a shared read from two reads that happened to agree.
 */
function gatedRead() {
    const pending: ReturnType<typeof deferred>[] = []
    const channels: string[] = []
    const read = (channel: string): Promise<RosterWindow> => {
        channels.push(channel)
        const d = deferred()
        pending.push(d)
        return d.promise
    }
    return {
        read,
        channels,
        get calls(): number {
            return channels.length
        },
        /** Settle the nth outstanding read. */
        settle(index: number, members: PresenceMember[]): void {
            pending[index].resolve({
                members,
                total: members.length,
                selves: [],
            })
        },
        fail(index: number, error: unknown): void {
            pending[index].reject(error)
        },
    }
}

/** Let every already-queued microtask run. */
const drain = () => new Promise((r) => setTimeout(r, 0))

Deno.test('#333 K concurrent callers on one channel cost exactly two reads', async () => {
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const inFlight = Array.from({ length: 8 }, () => barrier.snapshot('room'))
    await drain()

    assertEquals(
        gate.calls,
        1,
        'while the first read is in flight, no second read is issued — eight ' +
            'callers, one driver round-trip',
    )

    gate.settle(0, [{ id: 1 }])
    await drain()
    assertEquals(
        gate.calls,
        2,
        'and exactly one more, for everyone who arrived during it — not one ' +
            'per caller',
    )

    gate.settle(1, [{ id: 1 }, { id: 2 }])
    const rosters = await Promise.all(inFlight)
    assertEquals(rosters.length, 8)
    assertEquals(gate.calls, 2, 'nothing further was issued on drain')
})

Deno.test('#333 a caller that arrived during a read is answered by the NEXT one', async () => {
    // THE ROW THAT CHOOSES THE DESIGN. A leading-edge single-flight would hand
    // the second caller the first read's answer — and in the manager that means
    // a joiner whose own roster write committed after the read was issued
    // receives a roster it is not in. Everything else in this file passes under
    // both designs.
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const first = barrier.snapshot('room')
    await drain()
    const second = barrier.snapshot('room')
    await drain()

    gate.settle(0, [{ id: 'before' }])
    await drain()
    gate.settle(1, [{ id: 'before' }, { id: 'after' }])

    assertEquals(
        (await first).members.map((m) => m.id),
        ['before'],
        'the first caller gets the read it issued',
    )
    assertEquals(
        (await second).members.map((m) => m.id),
        ['before', 'after'],
        'the second gets a read issued AFTER it asked — never the one already ' +
            'running when it arrived',
    )
})

Deno.test('#333 sequential callers each get their own read', async () => {
    // The bound is "one in flight", not "one per interval". With nothing to
    // share, nothing is shared — which is why this change cannot make a
    // snapshot staler than it was.
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const first = barrier.snapshot('room')
    gate.settle(0, [{ id: 1 }])
    await first
    await drain()
    assertEquals(gate.calls, 1)
    assertEquals(barrier.size, 0, 'and the channel was released in between')

    const second = barrier.snapshot('room')
    gate.settle(1, [{ id: 2 }])
    await second
    assertEquals(gate.calls, 2, 'a later, unconcurrent ask is its own read')
})

Deno.test('#333 a rejection reaches every caller sharing the read', async () => {
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const callers = Array.from({ length: 4 }, () => barrier.snapshot('room'))
    await drain()

    // The FIRST caller owns read 0; the other three share read 1, because the
    // trailing edge answers them with a read issued after they asked. So a
    // rejection fans out across the callers of ONE read, and the two groups
    // are separate — which is itself worth pinning, since a leading-edge
    // implementation would collapse all four onto read 0 and pass a test that
    // only failed read 0.
    gate.fail(0, new Error('first read refused'))
    await assertRejects(() => callers[0], Error, 'first read refused')
    await drain()

    assertEquals(gate.calls, 2, 'the queued three got their own read issued')
    gate.fail(1, new Error('second read refused'))
    for (const caller of callers.slice(1)) {
        await assertRejects(
            () => caller,
            Error,
            'second read refused',
            'nothing is swallowed here — the manager owns the fallback',
        )
    }
    await drain()
    assertEquals(barrier.size, 0, 'two failures, and the channel is still free')
})

Deno.test('#333 a rejected read still releases the channel', async () => {
    // THE TRAP. `next` is chained on the running read; a continuation attached
    // only to the fulfilment path never runs when the driver rejects, and every
    // queued caller waits forever on a promise nothing will settle — with the
    // socket healthy and nothing logged.
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const first = barrier.snapshot('room')
    await drain()
    const queued = barrier.snapshot('room')
    await drain()

    gate.fail(0, new Error('driver refused'))
    await assertRejects(() => first, Error, 'driver refused')
    await drain()

    assertEquals(
        gate.calls,
        2,
        'the queued caller still got its read issued — a failed read must not ' +
            'take the callers behind it with it',
    )
    gate.settle(1, [{ id: 'recovered' }])
    assertEquals((await queued).members.map((m) => m.id), ['recovered'])

    await drain()
    assertEquals(barrier.size, 0, 'and the channel was given back')
})

Deno.test('#333 the barrier retains NOTHING once a burst settles', async () => {
    // The assertion the issue did not ask for, and the one that stops this
    // remedy from becoming #334 in a different map. The map is keyed by
    // channel and clients name channels; what bounds it is reads IN FLIGHT,
    // never names ever seen.
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const burst = Array.from({ length: 5 }, () => barrier.snapshot('room'))
    await drain()
    assertEquals(barrier.size, 1, 'one channel in flight')
    gate.settle(0, [])
    await drain()
    gate.settle(1, [])
    await Promise.all(burst)
    await drain()
    assertEquals(barrier.size, 0, 'and it is handed back')

    // Now the churn shape: many distinct names, each read once.
    for (let i = 0; i < 50; i++) {
        const pending = barrier.snapshot(`presence-churn-${i}`)
        gate.settle(gate.calls - 1, [])
        await pending
    }
    await drain()
    assertEquals(
        barrier.size,
        0,
        'fifty distinct channel names left fifty entries behind under any ' +
            'implementation keyed by names seen rather than reads in flight',
    )
})

Deno.test('#333 a synchronous read is normalised, and a synchronous throw rejects', async () => {
    // `MemoryBroadcastDriver.readRoster` answers synchronously. It must not
    // get a different rule — a type-level discriminator used as a cost signal
    // changes behaviour silently the day the type changes for another reason.
    const sync = new RosterReadBarrier(() => ({
        members: [{ id: 7 }],
        total: 1,
        selves: [],
    }))
    assertEquals((await sync.snapshot('room')).members.map((m) => m.id), [7])
    await drain()
    assertEquals(sync.size, 0)

    const throwing = new RosterReadBarrier(() => {
        throw new Error('synchronous fault')
    })
    await assertRejects(
        () => throwing.snapshot('room'),
        Error,
        'synchronous fault',
    )
    await drain()
    assertEquals(
        throwing.size,
        0,
        'a synchronous throw must reject like an async one, or the slot is ' +
            'never installed and the map leaks the channel',
    )
})

Deno.test('#333 channels do not share a barrier slot', async () => {
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)

    const a = barrier.snapshot('room-a')
    const b = barrier.snapshot('room-b')
    await drain()

    assertEquals(gate.channels, ['room-a', 'room-b'], 'two reads, two rooms')
    assertEquals(barrier.size, 2)
    gate.settle(0, [{ id: 'a' }])
    gate.settle(1, [{ id: 'b' }])
    assertEquals((await a).members.map((m) => m.id), ['a'])
    assertEquals((await b).members.map((m) => m.id), ['b'])
    await drain()
    assertEquals(barrier.size, 0)
})

Deno.test('#333 a sustained burst never runs two reads at once', async () => {
    // The bound stated as a property rather than a count: at every instant,
    // at most one read per channel is outstanding, however many callers pile
    // in and whenever they arrive.
    const gate = gatedRead()
    const barrier = new RosterReadBarrier(gate.read)
    let settledThrough = 0
    const pending: Promise<RosterWindow>[] = []

    for (let round = 0; round < 6; round++) {
        for (let i = 0; i < 4; i++) pending.push(barrier.snapshot('room'))
        await drain()
        assert(
            gate.calls - settledThrough <= 1,
            `round ${round}: ${
                gate.calls - settledThrough
            } reads outstanding at once`,
        )
        gate.settle(settledThrough, [{ id: round }])
        settledThrough++
        await drain()
    }
    while (settledThrough < gate.calls) {
        gate.settle(settledThrough, [])
        settledThrough++
        await drain()
    }
    await Promise.all(pending)
    await drain()
    assertEquals(barrier.size, 0)
})
