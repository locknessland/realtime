/**
 * @fileoverview #341 — what one presence subscribe INGESTS is independent of
 * the room's size.
 *
 * #339 capped what a subscribe RETURNS at K members. It did not cap what the
 * instance READS to build that reply: on Redis the read was one `HGETALL` of
 * the whole presence hash, so the bytes an instance pulled per subscribe grew
 * with the room — ~40 MB at 10 000 members of 4 KiB, to return 100.
 *
 * ## What is measured, and why it is bytes on the command connection
 *
 * A counting `RedisCommandClient` sits between the driver and `FakeRedis` and
 * sums the payload bytes of every reply the driver receives during ONE
 * subscribe. That is the instance's ingest: what crossed the wire into the
 * process and had to be buffered and parsed. Rooms of 1 000 and 10 000 members
 * must cost the same number.
 *
 * ## Why every size is fixed
 *
 * Member JSON is padded to exactly 4 096 bytes, ids are fixed width, and every
 * stored `owner` is 36 characters — the width of the driver's own
 * `crypto.randomUUID()` — so a byte count is a function of how MANY entries
 * were read and nothing else. The literals are literals for the reason
 * `presence_snapshot_bound_339.test.ts` gives: a test that computed its
 * expectation from the code under test would move with it.
 *
 * ## Driver order
 *
 * `FakeRedis` answers `HRANDFIELD` with the first `count` pairs in insertion
 * order, and the joiner is written last — so the joiner is never in the sampled
 * window and can only be kept from `selves`. On a real broker the sample is
 * random; this file relies on the fake's order and says so.
 *
 * @module @lockness/realtime/tests/presence_read_bound_341
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, type SubscribeResult } from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { RosterReadBarrier } from '../roster_read_barrier.ts'
import type { PresenceMember, PresenceSnapshot } from '../channel.ts'
import type { BroadcastDriver, RosterWindow } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: string
}

const ROOM = 'presence-room'
const PREFIX = 'app:rt'
const PRESENCE_KEY = `${PREFIX}__presence:${ROOM}`
const MEMBER_BYTES = 4096
/** The width of `crypto.randomUUID()`, which is what the driver stores. */
const OWNER = 'o'.repeat(36)
const SECRET = 's'.repeat(32)

/**
 * The pinned ingest of one subscribe at the default K = 100.
 *
 * One sampled pair is the 6-byte field plus the stored entry
 * `{"member":<4 096>,"owner":"<36>"}` — 4 154 bytes — so 4 160; K of them is
 * 416 000. The joiner's own entry from `HMGET` adds 4 154 (a field is not
 * echoed there). The instance-liveness `SET … GET` answers nil on its first
 * write, which carries no payload (#349; its `OK` added 2 before).
 *
 * **+7 bytes since #414**: the join's `holdMember` widened
 * `HOLD_MEMBER_SCRIPT`'s reply to `{arrived, ownedKind, instancesKind}` — an
 * integer (no payload) plus two bulk strings naming each self-healing key's
 * prior Redis type. Both are brand new on a fresh `FakeRedis`, so
 * `ownedKind` reads `'none'` (4 bytes) — nothing has touched this instance's
 * owned set yet. `instancesKind` reads `'set'` (3 bytes), not `'none'`: the
 * SAME `holdMember` call's `#ensureSweepStarted()` runs the boot heartbeat
 * FIRST, whose own raw `SADD` on the instances key already exists by the
 * time the script's own `INSTANCES_HEAL` reads its `TYPE` moments later, in
 * the same counted window. `4 + 3 = 7`. Nothing else the subscribe issues
 * replies with a payload.
 */
const PINNED_DEFAULT_BYTES = 420_161
/** The same arithmetic at K = 10: 41 600 + 4 154 + 7. */
const PINNED_K10_BYTES = 45_761

/** A fixed-width member id: `u00001` … `u10000`. */
const idOf = (n: number) => `u${String(n).padStart(5, '0')}`

/** A member whose `JSON.stringify` is exactly {@link MEMBER_BYTES} bytes. */
function paddedMember(id: string): PresenceMember {
    const base = JSON.stringify({ id, info: { pad: '' } }).length
    return { id, info: { pad: 'x'.repeat(MEMBER_BYTES - base) } }
}

function conn(id: string, userId: string): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<User>
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? paddedMember(identity.id) : false

/** The payload bytes of a `RespReply`-shaped value, recursively. */
function replyBytes(reply: unknown): number {
    if (typeof reply !== 'object' || reply === null) return 0
    const { type, value } = reply as { type?: string; value?: unknown }
    if (type === 'array' && Array.isArray(value)) {
        return value.reduce((sum: number, r) => sum + replyBytes(r), 0)
    }
    if ((type === 'bulk' || type === 'simple') && typeof value === 'string') {
        return new TextEncoder().encode(value).length
    }
    return 0
}

function hereOf(result: SubscribeResult): PresenceSnapshot {
    assert(result.ok, 'the join succeeded')
    assert(result.here !== undefined, 'a presence join carries a snapshot')
    return result.here
}

/**
 * Seed a room of `size - 1` members owned by another instance, then subscribe
 * member `size` through a manager over a counting client, and report what that
 * one subscribe ingested.
 */
async function ingestOfOneSubscribe(
    size: number,
    maxPresenceSnapshotMembers?: number,
): Promise<{ bytes: number; here: PresenceSnapshot; joiner: string }> {
    const redis = new FakeRedis()
    for (let n = 1; n < size; n++) {
        const member = paddedMember(idOf(n))
        await redis.command(
            'HSET',
            PRESENCE_KEY,
            member.id as string,
            JSON.stringify({ member, owner: OWNER }),
        )
    }
    let bytes = 0
    let counting = false
    const driver = new RedisBroadcastDriver(
        {
            command: async (...args: string[]) => {
                const reply = await redis.command(...args)
                if (counting) bytes += replyBytes(reply)
                return reply
            },
        },
        redis.subscriberFor(),
        { prefix: PREFIX, control: { secret: SECRET } },
    )
    try {
        const m = new ChannelManager<User>({
            driver,
            authorize,
            ...(maxPresenceSnapshotMembers === undefined
                ? {}
                : { maxPresenceSnapshotMembers }),
        })
        // The manager's construction registers onRevocationReconcile, whose
        // first-registration floor announce (#380) fires over this SAME
        // counting port. Since #405 that announce's reply carries `kind`
        // (never a payload before), so it must settle before the window
        // below opens, or a few of its bytes land in "one subscribe's
        // ingest" by a scheduling accident rather than by what a subscribe
        // itself reads. A real timer, not `Promise.resolve()`, because the
        // announce's chain is more than one microtask deep.
        await new Promise<void>((resolve) => setTimeout(resolve, 0))
        const joiner = idOf(size)
        counting = true
        const cJoiner = conn('c-joiner', joiner)
        m.register(cJoiner)
        const here = hereOf(await m.subscribe(cJoiner, ROOM))
        counting = false
        redis.assertNoRejections()
        return { bytes, here, joiner }
    } finally {
        await driver.close()
    }
}

Deno.test('#341 a subscribe ingests the same bytes from a room of 1 000 as from 10 000 (SC-001)', async () => {
    const small = await ingestOfOneSubscribe(1_000)
    const large = await ingestOfOneSubscribe(10_000)

    for (const [size, run] of [[1_000, small], [10_000, large]] as const) {
        assertEquals(run.here.total, size, '`total` is the whole room')
        assertEquals(run.here.members.length, 100, 'K members')
        assertEquals(run.here.source, 'authoritative')
        assert(
            run.here.members.some((m) => m.id === run.joiner),
            `the joiner is kept in a room of ${size} — from selves`,
        )
    }
    assertEquals(
        small.bytes,
        large.bytes,
        'ingest per subscribe must not grow with the room',
    )
    assertEquals(
        large.bytes,
        PINNED_DEFAULT_BYTES,
        'K = 100 sampled entries + the joiner, at 4 096-byte members',
    )
})

Deno.test('#341 a configured K bounds the ingest to its own pinned count', async () => {
    const small = await ingestOfOneSubscribe(1_000, 10)
    const large = await ingestOfOneSubscribe(2_000, 10)

    assertEquals(large.here.total, 2_000)
    assertEquals(large.here.members.length, 10)
    assert(large.here.members.some((m) => m.id === large.joiner))
    assertEquals(small.bytes, large.bytes)
    assertEquals(large.bytes, PINNED_K10_BYTES)
})

// ---------------------------------------------------------------------------
// US2 — concurrent subscribes still share one read (T010)
//
// Every gate below is a deferred the test opens by hand, and every wait is a
// bounded number of macrotask turns, never a deadline.
// ---------------------------------------------------------------------------

/** Let already-queued work run, a bounded number of macrotask turns. */
async function turns(n = 20): Promise<void> {
    for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0))
}

/**
 * A presence-capable double whose reads are HELD until the test releases them.
 * The window is computed when the read is issued, from the roster as it stands
 * then — ask-time, like a broker — and delivered when released.
 */
function gatedRosterDriver(seed: PresenceMember[]) {
    const roster = new Map<string, PresenceMember>(
        seed.map((m) => [String(m.id), m]),
    )
    const reads: string[][] = []
    const releases: (() => void)[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(_channel, member) {
            const arrived = !roster.has(String(member.id))
            roster.set(String(member.id), member)
            return { arrived }
        },
        releaseMember(_channel, memberId) {
            return { gone: roster.delete(String(memberId)) }
        },
        readRoster(_channel, limit, selfIds) {
            reads.push(selfIds.map(String))
            const window = asWindow([...roster.values()], limit, selfIds)
            return new Promise<RosterWindow>((resolve) => {
                releases.push(() => resolve(window))
            })
        },
    }
    return {
        driver,
        reads,
        /** Release the nth read issued. */
        release(index: number): void {
            releases[index]()
        },
    }
}

const idsOf = (here: PresenceSnapshot | undefined) =>
    (here?.members ?? []).map((m) => String(m.id))

Deno.test('#341 B and C outside the window each keep their OWN self from one shared read', async () => {
    // K = 1 and a resident written first: the window is always the resident,
    // so a joiner can only be kept from `selves` — and a shared read that
    // carried one caller's id and not the other's would drop that caller.
    const gate = gatedRosterDriver([{ id: 'r' }])
    const m = new ChannelManager<User>({
        driver: gate.driver,
        authorize: (identity) => identity ? { id: identity.id } : false,
        maxPresenceSnapshotMembers: 1,
    })
    const ca = conn('ca', 'a')
    m.register(ca)
    const a = m.subscribe(ca, ROOM)
    await turns()
    assertEquals(gate.reads.length, 1, 'A holds the one read in flight')

    const cb = conn('cb', 'b')
    m.register(cb)
    const b = m.subscribe(cb, ROOM)
    const cc = conn('cc', 'c')
    m.register(cc)
    const c = m.subscribe(cc, ROOM)
    await turns()
    assertEquals(gate.reads.length, 1, 'B and C queue; nothing else is issued')

    gate.release(0)
    await turns()
    assertEquals(gate.reads.length, 2, 'ONE trailing read serves B and C')
    assertEquals(
        [...gate.reads[1]].sort(),
        ['b', 'c'],
        'and it fetches both of their selves',
    )
    gate.release(1)

    const [hereA, hereB, hereC] = (await Promise.all([a, b, c])).map(hereOf)
    assertEquals(idsOf(hereA), ['a'])
    assertEquals(idsOf(hereB), ['b'], 'B keeps its own self, not C')
    assertEquals(idsOf(hereC), ['c'], 'C keeps its own self, not B')
    assertEquals(hereC.total, 4)
    assertEquals(gate.reads.length, 2, 'exactly two reads for three callers')
})

Deno.test('#341 an unsubscribe during the gated read drops self from the reply', async () => {
    const gate = gatedRosterDriver([{ id: 'r' }])
    const m = new ChannelManager<User>({
        driver: gate.driver,
        authorize: (identity) => identity ? { id: identity.id } : false,
        maxPresenceSnapshotMembers: 1,
    })
    const ca = conn('ca', 'a')
    m.register(ca)
    const a = m.subscribe(ca, ROOM)
    await turns()
    assertEquals(gate.reads.length, 1)
    assertEquals(gate.reads[0], ['a'], 'the read was asked for A — pre-await')

    assertEquals(await m.unsubscribe('ca', ROOM), 'left')
    gate.release(0)

    const here = hereOf(await a)
    assertEquals(
        idsOf(here),
        ['r'],
        'the read returned A in `selves`, but A left before it settled: self is ' +
            'looked up AFTER the await, so the fetched id never decides what is kept',
    )
})

/**
 * A barrier over a read the test drives, recording each read's self ids at
 * issue time.
 */
function gatedBarrier(maxSelfIds?: number) {
    const reads: string[][] = []
    /** Indexed by issue order; resolving one twice is a no-op. */
    const releases: (() => void)[] = []
    const read = (_channel: string, selfIds: readonly string[]) => {
        reads.push([...selfIds])
        return new Promise<RosterWindow>((resolve) => {
            releases.push(() => resolve({ members: [], total: 0, selves: [] }))
        })
    }
    const barrier = maxSelfIds === undefined
        ? new RosterReadBarrier(read)
        : new RosterReadBarrier(read, maxSelfIds)
    return {
        barrier,
        reads,
        /** Release the nth read issued. */
        release(index: number): void {
            releases[index]()
        },
        /** Release reads in issue order until none is outstanding. */
        async drainAll(): Promise<void> {
            for (let i = 0; i < 10_000 && i < releases.length; i++) {
                releases[i]()
                await turns(2)
            }
        },
    }
}

Deno.test('#341 a late same-id caller does not ride a read that already started (H1)', async () => {
    const gate = gatedBarrier()
    const first = gate.barrier.snapshot(ROOM, 'x')
    const early = gate.barrier.snapshot(ROOM, 'b')
    await turns()
    assertEquals(gate.reads, [['x']], "'b' queues behind the read in flight")

    gate.release(0)
    await turns()
    assertEquals(gate.reads, [['x'], ['b']], "the batch holding 'b' started")

    // 'b' asks AFTER its batch's read was issued. Joining that batch would
    // answer it with a read older than its ask — the barrier's one invariant.
    const late = gate.barrier.snapshot(ROOM, 'b')
    assert(late !== early, 'the late caller is not handed the started read')
    await gate.drainAll()
    await Promise.all([first, early, late])

    assertEquals(
        gate.reads,
        [['x'], ['b'], ['b']],
        "a started batch's ids leave `pending`, so the late 'b' opens a new read",
    )
    assertEquals(gate.barrier.size, 0)
})

Deno.test('#341 a batch opened past the cap waits for the batch AHEAD of it, not the running read (H2)', async () => {
    const gate = gatedBarrier(2)
    const all = [
        gate.barrier.snapshot(ROOM),
        gate.barrier.snapshot(ROOM, 'a'),
        gate.barrier.snapshot(ROOM, 'b'),
        gate.barrier.snapshot(ROOM, 'c'),
    ]
    await turns()
    assertEquals(gate.reads.length, 1)

    gate.release(0)
    await turns()
    assertEquals(
        gate.reads,
        [[], ['a', 'b']],
        "exactly two reads started: ['c'] is chained on ['a','b'], so one " +
            'read per channel is in flight — chaining it on the running read ' +
            'would issue both queued batches at once',
    )

    gate.release(1)
    await turns()
    assertEquals(gate.reads, [[], ['a', 'b'], ['c']])
    await gate.drainAll()
    await Promise.all(all)
    assertEquals(gate.barrier.size, 0)
})

Deno.test("#341 ids dedupe by String(id): 1 and '1' share one batch slot", async () => {
    const gate = gatedBarrier()
    const running = gate.barrier.snapshot(ROOM)
    const numeric = gate.barrier.snapshot(ROOM, 1)
    const text = gate.barrier.snapshot(ROOM, '1')
    assert(numeric === text, 'both callers share one batch')
    await gate.drainAll()
    await Promise.all([running, numeric, text])
    assertEquals(gate.reads, [[], ['1']], 'and the id is carried once')
})

Deno.test('#341 an id-less caller joins the OLDEST queued batch', async () => {
    const gate = gatedBarrier(1)
    const running = gate.barrier.snapshot(ROOM, 'r')
    const oldest = gate.barrier.snapshot(ROOM, 'a')
    const newest = gate.barrier.snapshot(ROOM, 'b')
    const idless = gate.barrier.snapshot(ROOM)
    assert(oldest !== newest, 'a cap of 1 opens a batch per id')
    assert(
        idless === oldest,
        'the id-less caller is answered by the earliest read',
    )
    await gate.drainAll()
    await Promise.all([running, oldest, newest, idless])
    assertEquals(gate.reads, [['r'], ['a'], ['b']])
})

Deno.test('#341 5 000 frames from ONE member id cost two reads, not six (S1)', async () => {
    const gate = gatedBarrier()
    const all = [gate.barrier.snapshot(ROOM, 'u1')]
    for (let i = 0; i < 5_000; i++) all.push(gate.barrier.snapshot(ROOM, 'u1'))
    await gate.drainAll()
    await Promise.all(all)

    assertEquals(
        gate.reads,
        [['u1'], ['u1']],
        'the id is counted once however many frames carry it — counting frames ' +
            'would be ⌈5 000 / 1 000⌉ + 1 = 6 reads of the same entry',
    )
    assertEquals(gate.barrier.size, 0, 'and nothing is retained once drained')
})

Deno.test('#341 past the cap a caller starts a NEW batch — FIFO, none over the cap', async () => {
    // The cap injected as 2 so the overflow is reachable in a unit. The first
    // read carries no id (a superseded join); then three distinct callers, one
    // of them repeated and one with no id at all.
    const gate = gatedBarrier(2)
    const all = [
        gate.barrier.snapshot(ROOM),
        gate.barrier.snapshot(ROOM, 'a'),
        gate.barrier.snapshot(ROOM, 'b'),
        gate.barrier.snapshot(ROOM, 'a'),
        gate.barrier.snapshot(ROOM),
        gate.barrier.snapshot(ROOM, 'c'),
    ]
    await turns()
    assertEquals(gate.reads.length, 1, 'one read in flight, whatever queues')

    await gate.drainAll()
    await Promise.all(all)

    assertEquals(
        gate.reads,
        [[], ['a', 'b'], ['c']],
        "three distinct callers past a cap of 2 are three reads: 'a' repeated " +
            'joins its pending batch without counting, no id contributes ' +
            "nothing, and 'c' — which would overflow — opens the next batch " +
            'instead of riding one without its id',
    )
    for (const ids of gate.reads) {
        assert(
            ids.length <= 2,
            `a read carried ${ids.length} ids, over the cap`,
        )
    }
    assertEquals(gate.barrier.size, 0, 'nothing is retained once drained')
})

// ---------------------------------------------------------------------------
// US3 — small rooms, the local fallback and roster-less drivers (T012)
//
// Characterisation rows: each describes behaviour that must NOT move, so each
// was green before the bounded read and has to stay green after it.
// ---------------------------------------------------------------------------

const plainAuthorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

/** Join `c-1 … c-n` as `u00001 … u0000n` and return the last joiner's `here`. */
async function joinInOrder(
    m: ChannelManager<User>,
    n: number,
): Promise<PresenceSnapshot> {
    let here: PresenceSnapshot | undefined
    for (let i = 1; i <= n; i++) {
        const cN = conn(`c-${i}`, idOf(i))
        m.register(cN)
        here = hereOf(await m.subscribe(cN, ROOM))
    }
    assert(here !== undefined)
    return here
}

const ORDER_OF_FIVE = [1, 2, 3, 4, 5].map(idOf)

Deno.test('#341 a room at or below K comes back whole, in join order, on the memory driver (SC-002)', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: plainAuthorize,
    })
    const here = await joinInOrder(m, 5)

    assertEquals(here.source, 'authoritative')
    assertEquals(idsOf(here), ORDER_OF_FIVE, 'every member, in join order')
    assertEquals(here.total, 5)
})

Deno.test('#341 a room at or below K comes back whole, in hash order, on Redis (SC-002)', async () => {
    const redis = new FakeRedis()
    for (let n = 1; n <= 4; n++) {
        const member = { id: idOf(n) }
        await redis.command(
            'HSET',
            PRESENCE_KEY,
            member.id,
            JSON.stringify({ member, owner: OWNER }),
        )
    }
    const driver = new RedisBroadcastDriver(
        redis,
        redis.subscriberFor(),
        { prefix: PREFIX, control: { secret: SECRET } },
    )
    try {
        const m = new ChannelManager<User>({
            driver,
            authorize: plainAuthorize,
        })
        const c5 = conn('c-5', idOf(5))
        m.register(c5)
        const here = hereOf(await m.subscribe(c5, ROOM))

        const hash = await redis.command('HGETALL', PRESENCE_KEY)
        const fields = (hash as { value: { value: string }[] }).value
            .filter((_, i) => i % 2 === 0)
            .map((field) => field.value)
        assertEquals(fields, ORDER_OF_FIVE, 'the fixture: the hash order')
        assertEquals(here.source, 'authoritative')
        assertEquals(
            idsOf(here),
            fields,
            'the whole room, in the order HGETALL reports — a room that fits ' +
                'is never sampled',
        )
        assertEquals(here.total, 5)
        redis.assertNoRejections()
    } finally {
        await driver.close()
    }
})

Deno.test("#341 the local fallback still answers a 'local' window of this instance's members", async () => {
    const m = new ChannelManager<User>({
        driver: {
            publish: () => {},
            onMessage: () => {},
            holdMember: () => Promise.resolve({ arrived: true }),
            releaseMember: () => Promise.resolve({ gone: true }),
            readRoster: (_channel, limit, selfIds) =>
                asWindow(
                    Promise.reject(new Error('broker unreachable')),
                    limit,
                    selfIds,
                ),
        },
        authorize: plainAuthorize,
        maxPresenceSnapshotMembers: 2,
    })
    const warn = console.warn
    console.warn = () => {}
    let here: PresenceSnapshot
    try {
        here = await joinInOrder(m, 3)
    } finally {
        console.warn = warn
    }

    assertEquals(here.source, 'local')
    assertEquals(here.total, 3, '`total` is the local population')
    assertEquals(
        idsOf(here),
        [idOf(1), idOf(3)],
        'cut to K, the joiner in the last slot',
    )
})

Deno.test("#341 a roster-less driver still answers an 'authoritative' window of its members", async () => {
    const m = new ChannelManager<User>({
        driver: {
            publish: () => {},
            onMessage: () => {},
        },
        authorize: plainAuthorize,
        maxPresenceSnapshotMembers: 2,
    })
    const here = await joinInOrder(m, 3)

    assertEquals(here.source, 'authoritative')
    assertEquals(here.total, 3)
    assertEquals(idsOf(here), [idOf(1), idOf(3)])
})
