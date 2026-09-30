/**
 * @fileoverview #339 — a presence subscribe returns a BOUNDED snapshot that
 * says when it is partial, and keeps the joiner in it.
 *
 * Before this, `SubscribeResult.members` was the whole room, cluster-wide, with
 * every member's `info`: 10 000 members at the 4096-byte member ceiling put
 * 40 970 001 bytes into one reply, on every join and every re-join. #333 bounded
 * how OFTEN that read happens; this bounds how LARGE one reply is.
 *
 * ## Why the literals are literals
 *
 * The plan's decision table gives the default bound one home,
 * `MAX_PRESENCE_SNAPSHOT_MEMBERS` in `manager.ts`, and forbids a literal `100`
 * elsewhere — with ONE exception, this file. A test that only imported the
 * constant would pass a `100 → 101` edit, because it would measure against the
 * edited number. Pinning `100`, `409 701` and the frame size literally, beside
 * an assertion that the constant still equals `100`, is what makes the ceiling
 * a witness rather than a restatement.
 *
 * ## Driver order
 *
 * Rows 1, 4 and 6 need the joiner to land LAST in driver order, so that keeping
 * it is a replacement rather than a coincidence. Only the memory driver and the
 * `FakeRedis` fake guarantee join order; Redis hash order does not. Each of
 * those rows says so at its call site.
 *
 * @module @lockness/realtime/tests/presence_snapshot_bound_339
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import {
    ChannelManager,
    MAX_PRESENCE_SNAPSHOT_MEMBERS,
    type SubscribeResult,
} from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { encodeServerMessage } from '../protocol.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember, PresenceSnapshot } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const ROOM = 'presence-room'
const MEMBER_BYTES = 4096

/** A member whose `JSON.stringify` is exactly {@link MEMBER_BYTES} bytes. */
function paddedMember(id: number): PresenceMember {
    const base = JSON.stringify({ id, info: { pad: '' } }).length
    return { id, info: { pad: 'x'.repeat(MEMBER_BYTES - base) } }
}

function conn(
    id: string,
    userId: number,
    sent: string[] = [],
): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (frame: string) => sent.push(frame),
        close: () => {},
    } as unknown as Connection<User>
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? paddedMember(identity.id) : false

const bytes = (value: unknown) =>
    new TextEncoder().encode(JSON.stringify(value)).length

/** The snapshot a join returned, asserted present. */
function hereOf(result: SubscribeResult): PresenceSnapshot {
    assert(result.ok, 'the join succeeded')
    assert(result.here !== undefined, 'a presence join carries a snapshot')
    return result.here
}

const ids = (snapshot: PresenceSnapshot) => snapshot.members.map((m) => m.id)

const CONSOLE_METHODS = ['log', 'info', 'warn', 'error', 'debug'] as const

/**
 * Run `work` with every console method replaced by a counter, and return how
 * many times any of them was called. The manager has no logger of its own —
 * `console` is its only log sink — so this is every log it could emit.
 */
async function consoleCallsDuring<T>(
    work: () => Promise<T>,
): Promise<{ result: T; calls: string[] }> {
    const calls: string[] = []
    const saved = CONSOLE_METHODS.map((name) => [name, console[name]] as const)
    for (const name of CONSOLE_METHODS) {
        console[name] = (...args: unknown[]) => {
            calls.push(`${name}: ${args.map(String).join(' ')}`)
        }
    }
    try {
        return { result: await work(), calls }
    } finally {
        for (const [name, fn] of saved) console[name] = fn
    }
}

/**
 * A memory driver pre-seeded with 250 members from "other instances", counting
 * roster writes. The 251st member is whoever subscribes next.
 */
function roomOf250() {
    const driver = new MemoryBroadcastDriver()
    for (let id = 1; id <= 250; id++) driver.holdMember(ROOM, paddedMember(id))
    let writes = 0
    const add = driver.holdMember.bind(driver)
    driver.holdMember = (channel, member) => {
        writes++
        return add(channel, member)
    }
    return {
        driver,
        get writes(): number {
            return writes
        },
    }
}

Deno.test('#339 the default ceiling is pinned: 100 members, 409 701 bytes, self kept', async () => {
    assertEquals(
        MAX_PRESENCE_SNAPSHOT_MEMBERS,
        100,
        'the default bound is a published number — a witness, not a magic one',
    )
    // DRIVER ORDER: the memory driver lists in join order, so the joiner is the
    // 251st entry and falls outside the first 100. Redis hash order would not
    // guarantee that.
    const room = roomOf250()
    const m = new ChannelManager<User>({ driver: room.driver, authorize })

    const c251 = conn('c251', 251)
    m.register(c251)
    const { result, calls } = await consoleCallsDuring(() =>
        m.subscribe(c251, ROOM)
    )
    const here = hereOf(result)

    assertEquals(
        calls,
        [],
        'FR-008: cutting is the designed reply, not a fault — no log at any level',
    )
    assertEquals(here.total, 251, '`total` is the room, counted before the cut')
    assertEquals(here.members.length, 100, 'the reply is cut to the bound')
    assert(ids(here).includes(251), 'the joiner is in its own snapshot')
    assertEquals(here.source, 'authoritative')
    assertEquals(
        bytes(here.members),
        409_701,
        'K·(M+1)+1 with K = 100 and M = 4096 — the most member JSON one ' +
            'subscribe can hand the application, however large the room',
    )
    // THE MEMBER BYTES ABOVE ARE THE CEILING. The framework never builds a
    // `subscribed` frame — the application does — so the envelope is built
    // here with `encodeServerMessage`, and its extra 70 bytes
    // (`{"type":"subscribed","channel":"presence-room","members":` … `,"total":251}`)
    // are this test's framing, not a second framework number.
    assertEquals(
        new TextEncoder().encode(encodeServerMessage({
            type: 'subscribed',
            channel: ROOM,
            members: here.members,
            total: here.total,
        })).length,
        409_771,
        'and the encoded `subscribed` frame carrying it',
    )
})

Deno.test('#339 a configured bound is honoured: 10 members, 40 971 bytes', async () => {
    // DRIVER ORDER: memory driver, joiner last — see the file header.
    const room = roomOf250()
    const m = new ChannelManager<User>({
        driver: room.driver,
        authorize,
        maxPresenceSnapshotMembers: 10,
    })

    const c251 = conn('c251', 251)
    m.register(c251)
    const here = hereOf(await m.subscribe(c251, ROOM))

    assertEquals(here.total, 251)
    assertEquals(here.members.length, 10)
    assert(ids(here).includes(251), 'self included at a configured bound too')
    assertEquals(bytes(here.members), 40_971)
    // The same 70-byte envelope the default row explains: built by this test,
    // never by the framework. `40 971` is the ceiling; this is its framing.
    assertEquals(
        new TextEncoder().encode(encodeServerMessage({
            type: 'subscribed',
            channel: ROOM,
            members: here.members,
            total: here.total,
        })).length,
        41_041,
    )
})

Deno.test('#339 a room within the bound is returned whole, in driver order', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize,
    })
    // Id 2 joins BEFORE id 1, so driver order is not sorted order — a sort
    // anywhere on the fitting path turns [2, 1] into [1, 2] and is caught.
    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, ROOM)
    const c1 = conn('c1', 1)
    m.register(c1)
    const here = hereOf(await m.subscribe(c1, ROOM))

    assertEquals(ids(here), [2, 1], 'unchanged, and not re-ordered')
    assertEquals(
        here.total,
        here.members.length,
        'a whole room says it is whole',
    )
    assertEquals(here.source, 'authoritative')
})

Deno.test("#339 a re-join over the bound has the first join's shape", async () => {
    // DRIVER ORDER: memory driver, joiner last — see the file header.
    const room = roomOf250()
    const sent: string[] = []
    const m = new ChannelManager<User>({ driver: room.driver, authorize })
    const observer = conn('c1', 1, sent)
    m.register(observer)
    await m.subscribe(observer, ROOM)
    const c251 = conn('c251', 251)
    m.register(c251)
    await m.subscribe(c251, ROOM)
    const writes = room.writes
    const frames = sent.length

    const here = hereOf(await m.subscribe(c251, ROOM))

    assertEquals(here.members.length, 100)
    assertEquals(here.total, 251)
    assert(here.members.length < here.total, 'partial, and it SAYS so')
    assert(ids(here).includes(251), 'the re-joiner is in its own snapshot')
    assertEquals(room.writes, writes, 'a re-join writes nothing (#327)')
    assertEquals(sent.length, frames, 'and announces nothing (#327)')
})

Deno.test('#339 the local fallback is bounded by the same rule', async () => {
    // Every read rejects, so every join falls back to this instance's own
    // members — which is where a cut applied only to the authoritative branch
    // would let an unbounded list through.
    const driver: BroadcastDriver = {
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
    }
    const m = new ChannelManager<User>({
        driver,
        authorize,
        maxPresenceSnapshotMembers: 10,
    })
    const warn = console.warn
    console.warn = () => {}
    let here: PresenceSnapshot
    try {
        for (let id = 1; id <= 11; id++) {
            const cN = conn(`c${id}`, id)
            m.register(cN)
            await m.subscribe(cN, ROOM)
        }
        const c12 = conn('c12', 12)
        m.register(c12)
        here = hereOf(await m.subscribe(c12, ROOM))
    } finally {
        console.warn = warn
    }

    assertEquals(here.source, 'local')
    assertEquals(here.members.length, 10, 'cut to the bound')
    assertEquals(here.total, 12, "`total` counts this instance's members")
    assert(ids(here).includes(12), 'self kept on the fallback too')
})

/** A roster driver whose reads are held until released (see #333's witness). */
function gatedRosterDriver() {
    const store = new Map<string, PresenceMember>()
    const releases: (() => void)[] = []
    let reads = 0
    let open = false
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(_channel, member) {
            const arrived = !store.has(String(member.id))
            store.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(_channel, memberId) {
            return { gone: store.delete(String(memberId)) }
        },
        readRoster(_channel, limit, selfIds) {
            return asWindow(
                (() => {
                    reads++
                    const snapshot = [...store.values()]
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
        seed(member: PresenceMember) {
            store.set(String(member.id), member)
        },
        get reads(): number {
            return reads
        },
        release(): void {
            for (const fire of releases.splice(0)) fire()
        },
        openGate(): void {
            open = true
            this.release()
        },
    }
}

const drain = () => new Promise((r) => setTimeout(r, 0))

Deno.test('#339 joiners sharing one read each keep THEMSELVES, never each other', async () => {
    // DRIVER ORDER: this double lists in insertion order, like the memory
    // driver — three seeded members first, then A, B and C as they commit.
    const gate = gatedRosterDriver()
    for (let id = 91; id <= 93; id++) gate.seed(paddedMember(id))
    const m = new ChannelManager<User>({
        driver: gate.driver,
        authorize,
        maxPresenceSnapshotMembers: 2,
    })

    const cA = conn('cA', 1)
    m.register(cA)
    const a = m.subscribe(cA, ROOM)
    await drain()
    assertEquals(gate.reads, 1, 'A issued read 1')
    const cB = conn('cB', 2)
    m.register(cB)
    const b = m.subscribe(cB, ROOM)
    const cC = conn('cC', 3)
    m.register(cC)
    const c = m.subscribe(cC, ROOM)
    await drain()
    assertEquals(gate.reads, 1, 'B and C are waiting on the barrier')
    gate.release()
    await drain()
    assertEquals(gate.reads, 2, 'B and C share read 2')
    gate.openGate()

    await a
    const hereB = hereOf(await b)
    const hereC = hereOf(await c)

    assertEquals(hereB.total, 6)
    assertEquals(hereC.total, 6)
    assertEquals(ids(hereB), [91, 2], "B's snapshot holds B, in the last slot")
    assertEquals(ids(hereC), [91, 3], "C's snapshot holds C, and not B")
    // NO "each caller owns its array" assertion here, deliberately. Above the
    // bound every caller's `members` is a fresh `slice`, on top of the
    // per-caller spread in `rosterSnapshot` — no single mutant can make B's
    // and C's arrays the same object, so the assertion could never fail. The
    // spread is guarded where it CAN fail, within the bound:
    // `presence_roster_read_333.test.ts`, "#333 every caller of a shared read
    // gets its OWN array".
})

Deno.test('#339 a join overtaken by its own leave, above the bound, keeps no self', async () => {
    // FR-006. The join commits and its closing read is issued with the joiner
    // in the roster; the connection then unsubscribes before that read
    // settles. Self is looked up AFTER the await, so the settled snapshot has
    // no self to keep — a self captured before the read would put a member
    // the connection no longer holds into its reply.
    //
    // DRIVER ORDER: this double lists in insertion order, and the read's
    // snapshot is taken when it is issued — 250 seeded members, then 251.
    const gate = gatedRosterDriver()
    for (let id = 1; id <= 250; id++) gate.seed(paddedMember(id))
    const m = new ChannelManager<User>({ driver: gate.driver, authorize })

    const c251 = conn('c251', 251)
    m.register(c251)
    const join = m.subscribe(c251, ROOM)
    await drain()
    assertEquals(gate.reads, 1, "the join's closing read is in flight")
    await m.unsubscribe('c251', ROOM)
    gate.openGate()
    const here = hereOf(await join)

    assert(here.members.length <= 100, 'still bounded')
    assertEquals(here.members.length, 100)
    assertEquals(here.total, 251, 'the read saw the joiner, and counts it')
    assert(
        !ids(here).includes(251),
        'the connection holds no member any more, so no self is kept',
    )
})

Deno.test('#339 construction refuses a bound that is not a positive integer', () => {
    for (const bad of [0, 1.5, NaN]) {
        assertThrows(
            () =>
                new ChannelManager<User>({
                    authorize,
                    maxPresenceSnapshotMembers: bad,
                }),
            Error,
            'maxPresenceSnapshotMembers must be a positive integer',
        )
    }
})
