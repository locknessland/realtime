/**
 * @fileoverview #343 — the LOCAL presence view counts members, not connections.
 *
 * The `presence` map is keyed by connection id, so one member holding two tabs
 * on this instance is two values in it. The authoritative roster is a hash
 * keyed by `String(member.id)` — one slot per member. Every path that read the
 * map's values raw therefore answered a different room than the roster: on the
 * local fallback and on a roster-less driver, `members` listed the member
 * twice and `total` counted connections.
 *
 * The fix is one rule, `uniqueMembers`, reached through one private
 * `#localRoster(channel)`: one entry per `String(id)`, first occurrence wins,
 * order kept. A re-join never re-inserts (#327), so map order is join order and
 * "first occurrence" is the earliest-joined connection still subscribed here —
 * the same connection the roster slot holds (#330).
 *
 * Every row seeds c1 (member 7, info `a`), c3 (member 8), c2 (member 7, info
 * `b`), in that order, unless it says otherwise.
 *
 * @module @lockness/realtime/tests/presence_local_member_343
 */

import { assert, assertEquals, assertNotStrictEquals } from '@std/assert'
import { ChannelManager, type SubscribeResult } from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { uniqueMembers } from '../presence_snapshot.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember, PresenceSnapshot } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number | string
    tab: string
}

const ROOM = 'presence-room'

function conn(id: string, userId: number | string, tab = ''): Connection<User> {
    return {
        id,
        identity: { id: userId, tab },
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<User>
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id, info: { tab: identity.tab } } : false

/** The snapshot a join returned, asserted present. */
function hereOf(result: SubscribeResult): PresenceSnapshot {
    assert(result.ok, 'the join succeeded')
    assert(result.here !== undefined, 'a presence join carries a snapshot')
    return result.here
}

const ids = (snapshot: PresenceSnapshot) => snapshot.members.map((m) => m.id)
const infoOf = (snapshot: PresenceSnapshot, id: number | string) =>
    snapshot.members.find((m) => String(m.id) === String(id))?.info

/** A roster-capable driver whose every read rejects — the local fallback. */
function unreadableDriver(): BroadcastDriver {
    return {
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
}

/** A driver with NO roster ops — the shape FR-005 allows (#342). */
function rosterlessDriver(): BroadcastDriver {
    return {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl: () => {},
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
}

/** Run `work` with `console.warn` silenced; the fallback WARN is by design. */
async function quietly<T>(work: () => Promise<T>): Promise<T> {
    const warn = console.warn
    console.warn = () => {}
    try {
        return await work()
    } finally {
        console.warn = warn
    }
}

/** The seeded sockets, held so a later call on one reuses its object. */
type Tabs = Record<'c1' | 'c2' | 'c3', Connection<User>>

/**
 * Seed c1 (7, a), c3 (8), then return c2 (7, b)'s join snapshot. Each socket
 * is registered at its open and recorded in `tabs` when one is passed.
 */
async function seedTwoTabs(
    m: ChannelManager<User>,
    tabs?: Partial<Tabs>,
): Promise<PresenceSnapshot> {
    const c1 = conn('c1', 7, 'a')
    m.register(c1)
    await m.subscribe(c1, ROOM)
    const c3 = conn('c3', 8)
    m.register(c3)
    await m.subscribe(c3, ROOM)
    const c2 = conn('c2', 7, 'b')
    m.register(c2)
    if (tabs) Object.assign(tabs, { c1, c2, c3 })
    return hereOf(await m.subscribe(c2, ROOM))
}

Deno.test('#343 the local fallback lists one entry per member', async () => {
    const m = new ChannelManager<User>({
        driver: unreadableDriver(),
        authorize,
    })

    const here = await quietly(() => seedTwoTabs(m))

    assertEquals(here.source, 'local')
    assertEquals(ids(here), [7, 8], 'member 7 once, in join order')
    assertEquals(here.total, 2, '`total` counts members, not connections')
    assertEquals(
        infoOf(here, 7),
        { tab: 'a' },
        "the earliest-joined connection's info — the one the roster slot holds",
    )
})

Deno.test('#343 a roster-less driver lists one entry per member', async () => {
    const m = new ChannelManager<User>({
        driver: rosterlessDriver(),
        authorize,
    })

    const here = await seedTwoTabs(m)

    assertEquals(here.source, 'authoritative')
    assertEquals(ids(here), [7, 8])
    assertEquals(here.total, 2)
    assertEquals(infoOf(here, 7), { tab: 'a' })
})

Deno.test('#343 when the winning connection leaves, the next one speaks for the member', async () => {
    const run = async (driver: BroadcastDriver) => {
        const m = new ChannelManager<User>({ driver, authorize })
        const tabs: Partial<Tabs> = {}
        await seedTwoTabs(m, tabs)
        await m.unsubscribe('c1', ROOM)
        return hereOf(await m.subscribe(tabs.c2!, ROOM))
    }

    const authoritative = await run(new MemoryBroadcastDriver())
    const local = await quietly(() => run(unreadableDriver()))

    assertEquals(authoritative.source, 'authoritative')
    assertEquals(local.source, 'local')
    assertEquals(
        infoOf(authoritative, 7),
        { tab: 'b' },
        '#330 rewrote the slot',
    )
    assertEquals(authoritative.total, 2)
    assertEquals(infoOf(local, 7), { tab: 'b' }, 'and the local view follows')
    assertEquals(local.total, 2)
    assertEquals(
        [...ids(local)].sort(),
        [...ids(authoritative)].sort(),
        'both sources describe the same room',
    )
})

Deno.test('#343 ids 1 and "1" are one member on the local view', async () => {
    const m = new ChannelManager<User>({
        driver: unreadableDriver(),
        authorize,
    })

    const here = await quietly(async () => {
        const c1 = conn('c1', 1, 'a')
        m.register(c1)
        await m.subscribe(c1, ROOM)
        const c2 = conn('c2', '1', 'b')
        m.register(c2)
        return hereOf(await m.subscribe(c2, ROOM))
    })

    assertEquals(here.total, 1, 'the roster hash is keyed by String(id)')
    assertEquals(ids(here), [1])
})

Deno.test('#343 the bound cuts members, and `total` counts members', async () => {
    const m = new ChannelManager<User>({
        driver: unreadableDriver(),
        authorize,
        maxPresenceSnapshotMembers: 1,
    })

    const here = await quietly(async () => {
        const tabs: Partial<Tabs> = {}
        await seedTwoTabs(m, tabs)
        return hereOf(await m.subscribe(tabs.c3!, ROOM))
    })

    assertEquals(ids(here), [8], 'self kept, looked up by connection id')
    assertEquals(here.total, 2, 'two members, however many tabs')
})

Deno.test('#343 join order is kept, not sorted, on the local view', async () => {
    const m = new ChannelManager<User>({
        driver: rosterlessDriver(),
        authorize,
    })

    const c9 = conn('c9', 9)
    m.register(c9)
    await m.subscribe(c9, ROOM)
    const c7a = conn('c7a', 7, 'a')
    m.register(c7a)
    await m.subscribe(c7a, ROOM)
    const c8 = conn('c8', 8)
    m.register(c8)
    await m.subscribe(c8, ROOM)
    const c7b = conn('c7b', 7, 'b')
    m.register(c7b)
    const here = hereOf(await m.subscribe(c7b, ROOM))

    assertEquals(ids(here), [9, 7, 8], 'first-join order, member 7 once')
    assertEquals(here.total, 3)
})

Deno.test("#343 under the bound, self as a member's SECOND connection is kept once", async () => {
    // Self is looked up by connection id and matched by member id. When the
    // caller is the later tab of a deduped member, the kept entry is the
    // earlier tab's — one entry, the roster's info, never self twice.
    const m = new ChannelManager<User>({
        driver: unreadableDriver(),
        authorize,
        maxPresenceSnapshotMembers: 1,
    })

    const here = await quietly(async () => {
        const c3 = conn('c3', 8)
        m.register(c3)
        await m.subscribe(c3, ROOM)
        const c1 = conn('c1', 7, 'a')
        m.register(c1)
        await m.subscribe(c1, ROOM)
        const c2 = conn('c2', 7, 'b')
        m.register(c2)
        return hereOf(await m.subscribe(c2, ROOM))
    })

    assertEquals(ids(here), [7], 'self kept, and only once')
    assertEquals(infoOf(here, 7), { tab: 'a' }, "the earliest tab's info")
    assertEquals(here.total, 2)
})

Deno.test('#343 unit: uniqueMembers keeps the first occurrence, in order, without mutating', () => {
    // OUT OF ORDER on purpose (#343 review): ascending input cannot tell
    // "order kept" from "sorted".
    const input: PresenceMember[] = [
        { id: 9 },
        { id: 7, info: { tab: 'a' } },
        { id: 8 },
        { id: '7', info: { tab: 'b' } },
        { id: 8, info: { tab: 'late' } },
    ]
    const before = structuredClone(input)

    const out = uniqueMembers(input)

    assertEquals(out, [
        { id: 9 },
        { id: 7, info: { tab: 'a' } },
        { id: 8 },
    ])
    assertEquals(input, before, 'the input is left exactly as it came')
    assertNotStrictEquals(out, input, 'a fresh array, never the input')
    assertEquals(uniqueMembers([]), [])
})
