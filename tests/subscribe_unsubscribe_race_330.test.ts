/**
 * @fileoverview #330 — one authoritative roster write per slot, at a time.
 *
 * `onMessage` is dispatched as `void guard(...)`
 * (`packages/realtime/websocket.ts`), so nothing serializes the verbs a client
 * sends. A `subscribe` and an `unsubscribe` for the same connection and channel
 * could therefore both be in flight, each performing its own authoritative
 * roster write, and whichever reached the wire second decided the outcome.
 *
 * **The nondeterminism was in ISSUE order, not in the broker.** That distinction
 * is the whole reason the fix could live in the manager. `RedisClient.command`
 * chains onto `commandTail` *synchronously at call time*
 * (`packages/redis/client.ts`), so commands commit in the order they are
 * enqueued. What was unordered was when each verb reached its enqueue:
 * `#joinPresence` claimed the membership, suspended at `#watch`, and only then
 * issued `holdMember` — **from a state that no longer held the membership** —
 * while `RedisBroadcastDriver.holdMember` awaits `#ensureSweepStarted()` before
 * enqueueing, so the join's write could reach the tail after a removal called
 * later. The member was written back into the roster after being removed, with
 * no local membership and its `left` already announced. Only the ghost sweep
 * reclaims that, and it enumerates a DEAD instance's owned set, so a live
 * instance never reaches its own.
 *
 * `#joinLocal` and `#leaveLocal` never had this problem, and the reason is the
 * design: each computes its transition and issues its wire op in the SAME
 * synchronous turn. `#syncRosterMember` restores that property for the roster,
 * by deriving the desired state at issue time instead of carrying it from the
 * caller.
 *
 * ## What these tests assert, and why it is not a commit order
 *
 * The primary assertion is **structural**: the driver counts roster calls in
 * flight per `(channel, member.id)` slot and fails if the count ever exceeds
 * one. With at most one outstanding write there is no order left to choose, so
 * the tests drive *both* gate orders and require the same end state rather than
 * picking the unlucky one. A witness that had to pick would assert that a
 * particular interleaving is handled; this asserts that no interleaving exists.
 *
 * The rig resolves the driver's promises BY NAME. Nothing depends on how many
 * microtasks a call happens to take, which is what made an earlier attempt at
 * this witness flaky — it passed and failed across runs of the same command,
 * and a flaky test in the gate is worth less than none.
 *
 * `MemoryBroadcastDriver`'s roster ops are synchronous, so the defect this
 * closes was strictly cross-process.
 *
 * @module @lockness/realtime/tests/subscribe_unsubscribe_race_330
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

/** Run the microtask queue out, so a resumed continuation reaches its next suspension. */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

/**
 * Promises the test resolves by name, so the schedule is chosen rather than
 * observed. Keys carry a sequence number because `publishControl` is called by
 * both the join and the leave, and a name-keyed map would let the second call
 * replace the first's resolver — stranding a promise, which reads as a hang
 * rather than as a defect in the rig.
 */
function gates() {
    const pending = new Map<string, () => void>()
    let seq = 0
    const gate = (name: string) =>
        new Promise<void>((resolve) => pending.set(`${name}#${++seq}`, resolve))
    const open = async (prefix: string) => {
        const key = [...pending.keys()].find((k) => k.startsWith(prefix))
        if (!key) return false
        pending.get(key)!()
        pending.delete(key)
        await settle()
        return true
    }
    const drain = async () => {
        while (pending.size > 0) {
            const key = [...pending.keys()][0]
            pending.get(key)!()
            pending.delete(key)
            await settle()
        }
    }
    return { gate, open, drain, pending }
}

function conn(
    id: string,
    userId: number,
): Connection<User> & { readonly received: Record<string, unknown>[] } {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Connection<User> & { readonly received: Record<string, unknown>[] }
}

function rig(
    memberFor: (identity: User) => PresenceMember = (i) => ({ id: i.id }),
) {
    const g = gates()
    const roster = new Map<string, Map<string, PresenceMember>>()
    const published: Record<string, unknown>[] = []
    // THE STRUCTURAL ASSERTION'S INSTRUMENT. One counter per slot, and the
    // high-water mark is what the tests read — a count sampled at the end
    // would be zero however badly the writes had overlapped.
    const inFlight = new Map<string, number>()
    let peak = 0
    const enter = (channel: string, field: string) => {
        const key = `${channel} ${field}`
        const now = (inFlight.get(key) ?? 0) + 1
        inFlight.set(key, now)
        if (now > peak) peak = now
        return () => inFlight.set(key, (inFlight.get(key) ?? 1) - 1)
    }
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control as unknown as Record<string, unknown>)
            return g.gate('publishControl')
        },
        async holdMember(channel, member) {
            const leave = enter(channel, String(member.id))
            try {
                await g.gate('holdMember')
                let members = roster.get(channel)
                if (!members) roster.set(channel, members = new Map())
                const arrived = !members.has(String(member.id))
                members.set(String(member.id), member)
                return { arrived }
            } finally {
                leave()
            }
        },
        async releaseMember(channel, memberId) {
            const leave = enter(channel, String(memberId))
            try {
                await g.gate('releaseMember')
                return {
                    gone: roster.get(channel)?.delete(String(memberId)) ??
                        false,
                }
            } finally {
                leave()
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
        watchChannel: () => g.gate('watchChannel'),
        unwatchChannel: () => g.gate('unwatchChannel'),
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: async (identity) => {
            await g.gate('authorize')
            return identity ? memberFor(identity) : false
        },
    })
    return { manager, g, roster, published, peak: () => peak }
}

const keys = (roster: Map<string, Map<string, PresenceMember>>) => [
    ...(roster.get(CHANNEL)?.keys() ?? []),
]

/**
 * A join and a leave in flight together, resolved in the caller's chosen order.
 * Both orders must reach the same end state — that is the property, and picking
 * one would only prove that one interleaving is handled.
 */
async function race(first: 'watchChannel' | 'unwatchChannel') {
    const r = rig()
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const join = r.manager.subscribe(c1, CHANNEL).catch(() => {})
    await settle()
    await r.g.open('authorize')
    const leave = r.manager.unsubscribe('c1', CHANNEL).catch(() => {})
    await settle()
    await r.g.open(first)
    await r.g.drain()
    await Promise.allSettled([join, leave])
    return r
}

Deno.test('#330 a pipelined subscribe+unsubscribe leaves no roster ghost', async () => {
    for (const first of ['watchChannel', 'unwatchChannel'] as const) {
        const r = await race(first)
        assertEquals(
            keys(r.roster),
            [],
            `resolving ${first} first must still leave the authoritative ` +
                'roster empty — a member it holds that the instance does not ' +
                'have locally outlives the connection, the channel and the ' +
                'process, because no live instance sweeps its own owned set',
        )
        assertEquals(
            r.peak(),
            1,
            'and never more than ONE roster write in flight for the slot — ' +
                'the end state is only safe because there was no second ' +
                'write to arrive out of order',
        )
    }
})

Deno.test('#330 a superseded join announces nothing', async () => {
    // #323's rule reached from a new direction: an announcement must never
    // claim a membership the authoritative roster did not accept. When the
    // leave overtakes the join, the join's write becomes a removal — so the
    // roster holds no such member and a `presence-join` would tell every other
    // instance about one.
    //
    // NO LOCAL OBSERVER, and that is forced rather than convenient. The join
    // can only be overtaken when it awaits `#watch`, which `#joinLocal` skips
    // when the channel is already hosted — so a witness with a second local
    // member in the room never reaches the case at all, and its join honestly
    // wins. That leaves the CONTROL frame as the observable, which is the right
    // one anyway: it is what the other instances would have been told.
    const r = rig()
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const join = r.manager.subscribe(c1, CHANNEL).catch(() => {})
    await settle()
    await r.g.open('authorize')
    const leave = r.manager.unsubscribe('c1', CHANNEL).catch(() => {})
    await settle()
    // The leave gets there first. Opened the other way round the join's write
    // genuinely lands before the removal, and `joined` then `left` is the
    // correct, honest sequence — the first draft of this test asserted silence
    // on an interleaving where speaking was right.
    await r.g.open('unwatchChannel')
    await r.g.drain()
    await Promise.allSettled([join, leave])

    assertEquals(
        r.published.filter((c) => c.kind === 'presence-join').length,
        0,
        'no instance is told a member joined when the roster does not hold it',
    )
    assertEquals(keys(r.roster), [], 'and the roster is empty')
    assertEquals(r.peak(), 1, 'one write in flight per slot throughout')
})

Deno.test('#330 a stale removal cannot land on top of a fresh re-join', async () => {
    // The mirror of the ghost, and the case a three-line claim re-validation
    // would have missed: leave, then re-subscribe. #327 and #331 make that
    // ordinary traffic — a reconnecting client does exactly this.
    const r = rig()
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const first = r.manager.subscribe(c1, CHANNEL)
    await settle()
    await r.g.drain()
    await first
    assertEquals(keys(r.roster), ['1'], 'precondition: the member is in')

    const leave = r.manager.unsubscribe('c1', CHANNEL).catch(() => {})
    await settle()
    const rejoin = r.manager.subscribe(c1, CHANNEL).catch(() => {})
    await settle()
    await r.g.drain()
    await Promise.allSettled([leave, rejoin])

    assertEquals(
        keys(r.roster),
        ['1'],
        'the re-join is the last word — a removal queued before it must not ' +
            'erase a membership the instance now holds',
    )
    assertEquals(r.peak(), 1, 'still one write in flight per slot')
})

Deno.test('#330 two connections sharing one member id are ONE roster slot', async () => {
    // The roster hash is keyed by `member.id`, not by connection id, so the
    // projection has to read the whole local map rather than one entry. Two
    // devices on one account is the everyday shape of this.
    const r = rig(() => ({ id: 7 }))
    const rosterReadsBefore = rosterReadCount()
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const a = r.manager.subscribe(c1, CHANNEL)
    await settle()
    await r.g.drain()
    await a
    assertRosterRead(rosterReadsBefore)
    const c2 = conn('c2', 2)
    r.manager.register(c2)
    const b = r.manager.subscribe(c2, CHANNEL)
    await settle()
    await r.g.drain()
    await b
    assertEquals(keys(r.roster), ['7'], 'one slot, not two')

    const goneA = r.manager.unsubscribe('c1', CHANNEL)
    await settle()
    await r.g.drain()
    await goneA

    assertEquals(
        keys(r.roster),
        ['7'],
        'one device leaving must not evict the account — the other ' +
            'connection still holds the slot, and the projection sees it',
    )
    assert(r.peak() <= 1, 'and the slot was never written concurrently')

    const goneB = r.manager.unsubscribe('c2', CHANNEL)
    await settle()
    await r.g.drain()
    await goneB
    assertEquals(keys(r.roster), [], 'the last one out removes the slot')
})
