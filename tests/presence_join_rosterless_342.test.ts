/**
 * @fileoverview #342 — a presence join on a driver without roster support.
 *
 * The roster ops on `BroadcastDriver` are OPTIONAL (FR-005), so a driver that
 * omits all three (`holdMember`, `releaseMember`, `readRoster`) is a driver the
 * contract allows. #342 found every first join on such a driver silent: the
 * roster write answered "no roster" with the same value that meant
 * "superseded", so `joined` was never announced.
 *
 * Since #344 there is no such return to confuse. `#syncRosterMember` resolves
 * `Promise<void>` and announces from inside the per-slot tail (#330) itself; on
 * a roster-less driver the arrival and departure come from the manager's own
 * `#heldSlots`, updated before the announcement runs.
 *
 * - W1: a roster-less first join announces `joined` exactly once.
 * - W2: a roster-less join an `unsubscribe` overtakes announces nothing —
 *   neither the `joined` its write no longer has a local entry for, nor a
 *   `left` from a release of a slot `#heldSlots` never held.
 *
 * **Known gaps, recorded rather than faked** (#342 review). Neither witness
 * here pins that the local read happens INSIDE the tail rather than at call
 * time on a roster-less driver (344-W8 pins it on a roster driver), and nothing
 * observes that a roster-less slot's tail entry is deleted once it settles —
 * private state no public surface exposes.
 *
 * @module @lockness/realtime/tests/presence_join_rosterless_342
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

/** Run the microtask queue out, so a resumed continuation reaches its next suspension. */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

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

/** Frames of `action` a connection received for `channel`. */
const presenceFrames = (c: Recording, action: string) =>
    c.received.filter((f) =>
        f.type === 'presence' && f.channel === CHANNEL && f.action === action
    )

Deno.test('#342 a roster-less first join announces joined exactly once', async () => {
    const published: Record<string, unknown>[] = []
    // NO `holdMember` / `releaseMember` / `readRoster` — the shape the contract
    // allows and neither built-in driver has.
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control as unknown as Record<string, unknown>)
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: (identity) => identity ? { id: identity.id } : false,
    })

    const observer = conn('c0', 1)
    manager.register(observer)
    await manager.subscribe(observer, CHANNEL)
    const newcomer = conn('c1', 2)
    manager.register(newcomer)
    await manager.subscribe(newcomer, CHANNEL)

    const joined = presenceFrames(observer, 'joined')
    assertEquals(
        joined.length,
        1,
        'the member already in the room is told about the newcomer once — on ' +
            'a roster-less driver there is no roster to refuse the join, so ' +
            'the local map is the authority and nothing superseded it',
    )
    assertEquals(joined[0].member, { id: 2 })
    assertEquals(
        published.filter((c) => c.kind === 'presence-join' && c.target === 'c1')
            .length,
        1,
        'and the other instances are told exactly once through the control plane',
    )
})

Deno.test('#342 a roster-less join overtaken by an unsubscribe announces nothing', async () => {
    // The gate order is `#330 a superseded join announces nothing`'s, on a
    // driver with the watch pair and no roster ops: the join claims, suspends
    // at `#watch`, and the leave lands first. The projection then reads a
    // local map without the member, so the join is superseded exactly as it
    // is on a roster-capable driver.
    const pending = new Map<string, () => void>()
    let seq = 0
    const gate = (name: string) =>
        new Promise<void>((resolve) => pending.set(`${name}#${++seq}`, resolve))
    const open = async (prefix: string) => {
        const key = [...pending.keys()].find((k) => k.startsWith(prefix))
        if (!key) throw new Error(`no pending gate named ${prefix}`)
        pending.get(key)!()
        pending.delete(key)
        await settle()
    }
    const drain = async () => {
        while (pending.size > 0) {
            const key = [...pending.keys()][0]
            pending.get(key)!()
            pending.delete(key)
            await settle()
        }
    }

    const published: Record<string, unknown>[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control as unknown as Record<string, unknown>)
            return gate('publishControl')
        },
        watchChannel: () => gate('watchChannel'),
        unwatchChannel: () => gate('unwatchChannel'),
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: async (identity) => {
            await gate('authorize')
            return identity ? { id: identity.id } : false
        },
    })

    // NOT `.catch(() => {})`: a join that threw after its claim would also
    // announce nothing, and swallowing it would read as a pass (#342 review).
    const c1 = conn('c1', 1)
    manager.register(c1)
    const join = manager.subscribe(c1, CHANNEL)
    await settle()
    await open('authorize')
    const leave = manager.unsubscribe('c1', CHANNEL)
    await settle()
    await open('unwatchChannel')
    await drain()
    const [joined, left] = await Promise.all([join, leave])
    assertEquals(
        joined.ok,
        true,
        'the superseded join resolves, it does not throw',
    )
    assertEquals(left, 'left', 'the leave that overtook it took the member out')

    assertEquals(
        published.filter((c) => c.kind === 'presence-join').length,
        0,
        'no instance is told a member joined when a leave already took it ' +
            'out of the local map — the authority on a roster-less driver',
    )
    // INTENTIONALLY INVERTED by #344 (plan §8, A1): this used to expect ONE
    // `presence-leave`, sent by the overtaking `unsubscribe` itself. A leave no
    // longer announces; the queued write that empties a slot does, and here
    // no write ever filled it — the superseded join held nothing. A `left`
    // for a member no `joined` was sent for is the defect 344-W6 pins.
    assertEquals(
        published.filter((c) => c.kind === 'presence-leave').length,
        0,
        'and the room hears no leave either — nobody was ever told it joined',
    )
})
