/**
 * @fileoverview #331 — a denial refuses the attempt and revokes nothing.
 *
 * `authorize` runs on every private/presence subscribe, re-subscribes included.
 * When an authorizer that previously approved now denies — revoked role,
 * expired entitlement, ban — `subscribe` answers `{ ok: false }` and the
 * standing subscription is left intact. The connection is told no and keeps
 * listening.
 *
 * **That is the decision, not an oversight**, and these tests exist so it
 * cannot be quietly reversed. `authorize` gates ADMISSION; `subscribe` adds or
 * does nothing, and never removes. Revocation is an explicit server-side verb.
 *
 * The reason `false` may not be given the force of an eviction is that it
 * already means other things. {@link Authorizer} sanctions an authorizer that
 * is a DB read, an audit write or a rate-limit increment, so a deployed `false`
 * carries "not this fast" and "I could not check" as well as "you may not".
 * Revoking on it would turn a database blip into a removal — silently, with no
 * compile error, and with no way to express the distinction while the
 * `AuthorizeResult` shape stays as it is.
 *
 * **Every assertion here is falsifiable**: each one fails if `subscribe` ever
 * starts revoking on a denial. A test that asserted only `ok === false` would
 * pass either way and would guard nothing.
 *
 * @module @lockness/realtime/tests/authorize_denial_331
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver, BroadcastMessage } from '../driver.ts'
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
const PRIVATE = 'private-orders'

/** A connection that records every frame it received. */
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

/** A working driver that records every command it was asked to run. */
function recordingDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const commands: string[] = []
    // The manager registers a delivery handler through `onMessage`, and
    // `broadcast` reaches subscribers only by going out through `publish` and
    // coming back through it. A fake whose `publish` is a no-op delivers
    // nothing, and every "still receives" assertion below would pass for the
    // wrong reason — so this one loops, as the broker does.
    let deliver: ((message: BroadcastMessage) => void) | undefined
    const driver: BroadcastDriver = {
        publish: (message) => deliver?.(message),
        onMessage: (handler) => void (deliver = handler),
        holdMember(channel, member) {
            commands.push(`holdMember ${channel}`)
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            commands.push(`releaseMember ${channel} ${memberId}`)
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
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
        onControl: () => {},
        publishControl(control) {
            commands.push(`publishControl ${control.kind}`)
            return Promise.resolve()
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    return { driver, roster, commands }
}

/** An authorizer the test can revoke mid-run, as a real one is revoked. */
function revocable() {
    const state = { allow: true }
    const authorize = (identity: User | null): PresenceMember | false =>
        state.allow && identity ? { id: identity.id } : false
    return { state, authorize }
}

Deno.test('#331 a denied re-subscribe leaves delivery, roster and membership intact', async () => {
    const { driver, roster, commands } = recordingDriver()
    const { state, authorize } = revocable()
    const m = new ChannelManager<User>({ driver, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    const observer = conn('c2', 2)
    m.register(observer)
    const rosterReadsBefore = rosterReadCount()
    await m.subscribe(holder, CHANNEL)
    assertRosterRead(rosterReadsBefore)
    await m.subscribe(observer, CHANNEL)

    // Baseline AFTER both joins, so the denial's effect is isolated rather
    // than inferred by subtracting a number nobody measured.
    const before = commands.length

    state.allow = false
    const denied = await m.subscribe(holder, CHANNEL)

    assertEquals(denied.ok, false, 'the frame is refused')
    assertEquals(
        denied.here,
        undefined,
        'and it carries no roster — a denied caller learns nothing about the ' +
            'room, which is why the authorizer runs before the re-join guard',
    )

    // The four assertions that make this a decision rather than a shrug.
    await m.broadcast(CHANNEL, 'tick', { n: 1 })
    assertEquals(
        holder.received.filter((f) => f.event === 'tick').length,
        1,
        'the connection STILL RECEIVES broadcasts — the subscription stands',
    )
    assertEquals(
        [...(roster.get(CHANNEL)?.keys() ?? [])],
        ['1', '2'],
        'and the authoritative roster still lists it',
    )
    assertEquals(
        observer.received.filter((f) => f.action === 'left').length,
        0,
        'no `left` was announced to the room',
    )
    assertEquals(
        commands.slice(before).filter((c) => !c.startsWith('publish ')),
        [],
        'and the denied frame ran no driver command at all — no releaseMember, ' +
            'no presence-leave publish',
    )
})

Deno.test('#331 a denial on a PRIVATE channel revokes nothing either', async () => {
    // Private channels have no roster and no presence frames, so the only
    // observable is delivery — which is the one that matters.
    const { driver, commands } = recordingDriver()
    const { state, authorize } = revocable()
    const m = new ChannelManager<User>({ driver, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    await m.subscribe(holder, PRIVATE)
    const before = commands.length

    state.allow = false
    assertEquals((await m.subscribe(holder, PRIVATE)).ok, false)

    await m.broadcast(PRIVATE, 'tick', { n: 1 })
    assertEquals(
        holder.received.filter((f) => f.event === 'tick').length,
        1,
        'delivery survives the denial on a private channel too',
    )
    assertEquals(
        commands.slice(before).filter((c) => !c.startsWith('publish ')),
        [],
        'and nothing was asked of the driver',
    )
})

Deno.test('#331 unsubscribe is what actually revokes', async () => {
    // The other half of the contract, and the reason the first half is safe:
    // a server that wants the connection gone has a verb for it, and that verb
    // does the whole job — delivery stops, the roster entry goes, the room is
    // told.
    const { driver, roster } = recordingDriver()
    const { authorize } = revocable()
    const m = new ChannelManager<User>({ driver, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(holder, CHANNEL)
    await m.subscribe(observer, CHANNEL)

    await m.unsubscribe('c1', CHANNEL)

    await m.broadcast(CHANNEL, 'tick', { n: 1 })
    assertEquals(
        holder.received.filter((f) => f.event === 'tick').length,
        0,
        'delivery stops',
    )
    assertEquals(
        [...(roster.get(CHANNEL)?.keys() ?? [])],
        ['2'],
        'the roster entry is gone',
    )
    assertEquals(
        observer.received.filter((f) => f.action === 'left').length,
        1,
        'and the room is told exactly once',
    )
})
