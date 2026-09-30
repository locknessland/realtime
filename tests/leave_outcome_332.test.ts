/**
 * @fileoverview #332 — a leave verb reports what it did, and reports it from
 * the right predicate.
 *
 * `unsubscribe(clientId, channel)` sits in the manager's **local** tier while
 * looking like an addressed one, because it takes a routable `clientId` where
 * `subscribe` takes a `Connection`. Called with an id owned by another instance
 * it removed nothing, announced nothing, and resolved `Promise<void>` — "I
 * removed it", "it was not in that room" and "you are talking to the wrong
 * process" all spelled `undefined`. `disconnect(clientId)` had the identical
 * defect one method over, in the verb whose name promises more.
 *
 * ## The trap this file exists to catch
 *
 * `#leaveLocal` has **one** membership predicate and **three** exits, and two
 * of the three mean *left*:
 *
 * ```ts
 * if (!set?.delete(clientId)) return false   // ← the only membership decision
 * this.#channelsByClient.get(clientId)?.delete(channel)
 * if (set.size > 0) return true             // left; others remain
 * this.subscriptions.delete(channel)
 * await this.#watcher?.unwatchChannel(channel)
 * return true                                // left; and we stopped hosting
 * ```
 *
 * A report taken from the **end** of that method is reached only on the 1→0
 * transition — so `unsubscribe` would answer `'not-subscribed'` for every leave
 * from a room that still holds somebody else. That is the common case, and
 * **every other fixture in this package is single-member**, so the mistake
 * passes the entire suite. The two-member test below is the only witness.
 *
 * @module @lockness/realtime/tests/leave_outcome_332
 */

import { assertEquals } from '@std/assert'
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

const ROOM = 'presence-room'
const PRIVATE = 'private-orders'

function conn(id: string, userId: number): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    } as Connection<User>
}

/** A driver that records every command it was asked to run. */
function recordingDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const commands: string[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
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

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

Deno.test("#332 a leave from a room that still holds SOMEONE ELSE reports 'left'", async () => {
    // THE COMMON CASE, and the one no other fixture in this package reaches.
    // A report taken from the tail of `#leaveLocal` — after the 1→0 branch —
    // answers 'not-subscribed' here and passes every single-member test.
    const { driver, roster } = recordingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const rosterReadsBefore = rosterReadCount()
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    assertRosterRead(rosterReadsBefore)
    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, ROOM)

    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'left',
        'a membership WAS removed; that the room still has another member ' +
            'decides whether we stop hosting, not whether this leave happened',
    )
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])],
        ['2'],
        'and the other member is untouched',
    )

    // The 1→0 case still reports the same thing, from the same predicate.
    assertEquals(await m.unsubscribe('c2', ROOM), 'left')
})

Deno.test('#332 the three outcomes are distinguishable', async () => {
    const { driver, commands } = recordingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    await m.subscribe(holder, ROOM)

    assertEquals(await m.unsubscribe('c1', ROOM), 'left')

    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'not-subscribed',
        'this instance owns the socket and it is no longer in that room — ' +
            'idempotent, correct, and NOT the same fact as a wrong process',
    )
    assertEquals(
        await m.unsubscribe('c1', 'presence-elsewhere'),
        'not-subscribed',
        'owned, never joined that room',
    )

    const before = commands.length
    assertEquals(
        await m.unsubscribe('nobody-here', ROOM),
        'not-owned',
        'the socket lives on another instance',
    )
    assertEquals(
        commands.slice(before),
        [],
        'and a misaddressed leave runs NO driver command and publishes NO ' +
            'control frame — it did not reach across, it reported that it ' +
            'could not',
    )
})

Deno.test('#332 the outcome is the same on a channel with no roster', async () => {
    // A private channel has no presence block at all, so the outcome cannot be
    // a side effect of the roster path.
    const { driver } = recordingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, PRIVATE)

    assertEquals(await m.unsubscribe('c1', PRIVATE), 'left')
    assertEquals(await m.unsubscribe('c1', PRIVATE), 'not-subscribed')
    assertEquals(await m.unsubscribe('elsewhere', PRIVATE), 'not-owned')
})

Deno.test('#332 disconnect reports whether it owned the socket', async () => {
    const { driver, roster, commands } = recordingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, ROOM)

    const before = commands.length
    assertEquals(
        await m.disconnect('nobody-here'),
        'not-owned',
        'it iterated an empty channel set and deleted two absent map entries ' +
            '— the same silent no-op unsubscribe had, in the verb whose name ' +
            'promises more',
    )
    assertEquals(commands.slice(before), [], 'and touched nothing')
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])].sort(),
        ['1', '2'],
        'both members still in the room',
    )

    assertEquals(await m.disconnect('c1'), 'disconnected')
    assertEquals([...(roster.get(ROOM)?.keys() ?? [])], ['2'])

    assertEquals(
        await m.disconnect('c1'),
        'not-owned',
        'a second disconnect of the same id is not owned any more — the ' +
            'connection was forgotten',
    )
})

Deno.test('#332 a connection with no channels still disconnects as OWNED', async () => {
    // `#channelsByClient` is empty for a registered connection that never
    // subscribed, so a disconnect that derived ownership from it — rather than
    // from `connections` — would call that a wrong process.
    const { driver } = recordingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    m.register(conn('c1', 1))

    assertEquals(await m.disconnect('c1'), 'disconnected')
})
