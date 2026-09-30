/**
 * @fileoverview #334 — an emptied presence channel map is deleted, not kept.
 *
 * `ChannelManager` never removed an entry from `this.presence`. Every unique
 * presence name an instance had ever hosted retained an empty inner `Map` for
 * the life of the process, so a `subscribe` / `unsubscribe` cycle over fresh
 * names was **not cost-neutral at rest** — which is the premise #329's cost
 * accounting rests on when it calls the net set delta zero.
 *
 * ## Why the witness here is structural, and why that is the point
 *
 * The sibling map got this right: `#leaveLocal` deletes its empty `Set`, and the
 * suite proves it **behaviourally** — `#checkChannelCaps` reads
 * `subscriptions.size`, so a retained empty set overshoots the channel cap and
 * refuses a subscribe on an instance nowhere near its limit. There is a visible
 * consequence, so there is a visible test.
 *
 * `presence` has no such reader. No cap counts it, and the one path that does
 * scan it — `#syncRosterMember`, deriving its desired state inside the serial
 * tail — computes the **same absent state** from an absent entry and an empty
 * one. That is exactly what made the fix safe, and exactly what let the defect
 * live: *there is no behaviour to assert on*. A test that could only observe
 * consequences would have to report this map as correct.
 *
 * So the entry is read directly. The cast is the honest shape of the claim, not
 * a shortcut around a better one.
 *
 * ## The one test here that guards the FIX rather than the defect
 *
 * `'the authoritative roster still receives the removal'`. `unsubscribe` deletes
 * the local entry **before** awaiting `#syncRosterMember`, which then reads
 * `presence.get(channel)` to decide what to write. The claim that absent and
 * empty compute alike is load-bearing: were it wrong, deleting the entry would
 * make the projection skip the removal and strand the member in the
 * cluster-wide roster permanently — a far worse defect than the leak being
 * fixed, and one no memory-shaped test would notice.
 *
 * @module @lockness/realtime/tests/presence_eviction_334
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
 * The manager's local presence map, read directly.
 *
 * `private` in TypeScript is erased at runtime, so this reaches the real field.
 * See the file header for why no observable path can answer this question.
 */
type PresenceView = { presence: Map<string, Map<string, PresenceMember>> }
const presenceOf = (m: ChannelManager<User>): PresenceView['presence'] =>
    (m as unknown as PresenceView).presence

/** A driver that records roster commands and can be made to fail its writes. */
function rosterDriver(options: { failAdd?: boolean } = {}) {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const commands: string[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            commands.push(`holdMember ${channel} ${member.id}`)
            if (options.failAdd) {
                return Promise.reject(new Error('roster refused the write'))
            }
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
        publishControl: () => Promise.resolve(),
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    return { driver, roster, commands }
}

Deno.test('#334 the channel entry is GONE once the last member leaves', async () => {
    const { driver } = rosterDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const rosterReadsBefore = rosterReadCount()
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    assertRosterRead(rosterReadsBefore)

    assert(presenceOf(m).has(ROOM), 'the entry exists while the member is in')

    assertEquals(await m.unsubscribe('c1', ROOM), 'left')

    assertEquals(
        presenceOf(m).has(ROOM),
        false,
        'the ENTRY is gone, not merely emptied — an empty inner map left ' +
            'behind is the whole defect, and it would satisfy any assertion ' +
            'written against the inner map instead',
    )
})

Deno.test('#334 the entry SURVIVES while any member remains', async () => {
    // The over-application guard. A delete that ran on every leave rather than
    // on the 1→0 transition would drop a map still holding somebody, and the
    // roster projection would then compute "absent" for a member who is
    // present — issuing a removal that evicts them from the cluster roster.
    const { driver } = rosterDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, ROOM)

    assertEquals(await m.unsubscribe('c1', ROOM), 'left')

    assert(presenceOf(m).has(ROOM), 'still hosting a member')
    assertEquals(
        [...(presenceOf(m).get(ROOM)?.keys() ?? [])],
        ['c2'],
        'and it holds exactly the member who did not leave',
    )

    assertEquals(await m.unsubscribe('c2', ROOM), 'left')
    assertEquals(presenceOf(m).has(ROOM), false, 'now the last one is out')
})

Deno.test('#334 the authoritative roster still receives the removal', async () => {
    // THE TEST THAT GUARDS THE FIX, not the defect. `unsubscribe` deletes the
    // local entry BEFORE awaiting `#syncRosterMember`, which derives what to
    // write by scanning `presence.get(channel)`. If an absent entry did not
    // compute the same absent state an empty one does, this fix would strand
    // the member in the cluster-wide roster for good.
    const { driver, roster, commands } = rosterDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, ROOM)
    assertEquals([...(roster.get(ROOM)?.keys() ?? [])], ['1'])

    await m.unsubscribe('c1', ROOM)

    assertEquals(
        presenceOf(m).has(ROOM),
        false,
        'the local entry went first — that is the ordering under test',
    )
    assert(
        commands.includes(`releaseMember ${ROOM} 1`),
        `the projection must still issue the removal; got ${
            JSON.stringify(commands)
        }`,
    )
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])],
        [],
        'and the member is out of the authoritative roster, not stranded in it',
    )
})

Deno.test('#334 churn over unique names is cost-neutral at rest', async () => {
    // #329's premise, stated as a test. Before the fix this map held 25
    // entries at the end of this loop and never gave one back.
    const { driver } = rosterDriver()
    const m = new ChannelManager<User>({ driver, authorize })

    const c1 = conn('c1', 1)
    m.register(c1)
    for (let i = 0; i < 25; i++) {
        const channel = `presence-churn-${i}`
        await m.subscribe(c1, channel)
        await m.unsubscribe('c1', channel)
    }

    assertEquals(
        presenceOf(m).size,
        0,
        'every name this instance hosted gave its entry back',
    )
})

Deno.test('#334 a rolled-back join does not strand the entry it created', async () => {
    // The SECOND deletion site, and the one no other test in this package
    // reaches. `#joinPresence` creates the entry, claims the membership, then
    // compensates when the authoritative write is refused — and that
    // compensation created the entry it is undoing, so it owes the same 1→0
    // delete. Routing both sites through one helper is what makes that
    // automatic rather than remembered.
    const { driver } = rosterDriver({ failAdd: true })
    const m = new ChannelManager<User>({ driver, authorize })

    let threw = false
    try {
        const c1 = conn('c1', 1)
        m.register(c1)
        await m.subscribe(c1, ROOM)
    } catch {
        threw = true
    }
    assert(threw, 'the refused roster write propagates — precondition')

    assertEquals(
        presenceOf(m).has(ROOM),
        false,
        'the compensation undoes exactly what the join did, entry included',
    )
})
