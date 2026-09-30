/**
 * @fileoverview #323/SC-007 — the channel cap is exact under concurrency.
 *
 * `#checkChannelCaps` reads `subscriptions.size` and `#joinLocal` spends it.
 * They agree only because **no `await` separates them**: an async function body
 * runs synchronously to its first internal await, and `#joinLocal`'s is after
 * both of its adds. Check and increment are therefore one event-loop turn and
 * cannot interleave.
 *
 * That pairing is why #323 moved the presence ANNOUNCEMENT behind the
 * authoritative roster write and left the WRITE itself below `#joinLocal`. The
 * obvious version of this fix — hoist `holdMember` to the front, so nothing is
 * visible before the roster accepts — puts a broker round-trip inside that
 * turn. `onMessage` is dispatched as `void guard(...)`, so a client's subscribe
 * frames are not serialized: it pipelines K of them into one write, all K read
 * the same count, and all K add.
 *
 * The cap this overshoots is not a comfort knob. `ChannelManager`'s own header
 * records that exceeding it voids the reconnect re-issue proof AND the bound on
 * the post-outage revocation window — so an overshoot widens the period an
 * evicted client stays reachable.
 *
 * **The double resolves on a later microtask turn — as robustness, not as the
 * mechanism.** This header used to claim an immediately-resolving stub would
 * make the file pass vacuously. **That is false, and it was measured:** with a
 * one-turn stub the witness still reports 5 admitted against 1 slot, because
 * ANY `await` yields and all five racers suspend before the check either way.
 * The deferral is kept because it survives an implementation that happens to
 * await something already resolved, and because a reader should not have to
 * work out how many turns are enough. It is not what makes the test valid.
 *
 * **THE HAZARD IS THE AWAIT'S POSITION, NOT THE WRITE'S — measured, and it is
 * not what you would guess.** Hoisting `roster.holdMember` ABOVE
 * `#checkChannelCaps` does NOT break the cap: every racer then suspends
 * *before* the read, and each resumes to run check-then-add with nothing
 * awaited between them, so the pairing survives. This file was written against
 * that mutant first and it SURVIVED. Only an await landing BETWEEN the check
 * and `#joinLocal`'s adds breaks it — 5 joins admitted against 1 free slot,
 * measured. `mutations/presence_join_323.ts` carries the row that dies to it.
 *
 * The distinction is invisible in a diff, so it is written here rather than
 * left as a mutation row that could not be expressed without duplicating the
 * write and dying to the wrong witness.
 *
 * @module @lockness/realtime/tests/presence_cap_concurrency_323
 */

import { assertEquals } from '@std/assert'
import { ChannelLimitError, ChannelManager } from '../manager.ts'
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

/** A roster whose every op settles a few microtask turns out, never inline. */
function deferredRoster(): BroadcastDriver {
    const roster = new Map<string, Map<string, PresenceMember>>()
    // Three hops, not one: a single `await` on an already-resolved promise is
    // enough to yield, but three makes the interleaving unmistakable to a
    // reader and robust to an implementation that happens to await once.
    const later = async (): Promise<void> => {
        await Promise.resolve()
        await Promise.resolve()
        await Promise.resolve()
    }
    return {
        publish: () => {},
        onMessage: () => {},
        async holdMember(channel, member) {
            await later()
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return { arrived }
        },
        async releaseMember(channel, memberId) {
            await later()
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (async () => {
                    await later()
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
    }
}

const conn = (id: string, userId: number): Connection<User> => ({
    id,
    identity: { id: userId },
    metadata: {},
    send: () => {},
    close: () => {},
})

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

Deno.test('#323/SC-007 K concurrent joins against ONE free slot admit exactly one', async () => {
    const m = new ChannelManager<User>({
        driver: deferredRoster(),
        authorize,
        maxWatchedChannels: 3,
        maxChannelsPerConnection: 3,
        // Identified connections reach the whole cap; the anonymous reserve
        // would otherwise decide this test instead of the cap under study.
        anonymousHostingShare: 1,
    })

    // Fill two of three slots, sequentially — no contention yet.
    const resident = conn('resident', 1)
    m.register(resident)
    await m.subscribe(resident, 'presence-a')
    await m.subscribe(resident, 'presence-b')

    // Now pipeline five joins to five DISTINCT new channels, exactly as an
    // unserialized onMessage delivers them. One slot is left.
    const contenders = ['c', 'd', 'e', 'f', 'g'].map((suffix, i) => {
        const connection = conn(`client-${i}`, 10 + i)
        m.register(connection)
        return { channel: `presence-${suffix}`, connection }
    })
    const settled = await Promise.allSettled(
        contenders.map(({ connection, channel }) =>
            m.subscribe(connection, channel)
        ),
    )

    const admitted = settled.filter((r) => r.status === 'fulfilled')
    const refused = settled.filter((r) => r.status === 'rejected')

    assertEquals(
        admitted.length,
        1,
        `exactly one join may take the last slot; ${admitted.length} did`,
    )
    assertEquals(refused.length, 4)
    for (const r of refused) {
        assertEquals(
            (r as PromiseRejectedResult).reason instanceof ChannelLimitError,
            true,
            'and the four refusals are cap refusals, not incidental errors',
        )
    }
})

Deno.test('#323/SC-007 the cap still admits up to its limit when uncontended', async () => {
    // The negative half: a cap that refuses everything would pass the test
    // above and be useless. Three slots, three sequential joins, all admitted.
    const m = new ChannelManager<User>({
        driver: deferredRoster(),
        authorize,
        maxWatchedChannels: 3,
        maxChannelsPerConnection: 3,
        anonymousHostingShare: 1,
    })
    const resident = conn('resident', 1)
    m.register(resident)
    const rosterReadsBefore = rosterReadCount()
    for (const channel of ['presence-a', 'presence-b', 'presence-c']) {
        assertEquals((await m.subscribe(resident, channel)).ok, true)
    }
    assertRosterRead(rosterReadsBefore)
})
