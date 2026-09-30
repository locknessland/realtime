/**
 * @fileoverview #408 — the roster slot key round-trips exactly, through the
 * one encode/decode pair `#rosterSlotKey` / `#rosterSlotChannel`, for a
 * channel and a member id drawn from the edges of what each is allowed to
 * contain.
 *
 * `manager.ts` built and parsed the `<channel>\0<memberId>` key in three
 * places that agreed only by convention (`#recordOwedRelease`,
 * `#syncRosterMember`'s tail, `#drainOwedReleases`'s decode). Nothing here
 * pins the format itself — that stays a NUL separator, unchanged — only that
 * every site now shares one definition, and that the definition survives the
 * one case the format's own comment calls out: a member id is TYPE-checked
 * only (`isPresenceMemberIdValue`, #346), never charset-checked, so it may
 * itself embed a NUL. A channel cannot (`isValidName`'s
 * `[A-Za-z0-9:._-]+`), which is exactly why decoding at the FIRST NUL is
 * always the separator, never a character the member id chose.
 *
 * The witness drives this the same way `owed_release_371.test.ts` does: a
 * driver double whose `releaseMember` can be told to reject once, so a slot
 * is queued in `#owedReleases` under the composite key and then drained
 * through `#drainOwedReleases`'s decode. A wrong decode would either retry
 * the wrong channel or fail to find the slot at all — either way the roster
 * would still hold the member after the drain.
 *
 * @module @lockness/realtime/tests/roster_slot_key_408
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'

interface User {
    id: string
}

/** A channel name exercising every punctuation `isValidName` allows. */
const CHANNEL = 'presence-room:1.sub_2-x'

/**
 * A member id embedding a NUL byte itself, plus the same punctuation a
 * channel is allowed and a member id is not restricted to — the case the
 * `#drainOwedReleases` decode comment names: the field after the separator is
 * never re-parsed, so whatever it contains cannot corrupt the channel that
 * comes before it.
 */
const MEMBER_ID = 'm\u0000id:with.punct-and_underscore\u0000tail'

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

function conn(id: string, userId: string): Recording {
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

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

/**
 * A presence-capable driver double whose `releaseMember` can be told to
 * reject the next N calls for one `(channel, id)` slot, and which captures
 * the handler `ChannelManager` registers on `onRosterMaintenance` — the same
 * shape `owed_release_371.test.ts` uses.
 */
function presenceDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const releaseFaults = new Map<string, number>()
    let maintenance: (() => void | Promise<void>) | undefined
    const slot = (channel: string, id: string) => `${channel}\0${id}`
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const field = String(member.id)
            const arrived = !members.has(field)
            members.set(field, member)
            return { arrived }
        },
        async releaseMember(channel, memberId) {
            const field = String(memberId)
            const key = slot(channel, field)
            await Promise.resolve()
            const remaining = releaseFaults.get(key) ?? 0
            if (remaining > 0) {
                releaseFaults.set(key, remaining - 1)
                throw new Error('ROSTER_RELEASE_REFUSED')
            }
            const gone = roster.get(channel)?.delete(field) ?? false
            return { gone }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                [...(roster.get(channel)?.values() ?? [])],
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl: () => Promise.resolve(),
        watchChannel: () => {},
        unwatchChannel: () => {},
        onRosterMaintenance(handler) {
            maintenance = handler
        },
    }
    return {
        driver,
        roster,
        /** Make the next `times` releases of this slot reject. */
        failReleaseTimes(channel: string, id: string, times = 1) {
            releaseFaults.set(
                slot(channel, id),
                (releaseFaults.get(slot(channel, id)) ?? 0) + times,
            )
        },
        /** Invoke the captured `onRosterMaintenance` handler once. */
        async drain(): Promise<void> {
            await maintenance?.()
        },
    }
}

Deno.test(
    '#408 W1 the roster slot key round-trips a channel using every ' +
        '`isValidName` punctuation and a member id embedding a NUL of its ' +
        'own — the drain still recovers the exact channel and completes ' +
        'the retry',
    async () => {
        await watchingEscapes(async (escaped) => {
            const driver = presenceDriver()
            const m = new ChannelManager<User>({
                driver: driver.driver,
                authorize,
            })
            const observer = conn('observer', 'obs')
            m.register(observer)
            await m.subscribe(observer, CHANNEL)
            const holder = conn('holder', MEMBER_ID)
            m.register(holder)
            await m.subscribe(holder, CHANNEL)

            driver.failReleaseTimes(CHANNEL, MEMBER_ID, 1)
            let rejection: unknown
            try {
                await m.unsubscribe(holder.id, CHANNEL)
            } catch (error) {
                rejection = error
            }
            await settle()
            assert(
                rejection instanceof Error,
                'unsubscribe rejects — the release never committed',
            )
            assertEquals(
                driver.roster.get(CHANNEL)?.has(MEMBER_ID),
                true,
                'the roster still holds the member: the release was queued, ' +
                    'not retried yet',
            )

            await driver.drain()

            assertEquals(
                driver.roster.get(CHANNEL)?.has(MEMBER_ID),
                false,
                'the drain decoded the exact channel back out of the ' +
                    "composite key — through the member id's own embedded " +
                    'NUL — and completed the release',
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)
