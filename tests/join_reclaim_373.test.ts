/**
 * @fileoverview #373 — the #323 join compensation's roster reclaim must run
 * whether or not `#leaveLocal` rejects, and the subscribe must still reject
 * with the ORIGINAL roster error.
 *
 * `#joinPresence`'s compensation used to `await this.#leaveLocal(...)`
 * sequentially, ahead of the reclaim. When the leave rejected — the 1→0
 * transition awaits `unwatchChannel`, which can reject — that `await` threw
 * immediately, so the reclaim (`#syncRosterMember` a second time, to remove a
 * roster write that committed behind a lost reply) never ran, and the
 * compensation surfaced the unwatch failure in place of the roster failure
 * that actually refused the join.
 *
 * The two faults are correlated, not independent: a broker connection drop is
 * exactly what loses the roster reply and fails the unwatch write in the same
 * moment (#373's own framing) — so this witness models both on one driver, on
 * a first join to a channel with no other local member (the only case a leave
 * reaches `unwatchChannel` at all).
 *
 * @module @lockness/realtime/tests/join_reclaim_373
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

/**
 * A driver whose roster write for the join COMMITS and then rejects (a lost
 * reply), and whose `unwatchChannel` also rejects — the correlated failure
 * #373 describes.
 */
function faultyJoinDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            // The write commits before the reply is lost.
            members.set(String(member.id), member)
            return Promise.reject(new Error('ROSTER_REPLY_LOST'))
        },
        releaseMember(channel, memberId) {
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
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
        unwatchChannel: () =>
            Promise.reject(new Error('BROKER_UNWATCH_FAILED')),
    }
    return { driver, roster }
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

function conn(id: string, userId: number): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    }
}

/** Capture every `console.warn` call, restoring it on scope exit. */
function warnCalls(): { readonly lines: string[] } & Disposable {
    const real = console.warn
    const lines: string[] = []
    console.warn = (...args: unknown[]) =>
        void lines.push(args.map(String).join(' '))
    return {
        lines,
        [Symbol.dispose]: () => {
            console.warn = real
        },
    }
}

Deno.test(
    '#373 the reclaim runs and the ORIGINAL roster error wins, even when ' +
        'the leave also fails',
    async () => {
        await watchingEscapes(async (escaped) => {
            const { driver, roster } = faultyJoinDriver()
            const m = new ChannelManager<User>({ driver, authorize })
            const newcomer = conn('c1', 1)
            m.register(newcomer)
            using warn = warnCalls()

            let rejection: unknown
            try {
                await m.subscribe(newcomer, CHANNEL)
            } catch (error) {
                rejection = error
            }
            await settle()

            assert(rejection instanceof Error, 'the subscribe must reject')
            assertEquals(
                (rejection as Error).message,
                'ROSTER_REPLY_LOST',
                "the subscribe's rejection is the ORIGINAL roster error, " +
                    'not the unwatch failure',
            )
            assertEquals(
                [...(roster.get(CHANNEL)?.keys() ?? [])],
                [],
                'the reclaim ran despite the leave rejecting: no committed-' +
                    'but-lost hold remains',
            )
            const unwatchWarnings = warn.lines.filter((line) =>
                line.includes('BROKER_UNWATCH_FAILED')
            )
            assertEquals(
                unwatchWarnings.length,
                1,
                `exactly one WARN names the unwatch failure: ${warn.lines}`,
            )
            assertEquals(escaped, [], 'no rejection escapes to the runtime')
        })
    },
)
