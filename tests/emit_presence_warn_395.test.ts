/**
 * @fileoverview #395 (security review, LOW) — `emitPresence`'s per-connection
 * delivery WARN never cancels fan-out to the remaining local sockets.
 *
 * `emitPresence` fans a presence frame to every local subscriber of a channel;
 * a socket that cannot receive it (a closing connection) is meant to be
 * skipped, "and the fan-out continues" per its own WARN text. Before this fix
 * that WARN was a bare `console.warn`: a throwing sink turned the catch's own
 * body into a new throw, aborting the `for` loop before any LATER socket in
 * the same fan-out was ever tried.
 *
 * Red before the fix: with a deaf socket first in the fan-out order, the
 * socket after it never received the frame — the sink's own throw escaped the
 * loop in its place.
 *
 * @module @lockness/realtime/tests/emit_presence_warn_395
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, EMIT_PRESENCE_LOG_FAILED } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: string
}

const ROOM = 'presence-room'

function conn(
    id: string,
    userId: string,
    sendThrows = false,
): Connection<User> & { frames: Record<string, unknown>[] } {
    const frames: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data: string) => {
            if (sendThrows) throw new Error('injected: socket closing (#395)')
            frames.push(JSON.parse(data))
        },
        close: () => {},
        frames,
    }
}

Deno.test("#395 (LOW) emitPresence: a later local socket still receives the frame when an earlier socket's WARN itself throws", async () => {
    await watchingEscapes(async (escaped) => {
        const driver: BroadcastDriver = {
            publish: () => {},
            onMessage: () => {},
            holdMember: () => Promise.resolve({ arrived: true }),
            releaseMember: () => Promise.resolve({ gone: true }),
            readRoster: (_channel, limit, selfIds) =>
                asWindow(Promise.resolve([]), limit, selfIds),
        }
        const manager = new ChannelManager<User>({
            driver,
            authorize: (identity: User | null): PresenceMember | false =>
                identity ? { id: identity.id, info: {} } : false,
        })
        // deaf joins first (its `send` throws on the newcomer's frame),
        // listener joins second (must still receive it), both BEFORE the
        // newcomer whose join triggers the fan-out.
        const deaf = conn('deaf', 'deaf-member', true)
        const listener = conn('listener', 'listener-member')
        manager.register(deaf)
        manager.register(listener)
        assertEquals((await manager.subscribe(deaf, ROOM)).ok, true)
        assertEquals((await manager.subscribe(listener, ROOM)).ok, true)

        const newcomer = conn('newcomer', 'newcomer-member')
        manager.register(newcomer)

        let lines: readonly string[] = []
        {
            using channels = everyChannelThrows()
            assertEquals((await manager.subscribe(newcomer, ROOM)).ok, true)
            await settle()
            lines = [...channels.errorLines()]
        }

        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            listener.frames.some((f) =>
                f.type === 'presence' && f.action === 'joined'
            ),
            true,
            "listener still received the newcomer's joined frame, even " +
                "though deaf's (earlier in the fan-out) own WARN threw",
        )
        const marked = lines.filter((line) =>
            line.startsWith(`${EMIT_PRESENCE_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
    })
})
