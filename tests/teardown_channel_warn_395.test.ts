/**
 * @fileoverview #395 (security review, LOW) — `#teardownChannels`'s "also
 * failed" WARN never cancels the rest of a disconnect's channel loop.
 *
 * `#teardownChannels` walks a connection's channels one at a time; the FIRST
 * one that fails to unsubscribe is recorded and re-thrown after the loop (so
 * `disconnect`'s caller still learns about it), and every SUBSEQUENT failure
 * is logged instead — `disconnect`'s own contract says giving up mid-loop
 * would leave every LATER channel's roster release, `left` announcement and
 * per-connection cap release skipped. Before this fix, that log line was a
 * bare `console.warn`: a throwing sink turned the catch's own body into a new
 * throw, which propagated straight out of the loop — so the very channel the
 * comment says must not be skipped, was.
 *
 * Red before the fix: with two failing channels ahead of it, the third
 * (succeeding) channel's `unwatchChannel` was never called — the sink's own
 * throw escaped the loop in its place.
 *
 * @module @lockness/realtime/tests/teardown_channel_warn_395
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, TEARDOWN_CHANNEL_LOG_FAILED } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection } from '../types.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: number
}

function conn(id: string): Connection<User> {
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: () => {},
    }
}

Deno.test("#395 (LOW) #teardownChannels: a later channel is still released when a middle failure's WARN itself throws", async () => {
    await watchingEscapes(async (escaped) => {
        const unwatched: string[] = []
        const driver: BroadcastDriver = {
            publish: () => {},
            onMessage: () => {},
            watchChannel: () => {},
            unwatchChannel: (channel: string) => {
                unwatched.push(channel)
                if (channel === 'public-a' || channel === 'public-b') {
                    return Promise.reject(
                        new Error(
                            `injected: unwatch ${channel} refused (#395)`,
                        ),
                    )
                }
                return Promise.resolve()
            },
        }
        const manager = new ChannelManager<User>({ driver })
        const c1 = conn('c1')
        manager.register(c1)
        // Subscribed in order: A and B both fail their unwatch; C succeeds.
        // `#channelsByClient` is a `Set`, so the teardown loop below walks
        // them in this same insertion order.
        assertEquals((await manager.subscribe(c1, 'public-a')).ok, true)
        assertEquals((await manager.subscribe(c1, 'public-b')).ok, true)
        assertEquals((await manager.subscribe(c1, 'public-c')).ok, true)

        let thrown: unknown = undefined
        let lines: readonly string[] = []
        {
            using channels = everyChannelThrows()
            try {
                await manager.disconnect(c1)
            } catch (error) {
                thrown = error
            }
            await settle()
            lines = [...channels.errorLines()]
        }

        // A's failure is the first: it is re-thrown by `disconnect`'s
        // contract, unchanged by this fix.
        assert(
            thrown instanceof Error &&
                thrown.message.includes('unwatch public-a refused'),
            `disconnect still rejects with A's own failure: ${String(thrown)}`,
        )
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            unwatched,
            ['public-a', 'public-b', 'public-c'],
            "C's unwatch still ran even though B's own WARN threw " +
                '(A first, its failure recorded; B second, its WARN thrown ' +
                'and guarded; C last, still reached)',
        )
        const marked = lines.filter((line) =>
            line.startsWith(`${TEARDOWN_CHANNEL_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
    })
})
