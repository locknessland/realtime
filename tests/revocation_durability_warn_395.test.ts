/**
 * @fileoverview #395 part 2 — `evict` and `revokeChannel`'s own durability
 * WARNs never cancel the revocation they report on.
 *
 * Both methods write the same durable record first (S1/FR-014) and their
 * documented contract is the same: a failure to persist it must never cancel
 * the revocation that is still possible without it — `evict` still hard-closes
 * or reaches the owning instance, `revokeChannel` still applies locally or
 * publishes the control frame. Before this fix, the `console.warn` that reports
 * the durability failure sat in a bare `catch`, with no guard of its own: a
 * throwing sink (a patched console, a logger transport that refuses the line)
 * turned that catch's own body into a new throw, which propagated straight out
 * of the method — skipping `revokeLocal` / `publishControl` entirely, so the
 * revocation was applied nowhere, local or remote. The #391 `writeMarkedFallback`
 * pattern closes it: the WARN is guarded, and a sink that refuses it writes one
 * marked ERROR line instead, which never throws past itself.
 *
 * Each row proves it two ways: the revocation still ran (`revokeLocal`'s own
 * hard-close for `evict`, `publishControl` for `revokeChannel`'s remote path),
 * and nothing escaped as an unhandled rejection — every log channel is made to
 * throw with {@link everyChannelThrows}, watched by {@link watchingEscapes}.
 *
 * Red before the fix: both rows failed on "the revocation still ran" — the
 * throwing sink aborted the method before `revokeLocal` / `publishControl` was
 * reached.
 *
 * @module @lockness/realtime/tests/revocation_durability_warn_395
 */

import { assert, assertEquals } from '@std/assert'
import {
    ChannelManager,
    EVICT_DURABILITY_LOG_FAILED,
    REVOKE_CHANNEL_DURABILITY_LOG_FAILED,
} from '../manager.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: number
}

const ROOM = 'private-orders'

function conn(id: string): Connection<User> & { closes: number[] } {
    const closes: number[] = []
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: (code: number) => void closes.push(code),
        closes,
    }
}

Deno.test('#395 part 2 evict: revokeLocal still runs when the durability WARN itself throws', async () => {
    await watchingEscapes(async (escaped) => {
        const failure = new Error('broker unreachable (#395 part 2)')
        const driver: BroadcastDriver = {
            publish: () => {},
            onMessage: () => {},
            markRevocation: () => Promise.reject(failure),
            listRevocations: () => Promise.resolve([]),
            clearRevocation: () => Promise.resolve(),
        }
        const m = new ChannelManager<User>({ driver })
        const victim = conn('c1')
        m.register(victim)

        let thrown: unknown = undefined
        let lines: readonly string[] = []
        {
            using channels = everyChannelThrows()
            try {
                await m.evict('c1')
            } catch (error) {
                thrown = error
            }
            await settle()
            lines = [...channels.errorLines()]
        }

        assertEquals(
            thrown,
            failure,
            "evict still rejects with the durability write's own error, not " +
                "the throwing sink's",
        )
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            victim.closes,
            [4403],
            'revokeLocal still hard-closed the socket even though the ' +
                'durability WARN itself threw',
        )
        const marked = lines.filter((line) =>
            line.startsWith(`${EVICT_DURABILITY_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
        assert(
            marked.some((line) => line.includes('broker unreachable')),
            `the marked line carries the original durability failure: ${
                JSON.stringify(marked)
            }`,
        )
        assert(
            marked.some((line) =>
                line.split('; sink failure')[0] !== line &&
                line.includes('sink failure')
            ),
            `the marked line carries the sink's own failure too: ${
                JSON.stringify(marked)
            }`,
        )
    })
})

Deno.test('#395 part 2 revokeChannel: publishControl still runs when the durability WARN itself throws', async () => {
    await watchingEscapes(async (escaped) => {
        const failure = new Error('broker unreachable (#395 part 2)')
        const published: ControlMessage[] = []
        const driver: BroadcastDriver = {
            publish: () => {},
            onMessage: () => {},
            onControl: () => {},
            publishControl: (control) => {
                published.push(control)
                return Promise.resolve()
            },
            markRevocation: () => Promise.reject(failure),
            listRevocations: () => Promise.resolve([]),
            clearRevocation: () => Promise.resolve(),
        }
        const m = new ChannelManager<User>({ driver })
        // Deliberately NOT registered on this instance: the socket lives on
        // another one, so `revokeChannel` must reach the owner over the
        // control plane rather than apply the revocation locally.

        let thrown: unknown = undefined
        let lines: readonly string[] = []
        {
            using channels = everyChannelThrows()
            try {
                await m.revokeChannel('c1', ROOM)
            } catch (error) {
                thrown = error
            }
            await settle()
            lines = [...channels.errorLines()]
        }

        assertEquals(
            thrown,
            failure,
            "revokeChannel still rejects with the durability write's own " +
                "error, not the throwing sink's",
        )
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            published.length,
            1,
            'publishControl still ran even though the durability WARN ' +
                'itself threw',
        )
        assertEquals(published[0]?.kind, 'revoke-channel')
        assertEquals(published[0]?.channel, ROOM)
        const marked = lines.filter((line) =>
            line.startsWith(`${REVOKE_CHANNEL_DURABILITY_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
        assert(
            marked.some((line) => line.includes('broker unreachable')),
            `the marked line carries the original durability failure: ${
                JSON.stringify(marked)
            }`,
        )
    })
})
