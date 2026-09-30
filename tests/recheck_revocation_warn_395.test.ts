/**
 * @fileoverview #395 (security review, HIGH) — `#recheckRevocations`'s own
 * wrapper WARN never cancels the rest of a reconcile pass.
 *
 * `#recheckRevocations`'s `apply` closure already contains everything
 * `#applyRevocation` throws — but not the failure of its OWN catch, the
 * `console.warn` it writes there. A sink that throws (the #391 class) used to
 * escape that closure, abort the `for` loop in `#recheckRevocations`, and skip
 * every revocation still queued behind the failing one in that pass — breaking
 * #349's "one revocation that throws never stops the ones after it".
 *
 * `revocation_tally_384.test.ts`'s T4 proves the SINGLE-failure case already
 * works: only `#applyRevocation`'s own WARN is refused, the wrapper's WARN
 * succeeds, and the next revocation is applied. This file proves the
 * DOUBLE-failure case T4 does not cover: the wrapper's own WARN ALSO refused —
 * every log channel throws, via {@link everyChannelThrows}.
 *
 * Red before the fix: the second revocation was never applied (`c2.closes`
 * stayed empty) — the sink's own throw escaped `#recheckRevocations`'s loop in
 * place of the second apply.
 *
 * @module @lockness/realtime/tests/recheck_revocation_warn_395
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, RECHECK_REVOCATION_LOG_FAILED } from '../manager.ts'
import type { BroadcastDriver, Revocation } from '../driver.ts'
import type { Connection } from '../types.ts'
import {
    everyChannelThrows,
    settle,
    watchingEscapes,
} from './escape_watcher.ts'

interface User {
    id: number
}

/** How the durable index keys a record: by its exact id when it has one. */
const key = (r: Revocation) => r.channel === undefined ? r.target : r.id

function conn(
    id: string,
    userId: number,
    closeThrows = false,
): Connection<User> & { closes: number[] } {
    const closes: number[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: (code?: number) => {
            closes.push(code ?? 1000)
            if (closeThrows) {
                throw new Error(
                    'injected: the socket refused to close (#395)',
                )
            }
        },
        closes,
    }
}

Deno.test('#395 (HIGH) #recheckRevocations: the second revocation is still applied when the wrapper WARN itself throws', async () => {
    await watchingEscapes(async (escaped) => {
        const index = new Map<string, Revocation>()
        let reconciler: (() => unknown) | undefined
        const driver: BroadcastDriver = {
            publish: () => {},
            onMessage: () => {},
            onRevocationReconcile(handler) {
                reconciler = handler
            },
            markRevocation: (r) => {
                index.set(key(r), r)
                return Promise.resolve()
            },
            listRevocations: () => Promise.resolve([...index.values()]),
            clearRevocation: (r) => {
                index.delete(key(r))
                return Promise.resolve()
            },
        }
        const manager = new ChannelManager<User>({ driver })
        // c1's own close() throws: `#applyRevocation`'s catch runs, and its
        // OWN WARN throws too under everyChannelThrows — the double failure
        // that reaches `#recheckRevocations`' wrapper catch.
        const c1 = conn('c1', 1, true)
        const c2 = conn('c2', 2)
        manager.register(c1)
        manager.register(c2)
        // Inserted in order: c1 is attempted, and must fail, BEFORE c2.
        index.set(key({ target: 'c1' }), { target: 'c1' })
        index.set(key({ target: 'c2' }), { target: 'c2' })

        assert(reconciler, 'precondition: the manager registered a re-check')

        let thrown: unknown = undefined
        let lines: readonly string[] = []
        {
            using channels = everyChannelThrows()
            try {
                await reconciler!()
            } catch (error) {
                thrown = error
            }
            await settle()
            lines = [...channels.errorLines()]
        }

        assertEquals(thrown, undefined, 're-check itself never throws')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            c1.closes,
            [4403],
            "c1's own hard-close still ran (that is what threw)",
        )
        assertEquals(
            c2.closes,
            [4403],
            'c2 is STILL applied, even though c1 (attempted before it) ' +
                'failed twice over',
        )
        const marked = lines.filter((line) =>
            line.startsWith(`${RECHECK_REVOCATION_LOG_FAILED} `)
        )
        assert(
            marked.length >= 1,
            `the site's own marked line was attempted: ${
                JSON.stringify(lines)
            }`,
        )
    })
})
