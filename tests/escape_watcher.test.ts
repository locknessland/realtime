/**
 * @fileoverview #374 — the contract of the one shared `unhandledrejection`
 * watcher every containment witness in this package runs under.
 *
 * Six test files carried their own copy, and two invariants drifted between
 * them: whether the listener outlives the turn in which a failing assertion
 * throws, and whether an escape that lands after the body returned is still
 * reported. These rows pin both on the shared helper, so no caller has to
 * re-derive them.
 *
 * Red before #374: the second row takes the whole file down as an
 * `(uncaught error)`, and the third would too, because the listener was
 * removed in the same turn as the body's last statement.
 *
 * @module @lockness/realtime/tests/escape_watcher_374
 */

import {
    assertEquals,
    AssertionError,
    assertRejects,
    assertStrictEquals,
} from '@std/assert'
import { settle, watchingEscapes } from './escape_watcher.ts'

Deno.test('#374 an escape during the body is recorded and prevented', async () => {
    const reason = new Error('escaped during the body')
    await watchingEscapes(async (escaped) => {
        void Promise.reject(reason)
        await settle()
        assertEquals(escaped.length, 1)
        assertStrictEquals(escaped[0], reason)
    })
})

Deno.test('#374 a rejection still pending when the body throws does not kill the runner, and the body error is the one reported', async () => {
    const error = await assertRejects(() =>
        watchingEscapes(() => {
            void Promise.reject(new Error('pending when the body threw'))
            return Promise.reject(new Error('the failing assertion'))
        })
    )
    assertEquals((error as Error).message, 'the failing assertion')
})

Deno.test('#374 an escape the body never waited for fails the watcher by name', async () => {
    await assertRejects(
        () =>
            watchingEscapes(() => {
                void Promise.reject(new Error('never waited for'))
                return Promise.resolve()
            }),
        AssertionError,
        'never waited for',
    )
})

Deno.test('#374 the listener is removed once the watcher returns', async () => {
    let seen: unknown[] = []
    await watchingEscapes((escaped) => {
        seen = escaped
        return Promise.resolve()
    })
    const after: unknown[] = []
    const own = (event: PromiseRejectionEvent) => {
        event.preventDefault()
        after.push(event.reason)
    }
    // A listener of this row's own, so the escape it provokes on purpose is
    // witnessed here rather than by the helper under test.
    globalThis.addEventListener('unhandledrejection', own)
    try {
        void Promise.reject(new Error('after the watcher returned'))
        await settle()
    } finally {
        globalThis.removeEventListener('unhandledrejection', own)
    }
    assertEquals(after.length, 1, 'the escape happened')
    assertEquals(seen, [], 'the helper no longer records')
})
