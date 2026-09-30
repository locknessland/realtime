/**
 * @fileoverview #369's mutation battery — no hook's rejection escapes
 * `buildEvents` unreported, and neither does the sink's own.
 *
 * Each row puts back one way the #369 sink can be wrong:
 *
 * - M1: `onClose` called with a bare `void` instead of through `guard()`.
 * - M2: `reportError()` re-throwing the app's `onError` failure instead of
 *   falling back. With either one, a single rejection reaches the runtime,
 *   which on Deno terminates the process.
 * - M3: the hook's own failure interpolated with `String()` instead of
 *   `renderError`, so a CR/LF, an ANSI sequence or a U+202E override in it
 *   reaches the log raw.
 * - M4: the `return` after a working `onError` deleted, so the default line is
 *   written as well.
 *
 * The witness each row dies on — the assertion that fails, not only the test:
 *
 * - M1: `no rejection reaches the runtime`. The re-thrown unwatch failure is
 *   recorded by the `unhandledrejection` listener, and onError is called 0
 *   times.
 * - M2: `no rejection reaches the runtime`. The sink's own failure escapes
 *   through `guard()`.
 * - M3: `no raw "\r" survives`.
 * - M4: `a working onError handled it: the default sink writes nothing`. One
 *   line was written.
 *
 * Each is named again on its row below.
 *
 * Every row was proven LIVE before it was trusted: a marker was placed on the
 * row's mutated line and seen to execute under the killing witness. A row
 * whose line never runs reports a kill it did not cause.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/websocket_close_guard_369.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/websocket_close_guard_369
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const WEBSOCKET = new URL('../../websocket.ts', import.meta.url)
/** Where the marked line is rendered since #391 — M3 mutates it there. */
const MARKED_FALLBACK = new URL('../../marked_fallback.ts', import.meta.url)
const SUITES = [
    new URL('../websocket_close_guard_369.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — onClose back to a bare void, bypassing guard()',
        file: WEBSOCKET,
        edits: [[
            'void guard(conn, () => hooks.onClose?.(conn, evt.code, evt.reason))',
            'void hooks.onClose?.(conn, evt.code, evt.reason)',
        ]],
        // Witness: `no rejection reaches the runtime` — the unwatch failure
        // `disconnect` re-throws is recorded by the listener, and onError is
        // called 0 times.
        killedBy:
            '#369 W1 handlerHooks: a rejected unwatch on the last close reaches onError exactly once',
    },
    {
        label: 'M2 — the onError failure re-thrown, no fallback line',
        file: WEBSOCKET,
        edits: [[
            '            } catch (failure) {\n',
            '            } catch (failure) {\n' +
            '                if (failure !== MUTANT_NEVER) throw failure\n',
        ], [
            '    const reportError = async (\n',
            '    const MUTANT_NEVER = Symbol()\n' +
            '    const reportError = async (\n',
        ]],
        // Witness: `no rejection reaches the runtime` — the sink's own
        // rejection escapes through `guard()`.
        killedBy: '#369 W3 an onError that rejects, on onMessage',
    },
    {
        label: 'M3 — the hook failure interpolated raw, not rendered',
        // Re-anchored by #391: the line is built by `writeMarkedFallback`,
        // so the second half's rendering is mutated in its one home.
        file: MARKED_FALLBACK,
        edits: [[
            '${renderError(failure.error)}',
            '${String(failure.error)}',
        ]],
        // Witness: `no raw "\r" survives` — String() keeps the CR/LF.
        killedBy: '#369 W3 the hook failure is encoded like the original error',
    },
    {
        label: 'M4 — the return after a working onError deleted',
        file: WEBSOCKET,
        edits: [[
            '            // both lines, and never the default line after a working hook.\n' +
            '            return\n',
            '            // both lines, and never the default line after a working hook.\n',
        ]],
        // Witness: `a working onError handled it: the default sink writes
        // nothing` — the default line is written too.
        killedBy:
            '#369 W2 app onClose: an async rejection reaches onError exactly once',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#369 — no rejection escapes the websocket hooks or their sink',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
