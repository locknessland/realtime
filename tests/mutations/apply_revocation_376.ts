/**
 * @fileoverview #376's mutation battery — a control-frame revocation whose own
 * WARN throws never reaches the runtime unhandled.
 *
 * Each row puts back one way the #376 containment can be wrong:
 *
 * - M1: the dispatch back to a bare `void`, no `.catch` — the shape #376 was
 *   filed against. A throwing WARN sink inside `#applyRevocation`'s catch
 *   rejects the apply, and on Deno that terminates the process.
 * - M2: the rejection interpolated with `String()` instead of `renderError`,
 *   so a CR/LF in the sink's failure reaches the log raw and can forge a line.
 * - M3: only the `revoke-channel` call site back to a bare `void`, the `evict`
 *   site left contained — a regression one call site at a time.
 *
 * The witness each row dies on — the assertion that fails, not only the test:
 *
 * - M1: `no rejection reaches the runtime`. The throwing sink's error is
 *   recorded by the `unhandledrejection` listener.
 * - M2: `no raw "\r" survives`.
 * - M3: `no rejection reaches the runtime`, in W2 — the only witness that
 *   drives a `revoke-channel` frame.
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
 * deno run -A packages/realtime/tests/mutations/apply_revocation_376.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/apply_revocation_376
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
/** Where the marked line is rendered since #391 — M2 mutates it there. */
const MARKED_FALLBACK = new URL('../../marked_fallback.ts', import.meta.url)
const SUITES = [
    new URL('../apply_revocation_376.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the control-frame dispatch back to a bare void, no catch',
        file: MANAGER,
        edits: [[
            '        this.#applyRevocation(revocation).catch((error: unknown) =>\n' +
            '            writeMarkedFallback(REVOCATION_APPLY_LOG_FAILED, error)\n' +
            '        )\n',
            '        void this.#applyRevocation(revocation)\n',
        ]],
        // Witness: `no rejection reaches the runtime` — the throwing WARN
        // sink's error escapes the fire-and-forget apply.
        killedBy:
            '#376 W1 evict: a throwing WARN sink on a failed teardown never escapes',
    },
    {
        label: 'M2 — the rejection interpolated raw, not rendered',
        // Re-anchored by #391: the line is built by `writeMarkedFallback`,
        // so the subject's rendering is mutated in its one home.
        file: MARKED_FALLBACK,
        edits: [[
            '${marker} ${renderError(subject)}',
            '${marker} ${String(subject)}',
        ]],
        // Witness: `no raw "\r" survives` — String() keeps the CR/LF.
        killedBy: '#376 W3 the sink failure is rendered, not interpolated raw',
    },
    {
        label: 'M3 — only the revoke-channel call site back to a bare void',
        file: MANAGER,
        edits: [[
            '                this.#dispatchRevocation({\n' +
            '                    target: control.target,\n' +
            '                    channel: control.channel,\n',
            '                void this.#applyRevocation({\n' +
            '                    target: control.target,\n' +
            '                    channel: control.channel,\n',
        ]],
        // Witness: `no rejection reaches the runtime`, in W2 — the clear
        // failure's throwing WARN escapes the revoke-channel apply. W1 and W3
        // drive `evict` only, so W2 is the one witness that sees this site.
        killedBy:
            '#376 W2 revoke-channel: a throwing WARN sink on a failed clear never escapes',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#376 — no control-frame revocation rejection escapes',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
