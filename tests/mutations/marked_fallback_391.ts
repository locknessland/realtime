/**
 * @fileoverview #391's mutation battery — no marked-fallback sink throws past
 * itself, even when every log channel refuses the line.
 *
 * Each row puts back one way the #391 containment can be wrong:
 *
 * - G1: `writeMarkedFallback`'s console guard removed — `consoleFailure` now
 *   escapes the catch (after the stderr write is still attempted, exactly as
 *   it is today), so ONLY the guard is gone. **Re-shaped for #399**: the
 *   original mutant swapped `catch` for `finally`, which runs on every path,
 *   not only a throwing one — it also broke H1, a WORKING console, which
 *   then wrote to stderr too. This one changes nothing about when the block
 *   runs, only that it no longer swallows what it catches.
 * - G2: the stderr guard removed — `Deno.stderr.writeSync` called bare, so a
 *   throwing stderr propagates out of the console's catch.
 * - S1–S7: one per sink, the inline, unguarded `console.error` put back in
 *   place of the helper — the shape #391 was filed against, one site at a
 *   time. Where the site's file no longer imports `renderError`, the row adds
 *   the import back too, so the mutant type-checks and a test actually runs.
 *
 * The witness each row dies on — the assertion that fails, not only the test:
 *
 * - G1: `#391 H2`, the throwing console — the helper itself throws.
 * - G2: `#391 H3`, console and stderr both throwing — the helper throws.
 * - S1–S7: `no rejection reaches the runtime` in the sink table's row of the
 *   same number, except S6, the deadline, whose sink runs in a timer callback
 *   and dies on `no synchronous throw` (FakeTime's `tickAsync` re-throws it).
 *
 * Each is named again on its row below.
 *
 * **S3 now `SURVIVED*` (#383, 2026-09-26).** Item 2's `#warnReconcileFailed`
 * self-guards `#runRevocationReconcile`'s own WARN, so nothing inside it can
 * reject the promise past the outer `.catch` any more — S3's fixture (the
 * handler throws, every channel throws) no longer reaches that `.catch` at
 * all. Recorded on the row, not silently dropped.
 *
 * Every row was proven LIVE before it was trusted: with the mutant applied,
 * the killing witness was run alone and the stack of the error it reported
 * was seen to pass through the row's mutated line. **That is a one-time,
 * manual check** (#399) — the artefact that stands behind it on every run
 * after is this battery's own printed output: `runBattery` names, per row,
 * `KILLED <label>` with the witness `killedBy` required, or `SURVIVED`/
 * `SURVIVED*` when it did not, so a row's claim to reach its sink is backed
 * by that line every time the battery runs, not only by this comment. A row
 * whose line never runs reports a kill it did not cause — which is exactly
 * what re-running the battery after touching a row is for.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/marked_fallback_391.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/marked_fallback_391
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const HELPER = new URL('../../marked_fallback.ts', import.meta.url)
const WEBSOCKET = new URL('../../websocket.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const DEADLINE = new URL(
    '../../drivers/enforcement_deadline.ts',
    import.meta.url,
)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../marked_fallback_391.test.ts', import.meta.url).pathname,
    new URL('../marked_fallback_sinks_391.test.ts', import.meta.url).pathname,
]

/**
 * The helper's import in websocket.ts and enforcement_deadline.ts, where a
 * row re-adds `renderError` beside it (#399: re-anchored onto the combined
 * `markedFallbackMarker` + `writeMarkedFallback` import each now uses, since
 * both files still declare their marker constants through
 * `markedFallbackMarker`, unaffected by a row that only mutates one CALL
 * site).
 */
const WEBSOCKET_IMPORT =
    "import { markedFallbackMarker, writeMarkedFallback } from './marked_fallback.ts'\n"
const WEBSOCKET_WITH_RENDER =
    "import { renderError } from '@lockness/contract'\nimport { markedFallbackMarker, writeMarkedFallback } from './marked_fallback.ts'\n"
const DEADLINE_IMPORT =
    "import {\n    markedFallbackMarker,\n    writeMarkedFallback,\n} from '../marked_fallback.ts'\n"
const DEADLINE_WITH_RENDER =
    "import { renderError } from '@lockness/contract'\nimport {\n    markedFallbackMarker,\n    writeMarkedFallback,\n} from '../marked_fallback.ts'\n"

const MUTATIONS: Mutation[] = [
    {
        label: "G1 — the helper's console guard removed (a throw now escapes)",
        // #399: the prior mutant swapped `catch` for `finally`, which runs on
        // EVERY path, not only a throwing console — so it also broke H1 (a
        // WORKING console), which then wrote to stderr too. This mutant keeps
        // the catch's OWN behaviour (stderr is still attempted, exactly once,
        // only when the console throws) and removes only the one thing the
        // label claims: the guard that stops `consoleFailure` from escaping.
        file: HELPER,
        edits: [[
            '    } catch (consoleFailure) {\n        // The console refused the ERROR line: write it past the console,\n        // naming what the console itself threw (#399) \u2014 the prior line named\n        // only what console.error was given, never why it refused it.\n        try {\n            Deno.stderr.writeSync(\n                new TextEncoder().encode(\n                    `${line}; console failure: ${\n                        renderError(consoleFailure)\n                    }\\n`,\n                ),\n            )\n        } catch {\n            // #391 THE LAST RESORT: the console and stderr both refused, so\n            // no channel is left to log this on, and a re-throw would reach\n            // a caller that has none \u2014 an unhandled rejection or an uncaught\n            // timer exception, which terminates the process on Deno. Dropping\n            // one log line is the lesser harm.\n        }\n    }\n',
            '    } catch (consoleFailure) {\n        // The console refused the ERROR line: write it past the console,\n        // naming what the console itself threw (#399) \u2014 the prior line named\n        // only what console.error was given, never why it refused it.\n        try {\n            Deno.stderr.writeSync(\n                new TextEncoder().encode(\n                    `${line}; console failure: ${\n                        renderError(consoleFailure)\n                    }\\n`,\n                ),\n            )\n        } catch {\n            // #391 THE LAST RESORT: the console and stderr both refused, so\n            // no channel is left to log this on, and a re-throw would reach\n            // a caller that has none \u2014 an unhandled rejection or an uncaught\n            // timer exception, which terminates the process on Deno. Dropping\n            // one log line is the lesser harm.\n        }\n        throw consoleFailure\n    }\n',
        ]],
        // Witness: H2 — a throwing console.error escapes the helper.
        killedBy: '#391 H2 (marker and subject)',
    },
    {
        label: "G2 — the helper's stderr guard removed (a bare writeSync)",
        // Re-anchored for #399: the stderr write now also names what the
        // console threw, so its call spans several lines.
        file: HELPER,
        edits: [[
            '        try {\n            Deno.stderr.writeSync(\n                new TextEncoder().encode(\n                    `${line}; console failure: ${\n                        renderError(consoleFailure)\n                    }\\n`,\n                ),\n            )\n        } catch {\n',
            '            Deno.stderr.writeSync(\n                new TextEncoder().encode(\n                    `${line}; console failure: ${\n                        renderError(consoleFailure)\n                    }\\n`,\n                ),\n            )\n        {\n',
        ]],
        // Witness: H3 — a throwing stderr escapes the helper.
        killedBy: '#391 H3 (marker and subject)',
    },
    {
        label: "S1 — websocket's #369 marked line back to a bare console.error",
        file: WEBSOCKET,
        edits: [
            [
                WEBSOCKET_IMPORT,
                WEBSOCKET_WITH_RENDER,
            ],
            [
                '                writeMarkedFallback(HOOK_FAILED_TOO, error, {\n' +
                "                    label: 'hook failure',\n" +
                '                    error: failure,\n' +
                '                })\n',
                '                console.error(\n' +
                '                    `${HOOK_FAILED_TOO} ${renderError(error)}` +\n' +
                '                        `; hook failure: ${renderError(failure)}`,\n' +
                '                )\n',
            ],
        ],
        // Witness: `no rejection reaches the runtime` — reportError rejects.
        killedBy: '#391 S1 ',
    },
    {
        label: "S2 — websocket's default line back to a bare console.error",
        file: WEBSOCKET,
        edits: [
            [
                WEBSOCKET_IMPORT,
                WEBSOCKET_WITH_RENDER,
            ],
            [
                '        writeMarkedFallback(UNHANDLED_WEBSOCKET_ERROR, error)\n',
                '        console.error(`${UNHANDLED_WEBSOCKET_ERROR} ${renderError(error)}`)\n',
            ],
        ],
        // Witness: `no rejection reaches the runtime` — reportError rejects.
        killedBy: '#391 S2 ',
    },
    {
        label:
            "S3 — redis revocation chain's last catch back to a bare console.error",
        file: REDIS,
        edits: [[
            '                writeMarkedFallback(REVOCATION_LOG_FAILED, error)\n',
            '                console.error(`${REVOCATION_LOG_FAILED} ${renderError(error)}`)\n',
        ]],
        // Witness: `no rejection reaches the runtime` — the chain's .catch
        // handler throws, and nothing is after it.
        killedBy: '#391 S3 ',
        expectSurvival:
            'SURVIVES since #383 item 2: #warnReconcileFailed self-guards ' +
            "#runRevocationReconcile's own WARN (S3's fixture is exactly a " +
            'thrown handler with every channel throwing), so nothing inside ' +
            'it can reject the promise past this outer .catch any more — ' +
            'true defence-in-depth now, unreachable from this suite.',
    },
    {
        label:
            "S4 — redis sweep chain's last catch back to a bare console.error",
        file: REDIS,
        edits: [[
            '                    writeMarkedFallback(SWEEP_LOG_FAILED, error)\n',
            '                    console.error(`${SWEEP_LOG_FAILED} ${renderError(error)}`)\n',
        ]],
        // Witness: `no rejection reaches the runtime`.
        killedBy: '#391 S4 ',
    },
    {
        label:
            "S5 — redis #guardedWarn's marked fallback back to a bare console.error",
        // Re-anchored for #409: #warnPassSample's own writeMarkedFallback
        // call moved into #guardedWarn, the one helper every self-guarded
        // WARN in the file now shares — so this mutates the shared call
        // itself, using #guardedWarn's own parameter names.
        file: REDIS,
        edits: [[
            '            writeMarkedFallback(marker, subject, {\n' +
            "                label: 'sink failure',\n" +
            '                error: sink,\n' +
            '            })\n',
            '            console.error(\n' +
            '                `${marker} ${renderError(subject)}; ` +\n' +
            '                    `sink failure: ${renderError(sink)}`,\n' +
            '            )\n',
        ]],
        // Witness: `no rejection reaches the runtime` — the rejection handler
        // on the handler's promise throws, and nothing is after it.
        killedBy: '#391 S5 ',
    },
    {
        label: "S6 — the deadline's #write back to a bare console.error",
        file: DEADLINE,
        edits: [
            [
                DEADLINE_IMPORT,
                DEADLINE_WITH_RENDER,
            ],
            [
                '            writeMarkedFallback(REVOCATION_LOG_FAILED, text, {\n' +
                "                label: 'sink failure',\n" +
                '                error: failure,\n' +
                '            })\n',
                '            console.error(\n' +
                '                `${REVOCATION_LOG_FAILED} ${renderError(text)}; ` +\n' +
                '                    `sink failure: ${renderError(failure)}`,\n' +
                '            )\n',
            ],
        ],
        // Witness: `no synchronous throw` — the timer callback throws, and
        // FakeTime's tickAsync re-throws it.
        killedBy: '#391 S6 ',
    },
    {
        label:
            "S7 — the manager's #dispatchRevocation back to a bare console.error",
        file: MANAGER,
        edits: [[
            '            writeMarkedFallback(REVOCATION_APPLY_LOG_FAILED, error)\n',
            '            console.error(\n' +
            '                `${REVOCATION_APPLY_LOG_FAILED} ${renderError(error)}`,\n' +
            '            )\n',
        ]],
        // Witness: `no rejection reaches the runtime` — the apply's .catch
        // handler throws, and nothing is after it.
        killedBy: '#391 S7 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#391 — no marked-fallback sink throws past itself',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
