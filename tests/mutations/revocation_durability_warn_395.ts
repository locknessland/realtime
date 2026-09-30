/**
 * @fileoverview #395 part 2's mutation battery — `evict` and `revokeChannel`'s
 * durability WARN stays guarded, so a throwing sink cannot cancel the
 * revocation.
 *
 * Each row removes one side's `try`/`catch`-and-`writeMarkedFallback` guard,
 * putting back the bare `console.warn` that let a throwing sink abort the
 * method before `revokeLocal` / `publishControl` ran:
 *
 * - M1: `evict`'s durability WARN loses its guard.
 * - M2: `revokeChannel`'s durability WARN loses its guard.
 *
 * Every row was proven LIVE before it was trusted: a marker was placed on the
 * row's mutated line and seen to execute under the killing witness.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/revocation_durability_warn_395.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_durability_warn_395
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_durability_warn_395.test.ts', import.meta.url)
        .pathname,
]

const W1 =
    '#395 part 2 evict: revokeLocal still runs when the durability WARN ' +
    'itself throws'
const W2 = '#395 part 2 revokeChannel: publishControl still runs when the ' +
    'durability WARN itself throws'

const MUTATIONS: Mutation[] = [
    {
        label: "M1 — evict's durability WARN loses its guard: a throwing " +
            'sink aborts the method before revokeLocal runs',
        file: MANAGER,
        edits: [[
            '            try {\n' +
            '                console.warn(\n' +
            "                    'realtime: the durable revocation write failed — revoking ' +\n" +
            "                        'anyway, but a lost control frame will NOT be recovered ' +\n" +
            '                        `by reconcile: ${renderError(error)}`,\n' +
            '                )\n' +
            '            } catch (sink) {\n' +
            '                // #395 part 2: a throwing sink must not abort `evict` here —\n' +
            '                // the durable write already failed, and skipping the apply\n' +
            '                // below too would leave the connection revoked NOWHERE, local\n' +
            '                // or remote. One marked line instead, which never throws\n' +
            '                // (#391).\n' +
            '                writeMarkedFallback(EVICT_DURABILITY_LOG_FAILED, error, {\n' +
            "                    label: 'sink failure',\n" +
            '                    error: sink,\n' +
            '                })\n' +
            '            }\n',
            '            // Mutant: the guard is gone — a throwing sink aborts evict here.\n' +
            '            console.warn(\n' +
            "                'realtime: the durable revocation write failed — revoking ' +\n" +
            "                    'anyway, but a lost control frame will NOT be recovered ' +\n" +
            '                    `by reconcile: ${renderError(error)}`,\n' +
            '            )\n',
        ]],
        // Witness: 'revokeLocal still hard-closed the socket even though the
        // durability WARN itself threw' — with the guard gone, the throwing
        // sink's own error propagates out of `evict` before `revokeLocal`
        // ever runs, so the socket is never closed.
        killedBy: W1,
    },
    {
        label: "M2 — revokeChannel's durability WARN loses its guard: a " +
            'throwing sink aborts the method before publishControl runs',
        file: MANAGER,
        edits: [[
            '            try {\n' +
            '                console.warn(\n' +
            '                    `realtime: the durable revocation write for ${\n' +
            '                        safeForLog(channel)\n' +
            '                    } failed — revoking anyway, but a lost control frame ` +\n' +
            '                        `will NOT be recovered by reconcile: ${\n' +
            '                            renderError(error)\n' +
            '                        }`,\n' +
            '                )\n' +
            '            } catch (sink) {\n' +
            "                // #395 part 2: same hazard as `evict`'s durability WARN — a\n" +
            '                // throwing sink must not skip the local apply or the\n' +
            '                // control-frame publish that follow. One marked line instead,\n' +
            '                // which never throws (#391).\n' +
            '                writeMarkedFallback(\n' +
            '                    REVOKE_CHANNEL_DURABILITY_LOG_FAILED,\n' +
            '                    error,\n' +
            "                    { label: 'sink failure', error: sink },\n" +
            '                )\n' +
            '            }\n',
            '            // Mutant: the guard is gone — a throwing sink aborts revokeChannel\n' +
            '            // here.\n' +
            '            console.warn(\n' +
            '                `realtime: the durable revocation write for ${\n' +
            '                    safeForLog(channel)\n' +
            '                } failed — revoking anyway, but a lost control frame ` +\n' +
            '                    `will NOT be recovered by reconcile: ${\n' +
            '                        renderError(error)\n' +
            '                    }`,\n' +
            '            )\n',
        ]],
        // Witness: 'publishControl still ran even though the durability WARN
        // itself threw' — with the guard gone, the throwing sink's own error
        // propagates out of `revokeChannel` before `publishControl` ever runs.
        killedBy: W2,
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                "#395 part 2 — evict/revokeChannel's durability WARN stays " +
                    'guarded, so a throwing sink cannot cancel the revocation',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
