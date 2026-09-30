/**
 * @fileoverview #419's mutation battery — the 2 catch sites this issue routed
 * through `#guardedWarn` (continuing #418's own trace: T1-T7 already covered
 * by `redis_warn_trace_418.ts`) stay guarded.
 *
 * Each row puts one site's direct `console.warn` call back, the pre-#419
 * shape:
 *
 * - M1: `#announceSwept`'s `dropped()` — unguarded, its throw rejects
 *   `#announceSwept`, which `#sweepPage`'s loop awaits with no `try` of its
 *   own, so the well-formed departure right after the dropped one is never
 *   reported — the identical #395 shape M3 in `redis_warn_trace_418.ts`
 *   proves for the departure-HANDLER's own catch, one exit earlier.
 * - M2: `#parseRosterValue`'s `skipped()` — unguarded, its throw escapes
 *   `#parseRosterValue` itself and, reached here through `#announceSwept`,
 *   the same rejection as M1 propagates into `#sweepPage`'s loop.
 *
 * The witness each row dies on:
 *
 * - M1: T8, either `no rejection reaches the runtime` or the "still
 *   reported" assertion — the mutant breaks both halves T8 checks.
 * - M2: T9, the same shape as M1.
 *
 * Both rows were proven LIVE before they were trusted: with the mutant
 * applied, the killing witness was run alone and the stack of the escape it
 * reported was seen to pass through the row's mutated line (in both cases,
 * the escape climbed all the way to `#sweepInstance`'s own sweep-failed
 * WARN, `SWEEP_INSTANCE_LOG_FAILED` — proof the row's OWN marker never
 * fires when its mutant is live).
 *
 * ```bash
 * deno task mutate announce_swept_guard_419
 * ```
 *
 * @module @lockness/realtime/tests/mutations/announce_swept_guard_419
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../announce_swept_guard_419.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label:
            'M1 — #announceSwept: the dropped() drop back to a bare console.warn',
        file: REDIS,
        edits: [[
            '        const dropped = () =>\n' +
            '            // #419: called BEFORE the departure handler ever runs, still\n' +
            "            // inside #sweepPage's loop over one dead instance's owned slots\n" +
            '            // with nothing between here and there that catches a throw — so\n' +
            '            // an unguarded `console.warn` failing here would reject THIS\n' +
            '            // call the same way an unguarded handler-catch WARN used to\n' +
            '            // (#418 T3), skipping every remaining slot on the page (and\n' +
            '            // every later page) for the SAME dead instance this pass. One\n' +
            "            // marked line, through #guardedWarn's shared #369 shape.\n" +
            '            this.#guardedWarn(\n' +
            '                SWEEP_DROPPED_LOG_FAILED,\n' +
            '                `realtime: a member swept from ${\n' +
            '                    safeForLog(channel)\n' +
            '                } was not announced as left — its roster entry is ` +\n' +
            "                    'not a departure this sweep can report. The ' +\n" +
            "                    'release is committed; clients heal on ' +\n" +
            "                    'resubscribe.',\n" +
            '            )\n',
            '        const dropped = () =>\n' +
            '            console.warn(\n' +
            '                `realtime: a member swept from ${\n' +
            '                    safeForLog(channel)\n' +
            '                } was not announced as left — its roster entry is ` +\n' +
            "                    'not a departure this sweep can report. The ' +\n" +
            "                    'release is committed; clients heal on ' +\n" +
            "                    'resubscribe.',\n" +
            '            )\n',
        ]],
        // Witness: T8 — either `no rejection reaches the runtime` or the
        // "still reported" assertion; the mutant breaks both.
        killedBy: '#419 T8 ',
    },
    {
        label:
            'M2 — #parseRosterValue: the skipped() drop back to a bare console.warn',
        file: REDIS,
        edits: [[
            '        const skipped = (reason: string) =>\n' +
            "            // #419: `readRoster`'s two loops and `#announceSwept` all call\n" +
            '            // this method with no `try` of their own — so an unguarded\n' +
            '            // `console.warn` failing here escaped whichever of them called\n' +
            "            // it, aborting a `for` loop (`readRoster`) or `#sweepPage`'s\n" +
            '            // (through `#announceSwept`, the #418 T3 shape) before its\n' +
            '            // remaining entries or slots were ever reached. One marked\n' +
            "            // line, through #guardedWarn's shared #369 shape.\n" +
            '            this.#guardedWarn(\n' +
            '                ROSTER_ENTRY_LOG_FAILED,\n' +
            '                `realtime: skipped a malformed roster entry on ${\n' +
            '                    safeForLog(channel)\n' +
            "                }: ${reason}${consequence ? ` — ${consequence}` : ''}`,\n" +
            '            )\n',
            '        const skipped = (reason: string) =>\n' +
            '            console.warn(\n' +
            '                `realtime: skipped a malformed roster entry on ${\n' +
            '                    safeForLog(channel)\n' +
            "                }: ${reason}${consequence ? ` — ${consequence}` : ''}`,\n" +
            '            )\n',
        ]],
        // Witness: T9 — the same shape as M1.
        killedBy: '#419 T9 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#419 — the 2 traced catch sites stay guarded',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
