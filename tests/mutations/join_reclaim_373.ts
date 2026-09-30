/**
 * @fileoverview #373's mutation battery — the #323 join compensation's roster
 * reclaim runs whether or not `#leaveLocal` rejects, and the subscribe always
 * rejects with the ORIGINAL roster error.
 *
 * Each row puts back one way the #373 fix can be wrong:
 *
 * - M1: the collected leave outcome reverted to a bare, sequential
 *   `await this.#leaveLocal(...)` ahead of the reclaim — the shape #373 was
 *   filed against. A rejecting `unwatchChannel` then throws before the
 *   reclaim ever runs, leaving a committed-but-lost roster hold in place.
 * - M2: the leave-failure WARN removed, while the reclaim keeps running —
 *   the reclaim's own outcome is unaffected, but the failure that preceded it
 *   is silently dropped instead of reported.
 * - M3: the leave's own error thrown in place of the original roster error
 *   when the leave failed — the ordering fix without the error-choice half.
 *
 * The witness each row dies on is the same single test for all three: it
 * combines a driver whose join-roster write commits and then rejects (a lost
 * reply) with an `unwatchChannel` that rejects, on a first presence join to a
 * channel with no other local member — the one shape that exercises all three
 * failure modes at once.
 *
 * - M1: `the reclaim ran despite the leave rejecting` (the roster still holds
 *   the member) and/or the rejection assertion, since the reclaim never runs
 *   and the wrong error surfaces.
 * - M2: `exactly one WARN names the unwatch failure` (zero, not one).
 * - M3: `the subscribe's rejection is the ORIGINAL roster error, not the
 *   unwatch failure` (the unwatch failure surfaces instead).
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
 * deno run -A packages/realtime/tests/mutations/join_reclaim_373.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/join_reclaim_373
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../join_reclaim_373.test.ts', import.meta.url).pathname,
]

/** The witness both mutation modes below die on. */
const WITNESS =
    '#373 the reclaim runs and the ORIGINAL roster error wins, even when the ' +
    'leave also fails'

const MUTATIONS: Mutation[] = [
    {
        label:
            'M1 — the collected leave outcome reverted to a bare, sequential ' +
            '`await #leaveLocal` ahead of the reclaim',
        file: MANAGER,
        edits: [[
            '                const leaveOutcome = await this.#collectLeaveOutcome(\n' +
            '                    channel,\n' +
            '                    connection.id,\n' +
            '                )\n' +
            '                if (leaveOutcome.failed) {\n' +
            '                    // Reported, never thrown: the reclaim below still must\n' +
            '                    // run, and the ORIGINAL roster error is still what this\n' +
            '                    // branch ends on.\n' +
            '                    try {\n' +
            '                        console.warn(\n' +
            '                            `realtime: the local leave during a #323 join ` +\n' +
            '                                `compensation on ${safeForLog(channel)} ` +\n' +
            '                                `failed, before the roster reclaim: ${\n' +
            '                                    renderError(leaveOutcome.error)\n' +
            '                                }`,\n' +
            '                        )\n' +
            '                    } catch (sink) {\n' +
            '                        // The `throw error` below must survive a throwing\n' +
            '                        // sink too — `writeMarkedFallback` never throws\n' +
            '                        // (#391).\n' +
            '                        writeMarkedFallback(\n' +
            '                            JOIN_COMPENSATION_LOG_FAILED,\n' +
            '                            leaveOutcome.error,\n' +
            "                            { label: 'sink failure', error: sink },\n" +
            '                        )\n' +
            '                    }\n' +
            '                }\n',
            '                await this.#leaveLocal(channel, connection.id)\n',
        ]],
        // Witness: the reclaim never runs, so the roster keeps the
        // committed-but-lost hold, AND the unwatch failure surfaces in place
        // of the roster error — either assertion in the single witness dies.
        killedBy: WITNESS,
    },
    {
        label: 'M2 — the leave-failure WARN removed, the reclaim untouched',
        file: MANAGER,
        edits: [[
            '                if (leaveOutcome.failed) {\n' +
            '                    // Reported, never thrown: the reclaim below still must\n' +
            '                    // run, and the ORIGINAL roster error is still what this\n' +
            '                    // branch ends on.\n' +
            '                    try {\n' +
            '                        console.warn(\n' +
            '                            `realtime: the local leave during a #323 join ` +\n' +
            '                                `compensation on ${safeForLog(channel)} ` +\n' +
            '                                `failed, before the roster reclaim: ${\n' +
            '                                    renderError(leaveOutcome.error)\n' +
            '                                }`,\n' +
            '                        )\n' +
            '                    } catch (sink) {\n' +
            '                        // The `throw error` below must survive a throwing\n' +
            '                        // sink too — `writeMarkedFallback` never throws\n' +
            '                        // (#391).\n' +
            '                        writeMarkedFallback(\n' +
            '                            JOIN_COMPENSATION_LOG_FAILED,\n' +
            '                            leaveOutcome.error,\n' +
            "                            { label: 'sink failure', error: sink },\n" +
            '                        )\n' +
            '                    }\n' +
            '                }\n',
            '                if (leaveOutcome.failed) {\n' +
            '                    // WARN removed by the mutant.\n' +
            '                }\n',
        ]],
        // Witness: `exactly one WARN names the unwatch failure` — zero, not
        // one, with the WARN removed.
        killedBy: WITNESS,
    },
    {
        label: "M3 — the leave's own error thrown in place of the ORIGINAL " +
            'roster error',
        file: MANAGER,
        edits: [[
            "                // ALWAYS THE ORIGINAL ERROR (#373): neither the leave's\n" +
            "                // failure nor the reclaim's replaces it. Both are WARNed,\n" +
            '                // never thrown, so the caller learns exactly what refused the\n' +
            '                // join — not a symptom of the compensation that followed it.\n' +
            '                throw error\n',
            '                if (leaveOutcome.failed) throw leaveOutcome.error\n' +
            '                throw error\n',
        ]],
        // Witness: `the subscribe's rejection is the ORIGINAL roster error,
        // not the unwatch failure` — the unwatch failure surfaces instead.
        killedBy: WITNESS,
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#373 — the #323 join compensation reclaims and rejects ' +
                    'with the original error',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
