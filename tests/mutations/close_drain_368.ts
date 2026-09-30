/**
 * @fileoverview #368's mutation battery — `close()` bounds its wait for the
 * ghost-sweep pass, the lapse run and the roster-maintenance drain (#371) at
 * one liveness TTL, through `drivers/close_drain.ts`.
 *
 * The decisions spread over two homes: `close_drain.ts` (the one shared
 * timer, cleared once both works settle, ref'd on purpose) and `close()`
 * itself in `drivers/redis.ts` (the budget it computes, the one WARN it
 * writes, and that the handler drops and the owned-connection closes run
 * whether or not the drain expired). Each row drops one clause a refactor
 * could drop while `close_drain_368.test.ts`'s witnesses that do not exercise
 * it stay green.
 *
 * - N1 the budget removed (effectively unbounded): close() never resolves
 *   within the ticks a witness bounds it with.
 * - N2 a budget per step, not one shared expiry: two stalled works double the
 *   wait instead of sharing it.
 * - N3 the wrong constant: `MAX_TIMER_MS` swapped for a value that still
 *   compiles and still bounds SOMETHING, so only a witness that pins the
 *   exact budget catches it.
 * - N4 no WARN: the sink call dropped, the fallback and the rest of teardown
 *   untouched.
 * - N5 the departure handler drop skipped on expiry: a release that settles
 *   late announces again.
 * - N6 the slot freed on expiry: the sweep pass's bookkeeping cleared before
 *   the WARN reads it, losing the age it should report.
 * - N7 the timer not cleared: a healthy drain still leaves one behind.
 * - N8 the timer unref'd: a stall with no I/O never lets a REAL process
 *   settle.
 * - N9 the fallback removed: a throwing `console.warn` escapes uncaught
 *   instead of falling back to the marked line.
 * - N10 the owned-connection close skipped on expiry: `close()` never
 *   releases what it owns once the drain has given up.
 * - N11 the roster-maintenance drain (#371) not shared with the budget: a
 *   healthy stand-in passed instead of the real drain, so a stalled drain no
 *   longer holds `close()` at all.
 *
 * `killedBy` strings end in a space (or a closing paren) so `#368 W1 ` never
 * matches `#368 W1b …`.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate close_drain_368
 * ```
 *
 * @module @lockness/realtime/tests/mutations/close_drain_368
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const CLOSE_DRAIN = new URL('../../drivers/close_drain.ts', import.meta.url)
const SUITES = [
    new URL('../close_drain_368.test.ts', import.meta.url).pathname,
]

/** The budget computation, whole. */
const BUDGET_MS = '        const budgetMs = Math.min(\n' +
    '            this.livenessTtlSeconds * 1000,\n' +
    '            MAX_TIMER_MS,\n' +
    '        )\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'N1 — the budget removed (effectively unbounded)',
        file: REDIS,
        edits: [[
            BUDGET_MS,
            '        const budgetMs = Number.MAX_SAFE_INTEGER\n',
        ]],
        killedBy: '#368 W1 ',
    },
    {
        label: 'N2 — a budget per step, not one shared expiry',
        file: CLOSE_DRAIN,
        edits: [[
            '    let lapseRunSettled = false\n' +
            '    await Promise.race([\n' +
            '        lapseRun.then(() => {\n' +
            '            lapseRunSettled = true\n' +
            '        }),\n' +
            '        expiry,\n' +
            '    ])\n',
            '    let lapseRunSettled = false\n' +
            '    await Promise.race([\n' +
            '        lapseRun.then(() => {\n' +
            '            lapseRunSettled = true\n' +
            '        }),\n' +
            '        new Promise<void>((resolve) => setTimeout(resolve, budgetMs)),\n' +
            '    ])\n',
        ]],
        killedBy: '#368 W3 ',
    },
    {
        label:
            'N3 — the wrong constant (MAX_TIMER_MS swapped for heartbeatIntervalMs)',
        file: REDIS,
        edits: [[
            '            MAX_TIMER_MS,\n        )\n',
            '            this.heartbeatIntervalMs,\n        )\n',
        ]],
        killedBy: '#368 W7 ',
    },
    {
        label: 'N4 — no WARN written on expiry',
        // Re-anchored for #409: #warnCloseDrainExpired's own console.warn
        // call moved into a call to #guardedWarn — its marker
        // (CLOSE_LOG_FAILED) is unique to this call, so no disambiguation is
        // needed.
        file: REDIS,
        edits: [[
            '        this.#guardedWarn(CLOSE_LOG_FAILED, text)\n',
            '        undefined\n',
        ]],
        killedBy: '#368 W1 ',
    },
    {
        label: 'N5 — the departure handler drop skipped on expiry',
        file: REDIS,
        edits: [[
            '        // A closed driver reports no departure either (#348) — dropped here\n' +
            '        // whether or not the drain above expired.\n' +
            '        this.#departureHandler = undefined\n',
            '        // A closed driver reports no departure either (#348) — dropped here\n' +
            '        // whether or not the drain above expired.\n' +
            '        if (!pending.sweepPass && !pending.lapseRun) {\n' +
            '            this.#departureHandler = undefined\n' +
            '        }\n',
        ]],
        killedBy: '#368 W1 ',
    },
    {
        // Re-anchored for #371: the guard now also reads
        // `pending.maintenanceDrain`.
        label:
            "N6 — the sweep pass's bookkeeping (the slot) freed before the WARN reads it",
        file: REDIS,
        edits: [[
            '        if (pending.sweepPass || pending.lapseRun || pending.maintenanceDrain) {\n' +
            '            this.#warnCloseDrainExpired(pending, budgetMs)\n' +
            '        }\n',
            '        if (pending.sweepPass || pending.lapseRun || pending.maintenanceDrain) {\n' +
            '            this.#sweepPass = undefined\n' +
            '            this.#warnCloseDrainExpired(pending, budgetMs)\n' +
            '        }\n',
        ]],
        killedBy: '#368 W1 ',
    },
    {
        // Re-anchored for #371: the return is now a three-field object
        // literal, not a one-line object expression.
        label: 'N7 — the timer not cleared',
        file: CLOSE_DRAIN,
        edits: [[
            '    clearTimeout(timer)\n' +
            '    return {\n' +
            '        sweepPass: !sweepPassSettled,\n' +
            '        lapseRun: !lapseRunSettled,\n' +
            '        maintenanceDrain: !maintenanceDrainSettled,\n' +
            '    }\n',
            '    return {\n' +
            '        sweepPass: !sweepPassSettled,\n' +
            '        lapseRun: !lapseRunSettled,\n' +
            '        maintenanceDrain: !maintenanceDrainSettled,\n' +
            '    }\n',
        ]],
        killedBy: '#368 W4 (i)',
    },
    {
        label: "N8 — the timer unref'd",
        file: CLOSE_DRAIN,
        edits: [[
            '    const timer = setTimeout(resolveExpiry, budgetMs)\n',
            '    const timer = setTimeout(resolveExpiry, budgetMs)\n' +
            '    Deno.unrefTimer(timer)\n',
        ]],
        killedBy: '#368 W8 ',
    },
    {
        label:
            'N9 — the marked fallback removed: a throwing console.warn escapes uncaught',
        // Re-anchored for #409: the try/catch moved into #guardedWarn — this
        // reverts the ONE call to a bare, unguarded console.warn (the
        // pre-#369 shape), same as N4's marker-uniqueness reasoning.
        file: REDIS,
        edits: [[
            '        this.#guardedWarn(CLOSE_LOG_FAILED, text)\n',
            '        console.warn(text)\n',
        ]],
        // W6's throwing console.warn now escapes `close()` as an unhandled
        // rejection — an uncaught error the harness attributes to the whole
        // module, never to one Deno.test name (harness.ts's own rule: a kill
        // with no name is still a kill).
        killedBy: '(uncaught error)',
    },
    {
        label: 'N10 — the owned-connection close skipped on expiry',
        file: REDIS,
        edits: [[
            '        for (const resource of this.owned) {\n' +
            '            await resource.close()\n' +
            '        }\n',
            '        if (!pending.sweepPass && !pending.lapseRun) {\n' +
            '            for (const resource of this.owned) {\n' +
            '                await resource.close()\n' +
            '            }\n' +
            '        }\n',
        ]],
        killedBy: '#368 W5 ',
    },
    {
        label:
            'N11 — the roster-maintenance drain (#371) not shared with the budget',
        file: REDIS,
        edits: [[
            '            maintenanceStopped,\n',
            '            Promise.resolve(),\n',
        ]],
        killedBy: '#368 W9 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                "#368 — close()'s bounded drain of the sweep pass, the " +
                    'lapse run and the roster-maintenance drain',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
