/**
 * @fileoverview #381's mutation battery — the Redis driver refuses a heartbeat
 * interval above the timer ceiling at construction.
 *
 * The decision lives in one home, `drivers/redis.ts`: the constructor's
 * ceiling clause, after the #293 relation, compared against `MAX_TIMER_MS`.
 * Each row drops one part a refactor could drop while the #293 witnesses stay
 * green, because every #381 witness pairs its interval with a liveness TTL the
 * relation admits. All rows are killed by `heartbeat_ceiling_381.test.ts`.
 *
 * `killedBy` strings end in a space, so `H1 ` never matches a later `H10`.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate heartbeat_ceiling_381
 * ```
 *
 * @module @lockness/realtime/tests/mutations/heartbeat_ceiling_381
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../heartbeat_ceiling_381.test.ts', import.meta.url).pathname,
]

/** The ceiling comparison, whole. */
const CEILING = '        if (this.heartbeatIntervalMs > MAX_TIMER_MS) {\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'N1 the ceiling comparison dropped',
        file: REDIS,
        edits: [[CEILING, '        if (false) {\n']],
        killedBy: '#381 H1 ',
    },
    {
        label: 'N2 the ceiling made inclusive (>=)',
        file: REDIS,
        edits: [[
            CEILING,
            '        if (this.heartbeatIntervalMs >= MAX_TIMER_MS) {\n',
        ]],
        killedBy: '#381 H2 ',
    },
    {
        label: 'N3 the ceiling one too loose (2^31 still admitted)',
        file: REDIS,
        edits: [[
            CEILING,
            '        if (this.heartbeatIntervalMs > MAX_TIMER_MS + 1) {\n',
        ]],
        killedBy: '#381 H1 ',
    },
    {
        label: 'N4 the ceiling dropped from the message',
        file: REDIS,
        edits: [[
            '                    `ceiling of ${MAX_TIMER_MS}ms (#381) — got ` +\n',
            '                    `ceiling (#381) — got ` +\n',
        ]],
        killedBy: '#381 H3 ',
    },
    {
        label: 'N5 the liveness TTL dropped from the message',
        file: REDIS,
        edits: [[
            '                    `livenessTtlSeconds=${this.livenessTtlSeconds}s. A longer ` +\n',
            '                    `A longer ` +\n',
        ]],
        killedBy: '#381 H3 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#381 — a heartbeat interval above the timer ceiling refuses ' +
                    'to boot',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
