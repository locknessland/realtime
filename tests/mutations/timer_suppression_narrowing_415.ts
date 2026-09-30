/**
 * @fileoverview #415's mutation battery — the fingerprint that narrows
 * `withSuppressedFloorAnnounceRetry` to `#announceFloor`'s own retry closure,
 * never any `setTimeout` armed at the same delay.
 *
 * The property under test lives entirely inside `driver_redis_live.test.ts`
 * itself, not `drivers/redis.ts` (#415 is a test-only change), so both rows
 * mutate the TEST file — the same instrument, aimed at a test helper instead
 * of production code. Each puts a pre-#415 matching rule back, one at a time:
 *
 * - M1: matching on delay alone, `>= 500 ms` — the shape #407 originally
 *   shipped, which also froze `RECONCILE_RETRY_MS`'s reconnect retry and any
 *   other long timer armed inside a wrapped test body.
 * - M2: matching on delay alone, `=== FLOOR_ANNOUNCE_RETRY_MS` — the
 *   fingerprint dropped, the exact-delay check kept. `RECONCILE_RETRY_MS`
 *   arms the identical 1000 ms for the unrelated reconnect retry
 *   (`#runRevocationReconcile`, `drivers/redis.ts`), so this still
 *   over-suppresses.
 *
 * Both are caught by the same witness: a plain, non-`#announceFloor` handler
 * armed at exactly `FLOOR_ANNOUNCE_RETRY_MS` must reach the real
 * `setTimeout` with its OWN delay, unchanged. Either mutant suppresses it
 * instead, and the witness's `assertEquals` on the forwarded delay goes red.
 *
 * ```bash
 * deno task mutate timer_suppression_narrowing_415
 * ```
 *
 * @module @lockness/realtime/tests/mutations/timer_suppression_narrowing_415
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const TEST_FILE = new URL('../driver_redis_live.test.ts', import.meta.url)
const SUITES = [
    new URL('../driver_redis_live.test.ts', import.meta.url).pathname,
]

const WITNESS =
    '#415: withSuppressedFloorAnnounceRetry passes through a non-floor-announce timer'

const MUTATIONS: Mutation[] = [
    {
        label:
            'M1 — back to matching on delay alone (>= 500 ms, the pre-#415 shape)',
        file: TEST_FILE,
        edits: [[
            '        const isFloorAnnounceRetry = delay === FLOOR_ANNOUNCE_RETRY_MS &&\n' +
            '            isFloorAnnounceRetryHandler(handler)\n',
            '        const isFloorAnnounceRetry = delay >= 500\n',
        ]],
        killedBy: WITNESS,
    },
    {
        label: 'M2 — the fingerprint dropped, the exact-delay match kept',
        file: TEST_FILE,
        edits: [[
            '        const isFloorAnnounceRetry = delay === FLOOR_ANNOUNCE_RETRY_MS &&\n' +
            '            isFloorAnnounceRetryHandler(handler)\n',
            '        const isFloorAnnounceRetry = delay === FLOOR_ANNOUNCE_RETRY_MS\n',
        ]],
        killedBy: WITNESS,
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#415 — narrow the timer suppression to the floor-announce retry',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
