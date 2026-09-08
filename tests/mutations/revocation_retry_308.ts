/**
 * @fileoverview #308's mutation battery — the retry, and its limits.
 *
 * Three of the four rows mutate toward "more retrying", not less. That is
 * deliberate: the failure mode this change could introduce is a hot loop
 * against the command socket at exactly the moment a broker is unhealthy, and
 * a battery that only checked the retry happens would not notice it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/revocation_retry_308.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_retry_308
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const DRIVER = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_retry.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'the retry removed — a failed seam pass is retried by nothing',
        file: DRIVER,
        edits: [[
            "            if (trigger !== 'reconnect') return",
            '            return',
        ]],
        killedBy: 'a failed SEAM reconcile is retried exactly once',
    },
    {
        label: 'the retry RETRIES ITSELF — one failing broker becomes a loop',
        file: DRIVER,
        edits: [[
            "            if (trigger !== 'reconnect') return",
            "            if (trigger === 'timer') return",
        ]],
        killedBy: 'a failed SEAM reconcile is retried exactly once',
    },
    {
        label:
            'the TIMER retries too — a self-inflicted load spike while the broker is down',
        file: DRIVER,
        edits: [["            if (trigger !== 'reconnect') return\n", '']],
        killedBy: 'a failed TIMER reconcile is NOT retried',
    },
    {
        label: 'the trigger is no longer named in the WARN',
        file: DRIVER,
        edits: [[
            '`realtime: revocation reconcile failed (${trigger}): ${',
            '`realtime: revocation reconcile failed: ${',
        ]],
        killedBy: 'the WARN names WHICH trigger failed',
    },
    {
        label: 'close() stops clearing the pending retry',
        file: DRIVER,
        edits: [[
            '        if (this.revocationRetryTimer !== undefined) {\n            clearTimeout(this.revocationRetryTimer)\n            this.revocationRetryTimer = undefined\n        }\n',
            '',
        ]],
        killedBy: 'close() leaves no pending retry behind',
        expectSurvival:
            'SURVIVES, and the reason is worth keeping: `close()` also sets ' +
            '`revocationHandler = undefined`, and `#runRevocationReconcile` ' +
            'returns on that guard before it can call anything. So a retry ' +
            'that outlives close() is already inert, and clearing the timer is ' +
            'defence in depth against a future path that quiesces the timer ' +
            'without dropping the handler. The test cannot see the difference ' +
            'because there is none to observe today.',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery('#308 — the revocation retry', SUITES, MUTATIONS) > 0
            ? 1
            : 0,
    )
}
