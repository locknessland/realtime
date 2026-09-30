/**
 * @fileoverview #384's mutation battery — the revocation re-check counts its
 * applies, each pass sample carries the counts, and only a clean pass
 * re-arms the enforcement deadline.
 *
 * The decisions live in `manager.ts` (whether an apply completed, and where
 * the tally is counted), `drivers/redis.ts` (the decoder, its WARN, the
 * sample's counts, the clean-pass condition, the sweep's counts) and
 * `drivers/enforcement_deadline.ts` (`passEnded`, the premise). Each row drops
 * one clause a refactor could drop while the rest of the suite stays green;
 * each is killed by the witness its `killedBy` names, in
 * `tests/revocation_tally_384.test.ts`.
 *
 * - **Manager.** K1 `#applyRevocation`'s catch reports completed (T2). K2
 *   `revokeLocal`'s catch reports fulfilled (T3). K3 the wrapper's catch not
 *   counted (T4). K4 `attempted` counted before the ownership check (T5). K5
 *   the channel apply reports `!clearFailed` (T6).
 * - **Decoder and sample.** K6 the resolved value discarded (R1). K7 `failed`
 *   above `attempted` accepted (R3b). K8 the safe-integer checks weakened to
 *   `typeof === 'number'` (R3b, the fractional count). K9 both keys added
 *   when the counts are `undefined` (R2). K19 the malformed mark dropped from
 *   the end site (R3b's `MISSED`). K20 a WARN for every value that is not
 *   `undefined` — the rejected S-F1 shape (R3a). K21 the decoder's own `try`
 *   removed, so a throwing getter fails the pass (R3b, the getter). K22 the
 *   malformed WARN written at the end site, after a trailing pass started
 *   (R3b's order).
 * - **Deadline.** K10 the clean clause dropped (R4). K11 the all-failed rule
 *   (R5). K12 a pass with no tally counted as failed (R7). K17 the `MISSED`
 *   premise reverted (R8). K18 `passEnded()` a no-op (R9).
 * - **Sweep.** K13 the failure not counted (S1). K14 the failure counted
 *   after the WARN (S1b). K15 `attempts` counted in `#reconcile` for every
 *   peer it probes, live ones included, instead of in `#sweepInstance` (S1,
 *   whose live peer is what kills it). K16 `renewed` counted as a failure
 *   (S3).
 * - **Review additions.** K23 the decoder's `failed < 0` check dropped (R3b,
 *   `failed below zero`). K24 `passSucceeded` no longer forgets a pass that
 *   ended (R10: fail, clean, stall reads `MISSED`). K25 the malformed WARN as
 *   a bare `console.warn` (R3c). K26 the resolved value rendered into the
 *   malformed WARN (R3b). K25 and K26 are row 5a's two rejected shapes.
 *
 * Every row was proven LIVE by the harness run that recorded it: the suite is
 * green on the pristine source, the mutant is the only change, and it turned
 * its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_tally_384
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_tally_384
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const DEADLINE = new URL(
    '../../drivers/enforcement_deadline.ts',
    import.meta.url,
)
const SUITES = [
    new URL('../revocation_tally_384.test.ts', import.meta.url).pathname,
]

/** `#applyRevocation`'s catch tail. */
const APPLY_CATCH_TAIL = '                } failed: ${renderError(error)}`,\n' +
    '            )\n' +
    '            return false\n'

/** `revokeLocal`'s catch tail. */
const REVOKE_CATCH_TAIL =
    '                    `after hard-close: ${renderError(error)}`,\n' +
    '            )\n' +
    '            return false\n'

/**
 * The wrapper's failure count in `#recheckRevocations`, above its WARN.
 * Re-anchored in the #384 review fix, which moved the count above the WARN.
 */
const WRAPPER_CATCH_COUNT =
    '                // Counted BEFORE the WARN, as the sweep counts: a sink that\n' +
    '                // throws cannot skip it.\n' +
    '                failed++\n'

/** The wrapper's attempt count. */
const WRAPPER_ATTEMPT = '            attempted++\n            try {\n'

/** The top of the grouping loop, with the ownership check. */
const OWNERSHIP_CHECK = '        for (const revocation of revocations) {\n' +
    '            if (!this.connections.has(revocation.target)) continue\n'

/** The channel scope of `#applyRevocation`. */
const CHANNEL_APPLY =
    '            await this.#revokeChannelLocal(revocation)\n' +
    '            return true\n'

/** The decode, inside `#runRevocationReconcile`'s `try`. */
const DECODE =
    '            const tally = decodeRevocationTally(await this.revocationHandler())\n'

/** The decoder's two safe-integer checks. */
const SAFE_ATTEMPTED =
    "        if (!Number.isSafeInteger(attempted)) return 'malformed'\n"
const SAFE_FAILED =
    "        if (!Number.isSafeInteger(failed)) return 'malformed'\n"

/** The decoder's range check. */
const RANGE =
    '        if (counts.failed < 0 || counts.failed > counts.attempted) {\n'

/** The decoder's own `try`, and its catch. */
const DECODER_TRY = '    try {\n' +
    "        if (!('attempted' in value) && !('failed' in value)) return undefined\n"
const DECODER_CATCH = '    } catch {\n' +
    "        // Not silent: `'malformed'` is what the caller WARNs on, once per pass.\n" +
    "        return 'malformed'\n" +
    '    }\n'

/** The sample's conditional counts. */
const SAMPLE_COUNTS =
    '            ...(attempts !== undefined && failures !== undefined\n'

/** The end site's clean-pass condition. */
const CLEAN = "                const clean = outcome === 'ok' &&\n" +
    '                    pass.malformed !== true &&\n' +
    '                    (pass.failures ?? 0) === 0\n'

/**
 * The malformed WARN's #391-shaped write. Prefixed with the `line`
 * construction's last, distinctive segment: since #409, both this method and
 * `#warnReconcileFailed` call the identical `this.#guardedWarn
 * (REVOCATION_LOG_FAILED, line)`, so the bare call alone now matches twice.
 */
const MALFORMED_WRITE = "            'packages/realtime/driver.ts).'\n" +
    '        this.#guardedWarn(REVOCATION_LOG_FAILED, line)\n'

/** The malformed WARN's one call. */
const WARN_MALFORMED = '                this.#warnMalformedTally(trigger)\n'

/** Where the end site has started the trailing pass, or armed the timer. */
const TRAILING_STARTED =
    '                if (rerun !== undefined) this.#startRevocationPass(rerun)\n' +
    '                else this.#armRevocationReconcile()\n'

/** The sweep's failure count, with its comment. */
const SWEEP_FAILURE =
    '            // Counted BEFORE the WARN, so a sink that throws cannot skip it.\n' +
    '            if (this.#sweepPass) this.#sweepPass.failures++\n'

/** The sweep's attempt count. */
const SWEEP_ATTEMPT =
    '        if (this.#sweepPass) this.#sweepPass.attempts++\n'

/** The tail of the sweep's failed-branch WARN. */
const SWEEP_FAILED_WARN_TAIL =
    '                    renderError(end.failed),\n' +
    '            )\n'

/** `#reconcile`'s self-skip: every instance after it is probed. */
const RECONCILE_SELF_SKIP =
    '                if (!id || id === this.instanceId) continue\n'

const MUTATIONS: Mutation[] = [
    // --- Manager ------------------------------------------------------------
    {
        label: "K1 — #applyRevocation's catch reports the apply completed",
        file: MANAGER,
        edits: [[
            APPLY_CATCH_TAIL,
            APPLY_CATCH_TAIL.replace('return false', 'return true'),
        ]],
        killedBy: '#384 T2 ',
    },
    {
        label: "K2 — revokeLocal's catch reports the teardown fulfilled",
        file: MANAGER,
        edits: [[
            REVOKE_CATCH_TAIL,
            REVOKE_CATCH_TAIL.replace('return false', 'return true'),
        ]],
        killedBy: '#384 T3 ',
    },
    {
        label: "K3 — the apply wrapper's catch does not count a failure",
        file: MANAGER,
        edits: [[WRAPPER_CATCH_COUNT, '']],
        killedBy: '#384 T4 ',
    },
    {
        label: 'K4 — attempted counted before the ownership check',
        file: MANAGER,
        edits: [
            [WRAPPER_ATTEMPT, '            try {\n'],
            [
                OWNERSHIP_CHECK,
                '        for (const revocation of revocations) {\n' +
                '            attempted++\n' +
                '            if (!this.connections.has(revocation.target)) continue\n',
            ],
        ],
        killedBy: '#384 T5 ',
    },
    {
        label: 'K5 — the channel apply reports !clearFailed',
        file: MANAGER,
        edits: [[
            CHANNEL_APPLY,
            '            return !(await this.#revokeChannelLocal(revocation))\n' +
            '                .clearFailed\n',
        ]],
        killedBy: '#384 T6 ',
    },
    // --- Decoder and sample -------------------------------------------------
    {
        label: 'K6 — the resolved value discarded',
        file: REDIS,
        edits: [[
            DECODE,
            '            await this.revocationHandler()\n' +
            '            const tally = decodeRevocationTally(undefined)\n',
        ]],
        killedBy: '#384 R1 ',
    },
    {
        label: 'K7 — the decoder accepts failed above attempted',
        file: REDIS,
        edits: [[RANGE, '        if (counts.failed < 0) {\n']],
        killedBy: '#384 R3b ',
    },
    {
        label: "K8 — the safe-integer checks weakened to typeof === 'number'",
        file: REDIS,
        edits: [
            [
                SAFE_ATTEMPTED,
                "        if (typeof attempted !== 'number') return 'malformed'\n",
            ],
            [
                SAFE_FAILED,
                "        if (typeof failed !== 'number') return 'malformed'\n",
            ],
        ],
        killedBy: '#384 R3b ',
    },
    {
        label: 'K9 — both keys added when the counts are undefined',
        file: REDIS,
        edits: [[SAMPLE_COUNTS, '            ...(true\n']],
        killedBy: '#384 R2 ',
    },
    {
        label: 'K19 — the malformed mark dropped from the end site',
        file: REDIS,
        edits: [[
            CLEAN,
            CLEAN.replace(
                '                    pass.malformed !== true &&\n',
                '',
            ),
        ]],
        killedBy: '#384 R3b ',
    },
    {
        label: 'K20 — a WARN for every value that is not undefined (S-F1)',
        file: REDIS,
        edits: [[
            DECODE,
            '            const value = await this.revocationHandler()\n' +
            '            const decoded = decodeRevocationTally(value)\n' +
            '            const tally = decoded === undefined && value !== undefined\n' +
            "                ? 'malformed'\n" +
            '                : decoded\n',
        ]],
        killedBy: '#384 R3a ',
    },
    {
        label: "K21 — the decoder's own try removed",
        file: REDIS,
        edits: [
            [DECODER_TRY, DECODER_TRY.replace('    try {\n', '    {\n')],
            [DECODER_CATCH, '    }\n'],
        ],
        killedBy: '#384 R3b ',
    },
    {
        label: 'K22 — the malformed WARN written at the end site, after the ' +
            'trailing pass started',
        file: REDIS,
        edits: [
            [WARN_MALFORMED, ''],
            [
                TRAILING_STARTED,
                TRAILING_STARTED +
                '                if (pass.malformed) this.#warnMalformedTally(trigger)\n',
            ],
        ],
        killedBy: '#384 R3b ',
    },
    // --- Deadline -----------------------------------------------------------
    {
        label: 'K10 — the clean clause dropped from the end site',
        file: REDIS,
        edits: [[CLEAN, "                const clean = outcome === 'ok'\n"]],
        killedBy: '#384 R4 ',
    },
    {
        label: 'K11 — the all-failed rule',
        file: REDIS,
        edits: [[
            CLEAN,
            CLEAN.replace(
                '(pass.failures ?? 0) === 0',
                '(pass.failures ?? 0) < (pass.attempts ?? 1)',
            ),
        ]],
        killedBy: '#384 R5 ',
    },
    {
        label: 'K12 — a pass with no tally counted as failed (?? 1)',
        file: REDIS,
        edits: [[
            CLEAN,
            CLEAN.replace(
                '(pass.failures ?? 0) === 0',
                '(pass.failures ?? 1) === 0',
            ),
        ]],
        killedBy: '#384 R7 ',
    },
    {
        label: 'K17 — the MISSED premise reverted',
        file: DEADLINE,
        edits: [[
            'export const REVOCATION_DEADLINE_MISSED =\n' +
            "    'realtime: revocation deadline MISSED (#362): no revocation pass ' +\n" +
            "    'completed without failures within revocationTtlSeconds of the last ' +\n" +
            '    "clean pass\'s start"\n',
            'export const REVOCATION_DEADLINE_MISSED =\n' +
            "    'realtime: revocation deadline MISSED (#362): no revocation pass ' +\n" +
            '    "completed within revocationTtlSeconds of the last success\'s start"\n',
        ]],
        killedBy: '#384 R8 ',
    },
    {
        label: 'K18 — passEnded() a no-op',
        file: DEADLINE,
        edits: [[
            '    passEnded(): void {\n        this.#ended = true\n    }\n',
            '    passEnded(): void {}\n',
        ]],
        killedBy: '#384 R9 ',
    },
    // --- Sweep --------------------------------------------------------------
    {
        label: "K13 — #sweepInstance's failure not counted",
        file: REDIS,
        edits: [[SWEEP_FAILURE, '']],
        killedBy: '#384 S1 ',
    },
    {
        label: 'K14 — the sweep failure counted after the WARN',
        file: REDIS,
        edits: [
            [SWEEP_FAILURE, ''],
            [
                SWEEP_FAILED_WARN_TAIL,
                SWEEP_FAILED_WARN_TAIL +
                '            if (this.#sweepPass) this.#sweepPass.failures++\n',
            ],
        ],
        killedBy: '#384 S1b ',
    },
    {
        label: "K15 — attempts counted in #reconcile's loop for every peer " +
            'it probes, live ones included',
        file: REDIS,
        edits: [
            [SWEEP_ATTEMPT, ''],
            [
                RECONCILE_SELF_SKIP,
                RECONCILE_SELF_SKIP +
                '                if (this.#sweepPass) this.#sweepPass.attempts++\n',
            ],
        ],
        killedBy: '#384 S1 ',
    },
    {
        label: 'K16 — renewed counted as a failure',
        file: REDIS,
        edits: [[
            "        } else if (end === 'renewed') {\n",
            "        } else if (end === 'renewed') {\n" +
            '            if (this.#sweepPass) this.#sweepPass.failures++\n',
        ]],
        killedBy: '#384 S3 ',
    },
    // --- Review additions -----------------------------------------------------
    {
        label: "K23 — the decoder's failed < 0 check dropped",
        file: REDIS,
        edits: [[
            RANGE,
            '        if (counts.failed > counts.attempted) {\n',
        ]],
        killedBy: '#384 R3b ',
    },
    {
        label: 'K24 — passSucceeded does not forget a pass that ended',
        file: DEADLINE,
        edits: [[
            '    ): void {\n' +
            '        this.#ended = false\n' +
            '        const previous = this.#previousReadAt\n',
            '    ): void {\n' +
            '        const previous = this.#previousReadAt\n',
        ]],
        killedBy: '#384 R10 ',
    },
    {
        label: 'K25 — the malformed WARN as a bare console.warn (row 5a)',
        file: REDIS,
        edits: [[
            MALFORMED_WRITE,
            "            'packages/realtime/driver.ts).'\n" +
            '        console.warn(line)\n',
        ]],
        killedBy: '#384 R3c ',
    },
    {
        label:
            'K26 — the resolved value rendered into the malformed WARN (row 5a)',
        file: REDIS,
        edits: [
            [
                DECODE,
                '            const value = await this.revocationHandler()\n' +
                '            const tally = decodeRevocationTally(value)\n',
            ],
            [
                WARN_MALFORMED,
                '                this.#warnMalformedTally(trigger, value)\n',
            ],
            [
                "    #warnMalformedTally(trigger: PassSample['trigger']): void {\n",
                '    #warnMalformedTally(\n' +
                "        trigger: PassSample['trigger'],\n" +
                '        value?: unknown,\n' +
                '    ): void {\n',
            ],
            [
                "            'packages/realtime/driver.ts).'\n",
                "            'packages/realtime/driver.ts). ' + String(JSON.stringify(value))\n",
            ],
        ],
        killedBy: '#384 R3b ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#384 — the revocation tally, the sample counts and the ' +
                    'clean-pass deadline',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
