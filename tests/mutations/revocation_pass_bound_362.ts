/**
 * @fileoverview #362's mutation battery — the Redis revocation pass is
 * bounded: a timing the driver cannot enforce refuses to boot, and a broken
 * enforcement guarantee is never silent.
 *
 * The decisions live in these homes (plan §5):
 * - `drivers/redis.ts`: the constructor's range guard and relation (N1–N8,
 *   N21–N24), the deadline's `inFlight` getter (N9), the pass's end site
 *   (N11, N13–N15), `close()` (N16), the first-registration arm (N17, N20),
 *   and `#passClock` (N18);
 * - `drivers/enforcement_deadline.ts`: the one-shot fire (N10), the
 *   success re-arm (N12), the arm-time `MISSED` (N19), the broker-clock
 *   check (N25–N27), the #369 fallback (N28) and the arm after a `SKEWED`
 *   line (N30); the pass chain's final handler is in `redis.ts` (N29).
 *
 * N30–N34 are not in the plan's table. N30 and N31 pin the architect's
 * ruling on the fix cycle: a decided line is carried through `arm()` —
 * written on a 0 ms timer, then the remaining time armed — and only
 * `close()` drops it. **Correction (#383, 2026-09-26):** this comment and
 * plan §11 previously said that carry covered "a decided `MISSED` or
 * `SKEWED`" on the strength of D7 (iv)/D7 (v) alone — those two witnesses
 * exercise only the `SKEWED` half. The overdue-`MISSED` half of the SAME
 * carry had no witness until #383's D4c/N38. N32–N34 are review findings:
 * the first-registration gate's `#closing` half, the `'failed'` default of
 * an unrecorded outcome, and `Deno.unrefTimer`.
 *
 * N35–N39 are #383's fix-cycle rows (the architect's disposition on that
 * issue): the `SKEWED` comparison scoped to the pass's own
 * `RevocationPassRecord.readAt` (N35, N36), `#warnReconcileFailed`'s
 * self-guard (N37), the overdue-`MISSED` carry (N38), and the one-line-per-
 * episode gate on a coinciding `SKEWED` (N39). **N37's self-guard closes off
 * the one path D8 (ii) used to reach `#runRevocationReconcile`'s outer
 * rejection** — N29 and N33 now `SURVIVED*` for that reason, recorded below
 * with why, not silently dropped.
 *
 * `killedBy` strings end in a space, or in a closing parenthesis, where a
 * shorter witness id is a prefix of a longer one (`D4 ` vs `D4b`, and now
 * `D4c`, which is a prefix of neither).
 *
 * SUITES now runs TWO files: this battery's own, and #383's
 * `revocation_lastreadat_383.test.ts`, whose (i)/(ii)/(iii) attribute N35 and
 * N36.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_pass_bound_362
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_pass_bound_362
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const DEADLINE = new URL(
    '../../drivers/enforcement_deadline.ts',
    import.meta.url,
)
const SUITES = [
    new URL('../revocation_pass_bound_362.test.ts', import.meta.url).pathname,
    // #383's own suite: N35 and N36 (item 1) are attributed to its (i)/(ii).
    new URL('../revocation_lastreadat_383.test.ts', import.meta.url).pathname,
]

const RELATION =
    '        if (this.reconcileIntervalMs * 2 > this.revocationTtlSeconds * 1000) {\n'
// Re-anchored for #384: the gate reads the end site's clean-pass condition,
// which `outcome === 'ok'` joined. N11, N13 and N15 keep their meaning over it.
const END_GATE = '                if (clean && !this.#closing) {\n'
const FIRST_GATE = '        if (first && !this.#closing) {\n'
const SKEW_TEST = '            readAt - previous >= this.#ttlMs / 1000\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'N1 the relation check removed',
        file: REDIS,
        edits: [[RELATION, '        if (false) {\n']],
        killedBy: '#362 B1 ',
    },
    {
        label: 'N2 the factor dropped',
        file: REDIS,
        edits: [[
            RELATION,
            '        if (this.reconcileIntervalMs > this.revocationTtlSeconds * 1000) {\n',
        ]],
        killedBy: '#362 B1 ',
    },
    {
        label: 'N3 the relation made inclusive (>=)',
        file: REDIS,
        edits: [[
            RELATION,
            '        if (this.reconcileIntervalMs * 2 >= this.revocationTtlSeconds * 1000) {\n',
        ]],
        killedBy: '#362 B2 ',
    },
    {
        label: 'N4 * 1000 dropped from the TTL side',
        file: REDIS,
        edits: [[
            RELATION,
            '        if (this.reconcileIntervalMs * 2 > this.revocationTtlSeconds) {\n',
        ]],
        killedBy: '#362 B3 ',
    },
    {
        label: "N5 the interval's finiteness clause dropped",
        file: REDIS,
        edits: [[
            '            !Number.isFinite(this.reconcileIntervalMs) ||\n',
            '',
        ]],
        killedBy: '#362 B4 ',
    },
    {
        label: "N6 the interval's < 1 clause dropped",
        file: REDIS,
        edits: [['            this.reconcileIntervalMs < 1 ||\n', '']],
        killedBy: '#362 B4 ',
    },
    {
        label: "N7 the TTL's safe-integer clause dropped",
        file: REDIS,
        edits: [[
            '            !Number.isSafeInteger(this.revocationTtlSeconds) ||\n',
            '',
        ]],
        killedBy: '#362 B5 ',
    },
    {
        label: "N8 the TTL's < 1 clause dropped",
        file: REDIS,
        edits: [['            this.revocationTtlSeconds < 1 ||\n', '']],
        killedBy: '#362 B5 ',
    },
    {
        label: 'N9 the fire abandons the pass (frees the slot, arms the timer)',
        file: REDIS,
        edits: [[
            '            inFlight: () => this.#revocationPass,\n',
            '            inFlight: () => {\n' +
            '                const pass = this.#revocationPass\n' +
            '                this.#revocationPass = undefined\n' +
            '                this.#armRevocationReconcile()\n' +
            '                return pass\n' +
            '            },\n',
        ]],
        killedBy: '#362 D1 ',
    },
    {
        label: 'N10 the fire re-arms itself (a WARN per TTL)',
        file: DEADLINE,
        edits: [[
            '            this.#setTimer(delayMs, () => this.#write(this.#expired()))\n',
            '            this.#setTimer(delayMs, () => {\n' +
            '                this.#write(this.#expired())\n' +
            '                this.arm(this.#ttlMs)\n' +
            '            })\n',
        ]],
        killedBy: '#362 D1 ',
    },
    {
        label: 'N11 a failed pass also re-arms the deadline',
        file: REDIS,
        edits: [[
            END_GATE,
            "                if (outcome !== 'closed' && !this.#closing) {\n",
        ]],
        killedBy: '#362 D2 ',
    },
    {
        label: 'N12 passSucceeded ignored once the deadline has fired',
        file: DEADLINE,
        edits: [[
            '        const previous = this.#previousReadAt\n',
            '        if (this.#timer === undefined) return\n' +
            '        const previous = this.#previousReadAt\n',
        ]],
        killedBy: '#362 D2 ',
    },
    {
        label: 'N13 the end site never calls passSucceeded',
        file: REDIS,
        edits: [[
            END_GATE,
            '                if (clean && !this.#closing && false) {\n',
        ]],
        killedBy: '#362 D3 ',
    },
    {
        label: 'N14 the deadline anchored at the end of the pass',
        file: REDIS,
        // Re-anchored for #383 (row 18 became `pass.readAt`, not
        // `this.#lastReadAt`): the anchor text moved, the meaning — the
        // deadline's START argument — did not.
        edits: [[
            '                        startedAt,\n' +
            '                        endedAt,\n' +
            '                        pass.readAt,\n',
            '                        endedAt,\n' +
            '                        endedAt,\n' +
            '                        pass.readAt,\n',
        ]],
        killedBy: '#362 D4 (a)',
    },
    {
        label: "N15 the end site's #closing gate dropped",
        file: REDIS,
        edits: [[END_GATE, '                if (clean) {\n']],
        killedBy: '#362 D5 (i)',
    },
    {
        label: 'N16 close() does not clear the deadline',
        file: REDIS,
        edits: [['        this.#deadline.close()\n', '']],
        killedBy: '#362 D5 (ii)',
    },
    {
        label: 'N17 onRevocationReconcile does not arm the deadline',
        file: REDIS,
        edits: [[
            '            this.#deadline.arm(this.revocationTtlSeconds * 1000)\n',
            '',
        ]],
        killedBy: '#362 D1 ',
    },
    {
        label: 'N18 #passClock reads the epoch clock',
        file: REDIS,
        edits: [[
            '        return performance.now()\n',
            '        return Date.now()\n',
        ]],
        killedBy: '#362 D6 ',
    },
    {
        label: 'N19 an overdue arm decides at fire, consulting inFlight()',
        file: DEADLINE,
        edits: [[
            '        this.#unwritten.push(this.#missed())\n' +
            '        this.#setTimer(0, () => this.#flush())\n',
            '        this.#setTimer(0, () => this.#write(this.#expired()))\n',
        ]],
        killedBy: '#362 D4b ',
    },
    {
        label: 'N20 every registration re-arms the deadline',
        file: REDIS,
        edits: [[
            FIRST_GATE,
            '        if ((first || true) && !this.#closing) {\n',
        ]],
        killedBy: '#362 D5 (iv)',
    },
    {
        label: "N21 the TTL's safe-integer clause weakened to isFinite",
        file: REDIS,
        edits: [[
            '            !Number.isSafeInteger(this.revocationTtlSeconds) ||\n',
            '            !Number.isFinite(this.revocationTtlSeconds) ||\n',
        ]],
        killedBy: '#362 B6 ',
    },
    {
        label: "N22 the TTL's upper bound dropped",
        file: REDIS,
        edits: [[
            // Re-anchored by #380, which hoisted the constructor's local to the
            // module constant MAX_REVOCATION_TTL_SECONDS: the source moved, the
            // guard remains.
            '            this.revocationTtlSeconds < 1 ||\n' +
            '            this.revocationTtlSeconds > MAX_REVOCATION_TTL_SECONDS\n',
            '            this.revocationTtlSeconds < 1\n',
        ]],
        killedBy: '#362 B6 ',
    },
    {
        label: "N23 the TTL's upper bound off by one (>=)",
        file: REDIS,
        edits: [[
            '            this.revocationTtlSeconds > MAX_REVOCATION_TTL_SECONDS\n',
            '            this.revocationTtlSeconds >= MAX_REVOCATION_TTL_SECONDS\n',
        ]],
        killedBy: '#362 B6 ',
    },
    {
        label: "N24 the interval's < 1 weakened to <= 0",
        file: REDIS,
        edits: [[
            '            this.reconcileIntervalMs < 1 ||\n',
            '            this.reconcileIntervalMs <= 0 ||\n',
        ]],
        killedBy: '#362 B6 ',
    },
    {
        label: 'N25 the broker-clock check removed',
        file: DEADLINE,
        edits: [[
            SKEW_TEST,
            '            readAt - previous >= this.#ttlMs / 1000 && false\n',
        ]],
        killedBy: '#362 D7 (i)',
    },
    {
        label: 'N26 the broker-clock check made strict (>)',
        file: DEADLINE,
        edits: [[
            SKEW_TEST,
            '            readAt - previous > this.#ttlMs / 1000\n',
        ]],
        killedBy: '#362 D7 (i)',
    },
    {
        label: "N27 the broker-clock check's pending condition dropped",
        file: DEADLINE,
        edits: [['            this.#timer !== undefined &&\n', '']],
        killedBy: '#362 D7 (iii)',
    },
    {
        label: "N28 the fire's #369 fallback removed (a bare console.warn)",
        file: DEADLINE,
        edits: [[
            '        try {\n' +
            '            console.warn(text)\n' +
            '        } catch (failure) {\n' +
            '            writeMarkedFallback(REVOCATION_LOG_FAILED, text, {\n' +
            "                label: 'sink failure',\n" +
            '                error: failure,\n' +
            '            })\n' +
            '        }\n',
            '        console.warn(text)\n',
        ]],
        killedBy: '#362 D8 (i)',
    },
    {
        label: "N29 the pass chain's final rejection handler removed",
        file: REDIS,
        edits: [[
            '            })\n' +
            '            .catch((error: unknown) => {\n' +
            '                // #369: nothing escapes the pass chain. A rejection reaches\n' +
            '                // here only when a log sink itself threw (#349); the marker\n' +
            '                // is the fixed prefix, the rejection is rendered, and the\n' +
            '                // line never throws past itself either (#391).\n' +
            '                writeMarkedFallback(REVOCATION_LOG_FAILED, error)\n' +
            '            })\n',
            '            })\n',
        ]],
        killedBy: '#362 D8 (ii)',
        expectSurvival:
            'SURVIVES since #383 item 2: #warnReconcileFailed self-guards ' +
            "every WARN inside #runRevocationReconcile's catch (the ONE " +
            'path that used to let a throwing console.warn reject the ' +
            "promise past D8 (ii)'s handler-rejects scenario). Nothing " +
            'inside #runRevocationReconcile can escape it any more, so the ' +
            'outer .catch stays defence-in-depth for a throw this suite does ' +
            'not currently model — ADR 011 §2 says so.',
    },
    {
        label: 'N30 a carried line is not followed by the remaining arm',
        file: DEADLINE,
        edits: [[
            '                this.arm(delayMs - (this.#now() - armedAt))\n',
            '',
        ]],
        killedBy: '#362 D7 (iv)',
    },
    {
        label: 'N31 the carry dropped: passSucceeded erases a decided line',
        file: DEADLINE,
        // Re-anchored for #383 item 4 (second-review finding): mutating
        // arm()'s own `#unwritten.length > 0` check erased a line THIS
        // call's own SKEWED push had just made, before arm() ever saw it —
        // so the old anchor killed D7 (i) and D7 (iv) too, not only D7 (v).
        // Moved to the top of passSucceeded, it can only erase a line
        // CARRIED IN from an earlier call — the one property D7 (v) tests.
        // #383's D4c and D7 (vi) test that SAME property for MISSED and for
        // the coinciding case, so both also die here; that is not
        // over-kill, since all three share the one defect this row plants.
        edits: [[
            '        this.#ended = false\n' +
            '        const previous = this.#previousReadAt\n',
            '        this.#unwritten = []\n' +
            '        this.#ended = false\n' +
            '        const previous = this.#previousReadAt\n',
        ]],
        killedBy: '#362 D7 (v)',
    },
    {
        label: "N32 the first-registration gate's #closing half dropped",
        file: REDIS,
        edits: [[FIRST_GATE, '        if (first) {\n']],
        killedBy: '#362 D5 (iii)',
    },
    {
        label: "N33 an unrecorded outcome defaults to 'ok'",
        file: REDIS,
        edits: [[
            // Re-anchored for #360: the alias became `PassOutcome`, and the
            // sweep's start site has the same line at 12 spaces — which
            // contains this one — so the anchor carries the record line above.
            '        this.#revocationPass = pass\n' +
            "        let outcome: PassOutcome = 'failed'\n",
            '        this.#revocationPass = pass\n' +
            "        let outcome: PassOutcome = 'ok'\n",
        ]],
        killedBy: '#362 D8 (ii)',
        expectSurvival: 'SURVIVES since #383 item 2, the same reason as N29: ' +
            "#runRevocationReconcile no longer rejects in D8 (ii)'s " +
            'scenario, so `.then()` always runs and overwrites `outcome` ' +
            'explicitly — the DEFAULT this row mutates is never read by ' +
            'this suite any more.',
    },
    {
        label: "N34 the deadline timer is not unref'd",
        file: DEADLINE,
        edits: [['        Deno.unrefTimer(id)\n', '']],
        killedBy: '#362 D9 ',
    },
    {
        label: 'N35 readAt never written on the pass (a no-op)',
        file: REDIS,
        // #383 item 1. This turns SKEWED off outright (readAt always
        // undefined), so it kills (ii) — the "still raises SKEWED" control
        // — rather than proving the fix; a weak kill on its own, measured,
        // which is why N36 exists beside it to prove the real defect.
        edits: [[
            '        if (this.#revocationPass) this.#revocationPass.readAt = t\n',
            '        if (this.#revocationPass) this.#revocationPass.readAt = undefined\n',
        ]],
        killedBy: '#383 (ii)',
    },
    {
        label: 'N36 the end site reverted to reading #lastReadAt',
        file: REDIS,
        // #383 item 1: the original bug — a driver-level handler that skips
        // enumeration on some passes compares a fresh readAt against a
        // STALE this.#lastReadAt left by an unrelated earlier pass.
        edits: [[
            '                        pass.readAt,\n',
            '                        this.#lastReadAt,\n',
        ]],
        killedBy: '#383 (i)',
    },
    {
        label: 'N37 #warnReconcileFailed reverted to a bare console.warn',
        file: REDIS,
        // #383 item 2. Reverting the self-guard reopens the pre-#383 defect:
        // the throwing console.warn escapes #runRevocationReconcile's catch
        // before either D8 (ii)'s new assertion or the #308 retry runs.
        //
        // Re-anchored for #409: #warnReconcileFailed's own try/catch moved
        // into a call to #guardedWarn, the one helper it now shares with
        // #warnMalformedTally (same marker, same `line` — the next method's
        // JSDoc still disambiguates which call this row mutates).
        edits: [[
            '        this.#guardedWarn(REVOCATION_LOG_FAILED, line)\n' +
            '    }\n' +
            '\n' +
            '    /**\n' +
            '     * Write the one {@link REVOCATION_TALLY_MALFORMED} WARN of a pass whose\n',
            '        console.warn(line)\n' +
            '    }\n' +
            '\n' +
            '    /**\n' +
            '     * Write the one {@link REVOCATION_TALLY_MALFORMED} WARN of a pass whose\n',
        ]],
        killedBy: '#362 D8 (ii)',
    },
    {
        label: 'N38 the overdue-MISSED carry reverted to a direct write',
        file: DEADLINE,
        // #383 item 3 (second-review MEDIUM): D4b alone let this survive —
        // it holds the trailing pass, so nothing races the pending 0 ms
        // timer before it fires either way. D4c does not hold it.
        edits: [[
            '        this.#unwritten.push(this.#missed())\n' +
            '        this.#setTimer(0, () => this.#flush())\n',
            '        this.#setTimer(0, () => this.#write(this.#missed()))\n',
        ]],
        killedBy: '#362 D4c ',
    },
    {
        label: 'N39 the #383 no-second-line gate dropped',
        file: DEADLINE,
        edits: [[
            '            this.#timer !== undefined &&\n' +
            '            this.#unwritten.length === 0 &&\n',
            '            this.#timer !== undefined &&\n',
        ]],
        killedBy: '#362 D7 (vi)',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#362 — the revocation pass is bounded, and a broken ' +
                    'enforcement guarantee is never silent',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
