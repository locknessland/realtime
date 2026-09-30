/**
 * @fileoverview #360's mutation battery — one frozen pass sample per completed
 * ghost sweep and revocation pass, taken at the pass's one end site from the
 * start site's closure, delivered unawaited and contained.
 *
 * The decisions live in `drivers/redis.ts`: `#emitPassSample` (the gate, the
 * freeze, the one `try` around the call and the adoption, the #369 fallback),
 * the two end sites, the two page increments, `#reconcile`'s outcome and its
 * reply decoders, and the sweep chain's final `.catch`. Each row drops one
 * clause a refactor could drop while the rest of the suite stays green.
 *
 * - M1 / M3 a page increment removed (revocation / sweep). M2 the revocation
 *   count kept in a field never reset per pass. M4 the sweep count reset per
 *   instance.
 * - M5 the revocation end site reports `ok` whatever happened. M6
 *   `#reconcile`'s catch returns `ok`. M7 the revocation trigger hard-coded.
 *   M8 the end site reads `#revocationPass` instead of its closure (D4).
 * - M9 the `#closing` gate removed. M10 the `try` around the call removed.
 *   M11 no rejection handler on what the handler returned. M12 the #369
 *   fallback removed. M13 the handler's promise awaited by the pass chain.
 * - M14 the sweep's start read on the epoch clock. M15 the handler captured
 *   at the start. M16 the sample not frozen. M17 a failure contained to one
 *   instance fails the pass. M18 the sweep sample before the re-arm, with M12.
 * - M19 the sweep chain's final `.catch` removed (A1). M20 the sweep's
 *   unrecorded outcome `ok`. M21 the instance-set decoder bypassed (S2). M22
 *   the adoption moved out of the `try` (S3). M23 a duck-typed `then` (S3).
 *
 * **M13 is attributed to P8 (ii), not the plan's P8 (iv).** Measured: under
 * FR-009's order the sweep's `finally` clears `#reconcilePass` BEFORE it
 * samples, and the `#closing` gate calls no handler once `close()` began, so
 * `close()` never awaits a pass whose handler was called — a handler that
 * never settles cannot hold it, and P8 (iv) stays green under M13. What the
 * await does change is the chain's settlement: a handler's rejection then
 * reaches the chain's final handler, a marked ERROR line P8 (ii) asserts
 * absent. P8 (iv) remains, as the pin that `close()` resolves.
 *
 * **M18 is attributed to P14, not P8 (iii)** (#386, the #360 review's item 1).
 * M18 bundles the reorder with M12's own edit (the #369 fallback removed), and
 * M12 alone already fails P8 (iii) — on a marked-line assertion, whatever the
 * order is — so a kill there never proved the order was what P8 (iii)
 * exercised. P14 has one assertion, the sweep's re-arm count, under the same
 * throwing handler and throwing `console.warn`; a kill there is the order and
 * nothing else.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate pass_sample_360
 * ```
 *
 * @module @lockness/realtime/tests/mutations/pass_sample_360
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../pass_sample_360.test.ts', import.meta.url).pathname,
]

/** The revocation page increment, in `listRevocations`. */
const REVOCATION_PAGE =
    '            if (this.#revocationPass) this.#revocationPass.pages++\n'

/** The sweep page increment, in `#sweepOwned`. */
const SWEEP_PAGE = '            if (this.#sweepPass) this.#sweepPass.pages++\n'

/** The one field block line a mutant can declare a field beside. */
const HANDLER_FIELD =
    '    #passCompleteHandler?: (sample: PassSample) => void\n'

/**
 * The revocation end site's sample, from the start site's closure.
 * Re-anchored for #384: the call also passes the record's counts.
 */
const REVOCATION_EMIT = '                this.#emitPassSample(\n' +
    "                    'revocation',\n" +
    '                    trigger,\n' +
    '                    outcome,\n' +
    '                    startedAt,\n' +
    '                    endedAt,\n' +
    '                    pass.pages,\n' +
    '                    pass.attempts,\n' +
    '                    pass.failures,\n' +
    '                )\n'

/**
 * The sweep's `finally`, in its decided order. Re-anchored for #384: the call
 * also passes the record's counts.
 */
const SWEEP_FINALLY =
    '                    const endedAt = this.#passClock()\n' +
    '                    this.#reconcilePass = undefined\n' +
    '                    this.#armReconcile()\n' +
    '                    this.#sweepPass = undefined\n' +
    '                    this.#emitPassSample(\n' +
    "                        'sweep',\n" +
    "                        'timer',\n" +
    '                        outcome,\n' +
    '                        pass.startedAt,\n' +
    '                        endedAt,\n' +
    '                        pass.pages,\n' +
    '                        pass.attempts,\n' +
    '                        pass.failures,\n' +
    '                    )\n'

/** The gate and the handler read, at the top of `#emitPassSample`. */
const GATE = '        if (this.#closing) return\n' +
    '        const handler = this.#passCompleteHandler\n'

/** The adoption of what the handler returned. */
const ADOPT = '            Promise.resolve(returned).then(\n' +
    '                undefined,\n' +
    '                (failure: unknown) => this.#warnPassSample(failure),\n' +
    '            )\n'

/** The call and the adoption, in their one `try`. */
const CALL = '        try {\n' +
    '            const returned: unknown = handler(sample)\n' +
    ADOPT +
    '        } catch (failure) {\n' +
    '            this.#warnPassSample(failure)\n' +
    '        }\n'

/**
 * `#warnPassSample`'s #369-shaped body. Re-anchored for #409: the try/catch
 * moved into a call to #guardedWarn, the one helper every self-guarded WARN
 * in the file now shares.
 */
const WARN_BODY = '        this.#guardedWarn(\n' +
    '            PASS_SAMPLE_LOG_FAILED,\n' +
    '            `${PASS_SAMPLE_FAILED} ${renderError(failure)}`,\n' +
    '            failure,\n' +
    '        )\n'
const BARE_WARN =
    '        console.warn(`${PASS_SAMPLE_FAILED} ${renderError(failure)}`)\n'

/** The sweep chain's final handler (A1). */
const SWEEP_CATCH = '                })\n' +
    '                .catch((error: unknown) => {\n' +
    '                    // #360 A1, the #369 rule: nothing escapes the sweep chain.\n' +
    '                    // A rejection reaches here only when a log sink threw\n' +
    '                    // inside the pass; the marker is the fixed prefix, and\n' +
    '                    // the line never throws past itself either (#391).\n' +
    '                    writeMarkedFallback(SWEEP_LOG_FAILED, error)\n' +
    '                })\n'

/** The revocation end site's sample, with its arguments replaced. */
const revocationEmit = (trigger: string, outcome: string, pages: string) =>
    '                this.#emitPassSample(\n' +
    "                    'revocation',\n" +
    `                    ${trigger},\n` +
    `                    ${outcome},\n` +
    '                    startedAt,\n' +
    '                    endedAt,\n' +
    `                    ${pages},\n` +
    '                    pass.attempts,\n' +
    '                    pass.failures,\n' +
    '                )\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the listRevocations page increment removed',
        file: REDIS,
        edits: [[REVOCATION_PAGE, '']],
        killedBy: '#360 P1 ',
    },
    {
        label: 'M2 — the revocation page count kept in a field never reset',
        file: REDIS,
        edits: [
            [HANDLER_FIELD, HANDLER_FIELD + '    #pagesEver = 0\n'],
            [
                REVOCATION_PAGE,
                '            this.#pagesEver++\n' +
                '            if (this.#revocationPass) {\n' +
                '                this.#revocationPass.pages = this.#pagesEver\n' +
                '            }\n',
            ],
        ],
        killedBy: '#360 P2 ',
    },
    {
        label: 'M3 — the #sweepOwned page increment removed',
        file: REDIS,
        edits: [[SWEEP_PAGE, '']],
        killedBy: '#360 P3 (i)',
    },
    {
        label: 'M4 — the sweep page count reset per instance',
        file: REDIS,
        edits: [[
            '    async #sweepInstance(deadId: string): Promise<void> {\n',
            '    async #sweepInstance(deadId: string): Promise<void> {\n' +
            '        if (this.#sweepPass) this.#sweepPass.pages = 0\n',
        ]],
        killedBy: '#360 P3 (iii)',
    },
    {
        label: "M5 — the revocation end site reports 'ok' whatever happened",
        file: REDIS,
        edits: [[
            REVOCATION_EMIT,
            revocationEmit('trigger', "'ok'", 'pass.pages'),
        ]],
        killedBy: '#360 P4 (i)',
    },
    {
        label: "M6 — #reconcile's catch returns 'ok'",
        file: REDIS,
        edits: [[
            '                `realtime: roster reconcile failed: ${renderError(error)}`,\n' +
            '            )\n' +
            "            return 'failed'\n",
            '                `realtime: roster reconcile failed: ${renderError(error)}`,\n' +
            '            )\n' +
            "            return 'ok'\n",
        ]],
        killedBy: '#360 P4 (ii)',
    },
    {
        label: "M7 — the revocation sample's trigger hard-coded 'timer'",
        file: REDIS,
        edits: [[
            REVOCATION_EMIT,
            revocationEmit("'timer'", 'outcome', 'pass.pages'),
        ]],
        killedBy: '#360 P5 ',
    },
    {
        label: 'M8 — the revocation end site reads #revocationPass (D4)',
        file: REDIS,
        edits: [[
            REVOCATION_EMIT,
            revocationEmit(
                '(this.#revocationPass ?? pass).trigger',
                'outcome',
                '(this.#revocationPass ?? pass).pages',
            ),
        ]],
        killedBy: '#360 P6 ',
    },
    {
        label: "M9 — #emitPassSample's #closing gate removed",
        file: REDIS,
        edits: [[GATE, '        const handler = this.#passCompleteHandler\n']],
        killedBy: '#360 P7 ',
    },
    {
        label: "M10 — the handler call's try/catch removed",
        file: REDIS,
        edits: [[
            CALL,
            '        const returned: unknown = handler(sample)\n' +
            '        Promise.resolve(returned).then(\n' +
            '            undefined,\n' +
            '            (failure: unknown) => this.#warnPassSample(failure),\n' +
            '        )\n',
        ]],
        killedBy: '#360 P8 (i)',
    },
    {
        label: 'M11 — no rejection handler on what the handler returned',
        file: REDIS,
        edits: [[ADOPT, '            void returned\n']],
        killedBy: '#360 P8 (ii)',
    },
    {
        label: 'M12 — the #369 fallback removed: a bare console.warn',
        file: REDIS,
        edits: [[WARN_BODY, BARE_WARN]],
        killedBy: '#360 P8 (iii)',
    },
    {
        label: 'M13 — the handler awaited: the chains return its promise',
        file: REDIS,
        edits: [
            [
                '    ): void {\n' + GATE,
                '    ): unknown {\n' + GATE,
            ],
            [ADOPT, ADOPT + '            return returned\n'],
            [
                '                    this.#emitPassSample(\n' +
                "                        'sweep',\n",
                '                    return this.#emitPassSample(\n' +
                "                        'sweep',\n",
            ],
            [
                '                this.#emitPassSample(\n' +
                "                    'revocation',\n",
                '                return this.#emitPassSample(\n' +
                "                    'revocation',\n",
            ],
        ],
        // Measured, not the plan's P8 (iv): see the header.
        killedBy: '#360 P8 (ii)',
    },
    {
        label: "M14 — the sweep's start read on the epoch clock",
        file: REDIS,
        // Re-anchored for #384: the record literal also carries the counts.
        edits: [[
            '                startedAt: this.#passClock(),\n' +
            '                pages: 0,\n' +
            '                attempts: 0,\n',
            '                startedAt: this.now(),\n' +
            '                pages: 0,\n' +
            '                attempts: 0,\n',
        ]],
        killedBy: '#360 P9 (sweep)',
    },
    {
        label: 'M15 — the handler captured at the start site',
        file: REDIS,
        edits: [
            [
                HANDLER_FIELD,
                HANDLER_FIELD +
                '    #startHandler?: (sample: PassSample) => void\n',
            ],
            [
                '        this.#revocationPass = pass\n',
                '        this.#revocationPass = pass\n' +
                '        this.#startHandler = this.#passCompleteHandler\n',
            ],
            [
                GATE,
                '        if (this.#closing) return\n' +
                '        const handler = this.#startHandler\n',
            ],
        ],
        killedBy: '#360 P10 (i)',
    },
    {
        label: 'M16 — the sample not frozen',
        file: REDIS,
        edits: [[
            '        const sample: PassSample = Object.freeze({\n',
            '        const sample: PassSample = ({\n',
        ]],
        killedBy: '#360 P1 ',
    },
    {
        label: "M17 — #sweepInstance's contained failure fails the pass",
        file: REDIS,
        edits: [[
            '            end = await this.#sweepOwned(deadId, count)\n' +
            '        } catch (error) {\n' +
            '            end = { failed: error }\n' +
            '        }\n',
            '            end = await this.#sweepOwned(deadId, count)\n' +
            '        } catch (error) {\n' +
            '            end = { failed: error }\n' +
            // Opaque to the checker, so `end` keeps its failed arm and the
            // mutant type-checks; every thrown value is rethrown.
            '            if (error !== Symbol.for("#360 M17")) throw error\n' +
            '        }\n',
        ]],
        killedBy: '#360 P4 (iii)',
    },
    {
        label: 'M18 — the sweep sample before the re-arm, with M12 applied',
        file: REDIS,
        edits: [
            [
                SWEEP_FINALLY,
                '                    const endedAt = this.#passClock()\n' +
                '                    this.#reconcilePass = undefined\n' +
                '                    this.#sweepPass = undefined\n' +
                '                    this.#emitPassSample(\n' +
                "                        'sweep',\n" +
                "                        'timer',\n" +
                '                        outcome,\n' +
                '                        pass.startedAt,\n' +
                '                        endedAt,\n' +
                '                        pass.pages,\n' +
                '                        pass.attempts,\n' +
                '                        pass.failures,\n' +
                '                    )\n' +
                '                    this.#armReconcile()\n',
            ],
            [WARN_BODY, BARE_WARN],
        ],
        // M18 bundles M12's edit, which alone already fails P8 (iii): see the
        // header. P14 asserts nothing but the re-arm.
        killedBy: '#360 P14 ',
    },
    {
        label: "M19 — the sweep chain's final .catch removed (A1)",
        file: REDIS,
        edits: [[SWEEP_CATCH, '                })\n']],
        killedBy: '#360 P12 (ii)',
    },
    {
        label: "M20 — the sweep's unrecorded outcome defaults to 'ok' (A1)",
        file: REDIS,
        edits: [[
            '            this.#sweepPass = pass\n' +
            "            let outcome: PassOutcome = 'failed'\n",
            '            this.#sweepPass = pass\n' +
            "            let outcome: PassOutcome = 'ok'\n",
        ]],
        killedBy: '#360 P12 (ii)',
    },
    {
        label: 'M21 — decodeMembersReply bypassed: `?? []` restored (S2)',
        file: REDIS,
        edits: [[
            '            const ids = decodeMembersReply(reply)\n',
            '            const ids = asArray(reply) ?? []\n',
        ]],
        killedBy: '#360 P4 (iv) (a)',
    },
    {
        label: 'M22 — the adoption moved out of the try (S3)',
        file: REDIS,
        edits: [[
            CALL,
            '        let returned: unknown\n' +
            '        try {\n' +
            '            returned = handler(sample)\n' +
            '        } catch (failure) {\n' +
            '            this.#warnPassSample(failure)\n' +
            '        }\n' +
            '        Promise.resolve(returned).then(\n' +
            '            undefined,\n' +
            '            (failure: unknown) => this.#warnPassSample(failure),\n' +
            '        )\n',
        ]],
        killedBy: '#360 P8 (v) (i)',
    },
    {
        label: 'M23 — a duck-typed then instead of Promise.resolve (S3)',
        file: REDIS,
        edits: [[
            ADOPT,
            '            const thenable = returned as { then?: unknown } | null\n' +
            "            if (typeof thenable?.then === 'function') {\n" +
            '                (returned as PromiseLike<unknown>).then(\n' +
            '                    undefined,\n' +
            '                    (failure: unknown) => this.#warnPassSample(failure),\n' +
            '                )\n' +
            '            }\n',
        ]],
        killedBy: '#360 P8 (v) (ii)',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#360 — one frozen pass sample per completed pass, taken at ' +
                    'its end site, delivered unawaited and contained',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
