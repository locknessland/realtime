/**
 * @fileoverview #395 (security review) mutation battery — the 3 additional
 * guarded WARNs the review found (one HIGH, two LOW) stay guarded, so a
 * throwing sink cannot cancel the work behind them.
 *
 * Each row removes one site's `try`/`catch`-and-`writeMarkedFallback` guard,
 * putting back the bare `console.warn` that let a throwing sink abort its
 * loop before the remaining work ran:
 *
 * - M1 (HIGH): `#recheckRevocations`' `apply` closure loses its guard — a
 *   throwing sink skips every revocation queued behind the failing one.
 * - M2 (LOW): `#teardownChannels`' "also failed" WARN loses its guard — a
 *   throwing sink skips a later channel's roster release / `left` / cap
 *   release.
 * - M3 (LOW): `emitPresence`'s per-connection delivery WARN loses its guard —
 *   a throwing sink aborts fan-out to the remaining local sockets.
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
 * deno run -A packages/realtime/tests/mutations/security_review_escape_395.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/security_review_escape_395
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../recheck_revocation_warn_395.test.ts', import.meta.url)
        .pathname,
    new URL('../teardown_channel_warn_395.test.ts', import.meta.url).pathname,
    new URL('../emit_presence_warn_395.test.ts', import.meta.url).pathname,
]

const W1 = '#395 (HIGH) #recheckRevocations: the second revocation is still ' +
    'applied when the wrapper WARN itself throws'
const W2 = '#395 (LOW) #teardownChannels: a later channel is still released ' +
    "when a middle failure's WARN itself throws"
const W3 = '#395 (LOW) emitPresence: a later local socket still receives the ' +
    "frame when an earlier socket's WARN itself throws"

const MUTATIONS: Mutation[] = [
    {
        label: "M1 (HIGH) — #recheckRevocations' wrapper WARN loses its " +
            'guard: a throwing sink skips every revocation queued behind ' +
            'the failing one',
        file: MANAGER,
        edits: [[
            '                try {\n' +
            '                    console.warn(\n' +
            "                        'realtime: a durable revocation could not be applied — ' +\n" +
            "                            'the reconcile goes on with the next one: ' +\n" +
            '                            renderError(error),\n' +
            '                    )\n' +
            '                } catch (sink) {\n' +
            '                    // #395 (security review HIGH): a throwing sink must not\n' +
            '                    // abort this `apply` — the loop above awaits it one\n' +
            '                    // revocation at a time, so an uncontained throw here would\n' +
            '                    // escape past this closure and skip every revocation\n' +
            "                    // still queued behind the failing one, breaking #349's\n" +
            '                    // "one revocation that throws never stops the ones after\n' +
            '                    // it". One marked line instead, which never throws (#391).\n' +
            '                    writeMarkedFallback(RECHECK_REVOCATION_LOG_FAILED, error, {\n' +
            "                        label: 'sink failure',\n" +
            '                        error: sink,\n' +
            '                    })\n' +
            '                }\n',
            '                // Mutant: the guard is gone — a throwing sink aborts the whole\n' +
            '                // reconcile pass here.\n' +
            '                console.warn(\n' +
            "                    'realtime: a durable revocation could not be applied — ' +\n" +
            "                        'the reconcile goes on with the next one: ' +\n" +
            '                        renderError(error),\n' +
            '                )\n',
        ]],
        // Witness: 'c2 is STILL applied, even though c1 (attempted before it)
        // failed twice over' — with the guard gone, the sink's own throw
        // escapes `apply`, aborts `#recheckRevocations`' loop, and c2 is
        // never reached.
        killedBy: W1,
    },
    {
        label: 'M2 (LOW) — #teardownChannels\' "also failed" WARN loses ' +
            'its guard: a throwing sink skips a later channel',
        file: MANAGER,
        edits: [[
            '                        try {\n' +
            '                            console.warn(\n' +
            '                                `realtime: tearing ${\n' +
            '                                    safeForLog(clientId)\n' +
            '                                } out of ` +\n' +
            '                                    `${safeForLog(channel)} also failed: ` +\n' +
            '                                    renderError(error),\n' +
            '                            )\n' +
            '                        } catch (sink) {\n' +
            '                            // #395 (security review LOW): a throwing sink\n' +
            "                            // must not abort this loop — a later channel's\n" +
            '                            // roster release, `left` announcement and cap\n' +
            '                            // release must still run. One marked line\n' +
            '                            // instead, which never throws (#391).\n' +
            '                            writeMarkedFallback(\n' +
            '                                TEARDOWN_CHANNEL_LOG_FAILED,\n' +
            '                                error,\n' +
            "                                { label: 'sink failure', error: sink },\n" +
            '                            )\n' +
            '                        }\n',
            '                        // Mutant: the guard is gone — a throwing sink aborts the\n' +
            '                        // whole teardown loop here.\n' +
            '                        console.warn(\n' +
            '                            `realtime: tearing ${\n' +
            '                                safeForLog(clientId)\n' +
            '                            } out of ` +\n' +
            '                                `${safeForLog(channel)} also failed: ` +\n' +
            '                                renderError(error),\n' +
            '                        )\n',
        ]],
        // Witness: "C's unwatch still ran even though B's own WARN threw" —
        // with the guard gone, the sink's own throw escapes the `for` loop
        // in `#teardownChannels`, and C is never reached.
        killedBy: W2,
    },
    {
        label: "M3 (LOW) — emitPresence's per-connection delivery WARN " +
            'loses its guard: a throwing sink aborts the rest of the ' +
            'fan-out',
        file: MANAGER,
        edits: [[
            '                try {\n' +
            '                    console.warn(\n' +
            '                        `realtime: a presence frame could not be delivered on ${\n' +
            '                            safeForLog(channel)\n' +
            '                        } — the socket is skipped and the fan-out continues: ${\n' +
            '                            renderError(error)\n' +
            '                        }`,\n' +
            '                    )\n' +
            '                } catch (sink) {\n' +
            '                    // #395 (security review LOW): a throwing sink must not\n' +
            '                    // abort this fan-out — the remaining local sockets must\n' +
            '                    // still receive the frame. One marked line instead, which\n' +
            '                    // never throws (#391).\n' +
            '                    writeMarkedFallback(EMIT_PRESENCE_LOG_FAILED, error, {\n' +
            "                        label: 'sink failure',\n" +
            '                        error: sink,\n' +
            '                    })\n' +
            '                }\n',
            '                // Mutant: the guard is gone — a throwing sink aborts the whole\n' +
            '                // fan-out here.\n' +
            '                console.warn(\n' +
            '                    `realtime: a presence frame could not be delivered on ${\n' +
            '                        safeForLog(channel)\n' +
            '                    } — the socket is skipped and the fan-out continues: ${\n' +
            '                        renderError(error)\n' +
            '                    }`,\n' +
            '                )\n',
        ]],
        // Witness: 'listener still received the newcomer's joined frame' —
        // with the guard gone, the sink's own throw escapes the `for` loop
        // in `emitPresence`, and listener is never reached.
        killedBy: W3,
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#395 (security review) — the 3 additional guarded WARNs ' +
                    'stay guarded',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
