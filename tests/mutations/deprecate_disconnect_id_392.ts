/**
 * @fileoverview #392's mutation battery — `disconnect`'s id form raises a
 * deprecation notice, once per manager instance, for application callers
 * only, and never before the teardown it must not pre-empt.
 *
 * The decision lives in four places, all in `manager.ts`: the
 * `typeof target === 'string'` gate in `disconnect` that decides WHETHER
 * `#warnIdForm` runs; `#warnIdForm`'s own `#idFormWarned` guard, which decides
 * HOW MANY TIMES it fires; `revokeLocal`'s direct `#teardown` call, which is
 * what keeps the framework's own id-form use silent; and `disconnect`'s own
 * ORDER — `#teardown(target)` is called and its promise captured BEFORE
 * `#warnIdForm` ever runs, so a `STRICT_DEPRECATIONS` throw (or a throwing
 * collector) can only affect what this call's promise settles with, never
 * whether the teardown happened at all.
 *
 * - M1 — the gate widened: `#warnIdForm` runs for the object form too.
 * - M2 — the gate removed: `#warnIdForm` never runs, not even for the id
 *   form.
 * - M3 — the once-per-instance guard removed: every id-form call fires,
 *   proving the #392 disposition's "once per manager instance" choice is
 *   load-bearing, not incidental.
 * - M4 — `revokeLocal` reverted to the public `disconnect`, which would raise
 *   the notice for the framework's own internal id-form caller.
 * - M5 — the notice moved back BEFORE the teardown (review HIGH,
 *   2026-09-26): `#teardown(target)` no longer runs first, so
 *   `STRICT_DEPRECATIONS` throws synchronously out of `disconnect` itself,
 *   before anything is retired or torn down — the exact regression the
 *   review found.
 *
 * Every row was proven LIVE: the harness ran the mutant and its named witness
 * went red.
 *
 * ```bash
 * deno task mutate deprecate_disconnect_id_392
 * ```
 *
 * @module @lockness/realtime/tests/mutations/deprecate_disconnect_id_392
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../deprecate_disconnect_id_392.test.ts', import.meta.url)
        .pathname,
]

/** The gate, as shipped. */
const GATE = "        if (typeof target === 'string') {\n"

/** `disconnect`'s body, as shipped — teardown started, then the notice. */
const DISCONNECT_BODY = '        const teardown = this.#teardown(target)\n' +
    GATE +
    '            try {\n' +
    '                this.#warnIdForm()\n' +
    '            } catch (deprecationError) {\n' +
    '                // Both reactions re-throw the SAME error: it wins even over a\n' +
    '                // teardown failure (see the JSDoc above for why), and\n' +
    '                // supplying both means `teardown` never carries an\n' +
    '                // unhandled rejection either way.\n' +
    '                return teardown.then(\n' +
    '                    () => {\n' +
    '                        throw deprecationError\n' +
    '                    },\n' +
    '                    (teardownError: unknown) => {\n' +
    '                        // The deprecation error wins the rejection, but the\n' +
    "                        // teardown's own failure is never dropped silently:\n" +
    '                        // it is WARNed, and a throwing sink falls back to the\n' +
    '                        // marked line, which never throws (#391).\n' +
    '                        try {\n' +
    '                            console.warn(\n' +
    "                                'realtime: a disconnect teardown failed while ' +\n" +
    "                                    'its id-form deprecation notice also ' +\n" +
    '                                    `threw (#392): ${\n' +
    '                                        renderError(teardownError)\n' +
    '                                    }`,\n' +
    '                            )\n' +
    '                        } catch (sink) {\n' +
    '                            writeMarkedFallback(\n' +
    '                                DISCONNECT_TEARDOWN_LOG_FAILED,\n' +
    '                                teardownError,\n' +
    "                                { label: 'sink failure', error: sink },\n" +
    '                            )\n' +
    '                        }\n' +
    '                        throw deprecationError\n' +
    '                    },\n' +
    '                )\n' +
    '            }\n' +
    '        }\n' +
    '        return teardown\n'

const MUTATIONS: Mutation[] = [
    {
        label:
            'M1 — the gate widened: #warnIdForm runs for the object form too',
        file: MANAGER,
        edits: [[GATE, '        if (true) {\n']],
        killedBy: '#392 W2 ',
    },
    {
        label: 'M2 — the gate removed: #warnIdForm never runs',
        file: MANAGER,
        edits: [[GATE, '        if (false) {\n']],
        killedBy: '#392 W1 ',
    },
    {
        label:
            'M3 — the once-per-instance guard removed: every id-form call fires',
        file: MANAGER,
        edits: [[
            '    #warnIdForm(): void {\n' +
            '        if (this.#idFormWarned) return\n' +
            '        this.#idFormWarned = true\n',
            '    #warnIdForm(): void {\n',
        ]],
        killedBy: '#392 W1 ',
    },
    {
        label:
            "M4 — revokeLocal reverted to the public disconnect, raising the notice for the framework's own caller",
        file: MANAGER,
        edits: [[
            '        try {\n' +
            '            await this.#teardown(clientId)\n' +
            '        } catch (error) {\n' +
            '            console.warn(\n' +
            '                `realtime: evict teardown for ${safeForLog(clientId)} failed ` +\n',
            '        try {\n' +
            '            await this.disconnect(clientId)\n' +
            '        } catch (error) {\n' +
            '            console.warn(\n' +
            '                `realtime: evict teardown for ${safeForLog(clientId)} failed ` +\n',
        ]],
        killedBy: '#392 W3 ',
    },
    {
        label:
            'M5 — the notice moved back BEFORE the teardown (review HIGH): a strict-mode throw pre-empts #teardown entirely',
        file: MANAGER,
        edits: [[
            DISCONNECT_BODY,
            "        if (typeof target === 'string') this.#warnIdForm()\n" +
            '        return this.#teardown(target)\n',
        ]],
        killedBy: '#392 W4 ',
    },
    {
        label:
            'M6 — the teardown failure dropped silently: its WARN removed while the deprecation error wins',
        file: MANAGER,
        edits: [[
            '                            console.warn(\n' +
            "                                'realtime: a disconnect teardown failed while ' +\n",
            '                            String(\n' +
            "                                'realtime: a disconnect teardown failed while ' +\n",
        ]],
        killedBy: '#392 W6 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                "#392 — disconnect's id form deprecation, once per manager instance, framework callers silent, never before the teardown",
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
