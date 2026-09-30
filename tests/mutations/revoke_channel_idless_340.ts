/**
 * @fileoverview #340's mutation battery — an id-less `revoke-channel` frame is
 * dropped with a WARN, the WARN is log-encoded, and only the owner says it.
 *
 * Four rows, one per rule the witness file pins. Each mutant is written to
 * COMPILE: a mutant the type-checker refuses is recorded dead and proves
 * nothing about the suite.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/revoke_channel_idless_340.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revoke_channel_idless_340
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../revoke_channel_idless_340.test.ts', import.meta.url).pathname,
]

const WARN_OPEN =
    '                if (control.revocationId === undefined) {\n' +
    '                    console.warn(\n'

const MUTATIONS: Mutation[] = [
    {
        label: '#340 the id-less drop is silent again (WARN removed)',
        file: MANAGER,
        edits: [[
            WARN_OPEN,
            '                if (control.revocationId === undefined) {\n' +
            '                    ((_message: string) => {})(\n',
        ]],
        // A no-op in the call's place keeps the argument list — trailing comma
        // included — valid, so the mutant compiles and the drop is exactly
        // #337's: not applied, not logged.
        killedBy: 'WITNESS',
    },
    {
        label: '#340 the channel name is logged raw',
        file: MANAGER,
        edits: [[
            '                        `realtime: a revoke-channel frame for ${\n' +
            '                            safeForLog(control.channel)\n',
            '                        `realtime: a revoke-channel frame for ${\n' +
            '                            control.channel\n',
        ]],
        // A name carrying a newline forges a second log line.
        killedBy: 'log-encoded',
    },
    {
        label: '#340 every instance warns, not only the owner',
        file: MANAGER,
        edits: [
            [
                '                if (\n' +
                '                    control.channel === undefined ||\n' +
                '                    !this.connections.has(control.target)\n' +
                '                ) {\n' +
                '                    return\n' +
                '                }\n',
                '                if (control.channel === undefined) return\n',
            ],
            [
                '                this.#dispatchRevocation({\n' +
                '                    target: control.target,\n' +
                '                    channel: control.channel,\n' +
                '                    ids: [control.revocationId],\n' +
                '                })\n',
                '                if (!this.connections.has(control.target)) {\n' +
                '                    return\n' +
                '                }\n' +
                '                this.#dispatchRevocation({\n' +
                '                    target: control.target,\n' +
                '                    channel: control.channel,\n' +
                '                    ids: [control.revocationId],\n' +
                '                })\n',
            ],
        ],
        // The ownership gate moves below the WARN: the apply is unchanged, and
        // one frame becomes one WARN per instance in the fleet.
        killedBy: 'stays silent',
    },
    {
        label: '#340 the id-less frame is applied anyway',
        file: MANAGER,
        edits: [
            [
                "                            'pass revocationId through unchanged.',\n" +
                '                    )\n' +
                '                    return\n',
                "                            'pass revocationId through unchanged.',\n" +
                '                    )\n',
            ],
            [
                '                    ids: [control.revocationId],\n',
                "                    ids: [control.revocationId ?? ''],\n",
            ],
        ],
        // The WARN stays, so only the "still not applied" half can kill it —
        // and the CONTROL test proves an applied frame lands inside the settle.
        killedBy: 'WITNESS',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#340 — an id-less revoke-channel frame is never silent',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
