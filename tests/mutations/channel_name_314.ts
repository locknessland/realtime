/**
 * @fileoverview #314's mutation battery — the third of three boundary guards.
 *
 * `subscribe` asserts `connection.id` (#304), the channel (#314) and, on a
 * presence channel, `member.id` (#306). Each has its own battery because each
 * was added for a different reason and the rows are not interchangeable — the
 * charset here is `isValidName`, where #306's is deliberately absent.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/channel_name_314.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/channel_name_314
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../channel_name_boundary.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'the channel assertion removed entirely',
        file: MANAGER,
        edits: [['        this.#assertUsableChannel(channel)\n', '']],
        killedBy: 'subscribe refuses a channel the WebSocket wire would refuse',
    },
    {
        label: 'the guard inverted — only VALID channels are refused',
        file: MANAGER,
        edits: [[
            '        if (isValidName(channel)) return\n        throw new ChannelNameError(channel)',
            '        if (!isValidName(channel)) return\n        throw new ChannelNameError(channel)',
        ]],
        killedBy: 'an ordinary channel is unaffected',
    },
    {
        label:
            'the assertion moved AFTER the authorizer — the side effect is spent anyway',
        file: MANAGER,
        edits: [
            ['        this.#assertUsableChannel(channel)\n', ''],
            [
                '        let set = this.subscriptions.get(channel)',
                '        this.#assertUsableChannel(channel)\n        let set = this.subscriptions.get(channel)',
            ],
        ],
        killedBy: 'the refusal happens BEFORE the authorizer runs',
    },
    {
        label: 'UNSUBSCRIBE guarded too — cleanup stops being total',
        file: MANAGER,
        edits: [[
            '    async unsubscribe(clientId: string, channel: string): Promise<void> {',
            '    async unsubscribe(clientId: string, channel: string): Promise<void> {\n        this.#assertUsableChannel(channel)',
        ]],
        killedBy: 'UNSUBSCRIBE is not guarded',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#314 — the channel name boundary',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
