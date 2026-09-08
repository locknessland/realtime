/**
 * @fileoverview #306's mutation battery — the bound, and the shape of it.
 *
 * Two kinds of row, deliberately. The refusals prove the guard is reachable
 * and load-bearing; the LAST row proves the decision itself — tightening the
 * length bound into `isValidName`'s charset must break the applications the
 * decision was made to protect. A battery that only mutated toward "weaker"
 * would let someone tighten this later and call it hardening.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once, a non-compiling mutant
 * reported DEAD, and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_member_306.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_member_306
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_member_id.test.ts', import.meta.url).pathname,
    // #312's recording roster. Row 5 below was an equivalent mutant against
    // the first suite alone — it uses no roster, so a write that had already
    // happened was invisible to it.
    new URL('../roster_control_atomicity.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'the boundary assertion removed entirely',
        file: MANAGER,
        edits: [[
            '                this.#assertUsableMemberId(member.id)\n',
            '',
        ]],
        killedBy:
            'an OVERSIZED member id is refused before anything is written',
    },
    {
        label: 'the length bound loosened past the roster field it protects',
        file: MANAGER,
        edits: [[
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH * 10) return',
        ]],
        killedBy: 'the boundary is EXACT',
    },
    {
        label: 'the empty-id half of the bound dropped',
        file: MANAGER,
        edits: [[
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
            'if (text.length <= MAX_NAME_LENGTH) return',
        ]],
        killedBy: 'an EMPTY member id is refused',
    },
    {
        label:
            'the non-finite number check removed — every NaN member shares one field',
        file: MANAGER,
        edits: [[
            "        if (typeof id === 'number' && !Number.isFinite(id)) {\n            throw new PresenceMemberIdError(String(id))\n        }\n",
            '',
        ]],
        killedBy: 'a NON-FINITE numeric id is refused',
    },
    {
        label:
            'the assertion moved AFTER the roster write — the partial write it exists to stop',
        file: MANAGER,
        edits: [
            [
                '                this.#assertUsableMemberId(member.id)\n',
                '',
            ],
            [
                '            if (this.roster) await this.roster.addMember(channel, member)',
                '            if (this.roster) await this.roster.addMember(channel, member)\n            this.#assertUsableMemberId(member.id)',
            ],
        ],
        // RED since #312, and the path is worth keeping. It survived here for
        // as long as `presence_member_id.test.ts` was the only suite: the
        // guard still THROWS when moved, so every assertion about refusal
        // still held, and those tests use no roster — so a write that had
        // already happened was invisible. The ordering was the reason the
        // guard sits where it does and was held by a comment alone.
        //
        // `roster_control_atomicity.test.ts` records every roster op, and an
        // EMPTY log is the assertion the first suite had no way to make.
        killedBy: 'the member-id assertion runs BEFORE the roster write',
    },
    {
        label:
            'THE DECISION INVERTED — the charset applied, as #304 does to Connection.id',
        file: MANAGER,
        edits: [[
            '        const text = String(id)\n        if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
            '        const text = String(id)\n        if (isValidName(text)) return',
        ]],
        killedBy: 'an application id the charset would REJECT is accepted',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#306 — the presence member id bound',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
