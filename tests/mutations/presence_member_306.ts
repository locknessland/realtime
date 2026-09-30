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
// #350 moved the id check into `admitPresenceMember`: source moved, guard
// remains (`docs/testing.md`). Rows 1, 2, 3 and 6 now anchor there.
const MEMBER = new URL('../../presence_member.ts', import.meta.url)
const PROTOCOL = new URL('../../protocol.ts', import.meta.url)
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
        // RE-ANCHORED by #350: the one call, inside the admission.
        file: MEMBER,
        edits: [[
            '    assertUsableMemberId(id)\n',
            '',
        ]],
        killedBy:
            'an OVERSIZED member id is refused before anything is written',
    },
    {
        label: 'the length bound loosened past the roster field it protects',
        file: MEMBER,
        edits: [[
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH * 10) return',
        ]],
        killedBy: 'the boundary is EXACT',
    },
    {
        label: 'the empty-id half of the bound dropped',
        file: MEMBER,
        edits: [[
            'if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
            'if (text.length <= MAX_NAME_LENGTH) return',
        ]],
        killedBy: 'an EMPTY member id is refused',
    },
    {
        label:
            'the non-finite number check removed — every NaN member shares one field',
        // RE-ANCHORED by #346: the finiteness rule moved out of
        // `#assertUsableMemberId` into `isPresenceMemberIdValue`, the one
        // predicate the join, the Redis frame ingest and the roster read
        // share. Same mutant, new home — #346's battery carries the same edit
        // as its M2; each battery stays self-contained.
        file: PROTOCOL,
        edits: [[
            "    return typeof value === 'number' && Number.isFinite(value)\n",
            "    return typeof value === 'number'\n",
        ]],
        killedBy: 'a NON-FINITE numeric id is refused',
    },
    {
        label:
            'the assertion moved AFTER the roster write — the partial write it exists to stop',
        file: MANAGER,
        edits: [
            [
                // RE-ANCHORED by #350: the admission is one call now, so
                // the row takes it out of `subscribe` (the member stays
                // the raw candidate) and runs it once the write committed.
                // RE-ANCHORED by #357: the object result is admitted on
                // EVERY kind before the presence branch, and `true`'s
                // `{ id: connection.id }` inside it — so the row takes out
                // both calls. The source moved; the guard remains.
                '            const returned = verdict.member === undefined\n' +
                '                ? undefined\n' +
                '                : admitPresenceMember(\n' +
                '                    verdict.member,\n' +
                '                    this.#maxPresenceMemberBytes,\n' +
                '                )\n',
                '            const returned = verdict.member as PresenceMember | undefined\n',
            ],
            [
                '                member = returned ?? admitPresenceMember(\n' +
                '                    { id: connection.id },\n' +
                '                    this.#maxPresenceMemberBytes,\n' +
                '                )\n',
                '                member = returned ?? { id: connection.id }\n',
            ],
            [
                // RE-ANCHORED TWICE. #328 moved the roster write out of
                // `subscribe` into `#joinPresence`, costing one level of
                // indentation; #330 then replaced the direct
                // `roster.addMember` with the serialized projection
                // `#syncRosterMember`. The FIRST edit above has moved through
                // neither — the id assertion is in the authorization block,
                // which stayed in `subscribe` — so only this half ever
                // changes, and a blanket re-edit of the row would break the
                // half that was still correct.
                // RE-ANCHORED a THIRD time by #344: the join's write returns
                // nothing now.
                // RE-PLACED by the #357 review: the admission used to be
                // inserted BEFORE `await this.#syncRosterMember` — after the
                // local join but ahead of the authoritative write, so the row
                // did not mutate what its label names. It now lands after the
                // whole write-and-compensate block, where the write has
                // committed and no compensation runs: the witness reads the
                // hold and the join frame in its log, not a hold-and-release
                // cleanup. The anchor carries the block's closing lines to
                // match exactly once — the closing read is also the re-join's
                // exit.
                '                throw error\n' +
                '            }\n' +
                '        }\n' +
                '        return await this.#closingRead(channel, connection.id)\n',
                '                throw error\n' +
                '            }\n' +
                '        }\n' +
                '        admitPresenceMember(member, this.#maxPresenceMemberBytes)\n' +
                '        return await this.#closingRead(channel, connection.id)\n',
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
        //
        // RE-ANCHORED for #323: the roster write moved inside a `try` that
        // compensates the local join, so the line gained eight spaces of
        // indentation and lost its `if (this.roster)` prefix. A stale anchor
        // here reports DEAD MUTANT rather than a miss, and the nightly sweep is
        // the only thing that runs this file — the pre-completion gate does
        // not.
        killedBy: 'the member-id assertion runs BEFORE the roster write',
    },
    {
        label:
            'THE DECISION INVERTED — the charset applied, as #304 does to Connection.id',
        file: MEMBER,
        edits: [
            [
                '    const text = String(id)\n    if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return',
                '    const text = String(id)\n    if (isValidName(text)) return',
            ],
            // `presence_member.ts` does not import the charset predicate.
            [
                '    isPresenceMemberWire,\n',
                '    isPresenceMemberWire,\n    isValidName,\n',
            ],
        ],
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
