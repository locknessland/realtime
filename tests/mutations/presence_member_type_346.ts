/**
 * @fileoverview #346's mutation battery — a presence member id is a string or
 * a finite number, decided by ONE predicate at all three sites.
 *
 * Each row puts back one way the rule can be wrong: the join's type check
 * removed (M1, the shipped defect), the finiteness half dropped (M2), the set
 * widened to booleans (M3), the shared wire predicate narrowed to string ids
 * (M4 — since #350 it is asked by the join's admission, the frame ingest and
 * the departure handler alike, so a numeric id is refused everywhere), the
 * roster read narrowed back to strings only so the sender accepts what a peer
 * skips (M5), `String(id)` moved ahead of the predicate so a null-prototype
 * object throws a `TypeError` instead of the named error (M6), the refused
 * value echoed into the message beside its type (M7), and a boxed boolean or
 * boxed symbol id named as a plain object (M8, M9 — #351).
 *
 * The killing assertions match the TYPE-BRANCH wording (`of type null`),
 * which #306's value-echoing wording cannot produce, so a refusal that comes
 * from anywhere else cannot pass for this one. M2 is the exception on
 * purpose: its witness is #306's own NaN row, the test that owned the
 * finiteness rule before it moved into the predicate.
 *
 * Every row was proven LIVE before it was trusted: a marker was placed at the
 * row's anchor and seen to execute under the killing witness. A row whose line
 * never runs reports a kill it did not cause.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_member_type_346.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_member_type_346
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const PROTOCOL = new URL('../../protocol.ts', import.meta.url)
// #350 moved the join's id check and `describeMemberId` into
// `presence_member.ts`: source moved, guard remains. M1, M6, M7 anchor there.
const MEMBER = new URL('../../presence_member.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const CHANNEL = new URL('../../channel.ts', import.meta.url)
const SUITES = [
    new URL('../presence_member_id_type_346.test.ts', import.meta.url).pathname,
    // #306's suite: M2's witness is its NaN row.
    new URL('../presence_member_id.test.ts', import.meta.url).pathname,
]

const TYPE_CHECK =
    '    if (!isPresenceMemberIdValue(id)) throw new PresenceMemberIdError(id)\n'
const TO_TEXT = '    const text = String(id)\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the join-time type check removed: the shipped defect',
        file: MEMBER,
        edits: [[TYPE_CHECK, '']],
        // `String(null)` is 'null': Alice and Bob joined as one member. Since
        // #350 the parsed copy's wire check still refuses the null id, as a
        // PresenceMemberShapeError — the witness wants PresenceMemberIdError.
        killedBy: '#346 (a) memory: two people with a null member id',
    },
    {
        label:
            'M2 — the finiteness half dropped: every NaN member shares "NaN"',
        file: PROTOCOL,
        edits: [[
            "    return typeof value === 'number' && Number.isFinite(value)\n",
            "    return typeof value === 'number'\n",
        ]],
        killedBy: 'a NON-FINITE numeric id is refused',
    },
    {
        label: 'M3 — the set widened to booleans',
        file: PROTOCOL,
        edits: [[
            "    if (typeof value === 'string') return true\n",
            "    if (typeof value === 'string' || typeof value === 'boolean') return true\n",
        ]],
        killedBy: '#346 (b) true member id is refused',
    },
    {
        label: 'M4 — isPresenceMemberWire narrowed to string ids: the join, ' +
            'the frame ingest and the departure handler all refuse a numeric id',
        // The member rule lives in `isPresenceMemberWire` (#348, #350), asked
        // by `isPlainMember`, the join's admission and the manager's departure
        // handler — so this no longer splits sender from receiver, it refuses
        // a numeric id at every site. The ingest row is the witness.
        file: PROTOCOL,
        edits: [[
            '    const idOk = isPresenceMemberIdValue(member.id)\n',
            "    const idOk = typeof member.id === 'string'\n",
        ]],
        killedBy: '#346 (c) frame ingest: a numeric id',
    },
    {
        label:
            'M5 — the roster read narrowed to strings: a peer skips what the sender wrote',
        file: REDIS,
        edits: [[
            '                !isPresenceMemberIdValue(member.id)\n',
            "                typeof member.id !== 'string'\n",
        ]],
        killedBy: '#346 (c) roster read: a numeric id',
    },
    {
        label: 'M6 — String(id) ahead of the predicate',
        file: MEMBER,
        edits: [[TYPE_CHECK + TO_TEXT, TO_TEXT + TYPE_CHECK]],
        // `String()` on a null-prototype object throws a TypeError before the
        // predicate can refuse it by name.
        killedBy: '#346 (b) a null-prototype object member id is refused',
    },
    {
        label: 'M7 — the refused value echoed into the message beside its type',
        file: MEMBER,
        edits: [[
            '    return `of type ${typeLabel(id)}`\n',
            '    return `of type ${typeLabel(id)} ${JSON.stringify(id)}`\n',
        ]],
        killedBy:
            '#346 (d) the message names an object id by type and never echoes it',
    },
    {
        label: 'M8 — a boxed boolean id named as a plain object (#351)',
        file: CHANNEL,
        edits: [[
            "    if (value instanceof Boolean) return 'boxed boolean'\n",
            '',
        ]],
        killedBy: '#346 (b) a boxed boolean member id is refused',
    },
    {
        label: 'M9 — a boxed symbol id named as a plain object (#351)',
        file: CHANNEL,
        edits: [[
            "    if (value instanceof Symbol) return 'boxed symbol'\n",
            '',
        ]],
        // M6 also dies on the boxed-symbol row now: `String()` on a Symbol
        // wrapper throws a `TypeError`, like a null-prototype object.
        killedBy: '#346 (b) a boxed symbol member id is refused',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#346 — the presence member id type',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
