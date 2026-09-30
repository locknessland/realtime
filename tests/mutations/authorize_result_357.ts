/**
 * @fileoverview #357's mutation battery — an object result admits only as a
 * `PresenceMember`, on every channel kind.
 *
 * Each row puts back one way the rule can be undone or distorted:
 *
 * - M1 the admission moved back inside `if (kind === 'presence')` — the tidy
 *   "a private channel never reads the member" edit, which restores
 *   object-admits: every miss wrapper admits a private channel again.
 * - M2 private validated in place with `isPresenceMemberWire` — "saving the
 *   parse". A second definition of "what a member is": `{ id: 7, info: new
 *   Date(0) }` passes in memory and fails the parsed check, so private admits
 *   what presence refuses.
 * - M3 private refuses every object — the product veto's shape, applied
 *   without the veto: a type-sanctioned `{ id, info }` throws on half the
 *   channels.
 * - M4 a non-member object on private folded into `{ ok: false }` — #347's
 *   rejected silent deny, which hides the authorizer's defect.
 * - M5 the private admission moved below `#checkChannelCaps` — a full
 *   connection hears `ChannelLimitError`, and a refusal is a partial write.
 *   Re-anchored by #370, which deleted the `connections` write below the
 *   caps.
 * - M6 `PRIVATE_CHANNEL_HINT` dropped — the member errors stop telling a
 *   private-channel authorizer to return a boolean.
 * - M7 the seat's condition widened to `kind === 'presence' || returned !==
 *   undefined` — a private channel stops discarding the admitted member and
 *   seats it: a roster entry, a `presence-join` publish and a `here` snapshot
 *   for a channel that has no roster. Admission and delivery are unchanged, so
 *   only a row that reads the discard itself can see it.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno task mutate authorize_result_357
 * ```
 *
 * @module @lockness/realtime/tests/mutations/authorize_result_357
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const MEMBER = new URL('../../presence_member.ts', import.meta.url)
const SUITES = [
    new URL('../authorize_result_357.test.ts', import.meta.url).pathname,
]

/** The every-kind admission of the authorizer's object, as shipped. */
const ADMISSION =
    '            const returned = verdict.member === undefined\n' +
    '                ? undefined\n' +
    '                : admitPresenceMember(\n' +
    '                    verdict.member,\n' +
    '                    this.#maxPresenceMemberBytes,\n' +
    '                )\n'

/** The presence seat, as shipped. */
const SEAT = '                member = returned ?? admitPresenceMember(\n' +
    '                    { id: connection.id },\n' +
    '                    this.#maxPresenceMemberBytes,\n' +
    '                )\n'

/**
 * The end of the caps check, then the blank line and the comment below it —
 * RE-ANCHORED by #370, which deleted the `connections` write that sat here.
 */
const CAPS_END = '            connection.identity !== null,\n' +
    '        )\n'
const BELOW_CAPS = '\n        // `member` is set'

const MUTATIONS: Mutation[] = [
    {
        label:
            "M1 — the admission back inside `if (kind === 'presence')`: object-admits restored",
        file: MANAGER,
        edits: [
            [ADMISSION, ''],
            [
                SEAT,
                '                member = admitPresenceMember(\n' +
                '                    verdict.member ?? { id: connection.id },\n' +
                '                    this.#maxPresenceMemberBytes,\n' +
                '                )\n',
            ],
        ],
        // (b)'s KV-miss row dies too: private answers `ok` where presence
        // throws.
        killedBy:
            '#357 (a) memory private-orders: a Deno KV miss entry throws PresenceMemberShapeError',
    },
    {
        label:
            'M2 — private validated in place with isPresenceMemberWire (no parse)',
        file: MANAGER,
        edits: [[
            ADMISSION,
            '            const returned = verdict.member === undefined\n' +
            '                ? undefined\n' +
            "                : kind === 'presence'\n" +
            '                ? admitPresenceMember(verdict.member, this.#maxPresenceMemberBytes)\n' +
            '                : isPresenceMemberWire(verdict.member)\n' +
            '                ? (verdict.member as PresenceMember)\n' +
            '                : (() => {\n' +
            "                    throw new TypeError('mutant: not a member')\n" +
            '                })()\n',
        ]],
        // The in-memory predicate accepts a `Date` as `info` (it is a
        // non-array object); the parsed check sees the string it serializes
        // to. The counting-Proxy row does NOT kill this mutant — the
        // predicate reads the keys, `id` and `info` once each too — which is
        // why the Date row exists.
        killedBy:
            '#357 (e) memory private-orders: { id: 7, info: new Date(0) } throws PresenceMemberShapeError',
    },
    {
        label: 'M3 — private refuses every object (the veto shape, unvetoed)',
        file: MANAGER,
        edits: [[
            ADMISSION,
            "            if (kind === 'private' && verdict.member !== undefined) {\n" +
            "                throw new AuthorizeResultError(channel, 'object')\n" +
            '            }\n' +
            ADMISSION,
        ]],
        killedBy: "#357 (c) memory private-orders: { id: 'u1' } admits",
    },
    {
        label: 'M4 — a non-member object on private folded into { ok: false }',
        file: MANAGER,
        edits: [[
            ADMISSION,
            '            let returned: PresenceMember | undefined\n' +
            '            try {\n' +
            '                returned = verdict.member === undefined\n' +
            '                    ? undefined\n' +
            '                    : admitPresenceMember(verdict.member, this.#maxPresenceMemberBytes)\n' +
            '            } catch (error) {\n' +
            "                if (kind === 'private') return { ok: false }\n" +
            '                throw error\n' +
            '            }\n',
        ]],
        killedBy:
            '#357 (a) memory private-orders: a Deno KV miss entry throws PresenceMemberShapeError',
    },
    {
        label: 'M5 — the private admission below #checkChannelCaps',
        file: MANAGER,
        edits: [
            [
                '        let member: PresenceMember | undefined\n' +
                "        if (kind !== 'public') {\n",
                '        let member: PresenceMember | undefined\n' +
                '        let deferred: object | undefined\n' +
                "        if (kind !== 'public') {\n",
            ],
            [
                ADMISSION,
                "            if (kind === 'private') deferred = verdict.member\n" +
                "            const returned = kind === 'presence' && verdict.member !== undefined\n" +
                '                ? admitPresenceMember(verdict.member, this.#maxPresenceMemberBytes)\n' +
                '                : undefined\n',
            ],
            [
                CAPS_END + BELOW_CAPS,
                CAPS_END +
                '        if (deferred !== undefined) {\n' +
                '            admitPresenceMember(deferred, this.#maxPresenceMemberBytes)\n' +
                '        }\n' +
                BELOW_CAPS,
            ],
        ],
        killedBy:
            '#357 (d) memory: a full connection whose private result is a KV miss',
    },
    {
        label: 'M6 — PRIVATE_CHANNEL_HINT dropped',
        file: MEMBER,
        edits: [[
            'const PRIVATE_CHANNEL_HINT =\n' +
            "    'On a `private-*` channel no member is used: return `true` to admit (#357).'\n",
            "const PRIVATE_CHANNEL_HINT = ''\n",
        ]],
        killedBy: '#357 (f) private-orders: a KV miss names `key`',
    },
    {
        label:
            'M7 — the seat widened to every admitted member: private seats it',
        file: MANAGER,
        edits: [[
            "            if (kind === 'presence') {\n" + SEAT,
            "            if (kind === 'presence' || returned !== undefined) {\n" +
            SEAT,
        ]],
        // Every (c) row but `true` dies; `true` carries no member, so the
        // widened condition is still false for it.
        killedBy:
            '#357 (c) memory private-orders: { id: 7, info: { n: 1 } } admits',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#357 — an object result admits only as a PresenceMember',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
