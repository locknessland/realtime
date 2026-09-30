/**
 * @fileoverview #353's mutation battery — a value the realtime classifiers
 * cannot inspect is refused with a named error, and the join's member
 * invariant precedes every write.
 *
 * Each row puts back one way the fix can be undone: the invariant moved back
 * below `#checkChannelCaps` (M1), the classifier's catch rethrowing (M2), the
 * uninspectable label collapsed to `'object'` — the tidy "use `typeof`'s
 * answer" edit, which ADMITS the value (M3), the wire predicate's catch
 * answering `true` (M4), its `Array.isArray` moved back outside the try (M5),
 * `typeLabel` inspecting before delegating, as it did before (M6), and a
 * live Proxy's reads escaping the wire predicate (#353 review) — its catch
 * rethrowing (M7), the key read hoisted above its try (M8, the `ownKeys`
 * trap) and a field read hoisted above it (M9, the `get` trap).
 *
 * **M1 is an equivalent mutant since #370**, recorded rather than deleted. It
 * used to force the invariant to fire (dropping the admitted member) and move
 * it below `subscribe`'s `connections` write, so the witness saw a binding
 * left behind by a throwing subscribe. #370 deleted that write: nothing is
 * written between the invariant's place and the join, so moving the check
 * below the caps changes no outcome the shipped code can reach — the invariant
 * never fires there (`admitPresenceMember` always returns a member), and a
 * kept forcing edit would be the only thing a witness could see (#370 review).
 * The row is now the bare move, and it SURVIVES by design.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/manager_debt_353.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/manager_debt_353
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const CHANNEL = new URL('../../channel.ts', import.meta.url)
const PROTOCOL = new URL('../../protocol.ts', import.meta.url)
const SUITES = [
    new URL('../manager_debt_353.test.ts', import.meta.url).pathname,
    new URL('../authorize_result_347.test.ts', import.meta.url).pathname,
]

const INVARIANT =
    "        if (kind === 'presence' && member === undefined) {\n" +
    '            throw new Error(\n' +
    "                'realtime: a presence admission reached the join without ' +\n" +
    "                    'a member — an invariant of subscribe is broken (#347).',\n" +
    '            )\n' +
    '        }\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the member invariant back BELOW `#checkChannelCaps`',
        file: MANAGER,
        edits: [
            [INVARIANT, ''],
            // RE-ANCHORED by #370: the `connections` write below the caps is
            // gone, so the invariant goes back below the caps check itself.
            [
                '            connection.identity !== null,\n' +
                '        )\n\n' +
                '        // `member` is set on a presence admission',
                '            connection.identity !== null,\n' +
                '        )\n' +
                INVARIANT +
                '\n        // `member` is set on a presence admission',
            ],
        ],
        killedBy: '(none — equivalent)',
        expectSurvival:
            '(none — equivalent) — since #370 nothing is written between the ' +
            "invariant's place and the join, so its position relative to the " +
            'caps check decides no reachable outcome: the invariant never ' +
            'fires on the shipped code, and a presence subscribe that passes ' +
            'the caps reaches the same join either way.',
    },
    {
        label: "M2 — the classifier's catch rethrows (the pre-#353 TypeError)",
        file: CHANNEL,
        edits: [[
            '        return UNINSPECTABLE_LABEL\n',
            "        throw new TypeError('mutant: the inspection threw')\n",
        ]],
        killedBy: 'classifyAuthorizeResult answers `invalid`',
    },
    {
        label: "M3 — the uninspectable label collapsed to `typeof`'s 'object'",
        file: CHANNEL,
        edits: [[
            "const UNINSPECTABLE_LABEL = 'uninspectable object'\n",
            "const UNINSPECTABLE_LABEL = 'object'\n",
        ]],
        // Not cosmetic: `isAdmittingObject` asks `objectLabel(...) ===
        // 'object'`, so the throwing-trap Proxy is ADMITTED to a private
        // channel.
        killedBy:
            'private-orders: an authorizer returning a Proxy whose getPrototypeOf trap throws gets AuthorizeResultError',
    },
    {
        label: "M4 — the wire predicate's catch answers true",
        file: PROTOCOL,
        edits: [[
            '        // revoked Proxy is no member and no `info`.\n' +
            '        return false\n',
            '        // revoked Proxy is no member and no `info`.\n' +
            '        return true\n',
        ]],
        killedBy: 'isPresenceMemberInfoValue answers false for a revoked Proxy',
    },
    {
        label: "M5 — the wire predicate's `Array.isArray` back outside the try",
        file: PROTOCOL,
        edits: [[
            '    try {\n' +
            '        return !Array.isArray(value)\n',
            '    if (Array.isArray(value)) return false\n' +
            '    try {\n' +
            '        return true\n',
        ]],
        killedBy: 'a departure a driver reports with a value it cannot inspect',
    },
    {
        label: 'M6 — typeLabel inspects before delegating, as it did pre-#353',
        file: CHANNEL,
        edits: [[
            "    if (typeof value === 'object') return objectLabel(value)\n",
            "    if (Array.isArray(value)) return 'array'\n" +
            "    if (typeof value === 'object') return objectLabel(value)\n",
        ]],
        killedBy:
            'a member whose id is a revoked Proxy throws PresenceMemberIdError',
    },
    {
        label: "M7 — the wire predicate's read catch rethrows",
        file: PROTOCOL,
        edits: [[
            "        // here with a driver's value.\n" +
            '        return false\n',
            "        // here with a driver's value.\n" +
            "        throw new TypeError('mutant: the read threw')\n",
        ]],
        killedBy:
            'isPresenceMemberWire answers false, and never throws, for a live Proxy',
    },
    {
        label: 'M8 — the key read hoisted above the try (the `ownKeys` trap)',
        file: PROTOCOL,
        edits: [[
            '    try {\n' +
            '        if (!Object.keys(value).every(isPresenceMemberKey)) return false\n',
            '    if (!Object.keys(value).every(isPresenceMemberKey)) return false\n' +
            '    try {\n',
        ]],
        killedBy: 'a departure a driver reports with a value it cannot inspect',
    },
    {
        // The `ownKeys` row passes under this mutant — only the `get` row
        // can kill it, so it proves that row is live on its own.
        label: 'M9 — a field read hoisted above the try (the `get` trap)',
        file: PROTOCOL,
        edits: [[
            '    try {\n' +
            '        if (!Object.keys(value).every(isPresenceMemberKey)) return false\n',
            '    void (value as { id?: unknown }).id\n' +
            '    try {\n' +
            '        if (!Object.keys(value).every(isPresenceMemberKey)) return false\n',
        ]],
        killedBy:
            'isPresenceMemberWire answers false, and never throws, for a live Proxy',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#353 — uninspectable values and the member invariant',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
