/**
 * @fileoverview #347's mutation battery — an authorizer result outside its
 * contract never admits, and is refused before anything is written.
 *
 * Each row puts back one way the admission rule can be wrong: the pre-#347
 * `result === false` gate (M1), each of the three clauses of "an admitting
 * object" (M2, M3, M6), the refusal folded into a deny (M4), the Laravel
 * truthiness rule the maintainer declined (M5), the classification moved behind
 * the caps check (M7; its `connections` write went with #370), and the value echoed into the
 * message (M8). Every row must die on a `#347` test naming
 * `AuthorizeResultError` or the type-label wording.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/authorize_result_347.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/authorize_result_347
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const CHANNEL = new URL('../../channel.ts', import.meta.url)
const SUITES = [
    new URL('../authorize_result_347.test.ts', import.meta.url).pathname,
]

const CLASSIFY =
    '            const verdict: AuthorizeVerdict = classifyAuthorizeResult(result)\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the pre-#347 gate: only `result === false` denies',
        file: MANAGER,
        edits: [[
            CLASSIFY,
            '            const verdict = (result === false\n' +
            "                ? { verdict: 'deny' }\n" +
            "                : { verdict: 'admit', member: result === true ? undefined : result }\n" +
            '            ) as AuthorizeVerdict\n',
        ]],
        // `as`, not an annotation: an annotated `const` is narrowed to its
        // initializer, so the `'invalid'` branch below would stop compiling
        // and the row would report DEAD rather than measure anything.
        // `undefined` on a private channel admitted — the shipped defect.
        killedBy: 'private-orders: an authorizer returning undefined throws',
    },
    {
        label: 'M2 — the object check dropped: any non-boolean admits',
        file: CHANNEL,
        // RE-EXPRESSED by #353: `objectLabel` takes an `object`, so deleting
        // the guard alone no longer type-checks (a DEAD row). The cast keeps
        // the mutant compiling; `objectLabel` answers 'object' for a primitive
        // or `null`, so every non-boolean admits, as before.
        edits: [[
            "    if (typeof value !== 'object' || value === null) return false\n" +
            "    return objectLabel(value) === 'object'\n",
            "    return objectLabel(value as object) === 'object'\n",
        ]],
        killedBy: 'private-orders: an authorizer returning null throws',
    },
    {
        label: 'M3 — arrays admitted, the shape of an empty query result',
        file: CHANNEL,
        // RE-ANCHORED by #353: the array and boxed-primitive checks moved
        // into `objectLabel`, which the classifier and `typeLabel` share.
        edits: [[
            "        if (Array.isArray(value)) return 'array'\n",
            '',
        ]],
        killedBy: 'private-orders: an authorizer returning [] throws',
    },
    {
        label: 'M4 — an invalid result folded into { ok: false }',
        file: MANAGER,
        edits: [[
            '                throw new AuthorizeResultError(channel, verdict.type)\n',
            '                return { ok: false }\n',
        ]],
        // The rejected shape: a missing `return` becomes a deny-all nobody
        // can diagnose, and a deny-only suite stays green on it.
        killedBy: 'presence-room: an authorizer returning undefined throws',
    },
    {
        label: 'M5 — Laravel truthiness: falsy denies, truthy admits',
        file: MANAGER,
        edits: [[
            CLASSIFY,
            '            const verdict = (result\n' +
            "                ? { verdict: 'admit', member: result === true ? undefined : result }\n" +
            "                : { verdict: 'deny' }\n" +
            '            ) as AuthorizeVerdict\n',
        ]],
        // Both halves are wrong: `'no'` and `1` admit, and `undefined` denies
        // quietly where the maintainer decided it throws.
        killedBy: "private-orders: an authorizer returning 'no' throws",
    },
    {
        label: 'M6 — boxed primitives admitted as objects',
        file: CHANNEL,
        // RE-ANCHORED by #353 (see M3).
        edits: [[
            "        return boxedPrimitiveLabel(value) ?? 'object'\n",
            "        return 'object'\n",
        ]],
        killedBy:
            'private-orders: an authorizer returning new Boolean(false) throws',
    },
    {
        // RE-ANCHORED by #370: `subscribe` no longer writes `connections`, so
        // the anchor is the caps check alone. Its killer is unchanged.
        label: 'M7 — the caps moved ahead of the classification',
        file: MANAGER,
        edits: [
            [
                '        this.#checkChannelCaps(\n' +
                '            channel,\n' +
                '            connection.id,\n' +
                '            connection.identity !== null,\n' +
                '        )\n',
                '',
            ],
            [
                '        let member: PresenceMember | undefined\n' +
                "        if (kind !== 'public') {\n",
                '        this.#checkChannelCaps(\n' +
                '            channel,\n' +
                '            connection.id,\n' +
                '            connection.identity !== null,\n' +
                '        )\n' +
                '        let member: PresenceMember | undefined\n' +
                "        if (kind !== 'public') {\n",
            ],
        ],
        killedBy:
            'classified BEFORE the caps: a full connection gets AuthorizeResultError',
    },
    {
        label: 'M8 — the value echoed into the message beside its type',
        file: CHANNEL,
        edits: [[
            "    return { verdict: 'invalid', type: typeLabel(result) }\n",
            "    return { verdict: 'invalid', type: `${typeLabel(result)} ${String(result)}` }\n",
        ]],
        killedBy:
            'the message names the type and the channel, and never echoes',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#347 — the authorizer result contract',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
