/**
 * @fileoverview #370 and #363's mutation battery — `register` is the only way
 * in, one owner object per connection id, and every teardown the framework runs
 * acts only on the owner.
 *
 * The decisions live in these homes, all in `manager.ts`: `#assertAdmissible`
 * (clause 1 retired, clause 2 any different holder); `#assertBound`
 * (`subscribe`'s decider: admissible, then registered); `#isOwner` and its
 * askers — `disconnect`'s object form, its loop, its `finally`, and
 * `handlerHooks.onMessage`; and `handlerHooks.onClose` passing the object.
 *
 * - M1 `#assertBound`'s unregistered throw removed.
 * - M2 the pre-check asks `#assertAdmissible` — only the authorizer's call
 *   count sees it.
 * - M3 clause 2 narrowed back to a retiring holder (the #361 rule).
 * - M4 clause 2 removed.
 * - M5 clause 2 over-widened to any binding — the same object is refused.
 * - M6 clause 1 removed.
 * - M7 `#assertBound` asks the binding before admissibility.
 * - M8 (survives) the post-check asks `#assertAdmissible`.
 * - M9 (survives) `subscribe`'s binding write restored below the caps.
 * - M10 `disconnect`'s object-form owner check removed.
 * - M11 `disconnect`'s `finally` guard removed.
 * - M12 `handlerHooks.onMessage`'s owner gate removed.
 * - M13 `handlerHooks.onClose` passes the id.
 * - M14 `disconnect`'s loop no longer stops once its object lost the id
 *   (#370 review).
 *
 * M8 and M9 are equivalent mutants; the reasons are `#assertBound`'s JSDoc and
 * are not restated here. Every other row was proven LIVE: the harness ran the
 * mutant and its named witness went red, attributed. Every `killedBy` ends in
 * a space, so `W1 ` is not a prefix of `W11`–`W15`.
 *
 * **M4 absorbs #361's N8 (#401).** `disconnect_admission_361.ts` carried a row
 * mutating clause 2's per-field guards before #363 widened them into the one
 * clause M4 mutates here; once widened, that row's edit was byte-identical to
 * this one — same anchor, same deletion. `docs/testing.md`'s subsumption rule
 * covers exactly this: the row is deleted there, not here, and its own
 * paragraph is where the reason is recorded.
 *
 * **Re-anchored for #393.** `disconnect` is no longer `async` (it delegates
 * to a private `#teardown` and returns or joins a promise), so its object-form
 * refusal reads `return Promise.resolve('not-owned')` — M10's anchor moved
 * with it, unchanged in what it deletes. W12 (ii) and W14, M11's and M14's
 * witnesses, no longer race an `evict` against the SAME object's own close to
 * reach "replaced mid-loop" — #393 makes that race join instead, closing the
 * window these two rows used to reach it — so both now write the replacement
 * through the same private-map reflection this suite's `state()` helper
 * already uses. Neither guard's own code moved; only the path to it did.
 *
 * **Re-anchored for #392.** `disconnect` is now a thin public wrapper; the
 * body M10's and M11's edits sit in — the object-form owner check and the
 * loop's `finally` guard — moved verbatim into private methods (`#teardown`
 * and, one level deeper, `#teardownChannels`) `disconnect` delegates to. Both
 * edits are plain source text, unmoved and unchanged, so M10 and M11 still
 * match and still kill the same way; only the surrounding method names moved.
 *
 * ```bash
 * deno task mutate register_only_admission_370
 * ```
 *
 * @module @lockness/realtime/tests/mutations/register_only_admission_370
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../register_only_admission_370.test.ts', import.meta.url).pathname,
    new URL('../disconnect_admission_361.test.ts', import.meta.url).pathname,
]

/** Clause 2 as shipped: any different holder. */
const CLAUSE_2 =
    '        if (bound !== undefined && bound !== connection) {\n' +
    '            throw new ConnectionIdInUseError()\n' +
    '        }\n'

/** `#assertBound`'s body as shipped. */
const BOUND_BODY = '        this.#assertAdmissible(connection)\n' +
    '        if (!this.connections.has(connection.id)) {\n' +
    '            throw new ConnectionNotRegisteredError()\n' +
    '        }\n'

/** The two equivalent rows point here rather than restate the reason. */
const SEE_ASSERT_BOUND =
    "(none — equivalent) — see `#assertBound`'s JSDoc in " +
    'manager.ts, which is the home of why this mutant cannot change behaviour.'

const MUTATIONS: Mutation[] = [
    {
        label: "M1 — #assertBound's unregistered throw removed",
        file: MANAGER,
        edits: [[BOUND_BODY, '        this.#assertAdmissible(connection)\n']],
        killedBy: '#370 W1 ',
    },
    {
        label: 'M2 — the pre-check asks #assertAdmissible',
        file: MANAGER,
        edits: [[
            '        const kind = channelKind(channel)\n' +
            '        this.#assertBound(connection)\n',
            '        const kind = channelKind(channel)\n' +
            '        this.#assertAdmissible(connection)\n',
        ]],
        killedBy: '#370 W1 ',
    },
    {
        label: 'M3 — clause 2 narrowed back to a retiring holder',
        file: MANAGER,
        edits: [[
            CLAUSE_2,
            '        if (bound !== undefined && this.#retired.has(bound)) {\n' +
            '            throw new ConnectionIdInUseError()\n' +
            '        }\n',
        ]],
        killedBy: '#370 W4 ',
    },
    {
        label: 'M4 — clause 2 removed',
        file: MANAGER,
        edits: [[
            '        const bound = this.connections.get(connection.id)\n' +
            CLAUSE_2,
            '',
        ]],
        killedBy: '#370 W4 ',
    },
    {
        label: 'M5 — clause 2 over-widened to any binding',
        file: MANAGER,
        edits: [[
            CLAUSE_2,
            '        if (bound !== undefined) {\n' +
            '            throw new ConnectionIdInUseError()\n' +
            '        }\n',
        ]],
        killedBy: '#370 W6 ',
    },
    {
        label: 'M6 — clause 1 removed',
        file: MANAGER,
        edits: [[
            '        if (this.#retired.has(connection)) {\n' +
            '            throw new ConnectionDisconnectedError(connection.id)\n' +
            '        }\n' +
            '        const bound = this.connections.get(connection.id)\n',
            '        const bound = this.connections.get(connection.id)\n',
        ]],
        killedBy: '#361 W5 ',
    },
    {
        label: 'M7 — #assertBound asks the binding before admissibility',
        file: MANAGER,
        edits: [[
            BOUND_BODY,
            '        if (!this.connections.has(connection.id)) {\n' +
            '            throw new ConnectionNotRegisteredError()\n' +
            '        }\n' +
            '        this.#assertAdmissible(connection)\n',
        ]],
        killedBy: '#370 W9 ',
    },
    {
        label: 'M8 — the post-check asks #assertAdmissible',
        file: MANAGER,
        edits: [[
            "        // authorizer ran. No await from here to the join's adds.\n" +
            '        this.#assertBound(connection)\n',
            "        // authorizer ran. No await from here to the join's adds.\n" +
            '        this.#assertAdmissible(connection)\n',
        ]],
        killedBy: '(none — equivalent)',
        expectSurvival: SEE_ASSERT_BOUND,
    },
    {
        label: "M9 — subscribe's binding write restored below the caps",
        file: MANAGER,
        edits: [[
            '            connection.identity !== null,\n' +
            '        )\n' +
            '\n' +
            '        // `member` is set',
            '            connection.identity !== null,\n' +
            '        )\n' +
            '        this.connections.set(connection.id, connection)\n' +
            '\n' +
            '        // `member` is set',
        ]],
        killedBy: '(none — equivalent)',
        expectSurvival: SEE_ASSERT_BOUND,
    },
    {
        label: "M10 — disconnect's object-form owner check removed",
        file: MANAGER,
        edits: [[
            "        if (typeof target !== 'string' && !this.#isOwner(target)) {\n" +
            "            return Promise.resolve('not-owned')\n" +
            '        }\n',
            '',
        ]],
        killedBy: '#370 W12 (i) ',
    },
    {
        label: "M11 — disconnect's finally guard removed",
        file: MANAGER,
        edits: [[
            '            if (bound !== undefined && this.#isOwner(bound)) {\n' +
            '                this.#channelsByClient.delete(clientId)\n' +
            '                this.connections.delete(clientId)\n' +
            '            }\n',
            '            this.#channelsByClient.delete(clientId)\n' +
            '            this.connections.delete(clientId)\n',
        ]],
        killedBy: '#370 W12 (ii) ',
    },
    {
        label: "M12 — handlerHooks.onMessage's owner gate removed",
        file: MANAGER,
        edits: [[
            '                if (!this.#isOwner(conn)) return\n' +
            '                return userHooks.onMessage?.(conn, data)\n',
            '                return userHooks.onMessage?.(conn, data)\n',
        ]],
        killedBy: '#370 W13 ',
    },
    {
        label: 'M13 — handlerHooks.onClose passes conn.id',
        file: MANAGER,
        edits: [[
            '                    await this.disconnect(conn)\n',
            '                    await this.disconnect(conn.id)\n',
        ]],
        killedBy: '#370 W11 ',
    },
    {
        label:
            "M14 — disconnect's loop no longer stops once its object lost the id",
        file: MANAGER,
        edits: [[
            '                if (bound !== undefined && !this.#isOwner(bound)) break\n',
            '',
        ]],
        killedBy: '#370 W14 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#370/#363 — register is the only way in; teardown acts only on the owner',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
