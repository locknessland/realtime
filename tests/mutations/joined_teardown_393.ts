/**
 * @fileoverview #393's mutation battery — one teardown per object: `#retired`
 * is a `WeakMap<Connection, Promise<DisconnectOutcome>>`, and a second
 * `disconnect` of an already-retiring object joins the first call's promise
 * instead of running its own copy of the reverse-index loop.
 *
 * The decision lives in `disconnect`, all in `manager.ts`: the join
 * early-return, keyed by the object `#teardown` was called with; the
 * synchronous, same-turn write of `#retired` right after `#teardown` is
 * called (so a same-turn double call still finds it); and `#teardown` itself,
 * the extracted loop+`finally` — unchanged from #361/#370/#363's shape,
 * moved rather than rewritten.
 *
 * - (a) the join early-return dropped — a second call always runs its own
 *   teardown again, exactly the pre-#393 behaviour.
 * - (b) `#retired` re-keyed by `clientId` instead of by the object — a
 *   settled teardown's entry never clears (terminal, #361), so a later,
 *   genuinely different object under the same (by-then-free) id silently
 *   joins the OLD object's already-resolved promise instead of running its
 *   own.
 * - (c) `#retired`'s write deferred past a microtask — a same-turn double
 *   call no longer finds it, so it joins nothing and races instead.
 *
 * **Re-anchored for #392.** `disconnect` is now a thin public wrapper that
 * raises an id-form deprecation notice, then delegates to a private
 * `#teardown(target)` — the body described above (the join, the retired-write
 * and the delegation) moved into it verbatim. The "extracted loop+`finally`"
 * this fileoverview calls `#teardown` is, since #392, one level deeper: it is
 * now `#teardownChannels`, called from inside `#teardown`. All three of (a),
 * (b) and (c)'s edits are plain source text, unmoved and unchanged, so they
 * still match and still kill the same way; only the surrounding method names
 * moved.
 *
 * Every row was proven LIVE: the harness ran the mutant and its named witness
 * went red, attributed.
 *
 * ```bash
 * deno task mutate joined_teardown_393
 * ```
 *
 * @module @lockness/realtime/tests/mutations/joined_teardown_393
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../joined_teardown_393.test.ts', import.meta.url).pathname,
]

/** The join early-return, as shipped. */
const JOIN = '        if (bound !== undefined) {\n' +
    '            const joining = this.#retired.get(bound)\n' +
    '            if (joining !== undefined) return joining\n' +
    '        }\n'

/** The synchronous, same-turn write, as shipped. */
const RETIRE_WRITE =
    '        if (bound !== undefined) this.#retired.set(bound, teardown)\n' +
    '        return teardown\n'

/** `#assertAdmissible`'s one read of `#retired`, as shipped. */
const ADMISSIBLE_CHECK = '        if (this.#retired.has(connection)) {\n'

/** The field declaration, as shipped. */
const FIELD_DECL = '    readonly #retired = new WeakMap<\n' +
    '        Connection<Identity>,\n' +
    '        Promise<DisconnectOutcome>\n' +
    '    >()\n'

const MUTATIONS: Mutation[] = [
    {
        label: '(a) — the join early-return dropped',
        file: MANAGER,
        edits: [[JOIN, '']],
        killedBy: '#393 W1 ',
    },
    {
        label: '(b) — #retired re-keyed by clientId instead of by object',
        file: MANAGER,
        edits: [
            [
                FIELD_DECL,
                '    readonly #retired = new Map<\n' +
                '        string,\n' +
                '        Promise<DisconnectOutcome>\n' +
                '    >()\n',
            ],
            [
                JOIN,
                '        if (bound !== undefined) {\n' +
                '            const joining = this.#retired.get(clientId)\n' +
                '            if (joining !== undefined) return joining\n' +
                '        }\n',
            ],
            [
                '        if (bound !== undefined) this.#retired.set(bound, teardown)\n',
                '        if (bound !== undefined) this.#retired.set(clientId, teardown)\n',
            ],
            [
                ADMISSIBLE_CHECK,
                '        if (this.#retired.has(connection.id)) {\n',
            ],
        ],
        killedBy: '#393 W3 ',
    },
    {
        label: "(c) — #retired's write deferred past a microtask",
        file: MANAGER,
        edits: [[
            RETIRE_WRITE,
            '        if (bound !== undefined) {\n' +
            '            const retiring = bound\n' +
            '            Promise.resolve().then(() =>\n' +
            '                this.#retired.set(retiring, teardown)\n' +
            '            )\n' +
            '        }\n' +
            '        return teardown\n',
        ]],
        killedBy: '#393 W1 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#393 — one teardown per object: #retired joins, never races',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
