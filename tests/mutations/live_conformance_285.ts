/**
 * @fileoverview #285's mutation battery — proving the differential suite bites.
 *
 * **A differential suite that cannot detect a divergence is worse than none**,
 * because it looks like coverage. The first version of
 * `live_fake_conformance.test.ts` was exactly that: `runNamespace()` mints a
 * fresh namespace per call, so calling it per key gave every step its own key,
 * nothing accumulated, and the fake and the broker agreed trivially on every
 * reply. It passed while detecting nothing, and only reverting real
 * divergences by hand found it. This file makes that check repeatable.
 *
 * Every row restores a divergence that was actually present in this repository
 * — six in `fake_redis.ts` that #280 fixed, and two in the Lua subset that
 * stands in for a real interpreter. The last one earned its place: comparing
 * the SET of revoked ids could not see a wrong clock source, because membership
 * is identical either way while every expiry is wrong.
 *
 * One row guards a model instead of restoring a past bug: #358 L1, a scan core
 * that skips a member present for the whole iteration — the one SCAN guarantee
 * the ghost sweep relies on, checked against the broker by #358 WC's union.
 * Another guards the comparison itself: #359 L3, a fake `HGETALL` that pairs
 * each field with another field's value. Only `sortedPairs` sees it — a flat
 * element sort puts both replies in the same order and hides it.
 *
 * **Requires a live broker.** Without one the suite is `ignored`, which the
 * harness reads as green — so every row would "survive" and the file would
 * report a catastrophe that is really just a missing service. It refuses to
 * start instead.
 *
 * ```bash
 * LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \
 *   deno run -A packages/realtime/tests/mutations/live_conformance_285.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/live_conformance_285
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'
import { LIVE_BROKER } from '../../../redis/tests/live_broker.ts'

const FAKE = new URL('../fake_redis.ts', import.meta.url)
const LUA = new URL('../../../redis/tests/lua_eval.ts', import.meta.url)
const SUITES = [
    new URL('../live_fake_conformance.test.ts', import.meta.url).pathname,
]

if (!LIVE_BROKER) {
    console.error(
        'This battery needs a live broker. Without one the suite it mutates is ' +
            '`ignored`, the harness reads that as green, and every row reports ' +
            'SURVIVED — a false catastrophe.\n\n' +
            '  LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \\\n' +
            '    deno run -A packages/realtime/tests/mutations/live_conformance_285.ts',
    )
    Deno.exit(2)
}

const MUTATIONS: Mutation[] = [
    {
        label: 'ZADD GT stops refusing a lower score — the #276 guarantee',
        file: FAKE,
        edits: [[
            'if (existing === undefined || !gt || score > existing) {',
            'if (true) {',
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'ZRANGEBYSCORE breaks ties by insertion order',
        file: FAKE,
        edits: [['a[1] - b[1] || (a[0] < b[0] ? -1 : 1)', 'a[1] - b[1]']],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'EXPIRE GT arms a key with no TTL — the #276 bug verbatim',
        file: FAKE,
        edits: [[
            'if (gt && (current === undefined || at <= current))',
            'if (gt && current !== undefined && at <= current)',
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'DEL takes only the first key',
        file: FAKE,
        edits: [[
            'for (const key of rest) if (this.#dropKey(key)) removed++',
            'for (const key of rest.slice(0, 1)) if (this.#dropKey(key)) removed++',
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'an emptied hash lingers as an existing key',
        file: FAKE,
        edits: [[
            'for (const field of fields) if (h?.delete(field)) removed++\n                this.#dropIfEmpty(key)',
            'for (const field of fields) if (h?.delete(field)) removed++',
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'ZADD writes only the first score/member pair',
        file: FAKE,
        edits: [[
            'for (let i = 0; i < pairs.length; i += 2) {\n                    const score = Number(pairs[i])',
            'for (let i = 0; i < 2; i += 2) {\n                    const score = Number(pairs[i])',
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'EXPIRE arms a key that does not exist',
        file: FAKE,
        edits: [[
            "if (!this.#keyExists(key)) return { type: 'integer', value: 0 }",
            "if (false) return { type: 'integer', value: 0 }",
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label: 'SMEMBERS invents a member for a missing key',
        file: FAKE,
        edits: [[
            'const set = this.#sets.get(rest[0])',
            "const set = this.#sets.get(rest[0]) ?? new Set(['ghost'])",
        ]],
        killedBy: 'the fake answers as a real broker does',
    },
    {
        label:
            "the Lua subset drops a statement — MARK_REVOKED_SCRIPT's ZADD never runs",
        file: LUA,
        // Re-anchored when the evaluator began parsing every expression up
        // front: a bare call statement is now its own node, and dropping it is
        // skipping its evaluation — no fall-through to a refusal any more.
        edits: [[
            '            evaluate(node.call)\n',
            '            void node.call\n',
        ]],
        killedBy: 'the revocation scripts agree',
    },
    {
        label: 'Lua [n] indexing off by one — TIME[1] reads microseconds',
        file: LUA,
        edits: [[
            'return result[Number(index) - 1]',
            'return result[Number(index)]',
        ]],
        killedBy: 'the revocation scripts agree',
    },
    {
        // (#358 L1) The scan core skips the member in each page's first slot:
        // an off-by-one that breaks the one SCAN guarantee the sweep relies
        // on — a member present for the whole iteration is returned — while
        // every page still looks well formed.
        label: 'the fake scan core skips a member present throughout',
        file: FAKE,
        edits: [[
            '            .filter(({ slot }) => slot >= from && slot < to)\n',
            '            .filter(({ slot }) => slot > from && slot < to)\n',
        ]],
        killedBy: '#358 WC a full SSCAN iteration',
    },
    {
        // (#359 L1) The fake's ZSCAN ARM drops one record before handing the
        // sorted set to the scan core — a revocation present for the whole
        // iteration never returned, while every page still looks well formed.
        // The core's own skip is #358 L1 above and is not duplicated.
        label: 'the fake ZSCAN arm drops a record present throughout',
        file: FAKE,
        edits: [[
            'const page = this.#scan(key, [...zset.keys()], cursor, pageSize)',
            'const page = this.#scan(key, [...zset.keys()].slice(1), cursor, pageSize)',
        ]],
        killedBy: '#359 WC a full ZSCAN iteration',
    },
    {
        // (#359 L2) An integral score written with a decimal point: every
        // score the pass reads would fall outside its epoch-seconds grammar,
        // so every revocation would be skipped — on the fake only.
        label: 'the fake formats an integral score with a decimal point',
        file: FAKE,
        edits: [[
            '        return String(score)\n',
            '        return `${score}.0`\n',
        ]],
        killedBy: '#359 WC a score MARK_REVOKED_SCRIPT writes reads back',
    },
    {
        // (#359 L3) Guards `sortedPairs`, not the fake: the fake's HGETALL
        // answers every field with the value of another, a permutation that
        // keeps the multiset of elements. The #285 normalizer sorts a flat
        // pair list BY PAIR, so the broker's `a 9 b 2 c 3` and this `a 3 b 2
        // c 9` diverge; sorted element by element (`sortedItems`) both read
        // `2 3 9 a b c` and the row survives. Nothing else compares HGETALL
        // across backends, so this kill rests on the pair sort alone.
        label: "the fake HGETALL pairs each field with another field's value",
        file: FAKE,
        edits: [[
            '                for (const [field, value] of h ?? []) {\n' +
            "                    flat.push({ type: 'bulk', value: field })\n" +
            "                    flat.push({ type: 'bulk', value })\n",
            '                const values = [...(h?.values() ?? [])].reverse()\n' +
            '                for (const [field] of h ?? []) {\n' +
            "                    flat.push({ type: 'bulk', value: field })\n" +
            "                    flat.push({ type: 'bulk', value: values.shift() ?? '' })\n",
        ]],
        killedBy: '#285 the fake answers as a real broker does',
    },
]

Deno.exit(
    await runBattery(
        '#285 mutation battery — does the differential suite actually bite?',
        SUITES,
        MUTATIONS,
    ),
)
