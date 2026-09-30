/**
 * @fileoverview #411's mutation battery — a wrong-typed revocation-INDEX key
 * self-heals inside `REAP_REVOKED_SCRIPT`'s and `MARK_REVOKED_SCRIPT`'s own
 * atomic `EVAL`s, the `INDEX_HEAL` fragment shared by both.
 *
 * The decisions live in `drivers/redis.ts`: `INDEX_HEAL`'s `TYPE` read and its
 * five type-gated `DEL` blocks, shared by `REAP_REVOKED_SCRIPT` and
 * `MARK_REVOKED_SCRIPT`; `REVOCATION_INDEX_WRONG_TYPE`, the one heal WARN;
 * `decodeAnnounceReply`, the folded LOW's strict decode.
 *
 * `INDEX_HEAL` is ONE fragment spliced into BOTH scripts, so a mutation to one
 * of its branches, its `TYPE` read or the WARN it feeds turns BOTH the reap
 * and the mark witness red together — `killedBy` names a substring common to
 * both test names (`#411 a <kind>-typed revocation index self-heals`, which
 * matches `… on the reap: …` and `… on the mark: …` alike) rather than
 * picking one arbitrarily.
 *
 * Each row drops one clause a refactor could drop while the rest of the suite
 * stays green. Every row was proven LIVE by the harness run that recorded it:
 * the mutant ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_index_wrong_type_411
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_index_wrong_type_411
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'
import { LIVE_BROKER } from '../../../redis/tests/live_broker.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_index_wrong_type_411.test.ts', import.meta.url)
        .pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: "N1 the 'string' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if indexKind == \'string\' then",\n',
            '    "if indexKind == \'strings\' then",\n',
        ]],
        killedBy: '#411 a string-typed revocation index self-heals',
    },
    {
        label: "N2 the 'list' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if indexKind == \'list\' then",\n',
            '    "if indexKind == \'lists\' then",\n',
        ]],
        // FakeRedis never models a list; killed only with a live broker up
        // (LOCKNESS_REDIS_INTEGRATION=1) — the fake-only run reports this row
        // SURVIVED, by design (see the file header).
        killedBy: '#411 (live) a list-typed revocation index self-heals',
    },
    {
        label: "N3 the 'set' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if indexKind == \'set\' then",\n',
            '    "if indexKind == \'sets\' then",\n',
        ]],
        killedBy: '#411 a set-typed revocation index self-heals',
    },
    {
        label: "N4 the 'hash' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if indexKind == \'hash\' then",\n',
            '    "if indexKind == \'hashes\' then",\n',
        ]],
        killedBy: '#411 a hash-typed revocation index self-heals',
    },
    {
        label: "N5 the 'stream' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if indexKind == \'stream\' then",\n',
            '    "if indexKind == \'streams\' then",\n',
        ]],
        // Live-broker only, same reasoning as N2.
        killedBy: '#411 (live) a stream-typed revocation index self-heals',
    },
    {
        label: 'N6 the TYPE read removed: every INDEX_HEAL call throws',
        file: REDIS,
        edits: [[
            "    \"local indexKind = redis.call('TYPE', index)['ok']\",\n",
            '',
        ]],
        killedBy: '#411 an absent or already-healthy index never WARNs',
    },
    {
        label: 'N7 the heal WARN text changed',
        file: REDIS,
        edits: [[
            "    'healed (#411); the pass completed normally. Prior type:'\n",
            "    'healed (#411); the pass completed normally. Was type:'\n",
        ]],
        killedBy: '#411 a string-typed revocation index self-heals',
    },
    {
        label:
            'N8 the announce decode LOW regresses: a non-bulk reply is silently accepted again',
        file: REDIS,
        edits: [[
            '    const kind = asBulk(reply)\n' +
            '    if (kind === undefined) throw new Error(ANNOUNCE_REPLY_REFUSED)\n' +
            '    return kind\n',
            '    return asBulk(reply) as string\n',
        ]],
        killedBy: '#411 the floor announce refuses a non-bulk reply',
    },
]

/**
 * The `list` and `stream` rows are killed only against a live broker: the
 * fake never models those Redis types, so without one their witnesses are
 * `ignored`, Deno reports `ok`, and each row would print SURVIVED — a false
 * coverage gap. Offline, this battery runs every other row, NAMES the rows
 * it skipped, and exits 2, which `deno task mutate` reports as PARTIAL (the
 * #248 convention).
 */
const NEEDS_BROKER = (m: Mutation) => (m.killedBy ?? '').includes('(live)')

if (import.meta.main) {
    const skipped = LIVE_BROKER ? [] : MUTATIONS.filter(NEEDS_BROKER)
    const rows = LIVE_BROKER
        ? MUTATIONS
        : MUTATIONS.filter((m) => !NEEDS_BROKER(m))

    const unresolved = await runBattery(
        "#411 — a wrong-typed revocation index self-heals inside INDEX_HEAL's EVAL",
        SUITES,
        rows,
    )

    if (skipped.length > 0) {
        console.error(
            `\nPARTIAL — ${skipped.length} row(s) NOT run, they need a live broker:`,
        )
        for (const m of skipped) console.error(`  - ${m.label}`)
        console.error(
            '\n  LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \\\n' +
                '    deno task mutate revocation_index_wrong_type_411',
        )
    }

    // Unresolved rows outrank partiality: a real red must not be reported as
    // "could not run".
    Deno.exit(unresolved > 0 ? 1 : skipped.length > 0 ? 2 : 0)
}
