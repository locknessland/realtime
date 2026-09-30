/**
 * @fileoverview #405's mutation battery — a wrong-typed revocation-floor key
 * self-heals inside `FLOOR_WRITE`'s own atomic `EVAL`.
 *
 * The decisions live in `drivers/redis.ts`: `FLOOR_WRITE`'s `TYPE` read and its
 * five type-gated `DEL` blocks; `decodeReapReply`, widened to the `{t, kind}`
 * pair (#411: `{t, indexKind, floorKind}` triple, `kind` renamed `floorKind`);
 * `REVOCATION_FLOOR_WRONG_TYPE`, the one heal WARN.
 *
 * Each row drops one clause a refactor could drop while the rest of the suite
 * stays green. Every row was proven LIVE by the harness run that recorded it:
 * the mutant ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_floor_wrong_type_405
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_floor_wrong_type_405
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'
import { LIVE_BROKER } from '../../../redis/tests/live_broker.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_floor_wrong_type_405.test.ts', import.meta.url)
        .pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: "N1 the 'string' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if kind == \'string\' then",\n',
            '    "if kind == \'strings\' then",\n',
        ]],
        killedBy: '#405 a string-typed revocation floor self-heals',
    },
    {
        label: "N2 the 'list' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if kind == \'list\' then",\n',
            '    "if kind == \'lists\' then",\n',
        ]],
        // FakeRedis never models a list; killed only with a live broker up
        // (LOCKNESS_REDIS_INTEGRATION=1) — the fake-only run reports this row
        // SURVIVED, by design (see the file header).
        killedBy: '#405 (live) a list-typed revocation floor self-heals',
    },
    {
        label: "N3 the 'set' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if kind == \'set\' then",\n',
            '    "if kind == \'sets\' then",\n',
        ]],
        killedBy: '#405 a set-typed revocation floor self-heals',
    },
    {
        label: "N4 the 'hash' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if kind == \'hash\' then",\n',
            '    "if kind == \'hashes\' then",\n',
        ]],
        killedBy: '#405 a hash-typed revocation floor self-heals',
    },
    {
        label: "N5 the 'stream' heal branch never fires",
        file: REDIS,
        edits: [[
            '    "if kind == \'stream\' then",\n',
            '    "if kind == \'streams\' then",\n',
        ]],
        // Live-broker only, same reasoning as N2.
        killedBy: '#405 (live) a stream-typed revocation floor self-heals',
    },
    {
        label: 'N6 the TYPE read removed: every FLOOR_WRITE call throws',
        file: REDIS,
        edits: [[
            "    \"local kind = redis.call('TYPE', floor)['ok']\",\n",
            '',
        ]],
        killedBy: '#405 an absent or already-healthy floor never WARNs',
    },
    {
        // Re-anchored after #411 widened the reap's decode from a {t, kind}
        // pair to a {t, indexKind, floorKind} triple: `kind` is now
        // `floorKind`, and the source moved, but the guard this row proves —
        // the floor's own field misread — remains.
        label:
            "N7 decodeReapReply's triple-decode inverted: indexKind and floorKind swapped",
        file: REDIS,
        edits: [[
            '    const indexKind = asBulk(items[1])\n' +
            '    const floorKind = asBulk(items[2])\n',
            '    const indexKind = asBulk(items[2])\n' +
            '    const floorKind = asBulk(items[1])\n',
        ]],
        killedBy: '#405 a string-typed revocation floor self-heals',
    },
    {
        label: 'N8 the heal WARN text changed',
        file: REDIS,
        edits: [[
            "    'healed (#405); the pass completed normally. Prior type:'\n",
            "    'healed (#405); the pass completed normally. Was type:'\n",
        ]],
        killedBy: '#405 a string-typed revocation floor self-heals',
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
        "#405 — a wrong-typed revocation floor self-heals inside FLOOR_WRITE's EVAL",
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
                '    deno task mutate revocation_floor_wrong_type_405',
        )
    }

    // Unresolved rows outrank partiality: a real red must not be reported as
    // "could not run".
    Deno.exit(unresolved > 0 ? 1 : skipped.length > 0 ? 2 : 0)
}
