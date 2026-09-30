/**
 * @fileoverview #414's mutation battery — the presence roster's five keys
 * each get the remedy ADR 016 assigns them: presence and holders fail
 * closed, owned and instances self-heal (`OWNED_HEAL` / `INSTANCES_HEAL`),
 * and `HOLD_MEMBER_SCRIPT` gains an up-front, no-heal presence guard.
 *
 * The decisions live in `drivers/redis.ts`: `OWNED_HEAL` and
 * `INSTANCES_HEAL`'s `TYPE` reads and their five type-gated `DEL` blocks
 * each, shared across `HOLD_MEMBER_SCRIPT`, `RELEASE_MEMBER_SCRIPT` and
 * `DEREGISTER_INSTANCE_SCRIPT`; `OWNED_SET_WRONG_TYPE` /
 * `INSTANCES_SET_WRONG_TYPE`, the two heal WARNs; the bare `HGET` presence
 * guard at the top of `HOLD_MEMBER_SCRIPT`.
 *
 * `OWNED_HEAL` and `INSTANCES_HEAL` are each ONE fragment spliced into
 * multiple scripts, so a mutation to one of their branches, `TYPE` reads or
 * WARN turns every witness that exercises ANY of those splice sites red —
 * `killedBy` names a substring common to all of them (e.g. "self-heals on")
 * rather than picking one arbitrarily.
 *
 * Each row drops one clause a refactor could drop while the rest of the
 * suite stays green. Every row was proven LIVE by the harness run that
 * recorded it: the mutant ran and turned its named witness red (`KILLED`,
 * attributed).
 *
 * ```bash
 * deno task mutate presence_key_guards_414
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_key_guards_414
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'
import { LIVE_BROKER } from '../../../redis/tests/live_broker.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../presence_roster_wrong_type_414.test.ts', import.meta.url)
        .pathname,
]

const MUTATIONS: Mutation[] = [
    // ── OWNED_HEAL's five gated branches ────────────────────────────────────
    {
        label: "N1 OWNED_HEAL's 'string' branch never fires",
        file: REDIS,
        edits: [[
            '    "if ownedKind == \'string\' then",\n',
            '    "if ownedKind == \'strings\' then",\n',
        ]],
        killedBy: 'a string-typed owned set self-heals on',
    },
    {
        label: "N2 OWNED_HEAL's 'list' branch never fires",
        file: REDIS,
        edits: [[
            '    "if ownedKind == \'list\' then",\n',
            '    "if ownedKind == \'lists\' then",\n',
        ]],
        // FakeRedis never models a list; killed only with a live broker up
        // (LOCKNESS_REDIS_INTEGRATION=1) — the fake-only run reports this
        // row SURVIVED, by design (see the file header of the witness).
        killedBy: '#414 (live) a list-typed owned set self-heals',
    },
    {
        label: "N3 OWNED_HEAL's 'hash' branch never fires",
        file: REDIS,
        edits: [[
            '    "if ownedKind == \'hash\' then",\n',
            '    "if ownedKind == \'hashes\' then",\n',
        ]],
        killedBy: 'a hash-typed owned set self-heals on',
    },
    {
        label: "N4 OWNED_HEAL's 'zset' branch never fires",
        file: REDIS,
        edits: [[
            '    "if ownedKind == \'zset\' then",\n',
            '    "if ownedKind == \'zsets\' then",\n',
        ]],
        killedBy: 'a zset-typed owned set self-heals on',
    },
    {
        label: "N5 OWNED_HEAL's 'stream' branch never fires",
        file: REDIS,
        edits: [[
            '    "if ownedKind == \'stream\' then",\n',
            '    "if ownedKind == \'streams\' then",\n',
        ]],
        // Live-broker only, same reasoning as N2.
        killedBy: '#414 (live) a stream-typed owned set self-heals',
    },
    {
        label: "N6 OWNED_HEAL's TYPE read removed: every call throws",
        file: REDIS,
        edits: [[
            "    \"local ownedKind = redis.call('TYPE', owned)['ok']\",\n",
            '',
        ]],
        killedBy: 'a healthy or absent owned/instances key never WARNs',
    },
    {
        label: 'N7 the owned-set heal WARN text changed',
        file: REDIS,
        edits: [[
            "    '(#414); the write completed normally. Prior type:'\n",
            "    '(#414); the write completed normally. Was type:'\n",
        ]],
        killedBy: 'a string-typed owned set self-heals on',
    },
    // ── INSTANCES_HEAL's five gated branches ────────────────────────────────
    {
        label: "N8 INSTANCES_HEAL's 'string' branch never fires",
        file: REDIS,
        edits: [[
            '    "if instancesKind == \'string\' then",\n',
            '    "if instancesKind == \'strings\' then",\n',
        ]],
        killedBy: 'a string-typed instances set self-heals on',
    },
    {
        label: "N9 INSTANCES_HEAL's 'list' branch never fires",
        file: REDIS,
        edits: [[
            '    "if instancesKind == \'list\' then",\n',
            '    "if instancesKind == \'lists\' then",\n',
        ]],
        killedBy: '#414 (live) a list-typed instances set self-heals',
    },
    {
        label: "N10 INSTANCES_HEAL's 'hash' branch never fires",
        file: REDIS,
        edits: [[
            '    "if instancesKind == \'hash\' then",\n',
            '    "if instancesKind == \'hashes\' then",\n',
        ]],
        killedBy: 'a hash-typed instances set self-heals on',
    },
    {
        label: "N11 INSTANCES_HEAL's 'zset' branch never fires",
        file: REDIS,
        edits: [[
            '    "if instancesKind == \'zset\' then",\n',
            '    "if instancesKind == \'zsets\' then",\n',
        ]],
        killedBy: 'a zset-typed instances set self-heals on',
    },
    {
        label: "N12 INSTANCES_HEAL's 'stream' branch never fires",
        file: REDIS,
        edits: [[
            '    "if instancesKind == \'stream\' then",\n',
            '    "if instancesKind == \'streams\' then",\n',
        ]],
        killedBy: '#414 (live) a stream-typed instances set self-heals',
    },
    {
        label: "N13 INSTANCES_HEAL's TYPE read removed: every call throws",
        file: REDIS,
        edits: [[
            "    \"local instancesKind = redis.call('TYPE', instances)['ok']\",\n",
            '',
        ]],
        killedBy: 'a healthy or absent owned/instances key never WARNs',
    },
    {
        label: 'N14 the instances-set heal WARN text changed',
        file: REDIS,
        edits: [[
            "    'healed (#414); the write completed normally. Prior type:'\n",
            "    'healed (#414); the write completed normally. Was type:'\n",
        ]],
        killedBy: 'a string-typed instances set self-heals on',
    },
    // ── The presence guard: fail closed, never healed ───────────────────────
    {
        label: "N15 HOLD_MEMBER_SCRIPT's up-front presence guard dropped",
        file: REDIS,
        // Without it, a presence-only corruption lets the holders HSET
        // commit before the (now second) presence write aborts — the exact
        // orphan the ordering witness checks for.
        edits: [[
            '    "redis.call(\'HGET\', KEYS[1], ARGV[1])",\n' +
            '    "local added = redis.call(\'HSET\', KEYS[2], ARGV[2], ARGV[3])",\n',
            '    "local added = redis.call(\'HSET\', KEYS[2], ARGV[2], ARGV[3])",\n',
        ]],
        killedBy: 'a presence-only corruption aborts BEFORE the holders write',
    },
]

/**
 * The `list` and `stream` rows are killed only against a live broker: the
 * fake never models those Redis types, so without one their witnesses are
 * `ignored`, Deno reports `ok`, and each row would print SURVIVED — a false
 * coverage gap. Offline, this battery runs every other row, NAMES the rows
 * it skipped, and exits 2, which `deno task mutate` reports as PARTIAL (the
 * #248 convention, applied here as it was to #405/#411 in 7c5db671).
 */
const NEEDS_BROKER = (m: Mutation) => (m.killedBy ?? '').includes('(live)')

if (import.meta.main) {
    const skipped = LIVE_BROKER ? [] : MUTATIONS.filter(NEEDS_BROKER)
    const rows = LIVE_BROKER
        ? MUTATIONS
        : MUTATIONS.filter((m) => !NEEDS_BROKER(m))

    const unresolved = await runBattery(
        '#414 — presence/roster keys fail closed, owned/instances self-heal',
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
                '    deno task mutate presence_key_guards_414',
        )
    }

    // Unresolved rows outrank partiality: a real red must not be reported as
    // "could not run".
    Deno.exit(unresolved > 0 ? 1 : skipped.length > 0 ? 2 : 0)
}
