/**
 * @fileoverview #345's mutation battery — a roster slot is held per instance,
 * and only its last holder takes it out.
 *
 * The defect was a slot with one writer's worth of bookkeeping and several
 * writers: instance A's leave, or A's ghost sweep, `HDEL`ed member 7 while B
 * still held it. The remedy spreads one decision across three structures (the
 * presence hash, a holders hash per slot, the owned set) and two scripts, and
 * every clause of those scripts is something a refactor could drop while the
 * single-instance suite stays green. Each row below drops one clause.
 *
 * - The release script's clauses: the presence `HDEL` only at `n == 0`, the
 *   holder's own `HDEL`, the `shown == mine` guard and the copy it guards.
 * - The hold script's clauses: the holders `HSET` and the instance
 *   registration (`SADD instances`, S1b).
 * - The sweep's: it releases with `deadId` through the same script, and it
 *   never `DEL`s the owned set, so a hold landing between its owned-set read
 *   and its end stays sweepable (S1c). Its three rows were re-anchored for
 *   #355, whose sweep asks for the liveness check and deregisters through a
 *   script instead of a raw `SREM`; each was re-proven live. The #355
 *   review moved the sweep's writes into `#sweepOwned`, one indent
 *   shallower: all three re-anchored again and re-proven live. #358 moved
 *   the per-entry body verbatim into `#sweepPage`, at the same indentation,
 *   so no anchor moved; the S1c row's `killedBy` follows the renamed test.
 * - The key layout: the holders key names the slot, not just the channel.
 * - The decoder: 1 → true, 0 → false, anything else throws (FR-004a).
 *
 * Every script row runs through `FakeRedis`'s shared Lua evaluator, so the
 * mutated script text is what executes. Every row was proven LIVE by the
 * harness run that recorded it: the mutant ran and turned its named witness
 * red.
 *
 * ```bash
 * deno task mutate presence_member_holds_345
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_member_holds_345
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
// Every row dies to the #345 witness file alone; no other suite is run.
const SUITES = [
    new URL('../roster_holders_345.test.ts', import.meta.url).pathname,
]

/** The ghost sweep's one release call (#355: it asks for the liveness check). */
const SWEEP_RELEASE = '            const outcome = await this.#release(\n' +
    '                channel,\n' +
    '                field,\n' +
    '                deadId,\n' +
    '                true,\n' +
    '            )\n'

const MUTATIONS: Mutation[] = [
    // ── the release script ─────────────────────────────────────────────────
    {
        label: '#345 release deletes the presence field unconditionally',
        file: REDIS,
        edits: [[
            '    "local n = redis.call(\'HLEN\', KEYS[2])",\n' +
            "    'if n == 0 then',\n" +
            '    "  redis.call(\'HDEL\', KEYS[1], ARGV[1])",\n',
            '    "redis.call(\'HDEL\', KEYS[1], ARGV[1])",\n' +
            '    "local n = redis.call(\'HLEN\', KEYS[2])",\n' +
            "    'if n == 0 then',\n",
        ]],
        // The shipped defect inside the new script: A's release takes 7 out of
        // the roster while B still holds it.
        killedBy: '#345 W1 A and B hold 7, A releases',
    },
    {
        label: "#345 release skips the releaser's holders HDEL",
        file: REDIS,
        edits: [[
            '    "redis.call(\'HDEL\', KEYS[2], ARGV[2])",\n',
            '',
        ]],
        // The holder count never reaches 0, so the last release leaves the
        // slot, and its holders hash, behind forever.
        killedBy: '#345 W3 both release',
    },
    {
        label: '#345 the `shown == mine` guard is dropped (always copy)',
        file: REDIS,
        edits: [["    'if shown == mine then',\n", "    'if true then',\n"]],
        // Releasing a holder whose entry is NOT shown overwrites the shown
        // info with a random remaining holder's.
        killedBy:
            '#345 W5 with three holders, releasing a holder that is NOT shown',
    },
    {
        label:
            "#345 the copy branch is dropped (a departed holder's info stays)",
        file: REDIS,
        edits: [[
            "    'if shown == mine then',\n" +
            "    \"  local promoted = redis.call('HRANDFIELD', KEYS[2], 1, 'WITHVALUES')\",\n" +
            '    "  redis.call(\'HSET\', KEYS[1], ARGV[1], promoted[2])",\n' +
            "    'end',\n",
            '',
        ]],
        killedBy:
            '#345 W5 with two holders, releasing the shown one shows the other exactly',
    },
    // ── the hold script ────────────────────────────────────────────────────
    {
        label: '#345 hold skips the holders HSET',
        file: REDIS,
        edits: [[
            '    "local added = redis.call(\'HSET\', KEYS[2], ARGV[2], ARGV[3])",\n',
            '    "local added = 1",\n',
        ]],
        // No holder is ever recorded: the first release finds `n == 0` and
        // deletes a slot the other instance still holds.
        killedBy: '#345 W1 A and B hold 7, A releases',
    },
    {
        // Re-anchored for #414: the instances key is bound to a local
        // (`instances`) rather than spelled `KEYS[4]` at the SADD site.
        label: '#345 hold skips registering its instance (SADD instances)',
        file: REDIS,
        edits: [['    "redis.call(\'SADD\', instances, ARGV[2])",\n', '']],
        // Registration is left to the heartbeat alone: an instance whose
        // heartbeat never landed holds slots no sweep can ever find.
        killedBy:
            '#345 S1b a hold by an instance whose registration never landed registers it',
    },
    // ── the key layout ─────────────────────────────────────────────────────
    {
        label: '#345 the holders key omits the member field',
        file: REDIS,
        edits: [[
            'holders:${channel}${OWNED_SEP}${\n' +
            '            String(id)\n' +
            '        }`',
            'holders:${channel}`',
        ]],
        // Every slot of a channel shares one holders hash: releasing 8 counts
        // 7's holder and leaves… or empties the wrong slot.
        killedBy:
            '#345 W8 one instance holding 7 and 8 releases 8 — 7 is still there',
    },
    // ── the sweep ──────────────────────────────────────────────────────────
    {
        label: '#345 the sweep goes back to a raw presence HDEL',
        file: REDIS,
        // Re-anchored for #348, then #355: the sweep keeps the release's
        // decoded outcome; the raw `HDEL` has none, so the mutant reads it as
        // *absent* and reports nothing.
        edits: [[
            SWEEP_RELEASE,
            "            await this.command.command('HDEL', this.presenceKey(channel), field)\n" +
            "            const outcome = { kind: 'absent' } as ReleaseOutcome\n",
        ]],
        // The original #345 defect on the crash path: B keeps holding 7, the
        // sweep of A deletes it anyway.
        killedBy: '#345 W2 B wrote last, A lapses, B sweeps A',
    },
    {
        label:
            "#345 the sweep releases with its OWN id instead of the dead one's",
        file: REDIS,
        // Re-anchored for #355: the sweep's release call now also asks for the
        // liveness check, and `KEYS[4]` follows the releaser — so releasing as
        // itself, the live sweeper is refused and releases nothing.
        edits: [[
            SWEEP_RELEASE,
            SWEEP_RELEASE.replace(
                '                deadId,\n',
                '                this.instanceId,\n',
            ),
        ]],
        // B's sweep of A never releases A's hold: a crashed holder's slot
        // outlives the instance declared dead.
        killedBy:
            '#345 W7 a 0.3.0 field with no holders hash is still reclaimed',
    },
    {
        label: "#345 the sweep DELs the dead instance's owned set again",
        file: REDIS,
        // Re-anchored for #355: the raw `SREM` it sat before is now the
        // deregistration script; the `DEL` goes in front of that call.
        edits: [[
            '        const deregistration = decodeDeregisterReply(\n',
            "        await this.command.command('DEL', this.ownedKey(deadId))\n" +
            '        const deregistration = decodeDeregisterReply(\n',
        ]],
        // A hold landing between the sweep's owned-set read and its end loses its
        // owned entry, and the next sweep can no longer reach it.
        killedBy:
            "#345 S1c a hold landing between a sweep's owned-set read and its end stays in the owned set",
    },
    // ── the decoder ────────────────────────────────────────────────────────
    {
        // Re-anchored for #414: the reply widened to {arrived, ownedKind,
        // instancesKind}, so the strict comparisons are now two compound
        // conditions rather than two bare ones. FR-004a's third row (a
        // well-formed triple with `arrived` outside 0/1) is what reaches
        // this exact line under the mutant; the first two rows never do.
        label: '#345 the transition decoder reads any truthy reply as true',
        file: REDIS,
        edits: [[
            '    if (\n' +
            '        arrived === 1 && ownedKind !== undefined && instancesKind !== undefined\n' +
            '    ) {\n' +
            '        return { arrived: true, ownedKind, instancesKind }\n' +
            '    }\n' +
            '    if (\n' +
            '        arrived === 0 && ownedKind !== undefined && instancesKind !== undefined\n' +
            '    ) {\n' +
            '        return { arrived: false, ownedKind, instancesKind }\n' +
            '    }\n',
            '    if (\n' +
            '        arrived && ownedKind !== undefined && instancesKind !== undefined\n' +
            '    ) {\n' +
            '        return { arrived: true, ownedKind, instancesKind }\n' +
            '    }\n' +
            '    if (\n' +
            '        !arrived && ownedKind !== undefined && instancesKind !== undefined\n' +
            '    ) {\n' +
            '        return { arrived: false, ownedKind, instancesKind }\n' +
            '    }\n',
        ]],
        // An integer 2, an error string or an array reads as an arrival (or as
        // nothing) instead of throwing — and the manager announces from it.
        killedBy: '#345 FR-004a',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#345 — a roster slot is held per instance',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
