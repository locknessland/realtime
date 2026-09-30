/**
 * @fileoverview #380's mutation battery — a fleet-wide revocation TTL floor:
 * every durable revocation record outlives the longest live reader's
 * `revocationTtlSeconds`.
 *
 * The decisions live in these homes (plan §5), all in `drivers/redis.ts`:
 * `markRevocation`'s `eff` and its fail-closed floor read;
 * `decodeRevocationFloor` (the reply, the grammar, the clamp);
 * `MAX_REVOCATION_TTL_SECONDS`; `revocationFloorKey`; `FLOOR_WRITE` and its
 * two callers, `REAP_REVOKED_SCRIPT` and `ANNOUNCE_FLOOR_SCRIPT`; the
 * first-registration gate in `onRevocationReconcile`; `#announceFloor` and its
 * retry; `#warnFloor`; and `close()`'s clear of the retry timer.
 *
 * Each row drops one clause a refactor could drop while the rest of the suite
 * stays green. N1–N18 and N20–N33 are killed by
 * `revocation_ttl_floor_380.test.ts`; N19 by `prefix_anchoring.test.ts`.
 * `killedBy` strings end in a space where a shorter witness id is a prefix of
 * a longer one (`F1 ` vs `F10`–`F15`).
 *
 * **Anchors.** `String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS)`
 * appears in both the reap and the announce, `'1',` in every one-key call,
 * and `skipped++` in both revocation decoders, so every row that touches one
 * of them carries a neighbouring line that is unique.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_ttl_floor_380
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_ttl_floor_380
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_ttl_floor_380.test.ts', import.meta.url).pathname,
    new URL('../prefix_anchoring.test.ts', import.meta.url).pathname,
]

/** The clamp, whole. */
const CLAMP = '        const clamped = Math.min(\n' +
    '            Math.max(Number(member), 1),\n' +
    '            MAX_REVOCATION_TTL_SECONDS,\n' +
    '        )\n'

/** The skip WARN after the mark's EVAL. */
const SKIP_WARN = '        if (skipped > 0) {\n' +
    '            this.#warnFloor(`${REVOCATION_FLOOR_SKIPPED} ${skipped}`)\n' +
    '        }\n'

/** The read-failed WARN after the mark's EVAL. */
const READ_FAILED_WARN = '        if (unreadable !== undefined) {\n' +
    '            this.#warnFloor(\n' +
    '                `${REVOCATION_FLOOR_READ_FAILED} ${\n' +
    '                    renderError(unreadable.error)\n' +
    '                }`,\n' +
    '            )\n' +
    '        }\n'

/** The head of the mark's EVAL. Re-anchored after #411: the reply is now captured. */
const MARK_EVAL = '        const reply = await this.command.command(\n' +
    "            'EVAL',\n" +
    '            MARK_REVOKED_SCRIPT,\n'

/** The announce's call, from the method head to the catch's WARN. */
const ANNOUNCE_HEAD =
    '    async #announceFloor(backoffMs: number): Promise<void> {\n' +
    '        try {\n' +
    '            const reply = await this.command.command(\n'

// Re-anchored after #411's folded LOW replaced the bare `asBulk(reply)` read
// with the strict `decodeAnnounceReply`; the announce's own catch is unchanged.
const ANNOUNCE_CATCH = '            )\n' +
    '            this.#warnIfFloorHealed(decodeAnnounceReply(reply))\n' +
    '        } catch (error) {\n' +
    '            this.#warnFloor(\n' +
    '                `${REVOCATION_FLOOR_ANNOUNCE_FAILED} ${renderError(error)}`,\n' +
    '            )\n'

/** The retry, from its delay to its field. */
const RETRY =
    '            const delay = Math.min(backoffMs, this.reconcileIntervalMs)\n' +
    '            const id = setTimeout(() => {\n' +
    '                this.#announceRetry = undefined\n' +
    '                if (this.#closing || this.#lastReadAt !== undefined) return\n' +
    '                void this.#announceFloor(delay * 2)\n' +
    '            }, delay)\n' +
    '            Deno.unrefTimer(id)\n' +
    '            this.#announceRetry = id\n'

/** The announce inside the first-registration gate. */
const ANNOUNCE_IN_GATE =
    '            // The floor announce (#380): the same gate, so a re-registration\n' +
    '            // or a registration after close() announces nothing.\n' +
    '            void this.#announceFloor(FLOOR_ANNOUNCE_RETRY_MS)\n' +
    '        }\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'N1 the mark ignores the floor: eff = own TTL',
        file: REDIS,
        edits: [[
            '            eff = floor.ttl\n',
            '            eff = this.revocationTtlSeconds\n',
        ]],
        killedBy: '#380 F1 ',
    },
    {
        label: 'N2 Math.min instead of Math.max in the decoder',
        file: REDIS,
        edits: [[
            '        ttl = Math.max(ttl, clamped)\n',
            '        ttl = Math.min(ttl, clamped)\n',
        ]],
        killedBy: '#380 F1 ',
    },
    {
        label: "N3 the index EXPIRE keeps the writer's own TTL plus slack",
        file: REDIS,
        edits: [[
            '            member,\n' +
            '            String(eff + INDEX_TTL_SLACK_SECONDS),\n',
            '            member,\n' +
            '            String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),\n',
        ]],
        killedBy: '#380 F7 ',
    },
    {
        label:
            'N4 the decoder accepts by Number.isFinite instead of EPOCH_SECONDS',
        file: REDIS,
        edits: [[
            '        if (!EPOCH_SECONDS.test(member)) {\n' +
            '            skipped++\n',
            '        if (!Number.isFinite(Number(member))) {\n' +
            '            skipped++\n',
        ]],
        killedBy: '#380 F6 ',
    },
    {
        label: "N5 the clamp's upper bound dropped",
        file: REDIS,
        edits: [[
            CLAMP,
            '        const clamped = Math.max(Number(member), 1)\n',
        ]],
        killedBy: '#380 F7 ',
    },
    {
        label: 'N6 the skip count dropped from the WARN',
        file: REDIS,
        edits: [[
            '            this.#warnFloor(`${REVOCATION_FLOOR_SKIPPED} ${skipped}`)\n',
            '            this.#warnFloor(REVOCATION_FLOOR_SKIPPED)\n',
        ]],
        killedBy: '#380 F6 ',
    },
    {
        label: 'N7 a skipped floor member throws instead of being counted',
        file: REDIS,
        edits: [[
            '        if (!EPOCH_SECONDS.test(member)) {\n' +
            '            skipped++\n',
            '        if (!EPOCH_SECONDS.test(member)) {\n' +
            '            throw new Error(REVOCATION_FLOOR_REFUSED)\n',
        ]],
        killedBy: '#380 F6 ',
    },
    {
        label: 'N8 a non-array floor reply decodes to [] instead of throwing',
        file: REDIS,
        edits: [[
            '    if (members === undefined) throw new Error(REVOCATION_FLOOR_REFUSED)\n',
            '    if (members === undefined) return { ttl: ownTtl, skipped: 0 }\n',
        ]],
        killedBy: '#380 F12 ',
    },
    {
        // Re-anchored after #411 spliced INDEX_HEAL in ahead of this line and
        // widened the return to a triple; the FLOOR_WRITE-removal this row
        // proves is unchanged.
        label: 'N9 FLOOR_WRITE removed from REAP_REVOKED_SCRIPT',
        file: REDIS,
        edits: [[
            "    'local keyTtl = ARGV[2]',\n" +
            '    FLOOR_WRITE,\n' +
            "    'return {t, indexKind, kind}',\n",
            "    'local keyTtl = ARGV[2]',\n" +
            "    'return {t, indexKind, kind}',\n",
        ]],
        killedBy: '#380 F3 ',
    },
    {
        label: 'N10 GT dropped from the floor ZADD',
        file: REDIS,
        edits: [[
            `    "redis.call('ZADD', floor, 'GT', t + ttl, ttl)",\n`,
            `    "redis.call('ZADD', floor, t + ttl, ttl)",\n`,
        ]],
        killedBy: '#380 F9 (i)',
    },
    {
        label: 'N11 the floor prune removed',
        file: REDIS,
        edits: [[
            `    "redis.call('ZREMRANGEBYSCORE', floor, '-inf', t)",\n`,
            '',
        ]],
        killedBy: '#380 F8 ',
    },
    {
        label: 'N12 EXPIRE … NX removed: the floor key is never armed',
        file: REDIS,
        edits: [[
            `    "redis.call('EXPIRE', floor, keyTtl, 'NX')",\n`,
            '',
        ]],
        killedBy: '#380 F3 ',
    },
    {
        label: 'N13 EXPIRE … GT removed: the floor key is never extended',
        file: REDIS,
        edits: [[
            `    "redis.call('EXPIRE', floor, keyTtl, 'GT')",\n`,
            '',
        ]],
        killedBy: '#380 F9 (ii)',
    },
    {
        label: 'N14 the announce removed',
        file: REDIS,
        edits: [[
            '            void this.#announceFloor(FLOOR_ANNOUNCE_RETRY_MS)\n',
            '',
        ]],
        killedBy: '#380 F4 (i)',
    },
    {
        label: 'N15 the announce on every registration (out of the first gate)',
        file: REDIS,
        edits: [[
            ANNOUNCE_IN_GATE,
            '        }\n' +
            '        void this.#announceFloor(FLOOR_ANNOUNCE_RETRY_MS)\n',
        ]],
        killedBy: '#380 F4 (ii)',
    },
    {
        label:
            'N16 the announce out of the #closing gate (first registration only)',
        file: REDIS,
        edits: [[
            ANNOUNCE_IN_GATE,
            '        }\n' +
            '        if (first) void this.#announceFloor(FLOOR_ANNOUNCE_RETRY_MS)\n',
        ]],
        killedBy: '#380 F4 (iii)',
    },
    {
        label: "N17 the announce's catch removed",
        file: REDIS,
        edits: [[
            ANNOUNCE_CATCH +
            '            if (this.#closing || this.#lastReadAt !== undefined) return\n' +
            RETRY +
            '        }\n',
            '            )\n' +
            '        } finally {\n' +
            '            void backoffMs\n' +
            '        }\n',
        ]],
        killedBy: '#380 F5 (i)',
    },
    {
        label: "N18 #warnFloor's try and marked fallback removed",
        // Re-anchored for #409: #warnFloor's own try/catch moved into a call
        // to #guardedWarn — its marker (REVOCATION_FLOOR_LOG_FAILED) is
        // unique to this call, so no disambiguation is needed.
        file: REDIS,
        edits: [[
            '        this.#guardedWarn(REVOCATION_FLOOR_LOG_FAILED, line)\n',
            '        console.warn(line)\n',
        ]],
        killedBy: '#380 F5 (ii)',
    },
    {
        label: 'N19 revocationFloorKey built without RESERVED_SEPARATOR_LEAD',
        file: REDIS,
        edits: [[
            '        return `${this.prefix}${RESERVED_SEPARATOR_LEAD}revocation-floor`\n',
            '        return `${this.prefix}:revocation-floor`\n',
        ]],
        killedBy: 'SC-001',
    },
    {
        label: "N20 the reap's floor key TTL drops the slack",
        file: REDIS,
        edits: [[
            '                REAP_REVOKED_SCRIPT,\n' +
            "                '2',\n" +
            '                this.revocationIndexKey,\n' +
            '                this.revocationFloorKey,\n' +
            '                String(this.revocationTtlSeconds),\n' +
            '                String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),\n',
            '                REAP_REVOKED_SCRIPT,\n' +
            "                '2',\n" +
            '                this.revocationIndexKey,\n' +
            '                this.revocationFloorKey,\n' +
            '                String(this.revocationTtlSeconds),\n' +
            '                String(this.revocationTtlSeconds),\n',
        ]],
        killedBy: '#380 F3 ',
    },
    {
        label: "N21 the announce sent in the reap's two-key form",
        file: REDIS,
        edits: [[
            '                ANNOUNCE_FLOOR_SCRIPT,\n' +
            "                '1',\n" +
            '                this.revocationFloorKey,\n',
            '                ANNOUNCE_FLOOR_SCRIPT,\n' +
            "                '2',\n" +
            '                this.revocationIndexKey,\n' +
            '                this.revocationFloorKey,\n',
        ]],
        killedBy: '#380 F4 (v)',
    },
    {
        label: 'N22 the announce retry removed',
        file: REDIS,
        edits: [[RETRY, '            void backoffMs\n']],
        killedBy: '#380 F13 (i)',
    },
    {
        label: 'N23 close() does not clear the announce retry timer',
        file: REDIS,
        edits: [[
            '        if (this.#announceRetry !== undefined) {\n' +
            '            clearTimeout(this.#announceRetry)\n' +
            '            this.#announceRetry = undefined\n' +
            '        }\n',
            '',
        ]],
        killedBy: '#380 F13 (ii)',
    },
    {
        label: 'N24 the first backoff step raised to 5 000 ms',
        file: REDIS,
        edits: [[
            'const FLOOR_ANNOUNCE_RETRY_MS = 1_000\n',
            'const FLOOR_ANNOUNCE_RETRY_MS = 5_000\n',
        ]],
        killedBy: '#380 F13 (i)',
    },
    {
        label: 'N25 a failed floor read fails the mark (re-thrown)',
        file: REDIS,
        edits: [[
            '            unreadable = { error }\n',
            '            throw error\n',
        ]],
        killedBy: '#380 F14 (i)',
    },
    {
        label: "N26 a failed floor read falls back to the writer's own TTL",
        file: REDIS,
        edits: [[
            '        let eff = MAX_REVOCATION_TTL_SECONDS\n',
            '        let eff = this.revocationTtlSeconds\n',
        ]],
        killedBy: '#380 F14 (i)',
    },
    {
        label: 'N27 the skip WARN written before the EVAL',
        file: REDIS,
        edits: [[SKIP_WARN, ''], [MARK_EVAL, SKIP_WARN + MARK_EVAL]],
        killedBy: '#380 F6 ',
    },
    {
        label:
            'N28 #announceFloor made synchronous: command() called outside the try',
        file: REDIS,
        edits: [
            [
                ANNOUNCE_HEAD,
                '    #announceFloor(backoffMs: number): Promise<void> {\n' +
                '        const sent = this.command.command(\n',
            ],
            [
                ANNOUNCE_CATCH,
                '            )\n' +
                '        return (async () => {\n' +
                '        try {\n' +
                '            await sent\n' +
                '        } catch (error) {\n' +
                '            this.#warnFloor(\n' +
                '                `${REVOCATION_FLOOR_ANNOUNCE_FAILED} ${renderError(error)}`,\n' +
                '            )\n',
            ],
            [
                '            this.#announceRetry = id\n' +
                '        }\n' +
                '    }\n',
                '            this.#announceRetry = id\n' +
                '        }\n' +
                '        })()\n' +
                '    }\n',
            ],
        ],
        killedBy: '#380 F5 (iv)',
    },
    // Review folds (#380 review, 2026-09-25): the backoff, the retry's stop
    // checks, and the read-failed WARN's order each get a row.
    {
        label: 'N29 the retry backoff does not double',
        file: REDIS,
        edits: [[
            '                void this.#announceFloor(delay * 2)\n',
            '                void this.#announceFloor(delay)\n',
        ]],
        killedBy: '#380 F16 ',
    },
    {
        label: 'N30 the retry backoff is not capped at reconcileIntervalMs',
        file: REDIS,
        edits: [[
            '            const delay = Math.min(backoffMs, this.reconcileIntervalMs)\n',
            '            const delay = backoffMs\n',
        ]],
        killedBy: '#380 F16 ',
    },
    {
        label: 'N31 a completed pass does not stop the pending retry',
        file: REDIS,
        edits: [[
            '                this.#announceRetry = undefined\n' +
            '                if (this.#closing || this.#lastReadAt !== undefined) return\n',
            '                this.#announceRetry = undefined\n' +
            '                if (this.#closing) return\n',
        ]],
        killedBy: '#380 F5 (iii)',
    },
    {
        label: 'N32 a failure after close() still arms a retry',
        file: REDIS,
        edits: [[
            '            )\n' +
            '            if (this.#closing || this.#lastReadAt !== undefined) return\n' +
            '            const delay',
            '            )\n' +
            '            if (this.#lastReadAt !== undefined) return\n' +
            '            const delay',
        ]],
        killedBy: '#380 F13 (ii)',
    },
    {
        label: 'N33 the read-failed WARN written before the EVAL',
        file: REDIS,
        edits: [[READ_FAILED_WARN, ''], [
            MARK_EVAL,
            READ_FAILED_WARN + MARK_EVAL,
        ]],
        killedBy: '#380 F14 (i)',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#380 — every revocation record outlives the longest live reader TTL',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
