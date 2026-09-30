/**
 * @fileoverview #359's mutation battery — the revocation re-check reads the
 * index in bounded `ZSCAN` pages, one pass at a time, and applies nothing
 * before the enumeration ends.
 *
 * The remedy's decisions live in these homes (plan §5):
 * - `drivers/redis.ts`: `REAP_REVOKED_SCRIPT` (the only delete, and the
 *   pass's one `now`), `REVOCATION_SCAN_COUNT` and the one `ZSCAN` in
 *   `listRevocations` (the page), its loop (one full iteration), its per-entry
 *   `score > t` filter, its two `#closing` checks, the `owns` step and the
 *   skip WARN; `decodeRevocationPage` (a well-formed pair, and the count);
 *   `#armRevocationReconcile` (the one arming site) and `#startRevocationPass`
 *   with `#revocationRerun` (one pass per driver, one trailing pass);
 * - `manager.ts`: the `owns` predicate the re-check hands over, its own
 *   `connections.has` check (the decider), and the serial tail behind
 *   `reconcileRevocations()`.
 *
 * Each row below drops one clause a refactor could drop while the rest of the
 * suite stays green.
 *
 * - M1 the one-reply read restored (`LIST_REVOKED_SCRIPT`'s
 *   `ZRANGEBYSCORE t +inf`); on a live broker R2 also kills it, past the cap.
 * - M2a no `COUNT` on the page read. M2b a `COUNT` other than the constant.
 * - M3 the page loop runs once.
 * - M4 an empty page ends the pass (an inserted `break`).
 * - M5 every `ZSCAN` sends cursor `'0'` — killed through the fake's scan call
 *   ceiling, which turns a pass that never ends into a rejection.
 * - M6 the `score > t` filter removed.
 * - M7 `TIME` re-read for each page (the reap repeated after each read).
 * - M8 liveness judged against `Date.now() / 1000`.
 * - M9 no `#closing` check before a page read.
 * - M10 the matches so far returned when a page read throws.
 * - M11 an odd-length page accepted, its trailing item dropped.
 * - M12 the Redis driver ignores `owns`.
 * - M13 the manager stops handing `owns` over.
 * - M14 the manager's own `connections.has` check deleted — a recorded
 *   equivalent mutant: unreachable for a transport that registers on open;
 *   #370 enforces it for every caller, so no membership can name an id
 *   absent from `connections` and applying there is a no-op (R13 (d), which
 *   built that state, is retired).
 * - M15 the revocation timer back on `setInterval`.
 * - M16 a reconnect during a pass dropped (no rerun recorded).
 * - M17 trailing passes counted instead of coalesced.
 * - M18 `#armRevocationReconcile`'s `#closing` check dropped.
 * - M19 the manager's re-check tail removed.
 * - M20 the tail continued only on success.
 * - M21 a malformed pair skipped without being counted.
 * - M22 the rerun slot keeps the LAST trigger instead of letting
 *   `'reconnect'` win — killed only by the reconnect-then-retry order.
 * - M23 a throw from `owns` swallowed and read as "not mine": the call
 *   returns a partial answer instead of failing.
 *
 * **Anchors.** `cursor = page.cursor`, `let cursor = '0'` / `do {` and the
 * loop's `} while (cursor !== '0')` also exist in `#sweepOwned` (#358), and
 * `if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)` appears twice
 * — before the reap and before each page read. So every anchor that touches
 * the loop carries a revocation-only line (the `REVOCATION_PASS_CLOSING`
 * check, `skipped += page.skipped`, the skip WARN), and M9 anchors on the
 * check PLUS the page-read line. `killedBy` strings end in a space where a
 * shorter witness id is a prefix of a longer one (`R1 ` vs `R10`–`R15`).
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate revocation_paging_359
 * ```
 *
 * @module @lockness/realtime/tests/mutations/revocation_paging_359
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../revocation_paging_359.test.ts', import.meta.url).pathname,
]

/**
 * The reap, from `listRevocations`' first `now` to the reply's decode — in
 * its two-key form since #380 (index, then the revocation floor it also
 * refreshes), widened to the `{t, kind}` pair and the heal WARN since #405,
 * widened again to the `{t, indexKind, floorKind}` triple and the index's own
 * heal WARN since #411.
 * Re-anchored, never deleted: the source moved, the guard remains.
 */
const REAP = '        const { t, indexKind, floorKind } = decodeReapReply(\n' +
    '            await this.command.command(\n' +
    "                'EVAL',\n" +
    '                REAP_REVOKED_SCRIPT,\n' +
    "                '2',\n" +
    '                this.revocationIndexKey,\n' +
    '                this.revocationFloorKey,\n' +
    '                String(this.revocationTtlSeconds),\n' +
    '                String(this.revocationTtlSeconds + INDEX_TTL_SLACK_SECONDS),\n' +
    '            ),\n' +
    '        )\n' +
    '        this.#warnIfIndexHealed(indexKind)\n' +
    '        this.#warnIfFloorHealed(floorKind)\n'

/** The closing check before a page read, plus the read it guards. */
const CLOSING_BEFORE_PAGE_READ =
    '            if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)\n' +
    '            const page = decodeRevocationPage(\n'

/** The one `ZSCAN` read's arguments. */
const PAGE_READ = '            const page = decodeRevocationPage(\n' +
    '                await this.command.command(\n' +
    "                    'ZSCAN',\n" +
    '                    this.revocationIndexKey,\n' +
    '                    cursor,\n' +
    "                    'COUNT',\n" +
    '                    String(REVOCATION_SCAN_COUNT),\n' +
    '                ),\n' +
    '            )\n'

const SCAN_ARGS = "                    'ZSCAN',\n" +
    '                    this.revocationIndexKey,\n' +
    '                    cursor,\n' +
    "                    'COUNT',\n" +
    '                    String(REVOCATION_SCAN_COUNT),\n'

/** The loop's head, on its revocation-only first line. */
const LOOP_HEAD = "        let cursor = '0'\n" +
    '        do {\n' +
    '            if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)\n'

/** The loop's end, on the revocation-only skip WARN after it. */
const LOOP_END = "        } while (cursor !== '0')\n" +
    '        if (skipped > 0) console.warn(`${REVOCATION_PAIRS_SKIPPED} ${skipped}`)\n'

const SKIP_SUM = '            skipped += page.skipped\n'

const FILTER = '                if (!(entry.score > t)) continue\n'

/** The `finally` that ends every revocation pass. */
const RERUN_TAKE = '                const rerun = this.#revocationRerun\n' +
    '                this.#revocationRerun = undefined\n'

const RERUN_RECORD = '            if (this.#revocationRerun !== ' +
    "'reconnect') {\n" +
    '                this.#revocationRerun = trigger\n' +
    '            }\n' +
    '            return\n'

const MUTATIONS: Mutation[] = [
    {
        label:
            "M1 — the one-reply read restored: LIST_REVOKED_SCRIPT's ZRANGEBYSCORE t +inf",
        file: REDIS,
        edits: [
            [
                REAP,
                '        const t = 0\n' +
                '        const all = asArray(\n' +
                '            await this.command.command(\n' +
                "                'EVAL',\n" +
                '                "local t = redis.call(\'TIME\')[1]\\n" +\n' +
                "                    \"redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', t)\\n\" +\n" +
                "                    \"return redis.call('ZRANGEBYSCORE', KEYS[1], t, '+inf')\",\n" +
                "                '1',\n" +
                '                this.revocationIndexKey,\n' +
                '            ),\n' +
                '        ) ?? []\n',
            ],
            [
                PAGE_READ,
                '            const page = {\n' +
                "                cursor: '0',\n" +
                '                skipped: 0,\n' +
                '                entries: all.map((m) => ({\n' +
                '                    member: String(asBulk(m)),\n' +
                '                    score: Infinity,\n' +
                '                })),\n' +
                '            }\n',
            ],
        ],
        killedBy: '#359 R1 ',
    },
    {
        label: 'M2a — the page read sends no COUNT',
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'ZSCAN',\n" +
            '                    this.revocationIndexKey,\n' +
            '                    cursor,\n',
        ]],
        killedBy: '#359 R1 ',
    },
    {
        label:
            'M2b — the page read sends a COUNT other than REVOCATION_SCAN_COUNT',
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'ZSCAN',\n" +
            '                    this.revocationIndexKey,\n' +
            '                    cursor,\n' +
            "                    'COUNT',\n" +
            '                    String(REVOCATION_SCAN_COUNT * 10),\n',
        ]],
        killedBy: '#359 R1 ',
    },
    {
        label: 'M3 — the page loop runs only once',
        file: REDIS,
        edits: [[
            LOOP_END,
            '        } while (false)\n' +
            '        if (skipped > 0) console.warn(`${REVOCATION_PAIRS_SKIPPED} ${skipped}`)\n',
        ]],
        killedBy: '#359 R1 ',
    },
    {
        label: 'M4 — an empty page ends the pass instead of cursor 0',
        file: REDIS,
        edits: [[
            SKIP_SUM,
            '            if (page.entries.length === 0) break\n' + SKIP_SUM,
        ]],
        killedBy: '#359 R7 ',
    },
    {
        label: "M5 — every page read sends cursor '0': the pass never advances",
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'ZSCAN',\n" +
            '                    this.revocationIndexKey,\n' +
            "                    '0',\n" +
            "                    'COUNT',\n" +
            '                    String(REVOCATION_SCAN_COUNT),\n',
        ]],
        killedBy: '#359 R7 ',
    },
    {
        label: 'M6 — the score > t filter removed',
        file: REDIS,
        edits: [[FILTER, '']],
        killedBy: '#359 R3 (a, b)',
    },
    {
        label: 'M7 — TIME re-read for each page: the reap repeated after it',
        file: REDIS,
        edits: [
            [
                REAP,
                REAP.replace(
                    '        const { t, indexKind, floorKind } = ',
                    '        let { t, indexKind, floorKind } = ',
                ),
            ],
            [
                SKIP_SUM,
                '            ;({ t, indexKind, floorKind } = decodeReapReply(\n' +
                '                await this.command.command(\n' +
                "                    'EVAL',\n" +
                '                    REAP_REVOKED_SCRIPT,\n' +
                "                    '2',\n" +
                '                    this.revocationIndexKey,\n' +
                '                    this.revocationFloorKey,\n' +
                '                    String(this.revocationTtlSeconds),\n' +
                '                    String(\n' +
                '                        this.revocationTtlSeconds +\n' +
                '                            INDEX_TTL_SLACK_SECONDS,\n' +
                '                    ),\n' +
                '                ),\n' +
                '            ))\n' +
                SKIP_SUM,
            ],
        ],
        killedBy: '#359 R3 (a, b)',
    },
    {
        label: 'M8 — liveness judged against the instance clock',
        file: REDIS,
        edits: [[
            FILTER,
            '                if (!(entry.score > Date.now() / 1000)) continue\n',
        ]],
        killedBy: '#359 R3 (c)',
    },
    {
        label: 'M9 — no closing check before a page read',
        file: REDIS,
        edits: [[
            CLOSING_BEFORE_PAGE_READ,
            '            const page = decodeRevocationPage(\n',
        ]],
        killedBy: '#359 R10 ',
    },
    {
        label: 'M10 — the matches so far returned when a page read throws',
        file: REDIS,
        edits: [
            [
                LOOP_HEAD,
                "        let cursor = '0'\n" +
                '        try {\n' +
                '        do {\n' +
                '            if (this.#closing) throw new Error(REVOCATION_PASS_CLOSING)\n',
            ],
            [
                LOOP_END,
                "        } while (cursor !== '0')\n" +
                '        } catch {\n' +
                '            return [...live.values()]\n' +
                '        }\n' +
                '        if (skipped > 0) console.warn(`${REVOCATION_PAIRS_SKIPPED} ${skipped}`)\n',
            ],
        ],
        killedBy: '#359 R11 ',
    },
    {
        label: 'M11 — an odd-length page accepted, its trailing item dropped',
        file: REDIS,
        edits: [
            [
                '    if (items.length % 2 !== 0) throw new Error(REVOCATION_PAGE_REFUSED)\n',
                '',
            ],
            [
                '    for (let i = 0; i < items.length; i += 2) {\n' +
                '        const member = asBulk(items[i])\n',
                '    for (let i = 0; i + 1 < items.length; i += 2) {\n' +
                '        const member = asBulk(items[i])\n',
            ],
        ],
        killedBy: '#359 R12 decodeRevocationPage: a bad envelope',
    },
    {
        label: 'M12 — the Redis driver ignores owns',
        file: REDIS,
        edits: [[
            '                if (owns !== undefined && !owns(revocation.target)) continue\n',
            '',
        ]],
        killedBy: '#359 R13 (a)',
    },
    {
        label: 'M13 — the manager stops handing owns to the driver',
        file: MANAGER,
        edits: [[
            '        const revocations = await this.#revocations?.listRevocations(\n' +
            '            (target) => this.connections.has(target),\n' +
            '        ) ?? []\n',
            '        const revocations = await this.#revocations?.listRevocations() ??\n' +
            '            []\n',
        ]],
        killedBy: '#359 R13 (b, c)',
    },
    {
        label: "M14 — the manager's own connections.has check deleted",
        file: MANAGER,
        edits: [[
            '            if (!this.connections.has(revocation.target)) continue\n' +
            '            if (revocation.channel === undefined) {\n',
            '            if (revocation.channel === undefined) {\n',
        ]],
        // Killed until #361 by R13 (d), which built a membership stranded by a
        // `subscribe` resolving during a `disconnect` of the same id. #361
        // narrowed that window for a transport that registers on open; #370
        // (register-only admission) enforces it at runtime, retiring R13 (d)
        // and moving this row to its recorded survival. Kept, never deleted:
        // if applying to an id absent from `connections` ever becomes
        // observable again, R13 (b, c) is the fixture that would kill it.
        killedBy: '(none — equivalent)',
        expectSurvival:
            "Unreachable for a transport that registers on open; #370 enforces it: `subscribe` now refuses admission (`ConnectionNotRegisteredError`) to any id `connections` does not already hold, before its authorizer runs and before anything is written, so no membership can name an id absent from `connections`. A retired connection is refused the same way (#361 W1, W3, W8), and `unsubscribe` forgets presence before its awaited leave (#361 W9). Against such an id, applying is a no-op. The fixture that would kill it is R13 (b, c)'s foreign id, if applying there ever became observable.",
    },
    {
        label: 'M15 — the revocation timer back on setInterval',
        file: REDIS,
        edits: [[
            '        this.revocationTimer = setTimeout(() => {\n' +
            '            this.revocationTimer = undefined\n' +
            "            this.#startRevocationPass('timer')\n" +
            '        }, this.reconcileIntervalMs)\n',
            '        this.revocationTimer = setInterval(() => {\n' +
            "            this.#startRevocationPass('timer')\n" +
            '        }, this.reconcileIntervalMs)\n',
        ]],
        killedBy: '#359 R8 ',
    },
    {
        label: 'M16 — a reconnect during a pass dropped: no rerun recorded',
        file: REDIS,
        edits: [[RERUN_RECORD, '            return\n']],
        killedBy: '#359 R9 (a)',
    },
    {
        label: 'M17 — trailing passes counted instead of coalesced',
        file: REDIS,
        edits: [
            [
                "    #revocationRerun?: 'reconnect' | 'reconnect-retry'\n",
                "    #revocationRerun?: 'reconnect' | 'reconnect-retry'\n" +
                '    #reruns = 0\n',
            ],
            [
                RERUN_RECORD,
                '            if (this.#revocationRerun !== ' +
                "'reconnect') {\n" +
                '                this.#revocationRerun = trigger\n' +
                '            }\n' +
                '            this.#reruns++\n' +
                '            return\n',
            ],
            [
                RERUN_TAKE,
                '                const rerun = this.#revocationRerun\n' +
                '                if (this.#reruns > 1) this.#reruns--\n' +
                '                else {\n' +
                '                    this.#revocationRerun = undefined\n' +
                '                    this.#reruns = 0\n' +
                '                }\n',
            ],
        ],
        killedBy: '#359 R9 (b)',
    },
    {
        label: "M18 — #armRevocationReconcile's closing check dropped",
        file: REDIS,
        edits: [[
            '    #armRevocationReconcile(): void {\n' +
            '        if (this.#closing) return\n',
            '    #armRevocationReconcile(): void {\n',
        ]],
        killedBy: '#359 R10 ',
    },
    {
        label: "M19 — the manager's re-check tail removed",
        file: MANAGER,
        edits: [[
            '        const run = this.#revocationTail.then(() => this.#recheckRevocations())\n',
            '        const run = this.#recheckRevocations()\n',
        ]],
        killedBy: '#359 R14 ',
    },
    {
        label: 'M20 — the re-check tail continued only on success',
        file: MANAGER,
        // `.then(() => {})` alone is the mutant; the `.catch` only marks the
        // stuck tail handled. Without it the rejected tail is also an
        // unhandled rejection, which fails the test file as an "(uncaught
        // error)" — a kill, but attributed to no witness. With it, the next
        // run inherits the stale rejection and issues no reap, which is the
        // claim R15 makes.
        edits: [[
            '        this.#revocationTail = run.then(() => {}, () => {})\n',
            '        this.#revocationTail = run.then(() => {})\n' +
            '        this.#revocationTail.catch(() => {})\n',
        ]],
        killedBy: '#359 R15 ',
    },
    {
        label: 'M21 — a malformed pair skipped without being counted',
        file: REDIS,
        // Anchored on the page decoder's own score test: since #380 the floor
        // decoder skips and counts with the same two lines. Re-anchored, never
        // deleted — the source moved, the guard remains.
        edits: [[
            '            !EPOCH_SECONDS.test(score)\n' +
            '        ) {\n' +
            '            skipped++\n            continue\n',
            '            !EPOCH_SECONDS.test(score)\n' +
            '        ) {\n' +
            '            continue\n',
        ]],
        killedBy: '#359 R12 through listRevocations',
    },
    {
        // The retry-then-reconnect order cannot see this: there the last
        // writer IS the reconnect. Only a retry arriving after the reconnect
        // tells "reconnect wins" from "the last one wins".
        label: "M22 — the rerun slot's last writer wins, not 'reconnect'",
        file: REDIS,
        edits: [[
            RERUN_RECORD,
            '            this.#revocationRerun = trigger\n' +
            '            return\n',
        ]],
        killedBy: '#359 R9 (c) a reconnect, then a retry',
    },
    {
        // The port contract says a throw from `owns` fails the call; a
        // defensive `try` that swallowed it would hand back the records asked
        // about before the throw as if they were the whole answer.
        label: 'M23 — a throw from owns swallowed: the record read as not mine',
        file: REDIS,
        edits: [[
            '                if (owns !== undefined && !owns(revocation.target)) continue\n',
            '                if (owns !== undefined) {\n' +
            '                    let mine = false\n' +
            '                    try {\n' +
            '                        mine = owns(revocation.target)\n' +
            '                    } catch {\n' +
            '                        mine = false\n' +
            '                    }\n' +
            '                    if (!mine) continue\n' +
            '                }\n',
        ]],
        killedBy: '#359 R13 (e)',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#359 — the revocation re-check reads the index in pages, one pass at a time',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
