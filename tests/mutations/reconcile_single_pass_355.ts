/**
 * @fileoverview #355's mutation battery — one Redis reconcile pass at a time,
 * a sweep that writes only while its target is dead, and a count of what it
 * actually removed.
 *
 * The remedy spreads its decisions over a handful of homes in
 * `drivers/redis.ts`: `#armReconcile` (the one arming site), `close()` (the
 * stop / drop / await order), the `#closing` checks (three at #355; #358
 * added a fourth, before each page read, which its own battery covers), the
 * two scripts (the in-write liveness check, deregistration only while dead
 * AND owning nothing), the decoders, and `#sweepInstance`'s count and
 * per-instance catch. Each row below drops one clause a refactor could drop while the rest
 * of the suite stays green.
 *
 * - M1 the sweep back on a `setInterval`: passes pile up on a slow broker.
 * - M2 the re-arm moved into `#reconcile`'s `try`, after the loop (A5): a
 *   failed pass arms no next one.
 * - M3 an *absent* release counted. M4 a *kept* release not counted.
 * - M5 the liveness check dropped from the release script.
 * - M6 a leave asks for the liveness check: its own live key refuses it.
 * - M7 the sweep asks with `false` (S5). M8 `KEYS[4]` is the sweeper's own
 *   liveness key (S5): every sweep release refused, the sweep jammed.
 * - M9 deregistration without the liveness condition. M10 without the
 *   owned-set condition (A4): a late hold orphaned.
 * - M11 `close()` does not await the pass.
 * - M12a / b / c the `#closing` check dropped at the top of the loop body /
 *   before each release / before the deregistration. (The fourth, before each
 *   page read, is `sweep_paging_358` M6; M12b anchors on the check plus the
 *   release, since the check line alone now appears twice at 12 spaces.)
 * - M13 `#armReconcile`'s closing check dropped (A2, S2).
 * - M14 `#ensureSweepStarted` arms the heartbeat while closing.
 * - M15 `revocationHandler` dropped after the await instead of before (A1).
 * - M16 the decoder maps *refused* to *absent*.
 * - M17 the per-instance catch rethrows (A3): one bad instance ends the pass.
 * - M18 (plan amendment, 2026-09-23) the "released" line dropped on the
 *   `close()` path: a sweep cut short after removing holds logs nothing.
 *
 * The review added seven, and re-anchored eight rows (M3, M4, M7, M12b,
 * M12c, M14, M17, M18) onto `#sweepOwned` and the one log site:
 *
 * - M19 the heartbeat registers first (A4). M20 it skips the registration
 *   after a failed `SET` (#310).
 * - M21 a per-release catch that goes on. M22 the "failed" line loses N.
 * - M23 the heartbeat a one-shot timeout. M24 an in-flight guard on it.
 * - M25 a revocation run failing during `close()` arms its retry.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate reconcile_single_pass_355
 * ```
 *
 * @module @lockness/realtime/tests/mutations/reconcile_single_pass_355
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../reconcile_single_pass_355.test.ts', import.meta.url).pathname,
    // WD, and M6's #345 W1: a leave of a held slot.
    new URL('../roster_holders_345.test.ts', import.meta.url).pathname,
    // M6's #344 leave witnesses.
    new URL('../presence_member_transitions_344.test.ts', import.meta.url)
        .pathname,
]

// Re-anchored for #360: the callback became the sweep's start and end site
// for its pass sample — the pass record at the start; the pass clock read
// first, the re-arm, and the sample last in the `finally`; and a final
// `.catch` for a log sink that threw (A1). M1 and M2 keep their mutants.
// Re-anchored for #384: the record and the sample carry the sweep's counts.
const ARM_TIMER_HEAD = '        this.reconcileTimer = setTimeout(() => {\n' +
    '            this.reconcileTimer = undefined\n' +
    '            const pass = {\n' +
    '                startedAt: this.#passClock(),\n' +
    '                pages: 0,\n' +
    '                attempts: 0,\n' +
    '                failures: 0,\n' +
    '            }\n' +
    '            this.#sweepPass = pass\n' +
    "            let outcome: PassOutcome = 'failed'\n" +
    '            this.#reconcilePass = this.#reconcile()\n' +
    '                .then((ended) => void (outcome = ended))\n' +
    '                .finally(() => {\n' +
    '                    const endedAt = this.#passClock()\n' +
    '                    this.#reconcilePass = undefined\n'
const ARM_TIMER_REARM = '                    this.#armReconcile()\n'
const ARM_TIMER_TAIL = '                    this.#sweepPass = undefined\n' +
    '                    this.#emitPassSample(\n' +
    "                        'sweep',\n" +
    "                        'timer',\n" +
    '                        outcome,\n' +
    '                        pass.startedAt,\n' +
    '                        endedAt,\n' +
    '                        pass.pages,\n' +
    '                        pass.attempts,\n' +
    '                        pass.failures,\n' +
    '                    )\n' +
    '                })\n' +
    '                .catch((error: unknown) => {\n' +
    '                    // #360 A1, the #369 rule: nothing escapes the sweep chain.\n' +
    '                    // A rejection reaches here only when a log sink threw\n' +
    '                    // inside the pass; the marker is the fixed prefix, and\n' +
    '                    // the line never throws past itself either (#391).\n' +
    '                    writeMarkedFallback(SWEEP_LOG_FAILED, error)\n' +
    '                })\n' +
    '        }, this.reconcileIntervalMs)\n'
const ARM_TIMER = ARM_TIMER_HEAD + ARM_TIMER_REARM + ARM_TIMER_TAIL

// Re-anchored by the #355 review: the sweep's writes moved from
// `#sweepInstance`'s try into `#sweepOwned`, one indent shallower, and every
// exit returns how the sweep ended instead of logging on its own.
const COUNT = "            if (outcome.kind === 'absent') continue\n" +
    '            count.released++\n' +
    "            if (outcome.kind === 'kept') continue\n"

const SWEEP_RELEASE = '            const outcome = await this.#release(\n' +
    '                channel,\n' +
    '                field,\n' +
    '                deadId,\n' +
    '                true,\n' +
    '            )\n'

const CLOSING_BEFORE_RELEASE =
    "            if (this.#closing) return 'closed'\n"

const CLOSING_BEFORE_DEREGISTRATION =
    "        if (this.#closing) return 'closed'\n" +
    '        const deregistration = decodeDeregisterReply(\n'

// `#heartbeat`'s two writes, in their decided order: liveness key, then
// registration. Re-anchored for #349: the liveness write became
// `SET … EX … GET`, its reply is kept, and it is decoded inside the same
// `try` — so the block that M19 moves is the call AND its decode.
const BEAT_SET = '            const reply = await this.command.command(\n' +
    "                'SET',\n" +
    '                this.aliveKey(this.instanceId),\n' +
    "                '1',\n" +
    "                'EX',\n" +
    '                String(this.livenessTtlSeconds),\n' +
    "                'GET',\n" +
    '            )\n' +
    '            // Decoded INSIDE the try (#349 S1): a reply it refuses is one\n' +
    '            // failed beat, never a rejection escaping an interval callback.\n' +
    '            outcome = decodeBeatReply(reply)\n'
const BEAT_SADD = '            await this.command.command(\n' +
    "                'SADD',\n" +
    '                this.instancesKey,\n' +
    '                this.instanceId,\n' +
    '            )\n'
const BEAT_BETWEEN = '        } catch (error) {\n' +
    '            failure = { error }\n' +
    '        }\n' +
    '        try {\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the sweep re-armed with setInterval again',
        file: REDIS,
        edits: [[
            ARM_TIMER,
            // `.then` keeps `#reconcilePass: Promise<void>` type-checking now
            // that `#reconcile` returns its outcome (#360).
            '        this.reconcileTimer = setInterval(() => {\n' +
            '            this.#reconcilePass = this.#reconcile().then(() => {})\n' +
            '        }, this.reconcileIntervalMs)\n',
        ]],
        killedBy: '#355 W1',
    },
    {
        label: "M2 — the re-arm moved into #reconcile's try, after the loop",
        file: REDIS,
        edits: [
            // Only the re-arm goes; the sample and the `.catch` stay (#360).
            [ARM_TIMER, ARM_TIMER_HEAD + ARM_TIMER_TAIL],
            [
                '                if (alive === 0) await this.#sweepInstance(id)\n' +
                '            }\n',
                '                if (alive === 0) await this.#sweepInstance(id)\n' +
                '            }\n' +
                '            this.#armReconcile()\n',
            ],
        ],
        // A rejected instance-set read skips the re-arm: no next pass.
        killedBy: '#355 W3',
    },
    {
        label: 'M3 — an absent release is counted',
        file: REDIS,
        edits: [[
            COUNT,
            '            count.released++\n' +
            "            if (outcome.kind === 'absent') continue\n" +
            "            if (outcome.kind === 'kept') continue\n",
        ]],
        killedBy: '#355 W2',
    },
    {
        label: 'M4 — a kept release is not counted',
        file: REDIS,
        edits: [[
            COUNT,
            "            if (outcome.kind === 'absent') continue\n" +
            "            if (outcome.kind === 'kept') continue\n" +
            '            count.released++\n',
        ]],
        killedBy: '#355 W2',
    },
    {
        label: 'M5 — the liveness check dropped from the release script',
        file: REDIS,
        edits: [[
            `    "if ARGV[4] == '1' then",\n`,
            `    "if ARGV[4] == 'never' then",\n`,
        ]],
        killedBy: '#355 W5',
    },
    {
        label: 'M6 — a leave asks for the liveness check too',
        file: REDIS,
        edits: [[
            '            String(memberId),\n' +
            '            this.instanceId,\n' +
            '            false,\n',
            '            String(memberId),\n' +
            '            this.instanceId,\n' +
            '            true,\n',
        ]],
        // A holds 7 and is alive: its own leave is refused, and throws.
        killedBy: '#345 W1',
    },
    {
        label: 'M7 — the sweep asks with false',
        file: REDIS,
        edits: [[
            '                deadId,\n' +
            '                true,\n',
            '                deadId,\n' +
            '                false,\n',
        ]],
        killedBy: '#355 W5',
    },
    {
        label: "M8 — KEYS[4] is the sweeper's own liveness key",
        file: REDIS,
        edits: [[
            '            this.aliveKey(releaserId),\n',
            '            this.aliveKey(this.instanceId),\n',
        ]],
        // The sweeper is alive, so every sweep release is refused.
        killedBy: '#355 W2',
    },
    {
        label: 'M9 — deregistration without the liveness condition',
        file: REDIS,
        edits: [[
            `    "local alive = redis.call('EXISTS', KEYS[2])",\n` +
            "    'if alive == 1 then',\n",
            `    "local alive = redis.call('EXISTS', KEYS[2])",\n` +
            "    'if alive == 99 then',\n",
        ]],
        // The trailing space is load-bearing: the harness attributes a kill
        // by SUBSTRING, and '#355 W6' alone would also match a failing
        // '#355 W6b' — a kill by the wrong witness read as this row's.
        killedBy: '#355 W6 ',
    },
    {
        label: 'M10 — deregistration without the owned-set condition',
        file: REDIS,
        edits: [[
            "    'if owns == 0 then',\n",
            "    'if 0 == 0 then',\n",
        ]],
        killedBy: '#355 W6b',
    },
    {
        // Re-anchored for #368: `await this.#reconcilePass` is now one
        // argument to `awaitCloseDrain`, not its own line. "Does not await
        // the pass" is the same mutant on the new shape: the pass argument
        // reads as already-settled.
        label: 'M11 — close() does not await the pass',
        file: REDIS,
        edits: [[
            '        const pending = await awaitCloseDrain(\n' +
            '            budgetMs,\n' +
            '            this.#reconcilePass,\n' +
            '            stopped,\n' +
            '            maintenanceStopped,\n' +
            '        )\n',
            '        const pending = await awaitCloseDrain(\n' +
            '            budgetMs,\n' +
            '            undefined,\n' +
            '            stopped,\n' +
            '            maintenanceStopped,\n' +
            '        )\n',
        ]],
        killedBy: '#355 W4 (i)',
    },
    {
        label: 'M12a — no closing check at the top of the loop body',
        file: REDIS,
        edits: [[
            // Re-anchored for #360: the check now returns the pass outcome.
            '            for (const raw of ids) {\n' +
            "                if (this.#closing) return 'closed'\n",
            '            for (const raw of ids) {\n',
        ]],
        killedBy: '#355 W4 (iii)',
    },
    {
        label: 'M12b — no closing check before each release',
        file: REDIS,
        edits: [[
            CLOSING_BEFORE_RELEASE + SWEEP_RELEASE,
            SWEEP_RELEASE,
        ]],
        killedBy: '#355 W4 (i)',
    },
    {
        label: 'M12c — no closing check before the deregistration',
        file: REDIS,
        edits: [[
            CLOSING_BEFORE_DEREGISTRATION,
            '        const deregistration = decodeDeregisterReply(\n',
        ]],
        killedBy: '#355 W4 (ii)',
    },
    {
        label: "M13 — #armReconcile's closing check dropped",
        file: REDIS,
        edits: [[
            '    #armReconcile(): void {\n' +
            '        if (this.#closing) return\n',
            '    #armReconcile(): void {\n',
        ]],
        // A pass finishing during close() arms the next one.
        killedBy: '#355 W4 (iv)',
    },
    {
        label: 'M14 — the heartbeat is armed while closing',
        file: REDIS,
        edits: [[
            '        if (!this.#closing) {\n' +
            '            // Refresh our own liveness key',
            '        if (!this.#closing || true) {\n' +
            '            // Refresh our own liveness key',
        ]],
        killedBy: '#355 W4 (v)',
    },
    {
        // Re-anchored for #368: the drop now sits before the single
        // `awaitCloseDrain` call, not before a standalone
        // `await this.#reconcilePass`. Moved to AFTER that call instead of
        // before it — same defect, new shape.
        label: 'M15 — revocationHandler dropped after the await',
        file: REDIS,
        edits: [
            [
                '        this.revocationHandler = undefined\n' +
                '        // The pass stops at its next write',
                '        // The pass stops at its next write',
            ],
            [
                '        const pending = await awaitCloseDrain(\n' +
                '            budgetMs,\n' +
                '            this.#reconcilePass,\n' +
                '            stopped,\n' +
                '            maintenanceStopped,\n' +
                '        )\n',
                '        const pending = await awaitCloseDrain(\n' +
                '            budgetMs,\n' +
                '            this.#reconcilePass,\n' +
                '            stopped,\n' +
                '            maintenanceStopped,\n' +
                '        )\n' +
                '        this.revocationHandler = undefined\n',
            ],
        ],
        killedBy: '#355 W4 (vi)',
    },
    {
        label: 'M16 — the decoder maps refused to absent',
        file: REDIS,
        edits: [[
            "    if (code === REFUSED) return { kind: 'refused' }\n",
            "    if (code === REFUSED) return { kind: 'absent' }\n",
        ]],
        // WD also dies; W5 is the behaviour: B sweeps a renewed instance on,
        // to a deregistration it must never ask for.
        killedBy: '#355 W5',
    },
    {
        label: 'M17 — the per-instance catch rethrows',
        file: REDIS,
        edits: [[
            '        } catch (error) {\n' +
            '            end = { failed: error }\n',
            '        } catch (error) {\n' +
            '            if (error !== undefined) throw error\n' +
            '            end = { failed: error }\n',
        ]],
        killedBy: '#355 W7',
    },
    {
        label: 'M18 — the "released" line dropped on the close() path',
        file: REDIS,
        // Re-anchored by the #355 review: the closing checks now return
        // 'closed', and the one log site is what would drop the line.
        // Re-written for #358: the branch now picks the *unfinished* suffix
        // with `end === 'kept' || end === 'closed'`, so a guard that NARROWS
        // `end` (the old `end === 'completed'`) no longer type-checks and the
        // row went DEAD. `String(end)` drops the line on the close() path
        // without narrowing; the anchor line is unchanged.
        edits: [[
            '        } else if (released > 0) {\n',
            "        } else if (released > 0 && String(end) !== 'closed') {\n",
        ]],
        killedBy: '#355 W4 (vii)',
    },
    {
        label:
            'M19 — the heartbeat registers before it writes the liveness key',
        file: REDIS,
        edits: [[
            BEAT_SET + BEAT_BETWEEN + BEAT_SADD,
            BEAT_SADD + BEAT_BETWEEN + BEAT_SET,
        ]],
        // Re-anchored for #349 (BEAT_SET above); the mutation and its
        // witness are unchanged.
        killedBy: '#355 W8',
    },
    {
        label: 'M20 — the heartbeat skips the registration after a failed SET',
        file: REDIS,
        edits: [[
            BEAT_BETWEEN,
            BEAT_BETWEEN.replace(
                '        try {\n',
                '        if (!failure) try {\n',
            ),
        ]],
        // Re-proven live for #349: the anchor is intact (FR-001 kept the
        // SET's catch to its two lines), but the `try` above it now also
        // decodes the reply.
        killedBy: '#355 W8',
    },
    {
        label: 'M21 — a failed release is caught and the sweep goes on',
        file: REDIS,
        edits: [[
            SWEEP_RELEASE,
            SWEEP_RELEASE.replace(
                '            )\n',
                "            ).catch((): ReleaseOutcome => ({ kind: 'absent' }))\n",
            ),
        ]],
        // A per-release catch that continues: the third hold is released
        // and the instance deregistered after a failure.
        killedBy: '#355 WF',
    },
    {
        label: 'M22 — the "failed" line forgets what was released before',
        file: REDIS,
        edits: [[
            '`${released} hold(s) released (${emptied} emptied): `',
            '`0 hold(s) released (0 emptied): `',
        ]],
        killedBy: '#355 WF',
    },
    {
        label: 'M23 — the heartbeat is a one-shot timeout',
        file: REDIS,
        edits: [[
            '            this.heartbeatTimer = setInterval(\n',
            '            this.heartbeatTimer = setTimeout(\n',
        ]],
        killedBy: '#355 W9',
    },
    {
        label: 'M24 — the heartbeat gains an in-flight guard',
        file: REDIS,
        edits: [
            [
                '    #departureHandler?: (departure: RosterDeparture) => void | Promise<void>\n',
                '    #departureHandler?: (departure: RosterDeparture) => void | Promise<void>\n' +
                '    #beat?: Promise<void>\n',
            ],
            [
                '                () => this.#heartbeat(),\n',
                '                () => {\n' +
                '                    if (this.#beat) return\n' +
                '                    this.#beat = this.#heartbeat().finally(() => {\n' +
                '                        this.#beat = undefined\n' +
                '                    })\n' +
                '                },\n',
            ],
        ],
        killedBy: '#355 W9',
    },
    {
        label: 'M25 — a revocation run failing during close() arms its retry',
        file: REDIS,
        // Re-anchored for #362: both guards now return the pass outcome.
        edits: [[
            "            if (trigger !== 'reconnect') return 'failed'\n" +
            '            // Nor once close() has begun (#355): a run already in flight when\n' +
            '            // close() started would arm a timer that outlives the driver.\n' +
            "            if (this.#closing) return 'failed'\n",
            "            if (trigger !== 'reconnect') return 'failed'\n",
        ]],
        killedBy: '#355 WR',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#355 — one reconcile pass at a time, a sweep that writes only ' +
                    'while its target is dead',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
