/**
 * @fileoverview #349's mutation battery — a lapsed-but-alive instance re-holds
 * its presence slots, and no connection hears presence about itself.
 *
 * The remedy spreads its decisions over a handful of homes (plan §5):
 * `#heartbeat` and `decodeBeatReply` detect the lapse, `#holdIssued` gates it,
 * `#lapseSuspected` carries a failed beat to the next one, `LapseRun` decides
 * when the handler runs, `close()` stops it, `#reassertRoster` re-checks
 * revocations and then writes each slot through `#syncRosterMember`, and
 * `emitPresence` keeps a member's own tabs out of every frame about it. Each
 * row below drops one clause a refactor could drop while the rest of the suite
 * stays green.
 *
 * **Driver and manager rows** (`lapse_rehold_349.test.ts`):
 *
 * - M1 the lapse bit ignored. M6 a failed beat not suspected. M16 the decoder
 *   reads a non-bulk as lapsed. M22 the decode outside the SET's `try`.
 * - M5a / b / c the hold gate: set at `holdMember`'s entry, "skip the first
 *   beat", read when the beat is issued.
 * - M2 the re-assert bypasses the slot tail. M3 it announces itself. M4 it
 *   drops `arrived`. M10 `Promise.all` over the slots. M18 it stops at the
 *   first failure. M15 the beat awaits the run.
 * - M19 / M20 / M21 the revocation re-check: removed, moved after the slots,
 *   rethrown into the aggregate.
 * - M8 the self-exclusion for `joined` only. M9 the registration without `?.`.
 *   M17 `close()` keeps the refusal handler.
 * - Review cycle (#349): M24 / M25 / M26 the shutdown rules — the per-slot
 *   abort check, the abort before the sweep pass's await, and the wait for
 *   the run; M27 a revocation that throws stops the reconcile; M28 the
 *   exclusion keyed on the entry, not the member id; M29 the re-assert walks
 *   the live presence map; M30 a failed SADD makes the lapse suspected; M31
 *   the suspicion never cleared; M32 / M33 the two WARNs name members.
 *
 * **The merged self-exclusion row** (M12) runs the #344 transitions suite too:
 * it subsumes `presence_member_transitions_344.ts` M7 and its `handleControl`
 * row, whose anchor (`exceptMemberId`) #349 removed.
 *
 * **`LapseRun` rows** (`lapse_run_349.test.ts`, no broker double, A6): M7 no
 * trailing run, M11 `close()` does not abort, M13 `trigger()` ignores the
 * closed state, M14 no `onFailure`, M23 the handler outside the `try`.
 *
 * **Where the plan's attribution was wrong, the row says so** and names the
 * witness that actually dies — `docs/testing.md`: `killedBy` is the field most
 * worth getting right. M1 and M15 are the two.
 *
 * `killedBy` is a SUBSTRING: a trailing space keeps `#349 W1 ` from matching a
 * failing W11–W15, `#349 W3 ` from W3b, `#349 W9 ` from #344 W9, and
 * `#349 W11b close()` from W11b (ii).
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate lapse_rehold_349
 * ```
 *
 * @module @lockness/realtime/tests/mutations/lapse_rehold_349
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const LAPSE_RUN = new URL('../../drivers/lapse_run.ts', import.meta.url)
const REHOLD = new URL('../lapse_rehold_349.test.ts', import.meta.url).pathname
const UNIT = new URL('../lapse_run_349.test.ts', import.meta.url).pathname
const TRANSITIONS = new URL(
    '../presence_member_transitions_344.test.ts',
    import.meta.url,
).pathname

// `#heartbeat`'s tail: the one lapse decision.
const TAIL_TRIGGER =
    "        } else if (outcome === 'lapsed' || this.#lapseSuspected) {\n"
const TAIL_GATE = '        if (!this.#holdIssued) return\n'
const TAIL_SUSPECT = '        if (outcome === undefined) {\n' +
    '            this.#lapseSuspected = true\n'

// `holdMember`: the gate is set just before the EVAL, after the boot beat.
const HOLD_ENTRY = '        await this.#ensureSweepStarted()\n' +
    '        const entry: RosterEntry = { member, owner: this.instanceId }\n'
const HOLD_GATE = '        this.#holdIssued = true\n' +
    '        // ONE operation (#323, #345).'

// `#reassertRoster`: the re-check, then the slot loop.
const RECHECK = '        try {\n' +
    '            await this.reconcileRevocations()\n' +
    '        } catch (error) {\n'
const SLOT_WRITE =
    '                await this.#syncRosterMember(channel, origin)\n' +
    '            } catch (error) {\n' +
    '                failures.push(error)\n'
const SLOT_LOOP = '        for (const { channel, origin } of slots) {\n' +
    '            if (signal.aborted) return\n' +
    '            try {\n' +
    '                await this.#syncRosterMember(channel, origin)\n' +
    '            } catch (error) {\n' +
    '                failures.push(error)\n' +
    '            }\n' +
    '        }\n'
const AGGREGATE = '        if (failures.length > 0) {\n'

// `emitPresence`: the self-exclusion.
const SELF_SKIP =
    '            if (entry && sameMemberId(entry.id, self)) continue\n'

/** Rows killed by `lapse_rehold_349.test.ts`. */
const REHOLD_ROWS: Mutation[] = [
    {
        label: 'M1 — the lapse bit ignored (no trigger on lapsed)',
        file: REDIS,
        edits: [[TAIL_TRIGGER, '        } else if (this.#lapseSuspected) {\n']],
        // NOT W1, which the plan named: W1 injects the lapse by refusing the
        // liveness SET, so every beat during it FAILS and marks the lapse
        // suspected — the healed beat triggers on the suspicion alone, and
        // W1 stays green under this mutant. W1b is US1's other path, added
        // by the review for exactly this row: a stalled loop, where no beat
        // fails and the next SET … GET's nil alone carries the lapse. W7
        // (ii) dies too (its boot beat re-creates a key that never existed).
        killedBy: '#349 W1b',
    },
    {
        label: 'M2 — the re-assert calls roster.holdMember over the snapshot',
        file: MANAGER,
        edits: [[
            SLOT_WRITE,
            '                const hold = await this.roster?.holdMember(channel, origin.member)\n' +
            '                if (hold?.arrived) await this.#announcePresence(\n' +
            "                    'joined', channel, origin.member, origin.clientId,\n" +
            '                )\n' +
            '            } catch (error) {\n' +
            '                failures.push(error)\n',
        ]],
        // It still announces on `arrived`, so only the ordering is lost: a
        // leave queued on 8's slot during the re-assert is overtaken, and 8
        // is re-held after it left.
        killedBy: '#349 W4 ',
    },
    {
        label: 'M3 — the re-assert announces joined itself',
        file: MANAGER,
        edits: [[
            SLOT_WRITE,
            '                await this.#syncRosterMember(channel, origin)\n' +
            "                await this.#announcePresence('joined', channel, origin.member, origin.clientId)\n" +
            '            } catch (error) {\n' +
            '                failures.push(error)\n',
        ]],
        // A slot the sweep kept (D still holds 7) is announced again. W6 (a
        // lapse nobody swept) dies to it too.
        killedBy: '#349 W5 ',
    },
    {
        label: 'M4 — the re-assert ignores arrived',
        file: MANAGER,
        edits: [[
            SLOT_WRITE,
            '                await this.roster?.holdMember(channel, origin.member)\n' +
            '            } catch (error) {\n' +
            '                failures.push(error)\n',
        ]],
        // The slot is written again and nobody is told: observers keep the
        // sweep's `left` and never hear 7 come back.
        killedBy: '#349 W2 ',
    },
    {
        label: "M5a — #holdIssued set at holdMember's entry",
        file: REDIS,
        edits: [
            [HOLD_GATE, '        // ONE operation (#323, #345).'],
            [HOLD_ENTRY, '        this.#holdIssued = true\n' + HOLD_ENTRY],
        ],
        // The boot beat's nil — a key that never existed — counts as a lapse.
        killedBy: '#349 W7 (i)',
    },
    {
        label: 'M5b — the gate is "skip the first beat"',
        file: REDIS,
        edits: [
            [
                '    #holdIssued = false\n',
                '    #holdIssued = false\n    #beats = 0\n',
            ],
            [TAIL_GATE, '        if (this.#beats++ === 0) return\n'],
        ],
        // A hold that overtook the boot beat and was swept is never
        // re-asserted: the beat that saw its lapse was skipped.
        killedBy: '#349 W7 (ii)',
    },
    {
        label:
            'M5c — #holdIssued read when the beat is issued, not at the tail',
        file: REDIS,
        edits: [
            [
                '        let outcome: BeatOutcome | undefined\n',
                '        let outcome: BeatOutcome | undefined\n' +
                '        const holdIssued = this.#holdIssued\n',
            ],
            [TAIL_GATE, '        if (!holdIssued) return\n'],
        ],
        killedBy: '#349 W7 (ii)',
    },
    {
        label: 'M6 — a failed beat does not set #lapseSuspected',
        file: REDIS,
        edits: [[TAIL_SUSPECT, '        if (outcome === undefined) {\n']],
        // The committed beat whose reply was lost left the key continuous:
        // the next beat's bulk reply carries no lapse, and only the
        // suspicion would have re-asserted.
        killedBy: '#349 W8 ',
    },
    {
        label: 'M8 — the self-exclusion applies to joined only',
        file: MANAGER,
        edits: [[
            SELF_SKIP,
            "            if (frame.action === 'joined' && entry && sameMemberId(entry.id, self)) continue\n",
        ]],
        // 7's own tab on A hears the sweeper's `left` for 7. W3b (7's new
        // tab on the sweeper) dies to it too.
        killedBy: '#349 W3 ',
    },
    {
        label: "M9 — the manager's registration drops ?.",
        file: MANAGER,
        edits: [[
            '            this.driver.onRosterLapse?.((signal) =>\n',
            '            this.driver.onRosterLapse!((signal) =>\n',
        ]],
        // A roster driver without the hook throws at construction.
        killedBy: '#349 W10 a hand-rolled roster driver',
    },
    {
        label: 'M10 — Promise.all over the slots',
        file: MANAGER,
        edits: [[
            SLOT_LOOP,
            '        await Promise.all(slots.map(async ({ channel, origin }) => {\n' +
            '            if (signal.aborted) return\n' +
            '            try {\n' +
            '                await this.#syncRosterMember(channel, origin)\n' +
            '            } catch (error) {\n' +
            '                failures.push(error)\n' +
            '            }\n' +
            '        }))\n',
        ]],
        // Five holds queued at once sit in front of the next beat's SET.
        killedBy: '#349 W13',
    },
    {
        label: 'M15 — the beat awaits the run',
        file: REDIS,
        edits: [
            [
                '    #holdIssued = false\n',
                '    #holdIssued = false\n    #running?: Promise<void>\n',
            ],
            [
                '        this.#lapse.register(handler)\n',
                '        this.#lapse.register((signal) =>\n' +
                '            (this.#running = Promise.resolve(handler(signal)))\n' +
                '        )\n',
            ],
            [
                '            this.#lapse.trigger()\n',
                '            this.#lapse.trigger()\n' +
                '            await this.#running?.catch(() => {})\n',
            ],
        ],
        // NOT W13, which the plan named: the heartbeat is an unguarded
        // `setInterval`, so a beat awaiting its run delays no LATER beat — the
        // one W13 watches still fires on time. The one beat something awaits
        // is the boot beat, inside `holdMember`: a lapse it reports re-writes
        // slots whose tails hold the very joins waiting on that boot beat, and
        // the joins never settle. W7 (iii) is that case, and it now fails
        // on its own assertion ("both joins settled"), raced against a
        // FakeTime-bounded wait — no longer by Deno's deadlock detection,
        // whose collateral used to fail every test after it. W7 (ii) dies
        // the same way, on "7's join settled".
        killedBy: '#349 W7 (iii)',
    },
    {
        label: 'M16 — decodeBeatReply reads any non-bulk as lapsed',
        file: REDIS,
        edits: [[
            "        (reply as { type?: unknown }).type === 'nil'\n",
            "        (reply as { type?: unknown }).type !== 'bulk'\n",
        ]],
        killedBy: '#349 WD',
    },
    {
        label: 'M17 — close() keeps controlRefusedHandler',
        file: REDIS,
        edits: [['        this.controlRefusedHandler = undefined\n', '']],
        killedBy: '#349 WL',
    },
    {
        label: 'M18 — the re-assert stops at the first failed slot',
        file: MANAGER,
        edits: [[
            SLOT_WRITE,
            '                await this.#syncRosterMember(channel, origin)\n' +
            '            } catch (error) {\n' +
            '                failures.push(error)\n' +
            '                break\n',
        ]],
        // 9's slot, after the failed 8, is never written on that run.
        killedBy: '#349 W9 ',
    },
    {
        label: 'M19 — the revocation re-check removed from #reassertRoster',
        file: MANAGER,
        edits: [[
            RECHECK,
            '        try {\n' +
            '        } catch (error) {\n',
        ]],
        killedBy: '#349 W15 ',
    },
    {
        label: 'M20 — the revocation re-check moved after the slot loop',
        file: MANAGER,
        edits: [
            [
                RECHECK,
                '        try {\n' +
                '        } catch (error) {\n',
            ],
            [
                AGGREGATE,
                '        await this.reconcileRevocations().catch(() => {})\n' +
                AGGREGATE,
            ],
        ],
        // The revoked member is re-held and announced `joined`, THEN revoked:
        // `left`, `joined`, `left`.
        killedBy: '#349 W15 ',
    },
    {
        label: 'M21 — a failed re-check rethrown into the aggregate rejection',
        file: MANAGER,
        edits: [
            [
                RECHECK,
                '        let recheck: unknown\n' +
                '        try {\n' +
                '            await this.reconcileRevocations()\n' +
                '        } catch (error) {\n' +
                '            recheck = error\n',
            ],
            [
                AGGREGATE,
                '        if (recheck !== undefined) failures.push(recheck)\n' +
                AGGREGATE,
            ],
        ],
        // The run fails, logs the run WARN and marks the lapse suspected — a
        // broken revocation store would re-assert every slot on every beat.
        killedBy: '#349 W15b',
    },
    {
        label: "M22 — decodeBeatReply moved outside the SET's try",
        file: REDIS,
        edits: [
            [
                '        let outcome: BeatOutcome | undefined\n',
                '        let outcome: BeatOutcome | undefined\n' +
                '        let reply: unknown\n',
            ],
            [
                '            const reply = await this.command.command(\n' +
                "                'SET',\n",
                '            reply = await this.command.command(\n' +
                "                'SET',\n",
            ],
            [
                '            outcome = decodeBeatReply(reply)\n' +
                '        } catch (error) {\n' +
                '            failure = { error }\n' +
                '        }\n',
                '        } catch (error) {\n' +
                '            failure = { error }\n' +
                '        }\n' +
                '        if (!failure) outcome = decodeBeatReply(reply)\n',
            ],
        ],
        // A refused reply escapes the beat: out of `holdMember` on the boot
        // beat, as an unhandled rejection from every interval beat.
        killedBy: '#349 WS1 a SET answered OK',
    },
    {
        label: 'M24 — the re-assert stops checking the signal before each slot',
        file: MANAGER,
        edits: [[
            '            if (signal.aborted) return\n            try {\n',
            '            try {\n',
        ]],
        // close() aborts, waits for slot 7, and the re-assert writes slot 8
        // anyway — while close() is still waiting for it.
        killedBy: '#349 W11b close()',
    },
    {
        // Re-anchored for #368: the two awaits `await this.#reconcilePass`
        // and `await stopped` are now one `awaitCloseDrain(budgetMs,
        // this.#reconcilePass, stopped)` call, so "the abort only after the
        // sweep pass's await" is now a mutation on WHEN `this.#lapse.close()`
        // itself runs — deferred to chain off `this.#reconcilePass`, instead
        // of the eager call `stopped` still names.
        label:
            "M25 — close() aborts the lapse run only after the sweep pass's await",
        file: REDIS,
        edits: [
            ['        const stopped = this.#lapse.close()\n', ''],
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
                '            (this.#reconcilePass ?? Promise.resolve()).then(\n' +
                '                () => this.#lapse.close(),\n' +
                '            ),\n' +
                '            maintenanceStopped,\n' +
                '        )\n',
            ],
        ],
        // The pass is held on its SMEMBERS: during that wait the re-assert,
        // not yet aborted, reaches its next slot and writes it.
        killedBy: '#349 W11b (ii)',
    },
    {
        // Re-anchored for #368: see M25's note. "Does not wait for the run
        // in flight" is now the third argument to `awaitCloseDrain` reading
        // as already-settled, never the real `stopped`.
        label: 'M26 — close() does not wait for the run in flight',
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
            '            this.#reconcilePass,\n' +
            '            Promise.resolve(),\n' +
            '            maintenanceStopped,\n' +
            '        )\n',
        ]],
        // close() resolves while slot 7's write is still in flight.
        killedBy: '#349 W11b close()',
    },
    {
        label: 'M27 — a revocation that throws stops the reconcile again',
        file: MANAGER,
        // Re-anchored by #395 (security review HIGH): the WARN gained its
        // own try/catch guard. The injected re-throw goes AFTER that guard's
        // closing brace now, still inside the outer `catch (error)` — inside
        // the guard itself, the fix's own `catch (sink)` would absorb it and
        // the mutant would prove nothing about the reconcile-pass defect it
        // exists to reinject.
        edits: [[
            '                    writeMarkedFallback(RECHECK_REVOCATION_LOG_FAILED, error, {\n' +
            "                        label: 'sink failure',\n" +
            '                        error: sink,\n' +
            '                    })\n' +
            '                }\n' +
            '            }\n',
            '                    writeMarkedFallback(RECHECK_REVOCATION_LOG_FAILED, error, {\n' +
            "                        label: 'sink failure',\n" +
            '                        error: sink,\n' +
            '                    })\n' +
            '                }\n' +
            '                throw error\n' +
            '            }\n',
        ]],
        killedBy: '#349 W15c',
    },
    {
        label:
            'M28 — the self-exclusion keyed on the entry (the connection), not the member id',
        file: MANAGER,
        edits: [[
            SELF_SKIP,
            '            if (entry && entry === frame.member) continue\n',
        ]],
        // The finer half of #344 M7's old guarantee, which M12's "dropped
        // outright" row is too coarse to pin: 7's SECOND tab, whose entry is
        // not the one announced, still hears 7 — and a remote frame, whose
        // member is no local entry, excludes nobody.
        killedBy: '#349 W3c',
    },
    {
        label:
            'M29 — the re-assert walks the live presence map, not a snapshot',
        file: MANAGER,
        edits: [[
            '        const failures: unknown[] = []\n' +
            '        for (const { channel, origin } of slots) {\n',
            '        const failures: unknown[] = []\n' +
            '        const live = function* (\n' +
            '            presence: Map<string, Map<string, PresenceMember>>,\n' +
            '        ): Generator<{ channel: string; origin: PresenceOrigin }> {\n' +
            '            for (const [channel, members] of presence) {\n' +
            '                for (const [clientId, member] of members) {\n' +
            '                    yield { channel, origin: { clientId, member } }\n' +
            '                }\n' +
            '            }\n' +
            '        }\n' +
            '        for (const { channel, origin } of live(this.presence)) {\n',
        ]],
        // 9, who joined after the run began, is written by the re-assert too:
        // under steady joins such a run never finishes.
        killedBy: '#349 W4b',
    },
    {
        label: 'M30 — a failed SADD makes the lapse suspected',
        file: REDIS,
        edits: [[
            '            failure ??= { error }\n',
            '            failure ??= { error }\n            this.#lapseSuspected = true\n',
        ]],
        killedBy: '#349 W8b',
    },
    {
        label: 'M31 — the suspicion is not cleared when the run is triggered',
        file: REDIS,
        edits: [[
            '            this.#lapseSuspected = false\n            // Never awaited',
            '            // Never awaited',
        ]],
        // Every beat after the repair re-asserts every slot again. W6 and
        // W15b die to it too.
        killedBy: '#349 W8c',
    },
    {
        label: "M32 — the re-assert's rejection names the members it re-held",
        file: MANAGER,
        edits: [[
            '                    `re-held after a liveness lapse: ${\n',
            '                    `re-held after a liveness lapse ${\n' +
            '                        JSON.stringify(slots)\n' +
            '                    }: ${\n',
        ]],
        killedBy: '#349 W9 ',
    },
    {
        label: "M33 — the re-check WARN names this instance's connections",
        file: MANAGER,
        edits: [[
            '                    "re-asserting this instance\'s presence holds failed — " +\n',
            '                    "re-asserting this instance\'s presence holds failed — " +\n' +
            "                    [...this.connections.keys()].join(',') +\n",
        ]],
        killedBy: '#349 W15b',
    },
]

/**
 * The merged self-exclusion row: `lapse_rehold_349.test.ts` and the #344
 * transitions suite.
 */
const SELF_EXCLUSION_ROWS: Mutation[] = [
    {
        label: 'M12 — the self-exclusion dropped from emitPresence',
        file: MANAGER,
        edits: [[
            SELF_SKIP,
            '            if (entry && sameMemberId(entry.id, self) && false) continue\n',
        ]],
        // SUBSUMES `presence_member_transitions_344.ts` "#344 M7 `joined`
        // excludes only the origin connection" and "#344 handleControl
        // re-emits a remote `joined` without exceptMemberId" (#349): the
        // `exceptMemberId` option both mutated is gone, and the exclusion is
        // now `emitPresence`'s own, for both actions, with no caller able to
        // opt out. This one row drops it for the local emit AND the remote
        // re-emit, so #344 W9, #344 W9 remote and #349 W3 all die to it.
        //
        // #344 M7's reason, verbatim: "SUCCESSOR to `presence_join_323.ts`
        // "the newcomer is no longer excluded from its own join", whose reason
        // was: "The exclusion used to be a consequence of WHERE the call sat.
        // This row is why it is now an argument: with the call below
        // `#joinLocal`, dropping `except` puts the joiner in its own
        // announcement, and nothing about the statement order would tell
        // you." Since #344 the argument is the member id: the origin
        // connection alone is not enough when two tabs race."
        //
        // The `handleControl` row's reason, verbatim: "A tab of member 7 on B
        // receives B's re-emit of A's `presence-join` for itself."
        killedBy: '#344 W9 two tabs of member 7 racing its arrival',
    },
]

/** `LapseRun` rows, killed by `lapse_run_349.test.ts` alone (A6). */
const LAPSE_RUN_ROWS: Mutation[] = [
    {
        label: 'M7 — LapseRun has no trailing run',
        file: LAPSE_RUN,
        edits: [[
            '            this.#trailing = true\n' +
            '            return\n',
            '            return\n',
        ]],
        killedBy: '#349 W12',
    },
    {
        label: 'M11 — LapseRun.close() does not abort the signal',
        file: LAPSE_RUN,
        edits: [['        this.#abort.abort()\n', '']],
        killedBy: '#349 W11 close() while a run is gated',
    },
    {
        label: 'M13 — LapseRun.trigger() ignores its closed state',
        file: LAPSE_RUN,
        edits: [[
            '        if (this.#closed || !this.#handler) return\n',
            '        if (!this.#handler) return\n',
        ]],
        // Killable only between close() being called and resolving: once it
        // resolves the handler is dropped, and `!this.#handler` alone refuses
        // the trigger. W14 as first committed triggered only after `await
        // close()` and stayed GREEN under this mutant; it now also triggers in
        // that window. Run both ways: the first W14 passed, the second fails.
        killedBy: '#349 W14',
    },
    {
        label: 'M14 — a failed run does not call onFailure',
        file: LAPSE_RUN,
        edits: [['            this.#onFailure()\n', '']],
        // W9 is the driver-level witness (the next beat restores the failed
        // slot only because the run marked the lapse suspected); this group
        // runs the unit suite alone, where WS2 counts the calls.
        killedBy: '#349 WS2',
    },
    {
        label: 'M23 — the handler called outside the try',
        file: LAPSE_RUN,
        edits: [[
            '        try {\n' +
            '            await handler(this.#abort.signal)\n' +
            '        } catch (error) {\n',
            '        const pending = handler(this.#abort.signal)\n' +
            '        try {\n' +
            '            await pending\n' +
            '        } catch (error) {\n',
        ]],
        // A synchronous throw escapes the run as a rejection nobody handles,
        // with no WARN and no `onFailure`: WS2 fails on "exactly one WARN".
        // It used to die as `(uncaught error)`: WS2's `finally` removed its
        // `unhandledrejection` listener in the same turn as the failing
        // assertion, before the event was dispatched, so the rejection took
        // the whole file down. The listener now outlives one macrotask — a
        // contract of the shared `watchingEscapes` since #374, pinned by
        // `escape_watcher.test.ts`.
        killedBy: '#349 WS2',
    },
]

if (import.meta.main) {
    const unresolved = await runBattery(
        '#349 — a lapsed instance re-holds its slots (driver and manager)',
        [REHOLD],
        REHOLD_ROWS,
    ) +
        await runBattery(
            '#349 — no self-frames (the merged #344 row)',
            [REHOLD, TRANSITIONS],
            SELF_EXCLUSION_ROWS,
        ) +
        await runBattery(
            '#349 — when the lapse handler runs (LapseRun)',
            [UNIT],
            LAPSE_RUN_ROWS,
        )
    Deno.exit(unresolved > 0 ? 1 : 0)
}
