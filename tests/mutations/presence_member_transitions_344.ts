/**
 * @fileoverview #344's mutation battery — `joined` and `left` announce a
 * member's transitions, not a connection's.
 *
 * The defect was an announcement per connection: a second tab of member 7
 * announced `joined` for a member already present, closing one of two tabs
 * announced `left` for a member still there, and on two instances each one
 * announced its own. The remedy moves the announcement INTO the queued roster
 * write that observes the transition (`arrived` 0 → 1, `gone` 1 → 0), so every
 * row below either breaks the observation or moves the announcement back out.
 *
 * - M1 / M2 / M5 / M10 break the observation: ignore `arrived`, ignore the
 *   roster's `gone`, let the roster-less `#heldSlots` release report `gone`
 *   always, let the memory driver report `arrived` always.
 * - M3 / M4 break it inside the Redis scripts: the hold drops `n == 1`, the
 *   release drops "a non-holder is not a departure".
 * - M6 moves an announcement back into `unsubscribe`; M8 rethrows a publish
 *   failure out of the queue; M13 lets an `encode` throw in the local emit
 *   skip the control publish again; the WARN row puts the member id back in
 *   the log.
 * - M11 makes an overtaken join announce `joined` for the member its own
 *   leave removed; M12 announces the origin's entry instead of the one the
 *   roster holds.
 * - M7 and the `handleControl` row, which narrowed `joined`'s exclusion back
 *   to one connection locally and on the receiving instance, are retired:
 *   see below.
 * - M9 stops refusing a driver that still offers `addMember` / `removeMember`.
 *
 * **Retired rows carried here** (`docs/testing.md`, subsumption):
 *
 * - `presence_join_rosterless_342.ts` — **the whole battery is retired and its
 *   file deleted.** Its M1, M2 and M3 mutated the "no roster" vs "superseded"
 *   return of `#syncRosterMember`. That return no longer exists — it is
 *   `Promise<void>` and the announcement is the write's own. Their question
 *   ("does a roster-less join announce exactly once, and a roster-less join
 *   overtaken by an unsubscribe announce nothing?") is now asked by M5, M2 and
 *   M11. M1 and M2's reason is carried verbatim onto M5. M3's reason had two
 *   halves — an overtaken join announcing `joined`, and the `left` that would
 *   then follow — and no single row carries both: M2 carries the `left` half
 *   (a release that never held the slot reporting `gone`), M11 the `joined`
 *   half, verbatim. The file was first kept as a record that printed and
 *   exited 0 — which `deno task mutate` and the nightly sweep counted as a
 *   clean battery with no rows, and the generated brief listed as live. A
 *   subsumed row leaves by deletion with its reason on its successor
 *   (`docs/testing.md`), so this header and those three rows are the record.
 *   Its witness suite, `presence_join_rosterless_342.test.ts`, is not retired:
 *   M5 still dies to it.
 * - `roster_sync_330.ts` "a superseded join announces anyway" neutralised the
 *   same superseded branch — its mutant published `presence-join` — so its
 *   successor is M11, not M2.
 * - `presence_join_323.ts` "the newcomer is no longer excluded from its own
 *   join" mutated `{ except: connection.id }` in `subscribe`; that call site is
 *   gone and its successor was M7 — now `lapse_rehold_349.ts` M12, below.
 * - **This battery's own M7** ("`joined` excludes only the origin
 *   connection") **and its `handleControl` row** ("`handleControl` re-emits
 *   a remote `joined` without exceptMemberId") are subsumed into
 *   `lapse_rehold_349.ts` **M12** ("the self-exclusion dropped from
 *   emitPresence"). Both mutated the `exceptMemberId` option, which #349
 *   removed: the exclusion is now `emitPresence`'s own, for `joined` and
 *   `left`, and no caller passes or can drop it. M12 carries both reasons
 *   verbatim and still dies to #344 W9 and #344 W9 remote, the two witnesses
 *   these rows named.
 *
 * Rows are grouped by the ONE test file each dies to, and each group runs only
 * that file. Every row was proven LIVE by the harness run that recorded it:
 * the mutant ran and turned its named witness red.
 *
 * ```bash
 * deno task mutate presence_member_transitions_344
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_member_transitions_344
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const MEMORY = new URL('../../drivers/memory.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const TRANSITIONS = new URL(
    '../presence_member_transitions_344.test.ts',
    import.meta.url,
).pathname
const ROSTERLESS = new URL(
    '../presence_join_rosterless_342.test.ts',
    import.meta.url,
).pathname

/** Rows killed by `presence_member_transitions_344.test.ts`. */
const TRANSITION_ROWS: Mutation[] = [
    {
        label: '#344 M1 the hold path ignores `arrived`',
        file: MANAGER,
        edits: [[
            '                if (arrived) {\n',
            '                if (true) {\n',
        ]],
        // Every queued hold announces: the second tab of a present member says
        // `joined` again.
        killedBy:
            '#344 W1–W3 memory: two tabs as one member announce ONE joined',
    },
    {
        label: "#344 M2 the release path ignores the roster's `gone`",
        file: MANAGER,
        edits: [[
            '                gone = (await roster.releaseMember(channel, field)).gone\n',
            '                await roster.releaseMember(channel, field)\n' +
            '                gone = true\n',
        ]],
        // Half-successor to `presence_join_rosterless_342.ts` M3 ("a
        // roster-less join skips the superseded check"): the `left` half. The
        // overtaken join now releases a slot it never held; only `gone` keeps
        // it from announcing a departure nobody was told had arrived. The
        // `joined` half is M11.
        killedBy: '#344 W6 a join overtaken by its own leave announces nothing',
    },
    {
        label: '#344 M11 an overtaken join announces `joined` anyway',
        file: MANAGER,
        edits: [[
            '            if (gone) {\n' +
            '                await this.#announcePresence(\n' +
            "                    'left',\n" +
            '                    channel,\n' +
            '                    origin.member,\n' +
            '                    origin.clientId,\n' +
            '                )\n' +
            '            }\n',
            '            if (gone) {\n' +
            '                await this.#announcePresence(\n' +
            "                    'left',\n" +
            '                    channel,\n' +
            '                    origin.member,\n' +
            '                    origin.clientId,\n' +
            '                )\n' +
            '            } else {\n' +
            "                await this.#announcePresence('joined', channel, origin.member, origin.clientId)\n" +
            '            }\n',
        ]],
        // SUCCESSOR to `presence_join_rosterless_342.ts` M3's `joined` half,
        // whose reason was: "The wrong fix: W1 goes green under it, and a join
        // an `unsubscribe` overtook announces `joined` for a member the local
        // map no longer holds — #330's rule broken on the drivers #330 never
        // exercised." The overtaken write finds no local entry and takes the
        // release path; this row announces from there.
        //
        // SUCCESSOR also to `roster_sync_330.ts` "#330 a superseded join
        // announces anyway", whose reason was: "The join then publishes
        // `presence-join` for a member its own write removed — #323's rule
        // broken from a direction #323 could not have seen, since the write
        // that supersedes it comes from another verb entirely."
        killedBy: '#344 W6 a join overtaken by its own leave announces nothing',
    },
    {
        label: "#344 M12 `joined` carries the origin's entry, not the held one",
        file: MANAGER,
        edits: [[
            "                        'joined',\n" +
            '                        channel,\n' +
            '                        desired,\n',
            "                        'joined',\n" +
            '                        channel,\n' +
            '                        origin.member,\n',
        ]],
        // The frame then shows other instances an `info` the roster does not
        // hold, whenever the write that observed the arrival is not the
        // member's earliest local connection's.
        killedBy:
            "#344 the arrival's joined carries the earliest local connection's entry",
    },
    {
        // Re-anchored for #414: the reply widened to
        // {arrived, ownedKind, instancesKind}.
        label: '#344 M3 the hold script drops `n == 1`',
        file: REDIS,
        edits: [[
            "    'if added == 1 then',\n" +
            "    '  if n == 1 then',\n" +
            "    '    return {1, ownedKind, instancesKind}',\n" +
            "    '  end',\n" +
            "    'end',\n",
            "    'if added == 1 then',\n" +
            "    '    return {1, ownedKind, instancesKind}',\n" +
            "    'end',\n",
        ]],
        // Every instance's first hold reads as an arrival: member 7 on two
        // instances is announced twice.
        killedBy: '#344 W5 member 7 on two instances: one presence-join',
    },
    {
        // Re-anchored for #414: the reply widened to {value, ownedKind}.
        label: '#344 M4 the release script reports a non-holder as a departure',
        file: REDIS,
        edits: [[
            "    '  if mine == false then',\n" +
            "    '    return {0, ownedKind}',\n" +
            "    '  end',\n",
            '',
        ]],
        // A release by an instance that never held the slot, on an empty or
        // legacy slot, used to answer `gone` and announce a `left` nobody
        // earned. Since #348 the script answers `mine` itself when the slot
        // empties, and a non-holder's `mine` is Lua `false` — a nil reply —
        // so the mutant now dies because `decodeReleaseReply` throws on nil:
        // the non-holder's release rejects instead of reporting nothing.
        // Re-proven live for #355: the decoder now names four replies
        // (emptied, KEPT, 0, REFUSED) and a nil is still none of them, so it
        // still throws — the anchor survived, the code under it changed.
        killedBy: '#344 W11 FakeRedis: the six contract rows',
    },
    {
        label: '#344 M6 unsubscribe announces `left` itself again',
        file: MANAGER,
        edits: [[
            '            await this.#syncRosterMember(channel, { clientId, member })\n',
            '            await this.#syncRosterMember(channel, { clientId, member })\n' +
            "            this.emitPresence(channel, { type: 'presence', channel, action: 'left', member })\n",
        ]],
        // The per-connection announcement, restored at the call site the
        // queue replaced. Re-proven live for #349: the injected call still
        // compiles against `emitPresence(channel, frame)`, whose exclusion
        // now covers `left` too.
        killedBy:
            '#344 W1–W3 memory: closing one of two tabs announces nothing',
    },
    {
        label: '#344 M8 a publish failure is rethrown out of the queue',
        file: MANAGER,
        edits: [[
            "            lost('other instances', error)\n",
            "            lost('other instances', error)\n" +
            '            throw error\n',
        ]],
        // `unsubscribe` rejects for a leave that committed, and a join's
        // rollback would undo a hold that committed.
        killedBy:
            '#344 W10 a failed presence-leave publish: unsubscribe resolves left',
    },
    {
        label: '#344 M13 an encode throw in the local emit skips the publish',
        file: MANAGER,
        edits: [[
            '        } catch (error) {\n' +
            "            lost('local subscribers', error)\n" +
            '        }\n' +
            '        try {\n',
            '        } catch (error) {\n' +
            "            lost('local subscribers', error)\n" +
            '            return\n' +
            '        }\n' +
            '        try {\n',
        ]],
        // FR-010's pre-review shape: one `try` around both halves, so the
        // application's codec refusing a frame for this instance's sockets
        // silenced every other instance too. Re-proven live for #349: the
        // local emit it guards lost its options argument.
        killedBy:
            '#344 W10 an encode that refuses the local joined frame: one WARN, no rethrow, and the presence-join is still published',
    },
    {
        label: '#344 S4 the announcement WARN names the member',
        file: MANAGER,
        edits: [[
            '                } was not announced to ${audience} — the roster is written ` +\n',
            '                } for ${String(member.id)} ${JSON.stringify(member.info)} was not announced to ${audience} — the roster is written ` +\n',
        ]],
        // Member ids and `info` may be application PII; the log carries the
        // channel, the action and the error only.
        killedBy: 'with one WARN naming no member',
    },
    {
        label: '#344 M9 addMember / removeMember are no longer refused',
        file: MANAGER,
        edits: [[
            "    'listMembers',\n" +
            "    'addMember',\n" +
            "    'removeMember',\n" +
            '] as const\n',
            "    'listMembers',\n" +
            '] as const\n',
        ]],
        // A 0.3.0 add/remove driver without `listMembers` constructs and fails
        // later with a `TypeError` inside the #323 rollback.
        killedBy:
            '#344 W12 a driver still offering addMember / removeMember is refused at construction',
    },
    {
        label: '#344 M10 the memory driver reports every hold as an arrival',
        file: MEMORY,
        edits: [[
            '        const arrived = !members.has(key)\n',
            '        const arrived = true\n',
        ]],
        killedBy:
            '#344 W1–W3 memory: two tabs as one member announce ONE joined',
    },
]

/** Rows killed by `presence_join_rosterless_342.test.ts`. */
const ROSTERLESS_ROWS: Mutation[] = [
    {
        label: '#344 M5 the roster-less `#heldSlots` release is always `gone`',
        file: MANAGER,
        edits: [[
            '                gone = this.#heldSlots.delete(key)\n',
            '                this.#heldSlots.delete(key)\n' +
            '                gone = true\n',
        ]],
        // SUCCESSOR to `presence_join_rosterless_342.ts` M1 and M2, whose
        // reason was: "The tail still runs and the slot still serializes; only
        // the answer is lost. Every roster-less join then reads as overtaken."
        // — "no roster" collapsing into "superseded". The collapse now has one
        // shape left: a roster-less release that never held the slot reporting
        // a departure, which announces `left` for an overtaken join.
        killedBy:
            '#342 a roster-less join overtaken by an unsubscribe announces nothing',
    },
]

if (import.meta.main) {
    const unresolved = await runBattery(
        '#344 — announcements follow member transitions (transitions suite)',
        [TRANSITIONS],
        TRANSITION_ROWS,
    ) +
        await runBattery(
            '#344 — announcements follow member transitions (roster-less suite)',
            [ROSTERLESS],
            ROSTERLESS_ROWS,
        )
    Deno.exit(unresolved > 0 ? 1 : 0)
}
