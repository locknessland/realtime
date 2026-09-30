/**
 * @fileoverview #348's mutation battery — a crashed instance's swept presence
 * members are announced as left, exactly once, and in order.
 *
 * The remedy spreads one decision over three homes: the release script says
 * which entry left (its reply), the sweep is the only caller of the departure
 * handler and checks what it reports, and the manager announces it through the
 * one announcement home — not on the slot's tail, and with nothing awaited in
 * between. Each row below drops one clause a refactor could drop while the
 * single-instance suite stays green.
 *
 * - M1 the sweep discards the release's reply: the #348 defect itself.
 * - M2 the kept reply carries the entry: a departure announced while a holder
 *   remains.
 * - M3 `releaseMember` reports a departure too: a second `left`.
 * - M4 the departure is decided from reads BEFORE the release: two sweepers
 *   both see the entry and both announce.
 * - M5 the manager registers without `?.`: a driver without the seam throws.
 * - M6 the departure's `target` is `''`: every peer refuses the frame.
 * - M7 the release decoder accepts an empty bulk as an entry.
 * - M8 the departure is chained on the slot's roster tail: `joined` then
 *   `left` for a member who is present.
 * - M9 one more command exchange is awaited before the handler: same order
 *   inversion. FakeRedis has no `PING`, so the extra exchange is an `EXISTS`
 *   on the instances set — a command it models — which on the serialized
 *   client W8 uses queues behind the in-flight hold exactly as a `PING` would.
 * - M10 the slot-binding check dropped: a broker-planted entry naming another
 *   member is reported as that member's departure.
 * - M11 a registration appends instead of replacing: a manager built twice on
 *   one driver announces every swept departure twice (A6).
 * - M12 `close()` keeps the handler: a sweep in flight when the driver closes
 *   still reports its departure (A6). Was `expectSurvival` since #355;
 *   re-proven a real kill since #368 (see the row).
 * - M13 the member rule's key check back to a COUNT (`<= 2`), the #348
 *   original: a driver-reported `{ id, smuggled }` passes the manager's
 *   departure check and the room hears `smuggled` in a `left` frame (S3).
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant ran
 * and turned its named witness red.
 *
 * **#355 re-anchored five rows and retired one kill.** M1 and M4 (the sweep's
 * release call), M2 (the script's tail now answers KEPT — the row became "the
 * kept reply answers the entry"), M3 (`releaseMember` maps a decoded outcome)
 * and M7 (the decoder's bulk branch builds the *emptied* outcome) were
 * repaired and re-proven live. M12 became `expectSurvival`: `close()` now
 * waits for the pass, which stops on `#closing`, so no departure can reach a
 * handler after it resolves — the reason on the row says what would falsify
 * that.
 *
 * **The #355 review re-anchored four more.** The sweep's writes moved into
 * `#sweepOwned` and the announcement into `#announceSwept`, which calls the
 * handler before its first await: M1 and M4 (the release call), M9 (the
 * handler call) and M10 (the slot-binding check) follow them, each re-proven
 * live, M9 still killed by W8.
 *
 * **#368 disproved M12's `expectSurvival` reasoning, and corrects it here
 * rather than erasing it.** The row's original claim — `close()` cannot
 * return before the pass has stopped, so a kept handler has no caller left
 * — held only while the wait was unbounded. Past the new TTL budget
 * (`drivers/close_drain.ts`) `close()` returns anyway, and a release still
 * in flight settles AFTER it: the handler being dropped is what makes that
 * settlement announce nothing (`close_drain_368.test.ts` W1). M12 is a real
 * kill again, by that same witness.
 *
 * ```bash
 * deno task mutate presence_sweep_departure_348
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_sweep_departure_348
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
// M13: the manager's departure handler asks `isPresenceMemberWire`.
const PROTOCOL = new URL('../../protocol.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_sweep_departure_348.test.ts', import.meta.url)
        .pathname,
    // M7's witness: the release decoder's row, #348 FR-004a, now #355 WD.
    new URL('../roster_holders_345.test.ts', import.meta.url).pathname,
    // M12's witness since #368: a release settling past the drain's budget.
    new URL('../close_drain_368.test.ts', import.meta.url).pathname,
]

// The sweep's one release call. Re-anchored for #355: it passes the liveness
// ask and answers a decoded `ReleaseOutcome`, not the raw entry — and, since
// the #355 review, it sits in `#sweepOwned`, one indent shallower.
const RELEASE_CALL = '            const outcome = await this.#release(\n' +
    '                channel,\n' +
    '                field,\n' +
    '                deadId,\n' +
    '                true,\n' +
    '            )\n'

const ANNOUNCE_DEPARTURE =
    "        return this.#announcePresence('left', channel, member, channel)\n"

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the sweep discards the release reply (the #348 defect)',
        file: REDIS,
        // Re-anchored for #355: the reply is still taken (the count needs
        // it), but an emptied slot's entry is thrown away — the #348 defect.
        edits: [[
            RELEASE_CALL,
            '            const reply = await this.#release(channel, field, deadId, true)\n' +
            "            const outcome = (reply.kind === 'emptied'\n" +
            "                ? { kind: 'kept' }\n" +
            '                : reply) as ReleaseOutcome\n',
        ]],
        killedBy: '#348 W1',
    },
    {
        label: 'M2 — the kept reply answers the entry',
        file: REDIS,
        // Re-anchored for #355: the script's tail no longer answers 0 while a
        // holder remains, it answers KEPT — rewritten as that reply carrying
        // the releaser's entry instead. Re-anchored again for #414: the
        // reply widened to {value, ownedKind}.
        edits: [[
            "    'if mine == false then',\n" +
            "    '  return {0, ownedKind}',\n" +
            "    'end',\n" +
            '    `return {${KEPT}, ownedKind}`,\n',
            "    'if mine == false then',\n" +
            "    '  return {0, ownedKind}',\n" +
            "    'end',\n" +
            "    'return {mine, ownedKind}',\n",
        ]],
        // B still holds 7: sweeping A would announce a member who is present.
        killedBy: '#348 W2',
    },
    {
        label: 'M3 — releaseMember also reports a departure',
        file: REDIS,
        // Re-anchored for #355: `releaseMember` maps a decoded outcome.
        edits: [[
            "        return { gone: outcome.kind === 'emptied' }\n",
            "        if (outcome.kind === 'emptied') {\n" +
            '            const member = this.#parseRosterValue(channel, outcome.entry)\n' +
            '            if (member) {\n' +
            '                await this.#departureHandler?.({ channel, member })\n' +
            '            }\n' +
            '        }\n' +
            "        return { gone: outcome.kind === 'emptied' }\n",
        ]],
        killedBy: '#348 W6',
    },
    {
        label: 'M4 — the departure decided from reads before the release',
        file: REDIS,
        // Re-anchored for #355: the reads decide the outcome; the release's
        // own reply is ignored.
        edits: [[
            RELEASE_CALL,
            '            const peek = asBulk(await this.command.command(\n' +
            "                'HGET', this.holdersKey(channel, field), deadId))\n" +
            '            const holders = asInteger(await this.command.command(\n' +
            "                'HLEN', this.holdersKey(channel, field)))\n" +
            '            await this.#release(channel, field, deadId, true)\n' +
            '            const outcome = (holders === 1 && peek\n' +
            "                ? { kind: 'emptied', entry: peek }\n" +
            "                : { kind: 'kept' }) as ReleaseOutcome\n",
        ]],
        // Right on one sweep; two interleaved sweeps both read the entry
        // before either release, and both announce it.
        killedBy: '#348 W3',
    },
    {
        label: 'M5 — the registration drops `?.`',
        file: MANAGER,
        edits: [[
            '            this.driver.onRosterDeparture?.((departure) =>\n',
            '            this.driver.onRosterDeparture!((departure) =>\n',
        ]],
        killedBy: '#348 W4 a driver without onRosterDeparture still builds',
    },
    {
        label: "M6 — the departure's target is ''",
        file: MANAGER,
        edits: [[
            ANNOUNCE_DEPARTURE,
            "        return this.#announcePresence('left', channel, member, '')\n",
        ]],
        // The peer's ingest refuses a presence frame whose target is not a
        // valid name: the non-winning sweeper's observer never hears it.
        killedBy: '#348 W3',
    },
    {
        label: 'M7 — the release decoder accepts any bulk, the empty one too',
        file: REDIS,
        // Re-anchored for #355: the decoder's bulk branch now builds the
        // *emptied* outcome, and the #348 FR-004a test became #355 WD.
        // Re-anchored again for #414: the outcome also carries `ownedKind`.
        edits: [[
            "    if (entry) return { kind: 'emptied', entry, ownedKind }\n",
            '    if (entry !== undefined) {\n' +
            "        return { kind: 'emptied', entry, ownedKind }\n" +
            '    }\n',
        ]],
        killedBy: '#355 WD a release reply is one of four outcomes',
    },
    {
        label: "M8 — the departure is chained on the slot's roster tail",
        file: MANAGER,
        edits: [[
            ANNOUNCE_DEPARTURE,
            '        const key = `${channel}\\0${String(member.id)}`\n' +
            '        const prior = this.#rosterTails.get(key) ?? Promise.resolve()\n' +
            '        return prior.then(() =>\n' +
            "            this.#announcePresence('left', channel, member, channel)\n" +
            '        )\n',
        ]],
        killedBy: '#348 W8',
    },
    {
        label: 'M9 — one more command exchange awaited before the handler',
        file: REDIS,
        edits: [[
            '            await handler({ channel, member })\n',
            "            await this.command.command('EXISTS', this.instancesKey)\n" +
            '            await handler({ channel, member })\n',
        ]],
        killedBy: '#348 W8',
    },
    {
        label: 'M10 — the slot-binding check dropped',
        file: REDIS,
        edits: [[
            '        if (!sameMemberId(member.id, field)) {\n',
            '        if (member.id === undefined) {\n',
        ]],
        killedBy: '#348 W7 an entry whose member id is not its slot',
    },
    {
        label: 'M11 — a second registration appends instead of replacing',
        file: REDIS,
        edits: [[
            '        this.#departureHandler = handler\n',
            '        const prior = this.#departureHandler\n' +
            '        this.#departureHandler = prior\n' +
            '            ? async (departure) => {\n' +
            '                await prior(departure)\n' +
            '                await handler(departure)\n' +
            '            }\n' +
            '            : handler\n',
        ]],
        killedBy: '#348 A6 the departure handler',
    },
    {
        // Re-anchored for #368 (the comment above the drop gained a second
        // line) and no longer `expectSurvival`: see the fileoverview's
        // correction note.
        label: 'M12 — close() keeps the departure handler',
        file: REDIS,
        edits: [[
            '        // A closed driver reports no departure either (#348) — dropped here\n' +
            '        // whether or not the drain above expired.\n' +
            '        this.#departureHandler = undefined\n',
            '        // A closed driver reports no departure either (#348) — dropped here\n' +
            '        // whether or not the drain above expired.\n',
        ]],
        // #368's own witness holds a release past the drain's budget on
        // purpose, so it is the one that now depends on this drop.
        killedBy: '#368 W1 ',
    },
    {
        label: 'M13 — the member key rule back to a count: { id, smuggled } ' +
            'is announced as left',
        file: PROTOCOL,
        edits: [[
            '    if (!Object.keys(value).every(isPresenceMemberKey)) return false\n',
            '    if (Object.keys(value).length > 2) return false\n',
        ]],
        killedBy: '#348 S3 a malformed departure from a driver is dropped',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#348 — swept presence members are announced as left',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
