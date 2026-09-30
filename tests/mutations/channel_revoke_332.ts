/**
 * @fileoverview #332's mutation battery — the four edits a green suite would
 * otherwise survive.
 *
 * Three of these mutants are invisible to every same-version test by
 * construction, which is why they are here rather than left to the suite:
 *
 * - **Dropping `clearRevocation`** looks correct on any single reconcile tick.
 *   Only the *second* tick, after a legitimate re-subscribe, sees the client
 *   kicked again — and only one witness fires two ticks.
 * - **Changing the composite delimiter to a charset character** passes
 *   everything except the mixed-fleet witness. It is the single most dangerous
 *   edit in this feature: inside the charset, a composite collides with a real
 *   connection id, and an instance running the previous release applies a room
 *   ban as a 4403 kill of the whole session.
 * - **Degrading the decoder to `{ target }` on a malformed member** is the same
 *   escalation arriving through the ingest path instead of the wire format. It
 *   type-checks, and every record this driver writes is well-formed, so nothing
 *   but a hostile planted member can tell.
 *
 * The fourth is ordinary and included because its failure is silent rather than
 * loud: reporting the leave from the wrong place inside `#leaveLocal`.
 *
 * **#337 adds four rows, one per place the revocation id must travel.** Drop it
 * from the member, the publish or the MAC, or apply the reconcile one record at
 * a time, and a clear stops being exact — every one of those still passes a
 * suite that only ever revokes a pair once.
 *
 * Every row names the test it dies to, and the harness verifies that
 * attribution: a kill by the wrong test is reported as MISATTRIBUTED.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/channel_revoke_332.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/channel_revoke_332
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const DRIVER = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../channel_revoke_332.test.ts', import.meta.url).pathname,
    new URL('../control_mac_coverage.test.ts', import.meta.url).pathname,
    new URL('../leave_outcome_332.test.ts', import.meta.url).pathname,
    new URL('../mixed_fleet_332.test.ts', import.meta.url).pathname,
    new URL('../revocation_encoding_332.test.ts', import.meta.url).pathname,
    new URL('../revocation_clear_race_337.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#332 the applied record is never cleared',
        file: MANAGER,
        edits: [[
            '            await this.#revocations?.clearRevocation(revocation)\n',
            '',
        ]],
        // The record then means "a revocation that was issued" rather than
        // "one the owner has not applied yet", and every reconcile tick
        // re-applies it for the whole TTL — kicking a client that legitimately
        // re-subscribed, once per tick. A single-tick witness sees nothing.
        killedBy: 'a LOST control frame is recovered by the reconcile',
    },
    {
        label: '#332 the composite delimiter moved INSIDE the name charset',
        file: DRIVER,
        edits: [[
            "const REVOCATION_SCOPE_SEPARATOR = ' '",
            "const REVOCATION_SCOPE_SEPARATOR = ':'",
        ]],
        // The whole mixed-fleet safety argument rests on this one character.
        // Inside the charset, `c1:presence-room` is a well-formed connection
        // id — so a reader on the previous release stops skipping the record
        // and hands it to a whole-connection revoke: hard-close 4403, every
        // other room gone. Same-version behaviour is completely unchanged,
        // which is exactly what makes the edit look like a tidy-up.
        killedBy:
            'a previous-release reader is INERT on a channel-scoped record',
    },
    {
        label:
            '#332 the decoder degrades to connection scope instead of dropping',
        file: DRIVER,
        edits: [[
            '        if (parts.length !== 3) return undefined',
            '        if (parts.length !== 3) return { target: parts[0] }',
        ]],
        // Escalation-by-omission through the ingest path. A malformed member
        // comes back carrying no channel, and the manager applies it as a
        // whole-connection revocation. It type-checks, and no record this
        // driver writes can trigger it — only a planted one can.
        killedBy: 'an undecodable member is DROPPED, never widened',
    },
    {
        // THE OTHER PARTS of the decoder guard: a composite carries THREE
        // names since #337 and all are broker-sourced. Validating only the
        // target admits a channel or an id outside the charset. Checking the
        // first part rather than deleting the guard, because that is the shape
        // a real edit takes: it still looks like a validation and still
        // refuses a bare invalid member.
        label:
            '#332 only ONE part of a composite revocation is charset-checked',
        file: DRIVER,
        edits: [[
            'if (!parts.every((part) => isValidName(part))) return undefined',
            'if (!isValidName(parts[0])) return undefined',
        ]],
        killedBy: 'an undecodable member is DROPPED, never widened',
    },
    {
        // PROMISED BY THE BREAKDOWN AND NOT WRITTEN — the review caught the
        // gap, and a clean battery had been reported over its absence. Without
        // the guard every instance applies every live record: the leave is a
        // no-op on a non-owner, but the CLEAR that follows is not, so the one
        // instance that could have acted on the record finds it already gone.
        label: '#332 the reconcile applies records it does not own',
        file: MANAGER,
        edits: [[
            '            if (!this.connections.has(revocation.target)) continue',
            '            if (this.connections.has(revocation.target)) continue',
        ]],
        killedBy: 'the reconcile applies a record ONLY to a socket',
    },
    {
        // The review's HIGH, so it can never come back quietly. `subscribe`
        // suspends at the authorizer before `#joinLocal`, so a revoke in that
        // window gets 'not-subscribed' and — clearing unconditionally — throws
        // away the only thing that can catch the membership about to land.
        // The connection then stays in the room permanently while the caller
        // was told the revoke was a no-op.
        label:
            '#332 the durable record is cleared even when nothing was removed',
        file: MANAGER,
        edits: [[
            "        if (left === 'left') {\n            for (const id of group.ids) {",
            '        if (left !== undefined) {\n            for (const id of group.ids) {',
        ]],
        killedBy:
            'a revoke that found nothing to remove KEEPS its durable record',
    },
    {
        // A frame that named a room and lost its channel must be dropped, not
        // read as "no channel, therefore the whole connection" — which the
        // scope mapping would turn into a hard-close 4403.
        label:
            '#332 a channel-less revoke-channel frame is widened to the socket',
        file: MANAGER,
        edits: [[
            '                    control.channel === undefined ||\n',
            '',
        ], [
            '                    channel: control.channel,\n                    ids: [control.revocationId],',
            '                    channel: control.channel as string,\n                    ids: [control.revocationId],',
        ], [
            '                            safeForLog(control.channel)\n',
            '                            safeForLog(String(control.channel))\n',
        ]],
        killedBy: 'a revoke-channel frame with NO channel is dropped',
    },
    {
        label: '#332 the leave is reported from the END of #leaveLocal',
        file: MANAGER,
        edits: [[
            '        if (set.size > 0) return true',
            '        if (set.size > 0) return false',
        ]],
        // The tail is reached only on the 1→0 transition, so `unsubscribe`
        // answers 'not-subscribed' for every leave from a room that still
        // holds somebody else — the common case. Proven live before this row
        // was written: with the mutation applied, exactly one witness fails
        // and the other four in its file pass.
        killedBy: 'a leave from a room that still holds SOMEONE ELSE',
    },
    {
        // #337, the member. Without the id every revocation of a pair is the
        // same member again, so a clear for the older one deletes the newer —
        // and the decoder now drops the two-part form, so the reconcile never
        // sees a channel record at all.
        label: '#337 the index member drops the revocation id',
        file: DRIVER,
        edits: [[
            '        return [target, channel, id].join(REVOCATION_SCOPE_SEPARATOR)',
            '        return [target, channel].join(REVOCATION_SCOPE_SEPARATOR)',
        ]],
        killedBy: 'two marks for one pair in the SAME second',
    },
    {
        // #337, the reconcile. One record at a time, the first leave returns
        // 'left' and clears its record; the second finds 'not-subscribed' and
        // survives to kick a re-subscribed client on the next tick.
        label: '#337 the reconcile applies each record on its own',
        file: MANAGER,
        // The id joins the grouping key, so every record is its own group —
        // the per-record apply expressed without losing type narrowing (a
        // rewrite to an unconditional apply leaves the grouping code
        // unreachable, and the mutant stops type-checking).
        edits: [[
            'const key = JSON.stringify([revocation.target, revocation.channel])',
            'const key = JSON.stringify([\n                revocation.target,\n                revocation.channel,\n                revocation.id,\n            ])',
        ]],
        killedBy: 'two records for one pair: ONE kick',
    },
    {
        // #337, the publish. A frame without its id names no record, so the
        // owner drops it and only the reconcile would ever apply the revoke.
        label: '#337 the revoke-channel publish omits revocationId',
        file: MANAGER,
        edits: [[
            '                revocationId: revocation.id,\n',
            '',
        ]],
        killedBy: 'A revokes one room on a socket B owns',
    },
    {
        // #337, the MAC. Same-version peers still agree with each other, so
        // nothing functional fails — the field simply ships unauthenticated,
        // and anyone with bus access could re-point a frame at another record.
        label: '#337 #canonical omits revocationId',
        file: DRIVER,
        edits: [[
            '            revocationId: wire.revocationId,\n        }))',
            '        }))',
        ]],
        killedBy: 'a revoke-channel frame is covered, revocationId included',
    },
]

Deno.exit(
    await runBattery(
        '#332/#337 mutation battery — scope, durability, exact clears and the mixed fleet',
        SUITES,
        MUTATIONS,
    ),
)
