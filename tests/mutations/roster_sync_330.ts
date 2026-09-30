/**
 * @fileoverview #330's mutation battery — one roster write per slot, at a time.
 *
 * The defect this closed was an ORDER, like #323's, and orders are what a suite
 * is worst at holding: invisible in every signature, surviving every type check.
 * Worse here, because the two writes reach the wire from different methods —
 * a witness that watched only the end state of a single verb saw nothing.
 *
 * Every row names the test it dies to, and the harness verifies that
 * attribution: a kill by the wrong test is reported as MISATTRIBUTED and
 * counted against the run.
 *
 * **One property here has a witness and no mutant, and that is stated rather
 * than papered over.** `#syncRosterMember` computes the desired state at ISSUE
 * time — inside the serial tail — rather than at call time, and that is the
 * design decision the whole remedy rests on. It cannot be expressed as a text
 * substitution: reversing it means hoisting the `find` out of the
 * `prior.then(...)` callback and passing its result in, which is a
 * restructuring, not an edit. `a stale removal cannot land on top of a fresh
 * re-join` is its only guard. A placeholder row was written for it and then
 * deleted: a mutation that cannot die reports a result without measuring one,
 * which is the same fault as a stale anchor wearing a green tick.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/roster_sync_330.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/roster_sync_330
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../subscribe_unsubscribe_race_330.test.ts', import.meta.url)
        .pathname,
    new URL('../presence_join_compensation_323.test.ts', import.meta.url)
        .pathname,
    new URL('../presence_rejoin_327.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#330 the per-slot tail is not chained (writes overlap again)',
        file: MANAGER,
        edits: [[
            '        const prior = this.#rosterTails.get(key) ?? Promise.resolve()\n',
            '        const prior = Promise.resolve()\n',
        ]],
        // The projection survives; only the serialization goes. Both writes
        // then compute their desired state from whatever the local map says at
        // their own issue time and race to the wire — which is the shipped
        // defect with a tidier shape. Proven live before this row was written:
        // two of the four witnesses fail.
        killedBy: 'a pipelined subscribe+unsubscribe leaves no roster ghost',
    },
    // ── RETIRED by #344, with the reason, rather than deleted ──────────────
    //
    // `#330 a superseded join announces anyway` stood here. It neutralised the
    // join's `if (applied === undefined) {` superseded branch, and its reason
    // was: "NEUTRALISED rather than deleted: the branch still compiles and the
    // `#closingRead` inside it stays reachable to the type checker, so the
    // mutant fails by test name rather than by a compile error the harness
    // would report as dead. The join then publishes `presence-join` for a
    // member its own write removed — #323's rule broken from a direction
    // #323 could not have seen, since the write that supersedes it comes
    // from another verb entirely."
    //
    // That branch is gone: since #344 the queued write announces, and a join
    // an `unsubscribe` overtook reaches the RELEASE path, where only `gone`
    // keeps it silent. Forcing the announcement there sends a `left`, not a
    // `presence-join` — and `a superseded join announces nothing` counts
    // `presence-join` alone, so a re-anchored row was MISATTRIBUTED (killed
    // only by an unrelated #323 test). Subsumed by `#344 M2 the release path
    // ignores the roster's gone` in `presence_member_transitions_344.ts`,
    // killed by 344-W6, which asserts silence on both frames.
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#330 — one roster write per slot, at a time',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
