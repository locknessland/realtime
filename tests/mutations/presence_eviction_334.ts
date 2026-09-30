/**
 * @fileoverview #334's mutation battery — an emptied presence map is given back.
 *
 * This invariant has **no observable consequence**, and that is the whole
 * reason it needed a battery. Its sibling in `subscriptions` is guarded by
 * arithmetic — `#checkChannelCaps` reads `subscriptions.size`, so a retained
 * empty `Set` overshoots the channel cap and a behavioural test catches it. No
 * cap counts `presence`, and the one path that scans it computes the same
 * absent state from an absent entry and an empty one. Every mutant below is
 * therefore killed by a test that reads the map directly, and a row that stops
 * dying means that reader was deleted or weakened — not that the code became
 * safe.
 *
 * The last row is the one worth keeping honest about. It does not mutate the
 * delete at all; it mutates the ORDER around it, because `unsubscribe` drops
 * the local entry before `#syncRosterMember` derives what to write from that
 * same entry. That coupling is what makes the fix safe, and it is invisible in
 * every signature involved.
 *
 * ## One row was written, ran, SURVIVED, and was removed rather than kept
 *
 * Weakening the helper's membership predicate to `if (!members)` — letting a
 * leave for a non-member reach the size check — survives, and tracing it says
 * why: it is **equivalent**, not merely unwitnessed. A non-member leave finds
 * `members.size > 0` whenever anyone is in the room, and the one state where
 * it would differ — an entry that exists and is empty — is precisely the state
 * this fix abolishes. The guard is still correct and still cheap, and it
 * stays; what it does not have is a test that can fail without it.
 *
 * The row is recorded here instead of left in the list, because a battery row
 * that cannot die reads as coverage on every future run. That is the same
 * mistake as a green test that never executes its subject.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_eviction_334.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_eviction_334
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_eviction_334.test.ts', import.meta.url).pathname,
    // The leave outcomes share the predicate this battery mutates: the helper
    // reports the removed member, and `unsubscribe` gates every announcement on
    // it. A mutation that breaks the report and not the delete dies here.
    new URL('../leave_outcome_332.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#334 the emptied channel map is kept, as it was before',
        file: MANAGER,
        edits: [[
            '        if (members.size === 0) this.presence.delete(channel)\n',
            '',
        ]],
        // The defect itself, restored. Nothing else in the package changes
        // behaviour — which is exactly how it survived to be filed.
        killedBy: 'the channel entry is GONE once the last member leaves',
    },
    {
        label: '#334 the delete runs on EVERY leave, not on the 1→0 transition',
        file: MANAGER,
        edits: [[
            '        if (members.size === 0) this.presence.delete(channel)\n',
            '        this.presence.delete(channel)\n',
        ]],
        // Far worse than the leak it over-corrects: the projection would then
        // compute "absent" for members who are still in the room and issue
        // removals that evict them from the cluster-wide roster.
        killedBy: 'the entry SURVIVES while any member remains',
    },
    {
        label:
            '#334 the local entry is dropped AFTER the roster write, not before',
        file: MANAGER,
        // RE-ANCHORED by #361: `unsubscribe` now forgets BEFORE its awaited
        // leave, so the forget and `if (member) {` are no longer adjacent. The
        // forget is still unique — the #323 compensation forgets
        // `connection.id` — and the guard is the same.
        edits: [[
            '        const member = this.#forgetPresenceMember(channel, clientId)\n',
            '        const member = this.presence.get(channel)?.get(clientId)\n',
        ]],
        // The ordering row. The local removal never happens at all, so
        // `#syncRosterMember` derives the member as still present and re-adds
        // the slot it was asked to remove — the ghost this fix must not create.
        killedBy: 'the authoritative roster still receives the removal',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#334 — an emptied presence map is given back',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
