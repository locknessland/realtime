/**
 * @fileoverview #333's mutation battery — the barrier shares a read without
 * costing freshness, and gives the channel back.
 *
 * The disposition that chose this design recommended exactly one row: invert
 * the trailing edge to the leading edge, and let the joiner-sees-itself test
 * kill it. That is the first row here, and it is the reason the battery exists
 * at all — **the two designs differ in one expression**, the smaller one is
 * wrong, and the wrong one is what gets re-proposed by anybody who reads
 * `snapshot` without reading why.
 *
 * The other three cover the traps this shape carries rather than the choice it
 * makes: a continuation that skips the rejection path, a slot that is never
 * given back, and a barrier that shares nothing at all.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/roster_read_barrier_333.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/roster_read_barrier_333
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const BARRIER = new URL('../../roster_read_barrier.ts', import.meta.url)
const SUITES = [
    new URL('../roster_read_barrier_333.test.ts', import.meta.url).pathname,
    // The manager-level witness, and the only place the leading edge shows up
    // as a CORRECTNESS failure rather than a count.
    new URL('../presence_roster_read_333.test.ts', import.meta.url).pathname,
    // The invariant this change must not weaken: a subscribe answers with the
    // authoritative roster, whole.
    new URL('../presence_rejoin_327.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#333 the barrier becomes a LEADING-edge single-flight',
        file: BARRIER,
        edits: [[
            '        const promise = ahead.then(\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '        )\n',
            '        const promise = ahead\n',
        ]],
        // THE DESIGN DECISION, as one expression: a caller arriving during a
        // read is answered by the read already running — so it is handed an
        // answer issued BEFORE it asked, and a joiner whose own roster write
        // committed in the meantime receives a roster it is not in. It renders
        // it. Nothing throws, nothing is logged, no type changes.
        //
        // Anchored on the CHAIN in `#open` — re-anchored for #341, where the
        // `slot.next` continuation became one continuation per queued batch.
        // `ahead` is the read already running for the first batch, so the
        // mutant hands every queued caller that read: the same leading edge.
        // Not on an early return: replacing one leaves code unreachable, the
        // type-checker refuses it, and a mutant that will not compile is not a
        // mutant — it is a green row reporting coverage it never measured.
        killedBy: 'a joiner sees ITSELF in its own reply',
    },
    {
        label: '#333 the queued continuation skips the rejection path',
        file: BARRIER,
        edits: [[
            '        const promise = ahead.then(\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '        )\n',
            '        const promise = ahead.then(() => this.#issue(channel, [...ids]))\n',
        ]],
        // The trap in this shape, now on every batch's continuation (#341).
        // One driver rejection and every caller queued
        // behind it waits on a promise nothing will ever settle — socket
        // healthy, nothing logged, the room unreadable for the life of the
        // process.
        killedBy: 'a rejected read still releases the channel',
    },
    {
        label: '#333 the settled slot is retained instead of given back',
        file: BARRIER,
        edits: [[
            '            if (!head) {\n' +
            '                this.#slots.delete(channel)\n' +
            '                return\n' +
            '            }\n',
            '            if (!head) return\n',
        ]],
        // Re-anchored for #341: the release now happens when no batch is left
        // to promote (`head`), not when `slot.next` is empty — same property.
        // Then the map is keyed by names ever seen rather than reads in
        // flight, and clients choose the names — which is #334's defect
        // relocated into the remedy for #333.
        killedBy: 'the barrier retains NOTHING once a burst settles',
    },
    {
        label: '#333 the barrier shares nothing and every caller reads',
        file: BARRIER,
        edits: [[
            '        const slot = this.#slots.get(channel)\n' +
            '        if (!slot) {\n',
            "        const slot = this.#slots.get(channel + '\\0never')\n" +
            '        if (!slot) {\n',
        ]],
        // THE NULL CHANGE: a barrier wired in, constructed, and doing nothing.
        // The issue's original criterion — "fewer than K reads" — is satisfied
        // by this mutant on most runs, which is why it was rewritten to exactly
        // 1 and then exactly 2.
        //
        // Two anchor lines, because `#watch` opens with the same lookup and one
        // line matches twice. A lookup that cannot hit rather than a cast: same
        // types, no unreachable code, every caller on the fresh-read path.
        killedBy: 'K concurrent callers on one channel cost exactly two reads',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#333 — a shared read that costs no freshness',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
