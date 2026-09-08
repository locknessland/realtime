/**
 * @fileoverview Unit tests for the control-plane replay window (#272).
 *
 * The clock is injected rather than faked globally: the class takes a required
 * `now`, which is the whole point of that seam — a test drives time by moving a
 * variable, with no `FakeTime` and no shared global to restore.
 *
 * @module @lockness/realtime/tests/control_replay_window
 */

import { assertEquals } from '@std/assert'
import { ControlReplayWindow } from '../control_replay_window.ts'

const WINDOW = 30_000

/** A window with a clock the test moves by hand. */
function windowAt(start = 1_000_000, maxEntries?: number) {
    let clock = start
    const seen = new ControlReplayWindow({
        windowMs: WINDOW,
        now: () => clock,
        maxEntries,
    })
    return {
        seen,
        advance: (ms: number) => void (clock += ms),
        get now() {
            return clock
        },
    }
}

Deno.test('FR-002: a frame inside the window is admitted', () => {
    const { seen, now } = windowAt()
    assertEquals(seen.admit('a', 'n1', now), 'ok')
})

Deno.test('FR-002: a frame older than the window is stale', () => {
    const { seen, now } = windowAt()
    assertEquals(seen.admit('a', 'n1', now - WINDOW - 1), 'stale')
})

Deno.test('FR-002: a FUTURE-dated frame is stale too', () => {
    const { seen, now } = windowAt()
    // A one-sided check would let anyone who can push a clock forward extend
    // their own replay window without limit.
    assertEquals(seen.admit('a', 'n1', now + WINDOW + 1), 'stale')
})

Deno.test('FR-002: the window boundary is inclusive on both sides', () => {
    const { seen, now } = windowAt()
    assertEquals(seen.admit('a', 'past', now - WINDOW), 'ok')
    assertEquals(seen.admit('a', 'future', now + WINDOW), 'ok')
})

Deno.test('FR-003: the same (origin, nonce) twice is a duplicate', () => {
    const { seen, now } = windowAt()
    assertEquals(seen.admit('a', 'n1', now), 'ok')
    assertEquals(seen.admit('a', 'n1', now), 'duplicate')
})

Deno.test('FR-001/S1: two origins may use the SAME nonce value', () => {
    // The regression that would silently break legitimate traffic. Keyed on the
    // nonce alone, instance B's first frame is dropped as A's duplicate — and
    // publishing is fire-and-forget, so the loss is silent and permanent.
    // Mutation-verify by keying on `nonce` instead of `${origin} ${nonce}`.
    const { seen, now } = windowAt()
    assertEquals(seen.admit('instance-a', 'shared-nonce', now), 'ok')
    assertEquals(
        seen.admit('instance-b', 'shared-nonce', now),
        'ok',
        'a second instance publishing concurrently is not a replay of the first',
    )
})

Deno.test('FR-004: an entry past the window is pruned, so its nonce is admissible again', () => {
    const w = windowAt()
    assertEquals(w.seen.admit('a', 'n1', w.now), 'ok')
    assertEquals(w.seen.size, 1)

    w.advance(WINDOW + 1)
    // The same (origin, nonce) is admitted again — the old entry was pruned,
    // so it is no longer consulted. Re-issued with a CURRENT timestamp, since
    // the original one is now stale on its own.
    assertEquals(w.seen.admit('a', 'n1', w.now), 'ok')
    assertEquals(
        w.seen.size,
        1,
        'the expired entry went; the new one replaced it',
    )
})

Deno.test('FR-004: pruning is driven by admit, not by a timer', () => {
    const w = windowAt()
    assertEquals(w.seen.admit('a', 'n1', w.now), 'ok')
    w.advance(WINDOW + 1)
    // Nothing ran in between — no timer, no interval. The prune is driven by
    // this call alone, which is why a quiet instance prunes nothing and the
    // guarantee is "never consulted", not "never exists".
    assertEquals(w.seen.size, 1, 'still held while nothing was ingested')
    assertEquals(w.seen.admit('a', 'other', w.now), 'ok')
    assertEquals(w.seen.size, 1, 'the stale entry went only when admit ran')
})

Deno.test('FR-004/SC-005: the store never exceeds its entry cap', () => {
    const { seen, now } = windowAt(1_000_000, 4)
    const realWarn = console.warn
    const warnings: string[] = []
    console.warn = (...args: unknown[]) => void warnings.push(String(args[0]))
    try {
        for (let i = 0; i < 50; i++) seen.admit('a', `n${i}`, now)
        assertEquals(seen.size, 4, 'the cap holds under sustained load')
        assertEquals(
            warnings.filter((w) => w.includes('replay window is at its'))
                .length,
            1,
            'the cap WARN fires once, not once per admission',
        )
    } finally {
        console.warn = realWarn
    }
})

Deno.test('FR-004: at the cap it is the OLDEST entry that goes, not the newest', () => {
    const { seen, now } = windowAt(1_000_000, 2)
    const realWarn = console.warn
    console.warn = () => {}
    try {
        seen.admit('a', 'oldest', now)
        seen.admit('a', 'middle', now)
        seen.admit('a', 'newest', now)
        // Drop-oldest fails open for exactly one forgotten nonce; refuse-new
        // would fail closed and stop the control plane, which the plan rates
        // as the worse outcome.
        assertEquals(
            seen.admit('a', 'oldest', now),
            'ok',
            'the oldest was evicted, so it is admissible again',
        )
        assertEquals(
            seen.admit('a', 'newest', now),
            'duplicate',
            'the newest is still remembered',
        )
    } finally {
        console.warn = realWarn
    }
})

// ---------------------------------------------------------------------------
// #283 — the cap is shared, so it needs a rule about WHOSE entry goes
//
// Mutation battery, each applied to `control_replay_window.ts`, run, observed
// RED, and reverted:
//
//   | #  | Mutation                                             | Observed |
//   | -- | ---------------------------------------------------- | -------- |
//   |  1 | evict from the origin holding the MOST (rule 2)*      | RED      |
//   |  2 | evict the NEWEST head instead of the oldest           | RED      |
//   |  3 | take the first over-share origin, comparing nothing   | RED      |
//   |  4 | index by `issuedAt` instead of arrival order          | RED      |
//   |  5 | `seq` frozen, so no head ever compares as older       | RED      |
//   |  6 | ignore the share — evict from any origin              | RED      |
//   |  7 | the share is not divided by the origin count          | RED      |
//   |  8 | `#prune` deletes without maintaining `#byOrigin`      | RED      |
//   |  9 | `#forget` leaves an emptied origin bucket behind      | RED      |
//   | 10 | `DEFAULT_MAX_ENTRIES` 10_000 -> 1                     | RED      |
//   | 11 | `WARN_INTERVAL_MS` 60_000 -> 1                        | RED      |
//   | 12 | the exact-division fallback charges a bystander        | RED      |
//
// Two more live in `control_replay.test.ts`, against `drivers/redis.ts`:
// accepting `control.maxEntries` without FORWARDING it (RED), and removing its
// validation guard (RED).
//
// **Read row 1 before trusting any row here.** An earlier version of this table
// certified it RED while the mutation actually run was
// `owned.size > oldest` with `oldest` starting at `+Infinity` — a comparison
// that is never true, so it disabled selection entirely rather than
// implementing the rule it was labelled with. The real rule-2 mutant passed the
// whole suite and reached a third review gate carrying this table's assurance.
// A row is a claim about the tests; the mutation has to be the one the label
// says.
//
// * Row 1 takes TWO edits, not one: the accumulator's comparison AND its
//   initial value, since `size > oldest` against `+Infinity` is the exact
//   never-true form that produced the false certification. Verified RED in both
//   readings — share-guard kept, and pure rule 2.
//
// **Three rules were shipped and withdrawn before this one**, which is why the
// battery pins the discarded shapes rather than describing them:
//
//  1. Plain drop-oldest lets a busy instance shorten everyone's retention to
//     `maxEntries / total rate`, so an origin publishing once per window can be
//     forgotten before the window is over.
//  2. Evicting from the heaviest origin equalises COUNTS, and equal counts are
//     not equal protection — an instance publishing ten times as much needs ten
//     times the slots for the same wall-clock window. It cut a busy instance
//     from 3 445 remembered nonces to 500 while quiet ones rose 345 -> 500.
//  3. A fixed 64-entry floor with the eviction walk bounded at 256: past the
//     bound it dropped the globally-oldest entry, which by construction belongs
//     to an origin the walk had just found to be AT or under its share. The
//     bound voided the guarantee for exactly the origins it exists for — 95 of
//     99 quiet origins reached zero on the shape `at a REALISTIC cap` uses.
//
// Rows 1-5 exist because rules 2 and 4 name the same victim in every
// arrangement with only ONE origin over its share — which is every cap test
// here except the three probes. That is how rule 2 survived a full gate.
//
// **Scale matters more than it looks.** Every cap test but one runs at
// `maxEntries <= 10`, small enough that `maxEntries / origins` is trivially the
// binding term. `at a REALISTIC cap` runs at the shipped default with a
// fleet-sized origin count, and it is the only test here that would have caught
// withdrawn rule 3.
//
// Attribution, measured rather than assumed: row 6 dies to four tests, of which
// `at a REALISTIC cap` is one; row 7 dies to the three probes below and NOT to
// `at a REALISTIC cap`. An earlier version of this header credited the wrong
// test for row 7 and would have called `at a REALISTIC cap` row 6's sole
// killer — both the same class of unchecked claim as the row-1 mislabelling
// above, and both found by running it instead of reasoning about it.
// ---------------------------------------------------------------------------

/** Silence the at-cap WARN for a body, and hand back what it would have said. */
function capturingWarn<T>(body: (lines: string[]) => T): T {
    const lines: string[] = []
    const real = console.warn
    console.warn = (...args: unknown[]) => void lines.push(String(args[0]))
    try {
        return body(lines)
    } finally {
        console.warn = real
    }
}

Deno.test('#283: a noisy origin cannot crowd a quiet one out of the store', () => {
    // The whole point of a per-origin rule. Under drop-globally-oldest the
    // quiet origins are evicted FIRST — their entries are the oldest, and the
    // noisy origin's are the majority — so a single busy instance silently
    // disarms replay protection for every other instance in the fleet.
    const { seen, now } = windowAt(1_000_000, 10)
    capturingWarn(() => {
        for (const quiet of ['quiet-1', 'quiet-2', 'quiet-3']) {
            assertEquals(seen.admit(quiet, 'n', now), 'ok')
        }
        // One instance reconnecting a few thousand clients.
        for (let i = 0; i < 200; i++) seen.admit('noisy', `n${i}`, now)

        assertEquals(seen.size, 10, 'the global cap still holds')
        for (const quiet of ['quiet-1', 'quiet-2', 'quiet-3']) {
            assertEquals(
                seen.admit(quiet, 'n', now),
                'duplicate',
                `${quiet} is still remembered after 200 frames from one peer`,
            )
        }
    })
})

Deno.test('#283: drop-oldest is unchanged WITHIN the evicted origin', () => {
    // The per-origin rule changes WHOSE entry goes, never which of that
    // origin's. Refuse-new would fail the control plane closed, which #272's
    // risk table rates as worse than one forgotten in-window nonce.
    const { seen, now } = windowAt(1_000_000, 3)
    capturingWarn(() => {
        seen.admit('a', 'oldest', now)
        seen.admit('a', 'middle', now)
        seen.admit('a', 'newest', now)
        seen.admit('a', 'fourth', now)
        assertEquals(
            seen.admit('a', 'oldest', now),
            'ok',
            'a‘s OLDEST was the one evicted, so it is admissible again',
        )
        assertEquals(
            seen.admit('a', 'newest', now),
            'duplicate',
            'a‘s newer entries are untouched',
        )
    })
})

Deno.test('#283: with every origin equal, eviction falls to the globally oldest', () => {
    // The degenerate case must behave exactly as it did before this change —
    // a new rule that alters the uncontended path is a regression wearing a
    // feature's clothes.
    const { seen, now } = windowAt(1_000_000, 3)
    capturingWarn(() => {
        seen.admit('a', 'n', now)
        seen.admit('b', 'n', now)
        seen.admit('c', 'n', now)
        seen.admit('d', 'n', now)
        assertEquals(
            seen.admit('a', 'n', now),
            'ok',
            'a was the globally oldest among joint-heaviest origins',
        )
        assertEquals(seen.admit('c', 'n', now), 'duplicate', 'c survived')
    })
})

Deno.test('#283: the per-origin ledger survives pruning, not just eviction', () => {
    // The defect this guards: `#prune` removes entries on the window boundary,
    // and if it did not decrement the ledger the counts would claim entries
    // that no longer exist. Eviction would then hunt for an entry belonging to
    // a "heaviest" origin that holds nothing, find none, and return WITHOUT
    // freeing a slot — so the cap would silently stop holding.
    const w = windowAt(1_000_000, 5)
    capturingWarn(() => {
        for (let i = 0; i < 5; i++) w.seen.admit('ghost', `n${i}`, w.now)
        // Past the window: every `ghost` entry is now prunable.
        w.advance(WINDOW + 1)
        for (let i = 0; i < 40; i++) w.seen.admit('live', `n${i}`, w.now)
        assertEquals(
            w.seen.size,
            5,
            'the cap still holds after the ledger was drained by pruning',
        )
        assertEquals(
            w.seen.admit('live', 'n39', w.now),
            'duplicate',
            'the most recent live entry is remembered, so eviction freed a ' +
                'real slot rather than silently doing nothing',
        )
    })
})

Deno.test('#283: the at-cap warning re-arms instead of firing once per process', () => {
    // A boolean latch made "we were at the cap for ten seconds during a deploy"
    // and "we have been at the cap for a week" produce the same single line.
    const w = windowAt(1_000_000, 2)
    capturingWarn((lines) => {
        const atCap = () =>
            lines.filter((l) => l.includes('replay window is at its')).length

        for (let i = 0; i < 10; i++) w.seen.admit('a', `n${i}`, w.now)
        assertEquals(
            atCap(),
            1,
            'still one line for a burst at the same instant',
        )

        // Far enough that the warning may speak again. Everything admitted so
        // far is now outside the window, so the first two admits refill the
        // store and the third is the one that finds it at the cap.
        w.advance(60_000)
        w.seen.admit('a', 'later-1', w.now)
        w.seen.admit('a', 'later-2', w.now)
        w.seen.admit('a', 'later-3', w.now)
        assertEquals(atCap(), 2, 'the condition is still visible a minute on')
    })
})

Deno.test('#283: a pruned origin must leave the ledger, or it skews the floor', () => {
    // The narrow half of the ledger invariant, and the one a coarser test
    // misses: `#prune` deleting from `#seen` WITHOUT going through `#forget`.
    //
    // A dead origin's count then lingers, which inflates `#perOrigin.size`, and
    // the floor is `maxEntries / origins` — so the surviving origins are each
    // guaranteed FEWER entries than they are owed, and one of them is evicted
    // that should have been protected.
    //
    // The numbers are chosen so the two floors straddle the quiet origin:
    // with the ghost gone there are 2 origins and the floor is 3, so `quiet`
    // (3 entries) is protected and `busy` (4) is evicted from. With the ghost
    // lingering there are 3 origins, the floor is 2, and `quiet` — the oldest —
    // is evicted instead.
    const w = windowAt(1_000_000, 7)
    capturingWarn(() => {
        w.seen.admit('ghost', 'g1', w.now)
        w.seen.admit('ghost', 'g2', w.now)
        // Past the window: the ghost's entries are prunable, and its bucket
        // must go with them.
        w.advance(WINDOW + 1)

        for (let i = 0; i < 3; i++) w.seen.admit('quiet', `q${i}`, w.now)
        for (let i = 0; i < 4; i++) w.seen.admit('busy', `b${i}`, w.now)
        assertEquals(w.seen.size, 7, 'the store is exactly at its cap')

        // The admission that has to evict something.
        w.seen.admit('busy', 'b4', w.now)

        assertEquals(
            w.seen.admit('quiet', 'q0', w.now),
            'duplicate',
            'quiet sits AT its floor of 3 and must not be evicted. If prune ' +
                'left the ghost in the ledger the floor would be 2, quiet ' +
                'would be over it, and its oldest nonce would be gone',
        )
    })
})

Deno.test('#283: the shipped default cap is 10 000', () => {
    // Pinned because it is the number the docs quote to operators sizing a
    // fleet, and nothing else in the suite would notice it changing.
    const { seen, now } = windowAt()
    for (let i = 0; i < 10_000; i++) seen.admit('a', `n${i}`, now)
    assertEquals(seen.size, 10_000, 'ten thousand entries fit without eviction')
    capturingWarn(() => seen.admit('a', 'one-more', now))
    assertEquals(seen.size, 10_000, 'and the ten-thousand-and-first evicts')
})

Deno.test('#283: the at-cap warning does NOT re-arm before its interval', () => {
    // The other half of the re-arm test. Without this, shortening the interval
    // to nothing — restoring the per-admission spam on the hot path — goes
    // unnoticed.
    const w = windowAt(1_000_000, 2)
    capturingWarn((lines) => {
        const atCap = () =>
            lines.filter((l) => l.includes('replay window is at its')).length
        for (let i = 0; i < 5; i++) w.seen.admit('a', `n${i}`, w.now)
        assertEquals(atCap(), 1, 'one line for the first burst')

        // One millisecond short of the interval, and still inside the window so
        // the entries are not simply pruned away.
        w.advance(WINDOW - 1)
        w.seen.admit('a', 'later-1', w.now)
        w.seen.admit('a', 'later-2', w.now)
        assertEquals(atCap(), 1, 'still silent a fraction before the interval')
    })
})

Deno.test('#283: at a REALISTIC cap, sustained load from one origin starves nobody', () => {
    // Every other cap test here runs at `maxEntries <= 10`, which is small
    // enough that the arithmetic degenerates and a whole class of defect hides
    // below it. This one runs at the shipped default with a fleet-sized origin
    // count, and it is the test that would have caught two shipped rules:
    //
    //  - "evict from the heaviest origin" — the busy origin is always heaviest,
    //    so it is driven to the share while holding the most traffic.
    //  - a fixed floor with a bounded eviction scan — past the bound the
    //    fallback dropped the GLOBALLY oldest entry, which by construction
    //    belongs to an origin already at or under its share. Measured on this
    //    exact shape: 95 of the 99 quiet origins reached zero remembered
    //    nonces, with the survivor count pinned to the scan limit.
    //
    // It is deliberately not a micro-test. The defects it catches only exist at
    // a scale where `maxEntries / origins` stops being the trivially binding
    // term.
    const CAP = 10_000
    const QUIET = 99
    const EACH = 64
    const { seen, now } = windowAt(1_000_000, CAP)
    capturingWarn(() => {
        for (let o = 0; o < QUIET; o++) {
            for (let i = 0; i < EACH; i++) {
                seen.admit(`quiet-${o}`, `n${i}`, now)
            }
        }
        let filled = 0
        while (seen.size < CAP) seen.admit('busy', `b${filled++}`, now)
        assertEquals(seen.size, CAP, 'the store starts exactly full')

        // The reconnect storm the cap exists for.
        for (let i = 0; i < 20_000; i++) seen.admit('busy', `x${i}`, now)

        // Every quiet origin is at 64, under its share of 10 000/100 = 100, so
        // not one of them may have lost anything at all.
        let starved = 0
        let lost = 0
        for (let o = 0; o < QUIET; o++) {
            for (let i = 0; i < EACH; i++) {
                if (seen.admit(`quiet-${o}`, `n${i}`, now) !== 'duplicate') {
                    lost++
                }
            }
            // `admit` above re-inserts anything it found missing, so count the
            // origin as starved from `lost` rather than re-probing the store.
        }
        starved = lost
        assertEquals(
            starved,
            0,
            `every quiet origin sits under its ${
                CAP / (QUIET + 1)
            }-entry share ` +
                'and must keep all 64 nonces. Any loss here means one origin ' +
                'over its share evicted another that was under it',
        )
    })
})

Deno.test('#283: the victim is the OLDEST over-share origin, not the biggest', () => {
    // The probe that separates the shipped rule from the one withdrawn before
    // it. Both evict "an origin that is over its share"; they disagree only
    // when MORE THAN ONE origin is over share and the heaviest is not the
    // oldest. Every other cap test here has at most one origin over share at
    // the moment eviction fires, so the two rules name the same victim and the
    // suite cannot tell them apart — which is exactly how the withdrawn rule
    // passed a full gate.
    //
    // cap 12, three origins, share = 4:
    //   a = 5 entries, arrived FIRST   (over share, oldest head)
    //   b = 6 entries, arrived SECOND  (over share, biggest bucket)
    //   c = 1 entry                    (under share, untouchable)
    // Oldest-over-share evicts a's head. Biggest-bucket evicts b's head.
    const { seen, now } = windowAt(1_000_000, 12)
    capturingWarn(() => {
        for (let i = 0; i < 5; i++) seen.admit('a', `a${i}`, now)
        for (let i = 0; i < 6; i++) seen.admit('b', `b${i}`, now)
        seen.admit('c', 'c0', now)
        assertEquals(seen.size, 12, 'the store is exactly at its cap')

        seen.admit('c', 'c1', now) // forces one eviction

        assertEquals(
            seen.admit('b', 'b0', now),
            'duplicate',
            'b is the BIGGEST over-share origin but not the oldest, so it ' +
                'keeps its head. Evicting b here is the withdrawn rule',
        )
        assertEquals(
            seen.admit('a', 'a0', now),
            'ok',
            'a holds the oldest entry among origins over their share, so a0 ' +
                'is what goes',
        )
    })
})

Deno.test('#283: eviction orders by ARRIVAL, not by the frame’s own timestamp', () => {
    // `seq` exists for exactly this, and nothing else tested it. The freshness
    // gate admits any `ts` within ±windowMs, so timestamps are NOT monotonic
    // across origins — two instances with skewed clocks produce frames whose
    // `issuedAt` order is the reverse of the order they arrived in.
    //
    // Ordering by `issuedAt` would evict whichever origin's clock runs slowest,
    // which is a property of that instance's NTP state and nothing else. The
    // same shape as the test above, with the timestamps set so the two orders
    // disagree: `a` arrives first but stamps its frames in the FUTURE, `b`
    // arrives second and stamps them in the past.
    const { seen, now } = windowAt(1_000_000, 12)
    capturingWarn(() => {
        for (let i = 0; i < 5; i++) seen.admit('a', `a${i}`, now + 20_000)
        for (let i = 0; i < 6; i++) seen.admit('b', `b${i}`, now - 20_000)
        seen.admit('c', 'c0', now)
        assertEquals(seen.size, 12, 'all twelve were admitted, none stale')

        seen.admit('c', 'c1', now)

        assertEquals(
            seen.admit('b', 'b0', now - 20_000),
            'duplicate',
            'b‘s frames carry the EARLIEST issuedAt, so ordering by timestamp ' +
                'would evict b — b arrived second and must keep its head',
        )
        assertEquals(
            seen.admit('a', 'a0', now + 20_000),
            'ok',
            'a arrived first, so a0 is the oldest held entry regardless of ' +
                'the clock its publisher stamped it with',
        )
    })
})

Deno.test('#283: heads are COMPARED, not taken in origin-discovery order', () => {
    // The remaining way to get the victim wrong: iterate `#byOrigin` and take
    // the first origin found over its share. That agrees with comparing heads
    // in almost every arrangement, because origin-discovery order and head age
    // are normally the same order — the origin discovered first also holds the
    // oldest entry.
    //
    // They come apart only when an origin's early entries are pruned while its
    // bucket SURVIVES, which needs it to have published again before the old
    // ones went stale. (If the bucket empties it is deleted and re-created
    // behind the others, which quietly restores the correspondence — the first
    // attempt at this test did exactly that and the mutant lived.)
    //
    // It also kills a frozen `seq`: with every entry carrying the same number
    // no head compares as older, and the loop degenerates to first-found.
    const w = windowAt(1_000_000, 12)
    capturingWarn(() => {
        // `a` is discovered FIRST. These three go stale later.
        for (let i = 0; i < 3; i++) w.seen.admit('a', `old${i}`, w.now)

        // `b` second; its entries stay inside the window throughout.
        w.advance(10_000)
        for (let i = 0; i < 5; i++) w.seen.admit('b', `b${i}`, w.now)

        // `a` publishes again while its own old entries are still held, so its
        // bucket is never empty and never re-registered. This batch takes the
        // store to the cap; `a` is the only origin over its share, so it pays
        // for its own admissions and its three originals go.
        w.advance(10_000)
        for (let i = 0; i < 6; i++) w.seen.admit('a', `new${i}`, w.now)

        w.advance(15_000)
        w.seen.admit('c', 'c0', w.now)
        assertEquals(
            w.seen.size,
            12,
            '6 a + 5 b + 1 c — a is discovered first but now holds a NEWER ' +
                'head than b',
        )

        w.seen.admit('c', 'c1', w.now) // forces one eviction

        assertEquals(
            w.seen.admit('a', 'new0', w.now),
            'duplicate',
            'a is the first over-share origin in discovery order, but pruning ' +
                'advanced its head past b‘s — taking the first one found ' +
                'evicts a here, and that is the defect',
        )
        assertEquals(
            w.seen.admit('b', 'b0', w.now),
            'ok',
            'b holds the genuinely oldest entry among origins over their ' +
                'share, so b0 is what goes',
        )
    })
})

Deno.test('#283: when every origin is exactly at its share, the NEWCOMER pays', () => {
    // The one remaining path by which an origin under its share could lose an
    // entry to another's traffic. When the cap divides evenly and nobody is
    // over their share, there is no over-user to charge — and dropping the
    // globally-oldest entry would take it from a bystander sitting exactly at
    // its allowance.
    //
    // The origin asking for the slot is the one about to exceed its share, so
    // it pays. That is what makes "an origin at or under its share is never
    // evicted for someone else" true without a qualifier, and an unqualified
    // guarantee is the point: all three withdrawn rules failed by being
    // conditional in a way the docs did not say.
    const { seen, now } = windowAt(1_000_000, 6)
    capturingWarn(() => {
        for (const o of ['a', 'b', 'c']) {
            for (let i = 0; i < 2; i++) seen.admit(o, `n${i}`, now)
        }
        assertEquals(seen.size, 6, 'three origins, two each, exactly at cap')
        // share = 6/3 = 2; nobody is above it.
        seen.admit('c', 'n2', now)

        assertEquals(
            seen.admit('a', 'n0', now),
            'duplicate',
            'a was the globally oldest and is exactly at its share — it must ' +
                'not pay for c‘s admission',
        )
        assertEquals(
            seen.admit('c', 'n0', now),
            'ok',
            'c asked for the slot, so c‘s own oldest is what went',
        )
    })
})
