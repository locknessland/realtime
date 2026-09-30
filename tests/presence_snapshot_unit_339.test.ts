/**
 * @fileoverview #339 — the cutting rule, as a pure function.
 *
 * `presence_snapshot_bound_339.test.ts` proves the manager applies the rule on
 * every exit and both roster sources. This pins the rule itself: fits
 * unchanged, cut to K, self replaces the last slot, input never mutated, and
 * `total` taken before the cut.
 *
 * @module @lockness/realtime/tests/presence_snapshot_unit_339
 */

import { assertEquals, assertStrictEquals } from '@std/assert'
import {
    boundPresenceSnapshot,
    localWindow,
    sameMemberId,
} from '../presence_snapshot.ts'
import type { PresenceMember } from '../channel.ts'

const roster = (...ids: (string | number)[]): PresenceMember[] =>
    ids.map((id) => ({ id }))
const idsOf = (members: PresenceMember[]) => members.map((m) => m.id)

Deno.test('#339 unit: a roster within the bound is returned unchanged', () => {
    const input = roster(1, 2, 3)
    const out = boundPresenceSnapshot(localWindow(input), 2, 3)
    assertStrictEquals(out.members, input, 'the same array, not a copy')
    assertEquals(out.total, 3)
})

Deno.test('#339 unit: an oversized roster is cut to the first K in driver order', () => {
    const out = boundPresenceSnapshot(
        localWindow(roster(5, 4, 3, 2, 1)),
        undefined,
        3,
    )
    assertEquals(idsOf(out.members), [5, 4, 3], 'driver order kept, no sort')
    assertEquals(out.total, 5, '`total` is the pre-cut roster size')
})

Deno.test('#339 unit: self outside the first K replaces the LAST slot', () => {
    const out = boundPresenceSnapshot(localWindow(roster(1, 2, 3, 4, 5)), 5, 3)
    assertEquals(idsOf(out.members), [1, 2, 5], 'still exactly K')
    assertEquals(out.total, 5)
})

Deno.test('#339 unit: self already inside the first K changes nothing', () => {
    const out = boundPresenceSnapshot(localWindow(roster(1, 2, 3, 4, 5)), 2, 3)
    assertEquals(idsOf(out.members), [1, 2, 3])
})

Deno.test('#339 unit: a self the roster does not hold is not invented', () => {
    const out = boundPresenceSnapshot(localWindow(roster(1, 2, 3, 4)), 9, 2)
    assertEquals(idsOf(out.members), [1, 2])
})

Deno.test('#339 unit: self is matched by String(id)', () => {
    const out = boundPresenceSnapshot(
        localWindow(roster('1', '2', '3', '42')),
        42,
        2,
    )
    assertEquals(idsOf(out.members), ['1', '42'])
    assertEquals(sameMemberId(42, '42'), true)
    assertEquals(sameMemberId('a', 'b'), false)
})

Deno.test('#339 unit: the input roster is never mutated', () => {
    const input = roster(1, 2, 3, 4, 5)
    const before = [...input]
    const out = boundPresenceSnapshot(localWindow(input), 5, 2)
    assertEquals(input, before, 'the shared read is left exactly as it came')
    assertEquals(idsOf(out.members), [1, 5])
})

// ─── #341: a window the driver already bounded ───────────────────────────────

const window = (
    members: PresenceMember[],
    total: number,
    selves: PresenceMember[] = [],
) => ({ members, total, selves })

Deno.test('#341 unit: localWindow is the whole list, its length, and no selves', () => {
    const input = roster(1, 2, 3)
    const out = localWindow(input)
    assertStrictEquals(out.members, input)
    assertEquals(out.total, 3)
    assertEquals(out.selves, [])
})

Deno.test('#341 unit: total is the window total, never the length of members', () => {
    const out = boundPresenceSnapshot(window(roster(1, 2, 3), 10_000), 2, 3)
    assertEquals(idsOf(out.members), [1, 2, 3])
    assertEquals(out.total, 10_000)
})

Deno.test('#341 unit: self from selves replaces the last slot of a full window', () => {
    const selves = [{ id: 9, info: { me: true } }]
    const input = roster(1, 2, 3)
    const out = boundPresenceSnapshot(window(input, 50, selves), 9, 3)
    assertEquals(out.members, [{ id: 1 }, { id: 2 }, selves[0]])
    assertEquals(idsOf(input), [1, 2, 3], 'the shared window is not mutated')
    assertEquals(out.total, 50)
})

Deno.test('#341 unit: self from selves is APPENDED to a short window, never overwriting a member', () => {
    // A window below the limit on a room above it: an unparseable entry was
    // skipped. Overwriting `members[limit - 1]` there would index past the
    // end; overwriting the last real member would drop somebody the roster
    // holds.
    const out = boundPresenceSnapshot(
        window(roster(1, 2), 5, [{ id: 9 }]),
        9,
        3,
    )
    assertEquals(idsOf(out.members), [1, 2, 9])
})

Deno.test('#341 unit: a window one short of total, completed by self from selves, reads as whole', () => {
    // `total === members.length + 1` and the missing entry is the caller's
    // own: appending it from `selves` makes `members.length === total`, so the
    // snapshot is not reported partial — and `total` is still never recounted.
    const out = boundPresenceSnapshot(
        window(roster(1, 2), 3, [{ id: 3 }]),
        3,
        5,
    )
    assertEquals(idsOf(out.members), [1, 2, 3])
    assertEquals(out.total, 3)
    assertEquals(out.members.length, out.total, 'a complete room is whole')
})

Deno.test('#341 unit: a self already in the window is not duplicated from selves', () => {
    const input = roster(1, 9, 3)
    const out = boundPresenceSnapshot(window(input, 50, [{ id: 9 }]), '9', 3)
    assertStrictEquals(out.members, input)
})

Deno.test('#341 unit: a self in neither members nor selves is not kept', () => {
    // The roster no longer holds it — it unsubscribed during the read, or a
    // sweep removed its entry — so the reply must not claim it (FR-006).
    const out = boundPresenceSnapshot(window(roster(1, 2, 3), 50, []), 9, 3)
    assertEquals(idsOf(out.members), [1, 2, 3])
})

Deno.test('#341 unit: selves are ignored when the caller holds no member', () => {
    const out = boundPresenceSnapshot(
        window(roster(1, 2, 3), 50, [{ id: 9 }]),
        undefined,
        3,
    )
    assertEquals(idsOf(out.members), [1, 2, 3])
})

Deno.test("#341 unit: another caller's self in a shared window is not taken as mine", () => {
    const out = boundPresenceSnapshot(
        window(roster(1, 2, 3), 50, [{ id: 8 }, { id: 9 }]),
        9,
        3,
    )
    assertEquals(idsOf(out.members), [1, 2, 9])
})
