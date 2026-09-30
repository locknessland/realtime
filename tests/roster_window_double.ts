/**
 * @fileoverview A test helper that turns a double's whole-room roster into the
 * {@link RosterWindow} `readRoster` returns (#341).
 *
 * The inline driver doubles across this suite model a roster as a list; this
 * is the one place that list becomes a window, so each double keeps its own
 * timing (synchronous, awaited, gated, rejecting) and none re-implements the
 * window rule. A synchronous list stays synchronous and a rejection passes
 * through untouched. A test helper — never imported by production code.
 *
 * @module @lockness/realtime/tests/roster_window_double
 */

import { assert } from '@std/assert'
import type { PresenceMember } from '../channel.ts'
import type { RosterWindow } from '../driver.ts'

/**
 * Every `asWindow` call in this process. A double whose `readRoster` is
 * misspelt or dropped is silently ROSTER-LESS, and the manager then serves the
 * local path with no error at all — so the suite would keep passing while
 * testing something else (#341 A8). Deno runs one test at a time per process,
 * so a delta taken around one subscribe belongs to that subscribe.
 */
let reads = 0

/**
 * How many roster windows this helper has built so far.
 *
 * @returns The running count; take it before a subscribe, compare after.
 */
export function rosterReadCount(): number {
    return reads
}

/**
 * Assert a double was asked for its roster since `before` — the proof that it
 * is roster-capable rather than silently falling back to the local window.
 *
 * @param before - {@link rosterReadCount} taken before the presence subscribe.
 * @throws AssertionError when no read happened.
 * @example
 * ```ts
 * const before = rosterReadCount()
 * await manager.subscribe(connection, 'presence-room')
 * assertRosterRead(before)
 * ```
 */
export function assertRosterRead(before: number): void {
    assert(
        reads > before,
        'the double is roster-capable: the presence subscribe read its roster ' +
            'through readRoster, not the local fallback (#341 A8)',
    )
}

/** The window over a whole-room list: first `limit`, its length, the selves. */
function windowOf(
    all: readonly PresenceMember[],
    limit: number,
    selfIds: readonly (string | number)[],
): RosterWindow {
    const wanted = new Set(selfIds.map(String))
    return {
        members: all.slice(0, limit),
        total: all.length,
        selves: all.filter((m) => wanted.has(String(m.id))),
    }
}

/**
 * Wrap a double's roster answer as a window, keeping its sync-or-async shape.
 *
 * @param roster - The whole room, or a promise of it.
 * @param limit - The `readRoster` limit.
 * @param selfIds - The `readRoster` self ids.
 * @returns The window, synchronously when `roster` was a list.
 * @example
 * ```ts
 * readRoster: (channel, limit, selfIds) =>
 *     asWindow([...store.values()], limit, selfIds)
 * ```
 */
export function asWindow(
    roster: readonly PresenceMember[] | Promise<readonly PresenceMember[]>,
    limit: number,
    selfIds: readonly (string | number)[],
): RosterWindow | Promise<RosterWindow> {
    reads++
    return Array.isArray(roster)
        ? windowOf(roster, limit, selfIds)
        : Promise.resolve(roster).then((all) => windowOf(all, limit, selfIds))
}
