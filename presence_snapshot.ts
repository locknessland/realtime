/**
 * @fileoverview How a presence roster is cut to the snapshot one `subscribe`
 * returns (#339), and how this instance's local members are made one entry per
 * member first (#343) — internal, and the single home of both rules.
 *
 * The two rules stay separate on purpose. `uniqueMembers` runs only on the
 * LOCAL view, which is keyed by connection and can repeat a member;
 * `boundPresenceSnapshot` runs on every roster and never deduplicates — the
 * authoritative roster is already one entry per member, and a pass there would
 * cost O(room) on every read and hide a driver that returned duplicates.
 *
 * Kept out of `manager.ts` so the rule is a pure function a unit test can pin
 * and a mutation battery can break, and kept out of `mod.ts` because nothing
 * outside the manager has a roster to cut. This module imports nothing from
 * `manager.ts`: the edge runs `manager.ts → presence_snapshot.ts` only, which is
 * also why the default bound is passed in rather than imported.
 *
 * @module @lockness/realtime/presence_snapshot
 */

import type { PresenceMember, PresenceSnapshot } from './channel.ts'
import type { RosterWindow } from './driver.ts'

/**
 * Whether two presence member ids name the same member.
 *
 * Ids compare as `String(id)`: the roster is a hash keyed by the string form,
 * so `1` and `'1'` are one slot there and must be one member here.
 *
 * @param a - One member id.
 * @param b - The other.
 * @returns `true` when both ids have the same string form.
 */
export function sameMemberId(a: string | number, b: string | number): boolean {
    return String(a) === String(b)
}

/**
 * One entry per member, from a list that may hold a member more than once.
 *
 * The local `presence` map is keyed by CONNECTION id, so one member holding two
 * tabs on this instance is two values in it; the authoritative roster is keyed
 * by `String(member.id)`, so it is one slot. Every read of the map's values
 * goes through this rule (via the manager's `#localRoster`), so the local view
 * and the roster count the same thing (#343).
 *
 * - **Keyed by `String(id)`**, the key {@link sameMemberId} and the roster hash
 *   use: `1` and `'1'` are one member.
 * - **First occurrence wins, order kept.** The map never re-inserts on a
 *   re-join (#327), so first is the earliest-joined connection still
 *   subscribed — the one whose `info` the roster slot holds (#330).
 * - **One `Map` pass, no sort, no logging, input not mutated.**
 *
 * @param members - Members in insertion order, possibly repeating an id.
 * @returns A new array with one member per `String(id)`.
 *
 * @example
 * ```ts
 * uniqueMembers([
 *     { id: 7, info: { tab: 'a' } },
 *     { id: 8 },
 *     { id: '7', info: { tab: 'b' } },
 * ])
 * // [{ id: 7, info: { tab: 'a' } }, { id: 8 }]
 * ```
 */
export function uniqueMembers(
    members: Iterable<PresenceMember>,
): PresenceMember[] {
    const byId = new Map<string, PresenceMember>()
    for (const member of members) {
        const key = String(member.id)
        if (!byId.has(key)) byId.set(key, member)
    }
    return [...byId.values()]
}

/**
 * The window a roster-less driver or the local fallback reports: the whole
 * local list, its length as `total`, and no separate selves (#341).
 *
 * The single home of that shape, so the fallback and the roster-less path
 * cannot build `{ members, total, selves }` two different ways. Local members
 * are already this instance's own, so the caller's member — when this instance
 * holds it — is in `members` and needs no second list.
 *
 * @param members - This instance's members of the channel, one per member.
 * @returns A window over exactly those members. The array is not copied.
 *
 * @example
 * ```ts
 * localWindow([{ id: 1 }, { id: 2 }])
 * // { members: [{ id: 1 }, { id: 2 }], total: 2, selves: [] }
 * ```
 */
export function localWindow(members: PresenceMember[]): RosterWindow {
    return { members, total: members.length, selves: [] }
}

/**
 * Cut a roster window to at most `limit` members, keeping the caller's own.
 *
 * - **Fits, self present or not held:** the window's `members` are returned
 *   unchanged, same array, same order.
 * - **Larger than `limit`** (only a local window can be): the first `limit` in
 *   driver order.
 * - **Self** is kept iff the roster holds it: found in the window's `members`
 *   first, else in its `selves` (the driver's same-instant lookup, #341). When
 *   it is not already among the cut members it **replaces the last slot if the
 *   cut is exactly `limit`**, and is **appended otherwise** — a short window
 *   (an unparseable entry was skipped) has room, and overwriting there would
 *   drop a member the roster holds.
 * - `total` is the window's `total`, counted by the driver inside the same
 *   read. Never `members.length`, which on a bounded read is at most `limit`.
 *
 * Rules this function carries, each of which has been proposed and each of
 * which is wrong:
 *
 * - **No sort, no shuffle.** Which members fill the window is the driver's
 *   decision (#341); this function only cuts and keeps self.
 * - **No mutation.** The window may be shared by every caller of one read; the
 *   manager's `rosterSnapshot` copy is what gives each caller its own array,
 *   and this function returns its input when nothing changes.
 * - **Silent.** Cutting is the designed reply, not a fault: no log, no meter,
 *   no error. Nothing here can reach a logger.
 * - **Not in the barrier or a driver.** Cutting a shared read would hand one
 *   caller's self to another; the cut happens per caller, after the read.
 * - **Self is decided by `selfId` looked up AFTER the read.** A self id taken
 *   before the await only widens what the driver fetches; it never decides
 *   what is kept here.
 *
 * @param window - The window a source reported. Not mutated.
 * @param selfId - The subscribing connection's member id, or `undefined` when
 *   it holds none (a superseded join).
 * @param limit - The bound, a positive integer validated by the manager.
 * @returns The members to return and the roster's `total`.
 *
 * @example
 * ```ts
 * boundPresenceSnapshot(
 *     { members: [{ id: 1 }, { id: 2 }], total: 9, selves: [{ id: 7 }] },
 *     7,
 *     2,
 * )
 * // { members: [{ id: 1 }, { id: 7 }], total: 9 }
 * ```
 */
export function boundPresenceSnapshot(
    window: RosterWindow,
    selfId: string | number | undefined,
    limit: number,
): Omit<PresenceSnapshot, 'source'> {
    const { total } = window
    const members = window.members.length <= limit
        ? window.members
        : window.members.slice(0, limit)
    if (
        selfId === undefined ||
        members.some((member) => sameMemberId(member.id, selfId))
    ) {
        return { members, total }
    }
    const isSelf = (member: PresenceMember) => sameMemberId(member.id, selfId)
    const self = window.members.find(isSelf) ?? window.selves.find(isSelf)
    if (!self) return { members, total }
    const kept = members === window.members ? [...members] : members
    if (kept.length === limit) kept[limit - 1] = self
    else kept.push(self)
    return { members: kept, total }
}
