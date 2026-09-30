/**
 * @fileoverview The in-process broadcast driver — single-process fan-out.
 *
 * `publish` loops the message straight back to the registered `onMessage`
 * handler in the same process; there is no cross-process transport. This is the
 * default and the MVP presence transport (single-process authoritative).
 *
 * @module @lockness/realtime/drivers/memory
 */

import {
    type BroadcastDriver,
    type BroadcastMessage,
    MAX_ROSTER_READ_SELF_IDS,
    type RosterHold,
    type RosterRelease,
    type RosterWindow,
} from '../driver.ts'
import type { PresenceMember } from '../channel.ts'

/**
 * A single-process broadcast driver.
 *
 * It owns the authoritative presence roster **in-process** (FR-005 — the memory
 * home in the decision table): the same driver-owned roster contract the Redis
 * driver satisfies over Redis, so `ChannelManager` routes through the driver in
 * both cases and never keeps a second source of truth. The ops are synchronous
 * — a single process needs no round-trip — so presence behaviour is
 * observationally unchanged from the pre-roster MVP. There is deliberately no
 * `onControl` / `publishControl` seam: a single process has no cross-instance
 * control plane.
 *
 * @example
 * ```ts
 * const manager = new ChannelManager({ driver: new MemoryBroadcastDriver() })
 * ```
 */
export class MemoryBroadcastDriver implements BroadcastDriver {
    private handler?: (message: BroadcastMessage) => void
    private readonly roster = new Map<string, Map<string, PresenceMember>>()

    /**
     * Loop a message back to the local handler.
     *
     * @param message - The message to broadcast.
     */
    publish(message: BroadcastMessage): void {
        this.handler?.(message)
    }

    /**
     * Register the local delivery handler.
     *
     * @param handler - Called with each published message.
     */
    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.handler = handler
    }

    /**
     * Hold the channel's in-process roster slot (FR-005, #345).
     *
     * One process is the only possible holder, so the slot is filled iff it
     * was empty before this call; holding it again replaces the entry.
     *
     * Stores the manager's admitted member BY REFERENCE, and `readRoster`
     * hands that same object out — correct only because it is deep-frozen
     * where it was minted (#354), so no reader can change it for the others.
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member to hold the slot as.
     * @returns `arrived: true` iff the slot was empty before this hold.
     */
    holdMember(channel: string, member: PresenceMember): RosterHold {
        let members = this.roster.get(channel)
        if (!members) this.roster.set(channel, members = new Map())
        const key = String(member.id)
        const arrived = !members.has(key)
        members.set(key, member)
        return { arrived }
    }

    /**
     * Release the channel's in-process roster slot (FR-005, #345).
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member whose slot is released.
     * @returns `gone: true` iff the slot was held and is now empty.
     */
    releaseMember(channel: string, memberId: string | number): RosterRelease {
        const gone = this.roster.get(channel)?.delete(String(memberId)) ?? false
        return { gone }
    }

    /**
     * Read a bounded window of the channel's in-process roster (FR-005, #341).
     *
     * Walks at most `limit` entries in join order — the same order this driver
     * has always reported — and never copies the whole room. `total` is the
     * map's size and `selves` are direct lookups, all within one synchronous
     * call, so the three describe one instant. The arrays are fresh; the
     * members in them are the stored, deep-frozen objects (#354).
     *
     * @param channel - The presence channel.
     * @param limit - The most members to return; a positive integer.
     * @param selfIds - The member ids to return in `selves` when held; at most
     *   {@link MAX_ROSTER_READ_SELF_IDS}.
     * @returns The window, the population and the selves.
     * @throws {Error} If `limit` is not a positive integer or `selfIds` is
     *   longer than {@link MAX_ROSTER_READ_SELF_IDS}.
     *
     * @example
     * ```ts
     * const { members, total, selves } = driver.readRoster('presence-room', 100, [7])
     * ```
     */
    readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow {
        // Before any work: the seam is exported, so this driver cannot rely on
        // the manager having validated its own call (S2).
        if (!Number.isInteger(limit) || limit < 1) {
            throw new Error(
                `realtime: readRoster limit must be a positive integer, got ${limit}`,
            )
        }
        if (selfIds.length > MAX_ROSTER_READ_SELF_IDS) {
            throw new Error(
                `realtime: readRoster accepts at most ${MAX_ROSTER_READ_SELF_IDS} ` +
                    `self ids, got ${selfIds.length}`,
            )
        }
        const room = this.roster.get(channel)
        if (!room) return { members: [], total: 0, selves: [] }
        const members: PresenceMember[] = []
        for (const member of room.values()) {
            if (members.length === limit) break
            members.push(member)
        }
        // One self per `String(id)`, as `RosterWindow` promises and the Redis
        // driver does: `7` and `'7'` name one roster entry.
        const selves: PresenceMember[] = []
        const seen = new Set<string>()
        for (const selfId of selfIds) {
            const id = String(selfId)
            const self = room.get(id)
            if (!self || seen.has(id)) continue
            seen.add(id)
            selves.push(self)
        }
        return { members, total: room.size, selves }
    }
}
