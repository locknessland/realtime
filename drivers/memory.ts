/**
 * @fileoverview The in-process broadcast driver — single-process fan-out.
 *
 * `publish` loops the message straight back to the registered `onMessage`
 * handler in the same process; there is no cross-process transport. This is the
 * default and the MVP presence transport (single-process authoritative).
 *
 * @module @lockness/realtime/drivers/memory
 */

import type { BroadcastDriver, BroadcastMessage } from '../driver.ts'
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
     * Add a member to the channel's in-process roster (FR-005).
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member to add.
     */
    addMember(channel: string, member: PresenceMember): void {
        let members = this.roster.get(channel)
        if (!members) this.roster.set(channel, members = new Map())
        members.set(String(member.id), member)
    }

    /**
     * Remove a member from the channel's in-process roster (FR-005).
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member to remove.
     */
    removeMember(channel: string, memberId: string | number): void {
        this.roster.get(channel)?.delete(String(memberId))
    }

    /**
     * List the channel's in-process roster (FR-005).
     *
     * @param channel - The presence channel.
     * @returns The current members ("here").
     */
    listMembers(channel: string): PresenceMember[] {
        return [...(this.roster.get(channel)?.values() ?? [])]
    }
}
