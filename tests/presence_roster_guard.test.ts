/**
 * @fileoverview Unit tests for the `presenceRoster` feature-detect guard (A5).
 *
 * The guard is the ONE place that decides whether a driver owns the
 * authoritative roster: it narrows a presence-capable driver to
 * {@link PresenceCapableDriver} and returns `undefined` for a single-process
 * driver (the memory driver), so roster-aware methods never repeat the probe.
 *
 * @module @lockness/realtime/tests/presence_roster_guard
 */

import { assert, assertEquals } from '@std/assert'
import { presenceRoster } from '../manager.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ControlMessage,
} from '../driver.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { PresenceMember } from '../channel.ts'

/** A driver that exposes the full presence-state surface. */
class FullPresenceDriver implements BroadcastDriver {
    publish(_message: BroadcastMessage): void {}
    onMessage(_handler: (message: BroadcastMessage) => void): void {}
    onControl(_handler: (control: ControlMessage) => void): void {}
    addMember(_channel: string, _member: PresenceMember): void {}
    removeMember(_channel: string, _memberId: string | number): void {}
    listMembers(_channel: string): PresenceMember[] {
        return []
    }
}

/** A driver missing one of the three ops — must NOT be treated as capable. */
class PartialPresenceDriver implements BroadcastDriver {
    publish(_message: BroadcastMessage): void {}
    onMessage(_handler: (message: BroadcastMessage) => void): void {}
    addMember(_channel: string, _member: PresenceMember): void {}
    // removeMember + listMembers intentionally absent.
}

Deno.test('presenceRoster narrows a fully presence-capable driver', () => {
    const roster = presenceRoster(new FullPresenceDriver())
    assert(roster !== undefined)
    // The narrowed type exposes the ops without a per-call probe.
    assertEquals(roster.listMembers('presence-lobby'), [])
})

Deno.test('presenceRoster narrows the memory driver — it now owns an in-process roster', () => {
    // US2 moved the roster onto the driver, including the memory driver (FR-005,
    // decision-table §5): it is now presence-capable, in-process.
    const roster = presenceRoster(new MemoryBroadcastDriver())
    assert(roster !== undefined)
    assertEquals(roster.listMembers('presence-lobby'), [])
})

/** A driver with none of the optional ops — the single-process baseline. */
class BareDriver implements BroadcastDriver {
    publish(_message: BroadcastMessage): void {}
    onMessage(_handler: (message: BroadcastMessage) => void): void {}
}

Deno.test('presenceRoster returns undefined for a driver with no roster ops', () => {
    assertEquals(presenceRoster(new BareDriver()), undefined)
})

Deno.test('presenceRoster returns undefined when any op is missing (all-or-nothing)', () => {
    assertEquals(presenceRoster(new PartialPresenceDriver()), undefined)
})
