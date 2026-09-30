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

import { assert, assertEquals, assertThrows } from '@std/assert'
import { ChannelManager, presenceRoster } from '../manager.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ControlMessage,
} from '../driver.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { PresenceMember } from '../channel.ts'
import type { RosterHold, RosterRelease } from '../driver.ts'

/** A driver that exposes the full presence-state surface. */
class FullPresenceDriver implements BroadcastDriver {
    publish(_message: BroadcastMessage): void {}
    onMessage(_handler: (message: BroadcastMessage) => void): void {}
    onControl(_handler: (control: ControlMessage) => void): void {}
    holdMember(_channel: string, _member: PresenceMember): RosterHold {
        return { arrived: true }
    }
    releaseMember(_channel: string, _memberId: string | number): RosterRelease {
        return { gone: true }
    }
    readRoster(
        _channel: string,
        _limit: number,
        _selfIds: readonly (string | number)[],
    ) {
        return { members: [], total: 0, selves: [] }
    }
}

/** A driver missing one of the three ops — must NOT be treated as capable. */
class PartialPresenceDriver implements BroadcastDriver {
    publish(_message: BroadcastMessage): void {}
    onMessage(_handler: (message: BroadcastMessage) => void): void {}
    holdMember(_channel: string, _member: PresenceMember): RosterHold {
        return { arrived: true }
    }
    // releaseMember + readRoster intentionally absent.
}

Deno.test('presenceRoster narrows a fully presence-capable driver', async () => {
    const roster = presenceRoster(new FullPresenceDriver())
    assert(roster !== undefined)
    // The narrowed type exposes the ops without a per-call probe.
    assertEquals((await roster.readRoster('presence-lobby', 1, [])).members, [])
})

Deno.test('presenceRoster narrows the memory driver — it now owns an in-process roster', async () => {
    // US2 moved the roster onto the driver, including the memory driver (FR-005,
    // decision-table §5): it is now presence-capable, in-process.
    const roster = presenceRoster(new MemoryBroadcastDriver())
    assert(roster !== undefined)
    assertEquals((await roster.readRoster('presence-lobby', 1, [])).members, [])
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

// ─── #341: readRoster is the capability; listMembers is refused ──────────────

const window = { members: [], total: 0, selves: [] }

/** The three presence ops of 0.4.0, over a bare transport. */
const bounded = () => ({
    publish: () => {},
    onMessage: () => {},
    holdMember: () => ({ arrived: true }),
    releaseMember: () => ({ gone: true }),
    readRoster: () => window,
})

Deno.test('#341 presenceRoster is holdMember + releaseMember + readRoster', () => {
    const driver: BroadcastDriver = bounded()
    const roster = presenceRoster(driver)
    assert(roster !== undefined, 'a bounded-read driver owns the roster')
    assertEquals(roster.readRoster('presence-lobby', 1, []), window)
    // The probe reports what is there and does not editorialise: the old
    // trio is simply not the capability any more.
    const { readRoster: _dropped, ...rest } = bounded()
    assertEquals(
        presenceRoster({ ...rest, ...({ listMembers: () => [] } as object) }),
        undefined,
    )
})

Deno.test('#341 a driver still carrying listMembers is refused at construction, naming readRoster (US4)', () => {
    const { readRoster: _dropped, ...preBounded } = bounded()
    for (
        const legacy of [
            // A pre-0.4.0 driver: the unbounded read and nothing else.
            { ...preBounded, listMembers: () => [] },
            // A half-migrated one: keeping `listMembers` beside `readRoster`
            // keeps the unbounded read public, which is the defect.
            { ...bounded(), listMembers: () => [] },
        ]
    ) {
        const error = assertThrows(
            () => new ChannelManager({ driver: legacy as BroadcastDriver }),
            Error,
        )
        assert(
            error.message.includes('listMembers') &&
                error.message.includes('readRoster'),
            `the error must NAME the migration, not merely refuse it. Got: ${error.message}`,
        )
    }
})

Deno.test('#341 a driver with readRoster and no listMembers constructs', () => {
    const m = new ChannelManager({ driver: bounded() })
    assert(m instanceof ChannelManager)
    assert(
        new ChannelManager({ driver: new MemoryBroadcastDriver() }) instanceof
            ChannelManager,
        'the memory driver carries no legacy read',
    )
})
