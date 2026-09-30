/**
 * @fileoverview Contract tests for the widened `BroadcastDriver` seam (A2/A5).
 *
 * A driver MAY expose the OPTIONAL presence-state ops
 * (`holdMember`/`releaseMember`/`readRoster`) and the distinct
 * `onControl(handler)` seam. A driver WITHOUT them — the memory driver, or any
 * third-party driver — still satisfies {@link BroadcastDriver} and keeps its
 * single-process behaviour, feature-detected rather than mandated.
 *
 * @module @lockness/realtime/tests/driver_contract
 */

import { assert, assertEquals } from '@std/assert'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ControlMessage,
} from '../driver.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { PresenceMember } from '../channel.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'
import type { RosterHold, RosterRelease, RosterWindow } from '../driver.ts'

/**
 * A tiny presence-capable fake driver: it implements the optional roster ops
 * and the `onControl` seam, without any transport. Exists only to prove the
 * widened contract compiles and is feature-detectable.
 */
class PresenceCapableFakeDriver implements BroadcastDriver {
    private messageHandler?: (message: BroadcastMessage) => void
    private controlHandler?: (control: ControlMessage) => void
    private readonly roster = new Map<string, Map<string, PresenceMember>>()

    publish(message: BroadcastMessage): void {
        this.messageHandler?.(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.messageHandler = handler
    }

    onControl(handler: (control: ControlMessage) => void): void {
        this.controlHandler = handler
    }

    /** Test seam: push a control message through the registered handler. */
    emitControl(control: ControlMessage): void {
        this.controlHandler?.(control)
    }

    holdMember(channel: string, member: PresenceMember): RosterHold {
        let members = this.roster.get(channel)
        if (!members) this.roster.set(channel, members = new Map())
        const arrived = !members.has(String(member.id))
        members.set(String(member.id), member)
        return { arrived }
    }

    releaseMember(channel: string, memberId: string | number): RosterRelease {
        return {
            gone: this.roster.get(channel)?.delete(String(memberId)) ?? false,
        }
    }

    readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow {
        return asWindow(
            (() => {
                return [...(this.roster.get(channel)?.values() ?? [])]
            })(),
            limit,
            selfIds,
        ) as RosterWindow
    }
}

Deno.test('a presence-capable driver exposes the optional roster ops', () => {
    const driver = new PresenceCapableFakeDriver()
    const rosterReadsBefore = rosterReadCount()
    assertEquals(driver.readRoster('presence-lobby', 1_000, []).members, [])
    assertRosterRead(rosterReadsBefore)
    driver.holdMember('presence-lobby', { id: 'u1' })
    driver.holdMember('presence-lobby', { id: 'u2', info: { name: 'Ada' } })
    assertEquals(
        driver.readRoster('presence-lobby', 1_000, []).members.length,
        2,
    )
    driver.releaseMember('presence-lobby', 'u1')
    assertEquals(driver.readRoster('presence-lobby', 1_000, []).members, [{
        id: 'u2',
        info: { name: 'Ada' },
    }])
})

Deno.test('the optional ops are feature-detectable on a presence-capable driver', () => {
    const driver: BroadcastDriver = new PresenceCapableFakeDriver()
    assert(typeof driver.holdMember === 'function')
    assert(typeof driver.releaseMember === 'function')
    assert(typeof driver.readRoster === 'function')
    assert(typeof driver.onControl === 'function')
})

/** A minimal driver with none of the optional ops — a third-party transport. */
class BareDriver implements BroadcastDriver {
    private handler?: (message: BroadcastMessage) => void
    publish(message: BroadcastMessage): void {
        this.handler?.(message)
    }
    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.handler = handler
    }
}

Deno.test('a driver WITHOUT the optional ops still satisfies BroadcastDriver', () => {
    const driver: BroadcastDriver = new BareDriver()
    // A driver that never grew the optional ops keeps single-process behaviour.
    assertEquals(driver.holdMember, undefined)
    assertEquals(driver.releaseMember, undefined)
    assertEquals(driver.readRoster, undefined)
    assertEquals(driver.onControl, undefined)
    assertEquals(driver.publishControl, undefined)
    // …and its core publish/onMessage loopback is untouched.
    const got: BroadcastMessage[] = []
    driver.onMessage((m) => got.push(m))
    driver.publish({ channel: 'c', event: 'e', data: 1 })
    assertEquals(got, [{ channel: 'c', event: 'e', data: 1 }])
})

Deno.test('the memory driver now owns an in-process roster (FR-005), no control plane', () => {
    const driver: BroadcastDriver = new MemoryBroadcastDriver()
    // It grew the presence-state ops (the in-process roster home)…
    assertEquals(typeof driver.holdMember, 'function')
    assertEquals(typeof driver.releaseMember, 'function')
    assertEquals(typeof driver.readRoster, 'function')
    // …but a single process has no cross-instance control plane.
    assertEquals(driver.onControl, undefined)
    assertEquals(driver.publishControl, undefined)
})

Deno.test('the onControl seam is distinct from onMessage (A2): a control message never reaches onMessage', () => {
    const driver = new PresenceCapableFakeDriver()
    const events: BroadcastMessage[] = []
    const controls: ControlMessage[] = []
    driver.onMessage((m) => events.push(m))
    driver.onControl((c) => controls.push(c))

    const control: ControlMessage = { kind: 'evict', target: 'client-7' }
    driver.emitControl(control)

    assertEquals(events.length, 0) // never routed through the event path
    assertEquals(controls, [control])
})

Deno.test('a ControlMessage carries the optional FR-015 authenticity tag', () => {
    const signed: ControlMessage = {
        kind: 'presence-leave',
        target: 'client-9',
        mac: 'deadbeef',
    }
    assertEquals(signed.mac, 'deadbeef')
    // The tag is optional on the type — an unauthenticated frame is representable
    // (and, per FR-015, is what the US3 ingest check rejects).
    const unsigned: ControlMessage = { kind: 'evict', target: 'client-9' }
    assertEquals(unsigned.mac, undefined)
})
