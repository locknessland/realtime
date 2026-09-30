/**
 * @fileoverview #312 — the roster write and its control frame, made observable.
 *
 * `subscribe` writes the authoritative roster and then publishes a
 * `presence-join` control frame, and until now **nothing could see that
 * order**. #306's battery records the consequence honestly: moving the member-id
 * assertion AFTER `holdMember` survives, because the suite that proves the
 * refusal uses no roster and cannot observe a write that has already happened.
 * The ordering was held by a comment at the call site.
 *
 * The double here records every roster op and every control publish into one
 * ordered log, which is the whole of what was missing.
 *
 * @module @lockness/realtime/tests/roster_control_atomicity
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { PresenceMemberIdError } from '../presence_member.ts'
import { MAX_NAME_LENGTH } from '../protocol.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'

interface User {
    id: number
}

/**
 * A connection whose frames land in the SAME ordered log as the driver ops.
 *
 * `send: () => {}` was the gap that made this file's log unable to witness the
 * one ordering #323 turns on. It recorded driver ops only, so `holdMember` vs
 * `publishControl` was observable and `emitPresence` vs `holdMember` was not —
 * a clean-join assertion passed identically before and after the announcement
 * moved. A home whose witness cannot see it is a comment.
 */
const connLogging = (log: string[], id: string): Connection<User> => ({
    id,
    identity: { id: 1 },
    metadata: {},
    send: (data) => {
        const frame = JSON.parse(data as string)
        log.push(
            `frame ${frame.action ?? frame.type} ${frame.channel} -> ${id}`,
        )
    },
    close: () => {},
})

/** A driver that records roster ops and control publishes in ONE ordered log. */
function recordingDriver(options: { refuseControl?: boolean } = {}) {
    const log: string[] = []
    const roster = new Map<string, Map<string, PresenceMember>>()
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            log.push(`holdMember ${channel} ${member.id}`)
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return { arrived }
        },
        releaseMember(channel, memberId) {
            log.push(`releaseMember ${channel} ${memberId}`)
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl(control) {
            // The driver's oversize branch, modelled at the seam the manager
            // actually sees: `RedisBroadcastDriver.publishControl` warns and
            // RETURNS on a payload over the ceiling, so from here a refusal and
            // a success are the same `Promise<void>`. That indistinguishability
            // is the defect, not the log line.
            log.push(
                options.refuseControl
                    ? `publishControl REFUSED ${control.kind}`
                    : `publishControl ${control.kind}`,
            )
        },
    }
    return { driver, log, roster }
}

Deno.test('#312 the member-id assertion runs BEFORE the roster write', async () => {
    // The row #306 could only record as an equivalent mutant. Its guard still
    // throws when moved after `holdMember`, so every refusal assertion in that
    // suite still passes — what changes is that the oversized field is already
    // in the authoritative roster when it does. An empty log is the assertion
    // that suite had no way to make.
    const { driver, log } = recordingDriver()
    const oversized = 'x'.repeat(MAX_NAME_LENGTH + 1)
    const m = new ChannelManager<User>({
        driver,
        authorize: () => ({ id: oversized }),
    })
    const c = connLogging(log, crypto.randomUUID())
    m.register(c)
    let threw: unknown
    try {
        await m.subscribe(c, 'presence-room')
    } catch (error) {
        threw = error
    }
    assert(
        threw instanceof PresenceMemberIdError,
        `the oversized id was accepted (got ${threw})`,
    )
    assertEquals(
        log,
        [],
        'the refusal must happen before ANY driver write — a guard that ' +
            'throws after `holdMember` leaves the oversized field in the ' +
            'authoritative roster and is a partial write, not a refusal',
    )
})

Deno.test('#312 a clean join writes the roster, THEN publishes the join frame', async () => {
    // The order itself, pinned. It is not arbitrary: the frame announces a
    // member that peers may immediately read back from the roster, so the
    // roster has to be authoritative by the time the frame lands.
    const { driver, log } = recordingDriver()
    const m = new ChannelManager<User>({
        driver,
        authorize: () => ({ id: 'ada' }),
    })
    const c = connLogging(log, crypto.randomUUID())
    m.register(c)
    const rosterReadsBefore = rosterReadCount()
    const result = await m.subscribe(c, 'presence-room')
    assertRosterRead(rosterReadsBefore)
    assertEquals(result.ok, true)
    assertEquals(log, [
        'holdMember presence-room ada',
        'publishControl presence-join',
    ])
})

Deno.test('#312 a REFUSED control frame loses the ANNOUNCEMENT, not the roster', async () => {
    // The state today, pinned rather than assumed — and it is narrower than
    // "present on one instance, absent from everywhere else".
    //
    // `publishControl` warns and returns when the payload is over the ceiling
    // (`PresenceMember.info` is unbounded, so this is reachable). The manager
    // cannot tell that apart from a success: the port returns
    // `void | Promise<void>` either way.
    //
    // What is actually lost is the live `joined` push to peers ALREADY in the
    // channel. The authoritative roster is written and correct, so any instance
    // that READS it — which `rosterSnapshot` does on every subscribe — sees the
    // member. Rolling the roster write back would not restore atomicity; it
    // would trade a lost notification for a genuine state divergence, with the
    // member locally subscribed and absent from the authoritative store.
    const { driver, log, roster } = recordingDriver({ refuseControl: true })
    const m = new ChannelManager<User>({
        driver,
        authorize: () => ({ id: 'ada' }),
    })
    const c = connLogging(log, crypto.randomUUID())
    m.register(c)
    const result = await m.subscribe(c, 'presence-room')

    assertEquals(result.ok, true, 'the join succeeds locally')
    assertEquals(log, [
        'holdMember presence-room ada',
        'publishControl REFUSED presence-join',
    ])
    assertEquals(
        [...(roster.get('presence-room')?.keys() ?? [])],
        ['ada'],
        'the AUTHORITATIVE roster holds the member',
    )
    assertEquals(
        result.here?.members.map((member) => member.id),
        ['ada'],
        'and a reader of that roster gets it back — the snapshot returned to ' +
            'the joiner comes from the driver, not from local state',
    )
})

Deno.test('#312 cleanup after a refused join is total — the leave still removes it', async () => {
    // The bound on the damage. A member whose join frame was refused is not
    // stranded: `unsubscribe` removes it from the authoritative roster on the
    // ordinary path, so the divergence lasts the connection's lifetime and no
    // longer. This is what makes the lost frame a NOTIFICATION gap rather than
    // a leak — and it is asserted here because the argument for leaving the
    // roster write in place depends on it.
    const { driver, log, roster } = recordingDriver({ refuseControl: true })
    const m = new ChannelManager<User>({
        driver,
        authorize: () => ({ id: 'ada' }),
    })
    const c = connLogging(log, crypto.randomUUID())
    m.register(c)
    await m.subscribe(c, 'presence-room')
    await m.disconnect(c.id)
    assertEquals(
        [...(roster.get('presence-room')?.keys() ?? [])],
        [],
        'the member is gone from the authoritative roster after disconnect',
    )
})

Deno.test('#323 the ONE log shows write, then frame, then announcement', async () => {
    // This is the assertion the file could not make before its connection
    // double learned to log. It is the whole of FR-001: the frame that tells a
    // subscriber somebody joined comes AFTER the write that makes it true.
    const { driver, log } = recordingDriver()
    let nextId = 'ada'
    const m = new ChannelManager<User>({
        driver,
        authorize: () => ({ id: nextId }),
    })
    const watcher = connLogging(log, 'watcher')
    m.register(watcher)
    await m.subscribe(watcher, 'presence-room')

    log.length = 0
    const newcomer = connLogging(log, 'newcomer')
    m.register(newcomer)
    await m.subscribe(newcomer, 'presence-room')

    // INTENTIONALLY INVERTED by #344 (plan §8, A1): the newcomer used to be a
    // second connection of `ada` whose join announced `joined` to the watcher
    // and published `presence-join`. `ada` was already present, so that frame
    // told the room a member already listed had joined. A second tab now only
    // re-holds the slot, and announces nothing.
    assertEquals(
        log,
        ['holdMember presence-room ada'],
        'a second connection of a present member writes its hold and ' +
            'announces nothing',
    )

    // FR-001's order, on a member that DOES arrive: the write, then the local
    // frame, then the announcement — and no connection of `bea` hears it.
    log.length = 0
    nextId = 'bea'
    const bea = connLogging(log, 'bea')
    m.register(bea)
    await m.subscribe(bea, 'presence-room')

    assertEquals(
        log,
        [
            'holdMember presence-room bea',
            'frame joined presence-room -> watcher',
            'frame joined presence-room -> newcomer',
            'publishControl presence-join',
        ],
        'the authoritative write leads; the frame follows it; and the ' +
            'arriving member is absent from its own announcement',
    )
})
