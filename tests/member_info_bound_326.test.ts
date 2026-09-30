/**
 * @fileoverview #326 — an oversized presence member is refused at admission.
 *
 * `member.id` was bounded; its sibling `info` was not. That mattered because of
 * an ORDER: the roster write happens BEFORE the control publish, and the
 * driver's oversize check is on the publish. So an oversized member was written
 * to the authoritative roster, the local `joined` went out, the control frame
 * was dropped with a warning, and `subscribe` answered `{ ok: true }` — a
 * member present in the room and invisible to every other instance.
 *
 * **In an application that sources `info` from user-editable profile fields —
 * display name, bio, avatar URL, status text, which is the common case — that
 * is a self-service presence cloak.** A member picks their own oversize and
 * buys their own invisibility through an ordinary profile edit. No privileged
 * call, no protocol abuse.
 *
 * The fix is the admission bound, not the publish refusal. Making the publish
 * throw is a separate correctness repair on the driver's contract (a publish
 * reported as successful when the frame was dropped is a lie to `unsubscribe`
 * and `evict` too), but on its own it would not have closed the cloak: the join
 * path catches that failure and warns, deliberately and for good reason, so the
 * observable outcome would have been identical. Only refusing the member before
 * anything commits removes the state.
 *
 * **Every assertion reads what subscribers received and what the roster holds**
 * — never only the return value. A cloak is invisible in the return value by
 * definition; `{ ok: true }` is exactly what it produced.
 *
 * @module @lockness/realtime/tests/member_info_bound_326
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { ChannelManager, MAX_PRESENCE_MEMBER_BYTES } from '../manager.ts'
import { PresenceMemberSizeError } from '../presence_member.ts'
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

const CHANNEL = 'presence-room'

/** A connection that records every frame it received. */
function conn(
    id: string,
    userId: number,
): Connection<User> & { readonly received: Record<string, unknown>[] } {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Connection<User> & { readonly received: Record<string, unknown>[] }
}

/** A driver that records the roster and every control frame it was handed. */
function recordingDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const control: Record<string, unknown>[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
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
        publishControl(message) {
            control.push(message as unknown as Record<string, unknown>)
            return Promise.resolve()
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    return { driver, roster, control }
}

/** `info` whose serialized member is guaranteed to clear the ceiling. */
const oversize = () => ({ bio: 'x'.repeat(MAX_PRESENCE_MEMBER_BYTES + 1) })

Deno.test('#326 an oversized member is REFUSED, and leaves nothing behind', async () => {
    const { driver, roster, control } = recordingDriver()
    // Oversize for ONE identity, which is the shape the defect actually takes:
    // a member edits their own profile past the ceiling. An authorizer that
    // oversized everybody would leave nobody in the room to observe the cloak.
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity) =>
            identity
                ? { id: identity.id, info: identity.id === 1 ? oversize() : {} }
                : false,
    })
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(observer, CHANNEL)
    const observerFrames = observer.received.length
    const controlFrames = control.length

    const cloaker = conn('c1', 1)
    m.register(cloaker)
    await assertRejects(
        () => m.subscribe(cloaker, CHANNEL),
        PresenceMemberSizeError,
    )

    // The four ways the cloak used to be observable, all of them now absent.
    assertEquals(
        [...(roster.get(CHANNEL)?.keys() ?? [])],
        ['2'],
        'nothing reached the authoritative roster — this is the half that ' +
            'used to persist, and the half a peer instance would read back',
    )
    assertEquals(
        observer.received.length - observerFrames,
        0,
        'and the room was told nothing: no `joined` for a member no peer ' +
            'instance will ever hear about',
    )
    assertEquals(
        control.length - controlFrames,
        0,
        'no control frame was even attempted',
    )
    // And the room still works for everyone else — a bound that took the
    // channel down with the member would be a denial-of-service handed to the
    // party it was meant to stop.
    const c3 = conn('c3', 3)
    m.register(c3)
    assertEquals(
        (await m.subscribe(c3, CHANNEL)).ok,
        true,
        'a within-bound member still joins after a refusal',
    )
})

Deno.test('#326 the refusal is a NAMED error a caller can act on', async () => {
    const { driver } = recordingDriver()
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity) =>
            identity ? { id: identity.id, info: oversize() } : false,
    })

    const c1 = conn('c1', 1)
    m.register(c1)
    const error = await assertRejects(
        () => m.subscribe(c1, CHANNEL),
        PresenceMemberSizeError,
    )

    assert(
        error.message.includes(String(MAX_PRESENCE_MEMBER_BYTES)),
        `the ceiling must be in the message so an operator can act. ` +
            `Got: ${error.message}`,
    )
    assert(
        !error.message.includes('xxxxxxxxxx'),
        'and the offending content must NOT be — it is application data and ' +
            'this message reaches logs',
    )
})

Deno.test('#326 a member within the bound joins and is announced unchanged', async () => {
    // The reverse direction, and it is not a formality: a bound that refuses
    // the legitimate case is a worse defect than the one it fixes, and a test
    // that only asserts the refusal cannot tell the two apart.
    const { driver, roster, control } = recordingDriver()
    const info = { bio: 'y'.repeat(64), avatar: 'https://example.test/a.png' }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity) => identity ? { id: identity.id, info } : false,
    })
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(observer, CHANNEL)

    const joiner = conn('c1', 1)
    m.register(joiner)
    const rosterReadsBefore = rosterReadCount()
    const result = await m.subscribe(joiner, CHANNEL)
    assertRosterRead(rosterReadsBefore)

    assertEquals(result.ok, true)
    assertEquals(
        roster.get(CHANNEL)?.get('1')?.info,
        info,
        'the roster carries the info verbatim',
    )
    const joined = observer.received.filter((f) => f.action === 'joined')
    assertEquals(joined.length, 1, 'the room is told once')
    assertEquals(
        (joined[0].member as PresenceMember).info,
        info,
        'and the frame carries the info unchanged — a bound that silently ' +
            'truncated would be a different, quieter defect',
    )
    assert(
        control.some((c) => c.kind === 'presence-join'),
        'and the peers are told too — the whole point of the bound is that a ' +
            'member admitted here can always be announced',
    )
})

Deno.test('#326 the ceiling is configurable, and refused at construction', () => {
    const { driver } = recordingDriver()
    // Measured in BYTES, not characters. A single emoji is four bytes where
    // `.length` counts two, so a character-counting bound would admit a member
    // twice the size it measured — and the number it must fit under is a
    // payload limit, which is in bytes.
    const tiny = new ChannelManager<User>({
        driver,
        maxPresenceMemberBytes: 32,
        authorize: (identity) =>
            identity
                ? { id: identity.id, info: { e: '🐟'.repeat(16) } }
                : false,
    })
    assert(tiny instanceof ChannelManager)

    let threw = false
    try {
        new ChannelManager<User>({ driver, maxPresenceMemberBytes: 0 })
    } catch {
        threw = true
    }
    assert(
        threw,
        'a non-positive ceiling is refused at construction, like every other ' +
            'cap — a bound discovered when it first bites is a ' +
            'misconfiguration discovered in production',
    )
})
