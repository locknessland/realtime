/**
 * @fileoverview Tests for presence channels — SC-006.
 *
 * The `here` roster + join/leave reach authorized members only; a leave fires
 * on unsubscribe and on disconnect (single-process authoritative presence).
 *
 * @module @lockness/realtime/tests/presence
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
    name: string
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    const frames: Array<Record<string, unknown>> = []
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void frames.push(JSON.parse(d as string)),
        close: () => {},
        get _frames() {
            return frames
        },
    } as Connection<User> & { readonly _frames: Record<string, unknown>[] }
}
const framesOf = (c: Connection<User>) =>
    (c as unknown as { _frames: Record<string, unknown>[] })._frames

// Presence authorizer returns each connection's public member payload.
const authorize = (id: User | null): PresenceMember | false =>
    id ? { id: id.id, info: { name: id.name } } : false

Deno.test('SC-006: subscribing returns the here-roster; a join notifies existing members', async () => {
    const m = new ChannelManager<User>({ authorize })
    const a = fakeConn('a', { id: 1, name: 'Alice' })
    const b = fakeConn('b', { id: 2, name: 'Bob' })

    const ra = await m.subscribe(a, 'presence-room')
    assertEquals(ra.ok, true)
    assertEquals(ra.members?.map((x) => x.id), [1]) // here = [Alice]

    const rb = await m.subscribe(b, 'presence-room')
    assertEquals(rb.members?.map((x) => x.id).sort(), [1, 2]) // here = [Alice, Bob]

    // Alice was notified of Bob's join.
    const aJoin = framesOf(a).find((f) => f.action === 'joined')
    assertEquals((aJoin?.member as PresenceMember)?.id, 2)
})

Deno.test('SC-006: a leave (unsubscribe) notifies remaining members', async () => {
    const m = new ChannelManager<User>({ authorize })
    const a = fakeConn('a', { id: 1, name: 'Alice' })
    const b = fakeConn('b', { id: 2, name: 'Bob' })
    await m.subscribe(a, 'presence-room')
    await m.subscribe(b, 'presence-room')

    await m.unsubscribe('b', 'presence-room')
    const aLeft = framesOf(a).find((f) => f.action === 'left')
    assertEquals((aLeft?.member as PresenceMember)?.id, 2)
})

Deno.test('SC-006: a disconnect emits a presence leave (unclean disconnect cleanup)', async () => {
    const m = new ChannelManager<User>({ authorize })
    const a = fakeConn('a', { id: 1, name: 'Alice' })
    const b = fakeConn('b', { id: 2, name: 'Bob' })
    await m.subscribe(a, 'presence-room')
    await m.subscribe(b, 'presence-room')

    await m.disconnect('b')
    const aLeft = framesOf(a).find((f) => f.action === 'left')
    assertEquals((aLeft?.member as PresenceMember)?.id, 2)
})

Deno.test('SC-006: a broadcast to a presence channel reaches its members', async () => {
    const m = new ChannelManager<User>({ authorize })
    const a = fakeConn('a', { id: 1, name: 'Alice' })
    await m.subscribe(a, 'presence-room')
    m.broadcast('presence-room', 'msg', { text: 'hi' })
    const evt = framesOf(a).find((f) => f.type === 'event')
    assertEquals(evt?.event, 'msg')
})
