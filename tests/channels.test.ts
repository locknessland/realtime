/**
 * @fileoverview Tests for channel authorization + fan-out — SC-003.
 *
 * Public delivers to all; a private channel rejects an unauthorized subscribe
 * and delivers no event to it; an authorized one delivers. Proven with fake
 * connections + a fake authorizer over the memory driver.
 *
 * @module @lockness/realtime/tests/channels
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { channelKind } from '../channel.ts'
import type { Connection } from '../types.ts'

Deno.test('channelKind derives the kind from the name prefix', () => {
    assertEquals(channelKind('presence-room.1'), 'presence')
    assertEquals(channelKind('private-orders'), 'private')
    assertEquals(channelKind('news'), 'public')
})

interface User {
    id: number
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    const sent: string[] = []
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void sent.push(d as string),
        close: () => {},
        // expose the log for assertions
        get _sent() {
            return sent
        },
    } as Connection<User> & { readonly _sent: string[] }
}
const sentOf = (c: Connection<User>) =>
    (c as unknown as { _sent: string[] })._sent

Deno.test('SC-003: a public channel delivers to all subscribers', async () => {
    const m = new ChannelManager<User>()
    const a = fakeConn('a', null)
    const b = fakeConn('b', null)
    await m.subscribe(a, 'news')
    await m.subscribe(b, 'news')

    m.broadcast('news', 'headline', { t: 'hi' })

    assertEquals(sentOf(a).length, 1)
    assertEquals(sentOf(b).length, 1)
    assertEquals(JSON.parse(sentOf(a)[0]).event, 'headline')
})

Deno.test('SC-003: a private channel rejects an unauthorized subscribe and delivers nothing to it', async () => {
    const m = new ChannelManager<User>({
        authorize: (id) => id?.id === 1, // only user 1 may join
    })
    const allowed = fakeConn('allowed', { id: 1 })
    const denied = fakeConn('denied', { id: 2 })

    const okA = await m.subscribe(allowed, 'private-orders')
    const okD = await m.subscribe(denied, 'private-orders')
    assertEquals(okA.ok, true)
    assertEquals(okD.ok, false)

    m.broadcast('private-orders', 'created', { id: 99 })

    assertEquals(sentOf(allowed).length, 1) // authorized receives
    assertEquals(sentOf(denied).length, 0) // unauthorized receives nothing
})

Deno.test('SC-003: a private channel with a null identity is denied (S1)', async () => {
    const m = new ChannelManager<User>({ authorize: () => true })
    const anon = fakeConn('anon', null)
    const result = await m.subscribe(anon, 'private-x')
    assertEquals(result.ok, false)
    m.broadcast('private-x', 'e', {})
    assertEquals(sentOf(anon).length, 0)
})

Deno.test('a public channel needs no authorizer', async () => {
    const m = new ChannelManager<User>() // no authorize
    const a = fakeConn('a', null)
    const result = await m.subscribe(a, 'lobby')
    assert(result.ok)
})
