/**
 * @fileoverview Tests for server-derived connection identity — SC-002a (S1).
 *
 * A connection's identity is the one resolved at the upgrade; a wire frame
 * carrying a forged id never becomes the identity the app sees.
 *
 * @module @lockness/realtime/tests/identity
 */

import { assert, assertEquals } from '@std/assert'
import { buildEvents } from '../websocket.ts'
import type { Connection, WSContext } from '../types.ts'
import type { WebSocketHooks } from '../types.ts'

function fakeSocket() {
    return { send() {}, close() {} }
}
const tick = () => new Promise((r) => setTimeout(r, 0))

interface User {
    id: number
}

Deno.test('SC-002a: Connection.identity is the upgrade-resolved identity', () => {
    let conn: Connection<User> | undefined
    const hooks: WebSocketHooks<User> = { onOpen: (c) => void (conn = c) }
    const events = buildEvents<User>(hooks, { id: 42 })
    events.onOpen?.(new Event('open'), fakeSocket() as unknown as WSContext)
    assertEquals(conn?.identity, { id: 42 })
})

Deno.test('SC-002a: a message carrying a forged user id does not change the identity', async () => {
    let seen: User | null = null
    const hooks: WebSocketHooks<User> = {
        onMessage: (c) => {
            seen = c.identity
        },
    }
    const events = buildEvents<User>(hooks, { id: 7 })
    // The frame claims to be user 999 — it must be ignored as an identity source.
    events.onMessage?.(
        { data: JSON.stringify({ userId: 999 }) } as MessageEvent,
        fakeSocket() as unknown as WSContext,
    )
    await tick()
    assertEquals(seen, { id: 7 })
})

Deno.test('an unauthenticated upgrade yields identity null', () => {
    let conn: Connection | undefined
    const hooks: WebSocketHooks = { onOpen: (c) => void (conn = c) }
    const events = buildEvents(hooks, null)
    events.onOpen?.(new Event('open'), fakeSocket() as unknown as WSContext)
    assert(conn?.identity === null)
})
