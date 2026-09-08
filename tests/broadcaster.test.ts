/**
 * @fileoverview Tests for the BroadcasterLike conformance + eviction — SC-005,
 * SC-005a.
 *
 * The manager satisfies `@lockness/notification`'s **real** `BroadcasterLike`
 * (imported here — a test-only edge, excluded from `deps:analyze`). Per-client
 * `send` reaches only the target; an evicted connection receives nothing.
 *
 * @module @lockness/realtime/tests/broadcaster
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { Connection } from '../types.ts'
// The real port — proving structural conformance, not a hand-mirror (A-M6).
import type { BroadcasterLike } from '@lockness/notification'

function fakeConn(id: string): Connection {
    const sent: string[] = []
    return {
        id,
        identity: null,
        metadata: {},
        send: (d) => void sent.push(d as string),
        close: () => {},
        get _sent() {
            return sent
        },
    } as Connection & { readonly _sent: string[] }
}
const sentOf = (c: Connection) => (c as unknown as { _sent: string[] })._sent

Deno.test("SC-005: the ChannelManager satisfies notification's BroadcasterLike", () => {
    const manager = new ChannelManager()
    // Structural assignment to the real interface — fails to compile on drift.
    const broadcaster: BroadcasterLike = manager
    assertEquals(typeof broadcaster.send, 'function')
})

Deno.test('SC-005: per-client send reaches only the target client', () => {
    const manager = new ChannelManager()
    const a = fakeConn('a')
    const b = fakeConn('b')
    manager.register(a)
    manager.register(b)

    const delivered = manager.send('a', 'notify', { x: 1 })
    assertEquals(delivered, true)
    assertEquals(sentOf(a).length, 1)
    assertEquals(sentOf(b).length, 0) // not the target

    assertEquals(manager.send('missing', 'e', {}), false) // no such client
})

Deno.test('SC-005a: an evicted (disconnected) connection receives nothing', () => {
    const manager = new ChannelManager()
    const a = fakeConn('a')
    manager.register(a)

    manager.disconnect('a')
    assertEquals(manager.send('a', 'e', {}), false)
    assertEquals(sentOf(a).length, 0)
})
