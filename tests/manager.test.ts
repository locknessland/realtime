/**
 * @fileoverview Tests for the manager's teardown seam + publish-error handling.
 *
 * `handlerHooks` registers a connection on open and disconnects it on close
 * (framework-owned teardown, A-M4); a failed cross-process publish is routed to
 * `onPublishError`, never silently lost.
 *
 * @module @lockness/realtime/tests/manager
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection } from '../types.ts'

function fakeConn(id: string): Connection {
    return { id, identity: null, metadata: {}, send: () => {}, close: () => {} }
}

Deno.test('handlerHooks registers on open and disconnects on close (A-M4)', async () => {
    const m = new ChannelManager()
    let userOpen = false
    let userClose = false
    const hooks = m.handlerHooks({
        onOpen: () => void (userOpen = true),
        onClose: () => void (userClose = true),
    })
    const conn = fakeConn('c1')

    hooks.onOpen?.(conn)
    assertEquals(m.connectionCount, 1)
    assertEquals(userOpen, true)

    await hooks.onClose?.(conn, 1000, '')
    assertEquals(m.connectionCount, 0) // framework torn down
    assertEquals(userClose, true) // user hook still ran
})

Deno.test('a failed cross-process publish is routed to onPublishError, not lost', async () => {
    const errors: unknown[] = []
    const failing: BroadcastDriver = {
        publish: () => Promise.reject(new Error('redis down')),
        onMessage: () => {},
    }
    const m = new ChannelManager({
        driver: failing,
        onPublishError: (e) => void errors.push(e),
    })
    m.broadcast('news', 'e', {})
    await new Promise((r) => setTimeout(r, 0))
    assertEquals(errors.length, 1)
})
