/**
 * @fileoverview Tests for the WebSocket lifecycle mapping — SC-001.
 *
 * `buildEvents` maps a socket's lifecycle to the hooks over a `Connection`; a
 * throwing `onMessage` is routed to `onError`, not crashing the connection.
 * Driven with a fake socket + synthetic events — no real upgrade.
 *
 * @module @lockness/realtime/tests/websocket
 */

import { assert, assertEquals } from '@std/assert'
import { buildEvents, makeConnection } from '../websocket.ts'
import type { Connection, WSContext } from '../types.ts'
import type { WebSocketHooks } from '../types.ts'

/** A fake socket recording sends/closes; stands in for a Hono WSContext. */
function fakeSocket() {
    const sent: unknown[] = []
    let closed: { code?: number; reason?: string } | undefined
    return {
        sent,
        get closed() {
            return closed
        },
        send(data: unknown) {
            sent.push(data)
        },
        close(code?: number, reason?: string) {
            closed = { code, reason }
        },
    }
}

const tick = () => new Promise((r) => setTimeout(r, 0))

Deno.test('SC-001: onOpen/onMessage/onClose fire with a working Connection', async () => {
    const seen: string[] = []
    let received: unknown
    const hooks: WebSocketHooks = {
        onOpen: (c) => {
            seen.push('open')
            c.send('welcome')
        },
        onMessage: (_c, data) => {
            seen.push('message')
            received = data
        },
        onClose: (_c, code) => {
            seen.push(`close:${code}`)
        },
    }
    const ws = fakeSocket()
    const events = buildEvents(hooks, null)

    events.onOpen?.(new Event('open'), ws as unknown as WSContext)
    events.onMessage?.(
        { data: 'hi' } as MessageEvent,
        ws as unknown as WSContext,
    )
    events.onClose?.(
        { code: 1000, reason: 'bye' } as CloseEvent,
        ws as unknown as WSContext,
    )
    await tick()

    assertEquals(seen, ['open', 'message', 'close:1000'])
    assertEquals(ws.sent, ['welcome'])
    assertEquals(received, 'hi')
})

Deno.test('SC-001: a throwing onMessage is routed to onError, not crashing', async () => {
    let errored: unknown
    const hooks: WebSocketHooks = {
        onMessage: () => {
            throw new Error('handler boom')
        },
        onError: (_c, err) => {
            errored = err
        },
    }
    const ws = fakeSocket()
    const events = buildEvents(hooks, null)

    events.onMessage?.(
        { data: 'x' } as MessageEvent,
        ws as unknown as WSContext,
    )
    await tick()

    assert(errored instanceof Error)
    assertEquals((errored as Error).message, 'handler boom')
})

Deno.test('the same Connection instance is reused across events', () => {
    const seen: Connection[] = []
    const hooks: WebSocketHooks = {
        onOpen: (c) => void seen.push(c),
        onClose: (c) => void seen.push(c),
    }
    const ws = fakeSocket()
    const events = buildEvents(hooks, null)
    events.onOpen?.(new Event('open'), ws as unknown as WSContext)
    events.onClose?.(
        { code: 1001, reason: '' } as CloseEvent,
        ws as unknown as WSContext,
    )
    assert(seen[0] === seen[1])
})

Deno.test('onError forwards the real transport event as the error cause', async () => {
    let errored: unknown
    const events = buildEvents({ onError: (_c, e) => void (errored = e) }, null)
    const evt = new Event('error')
    events.onError?.(evt, fakeSocket() as unknown as WSContext)
    await tick()
    assert(errored instanceof Error)
    assertEquals((errored as Error).cause, evt)
})

Deno.test('makeConnection freezes metadata and delegates send/close', () => {
    const ws = fakeSocket()
    const conn = makeConnection(ws, 'id-1', null, { room: 'a' })
    conn.send('data')
    conn.close(1000, 'done')
    assertEquals(ws.sent, ['data'])
    assertEquals(ws.closed, { code: 1000, reason: 'done' })
    assertEquals(Object.isFrozen(conn.metadata), true)
})
