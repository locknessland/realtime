/**
 * @fileoverview Tests for the thin RealtimeClient — FR-008.
 *
 * Send frames are encoded; inbound frames dispatch to event/frame handlers;
 * malformed inbound frames are ignored, never thrown.
 *
 * @module @lockness/realtime/tests/client
 */

import { assert, assertEquals } from '@std/assert'
import { RealtimeClient } from '../client.ts'

function makeClient() {
    const sent: string[] = []
    const client = new RealtimeClient((frame) => sent.push(frame))
    return { client, sent }
}

Deno.test('subscribe/unsubscribe/ping send encoded client frames', () => {
    const { client, sent } = makeClient()
    client.subscribe('private-room.1')
    client.unsubscribe('news')
    client.ping()
    assertEquals(JSON.parse(sent[0]), {
        type: 'subscribe',
        channel: 'private-room.1',
    })
    assertEquals(JSON.parse(sent[1]), { type: 'unsubscribe', channel: 'news' })
    assertEquals(JSON.parse(sent[2]), { type: 'ping' })
})

Deno.test('an inbound event frame dispatches to the matching event handler', () => {
    const { client } = makeClient()
    const received: Array<{ data: unknown; channel?: string }> = []
    client.on(
        'message.created',
        (data, channel) => received.push({ data, channel }),
    )

    client.receive(
        JSON.stringify({
            type: 'event',
            channel: 'private-room.1',
            event: 'message.created',
            data: { text: 'hi' },
        }),
    )
    assertEquals(received, [{
        data: { text: 'hi' },
        channel: 'private-room.1',
    }])
})

Deno.test('onMessage receives every frame; off removes an event handler', () => {
    const { client } = makeClient()
    const frames: string[] = []
    client.onMessage((m) => frames.push(m.type))
    const off = client.on('e', () => {})
    off()
    client.receive(JSON.stringify({ type: 'pong' }))
    assertEquals(frames, ['pong'])
})

Deno.test('connect wires the socket message listener to receive()', () => {
    const listeners: Record<string, (e: { data: string }) => void> = {}
    class FakeWS {
        sent: string[] = []
        constructor(readonly url: string) {}
        send(frame: string) {
            this.sent.push(frame)
        }
        addEventListener(type: string, cb: (e: { data: string }) => void) {
            listeners[type] = cb
        }
    }
    const client = RealtimeClient.connect(
        'wss://app.example/ws',
        FakeWS as unknown as typeof WebSocket,
    )
    const received: unknown[] = []
    client.on('e', (data) => received.push(data))
    listeners.message?.({
        data: JSON.stringify({ type: 'event', event: 'e', data: { n: 1 } }),
    })
    assertEquals(received, [{ n: 1 }])
})

Deno.test('a malformed inbound frame is ignored, never thrown', () => {
    const { client } = makeClient()
    let threw = false
    try {
        client.receive('{not json')
    } catch {
        threw = true
    }
    assert(!threw)
})
