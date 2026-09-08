/**
 * @fileoverview Tests for the events → broadcast bridge — SC-007a (S2, A-M5).
 *
 * Only `broadcastWith()` leaves the server (never the whole event); a
 * non-Broadcastable event is not forwarded; a public-channel target warns; the
 * subscription registers with an abort signal.
 *
 * @module @lockness/realtime/tests/events_bridge
 */

import { assert, assertEquals } from '@std/assert'
import {
    type AnyEventPayload,
    type DispatcherLike,
    forwardEvent,
    startBroadcasting,
} from '../events_bridge.ts'

/** A manager stub recording broadcasts. */
function fakeManager() {
    const calls: Array<{ channel: string; event: string; data: unknown }> = []
    return {
        calls,
        broadcast(channel: string, event: string, data: unknown) {
            calls.push({ channel, event, data })
        },
    }
}

class InvoicePaid {
    // Rendered/private state that must NOT leave via the bridge:
    readonly cardNumber = '4242-4242-4242-4242'
    constructor(readonly invoiceId: number) {}
    broadcastOn() {
        return ['private-billing']
    }
    broadcastWith() {
        return { invoiceId: this.invoiceId }
    }
}

Deno.test('SC-007a: only broadcastWith() leaves; a private field never does (S2)', () => {
    const m = fakeManager()
    forwardEvent(
        m,
        { event: 'InvoicePaid', data: new InvoicePaid(7) },
        () => {},
    )
    assertEquals(m.calls.length, 1)
    assertEquals(m.calls[0], {
        channel: 'private-billing',
        event: 'InvoicePaid',
        data: { invoiceId: 7 },
    })
    assert(!JSON.stringify(m.calls[0]).includes('4242'))
})

Deno.test('a non-Broadcastable event is not forwarded', () => {
    const m = fakeManager()
    forwardEvent(m, { event: 'Plain', data: { x: 1 } }, () => {})
    assertEquals(m.calls.length, 0)
})

Deno.test('absent broadcastWith() sends a minimal projection, never the event', () => {
    const m = fakeManager()
    class Ping {
        readonly secret = 's'
        broadcastOn() {
            return ['news']
        }
    }
    const warns: string[] = []
    forwardEvent(m, { event: 'Ping', data: new Ping() }, (w) => warns.push(w))
    assertEquals(m.calls[0].data, {}) // minimal, not the Ping instance
    assert(!JSON.stringify(m.calls[0]).includes('secret'))
    // 'news' is public → a warning fired.
    assert(warns.some((w) => w.includes('public channel')))
})

Deno.test('startBroadcasting registers onAny with an abort signal', async () => {
    let registered: { signal?: AbortSignal } | undefined
    let listener: ((p: AnyEventPayload) => void) | undefined
    const dispatcher: DispatcherLike = {
        onAny: (l, opts) => {
            listener = l
            registered = opts
        },
    }
    const m = fakeManager()
    const controller = new AbortController()
    const started = await startBroadcasting(m, {
        dispatcher,
        signal: controller.signal,
        warn: () => {},
    })
    assertEquals(started, true)
    assert(registered?.signal === controller.signal)

    // The wired listener forwards a Broadcastable event.
    listener?.({ event: 'InvoicePaid', data: new InvoicePaid(9) })
    assertEquals(m.calls[0].data, { invoiceId: 9 })
})

Deno.test('startBroadcasting soft-loads @lockness/events and forwards through the real dispatcher', async () => {
    // @lockness/events is resolvable in the monorepo, so this exercises the real
    // soft-load path (loadDispatcher success), not the injected one.
    const events = await import('@lockness/events')
    const m = fakeManager()
    const controller = new AbortController()
    try {
        const started = await startBroadcasting(m, {
            signal: controller.signal,
            warn: () => {},
        })
        assertEquals(started, true)
        await events.dispatcher().emitString(
            'InvoicePaid',
            new InvoicePaid(5),
        )
        assertEquals(m.calls[0]?.data, { invoiceId: 5 })
    } finally {
        controller.abort() // detach the listener from the global dispatcher
    }
})
