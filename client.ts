/**
 * @fileoverview The optional, dependency-free client helper — a thin
 * `RealtimeClient` over a WebSocket (FR-008).
 *
 * A **leaf** module: it imports only the protocol types (no server code). The
 * transport is injected as a `send` function, so the core (subscribe / receive
 * / dispatch) is testable without a live socket; `connect()` is the browser
 * glue. Server-relayed names/payloads are attacker-influenced — the client
 * hands them to your handler verbatim; **your handler must output-encode them**
 * (FR-006a).
 *
 * @module @lockness/realtime/client
 */

import type { ClientMessage, ServerMessage } from './protocol.ts'

/** A handler for every inbound server frame. */
export type ServerFrameHandler = (message: ServerMessage) => void
/** A handler for a named event on a channel. */
export type EventHandler = (data: unknown, channel?: string) => void

/**
 * A minimal real-time client: send subscribe/unsubscribe/ping frames and
 * dispatch inbound frames to handlers.
 *
 * @example
 * ```ts
 * const client = RealtimeClient.connect('wss://app.example/ws')
 * client.on('message.created', (data) => render(data)) // render escapes!
 * client.subscribe('private-room.1')
 * ```
 */
export class RealtimeClient {
    private readonly frameHandlers = new Set<ServerFrameHandler>()
    private readonly eventHandlers = new Map<string, Set<EventHandler>>()

    /**
     * @param send - Sends an encoded frame over the transport (e.g.
     *   `(f) => ws.send(f)`).
     */
    constructor(private readonly send: (frame: string) => void) {}

    /**
     * Open a WebSocket and wire it to a new client.
     *
     * @param url - The WebSocket URL.
     * @param WebSocketCtor - The `WebSocket` constructor (defaults to the global).
     * @returns The connected client.
     */
    static connect(
        url: string,
        WebSocketCtor: typeof WebSocket = WebSocket,
    ): RealtimeClient {
        const ws = new WebSocketCtor(url)
        const client = new RealtimeClient((frame) => ws.send(frame))
        ws.addEventListener('message', (e: MessageEvent) => {
            client.receive(typeof e.data === 'string' ? e.data : '')
        })
        return client
    }

    /** Subscribe to a channel. */
    subscribe(channel: string): void {
        this.sendMessage({ type: 'subscribe', channel })
    }

    /** Unsubscribe from a channel. */
    unsubscribe(channel: string): void {
        this.sendMessage({ type: 'unsubscribe', channel })
    }

    /** Send a keepalive ping. */
    ping(): void {
        this.sendMessage({ type: 'ping' })
    }

    /**
     * Register a handler for a named event.
     *
     * @param event - The event name.
     * @param handler - Called with the event's data (untrusted — encode it).
     * @returns An unsubscribe function.
     */
    on(event: string, handler: EventHandler): () => void {
        let set = this.eventHandlers.get(event)
        if (!set) this.eventHandlers.set(event, set = new Set())
        set.add(handler)
        return () => this.eventHandlers.get(event)?.delete(handler)
    }

    /**
     * Register a handler for every inbound server frame.
     *
     * @param handler - Called with each decoded frame.
     */
    onMessage(handler: ServerFrameHandler): void {
        this.frameHandlers.add(handler)
    }

    /**
     * Feed a raw inbound frame (call from your socket's message listener).
     * Malformed JSON is ignored, never thrown.
     *
     * @param raw - The raw frame text.
     */
    receive(raw: string): void {
        let message: ServerMessage
        try {
            message = JSON.parse(raw) as ServerMessage
        } catch {
            return
        }
        for (const handler of this.frameHandlers) handler(message)
        if (message.type === 'event') {
            const set = this.eventHandlers.get(message.event)
            if (set) {
                for (const handler of set) {
                    handler(message.data, message.channel)
                }
            }
        }
    }

    /** Encode and send a client frame. */
    private sendMessage(message: ClientMessage): void {
        this.send(JSON.stringify(message))
    }
}
