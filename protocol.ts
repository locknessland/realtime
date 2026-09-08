/**
 * @fileoverview The JSON wire protocol — envelope types, name validation, and
 * a fail-safe codec.
 *
 * Decoding is defensive: an oversized frame, malformed JSON, an unknown type,
 * or an out-of-charset / oversized channel name raises a {@link ProtocolError}
 * the caller turns into an `error` frame — never a thrown connection crash
 * (FR-006/FR-006a/FR-009, security S3).
 *
 * @module @lockness/realtime/protocol
 */

import type { PresenceMember } from './channel.ts'

/** Client → server frames. */
export type ClientMessage =
    | { type: 'subscribe'; channel: string }
    | { type: 'unsubscribe'; channel: string }
    | { type: 'ping' }

/** Server → client frames. */
export type ServerMessage =
    | { type: 'subscribed'; channel: string; members?: PresenceMember[] }
    | { type: 'unsubscribed'; channel: string }
    | { type: 'event'; channel?: string; event: string; data: unknown }
    | {
        type: 'presence'
        channel: string
        action: 'here' | 'joined' | 'left'
        members?: PresenceMember[]
        member?: PresenceMember
    }
    | { type: 'error'; message: string }
    | { type: 'pong' }

/** Raised for any invalid inbound frame. The caller sends an `error`, not a crash. */
export class ProtocolError extends Error {
    constructor(message: string) {
        super(message)
        this.name = 'ProtocolError'
    }
}

/** Max channel/event name length. */
export const MAX_NAME_LENGTH = 200
/** Default max inbound frame size in bytes. */
export const MAX_FRAME_BYTES = 16 * 1024
const NAME_RE = /^[A-Za-z0-9:._-]+$/

/**
 * Whether a channel/event name is within the allow-list charset and length.
 *
 * @param name - The name to validate.
 * @returns `true` when the name is safe to route and relay.
 */
export function isValidName(name: string): boolean {
    return typeof name === 'string' &&
        name.length > 0 &&
        name.length <= MAX_NAME_LENGTH &&
        NAME_RE.test(name)
}

/**
 * Encode a server frame for the wire.
 *
 * @param message - The frame to send.
 * @returns The JSON string.
 */
export function encodeServerMessage(message: ServerMessage): string {
    return JSON.stringify(message)
}

/**
 * Decode and validate an inbound client frame.
 *
 * @param raw - The raw frame (string or bytes).
 * @param maxBytes - The max accepted frame size (default {@link MAX_FRAME_BYTES}).
 * @returns The parsed, validated client message.
 * @throws {ProtocolError} On oversize, malformed JSON, unknown type, or an
 *   invalid channel name.
 */
export function decodeClientMessage(
    raw: string | ArrayBufferLike | Uint8Array,
    maxBytes: number = MAX_FRAME_BYTES,
): ClientMessage {
    const text = typeof raw === 'string'
        ? raw
        : new TextDecoder().decode(raw as ArrayBuffer)
    // Measure real UTF-8 bytes, not UTF-16 code units — a multi-byte frame must
    // not slip past the byte budget.
    if (new TextEncoder().encode(text).byteLength > maxBytes) {
        throw new ProtocolError('frame exceeds the maximum size')
    }

    let parsed: unknown
    try {
        parsed = JSON.parse(text)
    } catch {
        throw new ProtocolError('malformed JSON frame')
    }
    if (typeof parsed !== 'object' || parsed === null) {
        throw new ProtocolError('frame is not an object')
    }

    const msg = parsed as Record<string, unknown>
    switch (msg.type) {
        case 'subscribe':
        case 'unsubscribe': {
            if (typeof msg.channel !== 'string' || !isValidName(msg.channel)) {
                throw new ProtocolError('invalid channel name')
            }
            return { type: msg.type, channel: msg.channel }
        }
        case 'ping':
            return { type: 'ping' }
        default:
            throw new ProtocolError(`unknown frame type: ${String(msg.type)}`)
    }
}
