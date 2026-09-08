/**
 * @fileoverview Tests for the wire protocol codec — SC-007 + SC-007b (S3).
 *
 * Round-trip; malformed/oversized/unknown frames and out-of-charset/oversized
 * names all raise a typed ProtocolError (the caller sends an error frame, never
 * crashes).
 *
 * @module @lockness/realtime/tests/protocol
 */

import { assertEquals, assertThrows } from '@std/assert'
import {
    decodeClientMessage,
    encodeServerMessage,
    isValidName,
    MAX_NAME_LENGTH,
    ProtocolError,
} from '../protocol.ts'

Deno.test('SC-007: subscribe/unsubscribe/ping round-trip', () => {
    assertEquals(
        decodeClientMessage('{"type":"subscribe","channel":"private-room.1"}'),
        { type: 'subscribe', channel: 'private-room.1' },
    )
    assertEquals(
        decodeClientMessage('{"type":"unsubscribe","channel":"news"}'),
        { type: 'unsubscribe', channel: 'news' },
    )
    assertEquals(decodeClientMessage('{"type":"ping"}'), { type: 'ping' })
})

Deno.test('SC-007: a server frame encodes to JSON', () => {
    const raw = encodeServerMessage({
        type: 'event',
        channel: 'news',
        event: 'headline',
        data: { t: 1 },
    })
    assertEquals(JSON.parse(raw).event, 'headline')
})

Deno.test('SC-007: malformed JSON, unknown type, and non-object frames throw ProtocolError', () => {
    assertThrows(() => decodeClientMessage('{not json'), ProtocolError)
    assertThrows(() => decodeClientMessage('123'), ProtocolError)
    assertThrows(
        () => decodeClientMessage('{"type":"nope"}'),
        ProtocolError,
    )
})

Deno.test('SC-007: an oversized frame throws ProtocolError', () => {
    const huge = JSON.stringify({ type: 'subscribe', channel: 'a'.repeat(50) })
    assertThrows(() => decodeClientMessage(huge, 10), ProtocolError, 'maximum')
})

Deno.test('SC-007b: an out-of-charset or oversized channel name is rejected (S3)', () => {
    assertThrows(
        () =>
            decodeClientMessage(
                '{"type":"subscribe","channel":"<img onerror=x>"}',
            ),
        ProtocolError,
        'invalid channel name',
    )
    const long = 'x'.repeat(MAX_NAME_LENGTH + 1)
    assertThrows(
        () =>
            decodeClientMessage(
                `{"type":"subscribe","channel":"${long}"}`,
                1_000_000,
            ),
        ProtocolError,
        'invalid channel name',
    )
})

Deno.test('isValidName accepts the allow-list charset only', () => {
    assertEquals(isValidName('presence-room.1:v2_x-y'), true)
    assertEquals(isValidName('bad name'), false)
    assertEquals(isValidName('bad/slash'), false)
    assertEquals(isValidName(''), false)
})
