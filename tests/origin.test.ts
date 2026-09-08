/**
 * @fileoverview Tests for the CSWSH origin guard — SC-002.
 *
 * Exact origin-triple matching; absent, empty, literal `null`, and
 * substring-lookalike origins are all rejected (fail-closed).
 *
 * @module @lockness/realtime/tests/origin
 */

import { assertEquals } from '@std/assert'
import { checkOrigin, resolveAllowedOrigins } from '../websocket.ts'

const allow = resolveAllowedOrigins({
    appUrl: 'https://app.example.com/some/path',
    origins: ['https://admin.example.com'],
})

Deno.test('SC-002: an allow-listed exact origin passes', () => {
    assertEquals(checkOrigin('https://app.example.com', allow), true) // same-origin (path stripped)
    assertEquals(checkOrigin('https://admin.example.com', allow), true) // configured
})

Deno.test('SC-002: a not-listed origin is rejected', () => {
    assertEquals(checkOrigin('https://evil.example.com', allow), false)
})

Deno.test('SC-002: a substring-lookalike origin is rejected (no substring match)', () => {
    assertEquals(checkOrigin('https://app.example.com.evil.com', allow), false)
    assertEquals(checkOrigin('https://app.example.com:8443', allow), false) // different port
    assertEquals(checkOrigin('http://app.example.com', allow), false) // different scheme
})

Deno.test('SC-002: absent, empty, and literal null origins are rejected (fail-closed)', () => {
    assertEquals(checkOrigin(null, allow), false)
    assertEquals(checkOrigin('', allow), false)
    assertEquals(checkOrigin('null', allow), false)
})

Deno.test('with no appUrl and no origins, everything is rejected (default-closed)', () => {
    const empty = resolveAllowedOrigins({})
    assertEquals(checkOrigin('https://app.example.com', empty), false)
})
