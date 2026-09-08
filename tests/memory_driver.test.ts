/**
 * @fileoverview Tests for the memory broadcast driver — SC-004 (single-process).
 *
 * @module @lockness/realtime/tests/memory_driver
 */

import { assertEquals } from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { BroadcastMessage } from '../driver.ts'

Deno.test('memory driver loops a published message back to the local handler', () => {
    const driver = new MemoryBroadcastDriver()
    const got: BroadcastMessage[] = []
    driver.onMessage((m) => got.push(m))
    driver.publish({ channel: 'c', event: 'e', data: 1 })
    assertEquals(got, [{ channel: 'c', event: 'e', data: 1 }])
})

Deno.test('SC-004: the memory driver stays single-process (no shared state)', () => {
    const a = new MemoryBroadcastDriver()
    const b = new MemoryBroadcastDriver()
    const gotB: BroadcastMessage[] = []
    b.onMessage((m) => gotB.push(m))
    // A publishes; B is a separate instance and must not see it.
    a.publish({ channel: 'c', event: 'e', data: 1 })
    assertEquals(gotB.length, 0)
})
