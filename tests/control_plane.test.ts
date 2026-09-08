/**
 * @fileoverview Substrate tests for the authenticated control plane (#268 §5).
 *
 * The reserved control topic is a DISTINCT seam from channel events: a control
 * message published by one instance reaches every other instance's `onControl`
 * (never `onMessage`), carries an FR-015 authenticity MAC verified before it is
 * actioned, and never loops back to its own publisher. A frame with a
 * wrong-secret MAC — or when no secret is configured — is dropped, never
 * delivered. (The full US3 forgery matrix, SC-008, lives in `control_auth`.)
 *
 * @module @lockness/realtime/tests/control_plane
 */

import { assertEquals } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { BroadcastMessage, ControlMessage } from '../driver.ts'
import { FakeRedis } from './fake_redis.ts'

function driver(redis: FakeRedis, secret?: string) {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: 'app:rt', control: secret ? { secret } : undefined },
    )
}

Deno.test('a control message reaches onControl on other instances, never onMessage', async () => {
    const redis = new FakeRedis()
    const a = driver(redis, 'deployment-secret-with-enough-entropy')
    const b = driver(redis, 'deployment-secret-with-enough-entropy')
    try {
        const bControls: ControlMessage[] = []
        const bEvents: BroadcastMessage[] = []
        b.onControl((c) => bControls.push(c))
        b.onMessage((m) => bEvents.push(m))

        await a.publishControl({
            kind: 'presence-join',
            target: 'y',
            channel: 'presence-lobby',
            member: { id: 2, info: { name: 'Y' } },
        })

        assertEquals(bEvents.length, 0) // never routed through the event path
        assertEquals(bControls.length, 1)
        assertEquals(bControls[0].kind, 'presence-join')
        assertEquals(bControls[0].member?.id, 2)
        // The wire-only `mac`/`origin` never leak into the manager-facing shape.
        assertEquals(bControls[0].mac, undefined)
    } finally {
        await a.close()
        await b.close()
    }
})

Deno.test('a control publisher never receives its own control (self-loopback dropped)', async () => {
    const redis = new FakeRedis()
    const a = driver(redis, 'deployment-secret-with-enough-entropy')
    try {
        const aControls: ControlMessage[] = []
        a.onControl((c) => aControls.push(c))
        await a.publishControl({ kind: 'evict', target: 'victim' })
        assertEquals(aControls.length, 0)
    } finally {
        await a.close()
    }
})

Deno.test('FR-015: a control message signed with the wrong secret is dropped', async () => {
    const redis = new FakeRedis()
    const b = driver(redis, 'deployment-secret-with-enough-entropy')
    const forger = driver(redis, 'a-different-secret-with-enough-entropy!')
    try {
        const bControls: ControlMessage[] = []
        b.onControl((c) => bControls.push(c))
        await forger.publishControl({ kind: 'evict', target: 'victim' })
        assertEquals(bControls.length, 0) // failed MAC → never obeyed
    } finally {
        await b.close()
        await forger.close()
    }
})

Deno.test('FR-015: with no secret configured, publish is refused and ingest drops', async () => {
    const redis = new FakeRedis()
    const insecure = driver(redis) // no secret
    const signed = driver(redis, 'deployment-secret-with-enough-entropy')
    try {
        const gotOnInsecure: ControlMessage[] = []
        insecure.onControl((c) => gotOnInsecure.push(c))
        // A properly-signed message cannot be verified without a secret → dropped.
        await signed.publishControl({ kind: 'evict', target: 'victim' })
        assertEquals(gotOnInsecure.length, 0)

        // And an unconfigured driver refuses to emit an unauthenticated frame.
        const gotOnSigned: ControlMessage[] = []
        signed.onControl((c) => gotOnSigned.push(c))
        await insecure.publishControl({ kind: 'evict', target: 'victim' })
        assertEquals(gotOnSigned.length, 0)
    } finally {
        await insecure.close()
        await signed.close()
    }
})

Deno.test('FR-019: a control message with an out-of-charset target is dropped', async () => {
    const redis = new FakeRedis()
    const b = driver(redis, 'deployment-secret-with-enough-entropy')
    try {
        const bControls: ControlMessage[] = []
        b.onControl((c) => bControls.push(c))
        // Signed by a legit sender, but the target id is out of charset — the
        // ingest name check drops it even though the MAC verifies.
        await driver(redis, 'deployment-secret-with-enough-entropy')
            .publishControl({
                kind: 'evict',
                target: 'bad target!<x>',
            })
        assertEquals(bControls.length, 0)
    } finally {
        await b.close()
    }
})
