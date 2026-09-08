/**
 * @fileoverview FR-013/SC-006 (#272) — every field the driver puts on a control
 * frame is actually covered by that frame's MAC.
 *
 * **This file exists because of a counted finding, not a convention.** Before
 * it, `grep hmacSha256Hex packages/realtime/tests/` returned nothing: all
 * seventeen control-plane tests published through a real driver and let it sign
 * for them, so not one could tell whether a given field was inside the MAC or
 * outside it. Verified by mutation — removing `ts` and `nonce` from
 * `#canonical`, shipping both completely unauthenticated, left the whole
 * realtime suite green at 114 passed.
 *
 * The property here is not "a forged frame is rejected" (`control_auth.test.ts`
 * owns that) but "**the driver's authenticated payload is exactly the frame it
 * published**". Those are different claims, and only the second catches a field
 * added to the wire and forgotten in the canonical form.
 *
 * The mechanism: publish through a real driver over a capturing command client,
 * then recompute the MAC **independently** from the captured wire. If the
 * driver covers a different field set than this file declares, the two MACs
 * disagree and the test fails — which is impossible to detect by asking the
 * driver to sign twice.
 *
 * @module @lockness/realtime/tests/control_mac_coverage
 */

import { assert, assertEquals } from '@std/assert'
import { hmacSha256Hex } from '../../redis/mod.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'

const SECRET = 'a-deployment-secret-with-more-than-enough-entropy'
const KEY = new TextEncoder().encode(SECRET)

/** The wire shape, as the driver serialises it. */
interface Wire {
    kind: string
    target: string
    channel?: string
    member?: unknown
    origin: string
    ts: number
    nonce: string
    mac: string
}

/**
 * The canonical form this test asserts the driver uses — transcribed from
 * `drivers/redis.ts` `#canonical`, deliberately as a second copy.
 *
 * Being a copy is the point. If the driver's covered set drifts from this one
 * in either direction, the recomputed MAC stops matching the published one.
 */
function canonical(wire: Wire): Uint8Array<ArrayBuffer> {
    return new TextEncoder().encode(JSON.stringify({
        kind: wire.kind,
        target: wire.target,
        channel: wire.channel,
        member: wire.member,
        origin: wire.origin,
        ts: wire.ts,
        nonce: wire.nonce,
    }))
}

/** Publish one control frame through a real driver and capture the wire. */
async function publishAndCapture(
    control: Parameters<RedisBroadcastDriver['publishControl']>[0],
): Promise<Wire> {
    const payloads: string[] = []
    const command = {
        command: (...args: string[]) => {
            if (args[0] === 'PUBLISH') payloads.push(args[2])
            return Promise.resolve({ type: 'integer' as const, value: 1 })
        },
    }
    const driver = new RedisBroadcastDriver(command, { psubscribe: () => {} }, {
        prefix: 'mac-coverage',
        control: { secret: SECRET },
    })
    await driver.publishControl(control)
    await driver.close()
    assertEquals(payloads.length, 1, 'exactly one control frame was published')
    return JSON.parse(payloads[0]) as Wire
}

Deno.test('FR-013: the driver signs EXACTLY the fields it publishes', async () => {
    const wire = await publishAndCapture({
        kind: 'presence-join',
        target: 'conn-1',
        channel: 'presence-lobby',
        member: { id: 7, info: { name: 'Ada' } },
    })

    assertEquals(
        hmacSha256Hex(KEY, canonical(wire)),
        wire.mac,
        'the MAC recomputed from the published wire does not match the one ' +
            'the driver attached. Either #canonical covers a field this test ' +
            'does not, or — the dangerous direction — it OMITS a field that is ' +
            'on the wire, which ships that field unauthenticated.',
    )
})

Deno.test('FR-013: an evict frame (no channel, no member) is covered too', async () => {
    // `JSON.stringify` omits undefined, so the two frame shapes canonicalise
    // differently. A covered-set bug could hide in whichever one is untested.
    const wire = await publishAndCapture({ kind: 'evict', target: 'conn-9' })
    assertEquals(hmacSha256Hex(KEY, canonical(wire)), wire.mac)
})

Deno.test('FR-013/SC-006: mutating ANY covered field changes the MAC', async () => {
    const wire = await publishAndCapture({
        kind: 'presence-join',
        target: 'conn-1',
        channel: 'presence-lobby',
        member: { id: 7, info: { name: 'Ada' } },
    })
    const baseline = hmacSha256Hex(KEY, canonical(wire))

    const mutations: ReadonlyArray<[string, Partial<Wire>]> = [
        ['kind', { kind: 'evict' }],
        ['target', { target: 'conn-2' }],
        ['channel', { channel: 'presence-other' }],
        ['member', { member: { id: 8, info: { name: 'Grace' } } }],
        ['origin', { origin: 'some-other-instance' }],
        ['ts', { ts: wire.ts + 1 }],
        ['nonce', { nonce: 'b'.repeat(32) }],
    ]

    for (const [field, patch] of mutations) {
        assert(
            hmacSha256Hex(KEY, canonical({ ...wire, ...patch })) !== baseline,
            `changing '${field}' did not change the MAC, so an attacker can ` +
                'edit it freely on a captured frame and the MAC still verifies.',
        )
    }

    // Guards the list itself: a field added to the wire without a mutation case
    // would leave this file quietly proving less than it claims.
    assertEquals(
        mutations.map(([field]) => field).sort(),
        Object.keys(wire).filter((k) => k !== 'mac').sort(),
        'every non-mac field on the wire has a mutation case',
    )
})

Deno.test('FR-013: the negative control — identical input, identical MAC', () => {
    // Without this, a `hmacSha256Hex` that returned a fresh random value every
    // call would satisfy the mutation test above for entirely the wrong reason.
    const wire: Wire = {
        kind: 'evict',
        target: 't',
        origin: 'o',
        ts: 1,
        nonce: 'n',
        mac: '',
    }
    assertEquals(
        hmacSha256Hex(KEY, canonical(wire)),
        hmacSha256Hex(KEY, canonical(wire)),
    )
})
