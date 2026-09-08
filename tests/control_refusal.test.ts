/**
 * @fileoverview #318 — a refused control frame is observable, not just logged.
 *
 * `publishControl` warns and RETURNS when it declines a frame, so from the
 * manager's side a refusal and a success are the same `void | Promise<void>`:
 * `subscribe` answers `{ ok: true }`, the authoritative roster is correct, and
 * peers already in the channel hold a stale roster until they resubscribe. The
 * only signal was a WARN on the one instance that refused — invisible to
 * anything aggregating across instances.
 *
 * These tests pin the seam and, as much as the signal itself, the two things
 * that make it worth having: it distinguishes the two refusals, which have
 * different fixes, and it never fires on a clean join.
 *
 * What it deliberately does NOT do is change what happens. #312 settled that a
 * refusal leaves the roster write standing; this is reporting, not policy.
 *
 * @module @lockness/realtime/tests/control_refusal
 */

import { assert, assertEquals } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { ControlRefusal } from '../driver.ts'
import { FakeRedis } from './fake_redis.ts'

const SECRET = 'a'.repeat(64)

/** A driver over the in-process bus, with the refusal seam recording. */
function driverWith(
    options: { secret?: string; maxPayloadBytes?: number } = {},
) {
    const redis = new FakeRedis()
    const seen: ControlRefusal[] = []
    const secret = 'secret' in options ? options.secret : SECRET
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        {
            prefix: 'app',
            control: secret
                ? { secret, maxPayloadBytes: options.maxPayloadBytes }
                : undefined,
        },
    )
    driver.onControlRefused((refusal) => seen.push(refusal))
    return { driver, seen }
}

Deno.test('#318 an OVERSIZE refusal reaches the seam, naming the channel and the numbers', async () => {
    // The reachable trigger, and it is not theoretical: `PresenceMember.info`
    // carries no bound, and applications are told to put "public info shown to
    // other members" in it — avatars, profile blobs.
    const { driver, seen } = driverWith({ maxPayloadBytes: 256 })
    await driver.publishControl({
        kind: 'presence-join',
        target: 'conn-1',
        channel: 'presence-ops',
        member: { id: 'ada', info: { bio: 'x'.repeat(1024) } },
    })
    await driver.close()

    assertEquals(seen.length, 1, 'the refusal must reach the seam')
    const [refusal] = seen
    assertEquals(refusal.reason, 'oversize')
    assertEquals(refusal.kind, 'presence-join')
    assertEquals(
        refusal.channel,
        'presence-ops',
        'without the channel an operator cannot act on this without ' +
            'correlating raw logs across instances',
    )
    assertEquals(refusal.limit, 256)
    assert(
        (refusal.bytes ?? 0) > 256,
        `the measured size must exceed the limit; saw ${refusal.bytes}`,
    )
})

Deno.test('#318 a NO-SECRET refusal is reported as a DIFFERENT reason', async () => {
    // The two refusals have different fixes — shrink the member, or configure a
    // secret — so a single "publish failed" signal would leave the operator to
    // guess which. That is the whole argument for `reason` being an enum rather
    // than a message.
    const { driver, seen } = driverWith({ secret: undefined })
    await driver.publishControl({
        kind: 'evict',
        target: 'conn-1',
    })
    await driver.close()

    assertEquals(seen.length, 1)
    assertEquals(seen[0].reason, 'no-secret')
    assertEquals(seen[0].kind, 'evict')
    assertEquals(
        seen[0].channel,
        undefined,
        'an evict spans every channel and names none',
    )
    assertEquals(
        seen[0].bytes,
        undefined,
        'size is meaningless for this refusal and must not be invented',
    )
})

Deno.test('#318 a CLEAN publish fires nothing — the control', async () => {
    // Without this the two tests above are satisfied by a seam that fires on
    // every publish, which would be worse than no signal: an alert that is
    // always on is an alert nobody reads.
    const { driver, seen } = driverWith()
    await driver.publishControl({
        kind: 'presence-join',
        target: 'conn-1',
        channel: 'presence-ops',
        member: { id: 'ada' },
    })
    await driver.close()
    assertEquals(seen, [], 'a published frame is not a refusal')
})

Deno.test('#318 a THROWING handler does not become the publisher’s problem', async () => {
    // The seam reports on a path whose whole point is that the failure is
    // already being swallowed. Letting an observability callback throw would
    // make publishing MORE fragile than it was before the seam existed — the
    // same containment #296 established for the subscribe handler.
    const redis = new FakeRedis()
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: 'app', control: { secret: SECRET, maxPayloadBytes: 128 } },
    )
    driver.onControlRefused(() => {
        throw new Error('handler is broken')
    })
    // Must not reject.
    await driver.publishControl({
        kind: 'presence-join',
        target: 'conn-1',
        channel: 'presence-ops',
        member: { id: 'ada', info: { bio: 'x'.repeat(1024) } },
    })
    await driver.close()
})
