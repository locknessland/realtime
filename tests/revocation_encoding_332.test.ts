/**
 * @fileoverview #332 — a revocation record decodes back to what was written, or
 * to nothing at all.
 *
 * The revocation index is **the only cross-instance write channel in this
 * package with no authenticity tag.** A control frame carries a MAC and is
 * verified before it is obeyed; a sorted-set member is whatever is in the
 * sorted set. Whoever can write to the broker can put anything there, and what
 * comes back is handed almost directly to a revocation.
 *
 * That was already true before this issue, and one line held it:
 * `if (id && isValidName(id)) live.add(id)`. What #332 changes is that the
 * decoder now also decides **scope** — and the dangerous failure is no longer
 * "an unknown id is revoked" but **a channel-scoped record returned without its
 * channel**, which the manager applies as a whole-connection revocation:
 * hard-close 4403, every other room gone. A room ban escalated to a session
 * kill, with no error, no warning and no type failure.
 *
 * **So every negative case below asserts the record is DROPPED, never that it
 * is narrowed.** A test that only checked "the bad member did not appear as a
 * channel revocation" would pass for the worst possible reason.
 *
 * The other direction matters too and has its own test: the reap is by SCORE
 * only, so a reader that cannot decode a member also cannot delete it — which
 * is what preserves a channel-scoped record for the instance that owns the
 * socket during a rolling deploy.
 *
 * @module @lockness/realtime/tests/revocation_encoding_332
 */

import { assert, assertEquals } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { FakeRedis } from './fake_redis.ts'

const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
/** Revocation ids as the manager mints them (#337). */
const ID1 = '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c01'
const ID2 = '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c02'

/** A driver on `redis`, with a TTL long enough that nothing expires mid-test. */
function driverOn(redis: FakeRedis): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
}

/**
 * Write a raw member into the index, bypassing the driver.
 *
 * This is how a record from **another** writer arrives — an instance running an
 * older release, or anyone with bus access. Going through `markRevocation`
 * would only ever produce members this driver considers well-formed, which is
 * precisely the population these tests are not about.
 */
const plant = async (redis: FakeRedis, member: string) => {
    await redis.command('ZADD', INDEX, '9999999999', member)
}

Deno.test('#332 a bare member decodes to CONNECTION scope', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        await driver.markRevocation({ target: 'c1' })
        assertEquals(await driver.listRevocations(), [{ target: 'c1' }])
    } finally {
        await driver.close()
    }
})

Deno.test('#332 a composite member decodes to CHANNEL scope, round-trip', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        await driver.markRevocation({
            target: 'c1',
            channel: 'private-orders',
            id: ID1,
        })
        assertEquals(await driver.listRevocations(), [{
            target: 'c1',
            channel: 'private-orders',
            id: ID1,
        }], 'the id comes back verbatim — it is part of the record (#337)')
        // And the two scopes coexist in one index, on the same target, without
        // either shadowing the other — they are different members.
        await driver.markRevocation({ target: 'c1' })
        assertEquals(
            (await driver.listRevocations()).length,
            2,
            'a connection-scoped record and a channel-scoped one for the same ' +
                'target are two records, not one overwriting the other',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#332 an undecodable member is DROPPED, never widened', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        // Each of these is a member the driver would never write, arriving from
        // somewhere else. NONE may come back as `{ target }` — that is the
        // escalation this whole decoder exists to refuse.
        const hostile = [
            // The two-part form unreleased builds of `main` wrote before #337.
            // It names no id, so no clear could remove exactly it.
            'c1 private-orders',
            // A fourth part: a decoder that took the first three would hand
            // back a record that was never written.
            `c1 private-orders ${ID1} extra`,
            // An invalid target part.
            `c1;x private-orders ${ID1}`,
            // An invalid channel part — a wildcard is outside the charset and
            // is exactly what an attacker would reach for.
            `c1 * ${ID1}`,
            // An invalid id part.
            'c1 private-orders *',
            // A bare member outside the charset.
            'c1;FLUSHALL',
            // Leading separator: an empty target.
            ` private-orders ${ID1}`,
            // A doubled separator: an empty channel.
            `c1  ${ID1}`,
            // Trailing separator: an empty id.
            'c1 private-orders ',
        ]
        for (const member of hostile) await plant(redis, member)

        const live = await driver.listRevocations()

        assertEquals(
            live,
            [],
            'every undecodable member is dropped. A member returned here as ' +
                '`{ target }` would be applied as a WHOLE-CONNECTION ' +
                'revocation — hard-close 4403 — for a record that named a ' +
                'single room',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#332 a dropped member is not DELETED — the owner can still act on it', async () => {
    // The mixed-fleet property, from the index side. The reap is by SCORE only
    // (`ZREMRANGEBYSCORE`) and the charset filter is JS-side, so an instance
    // that cannot decode a member also cannot remove it. Make the reap drop
    // what it fails to parse and a rolling deploy silently deletes live
    // revocations — a change that would pass every same-version test.
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        await plant(redis, `c1 private-orders ${ID1} extra`)
        assertEquals(await driver.listRevocations(), [], 'skipped on read')
        assertEquals(
            redis.zcard(INDEX),
            1,
            'and still stored — a reader that cannot use a record must not ' +
                'be the reader that destroys it',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#332 the driver REFUSES to write a record it could not decode back', async () => {
    // The write side of the same rule. The manager asserts both names before
    // it gets here, so this is the last point at which an undecodable member
    // can be prevented rather than merely detected later.
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        for (
            const bad of [
                { target: 'c1', channel: 'orders room', id: ID1 },
                { target: 'has space', channel: 'orders', id: ID1 },
                { target: 'c1', channel: 'orders', id: 'not an id' },
                { target: 'c1;FLUSHALL' },
            ]
        ) {
            let threw = false
            try {
                await driver.markRevocation(bad)
            } catch {
                threw = true
            }
            assert(
                threw,
                `writing ${JSON.stringify(bad)} must be refused, not stored`,
            )
        }
        assertEquals(redis.zcard(INDEX), 0, 'nothing reached the index')
    } finally {
        await driver.close()
    }
})

Deno.test('#332 clearRevocation removes exactly its own record', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = driverOn(redis)
    try {
        await driver.markRevocation({
            target: 'c1',
            channel: 'orders',
            id: ID1,
        })
        await driver.markRevocation({
            target: 'c1',
            channel: 'billing',
            id: ID2,
        })
        await driver.markRevocation({ target: 'c1' })

        await driver.clearRevocation({
            target: 'c1',
            channel: 'orders',
            id: ID1,
        })

        assertEquals(
            // Sorted: the index returns by score then lexicographically, and
            // the ORDER of live revocations is not part of the contract — a
            // test that pinned it would fail on a change nobody made.
            (await driver.listRevocations()).map((r) => r.channel ?? '*')
                .sort(),
            ['*', 'billing'],
            'clearing one room leaves the other room AND the ' +
                'connection-scoped record untouched — three members, one ' +
                'removed',
        )
        // Clearing a record that is already gone is not an error: the reconcile
        // and the local apply can both reach it, and the second must be a
        // no-op rather than a failure that gets logged as a fault.
        await driver.clearRevocation({
            target: 'c1',
            channel: 'orders',
            id: ID1,
        })
        assertEquals((await driver.listRevocations()).length, 2)
    } finally {
        await driver.close()
    }
})
