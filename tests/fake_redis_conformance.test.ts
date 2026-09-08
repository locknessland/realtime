/**
 * @fileoverview #280 — the fake's arms audited against real Redis semantics.
 *
 * #276 shipped a security fix that was wrong **twice** with a green suite, both
 * times because this fake modelled a command incorrectly in exactly the spot
 * that mattered. The throwing `default:` added there catches an **unmodelled**
 * command; it does nothing about a command that is modelled and models the
 * wrong thing, which is what actually happened.
 *
 * So every assertion here states a real-Redis behaviour the fake must match,
 * and the ones that matter are the boring ones — an option token silently
 * ignored, a second key argument silently dropped, a clock that only half the
 * arms consult.
 *
 * @module @lockness/realtime/tests/fake_redis_conformance
 */

import { assertEquals, assertThrows } from '@std/assert'
import { FakeRedis } from './fake_redis.ts'

const int = (value: number) => ({ type: 'integer', value })

/**
 * Call the fake synchronously so `assertThrows` can see the throw.
 *
 * `command` evaluates the arm before wrapping the reply in a promise, so a
 * rejected option token throws out of the call itself rather than rejecting.
 */
const run = (r: FakeRedis, ...args: string[]) => void r.command(...args)

Deno.test('#280 DEL takes many keys, counts them, and reaches every store', async () => {
    const r = new FakeRedis()
    await r.command('SET', 'str', 'v')
    await r.command('SADD', 'set', 'a')
    await r.command('HSET', 'hash', 'f', 'v')
    await r.command('ZADD', 'zset', '1', 'm')

    // Real Redis: DEL key [key ...] -> the number of keys actually removed.
    assertEquals(
        await r.command('DEL', 'str', 'set', 'hash', 'zset', 'absent'),
        int(4),
    )
    for (const key of ['str', 'set', 'hash', 'zset']) {
        assertEquals(await r.command('EXISTS', key), int(0), key)
    }
})

Deno.test('#280 DEL removes a sorted set, which the revocation index is', async () => {
    // The arm reached #hashes, #sets and #strings and stopped there. The
    // revocation index #276 introduced is a ZSET, so deleting it returned 0 and
    // left every member in place — a cleanup that silently did nothing.
    const r = new FakeRedis()
    await r.command('ZADD', 'app:revocations', '99', 'client-1')
    assertEquals(await r.command('DEL', 'app:revocations'), int(1))
    assertEquals(
        await r.command('ZRANGEBYSCORE', 'app:revocations', '-inf', '+inf'),
        { type: 'array', value: [] },
    )
})

Deno.test('#280 EXISTS takes many keys and sees every type', async () => {
    const r = new FakeRedis()
    await r.command('SADD', 'set', 'a')
    await r.command('HSET', 'hash', 'f', 'v')
    await r.command('ZADD', 'zset', '1', 'm')
    await r.command('SET', 'str', 'v')

    // It consulted only #strings, so EXISTS on a set, hash or zset said 0.
    for (const key of ['set', 'hash', 'zset', 'str']) {
        assertEquals(await r.command('EXISTS', key), int(1), key)
    }
    assertEquals(await r.command('EXISTS', 'str', 'set', 'absent'), int(2))
})

Deno.test('#280 SET rejects an option it does not model', async () => {
    const r = new FakeRedis()
    // Silently ignoring NX is the exact shape that made an inert guard look
    // like a working one: the caller believes it wrote only if absent.
    for (const opts of [['NX'], ['XX'], ['KEEPTTL'], ['PX', '100'], ['GET']]) {
        assertThrows(
            () => run(r, 'SET', 'k', 'v', ...opts),
            Error,
            'unmodelled SET option',
            opts.join(' '),
        )
    }
    // EX is modelled, so it still works.
    assertEquals(await r.command('SET', 'k', 'v', 'EX', '30'), {
        type: 'simple',
        value: 'OK',
    })
})

Deno.test('#280 ZRANGEBYSCORE rejects an option it does not model', async () => {
    const r = new FakeRedis()
    await r.command('ZADD', 'z', '1', 'm')
    for (const opts of [['WITHSCORES'], ['LIMIT', '0', '10']]) {
        assertThrows(
            () => run(r, 'ZRANGEBYSCORE', 'z', '-inf', '+inf', ...opts),
            Error,
            'unmodelled ZRANGEBYSCORE option',
            opts.join(' '),
        )
    }
})

Deno.test('#280 the fake has ONE clock, and setTime moves all of it', async () => {
    // EXPIRE and the zset TTL consulted `#now()`, which `setTime()` overrides.
    // `SET ... EX` and the string liveness check consulted `Date.now()`, which
    // it does not. So advancing the clock expired sorted sets and never expired
    // string keys — two clocks in one fake, and a test could believe either.
    const r = new FakeRedis()
    r.setTime(1_000)

    await r.command('SET', 'liveness', 'up', 'EX', '30')
    await r.command('ZADD', 'z', '1', 'm')
    await r.command('EXPIRE', 'z', '30')

    assertEquals(await r.command('EXISTS', 'liveness'), int(1), 'before')

    r.setTime(1_031)
    assertEquals(await r.command('EXISTS', 'liveness'), int(0), 'string TTL')
    assertEquals(
        await r.command('ZRANGEBYSCORE', 'z', '-inf', '+inf'),
        { type: 'array', value: [] },
        'zset TTL',
    )
})

Deno.test('#280 HSET and HDEL take many field/value pairs', async () => {
    const r = new FakeRedis()
    // Real Redis: HSET key field value [field value ...] -> fields ADDED.
    assertEquals(await r.command('HSET', 'h', 'a', '1', 'b', '2'), int(2))
    assertEquals(await r.command('HSET', 'h', 'a', '9', 'c', '3'), int(1))
    assertEquals(await r.command('HDEL', 'h', 'a', 'b', 'absent'), int(2))
    assertEquals(await r.command('HGETALL', 'h'), {
        type: 'array',
        value: [{ type: 'bulk', value: 'c' }, { type: 'bulk', value: '3' }],
    })
})

Deno.test('#280 an odd HSET argument list is refused, not half-applied', () => {
    const r = new FakeRedis()
    // 'HSET' alone would also match the `default:` arm's "unmodelled command
    // 'HSET'", so deleting the whole case would leave this green. Assert the
    // arity message, which only this branch produces.
    assertThrows(
        () => run(r, 'HSET', 'h', 'a', '1', 'dangling'),
        Error,
        'needs field/value pairs',
    )
})

Deno.test('#280 SET refuses an EX with a missing or unparseable value', () => {
    const r = new FakeRedis()
    // `opts[++i]` consumed the argument without checking it was there. Both of
    // these returned OK and stored `expireAt = NaN`; `NaN >= x` is false
    // forever, so the key became IMMORTAL and nothing said so. That is the
    // silent no-op this very arm was rewritten to stop.
    for (const opts of [['EX'], ['EX', 'abc'], ['EX', '']]) {
        assertThrows(
            () => run(r, 'SET', 'k', 'v', ...opts),
            Error,
            'SET',
            opts.join(' '),
        )
    }
    // And a repeated EX is a syntax error in Redis, not last-wins.
    assertThrows(() => run(r, 'SET', 'k', 'v', 'EX', '10', 'EX', '20'), Error)
})

Deno.test('#280 key-level TTL is authoritative for every type', async () => {
    // `#keyExpiry` was consulted only through `#liveZset`, so EXPIRE on a
    // string, a set or a hash was inert for existence and DEL over-counted:
    // four logically-expired keys returned 3 where Redis returns 0.
    const r = new FakeRedis()
    r.setTime(1_000)
    await r.command('SET', 's', 'v')
    await r.command('SADD', 'set', 'a')
    await r.command('HSET', 'h', 'f', 'v')
    await r.command('ZADD', 'z', '1', 'm')
    for (const key of ['s', 'set', 'h', 'z']) {
        assertEquals(await r.command('EXPIRE', key, '30'), int(1), key)
    }

    r.setTime(1_031)
    for (const key of ['s', 'set', 'h', 'z']) {
        assertEquals(await r.command('EXISTS', key), int(0), key)
    }
    assertEquals(await r.command('DEL', 's', 'set', 'h', 'z'), int(0))
})

Deno.test('#280 a plain SET clears an existing TTL, as Redis does', async () => {
    const r = new FakeRedis()
    r.setTime(1_000)
    await r.command('SET', 'k', 'v', 'EX', '30')
    await r.command('SET', 'k', 'v2')

    r.setTime(1_031)
    assertEquals(
        await r.command('EXISTS', 'k'),
        int(1),
        'the TTL survived a SET',
    )
})

Deno.test('#280 the arms with no options refuse extra arguments', () => {
    const r = new FakeRedis()
    // The AGENTS.md rule says every arm rejects an option it does not
    // implement. It was written before these four did, which made the rule
    // broader than the code — the exact overclaim this issue is about.
    for (
        const argv of [
            ['ZADD', 'z', 'INCR', '5', 'm'],
            ['SMEMBERS', 'set', 'EXTRA'],
            ['HGETALL', 'h', 'EXTRA'],
            ['PUBLISH', 'topic', 'payload', 'EXTRA'],
        ]
    ) {
        assertThrows(() => run(r, ...argv), Error, 'FakeRedis', argv.join(' '))
    }
})

Deno.test('#280 a rejection survives a driver that swallows it', async () => {
    // The driver console.warns around its heartbeat, reconcile and revocation
    // passes, so a throw from this fake goes silent on exactly the paths the
    // new guards were added for — the #276 shape, one layer down. The fake
    // keeps its own ledger so a swallowed rejection still fails the run.
    const r = new FakeRedis()
    try {
        await r.command('SET', 'k', 'v', 'NX')
    } catch {
        // Swallowed, exactly as the driver would.
    }
    assertThrows(
        () => r.assertNoRejections(),
        Error,
        'unmodelled SET option',
    )
})

Deno.test('#280 an emptied collection stops existing, as Redis does', async () => {
    // Redis removes a key when its last element goes. This fake left the empty
    // container behind, which was invisible while EXISTS consulted only the
    // string store — and became wrong the moment EXISTS saw every type. The
    // sharpest case is the #276 revocation index, which the reap drives to
    // empty by design.
    const r = new FakeRedis()
    await r.command('SADD', 'set', 'a')
    await r.command('HSET', 'h', 'f', 'v')
    await r.command('ZADD', 'app:revocations', '5', 'client-1')

    await r.command('SREM', 'set', 'a')
    await r.command('HDEL', 'h', 'f')
    await r.command('ZREMRANGEBYSCORE', 'app:revocations', '-inf', '+inf')

    for (const key of ['set', 'h', 'app:revocations']) {
        assertEquals(await r.command('EXISTS', key), int(0), key)
        assertEquals(await r.command('DEL', key), int(0), key)
    }
})

Deno.test('#280 EXPIRE on a missing key changes nothing', async () => {
    // Redis returns 0 and records nothing. Writing the expiry anyway let a
    // LATER create inherit a TTL it never asked for — the hazard `#dropKey`
    // exists to prevent, closed on DEL and left open here.
    const r = new FakeRedis()
    r.setTime(1_000)
    assertEquals(await r.command('EXPIRE', 'ghost', '30'), int(0))

    await r.command('ZADD', 'ghost', '1', 'm')
    r.setTime(1_031)
    assertEquals(
        await r.command('EXISTS', 'ghost'),
        int(1),
        'the new key inherited an expiry set before it existed',
    )
})

Deno.test('#280 ZADD writes every score/member pair', async () => {
    // The one variadic arm still dropping arguments after the first — the same
    // class DEL, EXISTS, HSET and HDEL were all fixed for, left in the arm #276
    // actually depends on.
    const r = new FakeRedis()
    assertEquals(await r.command('ZADD', 'z', '1', 'a', '2', 'b'), int(2))
    assertEquals(await r.command('ZADD', 'z', '3', 'b', '4', 'c'), int(1))
    assertEquals(
        await r.command('ZRANGEBYSCORE', 'z', '-inf', '+inf'),
        {
            type: 'array',
            value: ['a', 'b', 'c'].map((value) => ({ type: 'bulk', value })),
        },
    )
    assertThrows(() => run(r, 'ZADD', 'z', '1', 'a', '2'), Error, 'ZADD')
    assertThrows(() => run(r, 'ZADD', 'z', 'notanumber', 'a'), Error, 'ZADD')
})

Deno.test('#280 ZADD reads its flags by position, not by content', async () => {
    // The old scan ran `filter` over the whole tail, so a MEMBER named `gt` was
    // silently accepted as a flag and one named `nx` raised a spurious
    // rejection: the argument list decided by content rather than position.
    const r = new FakeRedis()
    assertEquals(await r.command('ZADD', 'z', '1', 'gt'), int(1))
    assertEquals(await r.command('ZADD', 'z', '2', 'nx'), int(1))
    assertEquals(
        await r.command('ZRANGEBYSCORE', 'z', '-inf', '+inf'),
        {
            type: 'array',
            value: ['gt', 'nx'].map((value) => ({ type: 'bulk', value })),
        },
    )
    // A LEADING NX is still refused — it is a real option this fake does not model.
    assertThrows(() => run(r, 'ZADD', 'z', 'NX', '1', 'm'), Error, 'ZADD')
})

Deno.test('#280 ZRANGEBYSCORE breaks a score tie lexicographically', async () => {
    // A stable sort left ties in insertion order. Revocation scores are whole
    // seconds from TIME, so two evictions in the same second tie routinely and
    // consumers got an order the real broker would never produce.
    const r = new FakeRedis()
    await r.command('ZADD', 'z', '5', 'zebra', '5', 'alpha', '5', 'mid')
    assertEquals(await r.command('ZRANGEBYSCORE', 'z', '-inf', '+inf'), {
        type: 'array',
        value: ['alpha', 'mid', 'zebra'].map((value) => ({
            type: 'bulk',
            value,
        })),
    })
})

Deno.test('#280 DEL clears the key TTL, so a re-create does not inherit it', async () => {
    // `#dropKey`'s JSDoc states this as its reason for existing and nothing
    // exercised it: deleting the line left every test green. A key re-created
    // after a DEL must not expire at a time it never asked for.
    const r = new FakeRedis()
    r.setTime(1_000)
    await r.command('ZADD', 'z', '1', 'm')
    await r.command('EXPIRE', 'z', '30')
    assertEquals(await r.command('DEL', 'z'), int(1))

    await r.command('ZADD', 'z', '1', 'm')
    r.setTime(1_031)
    assertEquals(
        await r.command('EXISTS', 'z'),
        int(1),
        'the re-created key inherited the deleted TTL',
    )
})
