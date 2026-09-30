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

import { assert, assertEquals, assertRejects, assertThrows } from '@std/assert'
import { FakeRedis, serializedCommands } from './fake_redis.ts'

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
    // `GET` left this list with #349: it is modelled now (see the WC test
    // below), because the heartbeat reads the lapse bit from it.
    for (const opts of [['NX'], ['XX'], ['KEEPTTL'], ['PX', '100']]) {
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

Deno.test('#349 WC SET … GET answers the previous string, or nil when absent or expired', async () => {
    // Real Redis (>= 6.2): `SET key value [EX s] GET` writes, and answers the
    // value the key held BEFORE the write — nil when there was none. The
    // heartbeat reads its lapse bit from exactly that nil (#349 FR-001), so an
    // arm that answered `OK`, or read the previous value after the expiry was
    // ignored, would hide or invent every lapse.
    const r = new FakeRedis()
    r.setTime(1_000)
    assertEquals(
        await r.command('SET', 'k', 'v', 'EX', '30', 'GET'),
        { type: 'nil' },
        'absent',
    )
    assertEquals(
        await r.command('SET', 'k', 'v', 'EX', '30', 'GET'),
        { type: 'bulk', value: 'v' },
        'present',
    )
    // GET may sit anywhere among the options.
    assertEquals(
        await r.command('SET', 'k', 'w', 'GET', 'EX', '30'),
        { type: 'bulk', value: 'v' },
        'reordered',
    )
    // The write itself still happened, with its TTL.
    assertEquals(await r.command('EXISTS', 'k'), int(1))
    assertEquals(r.expiryOf('k'), 1_030)

    // Expired: the previous value is gone BEFORE the write reads it.
    r.setTime(1_031)
    assertEquals(
        await r.command('SET', 'k', 'x', 'EX', '30', 'GET'),
        { type: 'nil' },
        'expired',
    )
    assertEquals(r.expiryOf('k'), 1_061)
})

Deno.test('#349 WC SET refuses GET twice, and GET over a key of another type', async () => {
    const r = new FakeRedis()
    // GET twice: a declared gap — a real broker accepts it, the fake refuses
    // rather than guess (the live WC test pins both halves).
    assertThrows(
        () => run(r, 'SET', 'k', 'v', 'GET', 'EX', '30', 'GET'),
        Error,
        'SET given GET twice',
    )
    // The broker answers WRONGTYPE, and — unlike a plain SET — does not
    // overwrite the key (#349 S4).
    await r.command('HSET', 'h', 'f', 'v')
    assertThrows(
        () => run(r, 'SET', 'h', 'v', 'EX', '30', 'GET'),
        Error,
        'WRONGTYPE',
    )
    assertEquals(await r.command('HGET', 'h', 'f'), {
        type: 'bulk',
        value: 'v',
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

const bulk = (value: string) => ({ type: 'bulk', value })
const nil = { type: 'nil' }

Deno.test('#341 HLEN counts a hash, answers 0 for an absent key, and refuses extra arguments', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'a', '1', 'b', '2')
    assertEquals(await r.command('HLEN', 'h'), int(2))
    assertEquals(await r.command('HLEN', 'absent'), int(0))
    assertThrows(
        () => run(r, 'HLEN', 'h', 'EXTRA'),
        Error,
        'HLEN takes one key',
    )
})

Deno.test('#341 HMGET answers in field order with nil for an absent field', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'a', '1', 'b', '2')
    assertEquals(await r.command('HMGET', 'h', 'b', 'zz', 'a'), {
        type: 'array',
        value: [bulk('2'), nil, bulk('1')],
    })
    assertEquals(await r.command('HMGET', 'absent', 'a'), {
        type: 'array',
        value: [nil],
    })
})

Deno.test('#341 HMGET with no field is an arity error, as on a real broker', () => {
    // Real Redis answers `ERR wrong number of arguments for 'hmget'`. A fake
    // that returned an empty array would hide a script issuing HMGET with zero
    // self ids (#341 A1) until it met a real broker.
    const r = new FakeRedis()
    assertThrows(
        () => run(r, 'HMGET', 'h'),
        Error,
        'HMGET needs at least one field',
    )
})

Deno.test('#341 HRANDFIELD count WITHVALUES at or above the size returns the whole hash in HGETALL order', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'b', '2', 'a', '1', 'c', '3')
    const whole = await r.command('HGETALL', 'h')
    assertEquals(await r.command('HRANDFIELD', 'h', '3', 'WITHVALUES'), whole)
    assertEquals(
        await r.command('HRANDFIELD', 'h', '50', 'withvalues'),
        whole,
    )
})

Deno.test('#341 HRANDFIELD count WITHVALUES below the size returns count distinct pairs', async () => {
    // Real Redis picks the pairs at random; this fake answers the first
    // `count` in insertion order, which is one outcome a real broker can
    // produce — never a property a test may rely on beyond distinctness.
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'a', '1', 'b', '2', 'c', '3')
    assertEquals(await r.command('HRANDFIELD', 'h', '2', 'WITHVALUES'), {
        type: 'array',
        value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    })
    assertEquals(
        await r.command('HRANDFIELD', 'absent', '2', 'WITHVALUES'),
        { type: 'array', value: [] },
    )
})

Deno.test('#341 HRANDFIELD refuses the shapes it does not model', () => {
    // A negative count returns REPEATED pairs on a real broker, no count
    // returns one bare field, and no WITHVALUES changes the reply shape. Each
    // is refused rather than answered as if it were the modelled form.
    const r = new FakeRedis()
    for (
        const argv of [
            ['HRANDFIELD', 'h'],
            ['HRANDFIELD', 'h', '2'],
            ['HRANDFIELD', 'h', '-2', 'WITHVALUES'],
            ['HRANDFIELD', 'h', '1.5', 'WITHVALUES'],
            ['HRANDFIELD', 'h', '2', 'WITHVALUES', 'EXTRA'],
        ]
    ) {
        assertThrows(
            () => run(r, ...argv),
            Error,
            'FakeRedis: HRANDFIELD',
            argv.join(' '),
        )
    }
})

Deno.test('#341 EVAL returns an integer as an integer and a nil element as nil, nested', async () => {
    // Redis converts a Lua number to an integer reply and a `false` table
    // element to a nil bulk. Stringifying either would let a driver parse a
    // reply shape no real broker sends.
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'a', '1')
    const script = "local n = redis.call('HLEN', KEYS[1])\n" +
        "local got = redis.call('HMGET', KEYS[1], unpack(ARGV, 1))\n" +
        'return {n, got}'
    assertEquals(await r.command('EVAL', script, '1', 'h', '', 'a'), {
        type: 'array',
        value: [int(1), { type: 'array', value: [nil, bulk('1')] }],
    })
})

Deno.test('#345 HGET answers a bulk for a present field, nil for an absent one, and checks its arity', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'a', '1')
    assertEquals(await r.command('HGET', 'h', 'a'), bulk('1'))
    assertEquals(await r.command('HGET', 'h', 'zz'), nil)
    assertEquals(await r.command('HGET', 'absent', 'a'), nil)
    for (const argv of [['HGET', 'h'], ['HGET', 'h', 'a', 'EXTRA']]) {
        assertThrows(
            () => run(r, ...argv),
            Error,
            'FakeRedis: HGET',
            argv.join(' '),
        )
    }
})

Deno.test('#345 inside a script, HGET of a missing field is false — the nil reply converted once, at the bridge', async () => {
    // Redis hands Lua `false` for a nil reply. The release script's
    // `mine == false` decides the `gone` bit #344 announces from, so a bridge
    // handing Lua anything else would make the fake and a real broker disagree
    // on exactly that bit.
    const r = new FakeRedis()
    await r.command('HSET', 'h', 'present', 'v')
    const script = "local v = redis.call('HGET', KEYS[1], ARGV[1])\n" +
        'if v == false then\nreturn 1\nend\nreturn 0'
    assertEquals(await r.command('EVAL', script, '1', 'h', 'missing'), int(1))
    assertEquals(await r.command('EVAL', script, '1', 'absent', 'f'), int(1))
    assertEquals(await r.command('EVAL', script, '1', 'h', 'present'), int(0))
})

Deno.test('#345 HRANDFIELD key 1 WITHVALUES is [field, value], indexable in a script', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'holders', 'B', '{"id":7}')
    assertEquals(await r.command('HRANDFIELD', 'holders', '1', 'WITHVALUES'), {
        type: 'array',
        value: [bulk('B'), bulk('{"id":7}')],
    })
    const script =
        "local promoted = redis.call('HRANDFIELD', KEYS[1], 1, 'WITHVALUES')\n" +
        "redis.call('HSET', KEYS[2], ARGV[1], promoted[2])\n" +
        "return redis.call('HGET', KEYS[2], ARGV[1])"
    assertEquals(
        await r.command('EVAL', script, '2', 'holders', 'presence', '7'),
        bulk('{"id":7}'),
    )
})

Deno.test('#345 a holders hash emptied inside a script no longer EXISTS', async () => {
    const r = new FakeRedis()
    await r.command('HSET', 'holders', 'A', 'e')
    const script = "redis.call('HDEL', KEYS[1], ARGV[1])\n" +
        "return redis.call('HLEN', KEYS[1])"
    assertEquals(await r.command('EVAL', script, '1', 'holders', 'A'), int(0))
    assertEquals(await r.command('EXISTS', 'holders'), int(0))
})

Deno.test('#345 HSET returns 1 for a new field and 0 for an update, inside a script too', async () => {
    const r = new FakeRedis()
    assertEquals(await r.command('HSET', 'h', 'f', 'v1'), int(1))
    assertEquals(await r.command('HSET', 'h', 'f', 'v2'), int(0))
    const script =
        "local added = redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n" +
        'if added == 1 then\nreturn 1\nend\nreturn 0'
    assertEquals(await r.command('EVAL', script, '1', 'h', 'g', 'x'), int(1))
    assertEquals(await r.command('EVAL', script, '1', 'h', 'g', 'y'), int(0))
})

Deno.test('#348 the serialized wrapper runs one exchange at a time, in issue order', async () => {
    // `RedisClient` chains every command on one tail; FakeRedis settles each
    // on its own. W8's `left`-before-`joined` ordering depends on the former.
    const r = new FakeRedis()
    const started: string[] = []
    const serial = serializedCommands((...args) => {
        started.push(args[0])
        return r.command(...args)
    })
    const gate = serial.hold((args) => args[0] === 'HSET')
    const settled: string[] = []
    const first = serial.command('HSET', 'h', 'f', 'v').then((reply) => {
        settled.push('HSET')
        return reply
    })
    const second = serial.command('HGET', 'h', 'f').then((reply) => {
        settled.push('HGET')
        return reply
    })
    await gate.reached
    for (let i = 0; i < 20; i++) await Promise.resolve()
    assertEquals(
        started,
        ['HSET'],
        'the second command must not start while the first is in flight',
    )
    assertEquals(settled, [])

    gate.release()
    assertEquals(await first, int(1))
    assertEquals(await second, bulk('v'))
    assertEquals(started, ['HSET', 'HGET'])
    assertEquals(settled, ['HSET', 'HGET'])
})

Deno.test('#348 whenIssued resolves on the matching command being ISSUED, before it runs, and only for a match', async () => {
    // W8 waits on it to know the hold is queued behind the held release: it
    // must fire on issue (not on execution, which the gate is holding back)
    // and must not fire for a command that does not match.
    const r = new FakeRedis()
    const started: string[] = []
    const serial = serializedCommands((...args) => {
        started.push(args[0])
        return r.command(...args)
    })
    const gate = serial.hold((args) => args[0] === 'HSET')
    let issued = false
    const watched = serial.whenIssued((args) => args[0] === 'HGET')
        .then(() => void (issued = true))

    const first = serial.command('HSET', 'h', 'f', 'v')
    await gate.reached
    const other = serial.command('EXISTS', 'h')
    for (let i = 0; i < 20; i++) await Promise.resolve()
    assertEquals(issued, false, 'a non-matching command does not resolve it')

    const second = serial.command('HGET', 'h', 'f')
    await watched
    assertEquals(issued, true)
    assertEquals(
        started,
        ['HSET'],
        'resolved on issue: the matched command has not started yet',
    )

    gate.release()
    assertEquals(await first, int(1))
    assertEquals(await other, int(1))
    assertEquals(await second, bulk('v'))
    assertEquals(started, ['HSET', 'EXISTS', 'HGET'])
})

// --- #358: SSCAN and the scan core --------------------------------------------

/** An `SSCAN` reply as the fake returns it: `[cursor, [member, ...]]`. */
type ScanReply = {
    type: 'array'
    value: [{ value: string }, { value: { value: string }[] }]
}

/** One `SSCAN key cursor COUNT count` call: its next cursor and members. */
async function sscan(
    r: FakeRedis,
    key: string,
    cursor: string,
    count: string,
): Promise<{ cursor: string; members: string[] }> {
    const reply = await r.command(
        'SSCAN',
        key,
        cursor,
        'COUNT',
        count,
    ) as ScanReply
    return {
        cursor: reply.value[0].value,
        members: reply.value[1].value.map((m) => m.value),
    }
}

Deno.test('#358 WC SSCAN refuses a missing COUNT, any option but COUNT, and a non-canonical cursor — into the ledger', async () => {
    // The #280 rule: an option the fake does not model is REFUSED, never
    // ignored. MATCH filters the page, so an ignored MATCH hands the sweep
    // members a real broker would not; a missing COUNT is a scan whose page
    // its caller does not bound — the very defect #358 closes.
    const r = new FakeRedis()
    await r.command('SADD', 'set', 'a', 'b')
    const refused: [string[], string][] = [
        [['SSCAN', 'set', '0'], 'SSCAN without COUNT'],
        [
            ['SSCAN', 'set', '0', 'MATCH', '*', 'COUNT', '10'],
            "unmodelled SSCAN option 'MATCH'",
        ],
        [
            ['SSCAN', 'set', '0', 'COUNT', '10', 'MATCH', '*'],
            "unmodelled SSCAN option 'MATCH'",
        ],
        [
            ['SSCAN', 'set', '0', 'COUNT', '10', 'NOVALUES', 'x'],
            "unmodelled SSCAN option 'NOVALUES'",
        ],
        [
            ['SSCAN', 'set', '0', 'TYPE', 'string', 'COUNT', '10'],
            "unmodelled SSCAN option 'TYPE'",
        ],
        [['SSCAN', 'set', '00', 'COUNT', '10'], 'is not canonical decimal'],
        [['SSCAN', 'set', '01', 'COUNT', '10'], 'is not canonical decimal'],
        [['SSCAN', 'set', 'x1', 'COUNT', '10'], 'is not canonical decimal'],
        [['SSCAN', 'set', '', 'COUNT', '10'], 'is not canonical decimal'],
        [
            ['SSCAN', 'set', '1'.repeat(21), 'COUNT', '10'],
            'is not canonical decimal',
        ],
        [['SSCAN', 'set', '0', 'COUNT', '0'], 'COUNT must be a positive'],
        [['SSCAN', 'set', '0', 'COUNT', '-5'], 'COUNT must be a positive'],
        [['SSCAN', 'set', '0', 'COUNT'], 'COUNT must be a positive'],
        [
            ['SSCAN', 'set', '0', 'COUNT', '10', 'COUNT', '10'],
            'SSCAN given COUNT twice',
        ],
    ]
    for (const [argv, reason] of refused) {
        assertThrows(() => run(r, ...argv), Error, reason, argv.join(' '))
    }
    // Every refusal reached the ledger, so a driver that swallows one still
    // fails its teardown.
    assertThrows(
        () => r.assertNoRejections(),
        Error,
        `refused ${refused.length} command(s)`,
    )
})

Deno.test('#358 SSCAN answers a set of at most COUNT whole at cursor 0, and an absent key as [0, []]', async () => {
    const r = new FakeRedis()
    assertEquals(await sscan(r, 'absent', '0', '10'), {
        cursor: '0',
        members: [],
    })
    await r.command('SADD', 'set', 'a', 'b', 'c')
    assertEquals(await sscan(r, 'set', '0', '3'), {
        cursor: '0',
        members: ['a', 'b', 'c'],
    })
    r.assertNoRejections()
})

Deno.test('#358 SSCAN walks a larger set by stable slots: each member once per iteration, and a removal shifts nothing', async () => {
    const r = new FakeRedis()
    const seeded = Array.from({ length: 300 }, (_, i) => `m${i}`)
    await r.command('SADD', 'set', ...seeded)

    // One whole iteration: pages in slot order from the cursor, each member
    // exactly once, an empty page with a non-zero cursor, cursor 0 at the end.
    const seen: string[] = []
    let cursor = '0'
    let calls = 0
    let emptyMidway = false
    do {
        const page = await sscan(r, 'set', cursor, '10')
        calls++
        for (const m of page.members) {
            assert(
                FakeRedis.scanSlot(m) >= Number(cursor),
                `${m} lies at or ahead of cursor ${cursor}`,
            )
        }
        if (page.members.length === 0 && page.cursor !== '0') {
            emptyMidway = true
        }
        seen.push(...page.members)
        cursor = page.cursor
    } while (cursor !== '0')
    assert(calls > 1, 'a set larger than COUNT is paged')
    assert(emptyMidway, 'a sparse window answers an empty page, not the end')
    assertEquals(seen.length, seeded.length, 'no member returned twice')
    assertEquals([...seen].sort(), [...seeded].sort())

    // A removal mid-iteration moves nobody: the rest of the walk returns
    // exactly the members whose slots it had not visited yet.
    const first = await sscan(r, 'set', '0', '100')
    const next = Number(first.cursor)
    const behind = seeded.filter((m) => FakeRedis.scanSlot(m) < next)
    const ahead = seeded.filter((m) => FakeRedis.scanSlot(m) >= next)
    assertEquals([...first.members].sort(), [...behind].sort())
    await r.command('SREM', 'set', ahead[0], behind[0])
    const rest: string[] = []
    cursor = first.cursor
    do {
        const page = await sscan(r, 'set', cursor, '100')
        rest.push(...page.members)
        cursor = page.cursor
    } while (cursor !== '0')
    assertEquals([...rest].sort(), ahead.slice(1).sort())
    r.assertNoRejections()
})

Deno.test('#358 the scan core refuses past its per-key call ceiling, so a scan that never advances fails instead of hanging', async () => {
    const r = new FakeRedis()
    await r.command(
        'SADD',
        'set',
        ...Array.from({ length: 20 }, (_, i) => `m${i}`),
    )
    // A caller that restarts at cursor 0 on every call never ends an
    // iteration, so only a ceiling on calls — not on one iteration — stops it.
    let refusedAt = 0
    for (let call = 1; call <= 2_000 && refusedAt === 0; call++) {
        try {
            await sscan(r, 'set', '0', '1')
        } catch (error) {
            refusedAt = call
            assert(
                error instanceof Error &&
                    error.message.includes('scan call ceiling'),
                String(error),
            )
        }
    }
    assertEquals(refusedAt, 1_001, 'refused on the first call past 1,000')
    assertThrows(() => r.assertNoRejections(), Error, 'scan call ceiling')
})

Deno.test('#358 the scan core refuses past its cumulative call ceiling, across keys, so a caller that moves to a fresh key per call still fails', async () => {
    const r = new FakeRedis()
    const keys = Array.from({ length: 11 }, (_, i) => `set-${i}`)
    for (const key of keys) await r.command('SADD', key, 'm')
    // Ten keys at 1,000 calls each: every call within its key's ceiling, and
    // 10,000 in all — the cumulative ceiling exactly, not past it.
    for (const key of keys.slice(0, 10)) {
        for (let call = 0; call < 1_000; call++) {
            await sscan(r, key, '0', '1')
        }
    }
    r.assertNoRejections()
    // The 10,001st call is the first on the eleventh key: only the
    // cumulative ceiling can refuse it.
    await assertRejects(
        () => sscan(r, keys[10], '0', '1'),
        Error,
        '(1 on this key, 10001 in all)',
    )
    assertThrows(() => r.assertNoRejections(), Error, 'scan call ceiling')
})

Deno.test('#358 WC SSCAN refuses a missing cursor and a cursor past the slot table — into the ledger', async () => {
    // Real Redis requires the cursor, and accepts any number as one; this fake
    // refuses a cursor past its table because it never issued one, so a test
    // that drives it with a made-up cursor fails instead of reading a
    // plausible `[0, []]` — an empty, finished iteration.
    const r = new FakeRedis()
    await r.command('SADD', 'set', 'a', 'b')
    const refused: [string[], string][] = [
        [['SSCAN'], 'SSCAN takes key and cursor, got 0'],
        [['SSCAN', 'set'], 'SSCAN takes key and cursor, got 1'],
        [
            ['SSCAN', 'set', '1024', 'COUNT', '10'],
            'lies past the 1024-slot table',
        ],
        [
            ['SSCAN', 'set', '99999', 'COUNT', '10'],
            'lies past the 1024-slot table',
        ],
    ]
    for (const [argv, reason] of refused) {
        assertThrows(() => run(r, ...argv), Error, reason, argv.join(' '))
    }
    assertThrows(
        () => r.assertNoRejections(),
        Error,
        `refused ${refused.length} command(s)`,
    )
    // The last slot is still a cursor the fake can have issued.
    const reply = await r.command('SSCAN', 'set', '1023', 'COUNT', '1')
    assertEquals((reply as { type: string }).type, 'array')
})

/** One `ZSCAN key cursor COUNT count` call: its next cursor and flat items. */
async function zscan(
    r: FakeRedis,
    key: string,
    cursor: string,
    count: string,
): Promise<{ cursor: string; items: string[] }> {
    const reply = await r.command(
        'ZSCAN',
        key,
        cursor,
        'COUNT',
        count,
    ) as ScanReply
    return {
        cursor: reply.value[0].value,
        items: reply.value[1].value.map((m) => m.value),
    }
}

Deno.test('#359 WC ZSCAN refuses a missing COUNT, any option but COUNT, and a non-canonical cursor — into the ledger', async () => {
    // The #280 rule, for the revocation pass's paged read. MATCH filters the
    // page and NOSCORES (Redis 8) drops the scores: ignoring either hands the
    // pass a page a real broker would not send. A missing COUNT is a read its
    // caller does not bound — the defect #359 closes.
    const r = new FakeRedis()
    await r.command('ZADD', 'zset', '1', 'a', '2', 'b')
    const refused: [string[], string][] = [
        [['ZSCAN', 'zset', '0'], 'ZSCAN without COUNT'],
        [
            ['ZSCAN', 'zset', '0', 'MATCH', '*', 'COUNT', '10'],
            "unmodelled ZSCAN option 'MATCH'",
        ],
        [
            ['ZSCAN', 'zset', '0', 'COUNT', '10', 'MATCH', '*'],
            "unmodelled ZSCAN option 'MATCH'",
        ],
        [
            ['ZSCAN', 'zset', '0', 'COUNT', '10', 'NOSCORES'],
            "unmodelled ZSCAN option 'NOSCORES'",
        ],
        [
            ['ZSCAN', 'zset', '0', 'NOSCORES', 'COUNT', '10'],
            "unmodelled ZSCAN option 'NOSCORES'",
        ],
        [
            ['ZSCAN', 'zset', '0', 'TYPE', 'zset', 'COUNT', '10'],
            "unmodelled ZSCAN option 'TYPE'",
        ],
        [['ZSCAN', 'zset', '00', 'COUNT', '10'], 'is not canonical decimal'],
        [['ZSCAN', 'zset', '01', 'COUNT', '10'], 'is not canonical decimal'],
        [['ZSCAN', 'zset', 'x1', 'COUNT', '10'], 'is not canonical decimal'],
        [
            ['ZSCAN', 'zset', '1'.repeat(21), 'COUNT', '10'],
            'is not canonical decimal',
        ],
        [['ZSCAN', 'zset', '0', 'COUNT', '0'], 'COUNT must be a positive'],
        [['ZSCAN', 'zset', '0', 'COUNT', '-5'], 'COUNT must be a positive'],
        [['ZSCAN', 'zset', '0', 'COUNT'], 'COUNT must be a positive'],
        [
            ['ZSCAN', 'zset', '0', 'COUNT', '10', 'COUNT', '10'],
            'ZSCAN given COUNT twice',
        ],
        [['ZSCAN'], 'ZSCAN takes key and cursor, got 0'],
        [['ZSCAN', 'zset'], 'ZSCAN takes key and cursor, got 1'],
    ]
    for (const [argv, reason] of refused) {
        assertThrows(() => run(r, ...argv), Error, reason, argv.join(' '))
    }
    assertThrows(
        () => r.assertNoRejections(),
        Error,
        `refused ${refused.length} command(s)`,
    )
})

Deno.test('#359 ZSCAN answers [cursor, [member, score, …]] with integral scores as plain digits, an absent key as [0, []], and honours key expiry', async () => {
    const r = new FakeRedis()
    r.setTime(1_000)
    assertEquals(await zscan(r, 'absent', '0', '10'), {
        cursor: '0',
        items: [],
    })
    await r.command('ZADD', 'zset', '1800000300', 'a', '7', 'b')
    const small = await zscan(r, 'zset', '0', '10')
    assertEquals(small.cursor, '0')
    // Pairs stay together; the order is the scan core's.
    const pairs = new Map<string, string>()
    for (let i = 0; i < small.items.length; i += 2) {
        pairs.set(small.items[i], small.items[i + 1])
    }
    assertEquals(small.items.length, 4)
    assertEquals(
        pairs,
        new Map([['a', '1800000300'], ['b', '7']]),
        'an integral score is its digits — no decimal point, no exponent',
    )
    // The live sorted set, as every other ZSET arm reads it: a key whose
    // TTL has passed answers like an absent one.
    await r.command('EXPIRE', 'zset', '10')
    r.setTime(1_010)
    assertEquals(await zscan(r, 'zset', '0', '10'), { cursor: '0', items: [] })
    r.assertNoRejections()
    // A fractional score is refused, not formatted as JavaScript would: its
    // exact digits on a real broker are not modelled.
    await r.command('ZADD', 'fraction', '1.5', 'x')
    assertThrows(
        () => run(r, 'ZSCAN', 'fraction', '0', 'COUNT', '10'),
        Error,
        'unmodelled score format',
    )
    assertThrows(() => r.assertNoRejections(), Error, 'refused 1 command(s)')
})

Deno.test('#359 ZSCAN walks a larger sorted set on the shared scan core: each pair once per iteration, in FakeRedis.scanOrder', async () => {
    const r = new FakeRedis()
    const seeded = Array.from({ length: 300 }, (_, i) => `m${i}`)
    for (const [i, m] of seeded.entries()) {
        await r.command('ZADD', 'zset', String(1_000 + i), m)
    }
    const seen = new Map<string, string>()
    const order: string[] = []
    let cursor = '0'
    let calls = 0
    do {
        const page = await zscan(r, 'zset', cursor, '10')
        calls++
        for (let i = 0; i < page.items.length; i += 2) {
            const member = page.items[i]
            assert(
                FakeRedis.scanSlot(member) >= Number(cursor),
                `${member} lies at or ahead of cursor ${cursor}`,
            )
            assert(!seen.has(member), `${member} returned twice`)
            seen.set(member, page.items[i + 1])
            order.push(member)
        }
        cursor = page.cursor
    } while (cursor !== '0')
    assert(calls > 1, 'a sorted set larger than COUNT is paged')
    assertEquals(order, [...seeded].sort(FakeRedis.scanOrder))
    assertEquals(
        seen,
        new Map(seeded.map((m, i) => [m, String(1_000 + i)])),
    )
    r.assertNoRejections()
})
