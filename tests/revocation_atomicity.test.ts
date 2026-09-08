/**
 * @fileoverview #276 — a live revocation is never dropped, under any interleaving.
 *
 * The shipped shape stored a revocation as TWO structures — a per-target key
 * with a TTL and a separately-maintained index set — and reaped the index with
 * an `EXISTS` and an `SREM` as separate round-trips. Two interleavings could
 * delete a LIVE entry, and because both recovery triggers enumerate from the
 * index, a deleted entry is invisible to every recovery path the framework has.
 *
 * These tests do not guard those windows; they assert the windows do not exist.
 * One structure and one Redis-evaluated `now` mean there is nothing for a second
 * write to disagree with, and nothing read in an earlier round-trip to act on.
 *
 * Faults are injected at the COMMAND BOUNDARY, never by monkey-patching the
 * driver under test — the convention from `eviction_reconnect.test.ts`.
 *
 * @module @lockness/realtime/tests/revocation_atomicity
 */

import { assert, assertEquals } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`

/** A driver on `redis`, optionally through a command wrapper that injects faults. */
function driverOn(
    redis: FakeRedis,
    command: (...args: string[]) => Promise<unknown> = redis.command,
): RedisBroadcastDriver {
    return new RedisBroadcastDriver({ command }, redis.subscriberFor(), {
        prefix: PREFIX,
        revocationTtlSeconds: 300,
    })
}

Deno.test("#276 race 1: a re-eviction during another instance's reap survives", async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const a = driverOn(redis)
    const b = driverOn(redis)
    try {
        await b.markRevoked('x')
        assertEquals(await a.listRevoked(), ['x'])

        // X's revocation expires…
        redis.setTime(1_400)
        assertEquals(await a.listRevoked(), [], 'expired, as it should be')

        // …and B re-evicts X. Under the old two-structure shape, instance A's
        // in-flight reap could delete the entry B just wrote.
        await b.markRevoked('x')

        // A reaps and enumerates. X must survive: its score is now in the
        // future, and the reap is bounded by the same `now` the enumeration is.
        assertEquals(
            await a.listRevoked(),
            ['x'],
            'the re-eviction survives the concurrent reap',
        )
        assert(redis.zcard(INDEX) === 1, 'and is still stored')
    } finally {
        await a.close()
        await b.close()
    }
})

Deno.test('#276 race 2: recording a revocation has no window for a reap to step into', async () => {
    const redis = new FakeRedis()
    redis.setTime(2_000)
    // Count the commands `markRevoked` issues. The old shape issued SADD then
    // SET — two round-trips, and between them the id was enumerable with no
    // marker, so any concurrent reap deleted it. One operation has no between.
    const issued: string[] = []
    const counting = (...args: string[]): Promise<unknown> => {
        issued.push(args[0].toUpperCase())
        return redis.command(...args)
    }
    const a = driverOn(redis, counting)
    try {
        await a.markRevoked('y')
        assertEquals(
            issued.length,
            1,
            `markRevoked must be ONE operation, issued: ${issued.join(', ')}`,
        )
        assertEquals(issued[0], 'EVAL')
        assertEquals(await a.listRevoked(), ['y'])
    } finally {
        await a.close()
    }
})

Deno.test('#276 FR-011: a re-eviction extends a live revocation, never shortens it', async () => {
    const redis = new FakeRedis()
    redis.setTime(3_000)
    const a = driverOn(redis)
    // A second driver with a SHORTER ttl — a re-eviction through it must not
    // pull the expiry back in.
    const shortTtl = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 10 },
    )
    try {
        await a.markRevoked('z') // expires at 3300
        await shortTtl.markRevoked('z') // would expire at 3010 — must not win

        redis.setTime(3_100)
        assertEquals(
            await a.listRevoked(),
            ['z'],
            'the longer revocation stands; ZADD GT refused to shorten it',
        )
    } finally {
        await a.close()
        await shortTtl.close()
    }
})

Deno.test('#276 FR-012: two instances with different local clocks agree on what is live', async () => {
    const redis = new FakeRedis()
    redis.setTime(4_000)
    const a = driverOn(redis)
    const b = driverOn(redis)
    try {
        await a.markRevoked('w')
        // Both drivers share one Redis, and `now` is read FROM Redis inside the
        // script — so no instance's wall clock participates in the decision.
        // Whatever Date.now() says on either host, both see the same answer.
        assertEquals(await a.listRevoked(), ['w'])
        assertEquals(await b.listRevoked(), ['w'])

        redis.setTime(4_500) // Redis's clock, not an instance's, moves
        assertEquals(await a.listRevoked(), [])
        assertEquals(await b.listRevoked(), [])
    } finally {
        await a.close()
        await b.close()
    }
})

Deno.test('#276 FR-003: the reap releases storage, not merely the returned list', async () => {
    const redis = new FakeRedis()
    redis.setTime(5_000)
    const a = driverOn(redis)
    try {
        await a.markRevoked('p')
        await a.markRevoked('q')
        assertEquals(redis.zcard(INDEX), 2)

        redis.setTime(5_400)
        assertEquals(await a.listRevoked(), [])
        // The assertion that a filtered return value cannot make: the entries
        // are GONE from storage, so the index stays bounded.
        assertEquals(redis.zcard(INDEX), 0, 'the reap released storage')
    } finally {
        await a.close()
    }
})

Deno.test('#276 HIGH-1: a shorter-TTL instance cannot shrink the whole index key under live members', async () => {
    const redis = new FakeRedis()
    redis.setTime(8_000)
    const long = driverOn(redis) // 300s
    const short = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 5 },
    )
    try {
        await long.markRevoked('long-lived') // expires at 8300
        // A different id, written by an instance with a much shorter TTL. Its
        // own member score is short — that is fine and expected. What must NOT
        // happen is its EXPIRE pulling the whole key's lifetime in with it,
        // because Redis would then delete the key and every live member in it.
        await short.markRevoked('short-lived')

        redis.setTime(8_200) // past the short TTL, well inside the long one
        assertEquals(
            await long.listRevoked(),
            ['long-lived'],
            "the long revocation outlived the short instance's EXPIRE",
        )
        assertEquals(redis.zcard(INDEX), 1, 'and the key itself still exists')
    } finally {
        await long.close()
        await short.close()
    }
})

Deno.test('#276 HIGH-2: a failed durability write still revokes locally, then reports', async () => {
    const redis = new FakeRedis()
    redis.setTime(9_000)
    const failing = (...args: string[]): Promise<unknown> =>
        args[0] === 'EVAL'
            ? Promise.reject(new Error('redis down'))
            : redis.command(...args)
    const driver = driverOn(redis, failing)
    let closed = 0
    const manager = new ChannelManager<{ id: number }>({
        driver,
        authorize: () => true,
    })
    const realWarn = console.warn
    const warnings: string[] = []
    console.warn = (...a: unknown[]) => void warnings.push(String(a[0]))
    try {
        manager.register({
            id: 'doomed',
            identity: { id: 1 },
            metadata: {},
            send: () => {},
            close: () => void closed++,
        } as unknown as Connection<{ id: number }>)

        // The durable write fails. The local hard-close needs no Redis at all,
        // so it must still happen — skipping it would fail OPEN on the only
        // revocation path this framework has.
        let reported: unknown
        try {
            await manager.evict('doomed')
        } catch (error) {
            reported = error
        }

        assertEquals(closed, 1, 'the socket was hard-closed despite the fault')
        assert(reported instanceof Error, 'and the caller was told')
        assert(
            warnings.some((w) => w.includes('will NOT be recovered')),
            'the lost durability was warned about, never swallowed',
        )
    } finally {
        console.warn = realWarn
        await driver.close()
    }
})

Deno.test('#276 the index key is actually bounded — it does not live forever', async () => {
    const redis = new FakeRedis()
    redis.setTime(10_000)
    const a = driverOn(redis) // ttl 300
    try {
        await a.markRevoked('bounded')
        // The key must carry an expiry of its own, so an abandoned deployment
        // that stops enumerating still releases it. `EXPIRE … GT` alone cannot
        // establish one: a key with no TTL counts as an INFINITE TTL, so GT
        // always refuses it and the guard is inert.
        const expiry = redis.expiryOf(INDEX)
        assert(
            expiry !== undefined,
            'the index key has no TTL at all — EXPIRE … GT never armed it',
        )
        assert(
            expiry > 10_000 + 300,
            `the key outlives its longest member, got ${expiry}`,
        )
    } finally {
        await a.close()
    }
})

Deno.test('#276 FR-004: concurrent reapers are idempotent and lose nothing', async () => {
    const redis = new FakeRedis()
    redis.setTime(11_000)
    const a = driverOn(redis)
    const b = driverOn(redis)
    const c = driverOn(redis)
    try {
        await a.markRevoked('live-1')
        await a.markRevoked('live-2')
        redis.setTime(11_100)
        await b.markRevoked('live-3') // a later score than the first two

        // Three instances reap and enumerate at once. Each call reaps
        // `score <= t` and returns `score > t` under one `now`, so overlapping
        // passes cannot race each other: none can remove what another is about
        // to return.
        redis.setTime(11_200)
        const [ra, rb, rc] = await Promise.all([
            a.listRevoked(),
            b.listRevoked(),
            c.listRevoked(),
        ])
        assertEquals(ra.sort(), ['live-1', 'live-2', 'live-3'])
        assertEquals(rb.sort(), ['live-1', 'live-2', 'live-3'])
        assertEquals(rc.sort(), ['live-1', 'live-2', 'live-3'])

        // And past every expiry, they agree on nothing remaining — with the
        // storage released exactly once, not once per reaper.
        redis.setTime(11_500)
        const after = await Promise.all([
            a.listRevoked(),
            b.listRevoked(),
            c.listRevoked(),
        ])
        assertEquals(after, [[], [], []])
        assertEquals(redis.zcard(INDEX), 0)
    } finally {
        await a.close()
        await b.close()
        await c.close()
    }
})

Deno.test('#278/SC-001: listRevoked costs ONE command, whatever the count', async () => {
    // The dual read (#276's rollout shim) issued the EVAL, then an SMEMBERS on
    // the legacy index, then one EXISTS PER MEMBER. On a fleet with fifty
    // revoked connections that was fifty-two round trips on every reconcile
    // tick — and the reconcile runs unconditionally, on a dedicated timer, for
    // every deployment class.
    //
    // #278 deleted it. The point of this test is not that the answer is right
    // (other tests cover that) but that the COST is flat: a reader who adds a
    // second read path later fails here rather than in a latency graph.
    for (const revocations of [0, 1, 50]) {
        const redis = new FakeRedis()
        redis.setTime(1_000)
        const issued: string[][] = []
        const driver = new RedisBroadcastDriver(
            {
                command: (...args: string[]) => {
                    issued.push(args)
                    return redis.command(...args)
                },
            },
            redis.subscriberFor(),
            { prefix: PREFIX, revocationTtlSeconds: 300 },
        )
        try {
            for (let i = 0; i < revocations; i++) {
                await driver.markRevoked?.(`conn-${i}`)
            }
            issued.length = 0
            const live = await driver.listRevoked?.() ?? []
            assertEquals(
                live.length,
                revocations,
                'the positive control: the read must actually return the set, ' +
                    'or a one-command count is one command that does nothing',
            )
            assertEquals(
                issued.map((argv) => argv[0]),
                ['EVAL'],
                `listRevoked issued ${issued.length} commands for ` +
                    `${revocations} revocation(s): ${JSON.stringify(issued)}`,
            )
        } finally {
            await driver.close()
        }
    }
})
