/**
 * @fileoverview #405 — a wrong-typed revocation-floor key self-heals inside
 * {@link FLOOR_WRITE}'s own atomic `EVAL`, instead of aborting every reap or
 * announce with `WRONGTYPE` and halting revocation enforcement fleet-wide for
 * as long as the key stays corrupt.
 *
 * The architect-expert disposition (issue #405) is binding; `packages/realtime/AGENTS.md`'s
 * #380 entry and ADR 013 §2/§4 carry the shape. Three fake-broker kinds
 * (`string`, `hash`, `set`) prove the heal on `FakeRedis`; `list` and `stream`
 * are live-broker only — the fake never models either type, so `WRONGTYPE`
 * from those two kinds is only real against Redis itself
 * (`LOCKNESS_REDIS_INTEGRATION=1`).
 *
 * Every fake row asserts the same four things: the pass completes and returns
 * live revocations (enforcement never halts), exactly one
 * {@link REVOCATION_FLOOR_WRONG_TYPE} WARN naming the prior kind, the floor is
 * a `zset` afterward holding this instance's own entry, and a `markRevocation`
 * right after computes a normal (not `MAX_REVOCATION_TTL_SECONDS`) effective
 * TTL — proof the heal did not merely dodge the abort but left the floor
 * usable again in the very same pass.
 *
 * @module @lockness/realtime/tests/revocation_floor_wrong_type_405
 */

import { assert, assertEquals } from '@std/assert'
import {
    MAX_REVOCATION_TTL_SECONDS,
    RedisBroadcastDriver,
    type RedisSubscriber,
    REVOCATION_FLOOR_WRONG_TYPE,
} from '../drivers/redis.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
} from '../../redis/tests/live_broker.ts'
import { RedisClient } from '../../redis/mod.ts'

const PREFIX = 'app:rt'
const FLOOR = `${PREFIX}__revocation-floor`
const INDEX = `${PREFIX}__revocations`
const TTL = 300

/** Captured `console.warn` lines, restorable. */
function captureWarnings() {
    const original = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => {
        lines.push(parts.map(String).join(' '))
    }
    return {
        lines,
        having: (prefix: string) => lines.filter((l) => l.startsWith(prefix)),
        restore: () => void (console.warn = original),
    }
}

/** A raw `ZSCAN` read of the floor, member -> score, by the test only. */
async function floorEntries(
    redis: FakeRedis,
): Promise<Record<string, number>> {
    const reply = await redis.command('ZSCAN', FLOOR, '0', 'COUNT', '1000') as {
        value: [{ value: string }, { value: { value: string }[] }]
    }
    const items = reply.value[1].value
    const out: Record<string, number> = {}
    for (let i = 0; i < items.length; i += 2) {
        out[items[i].value] = Number(items[i + 1].value)
    }
    return out
}

/** One driver over a fresh `FakeRedis`, with the floor pre-corrupted. */
function fixture(command?: CommandFn) {
    const redis = new FakeRedis()
    redis.setTime(1_800_000_000)
    const subscriber: RedisSubscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver(
        { command: command ?? redis.command },
        subscriber,
        { prefix: PREFIX, revocationTtlSeconds: TTL },
    )
    return { redis, driver }
}

/** One fake-broker row, parameterised on how the floor is corrupted. */
function wrongTypeRow(
    kind: string,
    corrupt: (redis: FakeRedis) => Promise<void>,
) {
    Deno.test(
        `#405 a ${kind}-typed revocation floor self-heals: the pass completes, one WARN names '${kind}', the floor is a zset, and a mark right after is not fail-closed`,
        async () => {
            const { redis, driver } = fixture()
            const warnings = captureWarnings()
            try {
                await corrupt(redis)
                // A live, decodable connection-scoped revocation, so "applies
                // revocations" is provable, not merely "did not throw".
                await redis.command(
                    'ZADD',
                    INDEX,
                    String(1_800_000_000 + 300),
                    'conn-1',
                )

                const found = await driver.listRevocations()

                assertEquals(
                    found.map((r) => r.target),
                    ['conn-1'],
                    'the reap and the page read both ran: enforcement did not halt',
                )
                const warns = warnings.having(REVOCATION_FLOOR_WRONG_TYPE)
                assertEquals(warns.length, 1, 'exactly one heal WARN')
                assert(
                    warns[0].endsWith(kind),
                    `the WARN names the prior kind: ${warns[0]}`,
                )
                // A literal fragment, hardcoded rather than read off the
                // imported constant: a mutation to REVOCATION_FLOOR_WRONG_TYPE's
                // own wording must fail THIS line, not merely re-match itself.
                assert(
                    warns[0].includes('Prior type:'),
                    `the WARN's own wording: ${warns[0]}`,
                )
                assertEquals(
                    await redis.command('TYPE', FLOOR),
                    { type: 'simple', value: 'zset' },
                    'the floor is a zset after the heal',
                )
                const entries = await floorEntries(redis)
                assertEquals(
                    entries[String(TTL)],
                    1_800_000_000 + TTL,
                    "this instance's own entry is on the healed floor",
                )

                await driver.markRevocation({ target: 'conn-2' })
                const marked = await floorEntries(redis).then(async () => {
                    const raw = await redis.command(
                        'ZSCAN',
                        INDEX,
                        '0',
                        'COUNT',
                        '1000',
                    ) as {
                        value: [
                            { value: string },
                            { value: { value: string }[] },
                        ]
                    }
                    const items = raw.value[1].value
                    const out: Record<string, number> = {}
                    for (let i = 0; i < items.length; i += 2) {
                        out[items[i].value] = Number(items[i + 1].value)
                    }
                    return out
                })
                assertEquals(
                    marked['conn-2'],
                    1_800_000_000 + TTL,
                    'a mark right after the heal scores at the normal TTL, ' +
                        'not MAX_REVOCATION_TTL_SECONDS — the floor is usable again',
                )
                assert(
                    marked['conn-2'] !==
                        1_800_000_000 + MAX_REVOCATION_TTL_SECONDS,
                    'never the fail-closed maximum once the floor is healed',
                )
            } finally {
                warnings.restore()
                await driver.close()
            }
        },
    )
}

wrongTypeRow('string', async (redis) => {
    await redis.command('SET', FLOOR, 'corrupted-by-a-string-writer')
})
wrongTypeRow('hash', async (redis) => {
    await redis.command('HSET', FLOOR, 'field', 'value')
})
wrongTypeRow('set', async (redis) => {
    await redis.command('SADD', FLOOR, 'member')
})

Deno.test(
    '#405 an absent or already-healthy floor never WARNs (kind is none/zset)',
    async () => {
        const { driver } = fixture()
        const warnings = captureWarnings()
        try {
            await driver.listRevocations() // floor absent: kind = 'none'
            await driver.listRevocations() // floor now a zset: kind = 'zset'
            assertEquals(
                warnings.having(REVOCATION_FLOOR_WRONG_TYPE),
                [],
                'the hot path never WARNs',
            )
        } finally {
            warnings.restore()
            await driver.close()
        }
    },
)

// ---------------------------------------------------------------------------
// Live-broker rows: list and stream. FakeRedis never models either type, so
// WRONGTYPE against those two kinds is provable only against a real broker.
// ---------------------------------------------------------------------------

function liveWrongTypeRow(
    kind: 'list' | 'stream',
    corrupt: (client: RedisClient, floor: string) => Promise<void>,
) {
    Deno.test({
        name:
            `#405 (live) a ${kind}-typed revocation floor self-heals against a real broker`,
        ignore: !LIVE_BROKER,
        async fn() {
            const config = brokerConfig()
            await preflight(config)
            const live = new RedisClient(config)
            const ns = runNamespace()
            const prefix = `${ns}:rt`
            const floor = `${prefix}__revocation-floor`
            const index = `${prefix}__revocations`
            const subscriber: RedisSubscriber = { psubscribe: () => {} }
            const driver = new RedisBroadcastDriver(live, subscriber, {
                prefix,
                revocationTtlSeconds: TTL,
            })
            const warnings = captureWarnings()
            try {
                await corrupt(live, floor)
                const time = await live.command('TIME') as unknown as {
                    value: { value: string }[]
                }
                const t = Number(time.value[0].value)
                await live.command('ZADD', index, String(t + 300), 'conn-1')

                const found = await driver.listRevocations()

                assertEquals(found.map((r) => r.target), ['conn-1'])
                const warns = warnings.having(REVOCATION_FLOOR_WRONG_TYPE)
                assertEquals(warns.length, 1)
                assert(warns[0].endsWith(kind), warns[0])
                assertEquals(
                    await live.command('TYPE', floor),
                    { type: 'simple', value: 'zset' },
                )
            } finally {
                warnings.restore()
                await driver.close()
                await teardown(live, ns)
                await live.close()
            }
        },
    })
}

liveWrongTypeRow('list', async (client, floor) => {
    await client.command('RPUSH', floor, 'a', 'b', 'c')
})
liveWrongTypeRow('stream', async (client, floor) => {
    await client.command('XADD', floor, '*', 'field', 'value')
})
