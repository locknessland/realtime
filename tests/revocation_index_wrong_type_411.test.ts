/**
 * @fileoverview #411 — a wrong-typed revocation-INDEX key self-heals inside
 * {@link REAP_REVOKED_SCRIPT}'s and {@link MARK_REVOKED_SCRIPT}'s own atomic
 * `EVAL`s, the same way #405 made the revocation floor self-heal, instead of
 * aborting every reap or mark with `WRONGTYPE` and halting revocation
 * enforcement fleet-wide for as long as the key stays corrupt — the gap ADR
 * 013 §4 named as out of scope for #405 and #411 closes.
 *
 * The architect-expert disposition (issue #411) is binding; `packages/realtime/AGENTS.md`'s
 * #405 entry and ADR 013 §2/§4 carry the shared shape, generalised here to the
 * index at both write sites that touch it with a type-sensitive command.
 *
 * **Two different write patterns, two different post-heal shapes.** The mark
 * writes the SAME key it heals inside the SAME atomic `EVAL` (`ZADD` right
 * after `INDEX_HEAL`'s `DEL`), so the index is a `zset` again in the very same
 * round trip. The reap only prunes the index (`ZREMRANGEBYSCORE`) — it never
 * `ZADD`s — so a reap that heals a corrupt index leaves it ABSENT (`none`)
 * until the next mark or a raw write; "usable again", not "a zset again",
 * is what every reap row proves: a plain write against the healed key
 * succeeds and is read back live by the very next pass.
 *
 * Three fake-broker kinds (`string`, `hash`, `set`) prove the heal on
 * `FakeRedis`, on both paths; `list` and `stream` are live-broker only — the
 * fake never models either type, so `WRONGTYPE` from those two kinds is only
 * real against Redis itself (`LOCKNESS_REDIS_INTEGRATION=1`).
 *
 * @module @lockness/realtime/tests/revocation_index_wrong_type_411
 */

import { assert, assertEquals } from '@std/assert'
import {
    RedisBroadcastDriver,
    type RedisSubscriber,
    REVOCATION_FLOOR_ANNOUNCE_FAILED,
    REVOCATION_INDEX_WRONG_TYPE,
} from '../drivers/redis.ts'
import { type CommandFn, FakeRedis } from './fake_redis.ts'
import { settle } from './escape_watcher.ts'
import { isAnnounce } from './revocation_wire.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
} from '../../redis/tests/live_broker.ts'
import { RedisClient } from '../../redis/mod.ts'

const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const FLOOR = `${PREFIX}__revocation-floor`
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

/** One driver over a fresh `FakeRedis`. */
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

// ---------------------------------------------------------------------------
// Fake-broker rows: string / hash / set, on both the reap and the mark path.
// ---------------------------------------------------------------------------

/** One fake-broker row, parameterised on the path and how the index is corrupted. */
function wrongTypeRow(
    path: 'reap' | 'mark',
    kind: string,
    corrupt: (redis: FakeRedis) => Promise<void>,
) {
    Deno.test(
        `#411 a ${kind}-typed revocation index self-heals on the ${path}: the pass completes and one WARN names '${kind}'`,
        async () => {
            const { redis, driver } = fixture()
            const warnings = captureWarnings()
            try {
                await corrupt(redis)

                if (path === 'reap') {
                    // The reap's INDEX_HEAL only DELs — it never ZADDs — so
                    // enforcement not halting is the whole claim here: the
                    // pass completes instead of throwing WRONGTYPE.
                    const found = await driver.listRevocations()
                    assertEquals(
                        found,
                        [],
                        'the pass completed rather than throwing WRONGTYPE',
                    )
                } else {
                    // The mark writes the SAME key it heals inside the SAME
                    // atomic EVAL, so this both proves the heal AND records
                    // conn-1 in one round trip.
                    await driver.markRevocation({ target: 'conn-1' })
                }

                const warns = warnings.having(REVOCATION_INDEX_WRONG_TYPE)
                assertEquals(warns.length, 1, 'exactly one heal WARN')
                assert(
                    warns[0].endsWith(kind),
                    `the WARN names the prior kind: ${warns[0]}`,
                )
                // A literal fragment, hardcoded rather than read off the
                // imported constant: a mutation to REVOCATION_INDEX_WRONG_TYPE's
                // own wording must fail THIS line, not merely re-match itself.
                assert(
                    warns[0].includes('Prior type:'),
                    `the WARN's own wording: ${warns[0]}`,
                )

                // "Usable again", proven by a plain write against the healed
                // key succeeding and being read back live by the next pass —
                // never merely that nothing threw.
                await redis.command(
                    'ZADD',
                    INDEX,
                    String(1_800_000_000 + TTL),
                    'conn-2',
                )
                const after = await driver.listRevocations()
                assertEquals(
                    after.map((r) => r.target).sort(),
                    path === 'mark' ? ['conn-1', 'conn-2'] : ['conn-2'],
                    'a normal write against the healed index is live on the next pass',
                )
                assertEquals(
                    warnings.having(REVOCATION_INDEX_WRONG_TYPE).length,
                    1,
                    'the hot path never WARNs again once the index is healthy',
                )
            } finally {
                warnings.restore()
                await driver.close()
            }
        },
    )
}

for (const path of ['reap', 'mark'] as const) {
    wrongTypeRow(path, 'string', async (redis) => {
        await redis.command('SET', INDEX, 'corrupted-by-a-string-writer')
    })
    wrongTypeRow(path, 'hash', async (redis) => {
        await redis.command('HSET', INDEX, 'field', 'value')
    })
    wrongTypeRow(path, 'set', async (redis) => {
        await redis.command('SADD', INDEX, 'member')
    })
}

Deno.test(
    '#411 an absent or already-healthy index never WARNs (kind is none/zset)',
    async () => {
        const { redis, driver } = fixture()
        const warnings = captureWarnings()
        try {
            await driver.listRevocations() // index absent: kind = 'none'
            await redis.command(
                'ZADD',
                INDEX,
                String(1_800_000_000 + TTL),
                'conn-1',
            )
            await driver.listRevocations() // index now a zset: kind = 'zset'
            await driver.markRevocation({ target: 'conn-2' }) // still a zset
            assertEquals(
                warnings.having(REVOCATION_INDEX_WRONG_TYPE),
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
// The folded LOW: the floor announce refuses a non-bulk reply instead of
// silently skipping the heal check (#411 §2's `decodeAnnounceReply`).
// ---------------------------------------------------------------------------

Deno.test(
    '#411 the floor announce refuses a non-bulk reply instead of silently skipping the heal check, and lands in its existing WARN+retry catch',
    async () => {
        const redis = new FakeRedis()
        redis.setTime(1_800_000_000)
        const command: CommandFn = (...args) =>
            isAnnounce(args, FLOOR)
                // A non-bulk reply a real broker never sends for this script,
                // modelling a protocol-level anomaly: the pre-#411 `asBulk`
                // read it as `undefined` and silently skipped the heal check —
                // no throw, no WARN. Strict decoding must not repeat that.
                ? Promise.resolve({ type: 'integer', value: 1 })
                : redis.command(...args)
        const subscriber: RedisSubscriber = redis.subscriberFor()
        const driver = new RedisBroadcastDriver(
            { command },
            subscriber,
            { prefix: PREFIX, revocationTtlSeconds: TTL },
        )
        const warnings = captureWarnings()
        try {
            driver.onRevocationReconcile(() => {})
            await settle()
            const warns = warnings.having(REVOCATION_FLOOR_ANNOUNCE_FAILED)
            assertEquals(
                warns.length,
                1,
                'the non-bulk reply is treated as a failed announce, never a silent skip',
            )
            assert(
                warns[0].includes('bulk string'),
                `the decode failure names what it expected: ${warns[0]}`,
            )
        } finally {
            warnings.restore()
            await driver.close()
        }
    },
)

// ---------------------------------------------------------------------------
// Live-broker rows: list and stream, on both paths. FakeRedis never models
// either type, so WRONGTYPE against those two kinds is provable only against
// a real broker.
// ---------------------------------------------------------------------------

function liveWrongTypeRow(
    path: 'reap' | 'mark',
    kind: 'list' | 'stream',
    corrupt: (client: RedisClient, index: string) => Promise<void>,
) {
    Deno.test({
        name:
            `#411 (live) a ${kind}-typed revocation index self-heals on the ${path} against a real broker`,
        ignore: !LIVE_BROKER,
        async fn() {
            const config = brokerConfig()
            await preflight(config)
            const live = new RedisClient(config)
            const ns = runNamespace()
            const prefix = `${ns}:rt`
            const index = `${prefix}__revocations`
            const subscriber: RedisSubscriber = { psubscribe: () => {} }
            const driver = new RedisBroadcastDriver(live, subscriber, {
                prefix,
                revocationTtlSeconds: TTL,
            })
            const warnings = captureWarnings()
            try {
                await corrupt(live, index)

                if (path === 'reap') {
                    const found = await driver.listRevocations()
                    assertEquals(found, [])
                } else {
                    await driver.markRevocation({ target: 'conn-1' })
                }

                const warns = warnings.having(REVOCATION_INDEX_WRONG_TYPE)
                assertEquals(warns.length, 1)
                assert(warns[0].endsWith(kind), warns[0])

                const time = await live.command('TIME') as unknown as {
                    value: { value: string }[]
                }
                const t = Number(time.value[0].value)
                await live.command('ZADD', index, String(t + TTL), 'conn-2')
                const after = await driver.listRevocations()
                assertEquals(
                    after.map((r) => r.target).sort(),
                    path === 'mark' ? ['conn-1', 'conn-2'] : ['conn-2'],
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

for (const path of ['reap', 'mark'] as const) {
    liveWrongTypeRow(path, 'list', async (client, index) => {
        await client.command('RPUSH', index, 'a', 'b', 'c')
    })
    liveWrongTypeRow(path, 'stream', async (client, index) => {
        await client.command('XADD', index, '*', 'field', 'value')
    })
}
