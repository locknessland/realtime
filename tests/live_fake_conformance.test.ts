/**
 * @fileoverview #285 — the fake Redis, differentially checked against a real one.
 *
 * **Why a differential suite and not more hand-written assertions.** #276
 * shipped a security fix that was wrong twice with a green suite, both times
 * because `fake_redis.ts` modelled an option flag wrongly in exactly the place
 * the production code was wrong. Two wrongs agreeing look like a pass. #280
 * audited the fake's arms by hand and found eight divergences — but an audit is
 * a person reading two specifications side by side, and the next arm somebody
 * adds gets no such reading. This is the check that scales: run the same
 * sequence through the fake and through a real broker, and let the broker be
 * the oracle.
 *
 * Gated behind `LOCKNESS_REDIS_INTEGRATION=1`, so it never runs in the default
 * suite:
 *
 * ```bash
 * LOCKNESS_REDIS_PORT=<port> deno task test:redis
 * ```
 *
 * @module @lockness/realtime/tests/live_fake_conformance
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import type { RespReply } from '../../redis/resp.ts'
import { RedisClient } from '../../redis/mod.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
    waitFor,
} from '../../redis/tests/live_broker.ts'
import { FakeRedis } from './fake_redis.ts'
import { KEPT, RedisBroadcastDriver, REFUSED } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import { MAX_ROSTER_READ_SELF_IDS, type Revocation } from '../driver.ts'

/** One command, and whether the fake is expected to refuse it. */
interface Step {
    argv: string[]
    /**
     * The fake is expected to REFUSE this, and real Redis to accept it.
     *
     * A refusal is a deliberate modelling gap, not a divergence — but it has to
     * be declared, or "the fake threw" becomes a way to pass any comparison.
     * The suite still asserts that real Redis accepts it, so a step marked this
     * way is a live record of what the fake does not model.
     */
    fakeRefuses?: string
}

/** A named sequence run against both, step by step. */
interface Sequence {
    name: string
    steps: Step[]
}

/**
 * This run's key namespace, resolved ONCE.
 *
 * `runNamespace()` mints a fresh random namespace on every call, so calling it
 * per key gave every step a key of its own: nothing accumulated, every command
 * ran against an empty key, and the fake and the broker agreed trivially on
 * every reply. The suite passed and detected NOTHING — verified by reverting
 * three real divergences, including the exact `EXPIRE … GT` bug #276 shipped,
 * and watching it stay green through all three.
 */
const NS = runNamespace()

const K = (n: string) => `${NS}:${n}`

/**
 * Replies that may legitimately differ, normalised before comparison.
 *
 * Only `TIME`: the fake's clock is overridable by design and the broker's is
 * the wall clock, so comparing them would assert that two clocks agree rather
 * than that two implementations do. Nothing else is normalised — a normaliser
 * is how a differential suite stops being differential.
 */
function normalise(argv: string[], reply: RespReply): RespReply {
    const cmd = argv[0].toUpperCase()
    if (cmd === 'TIME') {
        // The SHAPE survives and only the digits are masked. Returning a
        // constant WITHOUT reading `reply` — which the first version did — made
        // this comparison `'<time>' === '<time>'`, a constant equal to itself,
        // detecting only that the fake had not thrown. That matters concretely:
        // the revocation reap (`REAP_REVOKED_SCRIPT`) reads
        // `redis.call('TIME')[1]` and every page's scores are judged against
        // it, so a fake TIME reporting MILLISECONDS would make every
        // revocation look expired at once and this suite stay green.
        //
        // The two fields are masked DIFFERENTLY, on purpose. Field 0 keeps its
        // digit count, which is the whole point: a 10-digit value is epoch
        // seconds and a 13-digit one is milliseconds, so the unit error the
        // script would silently inherit is exactly what this preserves.
        // Field 1 is masked to its shape alone, because the fake's clock has
        // second granularity and honestly reports `0` microseconds where the
        // broker reports six digits. That is a declared simplification, not a
        // divergence — and it is declared HERE rather than by widening the
        // field-0 check until both pass.
        if (reply.type !== 'array') return reply
        return {
            type: 'array',
            value: reply.value.map((part, index) =>
                part.type === 'bulk' && /^[0-9]+$/.test(part.value)
                    ? {
                        type: 'bulk' as const,
                        value: index === 0
                            ? `<epoch:${part.value.length} digits>`
                            : '<subsecond>',
                    }
                    : part
            ),
        }
    }
    // Redis does not specify SMEMBERS ordering, and a hash's field order
    // depends on its encoding — a broker with `hash-max-listpack-entries 0`
    // returns hashtable order and would diverge for a reason that has nothing
    // to do with the fake. Sort those two, and two more: the items of an
    // SSCAN or a ZSCAN reply whose cursor is `0` (#358, #359), a collection
    // answered WHOLE, whose order Redis does not specify either. A page with
    // a non-zero cursor is never sorted here — which members a page holds is
    // the hashtable walk itself, and the full-iteration coverage cases
    // compare that as a union instead. This is the one place SCAN replies are
    // compared across backends.
    //
    // A FLAT PAIR LIST is sorted by pair, never element by element (#359
    // A10): `HGETALL` and `ZSCAN` answer `[k, v, k, v, …]`, and sorting the
    // elements on their own would let a field paired with the wrong value —
    // or a member with the wrong score — compare equal.
    if (cmd === 'SMEMBERS' && reply.type === 'array') {
        return { type: 'array', value: sortedItems(reply.value) }
    }
    if (cmd === 'HGETALL' && reply.type === 'array') {
        return { type: 'array', value: sortedPairs(reply.value) }
    }
    if ((cmd === 'SSCAN' || cmd === 'ZSCAN') && reply.type === 'array') {
        const [cursor, items] = reply.value
        if (
            cursor?.type === 'bulk' && cursor.value === '0' &&
            items?.type === 'array'
        ) {
            return {
                type: 'array',
                value: [cursor, {
                    type: 'array',
                    value: cmd === 'ZSCAN'
                        ? sortedPairs(items.value)
                        : sortedItems(items.value),
                }],
            }
        }
    }
    return reply
}

/** A reply's elements in a stable order, for the unordered replies above. */
function sortedItems(items: readonly RespReply[]): RespReply[] {
    return [...items].sort((a, b) =>
        JSON.stringify(a) < JSON.stringify(b) ? -1 : 1
    )
}

/**
 * A flat `[k, v, k, v, …]` reply in a stable order, each pair kept together
 * (#359 A10) — the one pair-list rule, for `HGETALL` and a whole `ZSCAN`. An
 * odd-length list is returned as it is, so it diverges rather than being
 * repaired into a shape the other backend may not have.
 */
function sortedPairs(items: readonly RespReply[]): RespReply[] {
    if (items.length % 2 !== 0) return [...items]
    const pairs: [RespReply, RespReply][] = []
    for (let i = 0; i < items.length; i += 2) {
        pairs.push([items[i], items[i + 1]])
    }
    return pairs
        .sort((a, b) => JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)
        .flat()
}

/**
 * A script shaped like the release script's `mine == false` branch: 1 when the
 * `HGET` reply reached Lua as `false`, 0 otherwise (#345).
 */
const HGET_IS_FALSE_SCRIPT = [
    "local v = redis.call('HGET', KEYS[1], ARGV[1])",
    'if v == false then',
    '  return 1',
    'end',
    'return 0',
].join('\n')

const SEQUENCES: Sequence[] = [
    {
        name: 'the holders hash primitives #345 rests on',
        steps: [
            // HSET answers 1 for a NEW field and 0 for an update — the bit
            // the hold script reads as `added`.
            { argv: ['HSET', K('holders'), 'inst-a', '{"a":1}'] },
            { argv: ['HSET', K('holders'), 'inst-a', '{"a":2}'] },
            // The same bit from INSIDE a script, where the hold reads it.
            {
                argv: [
                    'EVAL',
                    "return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])",
                    '1',
                    K('holders'),
                    'inst-b',
                    '{"b":"é ✓"}',
                ],
            },
            {
                argv: [
                    'EVAL',
                    "return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])",
                    '1',
                    K('holders'),
                    'inst-b',
                    '{"b":"é ✓"}',
                ],
            },
            { argv: ['HLEN', K('holders')] },
            // A missing field reaches Lua as `false`; a present one does not.
            {
                argv: [
                    'EVAL',
                    HGET_IS_FALSE_SCRIPT,
                    '1',
                    K('holders'),
                    'nobody',
                ],
            },
            {
                argv: [
                    'EVAL',
                    HGET_IS_FALSE_SCRIPT,
                    '1',
                    K('holders'),
                    'inst-a',
                ],
            },
            { argv: ['HDEL', K('holders'), 'inst-a'] },
            // One field left, so `HRANDFIELD k 1 WITHVALUES` is deterministic:
            // the `[field, value]` shape the promotion indexes with `[2]`.
            { argv: ['HRANDFIELD', K('holders'), '1', 'WITHVALUES'] },
            // The promoted value copied through a script reads back byte for byte.
            {
                argv: [
                    'EVAL',
                    "local p = redis.call('HRANDFIELD', KEYS[1], 1, 'WITHVALUES')\n" +
                    "redis.call('HSET', KEYS[2], ARGV[1], p[2])\n" +
                    'return 0',
                    '2',
                    K('holders'),
                    K('shown'),
                    '7',
                ],
            },
            { argv: ['HGET', K('shown'), '7'] },
            // An emptied hash no longer exists — no stale holders key.
            { argv: ['HDEL', K('holders'), 'inst-b'] },
            { argv: ['EXISTS', K('holders')] },
        ],
    },
    {
        name: 'the revocation index triple #276 is built on',
        steps: [
            { argv: ['ZADD', K('rev'), '100', 'c1'] },
            { argv: ['ZADD', K('rev'), '200', 'c2', '300', 'c3'] },
            // GT must refuse to lower a score, and accept raising one.
            { argv: ['ZADD', K('rev'), 'GT', '50', 'c1'] },
            // THE OBSERVATION POINT. Without it the two GT calls leave an
            // identical store and identical replies whether GT is honoured or
            // ignored: ZADD answers the ADDED count, which is 0 either way, and
            // the second call raises c1 to 150 from either 100 or 50. This query
            // is empty when GT refused the lowering and returns c1 when it did
            // not. The option #276's whole guarantee rests on was otherwise
            // unpinned repo-wide.
            { argv: ['ZRANGEBYSCORE', K('rev'), '50', '50'] },
            { argv: ['ZADD', K('rev'), 'GT', '150', 'c1'] },
            { argv: ['ZRANGEBYSCORE', K('rev'), '150', '150'] },
            { argv: ['ZRANGEBYSCORE', K('rev'), '-inf', '+inf'] },
            { argv: ['ZRANGEBYSCORE', K('rev'), '(150', '300'] },
            { argv: ['ZREMRANGEBYSCORE', K('rev'), '-inf', '200'] },
            { argv: ['ZRANGEBYSCORE', K('rev'), '-inf', '+inf'] },
            // The reap empties it; Redis then drops the key entirely.
            { argv: ['ZREMRANGEBYSCORE', K('rev'), '-inf', '+inf'] },
            { argv: ['EXISTS', K('rev')] },
            { argv: ['DEL', K('rev')] },
        ],
    },
    {
        name: 'ZREM — the arm clear-on-apply added (#332)',
        steps: [
            { argv: ['ZADD', K('zrem'), '100', 'c1'] },
            { argv: ['ZADD', K('zrem'), '100', 'c1 presence-room'] },
            { argv: ['ZADD', K('zrem'), '100', 'c1 private-orders'] },
            // Removing the CHANNEL record must leave the connection record and
            // the other channel's standing — three members, one removed.
            { argv: ['ZREM', K('zrem'), 'c1 presence-room'] },
            { argv: ['ZRANGEBYSCORE', K('zrem'), '-inf', '+inf'] },
            // Removing what is already gone answers 0 rather than erroring:
            // both the local apply and the reconcile can reach a clear, and the
            // second must be a no-op rather than a fault somebody logs.
            { argv: ['ZREM', K('zrem'), 'c1 presence-room'] },
            { argv: ['ZREM', K('zrem'), 'never-existed'] },
            // Multi-member form, and then the key must DISAPPEAR when its last
            // element goes — the property the fake did not have for its other
            // zset arms until it was found the hard way.
            { argv: ['ZREM', K('zrem'), 'c1', 'c1 private-orders'] },
            { argv: ['EXISTS', K('zrem')] },
            { argv: ['DEL', K('zrem')] },
        ],
    },
    {
        name:
            'a score tie, which whole-second revocation scores produce routinely',
        steps: [
            {
                argv: [
                    'ZADD',
                    K('tie'),
                    '5',
                    'zebra',
                    '5',
                    'alpha',
                    '5',
                    'mid',
                ],
            },
            { argv: ['ZRANGEBYSCORE', K('tie'), '-inf', '+inf'] },
            { argv: ['DEL', K('tie')] },
        ],
    },
    {
        name: 'EXPIRE option flags — the exact pair #276 got backwards',
        steps: [
            { argv: ['ZADD', K('ttl'), '1', 'm'] },
            // GT against a key with NO TTL: Redis treats absent as infinite and
            // REFUSES. Arming it here is what made an inert guard look live.
            { argv: ['EXPIRE', K('ttl'), '100', 'GT'] },
            { argv: ['EXPIRE', K('ttl'), '100', 'NX'] },
            // NX against a key that now HAS one.
            { argv: ['EXPIRE', K('ttl'), '200', 'NX'] },
            { argv: ['EXPIRE', K('ttl'), '200', 'GT'] },
            { argv: ['EXPIRE', K('ttl'), '50', 'GT'] },
            // And on a key that does not exist at all.
            { argv: ['EXPIRE', K('ghost'), '100'] },
            { argv: ['EXISTS', K('ghost')] },
            { argv: ['DEL', K('ttl')] },
        ],
    },
    {
        name: 'the roster hash and the owned/instances sets',
        steps: [
            { argv: ['HSET', K('roster'), 'a', '1'] },
            { argv: ['HSET', K('roster'), 'b', '2', 'c', '3'] },
            { argv: ['HSET', K('roster'), 'a', '9'] },
            { argv: ['HGETALL', K('roster')] },
            { argv: ['HDEL', K('roster'), 'a', 'b', 'absent'] },
            { argv: ['HGETALL', K('roster')] },
            { argv: ['HDEL', K('roster'), 'c'] },
            { argv: ['EXISTS', K('roster')] },
            { argv: ['SADD', K('owned'), 'x'] },
            { argv: ['SADD', K('owned'), 'y', 'z'] },
            { argv: ['SREM', K('owned'), 'x', 'absent'] },
            { argv: ['SREM', K('owned'), 'y', 'z'] },
            { argv: ['EXISTS', K('owned')] },
        ],
    },
    {
        name: 'the liveness string, and multi-key DEL/EXISTS',
        steps: [
            { argv: ['SET', K('alive'), 'up', 'EX', '30'] },
            { argv: ['SET', K('plain'), 'v'] },
            { argv: ['EXISTS', K('alive'), K('plain'), K('absent')] },
            { argv: ['DEL', K('alive'), K('plain'), K('absent')] },
            { argv: ['EXISTS', K('alive'), K('plain')] },
        ],
    },
    {
        // A DECLARED GAP DESYNCHRONISES THE TWO STORES, so it must be the last
        // step touching its key. The broker applies the command and the fake
        // refuses it, so every later step on that key diverges for a reason
        // that has nothing to do with the arm under test. Found the hard way:
        // a trailing `DEL` here reported `real: 1, fake: 0` and read exactly
        // like a fake defect.
        //
        // Each key is used once and left for `teardown` to reap.
        name: 'options the fake deliberately does not model',
        steps: [
            {
                argv: ['SET', K('nx'), 'v', 'NX'],
                fakeRefuses: 'unmodelled SET option',
            },
            {
                argv: ['SET', K('xx-absent'), 'v', 'XX'],
                fakeRefuses: 'unmodelled SET option',
            },
            {
                argv: ['SET', K('keepttl'), 'v', 'KEEPTTL'],
                fakeRefuses: 'unmodelled SET option',
            },
            {
                argv: [
                    'ZRANGEBYSCORE',
                    K('rev2'),
                    '-inf',
                    '+inf',
                    'WITHSCORES',
                ],
                fakeRefuses: 'unmodelled ZRANGEBYSCORE option',
            },
            {
                argv: ['ZADD', K('zflags'), 'NX', '1', 'm'],
                fakeRefuses: 'unmodelled ZADD option',
            },
            {
                // The `default:` arm, pinned against a broker that accepts it.
                argv: ['INCR', K('counter')],
                fakeRefuses: "unmodelled command 'INCR'",
            },
            {
                // TTL is not a modelled arm at all, and saying so here keeps
                // the boundary visible: the fake stores an expiry it cannot be
                // asked to report, so a unit error in that stored value is
                // unobservable from outside. Declared, not hidden.
                argv: ['TTL', K('rev2')],
                fakeRefuses: "unmodelled command 'TTL'",
            },
        ],
    },
    {
        name:
            'SMEMBERS, which one production call site depends on (the instance-set read)',
        steps: [
            { argv: ['SMEMBERS', K('absent-set')] },
            { argv: ['SADD', K('members'), 'only'] },
            { argv: ['SMEMBERS', K('members')] },
            { argv: ['SADD', K('members'), 'second', 'third'] },
            { argv: ['SMEMBERS', K('members')] },
            { argv: ['DEL', K('members')] },
        ],
    },
    {
        // The reply SHAPE of the sweep's owned-set read (#358): an absent key
        // is `['0', []]`, and a set small enough for one reply is answered
        // whole with cursor `0`. What a multi-page walk returns is the
        // full-iteration coverage case below, compared as a union.
        name: 'SSCAN, the sweep’s paged owned-set read (#358)',
        steps: [
            { argv: ['SSCAN', K('absent-scan'), '0', 'COUNT', '10'] },
            { argv: ['SADD', K('scan'), 'channel-a 7', 'channel-b 8', 'c 9'] },
            { argv: ['SSCAN', K('scan'), '0', 'COUNT', '10'] },
            {
                // A scan its caller does not bound is refused by the fake,
                // never answered at a default — no driver sends it.
                argv: ['SSCAN', K('scan'), '0'],
                fakeRefuses: 'SSCAN without COUNT',
            },
            {
                // MATCH filters the page: an ignored MATCH would hand the
                // caller members a real broker does not.
                argv: ['SSCAN', K('scan'), '0', 'MATCH', 'c*', 'COUNT', '10'],
                fakeRefuses: "unmodelled SSCAN option 'MATCH'",
            },
            { argv: ['DEL', K('scan')] },
        ],
    },
    {
        // The reply SHAPE of the revocation pass's paged index read (#359):
        // an absent key is `['0', []]`, and a sorted set small enough for one
        // reply is answered whole with cursor `0`, as flat `member, score`
        // pairs — compared by pair, so a member carrying the wrong score
        // diverges. The scores are integral, the only kind the driver writes,
        // and read back as plain digits on both.
        name: 'ZSCAN, the revocation pass’s paged index read (#359)',
        steps: [
            { argv: ['ZSCAN', K('absent-zscan'), '0', 'COUNT', '100'] },
            {
                argv: [
                    'ZADD',
                    K('zscan'),
                    '1800000300',
                    'c1',
                    '1800000301',
                    'c2 private-room 5f0c1c8e',
                    '7',
                    'c3',
                ],
            },
            { argv: ['ZSCAN', K('zscan'), '0', 'COUNT', '100'] },
            {
                // A read its caller does not bound is refused by the fake,
                // never answered at a default — no driver sends it.
                argv: ['ZSCAN', K('zscan'), '0'],
                fakeRefuses: 'ZSCAN without COUNT',
            },
            {
                argv: ['ZSCAN', K('zscan'), '0', 'MATCH', 'c*', 'COUNT', '100'],
                fakeRefuses: "unmodelled ZSCAN option 'MATCH'",
            },
            { argv: ['DEL', K('zscan')] },
        ],
    },
    {
        name: 'PUBLISH to nobody, and TIME',
        steps: [
            { argv: ['PUBLISH', K('topic'), 'payload'] },
            { argv: ['TIME'] },
        ],
    },
]

Deno.test({
    name: '#285 the fake answers as a real broker does',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const divergences: string[] = []
        let executed = 0

        try {
            for (const sequence of SEQUENCES) {
                for (const { argv, fakeRefuses } of sequence.steps) {
                    executed++
                    let realReply: RespReply
                    try {
                        realReply = await live.command(...argv)
                    } catch (error) {
                        // A broker-side error used to escape the loop, so the
                        // failure named neither the sequence nor the step.
                        divergences.push(
                            `${sequence.name}: ${argv.join(' ')}\n` +
                                `  real: threw — ${
                                    error instanceof Error
                                        ? error.message
                                        : String(error)
                                }`,
                        )
                        continue
                    }

                    let fakeReply: RespReply | undefined
                    let refusal: string | undefined
                    try {
                        fakeReply = await fake.command(...argv) as RespReply
                    } catch (error) {
                        refusal = error instanceof Error
                            ? error.message
                            : String(error)
                    }

                    if (fakeRefuses !== undefined) {
                        // A declared gap has THREE halves, and the first
                        // version checked only one of them. "Something threw"
                        // passes if the fake refused for an unrelated reason —
                        // an arity check, a different option, a typo in the
                        // key — so the declared reason is matched against the
                        // message. And the broker's acceptance is asserted
                        // rather than assumed, because a gap where BOTH sides
                        // refuse is not a gap at all and should be deleted.
                        if (refusal === undefined) {
                            divergences.push(
                                `${sequence.name}: ${argv.join(' ')}\n` +
                                    `  declared unmodelled (${fakeRefuses}) but the fake ACCEPTED it`,
                            )
                        } else if (!refusal.includes(fakeRefuses)) {
                            divergences.push(
                                `${sequence.name}: ${argv.join(' ')}\n` +
                                    `  declared unmodelled (${fakeRefuses})\n` +
                                    `  but it refused for another reason: ${refusal}`,
                            )
                        }
                        continue
                    }
                    if (refusal !== undefined) {
                        divergences.push(
                            `${sequence.name}: ${argv.join(' ')}\n` +
                                `  real: ${JSON.stringify(realReply)}\n` +
                                `  fake: threw — ${refusal}`,
                        )
                        continue
                    }
                    const a = JSON.stringify(normalise(argv, realReply))
                    const b = JSON.stringify(normalise(argv, fakeReply!))
                    if (a !== b) {
                        divergences.push(
                            `${sequence.name}: ${argv.join(' ')}\n` +
                                `  real: ${a}\n  fake: ${b}`,
                        )
                    }
                }
            }
        } finally {
            await teardown(live, NS)
            await live.close()
        }

        // A FLOOR ON STEPS. `assertEquals(divergences, [])` passes when the
        // loops never execute — an emptied SEQUENCES, an emptied steps array, a
        // refactor that skips. That is the same "passed while detecting
        // nothing" shape this file already shipped once, and an empty-list
        // assertion cannot tell the two apart.
        const planned = SEQUENCES.reduce((n, s) => n + s.steps.length, 0)
        assertEquals(
            executed,
            planned,
            `only ${executed} of ${planned} steps ran`,
        )
        assertEquals(planned >= 45, true, `only ${planned} steps are defined`)

        assertEquals(
            divergences,
            [],
            `\n${divergences.length} divergence(s) between the fake and a real broker:\n\n` +
                `${divergences.join('\n\n')}\n`,
        )
    },
})

Deno.test({
    name: '#285 the revocation scripts agree, fake against broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // **The arm with the largest divergence surface, and the one the step
        // table cannot reach.** `EVAL` is where the fake stops modelling a
        // command and starts reimplementing a LANGUAGE: a 195-line Lua subset
        // in `packages/redis/tests/lua_eval.ts` standing in for a real
        // interpreter. Both #276 revocation scripts run through it, so the
        // whole atomicity guarantee rests on that subset being right.
        //
        // The scripts are module-private and copying them here would be the
        // verbatim-second-copy this package's own brief warns against. So this
        // drives the real production path instead — `markRevocation` and
        // `listRevocations` (one reap `EVAL`, then `ZSCAN` pages) on two
        // drivers, one on each backend — which exercises EVAL through its
        // actual caller and needs no export.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const nothing = { psubscribe: () => {} }

        try {
            const onLive = new RedisBroadcastDriver(live, nothing, {
                prefix: `${NS}-live`,
                revocationTtlSeconds: 300,
            })
            const onFake = new RedisBroadcastDriver(
                { command: fake.command },
                nothing,
                {
                    prefix: `${NS}-fake`,
                    revocationTtlSeconds: 300,
                },
            )

            // Empty on both, before anything is written.
            assertEquals(
                (await onLive.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                (await onFake.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                'an empty revocation index disagreed',
            )

            for (const id of ['c1', 'c2', 'c3']) {
                await onLive.markRevocation?.({ target: id })
                await onFake.markRevocation?.({ target: id })
            }

            assertEquals(
                (await onLive.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                ['c1', 'c2', 'c3'],
                'the live broker did not record the revocations',
            )
            assertEquals(
                (await onFake.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                (await onLive.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                'the fake and the broker disagreed on the live revocation set',
            )

            // **The SCORES, not just the membership.** Comparing the set of
            // ids cannot see a wrong `t`: mutate the Lua subset's `[n]`
            // indexing so `TIME[1]` reads the microseconds field instead of
            // the seconds one, and every score becomes ~300 instead of
            // ~now+300 — while `ZREMRANGEBYSCORE -inf t` still reaps nothing
            // and the `score > t` filter still keeps everything. Identical
            // membership, completely wrong expiry. Measured: that mutant
            // survived until this block existed.
            //
            // This is an INVARIANT check rather than a differential one, and
            // that is the point — it catches the class the diff structurally
            // cannot, because both sides would be wrong in the same way only
            // if the broker were wrong too.
            const now = Math.floor(Date.now() / 1000)
            for (
                const [label, driver] of [
                    ['broker', onLive],
                    ['fake', onFake],
                ] as const
            ) {
                const key = `${NS}-${
                    label === 'broker' ? 'live' : 'fake'
                }__revocations`
                const client = label === 'broker'
                    ? live
                    : { command: fake.command }
                const inWindow = await client.command(
                    'ZRANGEBYSCORE',
                    key,
                    String(now + 300 - 60),
                    String(now + 300 + 60),
                ) as RespReply
                assertEquals(
                    inWindow.type === 'array' ? inWindow.value.length : -1,
                    3,
                    `${label}: revocation scores are not ~now+ttl — a wrong ` +
                        'clock source would keep the membership identical',
                )
                assertEquals(typeof driver.listRevocations, 'function')
            }

            // Re-marking is idempotent on both — GT keeps the later expiry and
            // the member count does not grow.
            await onLive.markRevocation?.({ target: 'c1' })
            await onFake.markRevocation?.({ target: 'c1' })
            assertEquals(
                (await onFake.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                (await onLive.listRevocations?.() ?? []).map((r) => r.target)
                    .sort(),
                'a re-mark diverged',
            )

            // #332/#337: the CHANNEL scope and the exact clear, on both
            // backends. The composite is a plain member string, so the fake's
            // ZSET arms carry it unchanged — but `clearRevocation` needs ZREM,
            // which the fake did not model at all until #332, and an unmodelled
            // command is exactly the divergence this file exists to catch.
            //
            // TWO marks for ONE pair, then a clear of the first (#337). The
            // index semantics the fix rests on are the broker's: `ZADD GT`
            // must ADD the second member rather than update the first, and
            // `ZREM` must remove one exact member. A fake that got either
            // wrong would let a pair-keyed clear pass here.
            const first = '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c01'
            const second = '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c02'
            for (const driver of [onLive, onFake]) {
                for (const id of [first, second]) {
                    await driver.markRevocation?.({
                        target: 'c1',
                        channel: 'presence-room',
                        id,
                    })
                }
            }
            const scoped = (rs: Revocation[]) =>
                rs.map((r) =>
                    r.channel === undefined
                        ? `${r.target}/-`
                        : `${r.target}/${r.channel}/${r.id}`
                ).sort()
            assertEquals(
                scoped(await onFake.listRevocations?.() ?? []),
                scoped(await onLive.listRevocations?.() ?? []),
                'the channel-scoped records round-tripped differently — the ' +
                    'SCOPE and the ID are asserted here, not merely the ' +
                    'target, because a record that lost its channel is applied ' +
                    'as a socket kill and one that lost its id cannot be cleared',
            )
            // Only c1's records: earlier steps in this test mark other targets.
            const c1 = async () =>
                scoped(await onLive.listRevocations?.() ?? []).filter((r) =>
                    r.startsWith('c1/')
                )
            assertEquals(
                await c1(),
                [
                    'c1/-',
                    `c1/presence-room/${first}`,
                    `c1/presence-room/${second}`,
                ],
                'two revocations of one pair are two records on the broker',
            )

            for (const driver of [onLive, onFake]) {
                await driver.clearRevocation?.({
                    target: 'c1',
                    channel: 'presence-room',
                    id: first,
                })
            }
            assertEquals(
                scoped(await onFake.listRevocations?.() ?? []),
                scoped(await onLive.listRevocations?.() ?? []),
                'clearing one record diverged',
            )
            assertEquals(
                await c1(),
                ['c1/-', `c1/presence-room/${second}`],
                'the clear removed EXACTLY its own id: the newer record for the ' +
                    "same pair and c1's connection-scoped record both survive",
            )
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

Deno.test({
    name: '#323 the roster scripts agree, fake against broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // The same argument as the revocation arm, for the two scripts #323
        // added. `holdMember` and `releaseMember` each became one `EVAL`, and the
        // whole atomicity guarantee now rests on the fake's Lua subset agreeing
        // with a real interpreter about two multi-key scripts — the file's
        // first. Nothing else checks that.
        //
        // **What this arm can and cannot see.** It compares the OBSERVABLE
        // roster through the production path on both backends, which is what
        // the fake could get wrong. It cannot see cross-slot behaviour: a
        // single-node broker accepts a multi-key `EVAL` that Redis Cluster would
        // refuse, and that limitation is stated on HOLD_MEMBER_SCRIPT rather
        // than pretended away here.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const nothing = { psubscribe: () => {} }

        try {
            const onLive = new RedisBroadcastDriver(live, nothing, {
                prefix: `${NS}-live-roster`,
            })
            const onFake = new RedisBroadcastDriver(
                { command: fake.command },
                nothing,
                { prefix: `${NS}-fake-roster` },
            )
            const ids = (m: { id: string | number }[]) =>
                m.map((x) => String(x.id)).sort()
            const both = async (
                run: (d: RedisBroadcastDriver) => Promise<void>,
            ) => {
                await run(onLive)
                await run(onFake)
            }

            try {
                await both((d) =>
                    d.holdMember('presence-room', {
                        id: 'u1',
                        info: { name: 'Ada' },
                    }).then(() => {})
                )
                await both((d) =>
                    d.holdMember('presence-room', { id: 'u2' }).then(() => {})
                )

                assertEquals(
                    ids(
                        (await onLive.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    ['u1', 'u2'],
                    'the live broker did not record the roster',
                )
                assertEquals(
                    ids(
                        (await onFake.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    ids(
                        (await onLive.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    'the fake and the broker disagreed on the roster after adds',
                )

                await both((d) =>
                    d.releaseMember('presence-room', 'u1').then(() => {})
                )
                assertEquals(
                    ids(
                        (await onLive.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    ['u2'],
                    'the live broker did not remove the member',
                )
                assertEquals(
                    ids(
                        (await onFake.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    ids(
                        (await onLive.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    'the fake and the broker disagreed after removals',
                )

                // Idempotence, on both. The manager's best-effort reclaim after
                // a failed join issues a removal for a member that may never
                // have been written, so a second remove must be a no-op rather
                // than an error on either backend.
                await both((d) =>
                    d.releaseMember('presence-room', 'u1').then(() => {})
                )
                assertEquals(
                    ids(
                        (await onFake.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    ids(
                        (await onLive.readRoster!('presence-room', 1_000, []))
                            .members,
                    ),
                    'a repeated removal diverged',
                )
            } finally {
                await onLive.close()
                await onFake.close()
            }
        } finally {
            await teardown(live, NS)
        }
    },
})

Deno.test({
    name: '#341 the bounded roster read holds its contract on a real broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // `READ_ROSTER_SCRIPT` is the one read a presence subscribe issues, and
        // three of its properties rest on broker behaviour the fake only
        // imitates: `HRANDFIELD`'s count semantics, `HMGET`'s refusal of zero
        // fields, and the order a whole small hash comes back in. Each row
        // below runs the production script on a real interpreter.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const nothing = { psubscribe: () => {} }
        let commands = 0
        const counting = {
            command: (...argv: string[]) => {
                commands++
                return live.command(...argv)
            },
        }
        const prefix = `${NS}-live-bounded`
        const ROOM = 'presence-room'
        const key = `${prefix}__presence:${ROOM}`
        const driver = new RedisBroadcastDriver(counting, nothing, { prefix })
        const ids = (members: { id: string | number }[]) =>
            members.map((m) => String(m.id))

        try {
            // Seven members, each carrying its own id in `info`, so a self
            // matched to the wrong entry is visible (S3).
            const seeded = ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7']
            for (const id of seeded) {
                await driver.holdMember(ROOM, { id, info: { of: id } })
            }
            const hlen = await live.command('HLEN', key) as RespReply
            assertEquals(hlen, { type: 'integer', value: 7 }, 'the fixture')

            // min(K, N) DISTINCT members, and `total === HLEN`.
            const sampled = await driver.readRoster(ROOM, 3, [])
            assertEquals(sampled.members.length, 3, 'min(limit, total)')
            assertEquals(
                new Set(ids(sampled.members)).size,
                3,
                'a positive count never repeats a member',
            )
            for (const member of sampled.members) {
                assert(seeded.includes(String(member.id)), 'a stored member')
            }
            assertEquals(
                sampled.total,
                7,
                '`total` is HLEN, read in the script',
            )

            // A room at or below the limit is the WHOLE hash in HGETALL order
            // (SC-002). If this row fails, the script owes an `HGETALL` branch
            // when `HLEN <= limit` — inside the same EVAL (S5).
            const hash = await live.command('HGETALL', key) as RespReply
            const hashOrder = hash.type === 'array'
                ? hash.value.filter((_, i) => i % 2 === 0).map((field) =>
                    field.type === 'bulk' ? field.value : ''
                )
                : []
            const whole = await driver.readRoster(ROOM, 10, [])
            assertEquals(hashOrder.length, 7)
            assertEquals(
                ids(whole.members),
                hashOrder,
                'a room that fits comes back whole, in the hash order',
            )
            assertEquals(whole.total, 7)

            // Zero self ids is a valid read on a real `HMGET` (A1): the script
            // pads with `''`, which no member id can be.
            assertEquals((await driver.readRoster(ROOM, 1, [])).selves, [])

            // An ABSENT id between two present ones yields only the present
            // entries, each matched to its own id (S3).
            const window = await driver.readRoster(ROOM, 1, [
                'u2',
                'nobody',
                'u6',
            ])
            assertEquals(
                window.selves.map((m) => [String(m.id), m.info]).sort(),
                [['u2', { of: 'u2' }], ['u6', { of: 'u6' }]],
                'a nil element must not shift a later self onto another entry',
            )
            for (const self of window.selves) {
                assertEquals(
                    'owner' in self,
                    false,
                    'the stored owner never leaves the driver (S4)',
                )
            }

            // Invalid inputs throw BEFORE any command reaches the broker (S2).
            const tooMany = Array.from(
                { length: MAX_ROSTER_READ_SELF_IDS + 1 },
                (_, i) => `id${i}`,
            )
            for (
                const [label, limit, selves] of [
                    ['limit -1', -1, []],
                    ['limit 0', 0, []],
                    ['limit 1.5', 1.5, []],
                    ['MAX + 1 self ids', 3, tooMany],
                ] as const
            ) {
                const before = commands
                await assertRejects(
                    () => driver.readRoster(ROOM, limit, selves),
                    Error,
                    'readRoster',
                    `${label} was not refused`,
                )
                assertEquals(commands, before, `${label} reached the broker`)
            }
        } finally {
            await driver.close()
            await teardown(live, NS)
            await live.close()
        }
    },
})

Deno.test({
    name:
        '#341 a read carrying MAX_ROSTER_READ_SELF_IDS self ids agrees, fake against broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // The cap is the one place the script's `unpack(ARGV, 2)` spreads its
        // largest argument list: the `''` padding plus 1 000 ids. The fake's
        // Lua subset and a real interpreter must agree on that call — a stack
        // limit or an arity quirk would be invisible on a handful of ids.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const nothing = { psubscribe: () => {} }
        const onLive = new RedisBroadcastDriver(live, nothing, {
            prefix: `${NS}-live-selves`,
        })
        const onFake = new RedisBroadcastDriver(
            { command: fake.command },
            nothing,
            { prefix: `${NS}-fake-selves` },
        )
        const ROOM = 'presence-room'
        // 600 held, 400 absent: both the hit and the nil path at the cap.
        const HELD = 600
        const wanted = Array.from(
            { length: MAX_ROSTER_READ_SELF_IDS },
            (_, i) => `u${i}`,
        )
        const sorted = (members: readonly { id: string | number }[]) =>
            members.map((m) => String(m.id)).sort()

        try {
            for (const driver of [onLive, onFake]) {
                for (let i = 0; i < HELD; i++) {
                    await driver.holdMember(ROOM, {
                        id: `u${i}`,
                        info: { of: i },
                    })
                }
            }
            const fromLive = await onLive.readRoster(ROOM, 1, wanted)
            const fromFake = await onFake.readRoster(ROOM, 1, wanted)

            assertEquals(fromLive.total, HELD, 'the broker holds the fixture')
            assertEquals(fromLive.selves.length, HELD, 'every held id, once')
            assertEquals(fromFake.total, fromLive.total)
            assertEquals(
                sorted(fromFake.selves),
                sorted(fromLive.selves),
                'the fake and the broker return the same selves at the cap',
            )
            for (const self of fromLive.selves) {
                assertEquals(
                    self.info,
                    { of: Number(String(self.id).slice(1)) },
                    'each self is its own entry',
                )
            }
            fake.assertNoRejections()
        } finally {
            await onLive.close()
            await onFake.close()
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * Two roster drivers per backend, sharing that backend (#345). The sweep timings
 * are short so a lapse fits a live run; the fake is driven on the same clock.
 */
function holderPair(
    port: { command: (...args: string[]) => Promise<unknown> },
    prefix: string,
) {
    const nothing = { psubscribe: () => {} }
    const make = () =>
        new RedisBroadcastDriver(port as RedisClient, nothing, {
            prefix,
            presence: {
                livenessTtlSeconds: 1,
                heartbeatIntervalMs: 250,
                reconcileIntervalMs: 400,
            },
        })
    return { a: make(), b: make() }
}

Deno.test({
    name:
        '#345 holds and releases agree with a real broker (W1, W3, W5, W8, W11, invariant)',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const CH = 'presence-room'
        const observe = async (
            port: { command: (...args: string[]) => Promise<unknown> },
            prefix: string,
        ) => {
            const { a, b } = holderPair(port, prefix)
            const steps: unknown[] = []
            const slot = async (id: number) => {
                const field = await port.command(
                    'HGET',
                    `${prefix}__presence:${CH}`,
                    String(id),
                ) as RespReply
                const holders = await port.command(
                    'HLEN',
                    `${prefix}__holders:${CH} ${id}`,
                ) as RespReply
                const exists = await port.command(
                    'EXISTS',
                    `${prefix}__holders:${CH} ${id}`,
                ) as RespReply
                const n = holders.type === 'integer' ? holders.value : -1
                assertEquals(
                    field.type !== 'nil',
                    n >= 1,
                    `${prefix}: presence field exists iff holders >= 1`,
                )
                return { field: field.type, holders: n, exists }
            }
            const infos = async () =>
                (await a.readRoster(CH, 1_000, [])).members.map((m) => [
                    m.id,
                    m.info,
                ])
            try {
                const ada = (from: string) => ({ id: 7, info: { from } })
                steps.push(await a.holdMember(CH, ada('A'))) // W11 hold → true
                steps.push(await a.holdMember(CH, ada('A'))) // hold again → false
                steps.push(await b.holdMember(CH, ada('B'))) // B shown now
                steps.push(await slot(7))
                steps.push(await infos()) // W5: exact info with 2 holders
                steps.push(await b.releaseMember(CH, 7)) // shown == mine → promote
                steps.push(await infos()) // A's info, copied byte for byte
                steps.push(await slot(7))
                steps.push(await a.holdMember(CH, { id: 8, info: {} }))
                steps.push(await a.releaseMember(CH, 8)) // W8: 7 untouched
                steps.push((await a.readRoster(CH, 1_000, [])).total)
                steps.push(await b.releaseMember(CH, 7)) // non-holder → false
                steps.push(await a.releaseMember(CH, 7)) // last → true (W3)
                steps.push(await slot(7))
                steps.push((await a.readRoster(CH, 1_000, [])).total)
                steps.push(await b.releaseMember(CH, 7)) // empty slot, non-holder → false
                steps.push(await slot(7))
            } finally {
                await a.close()
                await b.close()
            }
            return steps
        }
        try {
            const onLive = await observe(live, `${NS}-live-holders`)
            const onFake = await observe(
                { command: fake.command },
                `${NS}-fake-holders`,
            )
            assertEquals(onFake, onLive, 'the fake and the broker disagreed')
            assertEquals(onLive[0], { arrived: true })
            assertEquals(onLive[1], { arrived: false })
            assertEquals(onLive[2], { arrived: false })
            assertEquals(onLive[4], [[7, { from: 'B' }]])
            assertEquals(onLive[5], { gone: false })
            assertEquals(onLive[6], [[7, { from: 'A' }]])
            assertEquals(onLive[10], 1)
            assertEquals(onLive[11], { gone: false })
            assertEquals(onLive[12], { gone: true })
            assertEquals(onLive[13], {
                field: 'nil',
                holders: 0,
                exists: { type: 'integer', value: 0 },
            })
            assertEquals(onLive[14], 0)
            assertEquals(
                onLive[15],
                { gone: false },
                'a non-holder releasing an already-empty slot is not a departure',
            )
            assertEquals(onLive[16], onLive[13], 'and it writes nothing')
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

Deno.test({
    name: '#345 a sweep releases, it does not delete (W2, W6) on a real broker',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const prefix = `${NS}-live-sweep`
        const CH = 'presence-room'
        const { a, b } = holderPair(live, prefix)
        try {
            await b.holdMember(CH, { id: 7, info: { from: 'B' } })
            await a.holdMember(CH, { id: 7, info: { from: 'A' } })
            await a.close()
            // A's liveness key lapses (1 s TTL); B's reconcile sweeps A. Wait
            // on the sweep's own effect — A's hold gone from the holders hash —
            // not on a fixed sleep: the lapse and the next reconcile tick both
            // run on the broker's clock, and a loaded host stretches them.
            const holders = `${prefix}__holders:${CH} 7`
            const deadline = Date.now() + 15_000
            for (;;) {
                const n = await live.command('HLEN', holders) as RespReply
                if (n.type === 'integer' && n.value === 1) break
                if (Date.now() > deadline) {
                    throw new Error(
                        `precondition: B never swept A within 15 s (holders ${
                            JSON.stringify(n)
                        })`,
                    )
                }
                await new Promise((resolve) => setTimeout(resolve, 100))
            }
            const window = await b.readRoster(CH, 1_000, [])
            assertEquals(
                window.members.map((m) => [m.id, m.info]),
                [[7, { from: 'B' }]],
                'W2: a dead holder never removes a slot a live one holds',
            )
            // W6: A, swept while it believed itself live, releases — B untouched.
            assertEquals(await a.releaseMember(CH, 7), { gone: false })
            assertEquals((await b.readRoster(CH, 1_000, [])).total, 1)
        } finally {
            await b.close()
            await teardown(live, NS)
            await live.close()
        }
    },
})

Deno.test({
    name:
        '#344 W5 one presence-join and one presence-leave across two managers on a real broker',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const prefix = `${NS}-live-announce`
        const CH = 'presence-room'
        const published: { kind: string; from: string }[] = []
        const nothing = { psubscribe: () => {} }
        const instance = (from: string) => {
            const driver = new RedisBroadcastDriver(live, nothing, {
                prefix,
                control: { secret: 'deployment-secret-with-enough-entropy' },
            })
            const publish = driver.publishControl.bind(driver)
            driver.publishControl = (control) => {
                published.push({ kind: control.kind, from })
                return publish(control)
            }
            const manager = new ChannelManager<{ id: number }>({
                driver,
                authorize: (identity) => identity ? { id: identity.id } : false,
            })
            return { driver, manager }
        }
        const connection = (id: string) => ({
            id,
            identity: { id: 7 },
            metadata: {},
            send: () => {},
            close: () => {},
        })
        const A = instance('A')
        const B = instance('B')
        try {
            const a1 = connection('a1')
            const b1 = connection('b1')
            A.manager.register(a1)
            B.manager.register(b1)
            await A.manager.subscribe(a1, CH)
            await B.manager.subscribe(b1, CH)
            assertEquals(
                published.filter((c) => c.kind === 'presence-join').length,
                1,
                'one arrival, one presence-join, whichever instance held first',
            )
            await A.manager.unsubscribe('a1', CH)
            assertEquals(
                published.filter((c) => c.kind === 'presence-leave').length,
                0,
                'A leaves while B holds: nothing',
            )
            await B.manager.unsubscribe('b1', CH)
            assertEquals(
                published.filter((c) => c.kind === 'presence-leave'),
                [{ kind: 'presence-leave', from: 'B' }],
                'the last holder announces the departure',
            )
        } finally {
            await A.driver.close()
            await B.driver.close()
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * Drive one release sequence through a holder pair on `inner` and collect every
 * release script reply (#348 FR-010): B releases while A still holds (0), A
 * releases last (its entry), B releases an empty slot it never held (0).
 *
 * The release script is observed on the wire, through the drivers' own port:
 * the script text stays private to the driver, and what is pinned is the reply
 * it produces on each backend.
 */
async function observeReleases(
    inner: { command: (...args: string[]) => Promise<unknown> },
    prefix: string,
): Promise<{ releases: RespReply[]; stored: RespReply | undefined }> {
    const CH = 'presence-room'
    const releases: RespReply[] = []
    const port = {
        command: async (...args: string[]) => {
            const reply = await inner.command(...args)
            // The release script, by its liveness ask — since #355 the
            // deregistration script `SREM`s too, so `'SREM'` no longer names it.
            if (isReleaseEval(args)) {
                releases.push(reply as RespReply)
            }
            return reply
        },
    }
    const { a, b } = holderPair(port, prefix)
    const holders = `${prefix}__holders:${CH} 7`
    let stored: RespReply | undefined
    try {
        await a.holdMember(CH, { id: 7, info: { from: 'A' } })
        await b.holdMember(CH, { id: 7, info: { from: 'B' } })
        await b.releaseMember(CH, 7) // a holder remains → KEPT (#355)
        const remaining = await inner.command('HGETALL', holders) as RespReply
        assert(
            remaining.type === 'array' && remaining.value.length === 2,
            `${prefix}: precondition, A alone holds 7`,
        )
        stored = remaining.value[1]
        await a.releaseMember(CH, 7) // the last holder → A's entry
        await b.releaseMember(CH, 7) // empty slot, non-holder → 0
    } finally {
        await a.close()
        await b.close()
    }
    return { releases, stored }
}

/**
 * A `{value, kind}` pair as {@link RELEASE_MEMBER_SCRIPT} /
 * {@link DEREGISTER_INSTANCE_SCRIPT} answer since #414 — every outcome but
 * *refused* widened to carry the owned/instances set's prior Redis type
 * alongside its original value.
 */
function pairReply(value: RespReply, kind: string): RespReply {
    return { type: 'array', value: [value, { type: 'bulk', value: kind }] }
}

/** The FR-010 reply contract, asserted on one backend's observed releases. */
function assertReleaseReplies(
    backend: string,
    seen: { releases: RespReply[]; stored: RespReply | undefined },
): void {
    assert(
        seen.stored?.type === 'bulk',
        `${backend}: A's stored entry is a bulk`,
    )
    assertEquals(
        seen.releases,
        [
            pairReply({ type: 'integer', value: KEPT }, 'set'),
            pairReply(seen.stored!, 'set'),
            // B's own owned set is empty and gone by its final, non-holder
            // release (B already released its one entry above) — a real
            // broker drops an emptied set, so `TYPE` reads `none` here, not
            // `set`. Measured against the fake, not assumed.
            pairReply({ type: 'integer', value: 0 }, 'none'),
        ],
        `${backend}: KEPT while a holder remains (#355), the released entry ` +
            'byte for byte when the slot empties, 0 for a non-holder — each ' +
            "paired with the owned set's prior kind (#414)",
    )
}

// The fake half needs no broker, so it runs on every `deno task test`: the
// sweep's departure is only as good as the fake's release reply, and a gated
// row left that unguarded wherever no broker is configured.
Deno.test('#348 FR-010 the release reply is the released entry when the slot empties, KEPT while a holder remains, 0 for a non-holder, on the fake', async () => {
    const fake = new FakeRedis()
    assertReleaseReplies(
        'fake',
        await observeReleases({ command: fake.command }, 'fr010-fake'),
    )
    fake.assertNoRejections()
})

Deno.test({
    name:
        '#348 FR-010 the release reply is the released entry when the slot empties, KEPT while a holder remains, 0 for a non-holder, and the fake agrees with the broker',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        try {
            const onLive = await observeReleases(live, `${NS}-live-release`)
            const onFake = await observeReleases(
                { command: fake.command },
                `${NS}-fake-release`,
            )
            assertReleaseReplies('broker', onLive)
            assertReleaseReplies('fake', onFake)
            // Each entry carries its driver's random instance id, so the two
            // backends agree on the replies' kinds and on each one's equality
            // to its own stored entry — not on the bytes.
            assertEquals(
                onFake.releases.map((r) => r.type),
                onLive.releases.map((r) => r.type),
                'the fake and the broker disagreed on the reply kinds',
            )
            fake.assertNoRejections()
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

// --- #355 WC: the sweep's four release replies and three deregistration replies

/** The release script on the wire: the only one that asks for the liveness check. */
const isReleaseEval = (args: string[]) =>
    args[0] === 'EVAL' && args[1].includes("ARGV[4] == '1'")

/** The deregistration script on the wire: the only one that tests the owned set. */
const isDeregisterEval = (args: string[]) =>
    args[0] === 'EVAL' &&
    args[1].includes("local owns = redis.call('EXISTS', KEYS[3])")

/** One #355 WC scenario: what the dead instance owns, and what lands mid-sweep. */
interface SweepScenario {
    name: string
    /** Slots the dead instance holds ALONE — each release empties it. */
    alone: string[]
    /** Slots the dead instance holds together with the live sweeper — kept. */
    shared: string[]
    /** Owned entries whose hold is already gone — absent. */
    absent: string[]
    /** A broker write issued just before the first `EVAL` of that script. */
    before?: {
        script: 'release' | 'deregister'
        write: 'renew' | 'late-hold'
    }
    /** Which reply ends the sweep: the first refused release, or the deregistration. */
    endsOn: 'release' | 'deregister'
}

/**
 * Plant a dead instance's holds on `inner`, let a live driver sweep it on the
 * real clock, and collect every release and deregistration reply (#355 WC).
 *
 * The scripts stay private to the driver: they are recognised on the wire and
 * their replies are what is pinned, on each backend. A renewal or a late hold
 * is staged as a raw broker write issued just BEFORE the named script's first
 * `EVAL`, so the script sees it — the in-write check is what is exercised.
 */
async function observeSweep(
    inner: { command: (...args: string[]) => Promise<unknown> },
    prefix: string,
    scenario: SweepScenario,
): Promise<{ releases: RespReply[]; deregistrations: RespReply[] }> {
    const CH = 'presence-room'
    const DEAD = 'instance-dead'
    const presence = `${prefix}__presence:${CH}`
    const owned = `${prefix}__owned:${DEAD}`
    const alive = `${prefix}__alive:${DEAD}`
    const instances = `${prefix}__instances`
    const plant = async (field: string, withHold: boolean) => {
        if (withHold) {
            const value = JSON.stringify({
                member: { id: Number(field) },
                owner: DEAD,
            })
            await inner.command(
                'HSET',
                `${prefix}__holders:${CH} ${field}`,
                DEAD,
                value,
            )
            await inner.command('HSET', presence, field, value)
        }
        await inner.command('SADD', owned, `${CH} ${field}`)
        await inner.command('SADD', instances, DEAD)
    }
    for (const field of [...scenario.alone, ...scenario.shared]) {
        await plant(field, true)
    }
    for (const field of scenario.absent) await plant(field, false)

    const releases: RespReply[] = []
    const deregistrations: RespReply[] = []
    let staged = false
    // Recording stops at the reply that ends the scenario. The sweeper runs on
    // the real clock, and its next pass (50 ms later) may start before the
    // wait below notices the end: without this, a pass sweeping the late hold
    // appended its own replies, and the result depended on the scheduler.
    let ended = false
    const port = {
        command: async (...args: string[]) => {
            const script = ended
                ? undefined
                : isReleaseEval(args)
                ? 'release'
                : isDeregisterEval(args)
                ? 'deregister'
                : undefined
            if (script && !staged && scenario.before?.script === script) {
                staged = true
                if (scenario.before.write === 'renew') {
                    await inner.command('SET', alive, '1', 'EX', '30')
                } else {
                    await inner.command('SADD', owned, `${CH} 99`)
                }
            }
            const reply = await inner.command(...args)
            if (script === 'release') releases.push(reply as RespReply)
            if (script === 'deregister') {
                deregistrations.push(reply as RespReply)
            }
            if (script === scenario.endsOn) ended = true
            return reply
        },
    }
    const nothing = { psubscribe: () => {} }
    const sweeper = new RedisBroadcastDriver(port as RedisClient, nothing, {
        prefix,
        presence: {
            livenessTtlSeconds: 1,
            heartbeatIntervalMs: 250,
            reconcileIntervalMs: 50,
        },
    })
    const warn = console.warn
    console.warn = () => {}
    try {
        // A shared slot is the sweeper's too; otherwise any hold starts its pass.
        for (const field of scenario.shared) {
            await sweeper.holdMember(CH, { id: Number(field) })
        }
        if (scenario.shared.length === 0) {
            await sweeper.holdMember('presence-other', { id: 9 })
        }
        await waitFor(
            () => ended,
            `${prefix}: ${scenario.name} — the sweep never reached its end`,
        )
    } finally {
        await sweeper.close()
        console.warn = warn
    }
    return { releases, deregistrations }
}

/** The #355 WC scenarios, and the replies each one must produce. */
const SWEEP_SCENARIOS: {
    scenario: SweepScenario
    /** Release replies, as kinds, order-free (the owned set is unordered). */
    releases: string[]
    deregistrations: RespReply[]
}[] = [
    {
        scenario: {
            name: 'emptied + kept + absent, then deregistered',
            alone: ['7'],
            shared: ['8'],
            absent: ['6'],
            endsOn: 'deregister',
        },
        releases: ['bulk', `integer:${KEPT}`, 'integer:0'],
        deregistrations: [pairReply({ type: 'integer', value: 0 }, 'set')],
    },
    {
        scenario: {
            name: 'a renewal before the release: refused',
            alone: ['7'],
            shared: [],
            absent: [],
            before: { script: 'release', write: 'renew' },
            endsOn: 'release',
        },
        releases: [`integer:${REFUSED}`],
        deregistrations: [],
    },
    {
        scenario: {
            name: 'a renewal before the deregistration: renewed',
            alone: ['7'],
            shared: [],
            absent: [],
            before: { script: 'deregister', write: 'renew' },
            endsOn: 'deregister',
        },
        releases: ['bulk'],
        deregistrations: [
            pairReply({ type: 'integer', value: REFUSED }, 'none'),
        ],
    },
    {
        scenario: {
            name: 'a late hold before the deregistration: kept',
            alone: ['7'],
            shared: [],
            absent: [],
            before: { script: 'deregister', write: 'late-hold' },
            endsOn: 'deregister',
        },
        releases: ['bulk'],
        deregistrations: [pairReply({ type: 'integer', value: KEPT }, 'none')],
    },
]

/**
 * A release reply as a comparable kind: `bulk`, or `integer:<n>` — unwrapping
 * the `{value, ownedKind}` pair every non-refused outcome carries since
 * #414, since only `value` (the first element) decides the kind here.
 */
function replyKind(reply: RespReply): string {
    if (reply.type === 'array' && reply.value.length === 2) {
        return replyKind(reply.value[0])
    }
    if (reply.type === 'integer') return `integer:${reply.value}`
    if (reply.type === 'bulk' && reply.value.length > 0) return 'bulk'
    return `unexpected:${reply.type}`
}

/** Run every WC scenario on one backend and assert its replies. */
async function assertSweepReplies(
    backend: string,
    inner: { command: (...args: string[]) => Promise<unknown> },
    prefix: string,
): Promise<string[][]> {
    const kinds: string[][] = []
    for (const [index, expected] of SWEEP_SCENARIOS.entries()) {
        const { scenario, releases, deregistrations } = expected
        // One namespace per scenario, by index: a prefix is at most 64 bytes.
        const seen = await observeSweep(inner, `${prefix}${index}`, scenario)
        const got = seen.releases.map(replyKind).sort()
        assertEquals(
            got,
            [...releases].sort(),
            `${backend}: ${scenario.name} — release replies`,
        )
        assertEquals(
            seen.deregistrations,
            deregistrations,
            `${backend}: ${scenario.name} — deregistration replies`,
        )
        kinds.push([...got, ...seen.deregistrations.map(replyKind)])
    }
    return kinds
}

// The fake half needs no broker and runs on every `deno task test`, like the
// FR-010 row above: the sweep's count and its "renewed" line are only as good
// as the fake's replies.
Deno.test('#355 WC the four release replies and three deregistration replies of a sweep, on the fake', async () => {
    const fake = new FakeRedis()
    await assertSweepReplies(
        'fake',
        { command: fake.command },
        'wc355-fake-',
    )
    fake.assertNoRejections()
})

Deno.test({
    name:
        '#355 WC the four release replies and three deregistration replies of a sweep, and the fake agrees with the broker',
    ignore: !LIVE_BROKER,
    async fn() {
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        try {
            const onLive = await assertSweepReplies(
                'broker',
                live,
                `${NS}-wc355-live-`,
            )
            const onFake = await assertSweepReplies(
                'fake',
                { command: fake.command },
                `${NS}-wc355-fake-`,
            )
            assertEquals(
                onFake,
                onLive,
                'the fake and the broker disagreed on a sweep reply',
            )
            fake.assertNoRejections()
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * Run `argv` on `client`, answering its reply or the message it was refused
 * with — so a step both sides refuse compares as agreement rather than
 * escaping the loop (#349).
 */
async function replyOrRefusal(
    client: { command(...args: string[]): Promise<unknown> },
    argv: string[],
): Promise<{ reply: unknown } | { refused: string }> {
    try {
        return { reply: await client.command(...argv) }
    } catch (error) {
        return {
            refused: error instanceof Error ? error.message : String(error),
        }
    }
}

Deno.test({
    name:
        '#349 WC SET … EX … GET answers nil, then the previous string, and refuses a hash — the fake agrees with the broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // The heartbeat reads its lapse bit from this reply (#349 FR-001).
        // Not a SEQUENCES entry: `GET` over a hash is refused by BOTH sides
        // (WRONGTYPE), and that runner asserts the broker ACCEPTS every
        // declared gap. Here the agreement asserted is the refusal itself.
        // The expired-key step lives in the fake-only WC test: a live broker
        // cannot be made to wait without a real sleep.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const key = K('wc349-alive')
        const hash = K('wc349-hash')
        try {
            const accepted: string[][] = [
                ['SET', key, '1', 'EX', '30', 'GET'],
                ['SET', key, '1', 'EX', '30', 'GET'],
                ['SET', key, '2', 'GET', 'EX', '30'],
                ['EXISTS', key],
            ]
            // The fake's clock is pinned, so its TTL is exact; the broker's
            // runs, so its TTL may already read one second less.
            fake.setTime(1_000)
            for (const argv of accepted) {
                const real = await replyOrRefusal(live, argv)
                const faked = await replyOrRefusal(fake, argv)
                assert('reply' in real, `broker refused ${argv.join(' ')}`)
                assertEquals(faked, real, argv.join(' '))
                if (!argv.includes('EX')) continue
                // `GET` must not cost the write its TTL, on either side: a
                // liveness key written without one would never lapse, and no
                // peer would ever sweep a crashed instance.
                const ttl = await live.command('TTL', key) as {
                    type?: unknown
                    value?: unknown
                }
                assert(
                    ttl.type === 'integer' &&
                        (ttl.value === 30 || ttl.value === 29),
                    `the broker kept EX 30 after ${argv.join(' ')}: ${
                        JSON.stringify(ttl)
                    }`,
                )
                assertEquals(
                    fake.expiryOf(key),
                    1_030,
                    `the fake kept EX 30 after ${argv.join(' ')}`,
                )
            }
            // A DECLARED GAP, and the last step on its key: the broker
            // accepts a repeated `GET`, the fake refuses it rather than
            // guessing what a second one means. No driver sends it.
            const twice = ['SET', key, '3', 'GET', 'EX', '30', 'GET']
            assert('reply' in await replyOrRefusal(live, twice))
            const fakeTwice = await replyOrRefusal(fake, twice)
            assert(
                'refused' in fakeTwice &&
                    fakeTwice.refused.includes('SET given GET twice'),
                'the fake accepted GET twice',
            )
            // Refused by BOTH, and the hash is left as it was (#349 S4).
            await live.command('HSET', hash, 'f', 'v')
            await fake.command('HSET', hash, 'f', 'v')
            const overHash = ['SET', hash, 'v', 'EX', '30', 'GET']
            const real = await replyOrRefusal(live, overHash)
            const faked = await replyOrRefusal(fake, overHash)
            assert(
                'refused' in real && real.refused.includes('WRONGTYPE'),
                'the broker accepted GET over a hash',
            )
            assert(
                'refused' in faked && faked.refused.includes('WRONGTYPE'),
                'the fake accepted GET over a hash',
            )
            // WRONGTYPE left the hash as it was, on both.
            assertEquals(
                await fake.command('HGET', hash, 'f'),
                await live.command('HGET', hash, 'f'),
            )
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * Walk `key` with `SSCAN … COUNT count` from cursor `0` back to `0`,
 * answering the union of its pages and how many calls it took (#358).
 */
async function scanAll(
    client: { command(...args: string[]): Promise<unknown> },
    key: string,
    count: string,
): Promise<{ members: Set<string>; calls: number }> {
    const members = new Set<string>()
    let cursor = '0'
    let calls = 0
    do {
        const reply = await client.command('SSCAN', key, cursor, 'COUNT', count)
        calls++
        const parts = (reply as { type?: string; value?: RespReply[] }).value
        const [next, items] = parts ?? []
        assert(
            next?.type === 'bulk' && items?.type === 'array',
            `an SSCAN reply is [cursor, array]: ${JSON.stringify(reply)}`,
        )
        for (const item of items.value) {
            assert(item.type === 'bulk', JSON.stringify(item))
            members.add(item.value)
        }
        cursor = next.value
        assert(calls <= 1_000, 'the walk never came back to cursor 0')
    } while (cursor !== '0')
    return { members, calls }
}

Deno.test({
    name:
        '#358 WC a full SSCAN iteration returns every seeded member, on the fake and on the broker — which needed more than one call',
    ignore: !LIVE_BROKER,
    async fn() {
        // The one SCAN guarantee the sweep relies on: a member present for
        // the whole iteration is returned. Page CONTENTS are the hashtable
        // walk and legitimately differ, so the comparison is the union. The
        // seed has more than 128 entries (Redis's default
        // `set-max-listpack-entries`), so the broker holds a real hashtable
        // and pages it — asserted, or this would compare two whole replies.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const key = K('wc358-owned')
        const seeded = Array.from(
            { length: 200 },
            (_, i) => `presence-room-${i % 7} member-${i}`,
        )
        try {
            await live.command('SADD', key, ...seeded)
            await fake.command('SADD', key, ...seeded)
            const onLive = await scanAll(live, key, '10')
            const onFake = await scanAll(fake, key, '10')
            const expected = [...seeded].sort()
            assertEquals([...onLive.members].sort(), expected, 'broker union')
            assertEquals([...onFake.members].sort(), expected, 'fake union')
            assert(
                onLive.calls > 1,
                `the broker answered ${seeded.length} members in one call — ` +
                    'it does not honour COUNT, so the page bound does not ' +
                    'hold on it',
            )
            assert(onFake.calls > 1, 'the fake paged the set')
            fake.assertNoRejections()
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * Walk `key` with `ZSCAN … COUNT count` from cursor `0` back to `0`,
 * answering the union of its `member → score` pairs and how many calls it
 * took (#359).
 */
async function zscanAll(
    client: { command(...args: string[]): Promise<unknown> },
    key: string,
    count: string,
): Promise<{ pairs: Map<string, string>; calls: number }> {
    const pairs = new Map<string, string>()
    let cursor = '0'
    let calls = 0
    do {
        const reply = await client.command('ZSCAN', key, cursor, 'COUNT', count)
        calls++
        const parts = (reply as { type?: string; value?: RespReply[] }).value
        const [next, items] = parts ?? []
        assert(
            next?.type === 'bulk' && items?.type === 'array' &&
                items.value.length % 2 === 0,
            `a ZSCAN reply is [cursor, [member, score, …]]: ${
                JSON.stringify(reply)
            }`,
        )
        for (let i = 0; i < items.value.length; i += 2) {
            const member = items.value[i]
            const score = items.value[i + 1]
            assert(
                member.type === 'bulk' && score.type === 'bulk',
                JSON.stringify([member, score]),
            )
            pairs.set(member.value, score.value)
        }
        cursor = next.value
        assert(calls <= 1_000, 'the walk never came back to cursor 0')
    } while (cursor !== '0')
    return { pairs, calls }
}

Deno.test({
    name:
        '#359 WC a full ZSCAN iteration returns every seeded (member, score) pair, on the fake and on the broker — which needed more than one call',
    ignore: !LIVE_BROKER,
    async fn() {
        // The one SCAN guarantee the revocation pass relies on — a record
        // present for the whole iteration is returned — plus the score each
        // record carries, since the pass reads liveness from it. Page CONTENTS
        // are the hashtable walk and differ, so the comparison is the union of
        // pairs. The seed has more than 128 entries (Redis's default
        // `zset-max-listpack-entries`), so the broker holds a skiplist and
        // pages it — asserted, or this would compare two whole replies. The
        // scores are fixed and far in the future, so both sides agree on them.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const key = K('wc359-index')
        const seeded = new Map(
            Array.from(
                { length: 200 },
                (
                    _,
                    i,
                ) => [`c${i} presence-room-${i % 7} id-${i}`, `${4e9 + i}`],
            ),
        )
        const zadd = [...seeded].flatMap(([member, score]) => [score, member])
        try {
            await live.command('ZADD', key, ...zadd)
            await fake.command('ZADD', key, ...zadd)
            const onLive = await zscanAll(live, key, '10')
            const onFake = await zscanAll(fake, key, '10')
            const byMember = (m: Map<string, string>) =>
                [...m].sort(([a], [b]) => (a < b ? -1 : 1))
            const expected = byMember(seeded)
            assertEquals(byMember(onLive.pairs), expected, 'broker union')
            assertEquals(byMember(onFake.pairs), expected, 'fake union')
            assert(
                onLive.calls > 1,
                `the broker answered ${seeded.size} records in one call — ` +
                    'it does not honour COUNT, so the page bound does not ' +
                    'hold on it',
            )
            assert(onFake.calls > 1, 'the fake paged the sorted set')
            fake.assertNoRejections()
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

Deno.test({
    name:
        '#359 WC a score MARK_REVOKED_SCRIPT writes reads back through ZSCAN as a decimal-integer string, on the fake and on the broker',
    ignore: !LIVE_BROKER,
    async fn() {
        // The pass parses every score it reads with one strict grammar
        // (canonical epoch seconds). A broker that wrote `1.8e9`, or `1800000300.0`,
        // would leave every revocation unenforced — so the format the
        // production write produces is asserted on the broker itself, and the
        // fake is held to it.
        const config = brokerConfig()
        await preflight(config)
        const live = new RedisClient(config)
        const fake = new FakeRedis()
        const nothing = { psubscribe: () => {} }
        try {
            const drivers = [
                ['broker', live, `${NS}-wc359-live`],
                ['fake', { command: fake.command }, `${NS}-wc359-fake`],
            ] as const
            for (const [label, client, prefix] of drivers) {
                const driver = new RedisBroadcastDriver(client, nothing, {
                    prefix,
                    revocationTtlSeconds: 300,
                })
                await driver.markRevocation?.({ target: 'c1' })
                await driver.markRevocation?.({
                    target: 'c1',
                    channel: 'presence-room',
                    id: '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c01',
                })
                const { pairs } = await zscanAll(
                    client,
                    `${prefix}__revocations`,
                    '100',
                )
                assertEquals(pairs.size, 2, `${label}: both records`)
                for (const score of pairs.values()) {
                    assert(
                        /^[1-9][0-9]*$/.test(score),
                        `${label}: the score ${score} is not a decimal integer`,
                    )
                }
            }
            fake.assertNoRejections()
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})

/**
 * #380 WC: the revocation floor one driver leaves behind, read raw on each
 * backend — its members, each score's distance from `now`, and the key's own
 * TTL. Both backends are read through `ZSCAN` (the fake refuses
 * `ZRANGEBYSCORE … WITHSCORES`, whose reply shape it does not model), and the
 * key TTL through `TTL` on the broker and `expiryOf` on the fake.
 */
async function floorState(
    client: { command(...args: string[]): Promise<unknown> },
    key: string,
    keyTtl: () => Promise<number>,
): Promise<{ members: string[]; lapsesIn: number[]; keyTtl: number }> {
    const now = Math.floor(Date.now() / 1000)
    const { pairs } = await zscanAll(client, key, '100')
    return {
        members: [...pairs.keys()].sort(),
        lapsesIn: [...pairs.values()].map((score) => Number(score) - now),
        keyTtl: await keyTtl(),
    }
}

/** Assert one backend's floor: exactly `{ 300 }`, lapsing ~300 s out, TTL ~360 s. */
function assertFloor(
    label: string,
    state: { members: string[]; lapsesIn: number[]; keyTtl: number },
): void {
    assertEquals(state.members, ['300'], `${label}: the floor's members`)
    const [lapse] = state.lapsesIn
    assert(lapse >= 298 && lapse <= 301, `${label}: entry lapses in ${lapse}s`)
    assert(
        state.keyTtl >= 358 && state.keyTtl <= 360,
        `${label}: the floor key's own TTL is ${state.keyTtl}s`,
    )
}

for (
    const [row, drive] of [
        [
            'a: one reap',
            (driver: RedisBroadcastDriver) =>
                driver.listRevocations().then(() => {}),
        ],
        [
            'b: the announce',
            (driver: RedisBroadcastDriver) => {
                driver.onRevocationReconcile(() => {})
                return Promise.resolve()
            },
        ],
    ] as const
) {
    Deno.test({
        name:
            `#380 WC ${row} writes the same revocation floor, fake against broker`,
        ignore: !LIVE_BROKER,
        async fn() {
            const config = brokerConfig()
            await preflight(config)
            const live = new RedisClient(config)
            const fake = new FakeRedis()
            const nothing = { psubscribe: () => {} }
            const onLive = new RedisBroadcastDriver(live, nothing, {
                prefix: `${NS}-wc380-live`,
                revocationTtlSeconds: 300,
            })
            const onFake = new RedisBroadcastDriver(
                { command: fake.command },
                nothing,
                { prefix: `${NS}-wc380-fake`, revocationTtlSeconds: 300 },
            )
            const liveKey = `${NS}-wc380-live__revocation-floor`
            const fakeKey = `${NS}-wc380-fake__revocation-floor`
            try {
                await drive(onLive)
                await drive(onFake)
                await waitFor(
                    async () =>
                        (await zscanAll(live, liveKey, '100')).pairs.size > 0 &&
                        fake.zcard(fakeKey) > 0,
                    'the floor entry lands on both backends',
                )
                const onBroker = await floorState(
                    live,
                    liveKey,
                    async () =>
                        Number(
                            (await live.command('TTL', liveKey) as {
                                value: number
                            }).value,
                        ),
                )
                const onFakeSide = await floorState(
                    { command: fake.command },
                    fakeKey,
                    () =>
                        Promise.resolve(
                            (fake.expiryOf(fakeKey) ?? -1) -
                                Math.floor(Date.now() / 1000),
                        ),
                )
                assertFloor('broker', onBroker)
                assertFloor('fake', onFakeSide)
                assertEquals(
                    onFakeSide.members,
                    onBroker.members,
                    'the fake and the broker disagree on the floor',
                )
            } finally {
                await onLive.close()
                await onFake.close()
                await teardown(live, NS)
                await live.close()
            }
        },
    })
}
