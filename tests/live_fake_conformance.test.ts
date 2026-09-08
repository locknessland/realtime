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
 * LOCKNESS_REDIS_PORT=6385 deno task test:redis
 * ```
 *
 * @module @lockness/realtime/tests/live_fake_conformance
 */

import { assertEquals } from '@std/assert'
import type { RespReply } from '../../redis/resp.ts'
import { RedisClient } from '../../redis/mod.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
} from '../../redis/tests/live_broker.ts'
import { FakeRedis } from './fake_redis.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'

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
        // LIST_REVOKED_SCRIPT reads `redis.call('TIME')[1]` and compares it
        // against member scores, so a fake TIME reporting MILLISECONDS would
        // make every revocation look live forever and this suite stay green.
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
    // to do with the fake. Sort those two, and only those two.
    if ((cmd === 'SMEMBERS' || cmd === 'HGETALL') && reply.type === 'array') {
        return {
            type: 'array',
            value: [...reply.value].sort((a, b) =>
                JSON.stringify(a) < JSON.stringify(b) ? -1 : 1
            ),
        }
    }
    return reply
}

const SEQUENCES: Sequence[] = [
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
        name: 'SMEMBERS, which three production call sites depend on',
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
        // drives the real production path instead — `markRevoked` and
        // `listRevoked` on two drivers, one on each backend — which exercises
        // EVAL through its actual caller and needs no export.
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
                (await onLive.listRevoked?.() ?? []).sort(),
                (await onFake.listRevoked?.() ?? []).sort(),
                'an empty revocation index disagreed',
            )

            for (const id of ['c1', 'c2', 'c3']) {
                await onLive.markRevoked?.(id)
                await onFake.markRevoked?.(id)
            }

            assertEquals(
                (await onLive.listRevoked?.() ?? []).sort(),
                ['c1', 'c2', 'c3'],
                'the live broker did not record the revocations',
            )
            assertEquals(
                (await onFake.listRevoked?.() ?? []).sort(),
                (await onLive.listRevoked?.() ?? []).sort(),
                'the fake and the broker disagreed on the live revocation set',
            )

            // **The SCORES, not just the membership.** Comparing the set of
            // ids cannot see a wrong `t`: mutate the Lua subset's `[n]`
            // indexing so `TIME[1]` reads the microseconds field instead of
            // the seconds one, and every score becomes ~300 instead of
            // ~now+300 — while `ZREMRANGEBYSCORE -inf t` still reaps nothing
            // and `ZRANGEBYSCORE t +inf` still returns everything. Identical
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
                assertEquals(typeof driver.listRevoked, 'function')
            }

            // Re-marking is idempotent on both — GT keeps the later expiry and
            // the member count does not grow.
            await onLive.markRevoked?.('c1')
            await onFake.markRevoked?.('c1')
            assertEquals(
                (await onFake.listRevoked?.() ?? []).sort(),
                (await onLive.listRevoked?.() ?? []).sort(),
                'a re-mark diverged',
            )
        } finally {
            await teardown(live, NS)
            await live.close()
        }
    },
})
