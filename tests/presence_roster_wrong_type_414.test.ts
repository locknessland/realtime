/**
 * @fileoverview #414 — the presence roster's five keys each get the remedy
 * ADR 016 assigns them, never a blanket self-heal.
 *
 * The architect-expert disposition (issue #414) is binding. Per-key table:
 *
 * - **Presence hash** and **holders hash** FAIL CLOSED — a heal-`DEL` would
 *   silently erase every OTHER member's shown entry (presence) or falsify
 *   the one `arrived`/`gone` signal `HLEN` decides (holders). Every read or
 *   write `HOLD_MEMBER_SCRIPT`, `RELEASE_MEMBER_SCRIPT` and
 *   `READ_ROSTER_SCRIPT` make against either now goes through
 *   `FakeRedis`'s new `#assertHashKey` guard, so a corruption a real broker
 *   would refuse with `WRONGTYPE` is refused here too — before this file,
 *   the fake let every hash command through unconditionally, so a
 *   corrupted presence/holders key read as merely ABSENT, indistinguishable
 *   from one that never existed, and this class of witness had nothing to
 *   observe.
 * - **Owned set** and **instances set** SELF-HEAL, `INDEX_HEAL`-shaped
 *   (`OWNED_HEAL` / `INSTANCES_HEAL`): single-writer-scoped or fully
 *   re-derivable, and neither feeds an announcement.
 * - **Liveness key** is the documented `EXISTS`-only exemption (untested
 *   here: `EXISTS` is never type-sensitive, so no row could exercise it).
 *
 * `HOLD_MEMBER_SCRIPT` also gains an up-front, NO-HEAL presence guard: a
 * bare `HGET` before any write, closing the ordering hazard where the
 * holders `HSET` (the script's old first statement) could commit before an
 * aborting presence `HSET` — a holder entry with no owned/instances entry,
 * the orphan shape ADR 004 §5 already closed for a different cause. The
 * ordering witness below proves the fix directly: it corrupts presence
 * ALONE and asserts holders/owned/instances are UNCHANGED after the throw.
 *
 * Three fake-broker kinds (`string`, `hash`, `set`) prove each self-heal;
 * `list` and `stream` are live-broker only (`LOCKNESS_REDIS_INTEGRATION=1`)
 * — the fake never models either type.
 *
 * @module @lockness/realtime/tests/presence_roster_wrong_type_414
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import {
    INSTANCES_SET_WRONG_TYPE,
    OWNED_SET_WRONG_TYPE,
    RedisBroadcastDriver,
    type RedisSubscriber,
} from '../drivers/redis.ts'
import { FakeRedis } from './fake_redis.ts'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
    teardown,
} from '../../redis/tests/live_broker.ts'
import { RedisClient } from '../../redis/mod.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
const HOLDERS_KEY = (channel: string, id: string | number) =>
    `${PREFIX}__holders:${channel} ${String(id)}`
const OWNED_KEY = (instanceId: string) => `${PREFIX}__owned:${instanceId}`
const INSTANCES_KEY = `${PREFIX}__instances`

type CommandFn = (...args: string[]) => Promise<unknown>

function driver(redis: FakeRedis, command: CommandFn = redis.command) {
    return new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
}

/** The instance id a driver tags its holds with. Test-only read of a private. */
const idOf = (d: RedisBroadcastDriver): string => d['instanceId']

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

/** A fake-broker Redis type other than `none`. */
type FakeKind = 'string' | 'hash' | 'set' | 'zset'

/**
 * Corrupt `key` to `kind` with a raw command.
 *
 * `zset` is included because a set-family key's own native type is `set`,
 * and a hash-family key's own is `hash` — so a row proving a heal, or proving
 * presence/holders stay fail-closed, must pick wrong kinds OUTSIDE that
 * key's own family, or the "corruption" is simply a legitimate write. The
 * three the fake can produce for each family are `string` plus the other
 * two of `hash`/`set`/`zset`.
 *
 * **`DEL`s the key first**, for every kind but `string`. A real broker's
 * `HSET`/`SADD`/`ZADD` each refuse `WRONGTYPE` against a key ALREADY held by
 * another type — the fake now refuses it too (#414's own `#assertHashKey` /
 * `#assertSetKey`) — so "corrupting" a key this test may have already
 * created (a prior hold's presence hash, its owned set) needs the SAME two
 * commands an actual attacker would issue: delete, then write the new type.
 * `SET` needs no `DEL`: it overwrites unconditionally, on the fake as for
 * real.
 */
async function corrupt(
    redis: FakeRedis,
    key: string,
    kind: FakeKind,
): Promise<void> {
    if (kind === 'string') {
        await redis.command('SET', key, 'corrupted-by-414')
        return
    }
    await redis.command('DEL', key)
    if (kind === 'hash') await redis.command('HSET', key, 'field', 'value')
    if (kind === 'set') await redis.command('SADD', key, 'member')
    if (kind === 'zset') await redis.command('ZADD', key, '1', 'member')
}

/** Wrong kinds for a HASH-family key (presence, holders): outside `hash`. */
const HASH_WRONG_KINDS: readonly FakeKind[] = ['string', 'set', 'zset']
/** Wrong kinds for a SET-family key (owned, instances): outside `set`. */
const SET_WRONG_KINDS: readonly FakeKind[] = ['string', 'hash', 'zset']

// ---------------------------------------------------------------------------
// Owned set: self-heals on HOLD and on RELEASE.
// ---------------------------------------------------------------------------

for (const kind of SET_WRONG_KINDS) {
    Deno.test(`#414 a ${kind}-typed owned set self-heals on HOLD_MEMBER_SCRIPT: the hold completes and one WARN names '${kind}'`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        const warnings = captureWarnings()
        try {
            await corrupt(redis, OWNED_KEY(idOf(d)), kind)
            const { arrived } = await d.holdMember(CHANNEL, { id: 7, info: {} })
            assertEquals(
                arrived,
                true,
                'the hold completed rather than throwing',
            )
            const warns = warnings.having(OWNED_SET_WRONG_TYPE)
            assertEquals(warns.length, 1, 'exactly one heal WARN')
            assert(warns[0].endsWith(kind), `names the prior kind: ${warns[0]}`)
            // A literal fragment, hardcoded rather than read off the
            // imported constant: a mutation to the WARN's own wording must
            // fail THIS line, not merely re-match itself.
            assert(
                warns[0].includes('Prior type:'),
                `the WARN's own wording: ${warns[0]}`,
            )

            // "Usable again": a plain SADD against the healed key succeeds,
            // and the hot path never WARNs again.
            await redis.command('SADD', OWNED_KEY(idOf(d)), 'x')
            await d.holdMember(CHANNEL, { id: 8, info: {} })
            assertEquals(
                warnings.having(OWNED_SET_WRONG_TYPE).length,
                1,
                'the hot path never WARNs again once the key is healthy',
            )
        } finally {
            warnings.restore()
            await d.close()
        }
    })

    Deno.test(`#414 a ${kind}-typed owned set self-heals on RELEASE_MEMBER_SCRIPT: the release completes and one WARN names '${kind}'`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        const warnings = captureWarnings()
        try {
            await d.holdMember(CHANNEL, { id: 7, info: {} })
            await corrupt(redis, OWNED_KEY(idOf(d)), kind)
            const { gone } = await d.releaseMember(CHANNEL, 7)
            assertEquals(
                gone,
                true,
                'the release completed rather than throwing',
            )
            const warns = warnings.having(OWNED_SET_WRONG_TYPE)
            assertEquals(warns.length, 1, 'exactly one heal WARN')
            assert(warns[0].endsWith(kind), `names the prior kind: ${warns[0]}`)
            // A literal fragment, hardcoded rather than read off the
            // imported constant: a mutation to the WARN's own wording must
            // fail THIS line, not merely re-match itself.
            assert(
                warns[0].includes('Prior type:'),
                `the WARN's own wording: ${warns[0]}`,
            )
        } finally {
            warnings.restore()
            await d.close()
        }
    })
}

// ---------------------------------------------------------------------------
// Instances set: self-heals on HOLD and on the sweep's deregistration.
// ---------------------------------------------------------------------------

for (const kind of SET_WRONG_KINDS) {
    Deno.test(`#414 a ${kind}-typed instances set self-heals on HOLD_MEMBER_SCRIPT: the hold completes and one WARN names '${kind}'`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        const warnings = captureWarnings()
        try {
            await corrupt(redis, INSTANCES_KEY, kind)
            const { arrived } = await d.holdMember(CHANNEL, { id: 7, info: {} })
            assertEquals(
                arrived,
                true,
                'the hold completed rather than throwing',
            )
            const warns = warnings.having(INSTANCES_SET_WRONG_TYPE)
            assertEquals(warns.length, 1, 'exactly one heal WARN')
            assert(warns[0].endsWith(kind), `names the prior kind: ${warns[0]}`)
            // A literal fragment, hardcoded rather than read off the
            // imported constant: a mutation to the WARN's own wording must
            // fail THIS line, not merely re-match itself.
            assert(
                warns[0].includes('Prior type:'),
                `the WARN's own wording: ${warns[0]}`,
            )

            await redis.command('SADD', INSTANCES_KEY, 'x')
            await d.holdMember(CHANNEL, { id: 8, info: {} })
            assertEquals(
                warnings.having(INSTANCES_SET_WRONG_TYPE).length,
                1,
                'the hot path never WARNs again once the key is healthy',
            )
        } finally {
            warnings.restore()
            await d.close()
        }
    })
}

for (const kind of SET_WRONG_KINDS) {
    Deno.test(`#414 a ${kind}-typed instances set self-heals on DEREGISTER_INSTANCE_SCRIPT: the sweep completes and one WARN names '${kind}'`, async () => {
        const redis = new FakeRedis()
        const DEAD = 'dead-instance-414'
        // Planted with RAW commands so nothing self-heals it before the sweep
        // reaches DEREGISTER_INSTANCE_SCRIPT — a real hold would run
        // INSTANCES_HEAL itself and mask the very corruption this row proves.
        const entry = JSON.stringify({ member: { id: 7 }, owner: DEAD })
        await redis.command('HSET', HOLDERS_KEY(CHANNEL, 7), DEAD, entry)
        await redis.command('HSET', PRESENCE_KEY(CHANNEL), '7', entry)
        await redis.command('SADD', OWNED_KEY(DEAD), `${CHANNEL} 7`)
        await redis.command('SADD', INSTANCES_KEY, DEAD)
        await corrupt(redis, INSTANCES_KEY, kind)

        const warnings = captureWarnings()
        const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
        const sweeper = driver(redis)
        try {
            // Any hold starts the sweeper's own reconcile loop.
            await sweeper.holdMember('presence-other', { id: 9, info: {} })
            await time.tickAsync(3_500)
            for (let i = 0; i < 100; i++) await Promise.resolve()

            const warns = warnings.having(INSTANCES_SET_WRONG_TYPE)
            assertEquals(warns.length, 1, 'exactly one heal WARN')
            assert(warns[0].endsWith(kind), `names the prior kind: ${warns[0]}`)
            // A literal fragment, hardcoded rather than read off the
            // imported constant: a mutation to the WARN's own wording must
            // fail THIS line, not merely re-match itself.
            assert(
                warns[0].includes('Prior type:'),
                `the WARN's own wording: ${warns[0]}`,
            )
            const instances = await redis.command(
                'SMEMBERS',
                INSTANCES_KEY,
            ) as {
                value: { value: string }[]
            }
            assert(
                !instances.value.some((m) => m.value === DEAD),
                'the dead instance was deregistered — the sweep completed',
            )
        } finally {
            warnings.restore()
            time.restore()
            await sweeper.close()
        }
    })
}

// ---------------------------------------------------------------------------
// Presence and holders: fail CLOSED, never healed.
// ---------------------------------------------------------------------------

for (const kind of HASH_WRONG_KINDS) {
    Deno.test(`#414 a ${kind}-typed presence key makes holdMember throw, never silently swallowed`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        try {
            await corrupt(redis, PRESENCE_KEY(CHANNEL), kind)
            await assertRejects(
                () => d.holdMember(CHANNEL, { id: 7, info: {} }),
                Error,
                'WRONGTYPE',
            )
        } finally {
            await d.close()
        }
    })

    Deno.test(`#414 a ${kind}-typed presence key makes releaseMember throw, never silently swallowed`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        try {
            await d.holdMember(CHANNEL, { id: 7, info: {} })
            await corrupt(redis, PRESENCE_KEY(CHANNEL), kind)
            await assertRejects(
                () => d.releaseMember(CHANNEL, 7),
                Error,
                'WRONGTYPE',
            )
        } finally {
            await d.close()
        }
    })

    Deno.test(`#414 a ${kind}-typed presence key makes readRoster throw, never silently swallowed`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        try {
            await corrupt(redis, PRESENCE_KEY(CHANNEL), kind)
            await assertRejects(
                () => d.readRoster!(CHANNEL, 10, []),
                Error,
                'WRONGTYPE',
            )
        } finally {
            await d.close()
        }
    })

    Deno.test(`#414 a ${kind}-typed holders key makes holdMember throw, never silently swallowed`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        try {
            await corrupt(redis, HOLDERS_KEY(CHANNEL, 7), kind)
            await assertRejects(
                () => d.holdMember(CHANNEL, { id: 7, info: {} }),
                Error,
                'WRONGTYPE',
            )
        } finally {
            await d.close()
        }
    })

    Deno.test(`#414 a ${kind}-typed holders key makes releaseMember throw, never silently swallowed`, async () => {
        const redis = new FakeRedis()
        const d = driver(redis)
        try {
            await d.holdMember(CHANNEL, { id: 7, info: {} })
            await corrupt(redis, HOLDERS_KEY(CHANNEL, 7), kind)
            await assertRejects(
                () => d.releaseMember(CHANNEL, 7),
                Error,
                'WRONGTYPE',
            )
        } finally {
            await d.close()
        }
    })
}

// ---------------------------------------------------------------------------
// The ordering fix: a presence-only corruption commits NOTHING (#414).
// ---------------------------------------------------------------------------

Deno.test('#414 a presence-only corruption aborts BEFORE the holders write: no orphan in holders or owned', async () => {
    const redis = new FakeRedis()
    const d = driver(redis)
    try {
        await corrupt(redis, PRESENCE_KEY(CHANNEL), 'set')
        await assertRejects(() => d.holdMember(CHANNEL, { id: 7, info: {} }))

        const holders = await redis.command(
            'HLEN',
            HOLDERS_KEY(CHANNEL, 7),
        ) as {
            value: number
        }
        assertEquals(holders.value, 0, 'no holders entry was written')
        const owned = await redis.command('SMEMBERS', OWNED_KEY(idOf(d))) as {
            value: unknown[]
        }
        assertEquals(owned.value, [], 'no owned entry was written')
        // The instances set is OUT of scope here: `#ensureSweepStarted`'s
        // heartbeat registers this instance with its OWN raw SADD, before
        // the guarded EVAL even runs — unconditionally, whether or not the
        // hold that follows commits. Its one entry is exactly what the
        // heartbeat itself would write, never a second one from the script.
        const instances = await redis.command('SMEMBERS', INSTANCES_KEY) as {
            value: { value: string }[]
        }
        assertEquals(
            instances.value.map((m) => m.value),
            [idOf(d)],
            "only the heartbeat's own registration, nothing from the script",
        )
    } finally {
        await d.close()
    }
})

Deno.test('#414 a healthy or absent owned/instances key never WARNs on hold, release or deregister', async () => {
    const redis = new FakeRedis()
    const DEAD = 'dead-instance-414-healthy'
    const entry = JSON.stringify({ member: { id: 9 }, owner: DEAD })
    await redis.command('HSET', HOLDERS_KEY(CHANNEL, 9), DEAD, entry)
    await redis.command('HSET', PRESENCE_KEY(CHANNEL), '9', entry)
    await redis.command('SADD', OWNED_KEY(DEAD), `${CHANNEL} 9`)
    await redis.command('SADD', INSTANCES_KEY, DEAD)

    const warnings = captureWarnings()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const d = driver(redis)
    try {
        await d.holdMember(CHANNEL, { id: 7, info: {} }) // owned/instances: 'none' then 'set'
        await d.releaseMember(CHANNEL, 7) // owned already 'set'
        // A healthy sweep of the dead instance: both keys are already 'set'.
        await time.tickAsync(3_500)
        for (let i = 0; i < 100; i++) await Promise.resolve()

        assertEquals(warnings.having(OWNED_SET_WRONG_TYPE), [])
        assertEquals(warnings.having(INSTANCES_SET_WRONG_TYPE), [])
    } finally {
        warnings.restore()
        time.restore()
        await d.close()
    }
})

// ---------------------------------------------------------------------------
// Live-broker rows: list and stream, on HOLD_MEMBER_SCRIPT — the one script
// that touches both self-healing keys. FakeRedis never models either type.
// ---------------------------------------------------------------------------

function liveWrongTypeRow(
    key: 'owned' | 'instances',
    kind: 'list' | 'stream',
    corrupt: (client: RedisClient, key: string) => Promise<void>,
) {
    Deno.test({
        name:
            `#414 (live) a ${kind}-typed ${key} set self-heals on HOLD_MEMBER_SCRIPT against a real broker`,
        ignore: !LIVE_BROKER,
        async fn() {
            const config = brokerConfig()
            await preflight(config)
            const live = new RedisClient(config)
            const ns = runNamespace()
            const prefix = `${ns}:rt`
            const subscriber: RedisSubscriber = { psubscribe: () => {} }
            const warnConst = key === 'owned'
                ? OWNED_SET_WRONG_TYPE
                : INSTANCES_SET_WRONG_TYPE
            const target = key === 'owned'
                ? `${prefix}__owned:` // instance id appended below
                : `${prefix}__instances`
            const warnings = captureWarnings()
            const d = new RedisBroadcastDriver(live, subscriber, { prefix })
            try {
                const instanceId = idOf(d)
                const fullKey = key === 'owned'
                    ? `${target}${instanceId}`
                    : target
                await corrupt(live, fullKey)
                const { arrived } = await d.holdMember(CHANNEL, {
                    id: 7,
                    info: {},
                })
                assertEquals(arrived, true)
                const warns = warnings.having(warnConst)
                assertEquals(warns.length, 1)
                assert(warns[0].endsWith(kind), warns[0])
            } finally {
                warnings.restore()
                await d.close()
                await teardown(live, ns)
                await live.close()
            }
        },
    })
}

for (const key of ['owned', 'instances'] as const) {
    liveWrongTypeRow(key, 'list', async (client, k) => {
        await client.command('RPUSH', k, 'a', 'b', 'c')
    })
    liveWrongTypeRow(key, 'stream', async (client, k) => {
        await client.command('XADD', k, '*', 'field', 'value')
    })
}
