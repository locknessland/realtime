/**
 * @fileoverview #345 — one member on two instances: the Redis roster slot is
 * held per instance.
 *
 * Before this item the slot was ONE field naming ONE `owner`: the last writer
 * won the `info`, a leave on one instance deleted a slot another instance still
 * held, and a dead instance's sweep deleted whatever its owned set listed —
 * including a slot a live instance held. The disposition gives each slot a
 * holders hash (`instanceId → entry`); a release drops only the releaser's
 * hold, and the presence field goes only when no holder is left.
 *
 * Every witness runs two or more real `RedisBroadcastDriver`s over one
 * `FakeRedis`, whose scripts run through the shared Lua evaluator. The ghost
 * sweep is driven by `FakeTime`, as `presence_sweep.test.ts` does.
 *
 * **The invariant is checked after every step**: for a slot written by 0.4.0,
 * the presence field exists if and only if its holders hash has at least one
 * entry. W3 and W8 were red on the pre-#345 driver THROUGH that invariant —
 * their end state alone already matched, because a single holder could not show
 * the defect; `mutations/presence_member_holds_345.ts` keeps each one killing
 * its mutant. W7 is green before and after, on purpose: a 0.3.0 field with no
 * holders hash must still be reclaimed by a sweep.
 *
 * The seam is `holdMember` / `releaseMember` (#344), called on the driver
 * directly: these witnesses pin the Redis slot, not the manager's announcements
 * (those are `presence_member_transitions_344.test.ts`).
 *
 * @module @lockness/realtime/tests/roster_holders_345
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OTHER = 'presence-other'
const PRESENCE_KEY = (channel: string) => `${PREFIX}__presence:${channel}`
/** Plan 260 FR-003: `<prefix>__holders:<channel> <String(id)>`. */
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
            control: { secret: 'deployment-secret-with-enough-entropy' },
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

const member = (id: number, name: string): PresenceMember => ({
    id,
    info: { name },
})

/** Drain the microtasks a reconcile pass queues (see `presence_sweep.test.ts`). */
async function flushMicrotasks(times = 100): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

/** Advance past a closed driver's liveness TTL so a live peer sweeps it. */
async function lapse(time: FakeTime): Promise<void> {
    await time.tickAsync(3_500)
    await flushMicrotasks()
}

async function read(d: RedisBroadcastDriver, channel = CHANNEL) {
    return await d.readRoster!(channel, 1_000, [])
}

function infoOf(members: readonly PresenceMember[], id: number) {
    return members.find((m) => m.id === id)?.info
}

/**
 * Field exists ⇔ holders ≥ 1, sampled after every step and asserted at the end.
 *
 * Collected rather than thrown at the step, so a witness whose behavioural
 * assertion is red reports THAT first: an early invariant throw would mask it
 * behind the one symptom every pre-#345 slot shares (no holders hash at all).
 *
 * @param redis - The shared fake.
 * @param slots - `[channel, id]` pairs written by this test.
 * @returns `check(step)` to sample, `assertHeld()` to fail on any violation.
 */
function slotInvariant(
    redis: FakeRedis,
    slots: ReadonlyArray<readonly [string, number]>,
) {
    const violations: string[] = []
    return {
        async check(step: string): Promise<void> {
            for (const [channel, id] of slots) {
                const field = await redis.command(
                    'HGET',
                    PRESENCE_KEY(channel),
                    String(id),
                ) as { type: string }
                const holders = await redis.command(
                    'HLEN',
                    HOLDERS_KEY(channel, id),
                ) as { value: number }
                const fieldExists = field.type !== 'nil'
                if (fieldExists !== holders.value >= 1) {
                    violations.push(
                        `${step}: slot ${channel}/${id} has its presence field ` +
                            `${fieldExists ? 'present' : 'absent'} and ` +
                            `${holders.value} holder(s)`,
                    )
                }
            }
        },
        assertHeld(): void {
            assertEquals(
                violations,
                [],
                'a 0.4.0 slot exists exactly while its holders hash has an entry',
            )
        },
    }
}

Deno.test('#345 W1 A and B hold 7, A releases — 7 is still read, total 1', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await inv.check('A holds')
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await inv.check('B holds')
        await a.releaseMember(CHANNEL, 7)
        await inv.check('A releases')

        const window = await read(b)
        assertEquals(
            window.members.map((m) => m.id),
            [7],
            'B still holds member 7, so A leaving must not take the slot',
        )
        assertEquals(window.total, 1)
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W2 B wrote last, A lapses, B sweeps A — 7 is still there', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await inv.check('both hold')
        await a.close()
        await lapse(time)
        await inv.check('after B swept A')

        const window = await read(b)
        assertEquals(window.members.map((m) => m.id), [7])
        assertEquals(infoOf(window.members, 7), { name: 'from-B' })
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test("#345 W2' A wrote last, A lapses, B sweeps A — 7 is there with B's info", async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await inv.check('both hold, A shown')
        await a.close()
        await lapse(time)
        await inv.check('after B swept A')

        const window = await read(b)
        assertEquals(window.members.map((m) => m.id), [7])
        assertEquals(
            infoOf(window.members, 7),
            { name: 'from-B' },
            "the shown entry was the departed holder's, so the survivor's " +
                'entry replaces it',
        )
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W3 both release — the slot is gone and its holders hash no longer exists', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await inv.check('A holds')
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await inv.check('B holds')
        await a.releaseMember(CHANNEL, 7)
        await inv.check('A releases')
        await b.releaseMember(CHANNEL, 7)
        await inv.check('B releases')

        assertEquals((await read(b)).total, 0)
        assertEquals(
            await redis.command('EXISTS', HOLDERS_KEY(CHANNEL, 7)),
            { type: 'integer', value: 0 },
        )
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W4 two interleaved sweeps of A change nothing further — 7 is still there', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const c = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7], [OTHER, 9]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        // C holds something elsewhere, so its own reconcile pass is running.
        await c.holdMember(OTHER, member(9, 'from-C'))
        await inv.check('set up')
        await a.close()
        await lapse(time)
        await inv.check('after B and C swept A')

        const sweepsOfA = redis.commandLog().filter(([cmd, key]) =>
            cmd === 'SSCAN' && key === OWNED_KEY(idOf(a))
        ).length
        assert(
            sweepsOfA >= 2,
            `precondition: both survivors swept A (saw ${sweepsOfA} owned-set ` +
                'reads) — otherwise this witnesses one sweep, not two',
        )
        const window = await read(b)
        assertEquals(
            window.members.map((m) =>
                m.id
            ),
            [7],
        )
        assertEquals(infoOf(window.members, 7), { name: 'from-B' })
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        await c.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W5 with two holders, releasing the shown one shows the other exactly', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await b.releaseMember(CHANNEL, 7)
        await inv.check('B (shown) released')

        const window = await read(a)
        assertEquals(window.members, [member(7, 'from-A')])
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W5 with three holders, releasing a holder that is NOT shown leaves the info unchanged', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const c = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await c.holdMember(CHANNEL, member(7, 'from-C'))
        await a.releaseMember(CHANNEL, 7)
        await inv.check('A (not shown) released')

        const window = await read(b)
        assertEquals(window.members, [member(7, 'from-C')])
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        await c.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test("#345 W6 A is swept while B holds, then A's own release leaves B's slot untouched", async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7]])
    try {
        await b.holdMember(CHANNEL, member(7, 'from-B'))
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        // A stops beating but its process is still up — a lapsed heartbeat.
        await a.close()
        await lapse(time)
        await inv.check('B swept A')
        // A's socket then closes: its release finds no hold of its own.
        await a.releaseMember(CHANNEL, 7)
        await inv.check('swept A released')

        const window = await read(b)
        assertEquals(window.members, [member(7, 'from-B')])
        inv.assertHeld()
    } finally {
        await a.close()
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W7 a 0.3.0 field with no holders hash is still reclaimed by a sweep', async () => {
    // Green before #345 and after: the upgrade must not strand a 0.3.0 ghost.
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const b = driver(redis)
    const deadId = 'instance-from-0-3-0'
    try {
        await redis.command(
            'HSET',
            PRESENCE_KEY(CHANNEL),
            '7',
            JSON.stringify({ member: { id: 7 }, owner: deadId }),
        )
        await redis.command('SADD', OWNED_KEY(deadId), `${CHANNEL} 7`)
        await redis.command('SADD', INSTANCES_KEY, deadId)
        // B holds something, so its reconcile pass runs.
        await b.holdMember(OTHER, member(9, 'from-B'))
        await lapse(time)

        assertEquals(await redis.command('HGET', PRESENCE_KEY(CHANNEL), '7'), {
            type: 'nil',
        })
        assertEquals((await read(b)).total, 0)
        // Only the legacy slot: B's own hold of 9 is written by whichever
        // driver runs this, and this witness must read the same on both.
        const inv = slotInvariant(redis, [[CHANNEL, 7]])
        await inv.check('swept')
        inv.assertHeld()
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 W8 one instance holding 7 and 8 releases 8 — 7 is still there', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    const inv = slotInvariant(redis, [[CHANNEL, 7], [CHANNEL, 8]])
    try {
        await a.holdMember(CHANNEL, member(7, 'seven'))
        await inv.check('A holds 7')
        await a.holdMember(CHANNEL, member(8, 'eight'))
        await inv.check('A holds 8')
        await a.releaseMember(CHANNEL, 8)
        await inv.check('A releases 8')

        assertEquals((await read(a)).members, [member(7, 'seven')])
        inv.assertHeld()
    } finally {
        await a.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#345 FR-004a a hold reply other than the integer 0 or 1 throws', async () => {
    // The manager announces `joined` from this bit, so a reply the script
    // never produces must fail loudly rather than read as a transition (an
    // integer 2) or as none (an array). The third row is well-FORMED (#414's
    // {arrived, ownedKind, instancesKind} triple, shape-valid) with `arrived`
    // outside 0/1 — the only row that reaches the decoder's truthy-vs-strict
    // comparison at all; the first two never get that far; a shape all three
    // share.
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    try {
        for (
            const reply of [
                { type: 'integer', value: 2 },
                { type: 'array', value: [] },
                {
                    type: 'array',
                    value: [
                        { type: 'integer', value: 2 },
                        { type: 'bulk', value: 'set' },
                        { type: 'bulk', value: 'set' },
                    ],
                },
            ]
        ) {
            const odd: CommandFn = (...args) =>
                args[0] === 'EVAL'
                    ? Promise.resolve(reply)
                    : redis.command(...args)
            const a = driver(redis, odd)
            try {
                await assertRejects(
                    () => a.holdMember(CHANNEL, member(7, 'seven')),
                    Error,
                    'other than 0 or 1',
                )
            } finally {
                await a.close()
            }
        }
    } finally {
        time.restore()
        redis.assertNoRejections()
    }
})

/** A marker a decoder's error message must never carry (#355 S4). */
const MARKER = 'reply-bytes-marker'

/**
 * Replies neither script ever produces. The pre-#348 integer 1 above all: a
 * new code must never reuse it, or this row would silently reverse. The last
 * row is well-FORMED since #414 (a genuine `{value, kind}` pair) with an
 * EMPTY bulk as its value — the one shape that reaches the decoder's
 * `entry` truthy-check at all; every row before it fails the outer shape
 * check first.
 */
const ODD_REPLIES: readonly unknown[] = [
    { type: 'integer', value: 1 },
    { type: 'nil' },
    { type: 'array', value: [] },
    { type: 'bulk', value: '' },
    { type: 'array', value: [{ type: 'bulk', value: MARKER }] },
    {
        type: 'array',
        value: [{ type: 'bulk', value: '' }, { type: 'bulk', value: 'set' }],
    },
]

Deno.test('#355 WD a release reply is one of four outcomes, a deregistration reply one of three, and anything else throws a message that never carries the reply', async () => {
    // Since #355 the release script answers emptied (the released entry),
    // kept, absent (0) or refused, and the deregistration script
    // deregistered (0), renewed or kept. The codes are private to the driver
    // and never spelled here: each reply below is produced by the real
    // script on the fake, which is also what proves it decodes.
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const warn = console.warn
    const warnings: string[] = []
    console.warn = (...parts: unknown[]) => void warnings.push(parts.join(' '))
    try {
        // Emptied, kept and absent, through a leave.
        const a = driver(redis)
        const b = driver(redis)
        try {
            await a.holdMember(CHANNEL, member(7, 'from-A'))
            await b.holdMember(CHANNEL, member(7, 'from-B'))
            assertEquals(await a.releaseMember(CHANNEL, 7), { gone: false })
            assertEquals(await a.releaseMember(CHANNEL, 7), { gone: false })
            assertEquals(await b.releaseMember(CHANNEL, 7), { gone: true })
        } finally {
            await a.close()
            await b.close()
        }

        // Refused, through a sweep: only a sweep asks for the liveness
        // check. The swept instance renews right after the sweep read it as
        // lapsed, so the release that follows is refused and changes nothing.
        const renewing = 'instance-renewing'
        const aliveKey = `${PREFIX}__alive:${renewing}`
        const entry = JSON.stringify({ member: { id: 5 }, owner: renewing })
        await redis.command('HSET', HOLDERS_KEY(CHANNEL, 5), renewing, entry)
        await redis.command('HSET', PRESENCE_KEY(CHANNEL), '5', entry)
        await redis.command('SADD', OWNED_KEY(renewing), `${CHANNEL} 5`)
        await redis.command('SADD', INSTANCES_KEY, renewing)
        const releases: unknown[] = []
        const port: CommandFn = async (...args) => {
            const reply = await redis.command(...args)
            if (args[0] === 'EXISTS' && args[1] === aliveKey) {
                await redis.command('SET', aliveKey, '1', 'EX', '30')
            }
            if (
                args[0] === 'EVAL' && args.includes(HOLDERS_KEY(CHANNEL, 5))
            ) {
                releases.push(reply)
            }
            return reply
        }
        const sweeper = driver(redis, port)
        try {
            await sweeper.holdMember(OTHER, member(9, 'sweeper'))
            await lapse(time)
        } finally {
            await sweeper.close()
        }
        assertEquals(releases.length, 1, 'precondition: the sweep released')
        assertEquals(
            warnings.filter((w) => w.includes(renewing)),
            [
                `realtime: instance ${renewing} renewed its liveness while ` +
                'being swept — a lapse, not a crash; 0 hold(s) released ' +
                '(0 emptied) before it did',
            ],
            'refused on its first release: one "renewed" line at N = 0, and ' +
                'no "released" or "failed" line',
        )
        assertEquals(
            await redis.command('HGET', HOLDERS_KEY(CHANNEL, 5), renewing),
            { type: 'bulk', value: entry },
            'a refused release changes nothing',
        )
        // A refused reply reaching a leave is a defect, not a "not gone".
        const refused = releases[0]
        const leave = driver(
            redis,
            (...args) =>
                args[0] === 'EVAL'
                    ? Promise.resolve(refused)
                    : redis.command(...args),
        )
        try {
            await assertRejects(
                () => leave.releaseMember(CHANNEL, 5),
                Error,
                'a leave never asks for the liveness check',
            )
        } finally {
            await leave.close()
        }

        // Anything else a release answers throws, and says only what it
        // accepts — never the reply.
        for (const reply of ODD_REPLIES) {
            const odd: CommandFn = (...args) =>
                args[0] === 'EVAL'
                    ? Promise.resolve(reply)
                    : redis.command(...args)
            const c = driver(redis, odd)
            try {
                const error = await assertRejects(
                    () => c.releaseMember(CHANNEL, 7),
                    Error,
                    'none of its four replies',
                )
                assert(!error.message.includes(MARKER), error.message)
            } finally {
                await c.close()
            }
        }
    } finally {
        console.warn = warn
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test('#355 WD a deregistration reply other than its three fails that sweep alone: one "failed" line without the reply, and no deregistration', async () => {
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    try {
        for (const reply of ODD_REPLIES) {
            const redis = new FakeRedis()
            const gone = 'instance-gone'
            // Registered, lapsed, owning nothing: straight to deregistration.
            await redis.command('SADD', INSTANCES_KEY, gone)
            const odd: CommandFn = (...args) =>
                args[0] === 'EVAL' && args[3] === INSTANCES_KEY
                    ? Promise.resolve(reply)
                    : redis.command(...args)
            const b = driver(redis, odd)
            const warn = console.warn
            const warnings: string[] = []
            console.warn = (...parts: unknown[]) =>
                void warnings.push(parts.join(' '))
            try {
                await b.holdMember(OTHER, member(9, 'from-B'))
                await lapse(time)
            } finally {
                console.warn = warn
                await b.close()
            }
            const failed = warnings.filter((w) =>
                w.includes(`sweep of dead instance ${gone} failed`)
            )
            assertEquals(failed.length, 1, JSON.stringify(reply))
            assert(failed[0].includes('none of its three replies'), failed[0])
            assert(!failed[0].includes(MARKER), failed[0])
            const instances = await redis.command(
                'SMEMBERS',
                INSTANCES_KEY,
            ) as { value: { value: string }[] }
            assert(
                instances.value.some((m) => m.value === gone),
                'an undecodable deregistration reply deregisters nothing',
            )
            redis.assertNoRejections()
        }
    } finally {
        time.restore()
    }
})

Deno.test('#345 S1b a hold by an instance whose registration never landed registers it', async () => {
    // A holders entry no sweep can reach is a permanent ghost. The heartbeat's
    // registration is a separate round-trip that can fail and is only logged,
    // so the hold itself must put its instance where the sweep looks.
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    let refused = 0
    const flaky: CommandFn = (...args) => {
        if (args[0] === 'SADD' && args[1] === INSTANCES_KEY && refused === 0) {
            refused++
            return Promise.reject(new Error('connection reset'))
        }
        return redis.command(...args)
    }
    const a = driver(redis, flaky)
    const warn = console.warn
    const warnings: string[] = []
    console.warn = (...parts: unknown[]) => void warnings.push(parts.join(' '))
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        assertEquals(refused, 1, 'precondition: the registration was refused')
        assert(
            warnings.some((w) => w.includes('heartbeat failed')),
            'precondition: and only logged',
        )

        const instances = await redis.command('SMEMBERS', INSTANCES_KEY) as {
            value: { value: string }[]
        }
        assert(
            instances.value.some((m) => m.value === idOf(a)),
            'the instance that holds 7 must be registered, or no sweep can ' +
                'ever release its hold',
        )
    } finally {
        console.warn = warn
        await a.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test("#345 S1c a hold landing between a sweep's owned-set read and its end stays in the owned set", async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const a = driver(redis)
    let holdDuringSweep: (() => Promise<void>) | undefined
    let landed = false
    // B's command client: right after B's sweep reads A's owned set, A (swept
    // while still up) completes a hold of 8 before the sweep goes on.
    const intercepting: CommandFn = async (...args) => {
        const reply = await redis.command(...args)
        if (
            args[0] === 'SSCAN' && args[1] === OWNED_KEY(idOf(a)) &&
            holdDuringSweep
        ) {
            const hold = holdDuringSweep
            holdDuringSweep = undefined
            await hold()
            landed = true
        }
        return reply
    }
    const b = driver(redis, intercepting)
    try {
        await a.holdMember(CHANNEL, member(7, 'from-A'))
        await b.holdMember(OTHER, member(9, 'from-B'))
        await a.close()
        holdDuringSweep = async () => {
            await a.holdMember(CHANNEL, member(8, 'from-A'))
        }
        await lapse(time)
        assert(landed, 'precondition: the hold landed inside the sweep')

        const owned = await redis.command('SMEMBERS', OWNED_KEY(idOf(a))) as {
            value: { value: string }[]
        }
        assert(
            owned.value.some((m) => m.value === `${CHANNEL} 8`),
            'the hold that landed after the sweep read the owned set must stay ' +
                'sweepable — dropping the whole owned set strands it',
        )
    } finally {
        await b.close()
        time.restore()
        redis.assertNoRejections()
    }
})

// --- Manager row ------------------------------------------------------------

interface User {
    id: number
    name: string
}

function conn(id: string, user: User): Connection<User> {
    return {
        id,
        identity: user,
        metadata: {},
        send: () => {},
        close: () => {},
    } as Connection<User>
}

Deno.test('#345 manager: id 7 on two instances — A leaving and then dying never removes 7 from B', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-15T10:00:00Z'))
    const authorize = (user: User | null): PresenceMember | false =>
        user ? { id: user.id, info: { name: user.name } } : false
    const driverA = driver(redis)
    const driverB = driver(redis)
    const managerA = new ChannelManager<User>({ driver: driverA, authorize })
    const managerB = new ChannelManager<User>({ driver: driverB, authorize })
    try {
        const a1 = conn('a1', { id: 7, name: 'Ada' })
        managerA.register(a1)
        await managerA.subscribe(a1, CHANNEL)
        const b1 = conn('b1', { id: 7, name: 'Ada' })
        managerB.register(b1)
        await managerB.subscribe(b1, CHANNEL)

        assertEquals(await managerA.unsubscribe('a1', CHANNEL), 'left')
        const b2 = conn('b2', { id: 9, name: 'Bo' })
        managerB.register(b2)
        const joiner = await managerB.subscribe(b2, CHANNEL)
        assertEquals(
            joiner.here?.members.map((m) => m.id).sort(),
            [7, 9],
            'b1 still holds member 7 on B, so a joiner on B must see it',
        )
        assertEquals(joiner.here?.total, 2)

        await driverA.close()
        await lapse(time)
        const b3 = conn('b3', { id: 11, name: 'Cy' })
        managerB.register(b3)
        const late = await managerB.subscribe(b3, CHANNEL)
        assertEquals(
            late.here?.members.map((m) => m.id).sort((x, y) =>
                Number(x) - Number(y)
            ),
            [7, 9, 11],
            "A's death is not B's: 7 stays",
        )
    } finally {
        await driverB.close()
        time.restore()
        redis.assertNoRejections()
    }
})
