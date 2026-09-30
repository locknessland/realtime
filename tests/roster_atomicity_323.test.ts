/**
 * @fileoverview #323 — the authoritative roster write is ONE fact, or none.
 *
 * `holdMember` stored a member as TWO structures — a field in the channel's
 * presence hash, and an entry in the owning instance's owned set — written by
 * two separate round-trips with no transaction. `releaseMember` removed them the
 * same way. Either pair can be interrupted between its halves, and each
 * direction fails differently:
 *
 * - **A half-written add** (`HSET` landed, `SADD` did not) is in no owned set,
 *   and {@link RedisBroadcastDriver} sweeps ghosts by enumerating owned sets —
 *   so the field is unreclaimable, by any instance, forever. It outlives the
 *   process that wrote it.
 * - **A half-removed member** (`HDEL` landed, `SREM` did not) leaves an owned
 *   entry naming a field that no longer belongs to it. `#sweepInstance` HDELs
 *   what the owned set names **without checking the entry's `owner`**, so once
 *   that member re-joins on another instance and the stale owner dies, the
 *   sweep deletes a LIVE member somebody else owns.
 *
 * The second is why atomicity here is not tidiness. The sweep trusts the owned
 * set completely, so the owned set must never be able to lie.
 *
 * These tests do not guard the windows; they assert the windows do not exist.
 * Faults are injected at the COMMAND BOUNDARY, never by monkey-patching the
 * driver under test — the convention `revocation_atomicity.test.ts` set for the
 * same defect shape one structure over.
 *
 * @module @lockness/realtime/tests/roster_atomicity_323
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { FakeRedis } from './fake_redis.ts'

const PREFIX = 'app:rt'
const CHANNEL = 'presence-room'
const OWNED_PREFIX = `${PREFIX}__owned:`

/** A driver on `redis`, optionally through a command wrapper that injects faults. */
function driverOn(
    redis: FakeRedis,
    command: (...args: string[]) => Promise<unknown> = redis.command,
): RedisBroadcastDriver {
    return new RedisBroadcastDriver({ command }, redis.subscriberFor(), {
        prefix: PREFIX,
    })
}

/** Every command whose operands name an owned-set key. */
const touchesOwnedSet = (args: string[]): boolean =>
    args.some((a) => a.startsWith(OWNED_PREFIX))

/** Every command that touches either half of the roster pair. */
const touchesRosterKey = (args: string[]): boolean =>
    args.some((a) => a.startsWith(OWNED_PREFIX) || a.includes(CHANNEL))

Deno.test('#323 holdMember is ONE operation — there is no between', async () => {
    const redis = new FakeRedis()
    const issued: string[][] = []
    const counting = (...args: string[]): Promise<unknown> => {
        issued.push(args)
        return redis.command(...args)
    }
    const driver = driverOn(redis, counting)
    try {
        // FILTERED, not reset on a warm-up call. `#ensureSweepStarted` issues
        // its own commands and resetting after the first add assumes it fires
        // exactly once and exactly there — an assumption that would make this
        // test fail for an unrelated reason the day it changes.
        await driver.holdMember(CHANNEL, { id: 'u1' })
        issued.length = 0
        await driver.holdMember(CHANNEL, { id: 'u2' })
        const rosterWrites = issued.filter(touchesRosterKey)

        assertEquals(
            rosterWrites.length,
            1,
            `holdMember must be ONE operation, issued: ${
                issued.map((a) => a[0]).join(', ')
            }`,
        )
        assertEquals(rosterWrites[0][0], 'EVAL')
        // Counting commands proves there is no WINDOW; it does not prove the
        // one command did anything. An EVAL that writes nothing satisfies the
        // count. State, then:
        assertEquals(
            (await driver.readRoster!(CHANNEL, 1_000, [])).members.map((m) =>
                m.id
            ).sort(),
            ['u1', 'u2'],
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#323 releaseMember is ONE operation — the owned set cannot lie', async () => {
    const redis = new FakeRedis()
    const issued: string[][] = []
    const counting = (...args: string[]): Promise<unknown> => {
        issued.push(args)
        return redis.command(...args)
    }
    const driver = driverOn(redis, counting)
    try {
        await driver.holdMember(CHANNEL, { id: 'u1' })
        issued.length = 0
        await driver.releaseMember(CHANNEL, 'u1')
        const rosterWrites = issued.filter(touchesRosterKey)

        assertEquals(
            rosterWrites.length,
            1,
            `releaseMember must be ONE operation, issued: ${
                issued.map((a) => a[0]).join(', ')
            }`,
        )
        assertEquals(rosterWrites[0][0], 'EVAL')
        assertEquals(
            (await driver.readRoster!(CHANNEL, 1_000, [])).members,
            [],
            'and the one command actually removed the member',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#323 a rejected add leaves NEITHER structure', async () => {
    const redis = new FakeRedis()
    // Reject the write that touches the owned set. Under the two-command shape
    // that is the SADD, and the HSET has already landed — the unreclaimable
    // case. Under one EVAL it is the whole write, and nothing lands.
    let armed = false
    const failing = (...args: string[]): Promise<unknown> => {
        if (armed && touchesOwnedSet(args)) {
            return Promise.reject(new Error('NOPERM'))
        }
        return redis.command(...args)
    }
    const driver = driverOn(redis, failing)
    try {
        // Start the sweep first, so its own owned-set writes are not the ones
        // this test rejects.
        await driver.holdMember(CHANNEL, { id: 'warmup' })
        await driver.releaseMember(CHANNEL, 'warmup')
        armed = true

        await driver.holdMember(CHANNEL, { id: 'u1' }).then(
            () => assert(false, 'the roster write must not report success'),
            () => {},
        )

        assertEquals(
            (await driver.readRoster!(CHANNEL, 1_000, [])).members,
            [],
            'a half-written member is unreclaimable — the sweep enumerates ' +
                'owned sets, and this field would be in none',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#323 a rejected remove leaves the member wholly present', async () => {
    const redis = new FakeRedis()
    let armed = false
    const failing = (...args: string[]): Promise<unknown> => {
        if (armed && touchesOwnedSet(args)) {
            return Promise.reject(new Error('NOPERM'))
        }
        return redis.command(...args)
    }
    const driver = driverOn(redis, failing)
    try {
        await driver.holdMember(CHANNEL, { id: 'u1' })
        armed = true

        await driver.releaseMember(CHANNEL, 'u1').then(
            () => assert(false, 'the roster removal must not report success'),
            () => {},
        )

        // Half-removed is the dangerous direction: an owned entry naming a
        // field it no longer owns makes the sweep delete a live member.
        assertEquals(
            (await driver.readRoster!(CHANNEL, 1_000, [])).members.map((m) =>
                m.id
            ),
            ['u1'],
            'a failed removal leaves the member present, not half-gone',
        )
    } finally {
        await driver.close()
    }
})

/** A driver whose liveness window is short enough for a test to outlive it. */
function sweepingDriverOn(redis: FakeRedis): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command: redis.command },
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

/** `tickAsync` fires the reconcile but does not await its multi-await body. */
async function flushMicrotasks(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

Deno.test(
    "#323 a stale owned entry would make the sweep delete somebody else's LIVE member",
    async () => {
        // The destructive direction, end to end. `#sweepInstance` HDELs whatever
        // a dead instance's owned set names and never checks the entry's owner,
        // so an owned entry that outlived its member is not merely litter — it
        // is a deletion order for whoever holds that field next.
        //
        // Shape-level assertions cannot see this: every other test in this file
        // reads the presence hash, and the whole point is that the hash and the
        // owned set can disagree. This one reads neither directly. It drives the
        // sweep and asks who is still `here` afterwards.
        const redis = new FakeRedis()
        const time = new FakeTime(new Date('2026-09-09T10:00:00Z'))
        const a = sweepingDriverOn(redis)
        const b = sweepingDriverOn(redis)
        try {
            // A holds the member, then gives it up. With a non-atomic removal
            // whose second half is lost, A's owned set still names it.
            await a.holdMember(CHANNEL, { id: 'u1' })
            await a.releaseMember(CHANNEL, 'u1')

            // The same member joins on B. It is B's now, and it is genuinely here.
            await b.holdMember(CHANNEL, { id: 'u1' })
            assertEquals(
                (await b.readRoster!(CHANNEL, 1_000, [])).members.map((m) =>
                    m.id
                ),
                ['u1'],
                'precondition: the member is present and owned by B',
            )

            // A crashes. B sweeps what A's owned set names.
            await a.close()
            await time.tickAsync(3_500)
            await flushMicrotasks()

            assertEquals(
                (await b.readRoster!(CHANNEL, 1_000, [])).members.map((m) =>
                    m.id
                ),
                ['u1'],
                'the sweep must not reclaim a member it does not own — a ' +
                    'removal that dropped only its owned-set half turns the ' +
                    "next instance's live member into a ghost",
            )
        } finally {
            await b.close()
            time.restore()
        }
    },
)
