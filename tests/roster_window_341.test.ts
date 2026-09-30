/**
 * @fileoverview #341 — each shipped driver's `readRoster`, unit by unit.
 *
 * The manager-level witness (`presence_read_bound_341.test.ts`) pins what a
 * subscribe ingests. This file pins the seam underneath it, per driver: the
 * window's size and order, `total` from the same read, `selves` matched by
 * parsed id with no driver-internal metadata, and the input asserts a driver
 * owes before it issues any command (S2) — because the seam is exported and a
 * caller other than `ChannelManager` can reach it.
 *
 * @module @lockness/realtime/tests/roster_window_341
 */

import { assert, assertEquals, assertRejects, assertThrows } from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { MAX_ROSTER_READ_SELF_IDS } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import { FakeRedis } from './fake_redis.ts'

const ROOM = 'presence-room'
/** The Redis driver's presence hash for {@link ROOM} under prefix `app:rt`. */
const PRESENCE_KEY = `app:rt__presence:${ROOM}`

const member = (id: number | string): PresenceMember => ({
    id,
    info: { name: `m${id}` },
})

const BAD_LIMITS = [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]

// ─── memory ──────────────────────────────────────────────────────────────────

function memoryRoom(size: number): MemoryBroadcastDriver {
    const driver = new MemoryBroadcastDriver()
    for (let id = 1; id <= size; id++) driver.holdMember(ROOM, member(id))
    return driver
}

Deno.test('#341 memory readRoster returns the first limit members in join order, and the total', () => {
    const window = memoryRoom(5).readRoster(ROOM, 3, [])
    assertEquals(window.members.map((m) => m.id), [1, 2, 3])
    assertEquals(window.total, 5)
    assertEquals(window.selves, [])
})

Deno.test('#341 memory readRoster returns a room at or below limit whole', () => {
    const window = memoryRoom(3).readRoster(ROOM, 3, [])
    assertEquals(window.members.map((m) => m.id), [1, 2, 3])
    assertEquals(window.total, 3)
})

Deno.test('#341 memory readRoster finds selves by String(id) and skips absent ids', () => {
    const window = memoryRoom(5).readRoster(ROOM, 2, ['5', 99, 4])
    assertEquals(window.selves, [member(5), member(4)])
})

Deno.test('#341 memory readRoster on an unknown channel is an empty window', () => {
    assertEquals(new MemoryBroadcastDriver().readRoster('presence-x', 3, [1]), {
        members: [],
        total: 0,
        selves: [],
    })
})

Deno.test('#341 memory readRoster hands out a fresh array, never its store', () => {
    const driver = memoryRoom(2)
    driver.readRoster(ROOM, 5, []).members.push(member(42))
    assertEquals(driver.readRoster(ROOM, 5, []).total, 2)
    assertEquals(driver.readRoster(ROOM, 5, []).members.length, 2)
})

Deno.test('#341 memory readRoster refuses a limit that is not a positive integer', () => {
    const driver = memoryRoom(2)
    for (const limit of BAD_LIMITS) {
        assertThrows(
            () => driver.readRoster(ROOM, limit, []),
            Error,
            'limit',
            String(limit),
        )
    }
})

Deno.test('#341 memory readRoster refuses more self ids than MAX_ROSTER_READ_SELF_IDS', () => {
    const ids = Array.from(
        { length: MAX_ROSTER_READ_SELF_IDS + 1 },
        (_, i) => i,
    )
    assertThrows(
        () => memoryRoom(2).readRoster(ROOM, 1, ids),
        Error,
        'self ids',
    )
    // Exactly the cap is accepted.
    memoryRoom(2).readRoster(ROOM, 1, ids.slice(1))
})

// ─── redis ───────────────────────────────────────────────────────────────────

/** A Redis driver over a fake broker, closed by `run`'s `finally`. */
async function withRedis(
    work: (
        driver: RedisBroadcastDriver,
        redis: FakeRedis,
        log: string[][],
    ) => Promise<void>,
): Promise<void> {
    const redis = new FakeRedis()
    const log: string[][] = []
    const driver = new RedisBroadcastDriver(
        {
            command: (...args: string[]) => {
                log.push(args)
                return redis.command(...args)
            },
        },
        redis.subscriberFor(),
        { prefix: 'app:rt' },
    )
    try {
        await work(driver, redis, log)
        redis.assertNoRejections()
    } finally {
        await driver.close()
    }
}

async function seed(driver: RedisBroadcastDriver, size: number) {
    for (let id = 1; id <= size; id++) {
        await driver.holdMember(ROOM, member(id))
    }
}

Deno.test('#341 redis readRoster is ONE EVAL returning limit members, the total and the selves', async () => {
    await withRedis(async (driver, _redis, log) => {
        await seed(driver, 5)
        log.length = 0
        const window = await driver.readRoster(ROOM, 3, [5, '4'])
        assertEquals(log.map((c) => c[0]), ['EVAL'], 'one command, one read')
        assertEquals(window.members.length, 3)
        assertEquals(
            new Set(window.members.map((m) => String(m.id))).size,
            3,
            'distinct members',
        )
        assertEquals(window.total, 5)
        assertEquals(
            [...window.selves].sort((a, b) =>
                String(a.id) < String(b.id) ? -1 : 1
            ),
            [member(4), member(5)],
        )
    })
})

Deno.test('#341 redis readRoster returns a room at or below limit whole, in hash order', async () => {
    await withRedis(async (driver) => {
        await seed(driver, 3)
        const window = await driver.readRoster(ROOM, 3, [])
        assertEquals(window.members, [member(1), member(2), member(3)])
        assertEquals(window.total, 3)
        assertEquals(window.selves, [])
    })
})

Deno.test('#341 redis readRoster with zero self ids still issues a valid HMGET (A1)', async () => {
    await withRedis(async (driver, redis) => {
        await seed(driver, 2)
        const window = await driver.readRoster(ROOM, 5, [])
        assertEquals(window.total, 2)
        const hmget = redis.commandLog().filter((c) => c[0] === 'HMGET')
        assertEquals(hmget.length, 1)
        assert(hmget[0].length >= 3, 'HMGET carries at least one field')
    })
})

Deno.test('#341 redis readRoster matches selves by parsed id, an absent id between two present ones (S3)', async () => {
    await withRedis(async (driver) => {
        await seed(driver, 4)
        const window = await driver.readRoster(ROOM, 1, [2, 'absent', 3])
        const byId = new Map(window.selves.map((m) => [String(m.id), m]))
        assertEquals(byId.size, 2)
        assertEquals(byId.get('2'), member(2))
        assertEquals(byId.get('3'), member(3))
    })
})

Deno.test('#341 redis readRoster drops a self entry whose stored id is not the id asked for (S3)', async () => {
    await withRedis(async (driver, redis) => {
        await seed(driver, 1)
        // A field whose stored member names somebody else: returning it as
        // `forged`'s self would hand one caller another member's entry.
        await redis.command(
            'HSET',
            PRESENCE_KEY,
            'forged',
            JSON.stringify({ member: member('victim'), owner: 'x' }),
        )
        const window = await driver.readRoster(ROOM, 5, ['forged'])
        assertEquals(window.selves, [])
    })
})

Deno.test('#341 redis readRoster never returns the owner', async () => {
    await withRedis(async (driver) => {
        await seed(driver, 2)
        const window = await driver.readRoster(ROOM, 5, [1])
        for (const m of [...window.members, ...window.selves]) {
            assertEquals(Object.keys(m).sort(), ['id', 'info'])
        }
    })
})

Deno.test('#341 redis readRoster skips an unparseable entry with a WARN', async () => {
    await withRedis(async (driver, redis) => {
        await seed(driver, 2)
        await redis.command('HSET', PRESENCE_KEY, 'bad', '{nope')
        const warned: unknown[] = []
        const warn = console.warn
        console.warn = (...args: unknown[]) => void warned.push(args)
        try {
            const window = await driver.readRoster(ROOM, 5, ['bad'])
            assertEquals(window.members.map((m) => m.id), [1, 2])
            assertEquals(window.total, 3, 'total is the hash length')
            assertEquals(window.selves, [])
        } finally {
            console.warn = warn
        }
        assert(warned.length >= 1, 'the skip is logged')
    })
})

Deno.test('#341 redis readRoster refuses bad input before issuing any command (S2)', async () => {
    await withRedis(async (driver, _redis, log) => {
        for (const limit of BAD_LIMITS) {
            await assertRejects(
                async () => await driver.readRoster(ROOM, limit, []),
                Error,
                'limit',
            )
        }
        const ids = Array.from(
            { length: MAX_ROSTER_READ_SELF_IDS + 1 },
            (_, i) => i,
        )
        await assertRejects(
            async () => await driver.readRoster(ROOM, 1, ids),
            Error,
            'self ids',
        )
        assertEquals(log, [], 'no command was issued')
    })
})

// ─── one contract, both shipped drivers ──────────────────────────────────────

/** A shipped driver under the shared contract, and how to release it. */
interface ContractSubject {
    readRoster: RedisBroadcastDriver['readRoster']
    holdMember(channel: string, member: PresenceMember): unknown
    close(): Promise<void>
}

const CONTRACT_SUBJECTS: [string, () => ContractSubject][] = [
    ['memory', () => {
        const driver = new MemoryBroadcastDriver()
        return {
            // `async` turns the driver's synchronous throw into a rejection,
            // so one set of assertions reads both drivers.
            readRoster: async (...args) => await driver.readRoster(...args),
            holdMember: (channel, m) => driver.holdMember(channel, m),
            close: () => Promise.resolve(),
        }
    }],
    ['redis over FakeRedis', () => {
        const redis = new FakeRedis()
        const driver = new RedisBroadcastDriver(redis, redis.subscriberFor(), {
            prefix: 'app:rt',
        })
        return {
            readRoster: (...args) => driver.readRoster(...args),
            holdMember: (channel, m) => driver.holdMember(channel, m),
            close: async () => {
                redis.assertNoRejections()
                await driver.close()
            },
        }
    }],
]

for (const [name, make] of CONTRACT_SUBJECTS) {
    Deno.test(`#341 readRoster contract — ${name}`, async () => {
        const driver = make()
        try {
            for (let id = 1; id <= 5; id++) {
                await driver.holdMember(ROOM, member(id))
            }

            const window = await driver.readRoster(ROOM, 3, [])
            assertEquals(window.members.length, 3, 'min(limit, total) members')
            assertEquals(
                new Set(window.members.map((m) => String(m.id))).size,
                3,
                'one entry per String(id)',
            )
            assertEquals(window.total, 5, 'total is the room, not the window')
            assertEquals(window.selves, [])

            const whole = await driver.readRoster(ROOM, 5, [])
            assertEquals(whole.members.length, 5, 'a room that fits is whole')
            assertEquals(whole.total, 5)

            // Repeated, mixed-type and absent ids: one self per String(id).
            const mixed = await driver.readRoster(
                ROOM,
                1,
                [5, '5', 4, 'absent', '4', 4],
            )
            assertEquals(
                [...mixed.selves].sort((a, b) => Number(a.id) - Number(b.id)),
                [member(4), member(5)],
                'each held id once, however many times and types it was asked',
            )

            assertEquals(await driver.readRoster('presence-none', 3, [1]), {
                members: [],
                total: 0,
                selves: [],
            })

            for (const limit of BAD_LIMITS) {
                await assertRejects(
                    () => driver.readRoster(ROOM, limit, []),
                    Error,
                    'limit',
                )
            }
            const ids = Array.from(
                { length: MAX_ROSTER_READ_SELF_IDS + 1 },
                (_, i) => i,
            )
            await assertRejects(
                () => driver.readRoster(ROOM, 1, ids),
                Error,
                'self ids',
            )
            const atCap = await driver.readRoster(ROOM, 1, ids.slice(1))
            assertEquals(atCap.selves.length, 5, 'exactly the cap is accepted')
        } finally {
            await driver.close()
        }
    })
}

Deno.test("#341 redis readRoster refuses an entry under another requested id's field (FR-001, S3)", async () => {
    await withRedis(async (driver, redis) => {
        await seed(driver, 1)
        // Field `7` holds an entry naming `9`, and `9` has no field of its
        // own. Both ids are requested. Matching against the SET of wanted ids
        // would hand `9` an entry only field `7` vouches for.
        await redis.command(
            'HSET',
            PRESENCE_KEY,
            '7',
            JSON.stringify({ member: member(9), owner: 'x' }),
        )
        const window = await driver.readRoster(ROOM, 5, [7, 9])
        assertEquals(window.selves, [], 'a self is proven by its own field')
    })
})
