/**
 * @fileoverview #346 — a presence member id is a string or a finite number, and
 * the sender refuses what the receiver would drop.
 *
 * #306 bounded the id by LENGTH and refused a non-finite number, but never
 * checked its TYPE. `String(null)` is `'null'`, `String(undefined)` is
 * `'undefined'` and `String({})` is `'[object Object]'` — each non-empty and
 * under 200 characters — and every consumer keys a member by `String(id)`. So
 * two different people whose `authorize()` handed back a malformed id collapsed
 * into ONE presence entry. On Redis it was worse: the Redis ingest guard
 * `isPlainMember` already refused such an id, so the join answered ok locally
 * and every peer dropped the frame announcing it.
 *
 * One predicate, `isPresenceMemberIdValue`, now decides the type at all three
 * places: the join boundary, the frame ingest and the roster read.
 *
 * - (a) The collapse, on every path it reached — memory, the local fallback, a
 *   roster-less driver and two fake-Redis instances. Each join now throws
 *   `PresenceMemberIdError`, and the state is READ afterwards: `writes()` — ONE
 *   counter over roster holds and control publishes together — stays 0, no
 *   local presence entry, no `connections` entry, nothing sent, and an empty
 *   roster where the path has one to read (memory, fake Redis). Each row then
 *   ends with a positive control (#351): a valid id's join on the same path
 *   moves `writes()` past its value just before that join, so its "0" is not
 *   the silence of an instrument that counts nothing. The control shows the
 *   counter registers an admitted join; it does not show roster holds and
 *   control publishes are each counted on their own.
 * - (b) The value table: what joins, and what is refused — never with a
 *   `TypeError` or `PresenceMemberSizeError`. Every boxed primitive is a row,
 *   `new Boolean(false)` and `Object(Symbol())` included.
 * - (c) Sender and receiver agree, across two fake-Redis instances, through the
 *   frame ingest and through the roster read.
 * - (d) The refusal never echoes a non-primitive id, only its type.
 *
 * Refusal rows assert the TYPE-BRANCH wording (`of type null`), which #306's
 * value-echoing wording cannot produce, so a refusal that comes from anywhere
 * else does not pass for this one.
 *
 * @module @lockness/realtime/tests/presence_member_id_type_346
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { ChannelManager, type SubscribeResult } from '../manager.ts'
import { PresenceMemberIdError } from '../presence_member.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { isPresenceMemberIdValue } from '../protocol.ts'
import type { Authorizer, PresenceMember } from '../channel.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const ROOM = 'presence-room'
const PREFIX = 'app:rt'
const CONTROL_TOPIC = `${PREFIX}__control`
const SECRET = 'deployment-secret-with-enough-entropy'

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

/** A connection that records every frame it received. */
function conn(id: string, userId: number): Recording {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Recording
}

/**
 * An authorizer handing back `{ id }` exactly as given — cast the way a
 * plain-JS app, an `any`-typed row or a nullable column reaches the manager.
 */
function returningId(id: () => unknown): Authorizer<User> {
    return (() => ({ id: id() })) as unknown as Authorizer<User>
}

/** Let the fake broker's pub/sub round-trip settle. */
async function settle(): Promise<void> {
    for (let i = 0; i < 5; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0))
    }
}

type PresenceView = { presence: Map<string, Map<string, PresenceMember>> }

/** The manager's local presence map — private, read for the no-write proof. */
const presenceOf = (m: ChannelManager<User>): PresenceView['presence'] =>
    (m as unknown as PresenceView).presence

/** Run `work` with `console.warn` silenced; a fallback WARN is by design. */
async function quietly<T>(work: () => Promise<T>): Promise<T> {
    const warn = console.warn
    console.warn = () => {}
    try {
        return await work()
    } finally {
        console.warn = warn
    }
}

/** A refused join: the named error, carrying the type-branch wording. */
async function assertRefused(
    join: () => Promise<SubscribeResult>,
    fragment: string,
): Promise<void> {
    const error = await assertRejects(join, PresenceMemberIdError)
    assert(
        error.message.includes(fragment),
        `the message names "${fragment}". Got: ${error.message}`,
    )
    assert(
        error.message.includes('a string or a finite number'),
        `the message states the type rule. Got: ${error.message}`,
    )
}

// --- (a) the collapse, on every path it reached -----------------------------

/** One path under test, and what it can tell us afterwards. */
interface Path {
    /** One manager per identity; on Redis, two instances over one broker. */
    managers: [ChannelManager<User>, ChannelManager<User>]
    /** Roster writes and control publishes the drivers were asked for. */
    writes(): number
    /** The roster's contents, where a roster exists to read. */
    roster(): Promise<PresenceMember[] | undefined>
    close(): Promise<void>
}

/** Wrap a driver so its roster writes and control publishes are counted. */
function counting(driver: BroadcastDriver, count: { n: number }) {
    const hold = driver.holdMember?.bind(driver)
    const publishControl = driver.publishControl?.bind(driver)
    if (hold) {
        driver.holdMember = (channel, member) => {
            count.n++
            return hold(channel, member)
        }
    }
    if (publishControl) {
        driver.publishControl = (control) => {
            count.n++
            return publishControl(control)
        }
    }
    return driver
}

/**
 * A broker command that WRITES presence state: a control `PUBLISH`, an `HSET`,
 * or an `EVAL` whose script writes — the hold script. The read-roster script
 * is an `EVAL` too, and it runs on every admitted join and on every
 * `path.roster()`: counting it let a join that wrote nothing move the counter
 * (#351). A script is classified by the Redis write commands it calls, not
 * matched verbatim, so a reworded hold script still counts and the read-only
 * script never does.
 */
function isWriteShaped([cmd, first]: string[]): boolean {
    if (cmd === 'PUBLISH') return first === CONTROL_TOPIC
    if (cmd === 'HSET') return true
    return cmd === 'EVAL' &&
        /redis\.call\('(HSET|HDEL|SADD|SREM)'/.test(first ?? '')
}

const PATHS: ReadonlyArray<
    readonly [string, (authorize: Authorizer<User>) => Path]
> = [
    ['memory', (authorize) => {
        const count = { n: 0 }
        const driver = counting(new MemoryBroadcastDriver(), count)
        const m = new ChannelManager<User>({ driver, authorize })
        return {
            managers: [m, m],
            writes: () => count.n,
            roster: async () =>
                (await driver.readRoster!(ROOM, 1_000, [])).members,
            close: () => Promise.resolve(),
        }
    }],
    ['local fallback', (authorize) => {
        // A roster-capable driver whose every read rejects: the `here`
        // snapshot falls back to the local view, which #343 dedupes by
        // `String(id)` — the second place the collapse reached.
        const count = { n: 0 }
        const driver = counting({
            publish: () => {},
            onMessage: () => {},
            onControl: () => {},
            publishControl: () => {},
            holdMember: () => Promise.resolve({ arrived: true }),
            releaseMember: () => Promise.resolve({ gone: true }),
            readRoster: (_channel, limit, selfIds) =>
                asWindow(
                    Promise.reject(new Error('broker unreachable')),
                    limit,
                    selfIds,
                ),
        }, count)
        const m = new ChannelManager<User>({ driver, authorize })
        return {
            managers: [m, m],
            writes: () => count.n,
            roster: () => Promise.resolve(undefined),
            close: () => Promise.resolve(),
        }
    }],
    ['roster-less', (authorize) => {
        // No roster ops at all (#342): the local view IS the authority.
        const count = { n: 0 }
        const driver = counting({
            publish: () => {},
            onMessage: () => {},
            onControl: () => {},
            publishControl: () => {},
            watchChannel: () => {},
            unwatchChannel: () => {},
        }, count)
        const m = new ChannelManager<User>({ driver, authorize })
        return {
            managers: [m, m],
            writes: () => count.n,
            roster: () => Promise.resolve(undefined),
            close: () => Promise.resolve(),
        }
    }],
    ['fake Redis', (authorize) => {
        // Two instances over one broker. The shipped symptom here: the join
        // answered ok on A and `isPlainMember` dropped the frame on B.
        const redis = new FakeRedis()
        const drivers = [0, 1].map(() =>
            new RedisBroadcastDriver(
                { command: redis.command },
                redis.subscriberFor(),
                { prefix: PREFIX, control: { secret: SECRET } },
            )
        )
        const managers = drivers.map((driver) =>
            new ChannelManager<User>({ driver, authorize })
        )
        return {
            managers: [managers[0], managers[1]],
            writes: () => redis.commandLog().filter(isWriteShaped).length,
            roster: async () =>
                (await drivers[1].readRoster(ROOM, 1_000, [])).members,
            close: async () => {
                for (const driver of drivers) await driver.close()
                redis.assertNoRejections()
            },
        }
    }],
]

const COLLAPSING: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['null', () => null, 'of type null'],
    ['undefined', () => undefined, 'of type undefined'],
    ['{}', () => ({}), 'of type object'],
]

/**
 * The identity whose authorizer answer is a VALID id — the positive control
 * that proves `writes()` can register a join on this path at all (#351).
 */
const CAROL = 3

for (const [pathName, makePath] of PATHS) {
    for (const [idName, id, fragment] of COLLAPSING) {
        Deno.test(`#346 (a) ${pathName}: two people with a ${idName} member id are each refused, and writes(), presence, connections, roster and frames stay empty`, async () => {
            const path = makePath(
                ((identity: User | null) => ({
                    id: identity?.id === CAROL ? 'carol' : id(),
                })) as unknown as Authorizer<User>,
            )
            try {
                const [onA, onB] = path.managers
                const alice = conn('alice', 1)
                onA.register(alice)
                const bob = conn('bob', 2)
                onB.register(bob)
                // Before #346 both joins answered ok and Bob's `here` listed
                // ONE entry, total 1 — two people merged under `String(id)`.
                await quietly(() =>
                    assertRefused(() => onA.subscribe(alice, ROOM), fragment)
                )
                await quietly(() =>
                    assertRefused(() => onB.subscribe(bob, ROOM), fragment)
                )
                await settle()

                assertEquals(path.writes(), 0, 'no roster write, no publish')
                for (const m of new Set(path.managers)) {
                    assertEquals(
                        presenceOf(m).get(ROOM)?.size ?? 0,
                        0,
                        'the local presence map is unchanged',
                    )
                    // Only the registrations at open are counted (#370).
                    const registered = (m === onA ? 1 : 0) + (m === onB ? 1 : 0)
                    assertEquals(
                        m.connectionCount,
                        registered,
                        'no connection beyond the registrations',
                    )
                }
                const roster = await path.roster()
                if (roster !== undefined) {
                    assertEquals(roster, [], 'the roster holds nobody')
                }
                assertEquals(alice.received, [], 'nothing sent to Alice')
                assertEquals(bob.received, [], 'nothing sent to Bob')

                // POSITIVE CONTROL (#351), LAST so it cannot disturb the
                // reads above: a valid id's join on the same path moves the
                // same counter. Without it, a `writes()` that never counts
                // anything passes the "0" above on every path. `before` is
                // read HERE, not taken as 0: only Carol's own join may move
                // it, not a read above that an over-broad counter caught.
                const carol = conn('carol', CAROL)
                onA.register(carol)
                const before = path.writes()
                const joined = await quietly(() => onA.subscribe(carol, ROOM))
                assertEquals(joined.ok, true, 'CONTROL: a valid id joins')
                assert(
                    path.writes() > before,
                    `CONTROL: an admitted join moves writes() on this path (${before} -> ${path.writes()})`,
                )
            } finally {
                await path.close()
            }
        })
    }
}

// --- (b) the value table -----------------------------------------------------

/** Ids that join — the framework's own path is exercised separately below. */
const ACCEPTED: ReadonlyArray<readonly [string, string | number]> = [
    ['a string', 'u1'],
    ['an email', 'user@example.com'],
    ['0', 0],
    ['-0', -0],
    ['a positive integer', 7],
    ['a negative integer', -3],
    ['a fraction', 1.5],
    ['MAX_SAFE_INTEGER', Number.MAX_SAFE_INTEGER],
    ['1e21 (pinned by #306)', 1e21],
]

for (const [name, id] of ACCEPTED) {
    Deno.test(`#346 (b) ${name} member id joins`, async () => {
        const driver = new MemoryBroadcastDriver()
        const m = new ChannelManager<User>({
            driver,
            authorize: () => ({ id }),
        })
        const c1 = conn('c1', 1)
        m.register(c1)
        assertEquals((await m.subscribe(c1, ROOM)).ok, true)
        assertEquals(
            (await driver.readRoster(ROOM, 1_000, [])).members.map((x) =>
                String(x.id)
            ),
            [String(id)],
        )
    })
}

Deno.test('#346 (b) the framework path — authorize() returning true — joins as the connection id', async () => {
    const driver = new MemoryBroadcastDriver()
    const m = new ChannelManager<User>({ driver, authorize: () => true })
    const c1 = conn('c1', 1)
    m.register(c1)
    assertEquals((await m.subscribe(c1, ROOM)).ok, true)
    assertEquals(
        (await driver.readRoster(ROOM, 1_000, [])).members,
        [{ id: 'c1' }],
    )
})

Deno.test('#346 (b) -0 and 0 are one member, as String(id) keys them', async () => {
    const driver = new MemoryBroadcastDriver()
    const ids = [-0, 0]
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity) => ({ id: ids[identity!.id] }),
    })
    const c0 = conn('c0', 0)
    m.register(c0)
    await m.subscribe(c0, ROOM)
    const c1 = conn('c1', 1)
    m.register(c1)
    const result = await m.subscribe(c1, ROOM)
    assert(result.ok && result.here !== undefined)
    assertEquals(result.here.total, 1)
})

/** Refused ids, each with the wording its message must carry. */
const REFUSED: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['NaN', () => Number.NaN, 'NaN'],
    ['Infinity', () => Number.POSITIVE_INFINITY, 'Infinity'],
    ['-Infinity', () => Number.NEGATIVE_INFINITY, '-Infinity'],
    ['null', () => null, 'of type null'],
    ['undefined', () => undefined, 'of type undefined'],
    ['true', () => true, 'of type boolean'],
    ['false', () => false, 'of type boolean'],
    ['a bigint', () => 1n, 'of type bigint'],
    ['a symbol', () => Symbol('member'), 'of type symbol'],
    ['a function', () => () => 'u1', 'of type function'],
    ['an empty array', () => [], 'of type array'],
    ["['u1']", () => ['u1'], 'of type array'],
    ['a boxed string', () => new String('u1'), 'of type boxed string'],
    ['a boxed number', () => new Number(1), 'of type boxed number'],
    ['a boxed bigint', () => Object(1n), 'of type boxed bigint'],
    // Falsy in its primitive form and truthy boxed — `if (id)` reads it as
    // present.
    ['a boxed boolean', () => new Boolean(false), 'of type boxed boolean'],
    // `String()` on it throws a `TypeError`: the unboxed Symbol reaches
    // `ToString`. The type check must run first.
    ['a boxed symbol', () => Object(Symbol('member')), 'of type boxed symbol'],
    ['{}', () => ({}), 'of type object'],
    ['a null-prototype object', () => Object.create(null), 'of type object'],
    [
        'an object whose toString is a valid id',
        () => ({ toString: () => 'u1' }),
        'of type object',
    ],
]

for (const [name, id, fragment] of REFUSED) {
    Deno.test(`#346 (b) ${name} member id is refused with PresenceMemberIdError`, async () => {
        const driver = new MemoryBroadcastDriver()
        const m = new ChannelManager<User>({
            driver,
            authorize: returningId(id),
        })
        // `assertRejects` with the class: a `TypeError` (a Symbol in a
        // template literal, `String()` on a null-prototype object) or the
        // #326 size error (`JSON.stringify` on a bigint) fails the row.
        const c1 = conn('c1', 1)
        m.register(c1)
        await assertRefused(() => m.subscribe(c1, ROOM), fragment)
        assertEquals(m.connectionCount, 1, 'only the registration is counted')
        assertEquals(presenceOf(m).get(ROOM)?.size ?? 0, 0)
        assertEquals((await driver.readRoster(ROOM, 1_000, [])).members, [])
    })
}

// --- (c) sender and receiver agree ------------------------------------------

/** Two driver instances over one fake broker. */
function twoInstances() {
    const redis = new FakeRedis()
    const make = () =>
        new RedisBroadcastDriver(
            { command: redis.command },
            redis.subscriberFor(),
            { prefix: PREFIX, control: { secret: SECRET } },
        )
    const a = make()
    const b = make()
    return {
        a,
        b,
        close: async () => {
            await a.close()
            await b.close()
            redis.assertNoRejections()
        },
    }
}

Deno.test('#346 (c) frame ingest: a numeric id joined on one instance is announced on the other', async () => {
    const { a, b, close } = twoInstances()
    try {
        const authorize: Authorizer<User> = (identity) => ({
            id: identity!.id === 1 ? 7 : 'observer',
        })
        const onA = new ChannelManager<User>({ driver: a, authorize })
        const onB = new ChannelManager<User>({ driver: b, authorize })
        const observer = conn('observer', 2)
        onB.register(observer)
        assertEquals((await onB.subscribe(observer, ROOM)).ok, true)

        const ada = conn('ada', 1)
        onA.register(ada)
        assertEquals((await onA.subscribe(ada, ROOM)).ok, true)
        await settle()

        // `isPlainMember` on B is what admits the frame; it accepts exactly
        // what the join boundary on A accepted.
        const joined = observer.received.filter((f) =>
            f.type === 'presence' && f.action === 'joined'
        )
        assertEquals(joined.map((f) => (f.member as PresenceMember).id), [7])
    } finally {
        await close()
    }
})

Deno.test('#346 (c) roster read: a numeric id written by one instance is read by the other', async () => {
    const { a, b, close } = twoInstances()
    try {
        const authorize: Authorizer<User> = (identity) => ({
            id: identity!.id === 1 ? 7 : 'observer',
        })
        const onA = new ChannelManager<User>({ driver: a, authorize })
        const onB = new ChannelManager<User>({ driver: b, authorize })
        const ada = conn('ada', 1)
        onA.register(ada)
        assertEquals((await onA.subscribe(ada, ROOM)).ok, true)

        const observer = conn('observer', 2)
        onB.register(observer)
        const result = await onB.subscribe(observer, ROOM)
        assert(result.ok && result.here !== undefined)
        // `#parseRosterValue` is what admits the stored entry.
        assertEquals(
            result.here.members.map((m) => m.id).sort(),
            [7, 'observer'].sort(),
        )
        assertEquals(result.here.total, 2)
    } finally {
        await close()
    }
})

/**
 * Ids a JSON wire can carry, within #306's length bound — the TYPE rule is what
 * the three sites must agree on; length stays join-time only.
 */
const WIRE_VALUES: ReadonlyArray<readonly [string, unknown]> = [
    ['a string', 'u1'],
    ['an email', 'user@example.com'],
    ['0', 0],
    ['a negative fraction', -1.5],
    ['1e21', 1e21],
    ['null', null],
    ['undefined', undefined],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['true', true],
    ['false', false],
    ['{}', {}],
    ['an array', ['u1']],
    ['a nested object', { id: 'u1' }],
]

Deno.test('#346 (c) the join boundary, the frame ingest and the roster read accept exactly one set', async () => {
    const { a, b, close } = twoInstances()
    try {
        const ingested: ControlMessage[] = []
        b.onControl((control) => void ingested.push(control))

        for (const [name, id] of WIRE_VALUES) {
            const channel = `presence-${name.replace(/[^a-z0-9]/gi, '')}`

            const joinManager = new ChannelManager<User>({
                driver: new MemoryBroadcastDriver(),
                authorize: returningId(() => id),
            })
            const c1 = conn('c1', 1)
            joinManager.register(c1)
            const joins = await joinManager.subscribe(c1, channel)
                .then(() => true, (error) => {
                    assert(error instanceof PresenceMemberIdError, `${error}`)
                    return false
                })

            const member = { id } as unknown as PresenceMember
            await quietly(async () => {
                await a.publishControl({
                    kind: 'presence-join',
                    target: 'c1',
                    channel,
                    member,
                })
                await settle()
            })
            const frameAccepted = ingested.some((c) => c.channel === channel)

            await a.holdMember(channel, member)
            const rosterAccepted = await quietly(async () =>
                (await b.readRoster(channel, 1_000, [])).members.length === 1
            )

            assertEquals(joins, isPresenceMemberIdValue(id), name)
            assertEquals(frameAccepted, joins, `frame ingest vs join: ${name}`)
            assertEquals(rosterAccepted, joins, `roster read vs join: ${name}`)
        }
    } finally {
        await close()
    }
})

// --- (d) no echo -------------------------------------------------------------

Deno.test('#346 (d) the message names an object id by type and never echoes it', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: returningId(() => ({ email: 'sentinel-346@example.com' })),
    })
    const c1 = conn('c1', 1)
    m.register(c1)
    const error = await assertRejects(
        () => m.subscribe(c1, ROOM),
        PresenceMemberIdError,
    )
    assert(
        error.message.includes('of type object'),
        `the type is named. Got: ${error.message}`,
    )
    assert(
        !error.message.includes('sentinel-346'),
        'an object id may be a user record, and this message reaches logs',
    )
    assertEquals(error.name, 'PresenceMemberIdError')
})
