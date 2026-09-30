/**
 * @fileoverview #350 — a presence member is exactly the JSON round trip of
 * `{ id, info? }`, made once at admission, and never the object `authorize()`
 * returned.
 *
 * Before #350 the manager stored and shipped the authorizer's object as-is.
 * Every own key of a raw row reached same-instance subscribers and the Redis
 * roster at rest; one extra key and no `info` reached every peer; a `toJSON`
 * decided what shipped; an `info` that serialized to a non-object joined
 * locally while every peer dropped it; and a getter `id` passed the checks with
 * one value and was stored as another.
 *
 * Groups, per the architect-expert disposition on the issue:
 *
 * - (a) an own key beside `id`/`info` throws `PresenceMemberShapeError`, names
 *   the key, never echoes a value, and leaves nothing behind.
 * - (b) a `toJSON` cannot decide what ships: own keys `id`/`info`, a `toJSON`
 *   answering `{ id, secret }`, and `secret` reaches nothing.
 * - (c) `id` and `info` are each read ONCE: a getter answering differently on
 *   a second read cannot bypass #306 or #346.
 * - (d) sender and receiver agree, on the parsed wire form, across two
 *   instances.
 * - (e) the Redis frame ingest drops a two-key `{ id, smuggled }` member.
 * - (f) the roster read skips a non-object `info` and keeps its reduction.
 * - (g) admission precedes the caps.
 *
 * Plus `admitPresenceMember` as a pure function, and the negative controls.
 *
 * @module @lockness/realtime/tests/presence_member_admission_350
 */

import {
    assert,
    assertEquals,
    assertInstanceOf,
    assertNotStrictEquals,
    assertRejects,
    assertThrows,
} from '@std/assert'
import { ChannelLimitError, ChannelManager } from '../manager.ts'
import {
    admitPresenceMember,
    PresenceMemberIdError,
    PresenceMemberShapeError,
    PresenceMemberSizeError,
} from '../presence_member.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { Authorizer, PresenceMember } from '../channel.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import * as publicApi from '../mod.ts'

interface User {
    id: number
}

const ROOM = 'presence-room'
const PREFIX = 'app:rt'
const CONTROL_TOPIC = `${PREFIX}__control`
const SECRET = 'deployment-secret-with-enough-entropy'
/** A value that must never reach a frame, a roster value, a log or an error. */
const SENTINEL = 'sentinel-350-secret'
/** A 500-character id: five times past #306's bound. */
const LONG_ID = 'x'.repeat(500)

/** The identity whose authorizer answer is the candidate under test. */
const JOINER = 1
/** The observer on the joiner's own instance. */
const OBSERVER_A = 2
/** The observer on the peer instance (Redis only). */
const OBSERVER_B = 3
/** A second valid joiner: the positive control for "nothing was written". */
const CONTROL = 4

const presenceKey = (channel: string) => `${PREFIX}__presence:${channel}`

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

/** Let the fake broker's pub/sub round-trip settle. */
async function settle(): Promise<void> {
    for (let i = 0; i < 5; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0))
    }
}

/** Run `work` with `console.warn` captured; returns what was warned. */
async function capturingWarns<T>(
    work: () => Promise<T>,
): Promise<{ result: T; warns: string[] }> {
    const warn = console.warn
    const warns: string[] = []
    console.warn = (...args: unknown[]) => void warns.push(args.join(' '))
    try {
        return { result: await work(), warns }
    } finally {
        console.warn = warn
    }
}

type PresenceView = { presence: Map<string, Map<string, PresenceMember>> }

/** The manager's local presence map — private, read for the storage proof. */
const presenceOf = (m: ChannelManager<User>): PresenceView['presence'] =>
    (m as unknown as PresenceView).presence

/** The `joined` frames a connection received for the member `id`. */
function joinedFor(c: Recording, id: string | number): PresenceMember[] {
    return c.received
        .filter((f) => f.type === 'presence' && f.action === 'joined')
        .map((f) => f.member as PresenceMember)
        .filter((m) => String(m?.id) === String(id))
}

/**
 * A broker command that WRITES presence state: a control `PUBLISH`, an `HSET`,
 * or an `EVAL` whose script writes (#351's classification — the read-roster
 * script is an `EVAL` too, and must not count).
 */
function isWriteShaped([cmd, first]: string[]): boolean {
    if (cmd === 'PUBLISH') return first === CONTROL_TOPIC
    if (cmd === 'HSET') return true
    return cmd === 'EVAL' &&
        /redis\.call\('(HSET|HDEL|SADD|SREM)'/.test(first ?? '')
}

/** Wrap a driver so its roster holds and control publishes are counted. */
function counting<D extends BroadcastDriver>(
    driver: D,
    count: { n: number },
): D {
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

/** One room under test, observers already in it. */
interface Room {
    readonly channel: string
    /** The manager the joiner subscribes on. */
    readonly joinOn: ChannelManager<User>
    /** An observer on the joiner's instance. */
    readonly local: Recording
    /** An observer on the peer instance — Redis only. */
    readonly peer: Recording | undefined
    /** Presence writes since the observers joined. */
    writes(): number
    /** Every byte handed to the roster or the control plane, as one string. */
    atRest(): Promise<string>
    /** The roster as a reader sees it — the PEER's read on Redis. */
    rosterRead(): Promise<PresenceMember[]>
    /** The roster's hash FIELDS (Redis) or the read's ids (memory). */
    rosterKeys(): Promise<string[]>
    close(): Promise<void>
}

type Kind = 'memory' | 'fake Redis'
const KINDS: readonly Kind[] = ['memory', 'fake Redis']

/** An authorizer answering `candidate()` for the joiner, a plain id otherwise. */
function authorizerFor(candidate: () => unknown): Authorizer<User> {
    return ((identity: User | null) =>
        identity?.id === JOINER
            ? candidate()
            : { id: `observer-${identity?.id}` }) as unknown as Authorizer<
            User
        >
}

async function makeRoom(
    kind: Kind,
    candidate: () => unknown,
    channel = ROOM,
    options: { maxChannelsPerConnection?: number } = {},
): Promise<Room> {
    const authorize = authorizerFor(candidate)
    if (kind === 'memory') {
        const count = { n: 0 }
        const driver = counting(new MemoryBroadcastDriver(), count)
        const m = new ChannelManager<User>({ driver, authorize, ...options })
        const local = conn('obs-a', OBSERVER_A)
        m.register(local)
        assertEquals((await m.subscribe(local, channel)).ok, true)
        const baseline = count.n
        const read = async () =>
            (await driver.readRoster(channel, 1_000, [])).members
        return {
            channel,
            joinOn: m,
            local,
            peer: undefined,
            writes: () => count.n - baseline,
            atRest: async () =>
                JSON.stringify(await read()) +
                JSON.stringify([...presenceOf(m).get(channel)?.values() ?? []]),
            rosterRead: read,
            rosterKeys: async () => (await read()).map((x) => String(x.id)),
            close: () => Promise.resolve(),
        }
    }
    const redis = new FakeRedis()
    const drivers = [0, 1].map(() =>
        new RedisBroadcastDriver(
            { command: redis.command },
            redis.subscriberFor(),
            { prefix: PREFIX, control: { secret: SECRET } },
        )
    )
    const [a, b] = drivers.map((driver) =>
        new ChannelManager<User>({ driver, authorize, ...options })
    )
    const local = conn('obs-a', OBSERVER_A)
    a.register(local)
    const peer = conn('obs-b', OBSERVER_B)
    b.register(peer)
    assertEquals((await a.subscribe(local, channel)).ok, true)
    assertEquals((await b.subscribe(peer, channel)).ok, true)
    await settle()
    const writeCount = () => redis.commandLog().filter(isWriteShaped).length
    const baseline = writeCount()
    return {
        channel,
        joinOn: a,
        local,
        peer,
        writes: () => writeCount() - baseline,
        atRest: async () =>
            JSON.stringify(redis.commandLog()) +
            JSON.stringify(
                await redis.command('HGETALL', presenceKey(channel)),
            ),
        rosterRead: async () =>
            (await drivers[1].readRoster(channel, 1_000, [])).members,
        rosterKeys: async () => {
            const reply = await redis.command(
                'HGETALL',
                presenceKey(channel),
            ) as { value: { value: string }[] }
            return reply.value
                .filter((_, i) => i % 2 === 0)
                .map((field) => field.value)
        },
        close: async () => {
            for (const driver of drivers) await driver.close()
            redis.assertNoRejections()
        },
    }
}

/** Assert the joiner left no trace, then prove the counter can move. */
async function assertNothingJoined(room: Room, memberId: string | number) {
    await settle()
    assertEquals(room.writes(), 0, 'no roster write, no publish')
    assertEquals(
        room.joinOn.connectionCount,
        2,
        'only the observer and the joiner, each registered at open (#370)',
    )
    assertEquals(
        presenceOf(room.joinOn).get(room.channel)?.has('joiner') ?? false,
        false,
        'no local presence entry',
    )
    assert(
        !(await room.rosterRead()).some((m) =>
            String(m.id) === String(memberId)
        ),
        'the roster holds no such member',
    )
    assertEquals(joinedFor(room.local, memberId), [], 'no local joined')
    if (room.peer) {
        assertEquals(joinedFor(room.peer, memberId), [], 'no peer joined')
    }
    // POSITIVE CONTROL, last: a valid join on the same room moves the counter.
    const before = room.writes()
    const control = conn('control', CONTROL)
    room.joinOn.register(control)
    const ok = await room.joinOn.subscribe(control, room.channel)
    assertEquals(ok.ok, true, 'CONTROL: a valid member joins')
    assert(room.writes() > before, 'CONTROL: an admitted join moves writes()')
}

// --- (a) an own key beside id/info is refused -------------------------------

for (const kind of KINDS) {
    Deno.test(`#350 (a) ${kind}: a member with an own key beside id and info throws PresenceMemberShapeError, names the key, never echoes its value, and writes nothing`, async () => {
        const room = await makeRoom(
            kind,
            () => ({ id: 'u1', info: {}, secret: SENTINEL }),
        )
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const error = await assertRejects(
                () => room.joinOn.subscribe(joiner, room.channel),
                PresenceMemberShapeError,
            )
            assertEquals(
                error.message,
                'realtime: a presence member may carry only `id` and ' +
                    '`info`, and this one has 1 other own key: "secret" ' +
                    '(#350). Their values are NOT echoed here — they are ' +
                    'application data and this message reaches logs. ' +
                    'Everything the member carries reaches every subscriber ' +
                    'of the room, so a raw database row would ship every ' +
                    'column; it is refused before anything is written. ' +
                    'Return `{ id, info }` and declare in `info` what the ' +
                    'room may see, e.g. `{ id: row.id, info: { name: ' +
                    'row.displayName } }`. On a `private-*` channel no ' +
                    'member is used: return `true` to admit (#357).',
                'the extra key is named, and its value is not',
            )
            assert(
                !error.message.includes(SENTINEL),
                `the value is never echoed. Got: ${error.message}`,
            )
            assertEquals(error.name, 'PresenceMemberShapeError')
            assertEquals(joiner.received, [], 'nothing sent to the joiner')
            await assertNothingJoined(room, 'u1')
            assert(!(await room.atRest()).includes(SENTINEL))
        } finally {
            await room.close()
        }
    })
}

// --- (b) a toJSON cannot decide what ships -----------------------------------

/** Own keys exactly `id` and `info`; `toJSON` says something else. */
class LeakyMember {
    id = 'u1'
    info = { name: 'Ada' }
    toJSON(): unknown {
        return { id: this.id, secret: SENTINEL }
    }
}

for (const kind of KINDS) {
    Deno.test(`#350 (b) ${kind}: a member whose toJSON answers { id, secret } joins as { id, info }, and secret reaches no frame, snapshot or roster value`, async () => {
        const room = await makeRoom(kind, () => new LeakyMember())
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const result = await room.joinOn.subscribe(joiner, room.channel)
            assert(result.ok && result.here !== undefined)
            await settle()
            const expected = { id: 'u1', info: { name: 'Ada' } }

            const local = joinedFor(room.local, 'u1')
            assertEquals(local, [expected], 'the local joined is the pair')
            if (room.peer) {
                assertEquals(
                    joinedFor(room.peer, 'u1'),
                    local,
                    "the peer's joined equals the local one",
                )
            }
            const own = result.here.members.find((m) => m.id === 'u1')
            assertEquals(own, expected, 'the here snapshot holds the pair')
            assert(!JSON.stringify(result.here).includes(SENTINEL))

            const stored = presenceOf(room.joinOn).get(room.channel)
                ?.get('joiner')
            assert(stored !== undefined)
            assert(
                !(stored instanceof LeakyMember),
                'the stored member is the parsed copy, not the object',
            )
            assertEquals(Object.getPrototypeOf(stored), Object.prototype)
            assertEquals(stored, expected)

            assert(
                (await room.rosterRead()).some((m) =>
                    JSON.stringify(m) === JSON.stringify(expected)
                ),
                'the roster read holds the pair',
            )
            for (const c of [room.local, room.peer, joiner]) {
                assert(!JSON.stringify(c?.received ?? []).includes(SENTINEL))
            }
            assert(
                !(await room.atRest()).includes(SENTINEL),
                'no roster value or control payload carries secret',
            )
        } finally {
            await room.close()
        }
    })
}

// --- (c) id and info are each read once --------------------------------------

/** A member whose `id` getter answers `answers[n]` on its n-th read. */
function flippingId(answers: readonly unknown[], reads: { n: number }) {
    return {
        get id(): unknown {
            const value = answers[Math.min(reads.n, answers.length - 1)]
            reads.n++
            return value
        },
        info: { name: 'Ada' },
    }
}

for (const kind of KINDS) {
    Deno.test(`#350 (c) ${kind}: a getter id answering 'ok' then 500 characters is admitted as 'ok' everywhere and read once`, async () => {
        const reads = { n: 0 }
        const room = await makeRoom(
            kind,
            () => flippingId(['ok', LONG_ID], reads),
        )
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const result = await room.joinOn.subscribe(joiner, room.channel)
            assert(result.ok && result.here !== undefined)
            await settle()

            assert(
                (await room.rosterKeys()).includes('ok'),
                'the roster field is ok',
            )
            assert(
                !(await room.rosterKeys()).includes(LONG_ID),
                'the 500-character id is not a roster field (#306 bypass)',
            )
            assertEquals(
                presenceOf(room.joinOn).get(room.channel)?.get('joiner')?.id,
                'ok',
                'the local map holds ok',
            )
            assert(result.here.members.some((m) => m.id === 'ok'))
            assert(!result.here.members.some((m) => m.id === LONG_ID))
            assertEquals(
                joinedFor(room.local, 'ok').map((m) => m.id),
                ['ok'],
                'the local joined is ok',
            )
            if (room.peer) {
                assertEquals(
                    joinedFor(room.peer, 'ok').map((m) => m.id),
                    ['ok'],
                    'the peer joined is ok',
                )
            }
            assert(!(await room.atRest()).includes(LONG_ID))
            assertEquals(reads.n, 1, 'the id is read exactly once')
        } finally {
            await room.close()
        }
    })

    Deno.test(`#350 (c) ${kind}: a getter id answering 500 characters first is refused with PresenceMemberIdError`, async () => {
        const reads = { n: 0 }
        const room = await makeRoom(
            kind,
            () => flippingId([LONG_ID, 'ok'], reads),
        )
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            await assertRejects(
                () => room.joinOn.subscribe(joiner, room.channel),
                PresenceMemberIdError,
            )
            await assertNothingJoined(room, 'ok')
        } finally {
            await room.close()
        }
    })

    Deno.test(`#350 (c) ${kind}: a getter id whose second answer is null never produces a 'null' key`, async () => {
        const reads = { n: 0 }
        const room = await makeRoom(kind, () => flippingId(['ok', null], reads))
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            assertEquals(
                (await room.joinOn.subscribe(joiner, room.channel)).ok,
                true,
            )
            await settle()
            const keys = await room.rosterKeys()
            assert(keys.includes('ok'), `the member is ok. Got: ${keys}`)
            assert(!keys.includes('null'), `no 'null' field. Got: ${keys}`)
            assertEquals(
                [...presenceOf(room.joinOn).get(room.channel)!.values()]
                    .map((m) => m.id).filter((id) => id === null),
                [],
            )
            assert(!joinedFor(room.local, 'null').length)
            if (room.peer) assert(!joinedFor(room.peer, 'null').length)
        } finally {
            await room.close()
        }
    })

    Deno.test(`#350 (c) ${kind}: an info getter answering differently on a second read is read once, and only its first answer ships`, async () => {
        const reads = { n: 0 }
        const room = await makeRoom(kind, () => ({
            id: 'u1',
            get info(): unknown {
                reads.n++
                return reads.n === 1 ? { name: 'Ada' } : { secret: SENTINEL }
            },
        }))
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const result = await room.joinOn.subscribe(joiner, room.channel)
            assert(result.ok && result.here !== undefined)
            await settle()
            const expected = { id: 'u1', info: { name: 'Ada' } }
            assertEquals(joinedFor(room.local, 'u1'), [expected])
            if (room.peer) assertEquals(joinedFor(room.peer, 'u1'), [expected])
            assertEquals(
                result.here.members.find((m) => m.id === 'u1'),
                expected,
            )
            assert(!(await room.atRest()).includes(SENTINEL))
            assertEquals(reads.n, 1, 'info is read exactly once')
        } finally {
            await room.close()
        }
    })
}

Deno.test('#350 (c) admitPresenceMember on a counting Proxy: ownKeys once, get id once, get info once', () => {
    const traps = { ownKeys: 0, id: 0, info: 0, other: [] as string[] }
    const candidate = new Proxy({ id: 'u1', info: { name: 'Ada' } }, {
        ownKeys(target) {
            traps.ownKeys++
            return Reflect.ownKeys(target)
        },
        get(target, key, receiver) {
            if (key === 'id') traps.id++
            else if (key === 'info') traps.info++
            else traps.other.push(String(key))
            return Reflect.get(target, key, receiver)
        },
    })
    const admitted = admitPresenceMember(candidate, 4096)
    assertEquals(admitted, { id: 'u1', info: { name: 'Ada' } })
    assertEquals(traps.ownKeys, 1, 'ownKeys')
    assertEquals(traps.id, 1, 'get id')
    assertEquals(traps.info, 1, 'get info')
    assertEquals(traps.other, [], 'no other property is read (no toJSON)')
})

/**
 * An `info` whose own `toJSON` answers `{ name: 'Ada' }` the first time and a
 * secret every time after, counting its calls.
 */
function flippingToJson(calls: { n: number }) {
    return {
        toJSON(): unknown {
            calls.n++
            return calls.n === 1 ? { name: 'Ada' } : { secret: SENTINEL }
        },
    }
}

Deno.test('#350 (c) admitPresenceMember: a nested info.toJSON runs once, and the member is its first answer', () => {
    const calls = { n: 0 }
    const admitted = admitPresenceMember(
        { id: 'u1', info: flippingToJson(calls) },
        4096,
    )
    assertEquals(calls.n, 1, 'info.toJSON runs exactly once')
    assertEquals(admitted, { id: 'u1', info: { name: 'Ada' } })
    assertEquals(
        JSON.stringify(admitted),
        '{"id":"u1","info":{"name":"Ada"}}',
        'a later serialization cannot reach toJSON again',
    )
    assertEquals(calls.n, 1, 'still once after a later serialization')
})

for (const kind of KINDS) {
    Deno.test(`#350 (c) ${kind}: a nested info.toJSON answering differently on a second call runs once, and the stored member is its first answer`, async () => {
        const calls = { n: 0 }
        const room = await makeRoom(kind, () => ({
            id: 'u1',
            info: flippingToJson(calls),
        }))
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const result = await room.joinOn.subscribe(joiner, room.channel)
            assert(result.ok && result.here !== undefined)
            await settle()
            const expected = { id: 'u1', info: { name: 'Ada' } }
            assertEquals(
                presenceOf(room.joinOn).get(room.channel)?.get('joiner'),
                expected,
                'the stored member is the first answer',
            )
            assertEquals(joinedFor(room.local, 'u1'), [expected])
            if (room.peer) assertEquals(joinedFor(room.peer, 'u1'), [expected])
            assert(!(await room.atRest()).includes(SENTINEL))
            assertEquals(calls.n, 1, 'info.toJSON runs exactly once')
        } finally {
            await room.close()
        }
    })
}

// --- (d) sender and receiver agree -------------------------------------------

class HiddenIdMember {
    readonly #id: string
    constructor(id: string) {
        this.#id = id
    }
    get id(): string {
        return this.#id
    }
}

const AGREED: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['{ id }', () => ({ id: 'u1' }), '{"id":"u1"}'],
    [
        '{ id, info: {} }',
        () => ({ id: 'u1', info: {} }),
        '{"id":"u1","info":{}}',
    ],
    [
        '{ id: 7, info: { n: 1 } }',
        () => ({ id: 7, info: { n: 1 } }),
        '{"id":7,"info":{"n":1}}',
    ],
    [
        'a class with a #id getter',
        () => new HiddenIdMember('u1'),
        '{"id":"u1"}',
    ],
    [
        '{ id, info: undefined }',
        () => ({ id: 'u1', info: undefined }),
        '{"id":"u1"}',
    ],
]

for (const [name, candidate, wire] of AGREED) {
    Deno.test(`#350 (d) accepted ${name}: A's local joined, B's joined and B's roster read are the same JSON`, async () => {
        const room = await makeRoom('fake Redis', candidate)
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            assertEquals(
                (await room.joinOn.subscribe(joiner, room.channel)).ok,
                true,
            )
            await settle()
            const id = JSON.parse(wire).id
            const local = joinedFor(room.local, id).map((m) =>
                JSON.stringify(m)
            )
            const peer = joinedFor(room.peer!, id).map((m) => JSON.stringify(m))
            const read = (await room.rosterRead())
                .filter((m) => String(m.id) === String(id))
                .map((m) => JSON.stringify(m))
            assertEquals(local, [wire], "A's local joined")
            assertEquals(peer, [wire], "B's joined")
            assertEquals(read, [wire], "B's roster read")
        } finally {
            await room.close()
        }
    })
}

const REFUSED_INFO: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['info: new Date(0)', () => new Date(0), 'string'],
    ['info: null', () => null, 'null'],
    ['info: []', () => [], 'array'],
    ["info: { toJSON: () => 'x' }", () => ({ toJSON: () => 'x' }), 'string'],
    // The value carries the sentinel: the message names the type, never it.
    ['info: [SENTINEL]', () => [SENTINEL], 'array'],
    [
        'info: { toJSON: () => SENTINEL }',
        () => ({ toJSON: () => SENTINEL }),
        'string',
    ],
]

for (const [name, info, label] of REFUSED_INFO) {
    Deno.test(`#350 (d) refused ${name}: PresenceMemberShapeError on A naming ${label}, nothing on B`, async () => {
        const room = await makeRoom(
            'fake Redis',
            () => ({ id: 'u1', info: info() }),
        )
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const error = await assertRejects(
                () => room.joinOn.subscribe(joiner, room.channel),
                PresenceMemberShapeError,
            )
            assert(
                error.message.startsWith(
                    "realtime: a presence member's `info` must serialize to " +
                        'a JSON object, and this one serializes to a value ' +
                        `of type ${label} (#350).`,
                ),
                `the parsed info's type is named. Got: ${error.message}`,
            )
            assert(
                !error.message.includes(SENTINEL),
                `the info's value is never echoed. Got: ${error.message}`,
            )
            await assertNothingJoined(room, 'u1')
        } finally {
            await room.close()
        }
    })
}

// --- (e) the frame ingest ----------------------------------------------------

Deno.test('#350 (e) frame ingest: a signed frame carrying a two-key { id, smuggled } member is dropped at the shape gate', async () => {
    const redis = new FakeRedis()
    const make = () =>
        new RedisBroadcastDriver(
            { command: redis.command },
            redis.subscriberFor(),
            { prefix: PREFIX, control: { secret: SECRET } },
        )
    const a = make()
    const b = make()
    try {
        const ingested: ControlMessage[] = []
        b.onControl((control) => void ingested.push(control))
        const { warns } = await capturingWarns(async () => {
            await a.publishControl({
                kind: 'presence-join',
                target: 'c1',
                channel: 'presence-smuggled',
                member: {
                    id: 'u1',
                    smuggled: SENTINEL,
                } as unknown as PresenceMember,
            })
            // CONTROL: the same frame with the pair is ingested.
            await a.publishControl({
                kind: 'presence-join',
                target: 'c2',
                channel: 'presence-control',
                member: { id: 'u2', info: {} },
            })
            await settle()
        })
        assertEquals(ingested.map((c) => c.channel), ['presence-control'])
        assert(
            warns.some((w) => w.includes('invalid shape')),
            `the drop is the shape gate's. Got: ${warns}`,
        )
    } finally {
        await a.close()
        await b.close()
        redis.assertNoRejections()
    }
})

// --- (f) the roster read -----------------------------------------------------

Deno.test('#350 (f) roster read: an entry whose info is not an object is skipped with a WARN naming its type; an entry with extra keys is reduced to { id, info }', async () => {
    const redis = new FakeRedis()
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, control: { secret: SECRET } },
    )
    const channel = 'presence-legacy'
    const INFO_TEXT = 'sentinel-350-info-text'
    try {
        await redis.command(
            'HSET',
            presenceKey(channel),
            'u1',
            JSON.stringify({
                member: { id: 'u1', info: INFO_TEXT },
                owner: 'x',
            }),
        )
        await redis.command(
            'HSET',
            presenceKey(channel),
            'u2',
            JSON.stringify({
                member: { id: 'u2', info: {}, email: SENTINEL },
                owner: 'x',
            }),
        )
        const { result, warns } = await capturingWarns(() =>
            driver.readRoster(channel, 1_000, [])
        )
        assertEquals(result.members, [{ id: 'u2', info: {} }])
        assertEquals(warns.length, 1, `one WARN, for u1. Got: ${warns}`)
        assert(
            warns[0].startsWith(
                'realtime: skipped a malformed roster entry on ' +
                    `${channel}: info of type string is not an object`,
            ),
            `the WARN names the info's type. Got: ${warns}`,
        )
        assert(
            !warns.some((w) => w.includes(INFO_TEXT) || w.includes(SENTINEL)),
            `the WARN never echoes the entry. Got: ${warns}`,
        )
    } finally {
        await driver.close()
        redis.assertNoRejections()
    }
})

// --- (g) ordering ------------------------------------------------------------

Deno.test('#350 (g) a full connection whose authorizer returns a raw row gets PresenceMemberShapeError, not ChannelLimitError', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        maxChannelsPerConnection: 1,
        authorize: authorizerFor(() => ({
            id: 7,
            email: SENTINEL,
            passwordHash: SENTINEL,
        })),
    })
    const c = conn('c1', JOINER)
    m.register(c)
    assertEquals((await m.subscribe(c, 'news')).ok, true)
    // CONTROL: the cap really is full — a public join is refused by it.
    await assertRejects(() => m.subscribe(c, 'weather'), ChannelLimitError)
    const error = await assertRejects(() => m.subscribe(c, ROOM), Error)
    assertInstanceOf(
        error,
        PresenceMemberShapeError,
        `the member is refused on its own terms first. Got: ${error.name}`,
    )
})

// --- negative controls -------------------------------------------------------

for (const kind of KINDS) {
    Deno.test(`#350 control ${kind}: authorize() returning true still admits as { id: connection.id }`, async () => {
        const room = await makeRoom(kind, () => true)
        try {
            const joiner = conn('joiner', JOINER)
            room.joinOn.register(joiner)
            const result = await room.joinOn.subscribe(joiner, room.channel)
            assert(result.ok && result.here !== undefined)
            await settle()
            assertEquals(
                result.here.members.find((m) => m.id === 'joiner'),
                { id: 'joiner' },
            )
            assertEquals(joinedFor(room.local, 'joiner'), [{ id: 'joiner' }])
            if (room.peer) {
                assertEquals(joinedFor(room.peer, 'joiner'), [{ id: 'joiner' }])
            }
        } finally {
            await room.close()
        }
    })
}

// --- admitPresenceMember, the pure function ---------------------------------

Deno.test('#350 admit: the result is a fresh parsed copy, never the candidate or its info', () => {
    const info = { name: 'Ada', tags: ['a'] }
    const candidate = { id: 'u1', info }
    const admitted = admitPresenceMember(candidate, 4096)
    assertEquals(admitted, { id: 'u1', info: { name: 'Ada', tags: ['a'] } })
    assertNotStrictEquals(admitted, candidate)
    assertNotStrictEquals(admitted.info, info)
    info.name = 'changed'
    assertEquals(admitted.info, { name: 'Ada', tags: ['a'] })
})

Deno.test('#350 admit: an absent or undefined info yields no info key', () => {
    for (const candidate of [{ id: 7 }, { id: 7, info: undefined }]) {
        const admitted = admitPresenceMember(candidate, 4096)
        assertEquals(Object.keys(admitted), ['id'])
    }
})

Deno.test('#350 admit: extra keys are checked BEFORE the id', () => {
    assertThrows(
        () => admitPresenceMember({ id: null, extra: 1 }, 4096),
        PresenceMemberShapeError,
    )
})

Deno.test('#350 admit: the message names at most three extra keys, the count, and no value', () => {
    const error = assertThrows(
        () =>
            admitPresenceMember({
                id: 'u1',
                a: SENTINEL,
                b: SENTINEL,
                c: SENTINEL,
                d: SENTINEL,
            }, 4096),
        PresenceMemberShapeError,
    )
    for (const key of ['a', 'b', 'c']) {
        assert(
            error.message.includes(`"${key}"`),
            `names ${key}: ${error.message}`,
        )
    }
    assert(!error.message.includes('"d"'), `at most three: ${error.message}`)
    assert(
        error.message.startsWith(
            'realtime: a presence member may carry only `id` and `info`, and ' +
                'this one has 4 other own keys: "a", "b", "c", and 1 more ' +
                '(#350).',
        ),
        `the count, and the rest summed: ${error.message}`,
    )
    assert(!error.message.includes(SENTINEL), `no value: ${error.message}`)
})

Deno.test('#350 admit: an extra key name is log-encoded', () => {
    const error = assertThrows(
        () => admitPresenceMember({ id: 'u1', ['evil\nline']: 1 }, 4096),
        PresenceMemberShapeError,
    )
    assert(!error.message.includes('\n'), `no raw newline: ${error.message}`)
})

Deno.test('#350 admit: the id is checked before the serialization', () => {
    // `JSON.stringify` throws on a bigint: a size error here would mean the
    // id rule ran after it.
    assertThrows(
        () => admitPresenceMember({ id: 1n }, 4096),
        PresenceMemberIdError,
    )
})

Deno.test('#350 admit: the byte bound is exact, and measured on the serialized pair', () => {
    const bytes =
        new TextEncoder().encode('{"id":"u1","info":{"n":"é"}}').length
    const member = { info: { n: 'é' }, id: 'u1' }
    assertEquals(admitPresenceMember(member, bytes), {
        id: 'u1',
        info: { n: 'é' },
    })
    assertThrows(
        () => admitPresenceMember(member, bytes - 1),
        PresenceMemberSizeError,
    )
})

Deno.test('#350 admit: an info that cannot be serialized is a size error of Infinity', () => {
    const cyclic: Record<string, unknown> = {}
    cyclic.self = cyclic
    for (const info of [cyclic, { n: 1n }]) {
        const error = assertThrows(
            () => admitPresenceMember({ id: 'u1', info }, 4096),
            PresenceMemberSizeError,
        )
        assert(error.message.includes('Infinity'), error.message)
    }
})

Deno.test('#350 admit: an info that serializes to nothing — a function, a symbol, a toJSON answering undefined — is refused, never admitted as { id }', () => {
    const cases: ReadonlyArray<readonly [string, unknown, string]> = [
        ['a function', () => SENTINEL, 'function'],
        ['a symbol', Symbol(SENTINEL), 'symbol'],
        ['a toJSON answering undefined', { toJSON: () => undefined }, 'object'],
    ]
    for (const [name, info, label] of cases) {
        const error = assertThrows(
            () => admitPresenceMember({ id: 'u1', info }, 4096),
            PresenceMemberShapeError,
            undefined,
            name,
        )
        assert(
            error.message.startsWith(
                "realtime: a presence member's `info` must serialize to a " +
                    `JSON object, and this one, a value of type ${label}, ` +
                    'serializes to nothing (#350).',
            ),
            `${name}: the supplied info's type is named. Got: ${error.message}`,
        )
        assert(!error.message.includes(SENTINEL), `${name}: no value echoed`)
    }
})

Deno.test('#350 the three member errors are exported from mod.ts, and they are the classes admission throws', () => {
    assertEquals(publicApi.PresenceMemberIdError, PresenceMemberIdError)
    assertEquals(publicApi.PresenceMemberSizeError, PresenceMemberSizeError)
    assertEquals(publicApi.PresenceMemberShapeError, PresenceMemberShapeError)
    const idError = assertThrows(() => admitPresenceMember({ id: null }, 4096))
    assertInstanceOf(idError, publicApi.PresenceMemberIdError)
    const sizeError = assertThrows(() =>
        admitPresenceMember({ id: 'u1', info: { n: 'x'.repeat(64) } }, 32)
    )
    assertInstanceOf(sizeError, publicApi.PresenceMemberSizeError)
    const shapeError = assertThrows(() =>
        admitPresenceMember({ id: 'u1', extra: 1 }, 4096)
    )
    assertInstanceOf(shapeError, publicApi.PresenceMemberShapeError)
})
