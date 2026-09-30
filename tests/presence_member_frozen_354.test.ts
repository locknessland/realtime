/**
 * @fileoverview #354 — a `PresenceMember` is an immutable value: deep-frozen
 * where the package mints it, and shared by reference everywhere after.
 *
 * Before #354 the member in a `here` snapshot was the STORED presence entry on
 * the memory driver, on a roster-less driver and on the local fallback, so an
 * application that decorated one ("add a display field before rendering")
 * changed presence state: every later `here` and the `left` frame on that
 * instance carried the write, and no other instance did. A custom `encode`
 * could do the same through `frame.member`, and on Redis the #333 barrier gave
 * one parsed read to every concurrent caller, so one viewer's write showed up
 * in another viewer's reply.
 *
 * Rows, per the architect-expert disposition on the issue:
 *
 * - (a) memory driver, the state property — written with `Reflect`, which
 *   answers `false` on a frozen target instead of throwing, so the green
 *   state asserts the property and never crashes the test.
 * - (b) the product default: a direct write throws `TypeError`.
 * - (c) the local fallback and (d) a roster-less driver: the rows of (a).
 * - (e) a custom `encode` cannot change stored state through `frame.member`.
 * - (f) Redis: callers sharing one read cannot see each other's writes; every
 *   node the roster read returns is frozen, and so is a swept departure's.
 * - (g) Redis ingest: a peer's `presence-join` member reaches `encode` frozen.
 * - (h) `freezePresenceMember` and `admitPresenceMember` as units, including a
 *   depth a recursive walk cannot reach.
 * - (i) negative controls: `here` and `here.members` stay the caller's, a copy
 *   is writable, and the wire bytes do not change.
 * - (k) types: `id` and `info` are `readonly`.
 *
 * **No "does not alias" assertion anywhere.** Frozen members ARE shared by
 * reference, on purpose — that is what keeps a roster read's cost per read
 * rather than per caller (#333/#341). Row (f) asserts the sharing as its
 * precondition.
 *
 * @module @lockness/realtime/tests/presence_member_frozen_354
 */

import {
    assert,
    assertEquals,
    assertFalse,
    assertStrictEquals,
    assertThrows,
} from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager, type SubscribeResult } from '../manager.ts'
import {
    admitPresenceMember,
    freezePresenceMember,
} from '../presence_member.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { encodeServerMessage } from '../protocol.ts'
import type {
    Authorizer,
    PresenceMember,
    PresenceSnapshot,
} from '../channel.ts'
import type { BroadcastDriver, RosterDeparture } from '../driver.ts'
import type { OutboundFrame } from '../manager.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

interface User {
    id: number
}

const ROOM = 'presence-room'
const PREFIX = 'app:rt'
const SECRET = 'deployment-secret-with-enough-entropy'

/** The joiner every row decorates. */
const JOINER = 1

/** What the joiner's member is, and must remain, everywhere. */
const ORIGINAL: PresenceMember = {
    id: JOINER,
    info: { name: 'Ada', tags: ['a'] },
}

/** A fresh copy of {@link ORIGINAL}: the authorizer's object, never stored. */
const original = (): PresenceMember => structuredClone(ORIGINAL)

/** The joiner answers {@link ORIGINAL}; everyone else a bare id. */
const authorize =
    ((identity: User | null) =>
        identity?.id === JOINER
            ? original()
            : { id: identity?.id ?? 0 }) as Authorizer<User>

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

/** How long {@link until} waits for a condition before failing the row. */
const UNTIL_TIMEOUT_MS = 2_000

/**
 * Wait until `condition` holds, polling on the event loop, and fail loudly —
 * naming `what` — if it does not within {@link UNTIL_TIMEOUT_MS}.
 *
 * A wait on the outcome, not on a count of ticks: a fixed number of yields is
 * a guess at how deep the async chain is, and a guess that goes stale silently
 * turns a row into one that asserts before its event arrived.
 */
async function until(condition: () => boolean, what: string): Promise<void> {
    const deadline = Date.now() + UNTIL_TIMEOUT_MS
    while (!condition()) {
        if (Date.now() >= deadline) {
            throw new Error(
                `timed out after ${UNTIL_TIMEOUT_MS} ms waiting for ${what}`,
            )
        }
        await new Promise((resolve) => setTimeout(resolve, 1))
    }
}

/**
 * {@link until} under a `FakeTime`: advance the fake clock in `stepMs` steps,
 * flushing microtasks between them, until `condition` holds — or fail loudly
 * after `budgetMs` of fake time.
 *
 * Stepping matters, not only the wait. One `tickAsync(3_500)` fires every
 * interval callback due in that span back to back and only then lets their
 * awaited continuations run, so three reconcile passes run concurrently where
 * a real clock would have run them a second apart.
 */
async function untilFaked(
    time: FakeTime,
    condition: () => boolean,
    what: string,
    budgetMs = 10_000,
    stepMs = 100,
): Promise<void> {
    for (let elapsed = 0;; elapsed += stepMs) {
        await time.runMicrotasks()
        if (condition()) return
        if (elapsed >= budgetMs) {
            throw new Error(
                `timed out after ${budgetMs} ms of fake time waiting for ${what}`,
            )
        }
        await time.tickAsync(stepMs)
    }
}

/** The presence snapshot a join returned, asserted present. */
function hereOf(result: SubscribeResult): PresenceSnapshot {
    assert(result.ok, 'the join succeeded')
    assert(result.here !== undefined, 'a presence join carries a roster')
    return result.here
}

/** The member `id` in a snapshot, asserted present. */
function memberOf(here: PresenceSnapshot, id: string | number): PresenceMember {
    const member = here.members.find((m) => String(m.id) === String(id))
    assert(member !== undefined, `member ${id} is in the snapshot`)
    return member
}

/** The `left` frames a connection received for member `id`. */
function leftFor(c: Recording, id: string | number): unknown[] {
    return c.received
        .filter((f) => f.type === 'presence' && f.action === 'left')
        .map((f) => f.member)
        .filter((m) => String((m as PresenceMember)?.id) === String(id))
}

/**
 * Whether every object and array reachable from `value` through own keys is
 * frozen. Iterative: row (h) walks a tree a recursive helper could not.
 */
function deepFrozen(value: unknown): boolean {
    const pending: unknown[] = [value]
    while (pending.length > 0) {
        const node = pending.pop()
        if (typeof node !== 'object' || node === null) continue
        if (!Object.isFrozen(node)) return false
        for (const key of Object.keys(node)) {
            pending.push((node as Record<string, unknown>)[key])
        }
    }
    return true
}

/**
 * Run `work` with `console.warn` captured; returns what was warned. `work` is
 * handed the live list, so it can wait on a warning arriving.
 */
async function capturingWarns<T>(
    work: (warns: readonly string[]) => Promise<T>,
): Promise<{ result: T; warns: string[] }> {
    const warn = console.warn
    const warns: string[] = []
    console.warn = (...args: unknown[]) => void warns.push(args.join(' '))
    try {
        return { result: await work(warns), warns }
    } finally {
        console.warn = warn
    }
}

/** A driver with NO roster operations — the shape FR-005 allows (#342). */
function rosterlessDriver(): BroadcastDriver {
    return {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl: () => {},
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
}

/** A memory driver whose roster read fails, so `here` is the local fallback. */
function failingReadDriver(): BroadcastDriver {
    const driver = new MemoryBroadcastDriver()
    driver.readRoster = () => {
        throw new Error('roster read is down')
    }
    return driver
}

/** One way a caller may try to change a member it was handed. */
interface Write {
    readonly label: string
    readonly attempt: (member: PresenceMember) => boolean
}

const WRITES: readonly Write[] = [
    {
        label: 'Reflect.set on info.name',
        attempt: (member) => Reflect.set(member.info ?? {}, 'name', 'X'),
    },
    {
        label: 'Reflect.set on info.tags',
        attempt: (member) =>
            Reflect.set((member.info?.tags ?? []) as unknown[], '1', 'n'),
    },
    {
        label: 'Reflect.deleteProperty of info',
        attempt: (member) => Reflect.deleteProperty(member, 'info'),
    },
]

/** Where the `here` the joiner decorates comes from. */
interface Source {
    readonly row: string
    readonly label: string
    readonly driver: () => BroadcastDriver
    readonly source: PresenceSnapshot['source']
}

const SOURCES: readonly Source[] = [
    {
        row: '(a)',
        label: 'memory',
        driver: () => new MemoryBroadcastDriver(),
        source: 'authoritative',
    },
    {
        row: '(c)',
        label: 'local fallback',
        driver: failingReadDriver,
        source: 'local',
    },
    {
        row: '(d)',
        label: 'roster-less driver',
        driver: rosterlessDriver,
        source: 'authoritative',
    },
]

// --- (a) / (c) / (d): the state property -----------------------------------

for (const src of SOURCES) {
    for (const write of WRITES) {
        Deno.test(`#354 ${src.row} ${src.label}: ${write.label} on A's snapshot member leaves B's here and B's left for A unchanged`, async () => {
            const m = new ChannelManager<User>({
                driver: src.driver(),
                authorize,
            })
            const a = conn('a', JOINER)
            m.register(a)
            const b = conn('b', 2)
            m.register(b)
            await capturingWarns(async () => {
                const hereA = hereOf(await m.subscribe(a, ROOM))
                assertEquals(
                    hereA.source,
                    src.source,
                    'precondition: the source',
                )
                write.attempt(memberOf(hereA, JOINER))

                const hereB = hereOf(await m.subscribe(b, ROOM))
                assertEquals(
                    memberOf(hereB, JOINER),
                    ORIGINAL,
                    "B's here still carries A exactly as A joined",
                )
                assertEquals(await m.unsubscribe(a.id, ROOM), 'left')
                await until(
                    () => leftFor(b, JOINER).length > 0,
                    "B's left frame for A",
                )
            })
            assertEquals(
                leftFor(b, JOINER),
                [ORIGINAL],
                "B's left for A carries A exactly as A joined",
            )
        })
    }
}

// --- (b): the product default — a write throws ------------------------------

Deno.test('#354 (b) memory: a direct write to a snapshot member throws TypeError — info.name, info.tags.push and id', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize,
    })
    const a = conn('a', JOINER)
    m.register(a)
    const member = memberOf(
        hereOf(await m.subscribe(a, ROOM)),
        JOINER,
    )
    assertThrows(() => {
        ;(member.info as Record<string, unknown>).name = 'X'
    }, TypeError)
    assertThrows(() => {
        ;(member.info?.tags as string[]).push('n')
    }, TypeError)
    assertThrows(() => {
        ;(member as { id: string | number }).id = 2
    }, TypeError)
    assertEquals(member, ORIGINAL, 'and nothing was written')
})

// --- (e): a custom encode ---------------------------------------------------

/**
 * A manager whose encoder hands the joiner's `joined` member to `tamper`.
 * Observer B is already in the room.
 */
async function encodingRoom(tamper: (member: PresenceMember) => void) {
    let armed = true
    const encode = (frame: OutboundFrame) => {
        if (
            armed && frame.type === 'presence' && frame.action === 'joined' &&
            frame.member?.id === JOINER
        ) {
            tamper(frame.member)
        }
        return JSON.stringify(frame)
    }
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize,
        encode,
    })
    const b = conn('b', 2)
    m.register(b)
    await m.subscribe(b, ROOM)
    return {
        m,
        b,
        disarm: () => void (armed = false),
    }
}

Deno.test('#354 (e) custom encode: a Reflect.set on frame.member leaves the next here unchanged', async () => {
    const room = await encodingRoom((member) =>
        void Reflect.set(member.info ?? {}, 'name', 'X')
    )
    const a = conn('a', JOINER)
    room.m.register(a)
    await room.m.subscribe(a, ROOM)
    room.disarm()
    const c = conn('c', 3)
    room.m.register(c)
    const here = hereOf(await room.m.subscribe(c, ROOM))
    assertEquals(memberOf(here, JOINER), ORIGINAL)
})

Deno.test("#354 (e) custom encode: a direct write to frame.member throws into #344's local WARN, and presence state is unchanged", async () => {
    const room = await encodingRoom((member) => {
        ;(member.info as Record<string, unknown>).name = 'X'
    })
    const isLocalWarn = (w: string) =>
        w.includes('was not announced to local subscribers') &&
        w.includes('TypeError')
    const { warns } = await capturingWarns(async (warns) => {
        const a = conn('a', JOINER)
        room.m.register(a)
        await room.m.subscribe(a, ROOM)
        await until(
            () => warns.some(isLocalWarn),
            "#344's local WARN for the encoder's TypeError",
        )
    })
    assert(
        warns.some(isLocalWarn),
        `the encoder's throw is #344's local WARN, got: ${warns.join(' | ')}`,
    )
    room.disarm()
    const c = conn('c', 3)
    room.m.register(c)
    const here = hereOf(await room.m.subscribe(c, ROOM))
    assertEquals(memberOf(here, JOINER), ORIGINAL)
})

// --- (f) / (g): Redis -------------------------------------------------------

function redisDriver(redis: FakeRedis): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        {
            prefix: PREFIX,
            control: { secret: SECRET },
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
}

Deno.test("#354 (f) Redis shared read: a Reflect.set on y's seed member leaves z's here unchanged", async () => {
    const redis = new FakeRedis()
    const driver = redisDriver(redis)
    const m = new ChannelManager<User>({ driver, authorize })
    try {
        const seed = conn('seed', JOINER)
        m.register(seed)
        await m.subscribe(seed, ROOM)
        const x = conn('x', 10)
        m.register(x)
        const yConn = conn('y', 11)
        m.register(yConn)
        const zConn = conn('z', 12)
        m.register(zConn)
        const [, y, z] = (await Promise.all([
            m.subscribe(x, ROOM),
            m.subscribe(yConn, ROOM),
            m.subscribe(zConn, ROOM),
        ])).map(hereOf)
        const ySeed = memberOf(y, JOINER)
        const zSeed = memberOf(z, JOINER)
        assertStrictEquals(
            ySeed,
            zSeed,
            'precondition: y and z shared one read — the #333 barrier',
        )
        Reflect.set(ySeed.info ?? {}, 'name', 'X')
        assertEquals(zSeed, ORIGINAL, "z's here is not y's to change")
    } finally {
        await driver.close()
        redis.assertNoRejections()
    }
})

Deno.test('#354 (f) Redis readRoster: every node of members and selves is frozen', async () => {
    const redis = new FakeRedis()
    const driver = redisDriver(redis)
    const m = new ChannelManager<User>({ driver, authorize })
    try {
        const a = conn('a', JOINER)
        m.register(a)
        await m.subscribe(a, ROOM)
        const b = conn('b', 2)
        m.register(b)
        await m.subscribe(b, ROOM)
        const window = await driver.readRoster(ROOM, 100, [JOINER])
        assertEquals(window.members.length, 2, 'precondition: both are read')
        assertEquals(window.selves, [ORIGINAL], 'precondition: self is read')
        // The arrays are the read's own; every member in them is frozen.
        assert(window.members.every(deepFrozen), 'every member node is frozen')
        assert(window.selves.every(deepFrozen), 'every self node is frozen')
    } finally {
        await driver.close()
        redis.assertNoRejections()
    }
})

Deno.test("#354 (f) Redis sweep: the member a swept departure announces is deep-frozen (#348's path)", async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-23T10:00:00Z'))
    const departures: RosterDeparture[] = []
    const crashed = redisDriver(redis)
    const sweeper = redisDriver(redis)
    const register = sweeper.onRosterDeparture.bind(sweeper)
    sweeper.onRosterDeparture = (handler) =>
        register((departure) => {
            departures.push(departure)
            return handler(departure)
        })
    const a = new ChannelManager<User>({ driver: crashed, authorize })
    const b = new ChannelManager<User>({ driver: sweeper, authorize })
    try {
        const observer = conn('observer', 2)
        b.register(observer)
        await b.subscribe(observer, ROOM)
        const aConn = conn('a', JOINER)
        a.register(aConn)
        await a.subscribe(aConn, ROOM)
        await crashed.close()
        await untilFaked(
            time,
            () => departures.length > 0,
            "the sweep's departure for the crashed instance's joiner",
        )
        assertEquals(
            departures.map((d) => d.member),
            [ORIGINAL],
            'precondition: the sweep reported the joiner',
        )
        assert(deepFrozen(departures[0].member), 'the departure is frozen')
    } finally {
        await crashed.close()
        await sweeper.close()
        time.restore()
        redis.assertNoRejections()
    }
})

Deno.test("#354 (g) Redis ingest: a peer's presence-join member reaches encode with member and member.info frozen", async () => {
    const redis = new FakeRedis()
    const [onA, onB] = [redisDriver(redis), redisDriver(redis)]
    const seen: PresenceMember[] = []
    const a = new ChannelManager<User>({ driver: onA, authorize })
    const b = new ChannelManager<User>({
        driver: onB,
        authorize,
        encode: (frame) => {
            if (
                frame.type === 'presence' && frame.action === 'joined' &&
                frame.member?.id === JOINER
            ) {
                seen.push(frame.member)
            }
            return JSON.stringify(frame)
        },
    })
    try {
        const observer = conn('observer', 2)
        b.register(observer)
        await b.subscribe(observer, ROOM)
        const aConn = conn('a', JOINER)
        a.register(aConn)
        await a.subscribe(aConn, ROOM)
        await until(() => seen.length > 0, "B's re-emit of A's join")
        assertEquals(seen, [ORIGINAL], 'precondition: B re-emitted the join')
        assert(Object.isFrozen(seen[0]), 'the member is frozen')
        assert(Object.isFrozen(seen[0].info), 'and so is its info')
        assert(deepFrozen(seen[0]), 'and every node below it')
    } finally {
        await onA.close()
        await onB.close()
        redis.assertNoRejections()
    }
})

// --- (h): units -------------------------------------------------------------

Deno.test('#354 (h) freezePresenceMember freezes every node of an object → array → object tree and returns the same reference', () => {
    const leaf = { deep: { deeper: 1 } }
    const list = [leaf, 'x']
    const info = { list, flag: true }
    const member = { id: 'u1', info }
    const frozen = freezePresenceMember(member)
    assertStrictEquals(frozen, member, 'the same reference, not a copy')
    for (const node of [member, info, list, leaf, leaf.deep]) {
        assert(Object.isFrozen(node), `${JSON.stringify(node)} is frozen`)
    }
})

Deno.test('#354 (h) no isFrozen short-circuit: a frozen root with a frozen info and a writable info.tags ends with info.tags frozen', () => {
    // The walk's precondition is a fresh JSON.parse tree, so it never needs
    // to ask whether a node is already frozen — and must not: `Object.freeze`
    // is shallow, so a frozen node says nothing about its children. A walk
    // that skipped frozen nodes would stop at this root and leave the array
    // below it writable.
    const tags = ['a']
    const info = Object.freeze({ name: 'Ada', tags })
    const member = Object.freeze({ id: JOINER, info })
    assertFalse(Object.isFrozen(tags), 'precondition: info.tags is writable')
    assertStrictEquals(freezePresenceMember(member), member)
    assert(Object.isFrozen(tags), 'info.tags is frozen after the walk')
    assert(deepFrozen(member), 'and so is every other node')
})

Deno.test("#354 (h) admitPresenceMember's result is deep-frozen", () => {
    const admitted = admitPresenceMember(original(), 4096)
    assertEquals(admitted, ORIGINAL)
    assert(deepFrozen(admitted), 'every node of the admitted member')
})

Deno.test('#354 (h) depth row: an info nested 50 000 levels deep is admitted and frozen without a RangeError', () => {
    const DEPTH = 50_000
    let nested: unknown[] = []
    const innermost = nested
    for (let i = 1; i < DEPTH; i++) nested = [nested]
    const admitted = admitPresenceMember(
        { id: 'deep', info: { nested } },
        128 * 1024,
    )
    assert(deepFrozen(admitted), 'every one of the 50 000 levels is frozen')
    assertFalse(Object.isFrozen(innermost), "the candidate's tree is untouched")
})

// --- (i): negative controls -------------------------------------------------

Deno.test("#354 (i) here and here.members stay the caller's: not frozen, and sort and push work", async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize,
    })
    const b = conn('b', 2)
    m.register(b)
    await m.subscribe(b, ROOM)
    const a = conn('a', JOINER)
    m.register(a)
    const here = hereOf(await m.subscribe(a, ROOM))
    assertFalse(Object.isFrozen(here), "the snapshot object is the caller's")
    assertFalse(Object.isFrozen(here.members), 'and so is its array')
    here.members.sort((l, r) => Number(l.id) - Number(r.id))
    assertEquals(here.members.map((x) => x.id), [1, 2])
    here.members.push({ id: 99 })
    assertEquals(here.members.length, 3)
})

Deno.test('#354 (i) a copy of a frozen member is writable: structuredClone and copy-then-decorate', () => {
    const member = admitPresenceMember(original(), 4096)
    const cloned = structuredClone(member)
    ;(cloned.info as Record<string, unknown>).name = 'X'
    assertEquals(cloned.info?.name, 'X')
    const decorated = { ...member, info: { ...member.info, isYou: true } }
    decorated.info.isYou = false
    assertEquals<unknown>(decorated.info, {
        name: 'Ada',
        tags: ['a'],
        isYou: false,
    })
    assertEquals(member, ORIGINAL, 'and the original is untouched')
})

Deno.test('#354 (i) freezing changes no byte: the subscribed and here frames encode exactly as before', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize,
    })
    const a = conn('a', JOINER)
    m.register(a)
    const here = hereOf(await m.subscribe(a, ROOM))
    const member = '{"id":1,"info":{"name":"Ada","tags":["a"]}}'
    assertEquals(
        encodeServerMessage({
            type: 'subscribed',
            channel: ROOM,
            members: here.members,
            total: here.total,
        }),
        `{"type":"subscribed","channel":"${ROOM}","members":[${member}],"total":1}`,
    )
    assertEquals(
        encodeServerMessage({
            type: 'presence',
            channel: ROOM,
            action: 'here',
            members: here.members,
            total: here.total,
        }),
        `{"type":"presence","channel":"${ROOM}","action":"here","members":[${member}],"total":1}`,
    )
})

// --- (k): types -------------------------------------------------------------

Deno.test('#354 (k) PresenceMember.id, .info and what info holds are readonly — the directives below fail as unused if any loses it', () => {
    // Never called: it exists for the type checker. Each directive is an
    // error when the line under it compiles, so removing `readonly` from
    // `id`, from the `info` property, or the `Readonly<…>` around what `info`
    // holds fails `deno test` / `deno check` on this file. The three are
    // independent — one is not implied by another — so each has its own line,
    // and the #354 battery removes each alone (T1, T2, T3).
    const typeRows = (member: PresenceMember) => {
        // @ts-expect-error - (k) info holds a Readonly<Record<string, unknown>>
        member.info!.name = 'x'
        // @ts-expect-error - (k) the info property is readonly
        member.info = {}
        // @ts-expect-error - (k) id is readonly
        member.id = 1
    }
    assertEquals(typeof typeRows, 'function')
})
