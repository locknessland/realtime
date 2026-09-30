/**
 * @fileoverview #344 — presence `joined` / `left` are announced per member, not
 * per connection.
 *
 * A member is present while at least one process holds its roster slot, so
 * the room must hear `joined` when the slot's holder count goes 0 → 1 and
 * `left` when it goes 1 → 0 — never once per tab. Before this item
 * `#joinPresence` announced every connection's join and `unsubscribe` every
 * connection's leave: a second tab told the room a present member had joined,
 * and closing one of two tabs removed a still-present member from every
 * client's list.
 *
 * The disposition moves the announcement into the per-slot queue: the queued
 * roster write that OBSERVES the transition announces it. These witnesses pin
 * the behaviour from the outside — the frames a connection receives and the
 * control frames a driver is asked to publish — on the memory driver, a
 * roster-less driver, two Redis-backed managers, and the #330 gated rig.
 *
 * 344-W11 (the driver contract rows: hold → `arrived`, release → `gone`) was
 * not expressible before the seam returned those values, so it landed with the
 * seam, at the end of this file; each row was proven red against a driver
 * returning the wrong bit. 344-W7 is green before and after on purpose, and so
 * is the W10 guard row: a failing release still rejects and must keep doing
 * so. The W12 rows keep the retired names on purpose — they are what the
 * refusal probes for.
 *
 * @module @lockness/realtime/tests/presence_member_transitions_344
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

/** Run the microtask queue out, so a resumed continuation reaches its next suspension. */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

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

/** `presence` frames of `action` a connection received, for member `id`. */
const presenceFrames = (c: Recording, action: string, id: number) =>
    c.received.filter((f) =>
        f.type === 'presence' && f.channel === CHANNEL && f.action === action &&
        (f.member as PresenceMember | undefined)?.id === id
    )

/** Control frames of `kind` published for member `id`. */
const controls = (published: ControlMessage[], kind: string, id: number) =>
    published.filter((c) => c.kind === kind && c.member?.id === id)

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

/**
 * Promises resolved by name, so a schedule is chosen rather than observed —
 * the #330 rig's discipline. Keys carry a sequence number so two calls of one
 * name never overwrite each other's resolver.
 */
function gates() {
    const pending = new Map<string, () => void>()
    let seq = 0
    const gate = (name: string) =>
        new Promise<void>((resolve) => pending.set(`${name}#${++seq}`, resolve))
    const open = async (prefix: string) => {
        const key = [...pending.keys()].find((k) => k.startsWith(`${prefix}#`))
        if (!key) throw new Error(`no pending gate named ${prefix}`)
        pending.get(key)!()
        pending.delete(key)
        await settle()
    }
    const drain = async () => {
        while (pending.size > 0) {
            const key = [...pending.keys()][0]
            pending.get(key)!()
            pending.delete(key)
            await settle()
        }
    }
    return { gate, open, drain, pending }
}

type GateName = 'authorize' | 'watch' | 'unwatch' | 'hold' | 'release'

/**
 * An authoritative in-memory roster double whose calls can be gated by name.
 *
 * @param options - Which calls wait on a gate, how `publishControl` and a
 *   release behave, and the member an identity is authorized as.
 */
function rig(options: {
    gated?: readonly GateName[]
    publishControl?: (control: ControlMessage) => void | Promise<void>
    release?: () => void
    memberFor?: (identity: User) => PresenceMember
} = {}) {
    const g = gates()
    const gated = new Set(options.gated ?? [])
    const wait = (name: GateName) =>
        gated.has(name) ? g.gate(name) : Promise.resolve()
    const roster = new Map<string, PresenceMember>()
    const published: ControlMessage[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control)
            return options.publishControl?.(control)
        },
        async holdMember(_channel, member) {
            await wait('hold')
            const arrived = !roster.has(String(member.id))
            roster.set(String(member.id), member)
            return { arrived }
        },
        async releaseMember(_channel, memberId) {
            await wait('release')
            options.release?.()
            return { gone: roster.delete(String(memberId)) }
        },
        readRoster: (_channel, limit, selfIds) =>
            asWindow([...roster.values()], limit, selfIds),
        watchChannel: () => wait('watch'),
        unwatchChannel: () => wait('unwatch'),
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: async (identity) => {
            await wait('authorize')
            if (!identity) return false
            return options.memberFor?.(identity) ?? { id: identity.id }
        },
    })
    return { manager, g, published, roster }
}

// --- W1–W3 on the memory driver, W4 = the same on a roster-less driver -------

/** The memory driver, with its control publishes recorded. */
function memoryDriver(published: ControlMessage[]): BroadcastDriver {
    const driver = new MemoryBroadcastDriver()
    return Object.assign(driver, {
        publishControl(control: ControlMessage) {
            published.push(control)
        },
    })
}

/** A driver with no roster ops at all — the contract allows it (#342). */
function rosterlessDriver(published: ControlMessage[]): BroadcastDriver {
    return {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control)
        },
    }
}

/**
 * An observer (member 1) in the room, then c1 and c2 both as member 7.
 *
 * @param make - Builds the driver, recording its control publishes.
 */
async function twoTabs(make: (published: ControlMessage[]) => BroadcastDriver) {
    const published: ControlMessage[] = []
    const manager = new ChannelManager<User>({
        driver: make(published),
        authorize,
    })
    const observer = conn('c0', 1)
    manager.register(observer)
    await manager.subscribe(observer, CHANNEL)
    const c1 = conn('c1', 7)
    manager.register(c1)
    await manager.subscribe(c1, CHANNEL)
    const c2 = conn('c2', 7)
    manager.register(c2)
    await manager.subscribe(c2, CHANNEL)
    return { manager, observer, published }
}

for (
    const [label, make] of [
        ['W1–W3 memory', memoryDriver],
        ['W4 roster-less', rosterlessDriver],
    ] as const
) {
    Deno.test(`#344 ${label}: two tabs as one member announce ONE joined and ONE presence-join`, async () => {
        const { observer, published } = await twoTabs(make)
        assertEquals(
            presenceFrames(observer, 'joined', 7).length,
            1,
            'the second tab joined a member already present',
        )
        assertEquals(controls(published, 'presence-join', 7).length, 1)
    })

    Deno.test(`#344 ${label}: closing one of two tabs announces nothing`, async () => {
        const { manager, observer, published } = await twoTabs(make)
        assertEquals(await manager.unsubscribe('c1', CHANNEL), 'left')
        assertEquals(
            presenceFrames(observer, 'left', 7),
            [],
            'c2 still holds member 7 — it has not left',
        )
        assertEquals(controls(published, 'presence-leave', 7), [])
    })

    Deno.test(`#344 ${label}: closing the last tab announces exactly one left`, async () => {
        const { manager, observer, published } = await twoTabs(make)
        await manager.unsubscribe('c1', CHANNEL)
        await manager.unsubscribe('c2', CHANNEL)
        assertEquals(presenceFrames(observer, 'left', 7).length, 1)
        assertEquals(controls(published, 'presence-leave', 7).length, 1)
    })
}

// --- W5: two instances on one broker ----------------------------------------

function redisInstance(
    redis: FakeRedis,
    tag: string,
    log: Array<ControlMessage & { from: string }>,
    command: (...args: string[]) => Promise<unknown> = redis.command,
) {
    const driver = new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        {
            prefix: 'app:rt',
            control: { secret: 'deployment-secret-with-enough-entropy' },
        },
    )
    const publish = driver.publishControl.bind(driver)
    driver.publishControl = (control) => {
        log.push({ ...control, from: tag })
        return publish(control)
    }
    const manager = new ChannelManager<User>({ driver, authorize })
    return { driver, manager }
}

Deno.test('#344 W5 member 7 on two instances: one presence-join; A leaving says nothing; B leaving is the one presence-leave', async () => {
    const redis = new FakeRedis()
    const log: Array<ControlMessage & { from: string }> = []
    const a = redisInstance(redis, 'A', log)
    const b = redisInstance(redis, 'B', log)
    try {
        const a1 = conn('a1', 7)
        a.manager.register(a1)
        await a.manager.subscribe(a1, CHANNEL)
        const b1 = conn('b1', 7)
        b.manager.register(b1)
        await b.manager.subscribe(b1, CHANNEL)
        await settle()
        assertEquals(
            log.filter((c) => c.kind === 'presence-join').length,
            1,
            'B holding a member A already holds is not an arrival',
        )

        await a.manager.unsubscribe('a1', CHANNEL)
        await settle()
        assertEquals(
            log.filter((c) => c.kind === 'presence-leave'),
            [],
            'B still holds member 7',
        )

        await b.manager.unsubscribe('b1', CHANNEL)
        await settle()
        assertEquals(
            log.filter((c) => c.kind === 'presence-leave').map((c) => c.from),
            ['B'],
            'the instance releasing the last hold announces the departure',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        redis.assertNoRejections()
    }
})

// --- W6 / W7: the #330 gated rig --------------------------------------------

Deno.test('#344 W6 a join overtaken by its own leave announces nothing — neither the join nor the leave', async () => {
    // No local observer: the join is only overtaken when it awaits `#watch`,
    // which a hosted channel skips (#330). The control frames are what every
    // other instance would be told (A-L4).
    const r = rig({
        gated: ['authorize', 'watch', 'unwatch', 'hold', 'release'],
        publishControl: () => {},
    })
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const join = r.manager.subscribe(c1, CHANNEL)
    await settle()
    await r.g.open('authorize')
    const leave = r.manager.unsubscribe('c1', CHANNEL)
    await settle()
    await r.g.open('unwatch')
    await r.g.drain()
    const [joined, left] = await Promise.all([join, leave])
    assertEquals(joined.ok, true)
    assertEquals(left, 'left')

    assertEquals(controls(r.published, 'presence-join', 1), [])
    assertEquals(
        controls(r.published, 'presence-leave', 1),
        [],
        'no `left` for a member the room was never told had joined',
    )
    assertEquals(r.roster.size, 0)
})

Deno.test('#344 W7 a hold that lands before the leave announces joined, then left', async () => {
    // Green before and after #344: this interleaving genuinely held the member.
    //
    // REPAIRED by #361, which made `unsubscribe` forget the member BEFORE its
    // awaited leave. The leave used to be issued while the join was still
    // suspended at its watch, before its hold existed, and the forget then
    // landed after the hold; now the forget lands first, so that interleaving
    // is a join the leave overtook (below). The hold is therefore put in
    // flight first here, which is what this witness is named for.
    const r = rig({
        gated: ['authorize', 'watch', 'unwatch', 'hold', 'release'],
        publishControl: () => {},
    })
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const join = r.manager.subscribe(c1, CHANNEL)
    await settle()
    await r.g.open('authorize')
    await r.g.open('watch') // the join's hold is issued and suspended
    const leave = r.manager.unsubscribe('c1', CHANNEL)
    await settle()
    await r.g.drain()
    await Promise.all([join, leave])

    assertEquals(
        r.published
            .filter((c) => c.member?.id === 1)
            .map((c) => c.kind),
        ['presence-join', 'presence-leave'],
    )
})

Deno.test('#344 W7b a leave issued before the join held anything overtakes it, and nothing is announced (#361)', async () => {
    // The pre-#361 interleaving of W7: the leave arrives while the join is
    // suspended at its watch. The leave forgets the member first, so the
    // join's queued write computes "absent" and holds nothing (#330) — no
    // `joined` for a member already leaving, and no `left` after it.
    const r = rig({
        gated: ['authorize', 'watch', 'unwatch', 'hold', 'release'],
        publishControl: () => {},
    })
    const c1 = conn('c1', 1)
    r.manager.register(c1)
    const join = r.manager.subscribe(c1, CHANNEL)
    await settle()
    await r.g.open('authorize')
    const leave = r.manager.unsubscribe('c1', CHANNEL)
    await settle()
    await r.g.open('watch')
    await r.g.drain()
    const [joined, left] = await Promise.all([join, leave])

    assertEquals(
        r.published.filter((c) => c.member?.id === 1).map((c) => c.kind),
        [],
    )
    assertEquals(r.roster.size, 0, 'the roster holds nobody')
    // The join committed before the leave began (#330): `ok`, not "held".
    assertEquals(joined.ok, true)
    assertEquals(left, 'left')
    assertEquals(r.manager.connectionCount, 1)
})

Deno.test("#344 W8 a join whose queued write finds the slot emptied sends the one left, from that join's write", async () => {
    const r = rig({ gated: ['hold', 'release'] })
    const observer = conn('c0', 1)
    r.manager.register(observer)
    const first = r.manager.subscribe(observer, CHANNEL)
    await settle()
    await r.g.drain()
    await first

    // c1's hold of 7 is in flight; c2 claims 7 and its write queues behind it.
    const c1 = conn('c1', 7)
    r.manager.register(c1)
    const c1Join = r.manager.subscribe(c1, CHANNEL)
    await settle()
    const c2 = conn('c2', 7)
    r.manager.register(c2)
    const c2Join = r.manager.subscribe(c2, CHANNEL)
    await settle()
    // Both leave before either queued write has run.
    const c1Leave = r.manager.unsubscribe('c1', CHANNEL)
    await settle()
    const c2Leave = r.manager.unsubscribe('c2', CHANNEL)
    await settle()

    await r.g.open('hold') // c1's join write: 7 is held
    await r.g.open('release') // c2's join write: the local map is empty now
    assertEquals(
        controls(r.published, 'presence-leave', 7).map((c) => c.target),
        ['c2'],
        "c2's queued join write is the one that emptied the slot, so it " +
            'announces the departure — before either leave write has run',
    )

    await r.g.drain()
    await Promise.all([c1Join, c2Join, c1Leave, c2Leave])
    assertEquals(controls(r.published, 'presence-leave', 7).length, 1)
    assertEquals(presenceFrames(observer, 'left', 7).length, 1)
})

// --- W9: `joined` never reaches a connection of the same member -------------

Deno.test('#344 W9 two tabs of member 7 racing its arrival — neither receives joined for 7', async () => {
    const r = rig({ gated: ['hold'] })
    const c1 = conn('c1', 7)
    r.manager.register(c1)
    const c2 = conn('c2', 7)
    r.manager.register(c2)
    const j1 = r.manager.subscribe(c1, CHANNEL)
    await settle()
    const j2 = r.manager.subscribe(c2, CHANNEL)
    await settle()
    await r.g.drain()
    await Promise.all([j1, j2])

    assertEquals(presenceFrames(c1, 'joined', 7), [])
    assertEquals(
        presenceFrames(c2, 'joined', 7),
        [],
        'c2 claimed member 7 before the arrival was announced; a member is ' +
            'never told it joined',
    )
})

Deno.test("#344 W9 remote: a tab of member 7 on B never receives B's re-emit of A's presence-join for 7", async () => {
    const redis = new FakeRedis()
    const log: Array<ControlMessage & { from: string }> = []
    const g = gates()
    const presenceKey = `app:rt__presence:${CHANNEL}`
    let gateNextEval = false
    // B's first script touching the room's presence hash is its hold: the
    // closing read comes after the write. Matched on a KEY argument, never on
    // script text.
    const gatedCommand = async (...args: string[]) => {
        if (args[0] === 'EVAL' && args.includes(presenceKey) && gateNextEval) {
            gateNextEval = false
            await g.gate('b-hold')
        }
        return await redis.command(...args)
    }
    const a = redisInstance(redis, 'A', log)
    const b = redisInstance(redis, 'B', log, gatedCommand)
    try {
        const onB = conn('b1', 7)
        b.manager.register(onB)
        gateNextEval = true
        const joinB = b.manager.subscribe(onB, CHANNEL)
        await settle()
        assertEquals(g.pending.size, 1, "precondition: B's hold is in flight")
        assertEquals(
            await redis.command('HGET', presenceKey, '7'),
            { type: 'nil' },
            "precondition: and has not landed, so A's hold is the arrival",
        )

        const onA = conn('a1', 7)
        a.manager.register(onA)
        await a.manager.subscribe(onA, CHANNEL)
        await settle()
        assert(
            log.some((c) => c.kind === 'presence-join' && c.from === 'A'),
            "precondition: A's hold was the arrival and A announced it",
        )
        assertEquals(
            presenceFrames(onB, 'joined', 7),
            [],
            'b1 already claimed member 7 on B; the re-emit must skip every ' +
                'local connection of that member, not just the origin',
        )

        await g.drain()
        await joinB
        assertEquals(presenceFrames(onA, 'joined', 7), [])
    } finally {
        await a.driver.close()
        await b.driver.close()
        redis.assertNoRejections()
    }
})

// --- W10: a lost presence-leave publish ------------------------------------

Deno.test('#344 W10 a failed presence-leave publish: unsubscribe resolves left, with one WARN naming no member', async () => {
    const secretId = 424242
    const secretInfo = 'ada@example.test'
    const r = rig({
        publishControl: (control) => {
            if (control.kind === 'presence-leave') {
                return Promise.reject(new Error('broker went away'))
            }
        },
        memberFor: (identity) => ({
            id: identity.id,
            info: { email: secretInfo },
        }),
    })
    const manager = r.manager
    const c1 = conn('c1', secretId)
    manager.register(c1)
    await manager.subscribe(c1, CHANNEL)

    const warn = console.warn
    const warnings: string[] = []
    console.warn = (...parts: unknown[]) => void warnings.push(parts.join(' '))
    let outcome: unknown
    try {
        outcome = await manager.unsubscribe('c1', CHANNEL)
    } catch (error) {
        outcome = error
    } finally {
        console.warn = warn
    }

    assertEquals(
        outcome,
        'left',
        'the membership was removed here; only the frame was lost',
    )
    assertEquals(warnings.length, 1, warnings.join('\n'))
    assert(
        warnings[0].includes(CHANNEL) && warnings[0].includes('left') &&
            warnings[0].includes('broker went away'),
        `the WARN must name the channel, the action and the error: ${
            warnings[0]
        }`,
    )
    assert(
        !warnings[0].includes(String(secretId)) &&
            !warnings[0].includes(secretInfo),
        `the WARN must carry neither the member id nor its info: ${
            warnings[0]
        }`,
    )
})

Deno.test('#344 W10 an encode that refuses the local joined frame: one WARN, no rethrow, and the presence-join is still published', async () => {
    // FR-010: an `encode` throw is a WARN like a publish failure. The two
    // halves fail independently — this instance's codec refusing the frame for
    // its own sockets does not keep the other instances from hearing of it.
    const secretId = 434343
    const published: ControlMessage[] = []
    const manager = new ChannelManager<User>({
        driver: memoryDriver(published),
        authorize,
        encode: (frame) => {
            const text = JSON.stringify(frame)
            if (
                text.includes('"action":"joined"') &&
                text.includes(String(secretId))
            ) {
                throw new Error('codec refused the frame')
            }
            return text
        },
    })
    const c0 = conn('c0', 1)
    manager.register(c0)
    await manager.subscribe(c0, CHANNEL)

    const warn = console.warn
    const warnings: string[] = []
    console.warn = (...parts: unknown[]) => void warnings.push(parts.join(' '))
    let outcome: unknown
    try {
        const c1 = conn('c1', secretId)
        manager.register(c1)
        outcome = await manager.subscribe(c1, CHANNEL)
    } catch (error) {
        outcome = error
    } finally {
        console.warn = warn
    }

    assertEquals(
        (outcome as { ok?: unknown }).ok,
        true,
        `a committed hold is not rolled back by a frame the codec refused: ${
            String(outcome)
        }`,
    )
    assertEquals(warnings.length, 1, warnings.join('\n'))
    assert(
        warnings[0].includes(CHANNEL) && warnings[0].includes('joined') &&
            warnings[0].includes('codec refused the frame') &&
            !warnings[0].includes(String(secretId)),
        `one WARN naming the channel, the action and the error only: ${
            warnings[0]
        }`,
    )
    assertEquals(
        controls(published, 'presence-join', secretId).length,
        1,
        'the local emit failed; the other instances must still be told',
    )
})

Deno.test("#344 the arrival's joined carries the earliest local connection's entry, not the origin's", async () => {
    // ADR 003 §7: the slot holds `desired`, the member's earliest local entry,
    // and FR-010 announces that same entry. Here the arrival is observed by
    // c2's write (the slot was emptied under c1, as another instance's release
    // or a sweep would), so `origin.member` and `desired` differ.
    type Tabbed = User & { tab: string }
    const r = rig({
        memberFor: (identity) => ({
            id: identity.id,
            info: { tab: (identity as Tabbed).tab },
        }),
    })
    const tab = (id: string, name: string): Recording =>
        Object.assign(conn(id, 7), { identity: { id: 7, tab: name } })
    const observer = conn('c0', 1)
    r.manager.register(observer)
    await r.manager.subscribe(observer, CHANNEL)
    const c1 = tab('c1', 'first')
    r.manager.register(c1)
    await r.manager.subscribe(c1, CHANNEL)
    r.roster.delete('7')
    const c2 = tab('c2', 'second')
    r.manager.register(c2)
    await r.manager.subscribe(c2, CHANNEL)

    const joins = controls(r.published, 'presence-join', 7)
    assertEquals(joins.map((c) => c.target), ['c1', 'c2'])
    assertEquals(
        joins[1].member,
        { id: 7, info: { tab: 'first' } },
        "c2's write announces the entry the roster now holds — c1's",
    )
    assertEquals(
        presenceFrames(observer, 'joined', 7).map((f) => f.member),
        [{ id: 7, info: { tab: 'first' } }, { id: 7, info: { tab: 'first' } }],
    )
    assertEquals(r.roster.get('7'), { id: 7, info: { tab: 'first' } })
})

Deno.test('#344 W10 (guard) a failed RELEASE still rejects unsubscribe', async () => {
    // Green before and after: only the announcement's failure is absorbed.
    const r = rig({
        publishControl: () => {},
        release: () => {
            throw new Error('roster write refused')
        },
    })
    const c1 = conn('c1', 7)
    r.manager.register(c1)
    await r.manager.subscribe(c1, CHANNEL)
    let rejected = false
    try {
        await r.manager.unsubscribe('c1', CHANNEL)
    } catch {
        rejected = true
    }
    assert(rejected, 'a release the roster refused is not a completed leave')
})

// --- W12: a driver on the retired roster names is refused once --------------

const legacyBase = {
    publish: () => {},
    onMessage: () => {},
    addMember: () => {},
    removeMember: () => {},
    readRoster: () => ({ members: [], total: 0, selves: [] }),
}

Deno.test('#344 W12 a driver still offering addMember / removeMember is refused at construction', () => {
    const error = assertThrows(
        () =>
            new ChannelManager<User>({
                driver: legacyBase as unknown as BroadcastDriver,
                authorize,
            }),
        Error,
    )
    for (const name of ['addMember', 'removeMember']) {
        assert(error.message.includes(name), `names ${name}: ${error.message}`)
    }
    assert(
        error.message.includes('The driver roster seam'),
        `cites the upgrade section by its title: ${error.message}`,
    )
})

Deno.test('#344 W12 a driver offering listMembers too is refused ONCE, naming all three', () => {
    const error = assertThrows(
        () =>
            new ChannelManager<User>({
                driver: {
                    ...legacyBase,
                    listMembers: () => [],
                } as unknown as BroadcastDriver,
                authorize,
            }),
        Error,
    )
    for (const name of ['listMembers', 'addMember', 'removeMember']) {
        assert(error.message.includes(name), `names ${name}: ${error.message}`)
    }
})

// --- W11: the driver contract — hold reports arrival, release departure ------

/** Two instances' view of one roster; memory has one process, so one driver. */
type ContractDriver = Pick<
    RedisBroadcastDriver,
    'holdMember' | 'releaseMember' | 'readRoster'
>

Deno.test('#344 W11 memory: hold → arrived, hold again → not, release → gone, then never again', async () => {
    const d: ContractDriver =
        new MemoryBroadcastDriver() as unknown as ContractDriver
    const ada = { id: 7, info: { name: 'Ada' } }
    assertEquals(await d.holdMember(CHANNEL, ada), { arrived: true }, 'hold')
    assertEquals(
        await d.holdMember(CHANNEL, ada),
        { arrived: false },
        'hold again: the slot was already filled',
    )
    assertEquals(
        await d.releaseMember(CHANNEL, 7),
        { gone: true },
        'last release',
    )
    assertEquals(
        await d.releaseMember(CHANNEL, 7),
        { gone: false },
        'a release on an empty slot is not a departure',
    )
    assertEquals(
        await d.releaseMember('presence-never-held', 7),
        { gone: false },
        'nor is a release on a channel never held',
    )
})

function contractRedis(redis: FakeRedis): RedisBroadcastDriver {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        {
            prefix: 'app:rt',
        },
    )
}

Deno.test('#344 W11 FakeRedis: the six contract rows across two instances', async () => {
    const redis = new FakeRedis()
    const a = contractRedis(redis)
    const b = contractRedis(redis)
    const ada = { id: 7, info: { name: 'Ada' } }
    try {
        assertEquals(
            await a.holdMember(CHANNEL, ada),
            { arrived: true },
            'hold',
        )
        assertEquals(
            await a.holdMember(CHANNEL, ada),
            { arrived: false },
            'hold again on the same instance',
        )
        assertEquals(
            await b.holdMember(CHANNEL, ada),
            { arrived: false },
            'a second instance holding a filled slot is not an arrival',
        )
        assertEquals(
            await a.releaseMember(CHANNEL, 7),
            { gone: false },
            'release while another instance holds',
        )
        assertEquals(
            await b.releaseMember(CHANNEL, 7),
            { gone: true },
            'last release',
        )
        assertEquals(
            await a.releaseMember(CHANNEL, 7),
            { gone: false },
            'a non-holder release on an empty slot',
        )
    } finally {
        await a.close()
        await b.close()
    }
})

Deno.test('#344 W11 FakeRedis: two instances interleaved — exactly one arrival, exactly one departure', async () => {
    const redis = new FakeRedis()
    const a = contractRedis(redis)
    const b = contractRedis(redis)
    const ada = { id: 7, info: { name: 'Ada' } }
    try {
        const holds = await Promise.all([
            a.holdMember(CHANNEL, ada),
            b.holdMember(CHANNEL, ada),
        ])
        assertEquals(
            holds.filter((h) => h.arrived).length,
            1,
            'the script serialises two 0 → 1 holds: one arrival',
        )
        const releases = await Promise.all([
            a.releaseMember(CHANNEL, 7),
            b.releaseMember(CHANNEL, 7),
        ])
        assertEquals(
            releases.filter((r) => r.gone).length,
            1,
            'and two 1 → 0 releases: one departure',
        )
        assertEquals((await a.readRoster(CHANNEL, 10, [])).total, 0)
    } finally {
        await a.close()
        await b.close()
    }
})
