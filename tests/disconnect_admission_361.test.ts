/**
 * @fileoverview #361 — a disconnected connection is refused at admission,
 * `unsubscribe` forgets before it leaves, and every manager collector records
 * a failure by a flag.
 *
 * `subscribe` awaits the application's authorizer before it registers and
 * joins, and `disconnect` copies the connection's channels once and forgets
 * the id in its `finally`. Nothing recorded that a connection had been
 * disconnected, so a `subscribe` spanning the disconnect of the same
 * connection wrote a membership nothing would ever tear down — and, after the
 * `finally`, re-registered a zombie. Three windows: (a) the authorizer
 * resolves while the teardown is suspended, (b) the teardown finishes while
 * the authorizer is pending, (c) the subscribe is issued after the teardown
 * settled.
 *
 * A second cause stranded the same state: `unsubscribe` awaited the leave
 * (whose unwatch may reject) before it forgot the presence member, so a failed
 * unwatch left a roster ghost the lapse re-assert (#349) re-held forever.
 *
 * **The harness lives here and nowhere else** (plan §5, row 19): a recording
 * driver on {@link MemoryBroadcastDriver} (its roster is what the witnesses
 * read), with a gated unwatch, a rejecting unwatch, a gated hold and a
 * revocation store as subclasses. Every wait is on a gate's promise or a
 * bounded poll of an observable condition, never a fixed microtask count.
 *
 * Committed red first (`e9c660c3`, the classes added so the file compiles):
 * W1, W2, W3 (i), W4, W5, W6 (i) and (ii), W8, W9, W10, W11 and W12. W3 (ii)
 * (a denial is still a denial) and W7 (a join that committed first) are pins,
 * green before and after. Each is paired with a mutant in
 * `mutations/disconnect_admission_361.ts`.
 *
 * @module @lockness/realtime/tests/disconnect_admission_361
 */

import {
    assert,
    assertEquals,
    assertRejects,
    assertStrictEquals,
    assertThrows,
} from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import {
    ChannelLimitError,
    ChannelManager,
    type ChannelManagerOptions,
    ConnectionDisconnectedError,
    ConnectionIdInUseError,
} from '../manager.ts'
import { buildEvents } from '../websocket.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ChannelRevocation,
    Revocation,
    RosterHold,
    RosterRelease,
    RosterWindow,
} from '../driver.ts'
import type { AuthorizeResult, PresenceMember } from '../channel.ts'
import type { Connection, WSContext } from '../types.ts'

const ROOM = 'presence-room'
const LOBBY = 'lobby'
const PRIVATE = 'private-x'

/** The words of the one WARN a failed teardown after a failed app `onClose` logs. */
const CLOSE_DISCONNECT_FAILED = "after the application's onClose threw"

interface User {
    id: number
    name: string
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
    readonly closed: number[]
}

/** A connection that records every frame and close code it is sent. */
function conn(id: string, userId: number | null = 1): Recording {
    const received: Record<string, unknown>[] = []
    const closed: number[] = []
    return {
        id,
        identity: userId === null
            ? null
            : { id: userId, name: `user-${userId}` },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: (code?: number) => void closed.push(code ?? 0),
        received,
        closed,
    } as Recording
}

/** What the application's authorizer decides, before any gate. */
function decide(user: User | null, channel: string): AuthorizeResult {
    if (user === null) return false
    return channel.startsWith('presence-')
        ? { id: user.id, info: { name: user.name } }
        : true
}

/**
 * A spy authorizer that counts its calls. After {@link gateNext}, the next call
 * for that channel suspends until the test admits or denies it.
 */
class SpyAuthorizer {
    calls = 0
    #gated?: string
    #settle?: (result: AuthorizeResult) => void
    #decided: AuthorizeResult = false
    #reach!: () => void
    /** Resolves once the gated call is suspended. */
    readonly reached = new Promise<void>((resolve) => (this.#reach = resolve))

    readonly fn = (
        user: User | null,
        channel: string,
    ): AuthorizeResult | Promise<AuthorizeResult> => {
        this.calls++
        if (channel !== this.#gated) return decide(user, channel)
        this.#gated = undefined
        this.#decided = decide(user, channel)
        return new Promise<AuthorizeResult>((resolve) => {
            this.#settle = resolve
            this.#reach()
        })
    }

    /**
     * Suspend the next authorization of `channel`.
     *
     * @param channel - The channel whose next call waits for the test.
     */
    gateNext(channel: string): void {
        this.#gated = channel
    }

    /** Let the suspended call approve, as the undelayed authorizer would. */
    admit(): void {
        this.#settle?.(this.#decided)
    }

    /** Let the suspended call deny. */
    deny(): void {
        this.#settle?.(false)
    }
}

/**
 * A roster-capable, channel-watching driver on {@link MemoryBroadcastDriver}
 * that records every watch, unwatch and hold, and captures the manager's
 * `onRosterLapse` handler so a test can fire it.
 */
class RecordingDriver implements BroadcastDriver {
    readonly memory = new MemoryBroadcastDriver()
    readonly watched: string[] = []
    readonly unwatched: string[] = []
    holds = 0
    lapse?: (signal: AbortSignal) => void | Promise<void>

    publish(message: BroadcastMessage): void {
        this.memory.publish(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.memory.onMessage(handler)
    }

    holdMember(
        channel: string,
        member: PresenceMember,
    ): RosterHold | Promise<RosterHold> {
        this.holds++
        return this.memory.holdMember(channel, member)
    }

    releaseMember(
        channel: string,
        memberId: string | number,
    ): RosterRelease | Promise<RosterRelease> {
        return this.memory.releaseMember(channel, memberId)
    }

    readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow {
        return this.memory.readRoster(channel, limit, selfIds)
    }

    watchChannel(channel: string): void | Promise<void> {
        this.watched.push(channel)
    }

    unwatchChannel(channel: string): void | Promise<void> {
        this.unwatched.push(channel)
    }

    onRosterLapse(
        handler: (signal: AbortSignal) => void | Promise<void>,
    ): void {
        this.lapse = handler
    }

    /** The member ids the authoritative roster holds for `channel`. */
    held(channel: string): (string | number)[] {
        return this.memory.readRoster(channel, 1_000, []).members.map((m) =>
            m.id
        )
    }
}

/**
 * The race harness: an unwatch that suspends until the test opens it — the
 * one hook that holds a `disconnect` (or an `unsubscribe`) inside its leave.
 */
class GatedUnwatchDriver extends RecordingDriver {
    #open!: () => void
    readonly #gate = new Promise<void>((resolve) => (this.#open = resolve))
    #reach!: () => void
    /** Resolves once an unwatch has been asked for and is suspended. */
    readonly unwatching = new Promise<void>((
        resolve,
    ) => (this.#reach = resolve))

    override unwatchChannel(channel: string): Promise<void> {
        super.unwatchChannel(channel)
        this.#reach()
        return this.#gate
    }

    /** Let every suspended unwatch, and every later one, finish. */
    openUnwatch(): void {
        this.#open()
    }
}

/** An unwatch that rejects for one named channel, with a chosen value. */
class RejectingUnwatchDriver extends RecordingDriver {
    constructor(readonly failing: string, readonly value: unknown) {
        super()
    }

    override async unwatchChannel(channel: string): Promise<void> {
        await super.unwatchChannel(channel)
        if (channel === this.failing) throw this.value
    }
}

/** A roster hold that suspends until the test releases it (W7). */
class GatedHoldDriver extends RecordingDriver {
    #open!: () => void
    readonly #gate = new Promise<void>((resolve) => (this.#open = resolve))
    #reach!: () => void
    /** Resolves once a hold has been asked for and is suspended. */
    readonly holding = new Promise<void>((resolve) => (this.#reach = resolve))

    override async holdMember(
        channel: string,
        member: PresenceMember,
    ): Promise<RosterHold> {
        this.#reach()
        await this.#gate
        return await super.holdMember(channel, member)
    }

    /** Let every suspended hold finish. */
    openHold(): void {
        this.#open()
    }
}

/**
 * A revocation store on the recording driver whose writes can reject with a
 * chosen value — `undefined` included, which is the point of W6 (ii).
 */
class RevocationDriver extends RecordingDriver {
    readonly records: Revocation[] = []
    markFails = false
    clearFails = false

    /** @param value - What a failing write rejects with; may be `undefined`. */
    constructor(readonly value: unknown) {
        super()
    }

    async markRevocation(revocation: Revocation): Promise<void> {
        await Promise.resolve()
        if (this.markFails) throw this.value
        this.records.push(revocation)
    }

    listRevocations(): Revocation[] {
        return [...this.records]
    }

    async clearRevocation(revocation: ChannelRevocation): Promise<void> {
        await Promise.resolve()
        if (this.clearFails) throw this.value
        const at = this.records.findIndex((r) =>
            r.channel !== undefined && r.id === revocation.id
        )
        if (at >= 0) this.records.splice(at, 1)
    }
}

/** The manager's maps, read through its TS-private fields. */
function state(manager: ChannelManager<User>) {
    return manager as unknown as {
        connections: Map<string, Connection<User>>
        subscriptions: Map<string, Set<string>>
        presence: Map<string, Map<string, PresenceMember>>
    }
}

/** Assert no map the test can reach names `id`, and no roster holds `memberId`. */
function assertForgotten(
    manager: ChannelManager<User>,
    driver: RecordingDriver,
    id: string,
    memberId: number,
): void {
    const s = state(manager)
    assert(!s.connections.has(id), `connections still names ${id}`)
    for (const [channel, set] of s.subscriptions) {
        assert(!set.has(id), `subscriptions of ${channel} still name ${id}`)
    }
    for (const [channel, members] of s.presence) {
        assert(!members.has(id), `presence of ${channel} still names ${id}`)
    }
    assert(
        !driver.held(ROOM).includes(memberId),
        `the roster still holds member ${memberId}`,
    )
}

/**
 * Assert a refused subscribe wrote nothing: no watch, no hold, no roster
 * entry, no membership and no presence entry.
 */
function assertNothingWritten(
    manager: ChannelManager<User>,
    driver: RecordingDriver,
): void {
    assertEquals(driver.watched, [], 'no channel was watched')
    assertEquals(driver.holds, 0, 'no roster hold was issued')
    assertEquals(driver.held(ROOM), [], 'the roster holds nobody')
    assertEquals(state(manager).subscriptions.size, 0, 'no membership')
    assertEquals(state(manager).presence.size, 0, 'no presence entry')
}

/** The presence frames `c` received that name member `id`. */
const presenceAbout = (c: Recording, id: number) =>
    c.received.filter((f) =>
        f.type === 'presence' &&
        (f.member as PresenceMember | undefined)?.id === id
    )

/** How a promise settled: `'resolved'`, or the rejection value itself. */
async function settled(promise: Promise<unknown>): Promise<unknown> {
    return await promise.then(() => 'resolved', (error) => error)
}

/** A macrotask: every queued microtask chain has run when it resolves. */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

/** Wait, a macrotask at a time, until `condition` holds — bounded. */
async function until(condition: () => boolean, what: string): Promise<void> {
    for (let i = 0; i < 50; i++) {
        if (condition()) return
        await tick()
    }
    throw new Error(`timed out waiting for ${what}`)
}

function captureWarnings() {
    const warn = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => void lines.push(parts.join(' '))
    return {
        lines,
        having: (words: string) => lines.filter((l) => l.includes(words)),
        restore: () => void (console.warn = warn),
    }
}

function managerOver(
    driver: BroadcastDriver,
    spy: SpyAuthorizer,
    options: ChannelManagerOptions<User> = {},
): ChannelManager<User> {
    return new ChannelManager<User>({ driver, authorize: spy.fn, ...options })
}

Deno.test('#361 W1 window (a): a subscribe admitted while its disconnect is suspended is refused, and nothing names the connection afterwards', async () => {
    const driver = new GatedUnwatchDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy)
    const o = conn('o', 2)
    manager.register(o)
    const c1 = conn('c1', 1)
    manager.register(c1)
    assert((await manager.subscribe(o, ROOM)).ok)
    assert((await manager.subscribe(c1, LOBBY)).ok)

    spy.gateNext(ROOM)
    const subscribing = settled(manager.subscribe(c1, ROOM))
    await spy.reached
    const disconnecting = manager.disconnect('c1')
    await driver.unwatching // the teardown is inside its loop
    spy.admit()
    const outcome = await subscribing
    assert(
        outcome instanceof ConnectionDisconnectedError,
        `the subscribe must be refused, got ${String(outcome)}`,
    )
    driver.openUnwatch()
    assertEquals(await disconnecting, 'disconnected')

    assertForgotten(manager, driver, 'c1', 1)
    assertEquals(driver.held(ROOM), [2])
    assertEquals(presenceAbout(o, 1), [], 'the room never heard of member 1')
})

Deno.test('#361 W2 caps and watch: a refused subscribe takes no slot and issues no watch', async () => {
    const driver = new GatedUnwatchDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy, {
        maxWatchedChannels: 2,
        maxChannelsPerConnection: 2,
    })
    const c1 = conn('c1', 1)
    manager.register(c1)
    assert((await manager.subscribe(c1, LOBBY)).ok)

    spy.gateNext(PRIVATE)
    const subscribing = settled(manager.subscribe(c1, PRIVATE))
    await spy.reached
    const disconnecting = manager.disconnect('c1')
    await driver.unwatching
    spy.admit()
    assert((await subscribing) instanceof ConnectionDisconnectedError)
    driver.openUnwatch()
    await disconnecting

    assert(!driver.watched.includes(PRIVATE), 'the driver never watched it')
    assert(!state(manager).subscriptions.has(PRIVATE))
    const n = conn('n', 3)
    manager.register(n)
    assert((await manager.subscribe(n, 'a')).ok)
    assert((await manager.subscribe(n, 'b')).ok, 'both slots are free')
})

Deno.test('#361 W3 (i) window (b): a subscribe whose authorizer outlives the disconnect is refused, and no zombie is registered', async () => {
    const driver = new RecordingDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy)
    const c1 = conn('c1', 1)
    manager.register(c1)

    spy.gateNext(PRIVATE)
    const subscribing = settled(manager.subscribe(c1, PRIVATE))
    await spy.reached
    assertEquals(await manager.disconnect('c1'), 'disconnected')
    spy.admit()
    assert((await subscribing) instanceof ConnectionDisconnectedError)

    assertEquals(manager.connectionCount, 0)
    assertForgotten(manager, driver, 'c1', 1)
    assert(!driver.watched.includes(PRIVATE))
})

Deno.test('#361 W3 (ii) pin: a denial during the disconnect is still a denial, and nothing is written', async () => {
    const driver = new RecordingDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy)
    const c1 = conn('c1', 1)
    manager.register(c1)

    spy.gateNext(PRIVATE)
    const subscribing = manager.subscribe(c1, PRIVATE)
    await spy.reached
    await manager.disconnect('c1')
    spy.deny()
    assertEquals(await subscribing, { ok: false })

    assertEquals(manager.connectionCount, 0)
    assert(!state(manager).subscriptions.has(PRIVATE))
    assert(!driver.watched.includes(PRIVATE))
})

Deno.test('#361 W4 window (c): a subscribe after the disconnect settled is refused before the authorizer runs, and spends no share', async () => {
    // (i) The authorizer never runs for a retired connection.
    {
        const spy = new SpyAuthorizer()
        const driver = new RecordingDriver()
        const manager = managerOver(driver, spy)
        const c1 = conn('c1', 1)
        manager.register(c1)
        await manager.disconnect('c1')
        await assertRejects(
            () => manager.subscribe(c1, PRIVATE),
            ConnectionDisconnectedError,
        )
        await assertRejects(
            () => manager.subscribe(c1, ROOM),
            ConnectionDisconnectedError,
        )
        assertEquals(spy.calls, 0, 'the authorizer was not called')
        assertEquals(manager.connectionCount, 0)
        assertNothingWritten(manager, driver)
    }
    // (ii) A public channel, an anonymous connection: the share is unspent.
    {
        const manager = managerOver(
            new RecordingDriver(),
            new SpyAuthorizer(),
            {
                maxWatchedChannels: 2,
                maxChannelsPerConnection: 2,
                anonymousHostingShare: 0.5,
            },
        )
        const a = conn('a', null)
        manager.register(a)
        await manager.disconnect('a')
        await assertRejects(
            () => manager.subscribe(a, 'news'),
            ConnectionDisconnectedError,
        )
        const b = conn('b', null)
        manager.register(b)
        assertEquals(await manager.subscribe(b, 'feed'), { ok: true })
        assertEquals(manager.connectionCount, 1)
    }
})

Deno.test('#361 W5 register refuses a disconnected connection object, after and during its teardown', async () => {
    {
        const manager = managerOver(new RecordingDriver(), new SpyAuthorizer())
        const c1 = conn('c1', 1)
        manager.register(c1)
        await manager.disconnect('c1')
        assertThrows(() => manager.register(c1), ConnectionDisconnectedError)
        assertEquals(manager.connectionCount, 0)
    }
    {
        const driver = new GatedUnwatchDriver()
        const manager = managerOver(driver, new SpyAuthorizer())
        const c1 = conn('c1', 1)
        manager.register(c1)
        assert((await manager.subscribe(c1, LOBBY)).ok)
        const disconnecting = manager.disconnect('c1')
        await driver.unwatching
        assertThrows(() => manager.register(c1), ConnectionDisconnectedError)
        assertEquals(manager.connectionCount, 1, 'still bound mid-teardown')
        driver.openUnwatch()
        await disconnecting
        assertEquals(manager.connectionCount, 0)
    }
})

Deno.test('#361 W6 (i) a failed unwatch leaves no presence ghost, and the rest of the teardown still runs', async () => {
    const E = new Error('unwatch failed')
    const driver = new RejectingUnwatchDriver(ROOM, E)
    const manager = managerOver(driver, new SpyAuthorizer())
    const c1 = conn('c1', 1)
    manager.register(c1)
    // In this order: the failing channel first, so `news` comes after it.
    assert((await manager.subscribe(c1, ROOM)).ok)
    assert((await manager.subscribe(c1, 'news')).ok)
    const holdsBefore = driver.holds

    assertStrictEquals(await settled(manager.disconnect('c1')), E)

    const s = state(manager)
    assert(!s.presence.has(ROOM), 'the local presence entry is forgotten')
    assert(!driver.held(ROOM).includes(1), 'the roster hold is released')
    assert(driver.unwatched.includes('news'), '`news` was still unwatched')
    assert(!s.subscriptions.has('news'), '`news` was still torn down')
    assert(!s.connections.has('c1'))
    await driver.lapse?.(new AbortController().signal)
    assertEquals(driver.holds, holdsBefore, 'the lapse re-holds nothing')
})

Deno.test('#372 a rejecting unwatchChannel fires exactly once — the retry moved into @lockness/redis, not the manager', async () => {
    // Companion to #372's redis-side fix, on the SAME harness: a rejecting
    // `unwatchChannel` still leaves `ChannelManager` alone. The redis-side
    // disposition explicitly rejected an owed-unwatch retry living here
    // (option 3) because it would duplicate a retry/backoff policy
    // `@lockness/redis` already owns and exercises for every other write on
    // that connection. This pins the absence: one call in, one rejection out,
    // no re-issue.
    const E = new Error('unwatch failed')
    const driver = new RejectingUnwatchDriver('news', E)
    const manager = managerOver(driver, new SpyAuthorizer())
    const c1 = conn('c1', 1)
    manager.register(c1)
    assert((await manager.subscribe(c1, 'news')).ok)

    assertStrictEquals(await settled(manager.unsubscribe('c1', 'news')), E)

    assertEquals(
        driver.unwatched.filter((c) => c === 'news').length,
        1,
        'unwatchChannel must be asked exactly once — a manager-level retry ' +
            'would duplicate the recovery the transport connection now owns',
    )
})

Deno.test('#361 W6 (ii) disconnect: an unwatch rejecting with undefined still rejects the disconnect', async () => {
    const driver = new RejectingUnwatchDriver(ROOM, undefined)
    const manager = managerOver(driver, new SpyAuthorizer())
    const c1 = conn('c1', 1)
    manager.register(c1)
    assert((await manager.subscribe(c1, ROOM)).ok)
    assertStrictEquals(await settled(manager.disconnect('c1')), undefined)
    assert(!state(manager).presence.has(ROOM))
})

/** The words of the WARN a failed durable revocation write logs. */
const DURABILITY_FAILED = 'the durable revocation write'
/** The words of the WARN a failed record clear logs. */
const CLEAR_FAILED = 'was applied but could not be cleared'

/** A defined failure, and the `undefined` one a value test would lose. */
const FAILURES: [string, unknown][] = [
    ['undefined', undefined],
    ['a defined error', new Error('the store failed')],
]

for (const [what, value] of FAILURES) {
    Deno.test(`#361 W6 (ii) evict: a durability write rejecting with ${what} rejects with that value, after the local revoke`, async () => {
        const driver = new RevocationDriver(value)
        driver.markFails = true
        const manager = managerOver(driver, new SpyAuthorizer())
        const c2 = conn('c2', 2)
        manager.register(c2)
        assert((await manager.subscribe(c2, ROOM)).ok)
        const warnings = captureWarnings()
        let outcome: unknown
        try {
            outcome = await settled(manager.evict('c2'))
        } finally {
            warnings.restore()
        }
        assertStrictEquals(outcome, value, 'the failure itself, not lost')
        assertEquals(warnings.having(DURABILITY_FAILED).length, 1)
        assert(c2.closed.includes(4403), 'revoked locally first')
        assert(!state(manager).connections.has('c2'))
    })

    Deno.test(`#361 W6 (ii) revokeChannel durability: a durability write rejecting with ${what} rejects with that value, after the local leave`, async () => {
        const driver = new RevocationDriver(value)
        driver.markFails = true
        const manager = managerOver(driver, new SpyAuthorizer())
        const c2 = conn('c2', 2)
        manager.register(c2)
        assert((await manager.subscribe(c2, ROOM)).ok)
        const warnings = captureWarnings()
        let outcome: unknown
        try {
            outcome = await settled(manager.revokeChannel('c2', ROOM))
        } finally {
            warnings.restore()
        }
        assertStrictEquals(outcome, value, 'the failure itself, not lost')
        assertEquals(warnings.having(DURABILITY_FAILED).length, 1)
        assert(
            !state(manager).subscriptions.get(ROOM)?.has('c2'),
            'left first',
        )
    })

    Deno.test(`#361 W6 (ii) revokeChannel clear: a record clear rejecting with ${what} rejects with that value, after the local leave`, async () => {
        const driver = new RevocationDriver(value)
        driver.clearFails = true
        const manager = managerOver(driver, new SpyAuthorizer())
        const c3 = conn('c3', 3)
        manager.register(c3)
        assert((await manager.subscribe(c3, ROOM)).ok)
        const warnings = captureWarnings()
        let outcome: unknown
        try {
            outcome = await settled(manager.revokeChannel('c3', ROOM))
        } finally {
            warnings.restore()
        }
        assertStrictEquals(outcome, value, 'the failure itself, not lost')
        assertEquals(warnings.having(CLEAR_FAILED).length, 1)
        assertEquals(warnings.having(DURABILITY_FAILED).length, 0)
        assert(
            !state(manager).subscriptions.get(ROOM)?.has('c3'),
            'left first',
        )
    })
}

Deno.test('#361 W7 pin: a join that committed before the disconnect resolves ok, and the disconnect tears it down', async () => {
    const driver = new GatedHoldDriver()
    const manager = managerOver(driver, new SpyAuthorizer())
    const c1 = conn('c1', 1)
    manager.register(c1)
    const subscribing = manager.subscribe(c1, ROOM)
    await driver.holding // committed: caps passed, the join started
    const disconnecting = manager.disconnect('c1')
    driver.openHold()
    assertEquals((await subscribing).ok, true)
    assertEquals(await disconnecting, 'disconnected')
    await tick()
    assertForgotten(manager, driver, 'c1', 1)
})

Deno.test('#361 W8 id reuse: a different object under a retiring id is refused, and a later one inherits nothing', async () => {
    const driver = new GatedUnwatchDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy)
    const o = conn('o', 2)
    manager.register(o)
    assert((await manager.subscribe(o, ROOM)).ok)
    const a = conn('c1', 1)
    manager.register(a)
    assert((await manager.subscribe(a, LOBBY)).ok)

    const disconnecting = manager.disconnect('c1')
    await driver.unwatching
    const b = conn('c1', 1)
    const calls = spy.calls
    assertThrows(() => manager.register(b), ConnectionIdInUseError)
    await assertRejects(
        () => manager.subscribe(b, ROOM),
        ConnectionIdInUseError,
    )
    assertEquals(spy.calls, calls, 'the authorizer was never called')
    driver.openUnwatch()
    await disconnecting

    const c = conn('c1', 1)
    manager.register(c)
    manager.broadcast(ROOM, 'ping', 1)
    const p = conn('p', 3)
    manager.register(p)
    assert((await manager.subscribe(p, ROOM)).ok) // a presence change
    await tick()
    assertEquals(c.received, [], 'nothing from a room it was never admitted to')

    // The positive control: once its own subscribe is authorized, it hears.
    assert((await manager.subscribe(c, ROOM)).ok)
    manager.broadcast(ROOM, 'pong', 2)
    await tick()
    assert(
        c.received.some((f) => f.type === 'event' && f.event === 'pong'),
        'delivery works, so the silence above is meaningful',
    )
})

Deno.test('#361 W9 a subscribe racing a suspended leave joins, instead of reading a membership the leave is removing', async () => {
    const driver = new GatedUnwatchDriver()
    const manager = managerOver(driver, new SpyAuthorizer())
    const c1 = conn('c1', 1)
    manager.register(c1)
    assert((await manager.subscribe(c1, ROOM)).ok)

    const leaving = manager.unsubscribe('c1', ROOM)
    await driver.unwatching
    assert((await manager.subscribe(c1, ROOM)).ok)
    driver.openUnwatch()
    await leaving
    await tick()

    const s = state(manager)
    assert(s.subscriptions.get(ROOM)?.has('c1'), 'subscribed')
    assert(s.presence.get(ROOM)?.has('c1'), 'a local presence member')
    assertEquals(driver.held(ROOM), [1], 'and a roster hold')
})

Deno.test('#361 W10 an evicted connection is retired: its subscribe is refused before the authorizer runs', async () => {
    const spy = new SpyAuthorizer()
    const driver = new RecordingDriver()
    const manager = managerOver(driver, spy)
    const c1 = conn('c1', 1)
    manager.register(c1)
    await manager.evict('c1')
    await assertRejects(
        () => manager.subscribe(c1, PRIVATE),
        ConnectionDisconnectedError,
    )
    await assertRejects(
        () => manager.subscribe(c1, ROOM),
        ConnectionDisconnectedError,
    )
    assertEquals(spy.calls, 0)
    assertNothingWritten(manager, driver)
})

Deno.test('#361 W11 the framework socket path presents one object for the socket life, so a closed socket stays retired', async () => {
    const manager = managerOver(new RecordingDriver(), new SpyAuthorizer())
    let captured: Connection<User> | undefined
    const events = buildEvents<User>(
        manager.handlerHooks({ onOpen: (c) => void (captured = c) }),
        { id: 1, name: 'user-1' },
    )
    const socket = { send() {}, close() {} } as unknown as WSContext
    events.onOpen?.(new Event('open'), socket)
    await until(() => manager.connectionCount === 1, 'the open')
    events.onClose?.(new CloseEvent('close', { code: 1000 }), socket)
    await until(() => manager.connectionCount === 0, 'the close')
    assert(captured !== undefined)
    await assertRejects(
        () => manager.subscribe(captured!, 'news'),
        ConnectionDisconnectedError,
    )
})

Deno.test('#361 W12 (i) handlerHooks disconnects even when the app onClose throws, and rejects with the app error', async () => {
    const APP = new Error('the application onClose failed')
    const manager = managerOver(new RecordingDriver(), new SpyAuthorizer())
    const hooks = manager.handlerHooks({
        onClose: () => {
            throw APP
        },
    })
    const c1 = conn('c1', 1)
    await hooks.onOpen?.(c1)
    assert((await manager.subscribe(c1, 'news')).ok)
    const closing = Promise.resolve().then(() => hooks.onClose?.(c1, 1000, ''))
    assertStrictEquals(await settled(closing), APP)
    assertEquals(manager.connectionCount, 0)
    assert(!state(manager).subscriptions.has('news'))
    await assertRejects(
        () => manager.subscribe(c1, 'news'),
        ConnectionDisconnectedError,
    )
})

Deno.test('#361 W12 (ii) when the disconnect fails too, the rejection is still the app error, and the disconnect failure is one WARN', async () => {
    const APP = new Error('the application onClose failed')
    const driver = new RejectingUnwatchDriver('news', new Error('unwatch'))
    const manager = managerOver(driver, new SpyAuthorizer())
    const hooks = manager.handlerHooks({
        onClose: () => {
            throw APP
        },
    })
    const c1 = conn('c1', 1)
    await hooks.onOpen?.(c1)
    assert((await manager.subscribe(c1, 'news')).ok)
    const warnings = captureWarnings()
    let outcome: unknown
    try {
        outcome = await settled(
            Promise.resolve().then(() => hooks.onClose?.(c1, 1000, '')),
        )
    } finally {
        warnings.restore()
    }
    assertStrictEquals(outcome, APP)
    assertEquals(warnings.having(CLOSE_DISCONNECT_FAILED).length, 1)
    assertEquals(manager.connectionCount, 0)
})

Deno.test('#361 W13 the post-site sits above the caps: a disconnect during the authorizer is ConnectionDisconnectedError, not ChannelLimitError', async () => {
    // Moved here from the #370 witness file (A3): the #361 battery runs only
    // this suite, and its row for "the post-site moved below the caps" dies on
    // this test. A full instance makes the order observable — below the caps,
    // the refusal a retired connection hears would be the cap's.
    const driver = new RecordingDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy, {
        maxWatchedChannels: 1,
        maxChannelsPerConnection: 1,
        anonymousHostingShare: 1,
    })
    const c2 = conn('c2', 2)
    manager.register(c2)
    assert((await manager.subscribe(c2, 'news')).ok, 'the one slot is taken')
    const c1 = conn('c1', 1)
    manager.register(c1)

    spy.gateNext('private-y')
    const subscribing = settled(manager.subscribe(c1, 'private-y'))
    await spy.reached
    await manager.disconnect('c1')
    spy.admit()
    const outcome = await subscribing

    assert(
        outcome instanceof ConnectionDisconnectedError,
        `the lifecycle refusal, not the cap's. Got: ${outcome}`,
    )
    assert(!(outcome instanceof ChannelLimitError))
})
