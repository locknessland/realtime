/**
 * @fileoverview #370 and #363 — `register` is the only way in, one owner object
 * per connection id, and every teardown the framework runs acts only on the
 * owner.
 *
 * Before this, `ChannelManager` admitted a connection object under its id at
 * two points — `register`, and `subscribe` implicitly — and neither asked
 * whether a DIFFERENT, live object already held that id. `disconnect` was keyed
 * by id, so it tore down whoever held the id when it ran. Three defects came
 * out of that:
 *
 * - **#370, a zombie.** A connection the app never registered, whose first
 *   `subscribe` waited on its authorizer while its socket closed, was bound by
 *   that subscribe AFTER the disconnect found nothing: a membership, cap slots
 *   and a count that nothing would ever reclaim.
 * - **#363, a takeover.** A second object under a live id took the binding, so
 *   delivery resolved to it for every channel the first one held — private and
 *   presence included — without its own authorizer ever running for them.
 * - **#363, a cross-teardown.** The refused socket's own close, or a late close
 *   after an evict and a fast reconnect, tore down the live holder.
 *
 * Each witness below reads state, not only the thrown class, and every "nothing
 * reached it" is paired with a positive control that does reach a connection,
 * so a broken broadcast cannot pass as silence.
 *
 * Test names start `#370 W<n> ` with a trailing space, so a battery naming
 * `W1 ` never matches `W11`.
 *
 * @module @lockness/realtime/tests/register_only_admission_370
 */

import {
    assert,
    assertEquals,
    assertInstanceOf,
    assertRejects,
    assertStrictEquals,
    assertThrows,
} from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import {
    ChannelManager,
    type ChannelManagerOptions,
    ConnectionDisconnectedError,
    ConnectionIdInUseError,
    ConnectionNotRegisteredError,
} from '../manager.ts'
import type { AuthorizeResult, PresenceMember } from '../channel.ts'
import type { Connection, WebSocketHooks } from '../types.ts'

const PRIVATE = 'private-x'
const ROOM = 'presence-room'

interface User {
    id: number
    name: string
}

const ALICE: User = { id: 1, name: 'alice' }
const MALLORY: User = { id: 3, name: 'mallory' }
const OBSERVER: User = { id: 2, name: 'observer' }

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
    /** Every `close(code, reason)` the transport was asked for. */
    readonly closes: [number | undefined, string | undefined][]
}

/** A fake transport socket that records its frames and its close calls. */
function conn(id: string, identity: User | null = ALICE): Recording {
    const received: Record<string, unknown>[] = []
    const closes: [number | undefined, string | undefined][] = []
    return {
        id,
        identity,
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: (code?: number, reason?: string) =>
            void closes.push([code, reason]),
        received,
        closes,
    } as Recording
}

/** What the application decides, before any gate: every identity admits. */
function decide(user: User | null, channel: string): AuthorizeResult {
    if (user === null) return false
    return channel.startsWith('presence-')
        ? { id: user.id, info: { name: user.name } }
        : true
}

/**
 * An authorizer that counts its calls, records the identities it saw, and can
 * suspend the next call for one channel until the test opens it.
 */
class SpyAuthorizer {
    calls = 0
    readonly saw: (User | null)[] = []
    /** Identities this authorizer admits; `undefined` admits every one. */
    only?: number
    #gated?: string
    #settle?: (result: AuthorizeResult) => void
    #decided: AuthorizeResult = false

    readonly fn = (
        user: User | null,
        channel: string,
    ): AuthorizeResult | Promise<AuthorizeResult> => {
        this.calls++
        this.saw.push(user)
        const decided = this.only !== undefined && user?.id !== this.only
            ? false
            : decide(user, channel)
        if (channel !== this.#gated) return decided
        this.#gated = undefined
        this.#decided = decided
        return new Promise<AuthorizeResult>((resolve) => {
            this.#settle = resolve
        })
    }

    /** Suspend the next authorization of `channel`. */
    gateNext(channel: string): void {
        this.#gated = channel
    }

    /** Let a suspended call answer as the ungated authorizer would. */
    admit(): void {
        this.#settle?.(this.#decided)
    }
}

/**
 * A memory driver that records watches and unwatches, and can suspend the
 * FIRST unwatch it is asked for until the test opens it (W12 (ii)).
 */
class RecordingDriver extends MemoryBroadcastDriver {
    readonly watched: string[] = []
    readonly unwatched: string[] = []
    #gateFirst = false
    #open!: () => void
    readonly #gate = new Promise<void>((resolve) => (this.#open = resolve))
    #reach!: () => void
    /** Resolves once the gated unwatch has been asked for and is suspended. */
    readonly unwatching = new Promise<void>((
        resolve,
    ) => (this.#reach = resolve))

    watchChannel(channel: string): void {
        this.watched.push(channel)
    }

    unwatchChannel(channel: string): void | Promise<void> {
        this.unwatched.push(channel)
        if (!this.#gateFirst) return
        this.#gateFirst = false
        this.#reach()
        return this.#gate
    }

    /** Suspend the next unwatch — and only that one. */
    gateFirstUnwatch(): void {
        this.#gateFirst = true
    }

    /** Let the suspended unwatch finish. */
    openUnwatch(): void {
        this.#open()
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

function managerOver(
    driver: RecordingDriver,
    spy: SpyAuthorizer,
    options: ChannelManagerOptions<User> = {},
): ChannelManager<User> {
    return new ChannelManager<User>({ driver, authorize: spy.fn, ...options })
}

/** How a promise settled: `'resolved'`, or the rejection value itself. */
async function settled(promise: Promise<unknown>): Promise<unknown> {
    return await promise.then(() => 'resolved', (error) => error)
}

/** A macrotask: every queued microtask chain has run when it resolves. */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

/** The event frames `c` received for `event`. */
const eventsNamed = (c: Recording, event: string) =>
    c.received.filter((f) => f.type === 'event' && f.event === event)

/** Assert no map the test can reach names `id`. */
function assertNothingNames(manager: ChannelManager<User>, id: string): void {
    const s = state(manager)
    assert(!s.connections.has(id), `connections still names ${id}`)
    for (const [channel, set] of s.subscriptions) {
        assert(!set.has(id), `subscriptions of ${channel} still name ${id}`)
    }
    for (const [channel, members] of s.presence) {
        assert(!members.has(id), `presence of ${channel} still names ${id}`)
    }
}

/**
 * A live A (`c1`, alice) holding `private-x` and the presence room, beside an
 * observer `o` holding both — the fixture W4, W5 and W8 start from.
 */
async function liveHolder(spy = new SpyAuthorizer()) {
    const driver = new RecordingDriver()
    const manager = managerOver(driver, spy)
    const o = conn('o', OBSERVER)
    manager.register(o)
    const a = conn('c1', ALICE)
    manager.register(a)
    assert((await manager.subscribe(o, PRIVATE)).ok)
    assert((await manager.subscribe(o, ROOM)).ok)
    assert((await manager.subscribe(a, PRIVATE)).ok)
    assert((await manager.subscribe(a, ROOM)).ok)
    await tick()
    return { driver, manager, spy, o, a }
}

/**
 * A custom transport driving `handlerHooks` directly, with a live A opened
 * through it — the fixture W7, W11 and W13 start from.
 */
async function hooksWithHolder(user: WebSocketHooks<User> = {}) {
    const driver = new RecordingDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy)
    const opened: Connection<User>[] = []
    const hooks = manager.handlerHooks({
        ...user,
        onOpen: (c) => void opened.push(c),
    })
    const a = conn('c1', ALICE)
    await hooks.onOpen?.(a)
    assert((await manager.subscribe(a, PRIVATE)).ok)
    return { driver, manager, spy, hooks, opened, a }
}

// --- US1: an unregistered connection is refused before it costs anything -----

Deno.test('#370 W1 an unregistered subscribe racing its own disconnect is refused before the authorizer, and holds nothing', async () => {
    const driver = new RecordingDriver()
    const spy = new SpyAuthorizer()
    const manager = managerOver(driver, spy, {
        maxWatchedChannels: 2,
        maxChannelsPerConnection: 2,
    })
    const c1 = conn('c1', ALICE)

    spy.gateNext(PRIVATE)
    const subscribing = settled(manager.subscribe(c1, PRIVATE))
    assertEquals(
        await manager.disconnect('c1'),
        'not-owned',
        'the disconnect finds nothing to retire',
    )
    spy.admit()
    const outcome = await subscribing
    await tick()

    assertInstanceOf(outcome, ConnectionNotRegisteredError)
    assert(!outcome.message.includes('c1'), 'the message carries no id (S3)')
    assertEquals(spy.calls, 0, 'the authorizer never ran')
    assertNothingNames(manager, 'c1')
    assertEquals(manager.connectionCount, 0, 'no zombie is counted')
    assert(!driver.watched.includes(PRIVATE), 'no watch of private-x')

    // CONTROL: the cap is intact — a registered connection takes both slots.
    const c2 = conn('c2', OBSERVER)
    manager.register(c2)
    assert((await manager.subscribe(c2, 'a')).ok)
    assert((await manager.subscribe(c2, 'b')).ok, 'both slots are free')
})

Deno.test('#370 W2 an unregistered anonymous connection takes no watch and no anonymous share', async () => {
    const driver = new RecordingDriver()
    const manager = managerOver(driver, new SpyAuthorizer(), {
        maxWatchedChannels: 2,
        maxChannelsPerConnection: 2,
        anonymousHostingShare: 0.5,
    })
    const a = conn('a', null)
    await assertRejects(
        () => manager.subscribe(a, 'news'),
        ConnectionNotRegisteredError,
    )
    assert(!driver.watched.includes('news'), 'no watch of news')

    // CONTROL: the one anonymous share is still free for a registered one.
    const b = conn('b', null)
    manager.register(b)
    assertEquals(await manager.subscribe(b, 'feed'), { ok: true })
    assertEquals(manager.connectionCount, 1)
})

Deno.test('#370 W3 a fresh object per call under a registered id is refused, before the authorizer', async () => {
    const spy = new SpyAuthorizer()
    const manager = managerOver(new RecordingDriver(), spy)
    manager.register(conn('c1', ALICE))
    await assertRejects(
        () => manager.subscribe(conn('c1', ALICE), PRIVATE),
        ConnectionIdInUseError,
    )
    assertEquals(spy.calls, 0, 'the authorizer never ran')
})

// --- US2: a second object under a live id is refused --------------------------

Deno.test('#370 W4 a second object under a live id is refused everywhere, receives nothing, and the holder keeps receiving', async () => {
    const { manager, spy, o, a } = await liveHolder()
    const b = conn('c1', MALLORY)
    const calls = spy.calls

    const error = assertThrows(
        () => manager.register(b),
        ConnectionIdInUseError,
    )
    await assertRejects(
        () => manager.subscribe(b, PRIVATE),
        ConnectionIdInUseError,
    )
    await assertRejects(
        () => manager.subscribe(b, ROOM),
        ConnectionIdInUseError,
    )
    assertEquals(spy.calls, calls, 'the authorizer never ran for B')
    assert(!error.message.includes('c1'), 'the message carries no id (S3)')
    assert(
        !error.message.includes('disconnect'),
        'and does not claim a teardown is running',
    )
    assertStrictEquals(state(manager).connections.get('c1'), a)

    manager.broadcast(PRIVATE, 'secret', 1)
    await manager.unsubscribe('o', ROOM) // a presence change by o
    await tick()
    assertEquals(b.received, [], 'B received no frame')
    // CONTROL: the holder received both, so B's silence is meaningful.
    assertEquals(eventsNamed(a, 'secret').length, 1, 'A hears the broadcast')
    assert(
        a.received.some((f) => f.type === 'presence' && f.action === 'left'),
        'A hears the presence change',
    )
    assertEquals(o.received.length > 0, true)
})

Deno.test('#370 W5 once the holder itself disconnects, the id is free and B is admitted on its own terms', async () => {
    const { manager, a } = await liveHolder()
    const b = conn('c1', MALLORY)
    assertThrows(() => manager.register(b), ConnectionIdInUseError)

    assertEquals(await manager.disconnect(a), 'disconnected')
    assert(!state(manager).connections.has('c1'), 'the id is unbound')
    assertEquals(manager.connectionCount, 1, 'only the observer is left')

    manager.register(b)
    assert((await manager.subscribe(b, PRIVATE)).ok)
    manager.broadcast(PRIVATE, 'after', 1)
    await tick()
    assertEquals(eventsNamed(b, 'after').length, 1, 'B hears its own channel')
})

Deno.test('#370 W15 a refused presence join takes no roster hold and announces no joined', async () => {
    const { manager, driver, o } = await liveHolder()
    // A presence channel A does NOT hold (#403 review MEDIUM). B shares A's
    // id, and ROOM is a channel A already holds under that SAME id — so
    // #327's own re-join guard (`members.has(connection.id)`) would mask
    // clause 2's refusal under test: remove clause 2 and B reaches
    // `#joinPresence`, but the re-join guard still reads B's (== A's) id as
    // already a member THERE and still takes no new hold and announces
    // nothing, for a wholly different reason. UNHELD is a channel neither A
    // nor B has ever joined, so once C controls that an admitted join DOES
    // move both readings, nothing but the admission refusal can explain B
    // leaving them unchanged.
    const UNHELD = 'presence-unheld'
    assert((await manager.subscribe(o, UNHELD)).ok)
    await tick()

    const heldBefore = driver.readRoster(UNHELD, 1_000, []).members.length
    const framesBefore = o.received.length
    const b = conn('c1', MALLORY)
    await assertRejects(
        () => manager.subscribe(b, UNHELD),
        ConnectionIdInUseError,
    )
    await tick()
    assertEquals(
        driver.readRoster(UNHELD, 1_000, []).members.length,
        heldBefore,
        'no roster hold was taken for B',
    )
    assertEquals(o.received.length, framesBefore, 'no frame reached the room')

    // CONTROL: an admitted join moves both readings.
    const c3 = conn('c3', { id: 4, name: 'carol' })
    manager.register(c3)
    assert((await manager.subscribe(c3, UNHELD)).ok)
    await tick()
    assertEquals(
        driver.readRoster(UNHELD, 1_000, []).members.length,
        heldBefore + 1,
    )
    assert(
        o.received.some((f) => f.type === 'presence' && f.action === 'joined'),
        'CONTROL: the room hears an admitted join',
    )
})

Deno.test('#370 W6 the same object registered twice is a no-op', async () => {
    const manager = managerOver(new RecordingDriver(), new SpyAuthorizer())
    const a = conn('c1', ALICE)
    manager.register(a)
    manager.register(a)
    assertEquals(manager.connectionCount, 1)
    assert((await manager.subscribe(a, 'news')).ok)
    manager.broadcast('news', 'ping', 1)
    await tick()
    assertEquals(eventsNamed(a, 'ping').length, 1, 'A still receives')
})

Deno.test('#370 W8 the registered identity is the authorized one — the authorizer never sees a second identity under a live id', async () => {
    const spy = new SpyAuthorizer()
    const { manager } = await liveHolder(spy)
    spy.only = MALLORY.id
    const b = conn('c1', MALLORY)
    await assertRejects(
        () => manager.subscribe(b, PRIVATE),
        ConnectionIdInUseError,
    )
    assert(
        !spy.saw.some((user) => user?.id === MALLORY.id),
        'the authorizer never saw mallory',
    )
})

Deno.test('#370 W9 a retired object is ConnectionDisconnectedError, not ConnectionNotRegisteredError — admissibility is asked first', async () => {
    const manager = managerOver(new RecordingDriver(), new SpyAuthorizer())
    const a = conn('c1', ALICE)
    manager.register(a)
    await manager.disconnect('c1')
    await assertRejects(
        () => manager.subscribe(a, 'news'),
        ConnectionDisconnectedError,
    )
})

// --- US5: a custom transport that reuses ids and drives handlerHooks ----------

Deno.test('#370 W7 handlerHooks refuses a second socket under a live id synchronously, with a 1011, and leaves the first untouched', async () => {
    const { manager, opened, hooks, a } = await hooksWithHolder()
    const b = conn('c1', MALLORY)
    assertThrows(() => hooks.onOpen?.(b), ConnectionIdInUseError)
    assertEquals(b.closes, [[1011, 'unusable connection id']])
    assertEquals(opened, [a], "the app's onOpen ran once, for A")
    assertStrictEquals(state(manager).connections.get('c1'), a)
    manager.broadcast(PRIVATE, 'still', 1)
    await tick()
    assertEquals(eventsNamed(a, 'still').length, 1, 'A still receives')
})

// --- US3: the refused socket's close, and a late close, harm no one else -------

Deno.test("#370 W11 the refused socket's own close leaves the holder bound, subscribed and receiving", async () => {
    const { manager, hooks, a } = await hooksWithHolder()
    const b = conn('c1', MALLORY)
    assertThrows(() => hooks.onOpen?.(b), ConnectionIdInUseError)

    assertEquals(
        await manager.disconnect(b),
        'not-owned',
        'the object form refuses a socket that does not own its id',
    )
    await hooks.onClose?.(b, 1000, '')

    assertStrictEquals(state(manager).connections.get('c1'), a)
    manager.register(a) // not retired: re-registering the owner is a no-op
    assert(state(manager).subscriptions.get(PRIVATE)?.has('c1'))
    manager.broadcast(PRIVATE, 'after-close', 1)
    await tick()
    assertEquals(eventsNamed(a, 'after-close').length, 1, 'A still receives')
})

Deno.test('#370 W12 (i) a late close of an evicted socket leaves its re-registered successor alone', async () => {
    const driver = new RecordingDriver()
    const manager = managerOver(driver, new SpyAuthorizer())
    const hooks = manager.handlerHooks()
    const a0 = conn('c1', ALICE)
    await hooks.onOpen?.(a0)
    await manager.evict('c1')

    const a1 = conn('c1', ALICE)
    await hooks.onOpen?.(a1)
    assert((await manager.subscribe(a1, 'news')).ok)

    assertEquals(
        await manager.disconnect(a0),
        'not-owned',
        'the evicted object no longer owns its id',
    )
    await hooks.onClose?.(a0, 4403, 'evicted') // the old socket's late close

    assertStrictEquals(state(manager).connections.get('c1'), a1)
    assert(state(manager).subscriptions.get('news')?.has('c1'))
    manager.broadcast('news', 'ping', 1)
    await tick()
    assertEquals(eventsNamed(a1, 'ping').length, 1, 'A1 still receives')
})

Deno.test('#370 W12 (ii) [re-anchored #393] a teardown whose object was replaced leaves the new binding alone', async () => {
    const driver = new RecordingDriver()
    const manager = managerOver(driver, new SpyAuthorizer())
    const hooks = manager.handlerHooks()
    const a0 = conn('c1', ALICE)
    await hooks.onOpen?.(a0)
    assert((await manager.subscribe(a0, 'news')).ok)

    // A0's own close starts the one teardown for it, suspended inside its
    // loop on the unwatch.
    driver.gateFirstUnwatch()
    const closing = settled(
        Promise.resolve().then(() => hooks.onClose?.(a0, 1000, '')),
    )
    await driver.unwatching

    // #393: a racing evict of the SAME object now JOINS that teardown
    // instead of running an independent copy that settles on its own — which
    // is what used to free the id early, mid-loop, for the `finally` guard
    // below to have to catch. Joined, the id stays A0's for as long as
    // either call is pending: a different object cannot take it over.
    const evicting = settled(manager.evict('c1'))
    // Give evict's OWN internal id-form call the chance it needs to run: in
    // the unfixed tree its independent copy finds `news` already gone (A0's
    // own sync delete, above) and settles right here, freeing the id early.
    await tick()
    assertThrows(
        () => manager.register(conn('c1', ALICE)),
        ConnectionIdInUseError,
        undefined,
        "the id is still A0's — evict joined rather than freeing it early",
    )
    assert(state(manager).connections.has('c1'), 'a0 is still bound')

    // The `finally` guard this row has pinned since #363 is reachable
    // through the public API only if a different object takes the binding
    // mid-loop regardless — a path #393 closes (proven above). The guard
    // itself is unchanged, so it is exercised directly here, the floor a
    // later change to it must not remove.
    const a1 = conn('c1', ALICE)
    state(manager).connections.set('c1', a1)

    driver.openUnwatch()
    assertEquals(await closing, 'resolved')
    assertEquals(await evicting, 'resolved')

    assertStrictEquals(state(manager).connections.get('c1'), a1)
})

Deno.test('#370 W14 [re-anchored #393] a teardown whose object was replaced stops its loop — the new owner keeps a channel the old one also held', async () => {
    // W12 (ii) pins the `finally`; this pins the loop (#370 review, MEDIUM).
    // #393 closes the public-API path that used to reach either guard: a
    // racing evict of the SAME object now joins its own teardown instead of
    // freeing the id early, mid-loop (proven in W12 (ii)). Both guards are
    // unchanged code, so this row still pokes the binding directly, mid-loop,
    // as the floor a later change to the loop-stop check must not remove.
    const driver = new RecordingDriver()
    const manager = managerOver(driver, new SpyAuthorizer())
    const hooks = manager.handlerHooks()
    const a0 = conn('c1', ALICE)
    await hooks.onOpen?.(a0)
    assert((await manager.subscribe(a0, 'news')).ok)
    assert((await manager.subscribe(a0, 'sports')).ok)

    driver.gateFirstUnwatch()
    const closing = settled(
        Promise.resolve().then(() => hooks.onClose?.(a0, 1000, '')),
    )
    await driver.unwatching

    // The binding changes hands WHILE the teardown's own loop is still stuck
    // on `news` — before it has re-checked ownership for `sports`, the
    // channel both objects held.
    const a1 = conn('c1', ALICE)
    state(manager).connections.set('c1', a1)
    // CONTROL: A1 (now the id's owner) hears `sports` before the stalled
    // teardown resumes.
    manager.broadcast('sports', 'before', 1)
    await tick()
    assertEquals(eventsNamed(a1, 'before').length, 1, 'CONTROL: A1 receives')

    driver.openUnwatch()
    assertEquals(await closing, 'resolved')

    assertStrictEquals(state(manager).connections.get('c1'), a1)
    assert(
        state(manager).subscriptions.get('sports')?.has('c1'),
        "A1 still holds `sports`: the old teardown did not leave it for A1's id",
    )
    manager.broadcast('sports', 'after', 1)
    await tick()
    assertEquals(eventsNamed(a1, 'after').length, 1, 'A1 still receives')
})

Deno.test("#370 W13 handlerHooks runs the app's onMessage only for the socket that owns its id", async () => {
    const seen: Connection<User>[] = []
    const { manager, hooks, a } = await hooksWithHolder({
        onMessage: (c) => void seen.push(c),
    })
    const b = conn('c1', MALLORY)
    assertThrows(() => hooks.onOpen?.(b), ConnectionIdInUseError)

    await hooks.onMessage?.(b, '{"type":"ping"}')
    assertEquals(seen, [], 'the refused socket reaches no app code')
    await hooks.onMessage?.(a, '{"type":"ping"}')
    assertEquals(seen, [a], 'CONTROL: the owner does')

    await manager.disconnect(a)
    await hooks.onMessage?.(a, '{"type":"ping"}')
    assertEquals(seen, [a], 'a retired socket reaches no app code')
})
