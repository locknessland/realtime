/**
 * @fileoverview #332 — `revokeChannel` reaches a socket on any instance, and
 * leaves it open.
 *
 * Before this verb there was no supported call for *"drop this connection from
 * this one room, everywhere."* `evict` is per-connection and hard-closes with
 * 4403, taking every other still-authorized channel with it — and the framework
 * client ships **no reconnect logic at all**, so a per-room moderation action
 * delivered by `evict` silently ends the user's whole realtime session. At
 * fleet scale, one kick per user per room is a socket storm. It is not a
 * narrower revoke; it is a different, louder action.
 *
 * **The socket-stays-open assertion is the headline**, and it is asserted by
 * observation rather than by return value: a broadcast on another channel the
 * connection still holds has to reach it. A test that only checked `close` was
 * not called would pass on an implementation that tore the connection out of
 * every room without closing it.
 *
 * **A revocation is not a ban.** The connection may re-subscribe immediately if
 * the application's `authorize` admits it; this framework owns no deny list and
 * `subscribe` never consults the revocation index. That is #331's settled model
 * and it is asserted here so it cannot be quietly reversed into one.
 *
 * @module @lockness/realtime/tests/channel_revoke_332
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import {
    ChannelManager,
    ChannelNameError,
    ConnectionIdError,
    RevocationScopeError,
} from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ControlMessage,
    Revocation,
} from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'

/**
 * How a stub store keys a record: by its exact id when it has one (#337).
 *
 * Keyed by the pair, a stub would model the defect #337 removed — two
 * revocations of one pair collapsing into one record — and every manager test
 * built on it would pass against a driver that reopened it.
 */
const key = (r: Revocation) => r.channel === undefined ? r.target : r.id

interface User {
    id: number
}

const ROOM = 'presence-room'
const OTHER = 'private-orders'

/**
 * Run the microtask queue out.
 *
 * **A control frame is applied asynchronously and deliberately.**
 * `handleControl` is synchronous and dispatches the apply as `void`, because
 * the driver's ingest path has no caller to await it and no one to hand a
 * rejection to. So `await a.revokeChannel(...)` resolves when A has recorded
 * and published — not when B has acted. Anything asserted about B's state has
 * to let B's continuations run first, and a test that forgot to would be
 * asserting on a frame still in flight.
 */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

function conn(
    id: string,
    userId: number,
): Connection<User> & {
    readonly received: Record<string, unknown>[]
    readonly closes: number[]
} {
    const received: Record<string, unknown>[] = []
    const closes: number[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: (code?: number) => void closes.push(code ?? 1000),
        received,
        closes,
    } as Connection<User> & {
        readonly received: Record<string, unknown>[]
        readonly closes: number[]
    }
}

/**
 * Two instances over one shared roster, one shared event bus and one shared
 * control plane — plus a shared revocation index they both read and write.
 *
 * `dropControl` models the case durability exists for: the frame is published
 * and never arrives. That is the only way to witness the reconcile doing its
 * job, and it must be a property of the BUS rather than of the manager, or the
 * test proves something about a method it stubbed.
 */
function twoInstances(options: { dropControl?: boolean } = {}) {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const index = new Map<string, Revocation>()
    const controlHandlers: ((control: ControlMessage) => void)[] = []
    const messageHandlers: ((message: BroadcastMessage) => void)[] = []
    const reconcilers: (() => unknown)[] = []

    const driverFor = (index_: number): BroadcastDriver => ({
        publish(message) {
            for (const handler of messageHandlers) handler(message)
        },
        onMessage(handler) {
            messageHandlers[index_] = handler
        },
        onControl(handler) {
            controlHandlers[index_] = handler
        },
        onRevocationReconcile(handler) {
            reconcilers[index_] = handler
        },
        publishControl(control) {
            if (options.dropControl) return Promise.resolve()
            for (let i = 0; i < controlHandlers.length; i++) {
                // The publisher's own loopback is dropped by the real driver
                // before the MAC check; modelled here so an instance never
                // acts twice on its own frame.
                if (i === index_) continue
                controlHandlers[i]?.(control)
            }
            return Promise.resolve()
        },
        holdMember(channel, member) {
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
        markRevocation(revocation) {
            index.set(key(revocation), revocation)
            return Promise.resolve()
        },
        listRevocations() {
            return Promise.resolve([...index.values()])
        },
        clearRevocation(revocation) {
            index.delete(key(revocation))
            return Promise.resolve()
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    })

    const authorize = (identity: User | null): PresenceMember | false =>
        identity ? { id: identity.id } : false

    // B first, so its control handler is registered before A publishes.
    const b = new ChannelManager<User>({ driver: driverFor(1), authorize })
    const a = new ChannelManager<User>({ driver: driverFor(0), authorize })
    /** Fire one reconcile tick on B — the instance that owns the sockets. */
    const tickB = async () => {
        await reconcilers[1]?.()
        await settle()
    }
    return { a, b, roster, index, tickB }
}

Deno.test('#332 A revokes one room on a socket B owns — and B keeps the socket', async () => {
    const { a, b, roster } = twoInstances()
    const victim = conn('c1', 1)
    b.register(victim)
    const observerOnA = conn('c2', 2)
    a.register(observerOnA)

    // B owns the socket, in TWO channels. The second one is what proves the
    // revoke is scoped rather than merely quiet.
    await b.subscribe(victim, ROOM)
    await b.subscribe(victim, OTHER)
    await a.subscribe(observerOnA, ROOM)
    const framesBefore = observerOnA.received.length

    const outcome = await a.revokeChannel('c1', ROOM)
    await settle()

    assertEquals(
        outcome,
        'not-owned',
        'A does not own the socket: it recorded the revocation and routed it',
    )
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])],
        ['2'],
        'the authoritative roster no longer lists the revoked member',
    )
    assertEquals(
        observerOnA.received.slice(framesBefore).filter((f) =>
            f.action === 'left'
        ).length,
        1,
        'a presence subscriber on the OTHER instance was told exactly once',
    )
    assertEquals(victim.closes, [], 'THE SOCKET STAYS OPEN — no close at all')
    assertEquals(
        victim.received.filter((f) => f.type === 'unsubscribed'),
        [{ type: 'unsubscribed', channel: ROOM }],
        'and the target is TOLD, exactly once — it was removed from the ' +
            "channel's subscriber set before the `left` fanned out, so it " +
            'does not even receive its own departure',
    )

    // The point of the whole verb: the other room still works.
    await b.broadcast(OTHER, 'tick', { n: 1 })
    assertEquals(
        victim.received.filter((f) => f.event === 'tick').length,
        1,
        'a broadcast on the channel it still holds reaches it',
    )
})

Deno.test('#332 revoking a room this instance OWNS reports what it did', async () => {
    const { b } = twoInstances()
    const victim = conn('c1', 1)
    b.register(victim)
    await b.subscribe(victim, ROOM)

    assertEquals(await b.revokeChannel('c1', ROOM), 'revoked')
    assertEquals(
        await b.revokeChannel('c1', ROOM),
        'not-subscribed',
        'owned, and no longer in that room — idempotent and distinguishable',
    )
    assertEquals(
        victim.received.filter((f) => f.type === 'unsubscribed').length,
        1,
        'and the client is told ONCE, not once per call: the frame is gated ' +
            'on a leave that actually happened, not on the verb being called',
    )
})

Deno.test('#332 a revocation is NOT a ban — the client may re-subscribe', async () => {
    // #331's settled model, asserted from this side so it cannot be quietly
    // reversed into a framework-owned deny list. `subscribe` does not consult
    // the revocation index, deliberately: a stale entry would otherwise refuse
    // a join the application has re-authorized, and the framework would own a
    // policy it cannot explain.
    const { b } = twoInstances()
    const victim = conn('c1', 1)
    b.register(victim)
    await b.subscribe(victim, ROOM)
    await b.revokeChannel('c1', ROOM)

    const rosterReadsBefore = rosterReadCount()
    const again = await b.subscribe(victim, ROOM)
    assertRosterRead(rosterReadsBefore)
    assertEquals(again.ok, true, 'the application authorizes; it is admitted')
    assertEquals(again.here?.members.map((m) => m.id), [1])
})

Deno.test('#332 BOTH names are asserted, and nothing is published on refusal', async () => {
    // A dedicated rig with a publish spy, because the assertion is that
    // NOTHING was published — which the shared two-instance rig cannot show.
    const published: ControlMessage[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control)
            return Promise.resolve()
        },
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: () => Promise.resolve(),
    }
    const m = new ChannelManager<User>({ driver })

    await assertRejects(
        () => m.revokeChannel('has space', ROOM),
        ConnectionIdError,
    )
    await assertRejects(
        () => m.revokeChannel('c1', 'orders room'),
        ChannelNameError,
    )
    assertEquals(
        published,
        [],
        'a name the control plane would drop on ingest must never be minted ' +
            'onto it — that is a revocation reporting success having revoked ' +
            'nothing, which is the defect this whole feature removes',
    )
})

Deno.test('#332 a driver that can ROUTE but not RECORD is refused', async () => {
    const published: ControlMessage[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl(control) {
            published.push(control)
            return Promise.resolve()
        },
        // No revocation trio.
    }
    const m = new ChannelManager<User>({ driver })

    await assertRejects(
        () => m.revokeChannel('c1', ROOM),
        RevocationScopeError,
    )
    assertEquals(
        published,
        [],
        'nothing published — a revoke resting on one frame arriving is the ' +
            'undurable path, and a lost frame there is invisible rather than ' +
            'merely silent',
    )
    // `evict` is unaffected: it never required a store, and it is the escape
    // hatch the docs point at. ASSERTED rather than merely called — an
    // un-asserted call proves only that it did not throw.
    await m.evict('c1')
    assertEquals(
        published.map((c) => c.kind),
        ['evict'],
        'evict still routes on a driver with no revocation store, which is ' +
            'what makes it the mid-deploy escape hatch the docs point at',
    )
})

Deno.test('#332 a single-process driver revokes locally and reports honestly', async () => {
    // No control plane and no store: nothing to route to, nothing to lose a
    // frame on, so no durability is owed. What must NOT happen is a silent
    // success for a target this instance does not hold — that is verbatim the
    // defect the whole issue is about, and it would have been reintroduced by
    // the verb built to fix it.
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        // A presence channel needs an authorizer, or the subscribe is denied
        // and the connection is never owned — at which point this test would
        // assert `'not-owned'` for the wrong reason entirely.
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const victim = conn('c1', 1)
    m.register(victim)
    await m.subscribe(victim, ROOM)

    assertEquals(await m.revokeChannel('c1', ROOM), 'revoked')
    assertEquals(victim.closes, [], 'socket still open')
    assertEquals(
        await m.revokeChannel('somebody-else', ROOM),
        'not-owned',
        'NOT a silent success. There is no owner to route to, and saying so ' +
            'is the whole point of the outcome',
    )
})

Deno.test('#332 a LOST control frame is recovered by the reconcile, exactly once', async () => {
    // The case durability exists for, and it is modelled at the BUS rather
    // than by stubbing the manager: the frame is published and never arrives.
    // Stubbing `handleControl` instead would prove something about a method
    // the test replaced.
    const { a, b, roster, index, tickB } = twoInstances({ dropControl: true })
    const victim = conn('c1', 1)
    b.register(victim)
    const observerOnB = conn('c2', 2)
    b.register(observerOnB)
    await b.subscribe(victim, ROOM)
    await b.subscribe(victim, OTHER)
    await b.subscribe(observerOnB, ROOM)

    assertEquals(await a.revokeChannel('c1', ROOM), 'not-owned')
    await settle()
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])].sort(),
        ['1', '2'],
        'precondition: the frame was lost, so nothing has happened yet',
    )
    assertEquals(index.size, 1, 'but the durable record was written first')

    await tickB()

    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])],
        ['2'],
        'the owning instance applies the channel leave on its next tick',
    )
    assertEquals(victim.closes, [], 'and still does not close the socket')
    assertEquals(
        observerOnB.received.filter((f) => f.action === 'left').length,
        1,
        'the room is told once',
    )
    assertEquals(
        index.size,
        0,
        'THE RECORD IS CLEARED ON APPLY. A record means exactly "a revocation ' +
            'the owner has not applied yet" — leaving it would re-apply the ' +
            'leave at every tick for the whole TTL',
    )

    // The half a single tick cannot show: the client legitimately re-subscribes
    // and must STAY. With the record uncleared this is where it gets kicked
    // again, once per tick, for up to `revocationTtlSeconds`.
    const again = await b.subscribe(victim, ROOM)
    assertEquals(again.ok, true)
    await tickB()
    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])].sort(),
        ['1', '2'],
        'a second tick does nothing, and the re-subscribed member stays',
    )
})

Deno.test('#332 a CONNECTION-scoped record still hard-closes — scope is dispatched, not re-decided', async () => {
    // The other arm of the one mapping. Both entry points that have no caller
    // — the control frame and the reconcile — route through it, so a third
    // scope added later cannot land in one and not the other.
    const { a, b, tickB } = twoInstances({ dropControl: true })
    const victim = conn('c1', 1)
    b.register(victim)
    await b.subscribe(victim, ROOM)

    await a.evict('c1')
    await settle()
    assertEquals(victim.closes, [], 'the frame was lost')

    await tickB()
    assertEquals(
        victim.closes,
        [4403],
        'a record with NO channel is a whole-connection revocation, and the ' +
            'reconcile applies it as one',
    )
})

Deno.test('#332 durability failure never cancels the revocation, and is reported after', async () => {
    // `evict`'s sequencing, applied to the new verb: the local apply needs no
    // broker at all, so letting a durability write reject out of the method
    // would skip the one revocation that was still possible — failing OPEN on
    // a revocation path.
    const failure = new Error('broker unreachable')
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.reject(failure),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: () => Promise.resolve(),
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const victim = conn('c1', 1)
    m.register(victim)
    await m.subscribe(victim, ROOM)

    const thrown = await assertRejects(() => m.revokeChannel('c1', ROOM))

    assertEquals(thrown, failure, 'the caller learns durability was lost')
    assertEquals(
        victim.received.filter((f) => f.type === 'unsubscribed').length,
        1,
        'AND the revocation happened anyway — the throw is after the apply, ' +
            'not instead of it',
    )
    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'not-subscribed',
        'the membership really is gone',
    )
})

Deno.test('#332 a failed CLEAR is reported to a caller and swallowed where there is none', async () => {
    // Asymmetric on purpose (FR-019). On `revokeChannel`'s own path there is a
    // caller to receive it. On the reconcile path there is not — the driver's
    // timer invoked it — so re-throwing there would be an unhandled rejection
    // rather than a signal, and would surface as a crash in whatever the
    // driver does with its own tick.
    const failure = new Error('clear failed')
    const live: Revocation[] = []
    let reconcile: (() => unknown) | undefined
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        onRevocationReconcile(handler) {
            reconcile = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: (revocation) => {
            live.push(revocation)
            return Promise.resolve()
        },
        listRevocations: () => Promise.resolve([...live]),
        clearRevocation: () => Promise.reject(failure),
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const direct = conn('c1', 1)
    m.register(direct)
    await m.subscribe(direct, ROOM)

    const thrown = await assertRejects(() => m.revokeChannel('c1', ROOM))
    assertEquals(thrown, failure, 'the DIRECT caller is told')

    // Now the same failure down the path with nobody to tell. The record is
    // still in `live` (the clear failed), a second connection holds the room,
    // and the tick must apply the leave and RESOLVE.
    const viaTick = conn('c9', 9)
    m.register(viaTick)
    await m.subscribe(viaTick, ROOM)
    live.push({ target: 'c9', channel: ROOM, id: crypto.randomUUID() })

    // `assertRejects` would be the wrong tool here — the assertion is that it
    // does NOT reject, and awaiting it is that assertion: an unhandled
    // rejection from `#applyRevocation` would fail this test by escaping.
    await reconcile?.()
    await settle()

    assertEquals(
        viaTick.received.filter((f) => f.type === 'unsubscribed').length,
        1,
        'the reconcile applied the leave despite the clear failing — a ' +
            'revocation is never abandoned because its bookkeeping failed',
    )
    assertEquals(
        await m.unsubscribe('c9', ROOM),
        'not-subscribed',
        'and the membership really is gone',
    )
})

Deno.test('#332 a revoke that found nothing to remove KEEPS its durable record', async () => {
    // THE REVIEW'S HIGH, and it is reachable rather than theoretical.
    //
    // `subscribe` suspends at the application authorizer before `#joinLocal`
    // runs, so there is a real window in which this instance OWNS the socket
    // and the membership has not landed yet. A `revokeChannel` inside it marks
    // the record, gets 'not-subscribed' from the leave, and — if the clear is
    // unconditional — deletes the record it wrote three lines earlier. The
    // authorizer then resolves, the membership lands, and the reconcile has
    // nothing left to find: the client stays in the channel permanently while
    // the operator was told the call was a no-op.
    //
    // `evict` never had this: its records go to the TTL, so its backstop
    // survives an apply that found nothing.
    const live = new Map<string, Revocation>()
    let reconcile: (() => unknown) | undefined
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        onRevocationReconcile(handler) {
            reconcile = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation(revocation) {
            live.set(key(revocation), revocation)
            return Promise.resolve()
        },
        listRevocations: () => Promise.resolve([...live.values()]),
        clearRevocation(revocation) {
            live.delete(key(revocation))
            return Promise.resolve()
        },
    }

    // An authorizer the test can hold open, which is exactly what a DB read is.
    let release: (() => void) | undefined
    const gate = new Promise<void>((resolve) => (release = resolve))
    const m = new ChannelManager<User>({
        driver,
        authorize: async (identity: User | null) => {
            await gate
            return identity ? { id: identity.id } : false
        },
    })

    const victim = conn('c1', 1)
    m.register(victim)
    const joining = m.subscribe(victim, ROOM)
    await settle()

    // The window: owned, not yet a member.
    const outcome = await m.revokeChannel('c1', ROOM)
    assertEquals(outcome, 'not-subscribed', 'nothing was there to remove yet')
    assertEquals(
        live.size,
        1,
        'THE RECORD MUST SURVIVE. Clearing it here throws away the only thing ' +
            'that can catch the membership that is about to land — and the ' +
            'call already reported success',
    )

    release?.()
    await joining
    await settle()

    await reconcile?.()
    await settle()

    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'not-subscribed',
        'the reconcile applied the revocation the operator asked for, so the ' +
            'connection is no longer in the room',
    )
    assertEquals(
        victim.received.filter((f) => f.type === 'unsubscribed').length,
        1,
        'and the client was told, once, when it was actually removed',
    )
    assertEquals(live.size, 0, 'the record is cleared once it has been applied')
})

Deno.test('#332 a revoke-channel frame with NO channel is dropped, not widened', async () => {
    // The guard at the control-frame case. Deleting it hands a channel-less
    // frame to the scope mapping, which reads "no channel" as a WHOLE
    // connection revocation — hard-close 4403 — from a frame that named a room
    // and lost it. Nothing else in the suite fails if that guard goes.
    let deliver: ((control: ControlMessage) => void) | undefined
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: () => Promise.resolve(),
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const victim = conn('c1', 1)
    m.register(victim)
    await m.subscribe(victim, ROOM)
    // Narrowed, not optional-chained: `deliver?.(...)` on an unregistered
    // seam is a no-op that makes every assertion below pass for the wrong
    // reason. The seam being registered IS part of what this test claims.
    assert(deliver !== undefined, 'the control seam was registered')
    const send = deliver as (control: ControlMessage) => void

    // A revocation id IS present, so the only thing missing is the channel —
    // this test pins the channel guard and not #337's id guard beside it.
    send({
        kind: 'revoke-channel',
        target: 'c1',
        revocationId: crypto.randomUUID(),
    })
    await settle()

    assertEquals(
        victim.closes,
        [],
        'a channel-less revoke-channel frame must NOT become a socket kill',
    )
    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'left',
        'and it must not remove the membership either — it is dropped',
    )
})

Deno.test('#332 the reconcile applies a record ONLY to a socket this instance owns', async () => {
    // BOTH DIRECTIONS, because each alone is witnessed by something else.
    //
    // Inverted, the guard makes the owning instance skip the record it is the
    // only one that can apply — enforcement silently stops. Removed entirely,
    // every instance applies every record, and the leave is a no-op on a
    // non-owner but the bookkeeping around it is not. One record of each kind
    // in one index is what pins the guard rather than its consequences.
    const live = new Map<string, Revocation>()
    for (
        const r of [
            { target: 'c1', channel: ROOM, id: crypto.randomUUID() },
            {
                target: 'owned-elsewhere',
                channel: ROOM,
                id: crypto.randomUUID(),
            },
        ]
    ) live.set(key(r), r)

    let reconcile: (() => unknown) | undefined
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        onRevocationReconcile(handler) {
            reconcile = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([...live.values()]),
        clearRevocation(revocation) {
            live.delete(key(revocation))
            return Promise.resolve()
        },
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const owned = conn('c1', 1)
    m.register(owned)
    await m.subscribe(owned, ROOM)

    await reconcile?.()
    await settle()

    assertEquals(
        owned.received.filter((f) => f.type === 'unsubscribed').length,
        1,
        'the record for a socket THIS instance owns is applied — invert the ' +
            'guard and the only instance that can enforce it stops doing so',
    )
    assertEquals(
        [...live.values()].map((r) => `${r.target} ${r.channel}`),
        [`owned-elsewhere ${ROOM}`],
        "the applied record is gone and the other instance's is untouched — " +
            'an instance that cannot act on a record must not consume it',
    )
})
