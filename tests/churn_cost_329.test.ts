/**
 * @fileoverview #329 — what one inbound frame costs, and the single home of
 * that number.
 *
 * **This file is the authority.** `docs/realtime.md`'s cost table is *derived*
 * from these totals and says so; a second hand-maintained count anywhere is the
 * defect the plan's decision table forbids. The framework ships no churn meter
 * — verb-rate policy is the application's `onMessage`, keyed on a stable
 * projection of `connection.identity` — so what it owes an operator instead is
 * the arithmetic they need to size their own budget. These are those numbers.
 *
 * ## What it does NOT assert, and why that matters
 *
 * The **atoms** are owned elsewhere and re-asserting them here would be a
 * second executable copy of a number that already has a home:
 *
 * - `channel_watch_295.test.ts` owns the watch pair — it already drives a
 *   literal churn cycle and asserts `['watch:news','unwatch:news','watch:news']`.
 * - `roster_atomicity_323.test.ts` owns "one roster write per transition".
 * - `presence_rejoin_327.test.ts` owns the re-join's zero writes and its
 *   authoritative read.
 *
 * What none of them owns is the **composite per-cycle total**, which is the
 * only quantity an operator sizing a bucket actually needs. That is this file.
 *
 * ## Four measures, not two
 *
 * #329's own acceptance criterion asked for driver calls and control publishes.
 * Two more terms are larger and were counted by nobody:
 *
 * - **Fleet-wide control verifications.** The loopback drop happens BEFORE the
 *   MAC, so every *other* instance pays a parse, an HMAC and a replay-window
 *   admit for a frame it may not even host. It scales with FLEET size, not with
 *   the room's population, and is charged to instances hosting nothing.
 * - **Application authorizer invocations.** The authorizer runs ahead of every
 *   cap, so a *denied* subscribe on an invented `private-*` name buys a full DB
 *   read for one small frame, charged by nothing. It is the term the operator
 *   actually pays for, and it lands on their database rather than on the broker.
 *
 * ## Driver calls, not Redis commands
 *
 * The rig counts calls on the `BroadcastDriver` interface. For
 * `RedisBroadcastDriver` they map one-to-one onto wire commands — `watchChannel`
 * → `SUBSCRIBE`, `unwatchChannel` → `UNSUBSCRIBE`, `holdMember`/`releaseMember` →
 * `EVAL`, `readRoster` → one read `EVAL` (`HGETALL` before #341) — and the
 * first two ride the subscribe connection while the rest ride the command
 * connection. A driver with no roster
 * capability has no `EVAL` and no read; a driver with no control plane has
 * no publishes. The published table states those collapse axes.
 *
 * ## The leave path has non-frame callers, and they are not in this table
 *
 * #332 added three: `revokeChannel`'s local apply, `handleControl`'s
 * `revoke-channel` case, and the reconcile pass. **None of them is a client
 * frame**, so none belongs in a per-frame cost table — but each costs what the
 * `unsubscribe` row below costs, per call. A burst of server-side revocations
 * is therefore priced by that row and by nothing here, which is the honest
 * answer rather than an omission.
 *
 * @module @lockness/realtime/tests/churn_cost_329
 */

import {
    assertEquals,
    assertNotEquals,
    assertStringIncludes,
} from '@std/assert'
import { ChannelManager, MAX_CHANNELS_PER_CONNECTION } from '../manager.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'

interface User {
    id: number
}

/**
 * Every measure the published table carries.
 *
 * **Decomposed by wire operation, not totalled.** A single `driverCommands`
 * bucket makes the table's `SUBSCRIBE` / `EVAL` / `HGETALL` breakdown
 * unfalsifiable: three wrong numbers summing to the right total pass. The
 * published table names each one, so each one is counted.
 */
interface Cost {
    /** `watchChannel` + `unwatchChannel` — `SUBSCRIBE` / `UNSUBSCRIBE`, on the subscribe connection. */
    watchOps: number
    /** `holdMember` + `releaseMember` — the roster `EVAL`s. */
    rosterWrites: number
    /**
     * `readRoster` — one read `EVAL`, bounded to K members plus the callers'
     * own since #341 (an `HGETALL` of the whole room before it).
     */
    rosterReads: number
    /** Control frames handed to the broker by the instance under test. */
    controlPublishes: number
    /** Control frames every OTHER instance had to parse, verify and admit. */
    fleetVerifications: number
    /** Application authorizer invocations — the term that lands on the app's database. */
    authorizerCalls: number
}

const zero = (): Cost => ({
    watchOps: 0,
    rosterWrites: 0,
    rosterReads: 0,
    controlPublishes: 0,
    fleetVerifications: 0,
    authorizerCalls: 0,
})

/** The table's `driver commands` column — the sum the docs publish. */
const driverCommands = (c: Cost): number =>
    c.watchOps + c.rosterWrites + c.rosterReads

function conn(id: string, userId: number): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    } as Connection<User>
}

/**
 * Two instances over one shared roster and one shared control topic — the
 * smallest fleet in which the verification term is observable at all.
 *
 * The control plane routes a frame to the OTHER instance only, which is what
 * the Redis driver does: it drops its own loopback before the MAC check.
 */
function fleet(deny = false, size = 3) {
    const cost = zero()
    const roster = new Map<string, Map<string, PresenceMember>>()
    const controlHandlers: ((control: ControlMessage) => void)[] = []

    const driverFor = (index: number): BroadcastDriver => ({
        publish: () => {},
        onMessage: () => {},
        onControl(handler) {
            controlHandlers[index] = handler
        },
        publishControl(control) {
            // Instance 0 is the one under test; a peer's own publishes are not
            // charged to it. Without this the counters silently conflated the
            // instance's cost with the fleet's.
            if (index === 0) cost.controlPublishes++
            for (let i = 0; i < controlHandlers.length; i++) {
                // The peer pays; the publisher's own loopback is dropped —
                // and it is dropped BEFORE the MAC, which is why the peer's
                // cost is a full parse-verify-admit rather than a no-op.
                if (i === index) continue
                if (index === 0) cost.fleetVerifications++
                controlHandlers[i]?.(control)
            }
        },
        holdMember(channel, member) {
            if (index === 0) cost.rosterWrites++
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            if (index === 0) cost.rosterWrites++
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    if (index === 0) cost.rosterReads++
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
        watchChannel: () => {
            if (index === 0) cost.watchOps++
        },
        unwatchChannel: () => {
            if (index === 0) cost.watchOps++
        },
    })

    const authorize = (identity: User | null): PresenceMember | false => {
        cost.authorizerCalls++
        if (deny || !identity) return false
        return { id: identity.id }
    }

    // The peers are constructed FIRST so their control handlers are registered
    // before instance 0 publishes anything.
    //
    // THE FLEET IS THREE, not two, and that is load-bearing: at a fleet of two
    // the `N-1` column is numerically identical to the publish count, so a rig
    // that simply echoed `controlPublishes` would pass. At three they differ,
    // and the column can fail on its own.
    const peers: ChannelManager<User>[] = []
    for (let i = size - 1; i >= 1; i--) {
        peers.unshift(
            new ChannelManager<User>({ driver: driverFor(i), authorize }),
        )
    }
    const local = new ChannelManager<User>({ driver: driverFor(0), authorize })
    return { local, peer: peers[0], peers, cost, roster, size }
}

/** Read the counters back to zero, so each frame is measured on its own. */
const take = (cost: Cost): Cost => {
    const snapshot = { ...cost }
    Object.assign(cost, zero())
    return snapshot
}

Deno.test('#329 a presence churn PAIR, sole holder — the worst-case cell', async () => {
    const f = fleet()
    const c = conn('c1', 1)
    f.local.register(c)

    assertEquals((await f.local.subscribe(c, 'presence-room')).ok, true)
    const join = take(f.cost)
    await f.local.unsubscribe('c1', 'presence-room')
    const leave = take(f.cost)

    // The join: SUBSCRIBE (0->1 hosting), EVAL (roster add), the closing read
    // — whose reply was the WHOLE ROOM, the term a frame-rate meter cannot
    // bound and why #333 and #341 were filed. #341 bounded its size and left
    // this count unchanged (its SC-004).
    assertEquals(join, {
        watchOps: 1,
        rosterWrites: 1,
        rosterReads: 1,
        controlPublishes: 1,
        fleetVerifications: f.size - 1,
        authorizerCalls: 1,
    })
    // The leave: EVAL (roster remove) + UNSUBSCRIBE (1->0 hosting). No
    // authorizer runs on this half AT ALL — which is the structural reason the
    // shipped `authorize` advice cannot carry a verb budget.
    assertEquals(leave, {
        watchOps: 1,
        rosterWrites: 1,
        rosterReads: 0,
        controlPublishes: 1,
        fleetVerifications: f.size - 1,
        authorizerCalls: 0,
    })

    assertEquals(
        driverCommands(join) + driverCommands(leave),
        5,
        'THE published worst-case pair. #329 said "two broker writes and two ' +
            'cluster-wide fan-outs" — it missed the watch pair and the closing ' +
            'read, so it under-counted by more than half',
    )
    assertEquals(
        join.fleetVerifications + leave.fleetVerifications,
        2 * (f.size - 1),
        'and the fan-out term is N-1 per publish, charged to instances that ' +
            'may host nothing — measured at a fleet of THREE, where it is not ' +
            'numerically identical to the publish count',
    )
})

Deno.test('#329 an ALREADY-HOSTED presence channel is the low end of the range', async () => {
    // The published table gives ranges, and five of its eight rows would have
    // had an unasserted low end. The low end is "another connection already
    // holds this channel", so no watch op is issued — and a budget cannot tell
    // which case it is in, which is exactly why the docs say to size on the
    // worst.
    const f = fleet()
    const c0 = conn('c0', 99)
    f.local.register(c0)
    await f.local.subscribe(c0, 'presence-room')
    take(f.cost)

    const c1 = conn('c1', 1)
    f.local.register(c1)
    assertEquals(
        (await f.local.subscribe(c1, 'presence-room')).ok,
        true,
    )
    const join = take(f.cost)
    await f.local.unsubscribe('c1', 'presence-room')
    const leave = take(f.cost)

    assertEquals(join.watchOps, 0, 'no SUBSCRIBE — the channel was hosted')
    assertEquals(driverCommands(join), 2, 'roster EVAL + roster read only')
    assertEquals(leave.watchOps, 0, 'and no UNSUBSCRIBE — a member remains')
    assertEquals(driverCommands(leave), 1, 'EVAL only')
})

Deno.test('#329 a presence re-join is one authoritative READ, bounded since #341', async () => {
    const f = fleet()
    const c = conn('c1', 1)
    f.local.register(c)
    await f.local.subscribe(c, 'presence-room')
    take(f.cost)

    const rosterReadsBefore = rosterReadCount()
    const again = await f.local.subscribe(c, 'presence-room')
    assertRosterRead(rosterReadsBefore)

    assertEquals(again.ok, true, 'a re-join is never refused')
    assertEquals(
        take(f.cost),
        {
            watchOps: 0,
            rosterWrites: 0,
            rosterReads: 1,
            controlPublishes: 0,
            fleetVerifications: 0,
            authorizerCalls: 1,
        },
        '#327 made this write nothing, announce nothing and publish nothing — ' +
            'and it still costs one roster read (bounded to K members plus ' +
            'its own since #341), plus one authorizer call. "Free" was never what the code ' +
            'supported, and three shipped documents implied it',
    )
})

Deno.test('#329 a PRIVATE channel isolates the authorizer from the roster work', async () => {
    const f = fleet()
    const c = conn('c1', 1)
    f.local.register(c)

    assertEquals((await f.local.subscribe(c, 'private-orders')).ok, true)
    const join = take(f.cost)
    await f.local.unsubscribe('c1', 'private-orders')
    const leave = take(f.cost)

    assertEquals(join, {
        watchOps: 1, // SUBSCRIBE only — a private channel has no roster
        rosterWrites: 0,
        rosterReads: 0,
        controlPublishes: 0,
        fleetVerifications: 0,
        authorizerCalls: 1,
    })
    assertEquals(leave, {
        watchOps: 1, // UNSUBSCRIBE
        rosterWrites: 0,
        rosterReads: 0,
        controlPublishes: 0,
        fleetVerifications: 0,
        authorizerCalls: 0,
    })
})

Deno.test('#329 a DENIED subscribe costs an authorizer call and nothing else — ahead of every cap', async () => {
    // The cheapest charge in the package that lands on the APPLICATION's
    // database rather than on the broker. `#checkChannelCaps` is downstream of
    // the authorizer, so no cap sees this, and the channel need not exist —
    // which makes it a name-enumeration oracle as well as an amplifier.
    const f = fleet(true)

    const c1 = conn('c1', 1)
    f.local.register(c1)
    const result = await f.local.subscribe(c1, 'private-anything')

    assertEquals(result.ok, false)
    assertEquals(take(f.cost), { ...zero(), authorizerCalls: 1 })
})

Deno.test('#329 a PUBLIC churn pair runs NO authorizer — the path the shipped advice cannot see', async () => {
    const f = fleet()
    const c = conn('c1', 1)
    f.local.register(c)

    assertEquals((await f.local.subscribe(c, 'news')).ok, true)
    const join = take(f.cost)
    await f.local.unsubscribe('c1', 'news')
    const leave = take(f.cost)

    assertEquals(join, { ...zero(), watchOps: 1 })
    assertEquals(leave, { ...zero(), watchOps: 1 })
    // Two frames, two broker commands on the SHARED subscribe connection, no
    // authorizer anywhere in the path, no identity required, and no cap
    // charged — the set returns to rest. On unique names this runs forever.
})

Deno.test('#329 an unsubscribe this instance does not own costs ZERO', async () => {
    // The ordinary double-unsubscribe, and the cell #332's `revokeChannel`
    // will add a caller to. `#leaveLocal` returns early and the presence
    // branch is gated on the member lookup.
    const f = fleet()
    const c1 = conn('c1', 1)
    f.local.register(c1)
    await f.local.subscribe(c1, 'presence-room')
    take(f.cost)

    await f.local.unsubscribe('c1', 'presence-room')
    take(f.cost)
    await f.local.unsubscribe('c1', 'presence-room')

    assertEquals(take(f.cost), zero(), 'a second leave is free')

    await f.peer.unsubscribe('c1', 'presence-room')
    assertEquals(
        take(f.cost),
        zero(),
        'and so is a leave on an instance that never owned the connection',
    )
})

Deno.test('#329 `maxChannelsPerConnection` reports the EFFECTIVE cap, not the default', () => {
    // The branch's only new production API, and the whole reason it exists.
    // Returning MAX_CHANNELS_PER_CONNECTION here would pass every other test
    // in the package — and `docs/realtime.md` tells operators to size their
    // token bucket on this value, so being wrong here refuses the reconnect of
    // a client holding a set the framework itself permitted.
    const f = fleet()
    assertEquals(
        f.local.maxChannelsPerConnection,
        MAX_CHANNELS_PER_CONNECTION,
        'an unconfigured manager reports the default',
    )

    const configured = new ChannelManager<User>({ maxChannelsPerConnection: 7 })
    assertEquals(
        configured.maxChannelsPerConnection,
        7,
        'a CONFIGURED manager reports what it was configured with — this is ' +
            'the distinction the getter exists for, and the one a default ' +
            'constant cannot express',
    )
    assertNotEquals(
        configured.maxChannelsPerConnection,
        MAX_CHANNELS_PER_CONNECTION,
        'and the two are genuinely different here, so the assertion above ' +
            'cannot pass by coincidence',
    )
})

Deno.test('#329 the decision that there is no framework meter is ANCHORED, not merely written', async () => {
    // Deleting the docstring must fail something. Without this the home in the
    // plan's decision table is enforced by nothing: every gate stays green
    // while the reason a maintainer needs is removed. Precedent:
    // `log_encoding_291.test.ts` pins a source marker the same way.
    const source = await Deno.readTextFile(
        new URL('../manager.ts', import.meta.url),
    )
    const start = source.indexOf('    handlerHooks(')
    const docStart = source.lastIndexOf('    /**', start)
    assertStringIncludes(
        source.slice(docStart, start),
        "VERB RATE IS THE APPLICATION'S",
        'the `handlerHooks` docstring must keep the marker naming the ' +
            'pass-through as a DECISION — it is the single home of "the ' +
            'framework does not meter the verb rate", and prose with no anchor ' +
            'is prose that rots',
    )

    // AND THE CODE THE DOCSTRING DESCRIBES — pinned by BEHAVIOUR, not by a
    // second copy of manager.ts's comment (#401): a meter added later would
    // silently drop some calls from a socket that OWNS its id, which this
    // loop catches whether or not it changes a single line of prose. `onOpen`
    // and `onClose` are composed and this one deliberately is not — composing
    // a budget in is how a churn meter arrives in the framework by the back
    // door, and it would charge the five non-client paths into `unsubscribe`.
    //
    // What this loop does NOT prove (#417): a counting-only meter that lets
    // every call through while it accumulates state. That case is left to
    // review on purpose. A private `#count++` is invisible to reflection, and
    // a heap-growth probe would couple the gate to the GC. Spying on a meter
    // seam would require the very seam this decision forbids. A syntactic pin
    // on the wrapper is what #401 removed.
    const f = fleet()
    const c = conn('c1', 1)
    f.local.register(c)

    let seen = 0
    const hooks = f.local.handlerHooks({ onMessage: () => void seen++ })
    const CALLS = 500
    for (let i = 0; i < CALLS; i++) {
        await hooks.onMessage?.(c, 'frame')
    }
    assertEquals(
        seen,
        CALLS,
        'every call must reach the app UNMETERED — since #363 the only ' +
            'wrapper on `onMessage` is the ownership gate, and this connection ' +
            'owns its id for the whole loop, so nothing may drop a call',
    )
})
