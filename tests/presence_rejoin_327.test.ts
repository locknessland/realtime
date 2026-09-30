/**
 * @fileoverview #327 — a re-join is not a join, and costs what a read costs.
 *
 * A connection that subscribed to a presence channel it **already held** was
 * charged by neither channel cap — both meter set *growth*, and a re-join grows
 * no set — and still paid a full join: a `joined` frame to every local
 * subscriber, an `EVAL` roster write, and a `presence-join` control publish
 * that `handleControl` re-emits as another `joined` on **every other
 * instance**. One inbound frame, work proportional to the room's cluster-wide
 * population, metered by nothing.
 *
 * **The missing meter was the smaller half.** `joined` is a domain event and
 * must record a transition. Membership is a set, so a re-join transitions
 * nothing — and the manager announced one anyway. Every client maintaining a
 * roster from frames was told a member already in the room had joined it, for
 * the second, hundredth, thousandth time. A budget bounds how often a wrong
 * event is produced; it cannot make it right. So the work is deleted rather
 * than metered, and the re-join becomes what it always was: an authorized
 * roster read.
 *
 * **Most assertions here count what left the manager** — frames subscribers
 * received and calls the driver saw. A return value cannot tell you whether the
 * room was told something false.
 *
 * Two witnesses pin the return value deliberately, and they are the exception
 * that proves the rule: the constraint-2 case and the degraded-read case exist
 * *because* the return value is the thing that must not change. A client
 * re-subscribing after a network blip has to be unable to tell its re-join from
 * a first join, and the return value is the only place it could.
 *
 * The disposition, its rejected alternatives and what it does NOT solve are
 * recorded on the issue; the seat that decided it is `architect-expert`, under
 * hard rule #11.
 *
 * @module @lockness/realtime/tests/presence_rejoin_327
 */

import { assertEquals, assertThrows } from '@std/assert'
import { ChannelManager, ConnectionIdInUseError } from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const CHANNEL = 'presence-room'

/** A connection that records every frame it received. */
function conn(
    id: string,
    userId: number,
): Connection<User> & { readonly received: Record<string, unknown>[] } {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Connection<User> & { readonly received: Record<string, unknown>[] }
}

/**
 * A working driver that COUNTS every call. The counts are the test: "bounded"
 * was the language a budget would have needed, and this issue's answer is
 * exact — zero writes, zero announcements, one read.
 */
function countingDriver() {
    const roster = new Map<string, Map<string, PresenceMember>>()
    const calls = { holdMember: 0, releaseMember: 0, readRoster: 0, control: 0 }
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        holdMember(channel, member) {
            calls.holdMember++
            let members = roster.get(channel)
            if (!members) roster.set(channel, members = new Map())
            const arrived = !members.has(String(member.id))
            members.set(String(member.id), member)
            return Promise.resolve({ arrived })
        },
        releaseMember(channel, memberId) {
            calls.releaseMember++
            return {
                gone: roster.get(channel)?.delete(String(memberId)) ?? false,
            }
        },
        readRoster(channel, limit, selfIds) {
            return asWindow(
                (() => {
                    calls.readRoster++
                    return [...(roster.get(channel)?.values() ?? [])]
                })(),
                limit,
                selfIds,
            )
        },
        onControl: () => {},
        publishControl() {
            calls.control++
            return Promise.resolve()
        },
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    return { driver, roster, calls }
}

/** Authorizes everyone, carrying `info` so the discard case has something to see. */
const authorize = (
    identity: User | null,
    _channel: string,
    info?: Record<string, unknown>,
): PresenceMember | false =>
    identity ? { id: identity.id, info: info ?? { seat: 'original' } } : false

Deno.test('#327 a re-join announces NOTHING and writes NOTHING', async () => {
    const { driver, calls } = countingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(holder, CHANNEL)
    await m.subscribe(observer, CHANNEL)

    // Baseline AFTER both first joins, so the re-join's cost is isolated
    // rather than inferred by subtracting a number nobody measured.
    const before = { ...calls }
    const observerFrames = observer.received.length
    const holderFrames =
        holder.received.filter((f) => f.action === 'joined').length

    await m.subscribe(holder, CHANNEL)
    await m.subscribe(holder, CHANNEL)
    await m.subscribe(holder, CHANNEL)

    assertEquals(
        calls.holdMember - before.holdMember,
        0,
        'three re-joins perform EXACTLY zero roster writes',
    )
    assertEquals(
        calls.control - before.control,
        0,
        'and publish EXACTLY zero control frames — this is the term that ' +
            'reached every other instance, and it is the whole amplification',
    )
    assertEquals(
        observer.received.length - observerFrames,
        0,
        'a subscriber already in the room is told NOTHING: no member joined',
    )
    assertEquals(
        calls.readRoster - before.readRoster,
        3,
        'what remains is one authoritative READ per call — the same read a ' +
            "first join performs, on the caller's own socket",
    )

    // LAST, and outside every count above, because this one JOINS. Asserting
    // that the re-joiner hears nothing about ITSELF would be free — the
    // `except` on `emitPresence` already guarantees it, so that assertion
    // passed before the fix and read as if it guarded something. A third party
    // arriving after the re-joins is a frame the re-joiner MUST still receive:
    // it proves the guard bought its silence by doing nothing, rather than by
    // breaking this connection's delivery.
    const c9 = conn('c9', 9)
    m.register(c9)
    await m.subscribe(c9, CHANNEL)
    assertEquals(
        holder.received.filter((f) => f.action === 'joined').length -
            holderFrames,
        1,
        'and the re-joiner is still routed — silence is not deafness',
    )
})

Deno.test('#327 a re-join still returns the authoritative roster', async () => {
    // Constraint 2, and the reason the work is deleted rather than refused: a
    // client re-subscribing after a network blip is legitimate traffic and
    // must not be able to tell its re-join from a first join.
    const { driver } = countingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const c1 = conn('c1', 1)
    m.register(c1)
    await m.subscribe(c1, CHANNEL)
    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, CHANNEL)

    const again = await m.subscribe(c1, CHANNEL)

    assertEquals(again.ok, true, 'a re-join is never refused')
    // A WHOLE-ROOM assertion, and still correct under #339: the snapshot is
    // bounded at `maxPresenceSnapshotMembers` (default 100) and this two-member
    // room fits it, so nothing is cut. "Never a fragment that does not say so"
    // over the bound is `presence_snapshot_bound_339.test.ts`'s re-join row.
    assertEquals(
        again.here?.members.map((x) => x.id).sort(),
        [1, 2],
        'and it answers with the whole room, since the room fits the bound',
    )
    assertEquals(again.here?.total, 2, 'and says the room is whole')
    assertEquals(
        again.here?.source,
        'authoritative',
        "from the roster, not from this instance's local view",
    )
})

Deno.test('#327 K pipelined subscribe frames produce exactly ONE join', async () => {
    // The window the guard would leave if it checked AFTER `#joinLocal`:
    // `#joinLocal` awaits `#watch`, and `onMessage` is dispatched as
    // `void guard(...)`, so nothing serializes these. All K would read
    // "not a member" and all K would perform a full join.
    //
    // NOT AWAITED INDIVIDUALLY — awaiting each one in turn is the sequential
    // case, which the guard passes trivially and which proves nothing about
    // the race it exists to close.
    const { driver, calls } = countingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(observer, CHANNEL)
    const before = { ...calls }
    const observerFrames = observer.received.length

    const joiner = conn('c1', 1)
    m.register(joiner)
    const K = 8
    const results = await Promise.all(
        Array.from({ length: K }, () => m.subscribe(joiner, CHANNEL)),
    )

    assertEquals(
        results.every((r) => r.ok),
        true,
        'every frame is answered — none is refused',
    )
    assertEquals(
        calls.holdMember - before.holdMember,
        1,
        `${K} pipelined frames perform ONE roster write, not ${K}`,
    )
    assertEquals(
        calls.control - before.control,
        1,
        'and publish ONE control frame, not K — the cluster-wide term',
    )
    assertEquals(
        observer.received.length - observerFrames,
        1,
        'the room is told the member joined exactly once',
    )
})

Deno.test('#327 a re-join DISCARDS its payload rather than broadcasting an update', async () => {
    // A deliberate behaviour change, tested so it is documented rather than
    // discovered. Detecting a changed `info` means deep-equality over
    // unbounded application data (#326) — per-frame cost proportional to what
    // an attacker controls. The domain has no "member updated" event, and
    // `joined` must not be pressed into service as one.
    const { driver, roster, calls } = countingDriver()
    let seatToHandOut = 'original'
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity, channel) =>
            authorize(identity, channel, { seat: seatToHandOut }),
    })
    const holder = conn('c1', 1)
    m.register(holder)
    const observer = conn('c2', 2)
    m.register(observer)
    await m.subscribe(holder, CHANNEL)
    await m.subscribe(observer, CHANNEL)
    const before = { ...calls }
    const observerFrames = observer.received.length

    seatToHandOut = 'rewritten'
    await m.subscribe(holder, CHANNEL)

    assertEquals(
        roster.get(CHANNEL)?.get('1')?.info,
        { seat: 'original' },
        'the authoritative entry the first join wrote still stands',
    )
    assertEquals(
        calls.holdMember - before.holdMember,
        0,
        'and no write was attempted to change it',
    )
    assertEquals(
        observer.received.length - observerFrames,
        0,
        'the room is not told a member "joined" carrying new metadata',
    )
})

Deno.test('#327 a reconnect re-binds the socket, and frames follow the NEW one', async () => {
    // The case the whole disposition exists to protect: a client whose socket
    // dropped re-subscribes to everything it held — and the frames must
    // follow the new socket, never the dead one. Since #370/#363 a subscribe
    // never re-binds an id: the new socket is refused while the dropped one
    // still holds it, and binds by `register` once the old close has torn the
    // dropped one down. Get that wrong and a second socket takes over the
    // first one's channels, or frames keep going to a dead socket.
    //
    // THE RE-JOIN BRANCH STILL RUNS (#370 review): a reconnecting client
    // re-issues its whole channel set, and a blip makes it re-issue it twice.
    // The second subscribe below is a real re-join — `c1` is already a member
    // — and must take the early exit: no roster write, still answered `ok`.
    const { driver, calls } = countingDriver()
    const m = new ChannelManager<User>({ driver, authorize })
    const dropped = conn('c1', 1)
    m.register(dropped)
    await m.subscribe(dropped, CHANNEL)

    const reconnected = conn('c1', 1)
    assertThrows(
        () => m.register(reconnected),
        ConnectionIdInUseError,
        undefined,
        'not while the dropped socket still holds the id',
    )
    await m.disconnect(dropped) // the old socket's close, which releases the
    // hold `dropped` took — so `reconnected`'s own subscribe below is a real
    // first join, not a re-join riding the dropped socket's leftover entry.
    m.register(reconnected)
    const beforeAgain = calls.holdMember
    const again = await m.subscribe(reconnected, CHANNEL)
    assertEquals(again.ok, true, 'the reconnect is answered, not refused')
    assertEquals(
        calls.holdMember,
        beforeAgain + 1,
        // (#403 review MEDIUM) the row below reads "no write on the SECOND
        // subscribe" as proof of the re-join exit — which means nothing
        // unless a write is shown here, on the FIRST one, to compare it
        // against. Without this, a `holdMember` that silently stopped firing
        // for EVERY subscribe (first join included) would leave both counts
        // equal and this test green.
        "the reconnect's own first join DOES take a fresh hold",
    )

    const holdsBefore = calls.holdMember
    const rejoin = await m.subscribe(reconnected, CHANNEL)
    assertEquals(rejoin.ok, true, 'the re-join is answered, not refused')
    assertEquals(
        calls.holdMember,
        holdsBefore,
        'the second subscribe took the re-join exit: no roster write',
    )

    const c2 = conn('c2', 2)
    m.register(c2)
    await m.subscribe(c2, CHANNEL)

    assertEquals(
        reconnected.received.filter((f) => f.action === 'joined').length,
        1,
        'the live socket receives the third party joining',
    )
    assertEquals(
        dropped.received.filter((f) => f.action === 'joined').length,
        0,
        'and the socket it replaced receives nothing',
    )
})

Deno.test('#327 a re-join degrades like a first join when the roster read fails', async () => {
    // `#closingRead` is shared by both exits precisely so a re-join cannot
    // answer differently from a first join — including when it answers badly.
    // Without this the re-join exit was witnessed happy-path only, and the
    // shared implementation's whole point is what happens off it.
    const { driver } = countingDriver()
    const failing: BroadcastDriver = {
        ...driver,
        readRoster: (_channel, limit, selfIds) =>
            asWindow(Promise.reject(new Error('LOADING')), limit, selfIds),
    }
    const m = new ChannelManager<User>({ driver: failing, authorize })
    const holder = conn('c1', 1)
    m.register(holder)
    await m.subscribe(holder, CHANNEL)

    const again = await m.subscribe(holder, CHANNEL)

    assertEquals(again.ok, true, 'a failed READ never fails a committed join')
    assertEquals(
        again.here?.source,
        'local',
        'and it SAYS the answer is this instance only — the same degradation ' +
            'a first join reports, from the same code',
    )
    assertEquals(again.here?.members.map((x) => x.id), [1])
})
