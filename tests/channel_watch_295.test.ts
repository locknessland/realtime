/**
 * @fileoverview #295 — the membership funnel and the per-channel watch seam.
 *
 * `ChannelManager` now declares to the driver which channels this instance
 * hosts, so the broker sends it those and nothing else. The whole failure class
 * here is SILENT: a channel hosted but never watched drops every message while
 * `subscribe` answers `{ ok: true }`, and a channel unwatched while a live
 * subscriber holds it goes deaf permanently, because the reconnect that heals
 * every other deafness is guaranteed not to re-issue it.
 *
 * @module @lockness/realtime/tests/channel_watch_295
 */

import {
    assert,
    assertEquals,
    assertRejects,
    assertStringIncludes,
    assertThrows,
} from '@std/assert'
import {
    ChannelLimitError,
    ChannelManager,
    MAX_CHANNELS_PER_CONNECTION,
    MAX_WATCHED_CHANNELS,
} from '../manager.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection } from '../types.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { recordingPorts } from './recording_ports.ts'

/** A driver that records every watch/unwatch, in order. */
function watchingDriver(options: { failWatch?: boolean } = {}): {
    driver: BroadcastDriver
    ops: string[]
} {
    const ops: string[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        watchChannel: (channel) => {
            ops.push(`watch:${channel}`)
            return options.failWatch
                ? Promise.reject(new Error('broker write failed'))
                : Promise.resolve()
        },
        unwatchChannel: (channel) => {
            ops.push(`unwatch:${channel}`)
            return Promise.resolve()
        },
    }
    return { driver, ops }
}

/** A driver offering `watchChannel` and NOT `unwatchChannel` (D-5). */
function halfCapableDriver(): { driver: BroadcastDriver; ops: string[] } {
    const ops: string[] = []
    return {
        driver: {
            publish: () => {},
            onMessage: () => {},
            watchChannel: (channel) => void ops.push(`watch:${channel}`),
        },
        ops,
    }
}

/**
 * An IDENTIFIED connection.
 *
 * `conn` above is anonymous (`identity: null`), which is what every #295 test
 * used and what #322's reservation now bounds: an anonymous caller may only
 * grow the hosted set while it is below `maxWatchedChannels ×
 * anonymousHostingShare`. A test that means to reach the FULL instance cap must
 * use this one, or it measures the reservation and reports it as the cap.
 */
function identified(id: string): Connection<{ sub: string }> {
    return {
        id,
        identity: { sub: id },
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<{ sub: string }>
}

function conn(id: string): Connection<null> {
    return {
        id,
        identity: null,
        metadata: {},
        send: () => {},
        close: () => {},
    } as unknown as Connection<null>
}

Deno.test('#295/FR-003: the watch fires on 0→1 and the unwatch on 1→0, and at no other time', async () => {
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    await m.subscribe(conn('a'), 'news')
    await m.subscribe(conn('b'), 'news')
    await m.subscribe(conn('c'), 'sport')
    assertEquals(
        ops,
        ['watch:news', 'watch:sport'],
        'two clients on one channel produce ONE watch',
    )
    await m.unsubscribe('a', 'news')
    assertEquals(ops.length, 2, 'a non-last leaver produces no wire op')
    await m.unsubscribe('b', 'news')
    assertEquals(
        ops,
        ['watch:news', 'watch:sport', 'unwatch:news'],
        'the LAST leaver unwatches, exactly once',
    )
})

Deno.test('#295/FR-011: an emptied channel leaves no entry behind, so re-joining watches again', async () => {
    // "Not hosted" must have ONE spelling. An empty `Set` left in the map is a
    // second one, it grows without bound, and a re-join against it reads as
    // already-hosted so the wire op never fires.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    await m.subscribe(conn('a'), 'news')
    await m.unsubscribe('a', 'news')
    await m.subscribe(conn('b'), 'news')
    assertEquals(ops, ['watch:news', 'unwatch:news', 'watch:news'])
})

Deno.test("#295/FR-012: disconnect unwatches THIS connection's channels, not every channel hosted", async () => {
    // `disconnect` used to iterate every channel this instance had ever hosted
    // and call `unsubscribe` for each. That was harmless only while a 1→0
    // transition had no wire op — it acquired one here. It was also O(channels
    // under the prefix) per disconnect.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    await m.subscribe(conn('a'), 'alpha')
    await m.subscribe(conn('b'), 'beta')
    await m.subscribe(conn('b'), 'gamma')
    ops.length = 0
    await m.disconnect('b')
    assertEquals(
        ops.sort(),
        ['unwatch:beta', 'unwatch:gamma'],
        'alpha still has a live subscriber and must NOT be unwatched',
    )
})

Deno.test('#295/FR-002: a refused watch keeps the membership and warns', async () => {
    // A rejection means the frame did not reach the socket. It does NOT mean
    // the channel is unhosted: the driver re-issues from its own recorded set,
    // so dropping the membership here would turn a transient write failure into
    // permanent local deafness.
    const { driver } = watchingDriver({ failWatch: true })
    const m = new ChannelManager({ driver })
    const warnings: string[] = []
    const realWarn = console.warn
    console.warn = (...args: unknown[]) => {
        warnings.push(args.map(String).join(' '))
    }
    try {
        const result = await m.subscribe(conn('a'), 'news')
        assertEquals(
            result,
            { ok: true },
            "the join succeeded; delivery resumes on the driver's own retry",
        )
    } finally {
        console.warn = realWarn
    }
    assert(
        warnings.some((w) => w.includes('could not subscribe to')),
        'the refusal is reported, never silent',
    )
    // Still hosted, which is the half that matters: the leave must still find
    // a membership to remove.
    await m.unsubscribe('a', 'news')
})

Deno.test("#295/FR-025: a driver with watch and NO unwatch keeps today's behaviour", async () => {
    // Detected as a SET. Watching without unwatching makes the subscribed set
    // monotonic over the process lifetime — one permanent subscription per
    // channel ever hosted, strictly worse than the prefix-wide subscription it
    // replaces, and invisible because delivery stays correct.
    const { driver, ops } = halfCapableDriver()
    const m = new ChannelManager({ driver })
    await m.subscribe(conn('a'), 'news')
    await m.unsubscribe('a', 'news')
    assertEquals(ops, [], 'neither op fires: the pair is all-or-nothing')
})

Deno.test('#295: a driver with neither op is untouched — the memory driver stays single-glob', async () => {
    const plain: BroadcastDriver = { publish: () => {}, onMessage: () => {} }
    const m = new ChannelManager({ driver: plain })
    const r = await m.subscribe(conn('a'), 'news')
    assertEquals(r, { ok: true })
    await m.disconnect('a')
})

Deno.test('#322/SC-005: the per-connection cap REFUSES, and mutates nothing', async () => {
    // #295 shipped this cap inert — a `console.warn` naming the count, then the
    // subscribe admitted. #322 makes it refuse. The assertion that matters is
    // not that it throws: it is that NOTHING moved. A partial join would be
    // worse than either outcome, because the connection would hold a channel
    // the instance never watched and every message on it would be dropped in
    // silence.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    const c = conn('greedy')
    for (let i = 0; i < MAX_CHANNELS_PER_CONNECTION; i++) {
        assertEquals(await m.subscribe(c, `ch${i}`), { ok: true })
    }
    const watchesBefore = [...ops]
    const connectionsBefore = m.connectionCount

    const error = await assertRejects(
        () => m.subscribe(c, 'one-too-many'),
        ChannelLimitError,
    )
    assertEquals(error.scope, 'connection')
    assertEquals(error.count, MAX_CHANNELS_PER_CONNECTION)
    assertEquals(error.limit, MAX_CHANNELS_PER_CONNECTION)

    assertEquals(
        ops,
        watchesBefore,
        'the refused subscribe still told the broker to watch the channel — ' +
            'the instance now holds a subscription no connection is on',
    )
    assertEquals(
        m.connectionCount,
        connectionsBefore,
        'the refused subscribe still registered the connection',
    )
    // And the channel is genuinely not held. `subscribe` answering `{ ok: true }`
    // after a leave does NOT show that: a phantom row for 'one-too-many' in the
    // reverse index would make the cap check treat the join as growing nothing
    // and admit it — the assertion would pass BECAUSE of the defect it is meant
    // to catch. What proves it is the broker: a channel genuinely unheld gets a
    // watch when it is finally joined.
    await m.unsubscribe(c.id, 'ch0')
    assertEquals(await m.subscribe(c, 'one-too-many'), { ok: true })
    assertEquals(
        ops.filter((op) => op === 'watch:one-too-many'),
        ['watch:one-too-many'],
        'the channel was admitted without a broker watch, so the refused ' +
            'subscribe had left the connection holding it',
    )
})

Deno.test('#295/FR-017: a join that grows NO set is not charged against a cap', async () => {
    // A second client on a hosted channel adds no broker subscription, and a
    // client re-joining a channel it already holds adds nothing either.
    // Charging for those refuses work that costs the broker nothing — and it is
    // the easy mistake, because `subscribe` is called either way.
    //
    // The assertion USED to be that no `console.warn` mentioned "watched
    // channels". Once #322 removed both warns that filter is empty for every
    // input, so it passed for a reason unrelated to the exemption — a
    // tautology reading as a guard. It asserts the refusal directly now.
    const { driver } = watchingDriver()
    const m = new ChannelManager({ driver })
    const c = conn('a')
    for (let i = 0; i < MAX_CHANNELS_PER_CONNECTION; i++) {
        await m.subscribe(c, `ch${i}`)
    }
    // AT the limit, not past it. Re-joining a channel already held grows
    // nothing, so it must not be charged.
    assertEquals(await m.subscribe(c, 'ch0'), { ok: true })
    // And a SECOND connection joining an already-hosted channel adds no
    // subscription to the instance's set.
    assertEquals(await m.subscribe(conn('b'), 'ch0'), { ok: true })
})

Deno.test('#295/FR-019: the delivered TOPIC decides the channel, never the watched one', async () => {
    // The shape an implementer reaches for, because `channel` is right there in
    // scope: `watchChannel(ch)` closing over `ch` and handing it to the
    // handler. It looks correct. It also makes `onMessage`'s deny-by-default
    // `topic.startsWith(marker)` check and its fixed-offset slice dead code,
    // and a later tidy-up removes dead code with a clean conscience.
    //
    // The witness feeds the recorded handler a topic for a DIFFERENT channel
    // than the one watched. A closure-carried channel answers `alpha`; the
    // topic-derived one answers `beta`, which is what the broker actually said.
    const { command, subscriber, recording } = recordingPorts({})
    const driver = new RedisBroadcastDriver(command, subscriber, {
        prefix: 'app',
    })
    const got: string[] = []
    try {
        driver.onMessage((m) => got.push(m.channel))
        await driver.watchChannel('alpha')
        const sub = recording.subscriptions.find((s) =>
            s.pattern === 'app__event:alpha'
        )!
        assert(sub !== undefined, 'the watch reached the subscriber')
        sub.handler(
            'app__event:beta',
            JSON.stringify({ event: 'e', data: null }),
        )
        assertEquals(
            got,
            ['beta'],
            'the message was attributed to the WATCHED channel rather than the ' +
                'delivered topic — a closure is carrying the channel into ' +
                'delivery, and the topic check above it is now dead code',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#295/FR-019: a topic outside the marker is dropped, not renamed', async () => {
    // The other half, and the reason the first one matters. Deny by default:
    // a topic the subscription could not have produced must reach nothing, not
    // become a plausible channel name nobody chose.
    const { command, subscriber, recording } = recordingPorts({})
    const driver = new RedisBroadcastDriver(command, subscriber, {
        prefix: 'app',
    })
    const got: string[] = []
    const warnings: string[] = []
    const realWarn = console.warn
    console.warn = (...args: unknown[]) => {
        warnings.push(args.map(String).join(' '))
    }
    try {
        driver.onMessage((m) => got.push(m.channel))
        await driver.watchChannel('alpha')
        const sub = recording.subscriptions.find((s) =>
            s.pattern === 'app__event:alpha'
        )!
        sub.handler('app__control', JSON.stringify({ event: 'e', data: null }))
        assertEquals(
            got,
            [],
            'a topic outside the event marker reached fan-out',
        )
    } finally {
        console.warn = realWarn
        await driver.close()
    }
    assert(
        warnings.some((w) => w.includes('event marker')),
        'the drop was silent',
    )
})

Deno.test('#322/SC-004: the INSTANCE cap is a distinct branch, and it refuses on its own', async () => {
    // Driven with one IDENTIFIED connection per channel: one per channel keeps
    // the per-connection cap well clear so only the instance branch can fire,
    // and identified keeps #322's anonymous reservation clear so the branch
    // fires at the FULL cap rather than at 80% of it. An anonymous run here
    // would measure the reservation and report it as the cap.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    for (let i = 0; i < MAX_WATCHED_CHANNELS; i++) {
        assertEquals(await m.subscribe(identified(`c${i}`), `ch${i}`), {
            ok: true,
        })
    }
    const watchesBefore = ops.length

    const error = await assertRejects(
        () => m.subscribe(identified('last'), 'one-too-many'),
        ChannelLimitError,
    )
    assertEquals(
        error.scope,
        'instance',
        'the per-connection branch fired on a run that gives each channel its ' +
            'own connection — the two branches are not distinguishable',
    )
    assertEquals(error.count, MAX_WATCHED_CHANNELS)
    assertEquals(error.limit, MAX_WATCHED_CHANNELS)
    assertEquals(ops.length, watchesBefore, 'the refused subscribe watched')
})

Deno.test('#322/SC-006: at the INSTANCE cap, a join that grows no set is still admitted', async () => {
    // SC-006's other half. #295's exemption witness only ever reached the
    // per-connection cap — it loops 100 channels on one connection, so
    // `subscriptions.size` never approached the instance cap. The exemption
    // that matters most is the one at the instance cap, because that is where
    // refusing a free join denies an entire deployment.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager({ driver })
    for (let i = 0; i < MAX_WATCHED_CHANNELS; i++) {
        await m.subscribe(identified(`c${i}`), `ch${i}`)
    }
    const watchesBefore = ops.length
    // A brand-new connection on an ALREADY-hosted channel: the instance is at
    // its cap and this join costs the broker nothing.
    assertEquals(await m.subscribe(identified('newcomer'), 'ch0'), { ok: true })
    // An anonymous one too — the reservation bounds 0 -> 1 transitions only.
    assertEquals(await m.subscribe(conn('anon'), 'ch0'), { ok: true })
    assertEquals(
        ops.length,
        watchesBefore,
        'a join that grows no set reached the broker',
    )
})

Deno.test('#322/SC-013: anonymous callers cannot spend the reserved share', async () => {
    // The security seat's HIGH finding. `subscribe` runs no authorizer for a
    // public channel, so an anonymous socket can drive the hosted set on its
    // own. While the cap only WARNed that was unbounded growth; making it
    // refuse turns it into a DENIAL of all new hosting, for every connection on
    // the instance including authenticated ones. The reservation is what keeps
    // an identified caller's join reachable.
    const { driver } = watchingDriver()
    // Both caps, because a per-connection cap left at its 100 default above a
    // 10-channel instance is refused at construction — one connection could
    // otherwise take the whole instance.
    const m = new ChannelManager({
        driver,
        maxWatchedChannels: 10,
        maxChannelsPerConnection: 10,
    })
    // 0.8 x 10 = 8.
    for (let i = 0; i < 8; i++) {
        assertEquals(await m.subscribe(conn(`anon${i}`), `ch${i}`), {
            ok: true,
        })
    }
    const error = await assertRejects(
        () => m.subscribe(conn('anon-extra'), 'ch-new'),
        ChannelLimitError,
    )
    assertEquals(
        error.scope,
        'instance-anonymous',
        'the reservation must be distinguishable from a full instance — an ' +
            'operator responds to the two differently',
    )
    assertEquals(error.limit, 8, 'the reserved ceiling, not the instance cap')
    // The identified caller still gets through, which is the entire point.
    assertEquals(await m.subscribe(identified('member'), 'ch-new'), {
        ok: true,
    })
    // And the anonymous caller may still JOIN what is already hosted.
    assertEquals(await m.subscribe(conn('anon-extra'), 'ch-new'), { ok: true })
})

Deno.test('#322/SC-014: anonymousHostingShare of 1 disables the reservation', async () => {
    // A deployment that authenticates nobody must not be degraded to 80% of
    // its own cap. It sets the share to 1 and carries the original exposure
    // knowingly — which is the same posture the docs already described, now
    // with a dial rather than a paragraph.
    const { driver } = watchingDriver()
    const m = new ChannelManager({
        driver,
        maxWatchedChannels: 10,
        maxChannelsPerConnection: 10,
        anonymousHostingShare: 1,
    })
    for (let i = 0; i < 10; i++) {
        assertEquals(await m.subscribe(conn(`anon${i}`), `ch${i}`), {
            ok: true,
        })
    }
    const error = await assertRejects(
        () => m.subscribe(conn('anon-extra'), 'ch-new'),
        ChannelLimitError,
    )
    assertEquals(error.scope, 'instance', 'with no reservation it is the cap')
    assertEquals(error.limit, 10)
})

Deno.test('#322/SC-007: a raised cap admits what the default refuses', async () => {
    const { driver } = watchingDriver()
    const m = new ChannelManager({
        driver,
        maxChannelsPerConnection: MAX_CHANNELS_PER_CONNECTION + 5,
    })
    const c = identified('roomy')
    for (let i = 0; i < MAX_CHANNELS_PER_CONNECTION + 5; i++) {
        assertEquals(await m.subscribe(c, `ch${i}`), { ok: true })
    }
    // Past the RAISED value it still refuses — a movable cap is not an absent
    // one.
    const error = await assertRejects(
        () => m.subscribe(c, 'past-the-raised-one'),
        ChannelLimitError,
    )
    assertEquals(error.limit, MAX_CHANNELS_PER_CONNECTION + 5)
})

Deno.test('#322/SC-008: an unusable cap is refused at CONSTRUCTION', () => {
    // At construction, not at the thousandth subscribe. A cap discovered when
    // it first bites is a misconfiguration discovered in production.
    const { driver } = watchingDriver()
    for (const bad of [0, -1, 1.5, NaN, Infinity, '500' as unknown as number]) {
        assertThrows(
            () => new ChannelManager({ driver, maxWatchedChannels: bad }),
            Error,
            'maxWatchedChannels',
            `${JSON.stringify(bad)} was accepted as a cap`,
        )
    }
    // 1.0 is an integer in JS and must be accepted — there is no integral-float
    // case to guard against.
    new ChannelManager({
        driver,
        maxWatchedChannels: 1.0,
        maxChannelsPerConnection: 1,
        // The share has to go, or 1 x 0.8 floors to a ceiling of 0 and the
        // reservation guard fires first — see the test below, which is where
        // that case belongs.
        anonymousHostingShare: 1,
    })
    // The per-connection cap has its OWN assertion, and it needs one: with only
    // the instance cap driven, deleting `assertCap('maxChannelsPerConnection')`
    // goes unnoticed.
    for (const bad of [0, -1, 1.5, NaN]) {
        assertThrows(
            () => new ChannelManager({ driver, maxChannelsPerConnection: bad }),
            Error,
            'maxChannelsPerConnection',
            `${JSON.stringify(bad)} was accepted as a per-connection cap`,
        )
    }
    // A per-connection cap ABOVE the instance cap lets ONE connection consume
    // the whole instance budget, which is the security finding without needing
    // ten sockets.
    assertThrows(
        () =>
            new ChannelManager({
                driver,
                maxWatchedChannels: 10,
                maxChannelsPerConnection: 11,
            }),
        Error,
        'exceeds',
    )
})

Deno.test('#322/SC-015: an unusable anonymousHostingShare is refused too', () => {
    // Its own predicate: a fraction, not a count. `Number.isInteger` would
    // refuse every legitimate value.
    const { driver } = watchingDriver()
    for (const bad of [0, -0.1, 1.1, NaN, '0.8' as unknown as number]) {
        assertThrows(
            () => new ChannelManager({ driver, anonymousHostingShare: bad }),
            Error,
            'anonymousHostingShare',
            `${JSON.stringify(bad)} was accepted as a share`,
        )
    }
    new ChannelManager({ driver, anonymousHostingShare: 1 })
    new ChannelManager({ driver, anonymousHostingShare: 0.5 })
})

Deno.test('#322/SC-010: ChannelLimitError carries the numbers OFF its message', async () => {
    // #295 exported this type unraisable so an application could catch it
    // before it could be thrown. #322 raises it — and takes the numbers out of
    // the message, which is the security seat's finding: `subscribe` runs no
    // authorizer for a public channel, and most applications wire `onError` to
    // a close-frame reason, so an interpolated instance-wide count is a live
    // load signal handed to an unauthenticated caller.
    const error = new ChannelLimitError('connection', 100, 100)
    assert(error instanceof Error, 'it must be catchable as an Error')
    assert(
        error instanceof ChannelLimitError,
        'and narrowable to itself — an application catches on this',
    )
    assertEquals(error.name, 'ChannelLimitError')
    assertEquals(error.scope, 'connection')
    // The numbers stay READABLE — server-side logging is what should have them.
    assertEquals(error.count, 100)
    assertEquals(error.limit, 100)
    assert(
        !/\d/.test(error.message),
        `the message leaks a number: ${error.message}`,
    )
    assertStringIncludes(
        error.message,
        'reconnect',
        'the message must say WHY the set is bounded — each channel is a ' +
            'subscription re-issued on every reconnect — or an operator ' +
            'reading it just raises the limit',
    )
    // A scope this version does not know renders generically rather than
    // producing "this undefined is at its limit". `scope` is an OPEN set.
    assertStringIncludes(
        new ChannelLimitError('shard', 1, 1).message,
        'shard',
    )
    // Exported from the package root, not only from the module.
    const mod = await import('../mod.ts')
    assertEquals(
        (mod as { ChannelLimitError?: unknown }).ChannelLimitError,
        ChannelLimitError,
        'an application imports from the package root',
    )
    assertEquals(
        (mod as { CHANNEL_LIMIT_SCOPES?: readonly string[] })
            .CHANNEL_LIMIT_SCOPES,
        ['instance', 'connection', 'instance-anonymous'],
    )
})

Deno.test('#322/FR-007: the cap is checked AFTER authorization, never before', async () => {
    // The review gate's one HIGH, and the plan asked for exactly this: FR-007
    // says the ordering is "asserted rather than assumed". Every other cap
    // witness subscribes a PUBLIC channel on a manager with no authorizer, so
    // all of them are green whichever side of the authorize block the check
    // sits on.
    //
    // Move `#checkChannelCaps` above `if (kind !== 'public')` and a caller the
    // application DENIED gets `ChannelLimitError` instead of `{ ok: false }` —
    // which hands someone who just failed authorization a live capacity oracle
    // for the instance, and makes a policy decision indistinguishable from
    // resource exhaustion in their client code.
    const { driver, ops } = watchingDriver()
    const m = new ChannelManager<{ sub: string }>({
        driver,
        maxWatchedChannels: 4,
        maxChannelsPerConnection: 4,
        anonymousHostingShare: 1,
        authorize: (_identity, channel) => channel !== 'private-denied',
    })
    // Fill the instance to its cap with channels the authorizer allows.
    for (let i = 0; i < 4; i++) {
        assertEquals(await m.subscribe(identified(`c${i}`), `private-ok${i}`), {
            ok: true,
        })
    }
    const watchesBefore = ops.length

    // A DENIED subscribe on a full instance. Authorization runs first, so this
    // is `{ ok: false }` — not a throw, and not a hint that the instance is
    // full.
    assertEquals(
        await m.subscribe(identified('denied'), 'private-denied'),
        { ok: false },
        'a denied subscribe on a full instance leaked the cap breach — the ' +
            'check has moved above the authorizer',
    )
    // An ALLOWED subscribe on the same full instance still refuses, so the
    // assertion above is not passing merely because the cap is unreachable.
    await assertRejects(
        () => m.subscribe(identified('allowed'), 'private-new'),
        ChannelLimitError,
        undefined,
        'positive control: the instance really is at its cap',
    )
    assertEquals(ops.length, watchesBefore, 'a refused subscribe watched')
})

Deno.test('#322/SC-004: a presence cap breach leaves the roster untouched', async () => {
    // Presence is the branch with the most state to corrupt: a member map, a
    // driver roster and a control publish, all after the cap check. No cap
    // witness reached it, so a check that moved below the roster write would
    // leave a member in a channel the instance never watched.
    const { driver, ops } = watchingDriver()
    const joins: string[] = []
    const m = new ChannelManager<{ sub: string }>({
        driver: {
            ...driver,
            addMember: (channel: string) => {
                joins.push(channel)
                return Promise.resolve()
            },
            listMembers: () => Promise.resolve([]),
            removeMember: () => Promise.resolve(),
        } as unknown as BroadcastDriver,
        maxWatchedChannels: 2,
        maxChannelsPerConnection: 2,
        anonymousHostingShare: 1,
        authorize: () => true,
    })
    for (let i = 0; i < 2; i++) {
        await m.subscribe(identified(`p${i}`), `presence-room${i}`)
    }
    const watchesBefore = ops.length
    const joinsBefore = [...joins]

    await assertRejects(
        () => m.subscribe(identified('late'), 'presence-late'),
        ChannelLimitError,
    )
    assertEquals(joins, joinsBefore, 'the refused join reached the roster')
    assertEquals(ops.length, watchesBefore, 'the refused join watched')
})

Deno.test('#322: a reservation that floors to zero is refused at construction', () => {
    // Two individually valid options that combine into "no anonymous
    // connection may ever host a channel". Fail-closed, so nothing leaks — but
    // a deployment meaning to reserve a fifth of its budget and instead
    // disabling anonymous hosting entirely should hear it at construction.
    const { driver } = watchingDriver()
    assertThrows(
        () =>
            new ChannelManager({
                driver,
                maxWatchedChannels: 1,
                maxChannelsPerConnection: 1,
            }),
        Error,
        'floors to 0',
    )
    // And the deliberate way to say it is accepted.
    new ChannelManager({
        driver,
        maxWatchedChannels: 1,
        maxChannelsPerConnection: 1,
        anonymousHostingShare: 1,
    })
})

Deno.test('#322: the ceiling FLOORS a fractional product', async () => {
    // `Math.floor` was never exercised at a fractional product — every other
    // fixture uses a share that divides the cap exactly, so replacing it with
    // `Math.ceil` or `Math.round` changed nothing observable.
    // 100 x 0.29 = 28.999..., which floors to 28 and rounds/ceils to 29.
    const { driver } = watchingDriver()
    const m = new ChannelManager({
        driver,
        maxWatchedChannels: 100,
        maxChannelsPerConnection: 100,
        anonymousHostingShare: 0.29,
    })
    for (let i = 0; i < 28; i++) {
        assertEquals(await m.subscribe(conn(`a${i}`), `ch${i}`), { ok: true })
    }
    const error = await assertRejects(
        () => m.subscribe(conn('a28'), 'ch28'),
        ChannelLimitError,
    )
    assertEquals(
        error.limit,
        28,
        'the ceiling did not FLOOR: 100 x 0.29 is 28.999..., and admitting a ' +
            '29th anonymous host means the arithmetic rounds',
    )
    assertEquals(error.scope, 'instance-anonymous')
    // And the message renders the reserved-share branch, which no other test
    // reaches — the three scopes have three different sentences.
    assertStringIncludes(error.message, 'no identity')
})
