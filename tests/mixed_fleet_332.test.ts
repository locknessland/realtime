/**
 * @fileoverview #332 — a fleet running two releases at once never widens a room
 * revocation into a session kill.
 *
 * `@lockness/realtime` is **published**, so a rolling deploy is a real scenario
 * rather than a hypothetical one, and both directions of it are load-bearing.
 * Two mechanisms carry the whole safety argument, and each has exactly one
 * witness here because neither is visible from any same-version test:
 *
 * 1. **The space delimiter.** A connection id is asserted against
 *    `/^[A-Za-z0-9:._-]+$/` before it is ever minted, so it can never contain a
 *    space. A reader of the previous release therefore looks up
 *    `"c1 private-orders <id>"` in its connection map, finds nothing
 *    **structurally**, and skips the record. Move the delimiter to a `:` or a `.` "for
 *    readability" and a composite can collide with a real id — at which point
 *    that reader applies a room revocation as a hard-close 4403 of the whole
 *    session. Every same-version test still passes.
 *
 * 2. **A new field only on a kind no published peer acts on** (#337). The
 *    control MAC covers a fixed field list, and `revoke-channel` now carries
 *    `revocationId`, appended to it LAST. A field added to the wire but not to
 *    the canonical form would ship **unauthenticated**, so it is on both — and
 *    a peer on the previous release therefore drops `revoke-channel` as an
 *    invalid MAC, with a WARN. That peer never acted on the kind, so it loses
 *    nothing it had. What must NOT move is every other kind: an absent field is
 *    omitted from the canonical bytes, so `evict` and the presence frames stay
 *    byte-identical to the previous release and still verify there.
 *
 * **The honest cost is asserted too**, not glossed: a revoke aimed at a socket
 * an older instance owns does **not** land, and no reconcile can rescue it —
 * the record is only ever applied by the owner, and the owner is the instance
 * that cannot read it. That is bounded by the deploy, and `evict` is the verb
 * every release obeys.
 *
 * @module @lockness/realtime/tests/mixed_fleet_332
 */

import { assert, assertEquals } from '@std/assert'
import { hmacSha256Hex } from '../../redis/mod.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import { isValidName } from '../protocol.ts'
import type { PresenceMember } from '../channel.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import {
    assertRosterRead,
    asWindow,
    rosterReadCount,
} from './roster_window_double.ts'

interface User {
    id: number
}

const SECRET = 'a-deployment-secret-with-more-than-enough-entropy'
const KEY = new TextEncoder().encode(SECRET)
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const ROOM = 'presence-room'
const ID = '5f0c1c8e-8a4e-4a57-9d8e-2f4b6b1d7c01'

/**
 * The canonical bytes a PREVIOUS-release peer MACs, transcribed as a second
 * copy — its fixed field list, with no `revocationId`. Asking the driver to
 * sign twice could not detect a field the driver forgot to cover.
 */
function previousReleaseCanonical(
    wire: Record<string, unknown>,
): Uint8Array<ArrayBuffer> {
    return new TextEncoder().encode(JSON.stringify({
        kind: wire.kind,
        target: wire.target,
        channel: wire.channel,
        member: wire.member,
        origin: wire.origin,
        ts: wire.ts,
        nonce: wire.nonce,
    }))
}

/** Every control frame `driver` publishes while `run` executes, parsed. */
async function publishedFrames(
    run: (driver: RedisBroadcastDriver) => Promise<void>,
): Promise<Record<string, unknown>[]> {
    const payloads: string[] = []
    const driver = new RedisBroadcastDriver(
        {
            command: (...args: string[]) => {
                if (args[0] === 'PUBLISH') payloads.push(args[2])
                return Promise.resolve({ type: 'integer' as const, value: 1 })
            },
        },
        { psubscribe: () => {} },
        { prefix: 'mixed-fleet', control: { secret: SECRET } },
    )
    try {
        await run(driver)
    } finally {
        await driver.close()
    }
    return payloads.map((p) => JSON.parse(p) as Record<string, unknown>)
}

/**
 * How an instance of the PREVIOUS release read the revocation index.
 *
 * Transcribed from the shipped `listRevoked`, deliberately as a second copy —
 * it returned bare connection ids, filtered on the charset, and the reconcile
 * handed each straight to a whole-connection revoke. Being a copy is the point:
 * the current driver can no longer produce this behaviour, and the question is
 * what the code that still exists in the fleet does with what we now write.
 */
function readAsPreviousRelease(members: string[]): string[] {
    const live = new Set<string>()
    for (const id of members) {
        if (id && isValidName(id)) live.add(id)
    }
    return [...live]
}

function conn(id: string, userId: number): Connection<User> {
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: () => {},
        close: () => {},
    } as Connection<User>
}

Deno.test('#332 row 1+2: a previous-release reader is INERT on a channel-scoped record', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        await driver.markRevocation({ target: 'c1' })
        await driver.markRevocation({ target: 'c2', channel: ROOM, id: ID })

        const raw = await redis.command('ZRANGEBYSCORE', INDEX, '-inf', '+inf')
        const members = (raw as { value: { value: string }[] }).value
            .map((entry) => entry.value)

        assertEquals(
            readAsPreviousRelease(members).sort(),
            ['c1'],
            'ROW 1: a bare record is read unchanged, so a connection ' +
                'revocation written by either release still works. ROW 2: the ' +
                'composite is SKIPPED — and skipped structurally, because a ' +
                'connection id can never contain a space, not because anything ' +
                'recognised the new format',
        )

        // The dangerous alternative, spelled out so the assertion above cannot
        // be read as a formality: had the composite survived that filter, the
        // previous release's reconcile would have passed the whole string to
        // its connection-scoped revoke.
        assert(
            !isValidName(`c2 presence-room ${ID}`),
            'the delimiter must stay OUTSIDE the name charset. Inside it, a ' +
                'composite is indistinguishable from a real connection id and ' +
                'a room ban becomes a 4403 session kill',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#332 row 3+4: the current reader handles both shapes, with no index migration', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        // A bare member as the PREVIOUS release would have written it, planted
        // directly — the current driver would write the same bytes, which is
        // exactly why no migration is owed.
        await redis.command('ZADD', INDEX, '9999999999', 'c1')
        await driver.markRevocation({ target: 'c2', channel: ROOM, id: ID })

        const live = await driver.listRevocations()
        assertEquals(
            live.sort((a, b) => a.target.localeCompare(b.target)),
            [{ target: 'c1' }, { target: 'c2', channel: ROOM, id: ID }],
            'ROW 3: a bare member written by the previous release parses as ' +
                'connection scope. ROW 4: a composite parses as channel scope. ' +
                'One index, both shapes, no dual-write and nothing to migrate',
        )
    } finally {
        await driver.close()
    }
})

Deno.test('#337 evict and presence frames are BYTE-IDENTICAL to the previous release', async () => {
    // The half of the mixed-fleet argument that must never move. A field
    // present on these kinds would change their canonical bytes, every older
    // peer would drop them as forgeries, and an evict issued mid-deploy would
    // never land on a previous-release owner.
    const member: PresenceMember = { id: 7, info: { name: 'Ada' } }
    const frames = await publishedFrames(async (driver) => {
        await driver.publishControl({ kind: 'evict', target: 'c1' })
        await driver.publishControl({
            kind: 'presence-join',
            target: 'c1',
            channel: ROOM,
            member,
        })
        await driver.publishControl({
            kind: 'presence-leave',
            target: 'c1',
            channel: ROOM,
            member,
        })
    })

    assertEquals(frames.length, 3)
    for (const wire of frames) {
        assertEquals(
            hmacSha256Hex(KEY, previousReleaseCanonical(wire)),
            wire.mac,
            `a previous-release peer verifies ${wire.kind}: its canonical ` +
                'bytes are unchanged',
        )
        assert(
            !('revocationId' in wire),
            `${wire.kind} carries no revocationId — the field exists only on ` +
                'revoke-channel',
        )
    }
})

Deno.test('#337 a previous-release peer DROPS revoke-channel — and it is covered by the MAC here', async () => {
    const [wire] = await publishedFrames((driver) =>
        driver.publishControl({
            kind: 'revoke-channel',
            target: 'c1',
            channel: ROOM,
            revocationId: ID,
        })
    )

    assertEquals(
        wire.revocationId,
        ID,
        'the frame names the record the owner will clear',
    )
    // The current canonical form: the previous field list, `revocationId`
    // appended LAST. Transcribed, not re-derived through the driver.
    const current = new TextEncoder().encode(JSON.stringify({
        kind: wire.kind,
        target: wire.target,
        channel: wire.channel,
        member: wire.member,
        origin: wire.origin,
        ts: wire.ts,
        nonce: wire.nonce,
        revocationId: wire.revocationId,
    }))
    assertEquals(
        hmacSha256Hex(KEY, current),
        wire.mac,
        'revocationId is INSIDE the MAC. Outside it, anyone with bus access ' +
            'could re-point a genuine frame at a different record',
    )
    assert(
        hmacSha256Hex(KEY, previousReleaseCanonical(wire)) !== wire.mac,
        'and a previous-release peer, which cannot cover the field, computes ' +
            'a different MAC and drops the frame with a WARN before it reaches ' +
            'a handler. INERT: that peer never acted on revoke-channel, so the ' +
            'cost is a log line, not a lost revocation — and a frame it did ' +
            'act on (above) is untouched',
    )
})

Deno.test('#332 a previous-release peer receiving the new kind does NOTHING', async () => {
    // `handleControl`'s switch has three arms and no `default`, so an unknown
    // kind falls off the end. Modelled by handing a manager a frame it does
    // not know: the `evict` arm must not fire, and nothing may be torn down.
    let deliver: ((control: ControlMessage) => void) | undefined
    const roster = new Map<string, Map<string, PresenceMember>>()
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
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
        watchChannel: () => {},
        unwatchChannel: () => {},
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (identity: User | null) =>
            identity ? { id: identity.id } : false,
    })
    const holder = conn('c1', 1)
    m.register(holder)
    const rosterReadsBefore = rosterReadCount()
    await m.subscribe(holder, ROOM)
    assertRosterRead(rosterReadsBefore)

    // NARROWED, not optional-chained. `deliver?.(...)` on a seam that was
    // never registered is a silent no-op, and every assertion below would then
    // pass having delivered nothing — the frame arriving IS half of what this
    // test claims.
    assert(deliver !== undefined, 'the control seam was registered')
    const send = deliver as (control: ControlMessage) => void

    // A kind this manager's switch does not carry — the shape a peer running
    // an older release sees when a newer kind reaches it with a MAC it can
    // verify. (`revoke-channel` itself no longer does: see the test above.)
    send({ kind: 'no-such-kind' as never, target: 'c1', channel: ROOM })
    for (let i = 0; i < 50; i++) await Promise.resolve()

    assertEquals(
        [...(roster.get(ROOM)?.keys() ?? [])],
        ['1'],
        'inert: an unknown kind removes nothing. A `default` arm that threw ' +
            'or warned would turn a forward-compatible frame into noise on ' +
            'every peer for the length of a rolling deploy',
    )
    assertEquals(await m.unsubscribe('c1', ROOM), 'left', 'still subscribed')
})

Deno.test('#332 the accepted cost: a revoke aimed at a previous-release owner does not land', async () => {
    // Stated rather than skipped. The record is only ever applied by the
    // instance that owns the socket, and that instance is precisely the one
    // that cannot read a channel-scoped record — so the reconcile cannot
    // rescue this and no design change here would make it.
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        await driver.markRevocation({
            target: 'owned-by-old',
            channel: ROOM,
            id: ID,
        })
        const raw = await redis.command('ZRANGEBYSCORE', INDEX, '-inf', '+inf')
        const members = (raw as { value: { value: string }[] }).value
            .map((entry) => entry.value)

        assertEquals(
            readAsPreviousRelease(members),
            [],
            'the owner sees nothing to apply — bounded by the deploy, and ' +
                '`evict` is the verb every release obeys if certainty is ' +
                'needed mid-deploy',
        )
        assertEquals(
            redis.zcard(INDEX),
            1,
            'and the record is NOT destroyed by that reader, so the instant ' +
                'the socket moves to an upgraded instance it is applied',
        )
    } finally {
        await driver.close()
    }
})
