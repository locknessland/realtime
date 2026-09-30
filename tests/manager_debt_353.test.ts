/**
 * @fileoverview #353 — a value the realtime classifiers cannot inspect is
 * refused with a named error, and the join's member invariant precedes every
 * write.
 *
 * **A value the classifiers cannot inspect.** `Array.isArray` throws on a
 * revoked Proxy ("Cannot perform 'IsArray' on a proxy that has been revoked"),
 * and `instanceof` — which asks the handler for a prototype — throws on a
 * revoked Proxy or on one whose `getPrototypeOf` trap throws. The
 * authorizer-result classifier, `typeLabel` and the wire predicates in
 * `protocol.ts` all ran one of the two, so a member whose `id` is a revoked
 * Proxy surfaced as an anonymous `TypeError` instead of
 * `PresenceMemberIdError`, an authorizer returning a throwing-trap Proxy
 * surfaced as that trap's error instead of `AuthorizeResultError`, and a
 * driver reporting a revoked-Proxy departure made the manager's "never
 * throws" handler throw. Each site now answers with a verdict or a label that
 * no trap produced: `'uninspectable object'`.
 *
 * **A LIVE Proxy too** (#353 review). `isPresenceMemberWire` reads the
 * member's own keys and its `id` and `info`; a Proxy whose `ownKeys` or `get`
 * trap throws passed the non-array check and then threw from those reads, so
 * the departure handler threw again. The predicate now answers `false` for
 * any read that throws.
 *
 * **What this does not reach.** An authorizer whose result's `then` cannot be
 * read still rejects with the error that read raised, and cannot be made not
 * to: `await` reads `then` before the manager holds the value, and for an
 * `async` authorizer the rejection comes from the authorizer's own promise —
 * the value never reaches Lockness at all. A revoked Proxy rejects with the
 * engine's `TypeError`; a throwing `get` trap or `then` getter rejects with
 * its own error. Those rows are pinned below, with the part Lockness does own:
 * nothing is written.
 *
 * **Every "writes nothing" row reads four instruments, each shown to move
 * first (#351).** An admitted observer joins the same channel before the
 * refused subscribe, and that join must move the channel's membership, put the
 * observer on the roster and — on a presence channel over the fake Redis —
 * publish one control frame. Only then are the refusal's zeros read, and the
 * delivery half is paired with a broadcast the observer does receive. The
 * control-publish zero is presence-only and fake-Redis-only for the reason
 * `authorize_result_347.test.ts` pins: an admitted private subscribe publishes
 * nothing, and the memory driver has no control plane to count.
 *
 * **The member invariant.** `subscribe` once threw "a presence admission
 * reached the join without a member" after it had written the connection's
 * binding. Since #370 `subscribe` writes no binding at all, so the last test
 * here pins that directly, and battery row M1 (the invariant moved below the
 * caps) is recorded as an equivalent mutant.
 *
 * @module @lockness/realtime/tests/manager_debt_353
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import {
    type Authorizer,
    AuthorizeResultError,
    ChannelLimitError,
    ChannelManager,
    MemoryBroadcastDriver,
    type PresenceMember,
    PresenceMemberIdError,
    RedisBroadcastDriver,
} from '../mod.ts'
import { classifyAuthorizeResult, typeLabel } from '../channel.ts'
import {
    isPresenceMemberIdValue,
    isPresenceMemberInfoValue,
    isPresenceMemberWire,
} from '../protocol.ts'
import type {
    BroadcastDriver,
    ControlMessage,
    RosterDeparture,
} from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'
import { asWindow } from './roster_window_double.ts'

interface User {
    id: number
}

const PRIVATE = 'private-orders'
const PRESENCE = 'presence-room'
const PREFIX = 'app:rt'
const CONTROL_TOPIC = `${PREFIX}__control`
const UNINSPECTABLE = 'uninspectable object'
/** V8's wording for a `[[Get]]` on a revoked Proxy — `await` reading `then`. */
const REVOKED_GET = "Cannot perform 'get' on a proxy that has been revoked"
/** What every application-written trap below throws. */
const TRAP_MESSAGE = 'a trap the application wrote'

/** The identity whose authorizer answer is the value under test. */
const SUSPECT = 1
/** The identity of the admitted observer — every row's positive control. */
const OBSERVER = 2

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

/** A Proxy whose handler was revoked: every trap-reaching operation throws. */
function revokedProxy(): object {
    const { proxy, revoke } = Proxy.revocable({ id: 1 }, {})
    revoke()
    return proxy
}

/** A live Proxy whose `getPrototypeOf` trap throws — `instanceof` runs it. */
function throwingPrototypeProxy(): object {
    return new Proxy({ id: 1 }, {
        getPrototypeOf() {
            throw new Error(TRAP_MESSAGE)
        },
    })
}

/** A live Proxy whose `ownKeys` trap throws — `Object.keys` runs it. */
function throwingOwnKeysProxy(): object {
    return new Proxy({ id: 1 }, {
        ownKeys() {
            throw new Error(TRAP_MESSAGE)
        },
    })
}

/**
 * A live Proxy whose `get` trap throws. Its own keys are only `id`, so the
 * key rule passes and the first throwing read is the `id` field's.
 */
function throwingGetProxy(): object {
    return new Proxy({ id: 1 }, {
        get() {
            throw new Error(TRAP_MESSAGE)
        },
    })
}

/** An array dressed as a plain object: its prototype trap answers Object's. */
function arrayDisguisedAsObject(): object {
    return new Proxy([], { getPrototypeOf: () => Object.prototype })
}

/** A plain object dressed as an array: its prototype trap answers Array's. */
function objectDisguisedAsArray(): object {
    return new Proxy({ id: 1 }, { getPrototypeOf: () => Array.prototype })
}

/** A prototype-less member — no `toString`, no `hasOwnProperty`, no `then`. */
function nullPrototypeMember(id: unknown): object {
    return Object.assign(Object.create(null), { id })
}

/** An authorizer answering `value` to everyone, as plain JS reaches it. */
function answering(value: () => unknown): Authorizer<User> {
    return (() => value()) as unknown as Authorizer<User>
}

/**
 * How many connections this instance holds on `channel` — the membership a
 * refused subscribe must not write (#370 review: `connectionCount` moves only
 * on `register` now, so it can no longer tell a refusal from an admission).
 */
function membersOn(m: ChannelManager<User>, channel: string): number {
    return (m as unknown as { subscriptions: Map<string, Set<string>> })
        .subscriptions.get(channel)?.size ?? 0
}

/**
 * An authorizer that answers `value` for the suspect and a contract-valid
 * admission for the observer, cast the way plain JS reaches the manager.
 */
function suspectAuthorizer(value: () => unknown): Authorizer<User> {
    return ((identity: User | null, channel: string) => {
        if (identity?.id === SUSPECT) return value()
        return channel.startsWith('presence-') ? { id: identity?.id } : true
    }) as unknown as Authorizer<User>
}

/** Let the fake broker's pub/sub round-trip settle. */
async function settle(): Promise<void> {
    for (let i = 0; i < 5; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0))
    }
}

/** One backend under test: a driver, and what it can tell us afterwards. */
interface Backend {
    driver: BroadcastDriver
    /** Control frames published so far — `undefined` with no control plane. */
    controlPublishes(): number | undefined
    close(): Promise<void>
}

const BACKENDS: ReadonlyArray<readonly [string, () => Backend]> = [
    ['memory', () => ({
        driver: new MemoryBroadcastDriver(),
        controlPublishes: () => undefined,
        close: () => Promise.resolve(),
    })],
    ['fake Redis', () => {
        const redis = new FakeRedis()
        const driver = new RedisBroadcastDriver(
            { command: redis.command },
            redis.subscriberFor(),
            {
                prefix: PREFIX,
                control: { secret: 'deployment-secret-with-enough-entropy' },
            },
        )
        return {
            driver,
            controlPublishes: () =>
                redis.commandLog().filter(([cmd, topic]) =>
                    cmd === 'PUBLISH' && topic === CONTROL_TOPIC
                ).length,
            close: async () => {
                await driver.close()
                redis.assertNoRejections()
            },
        }
    }],
]

/** The roster's member ids for a channel, as the driver reports them. */
async function rosterIds(driver: BroadcastDriver, channel: string) {
    const window = await driver.readRoster!(channel, 1_000, [])
    return window.members.map((m) => m.id)
}

/**
 * What a refusal row on `channel` over `backendName` asserts — the row's
 * title claims exactly this and no more.
 */
function refusalClaims(backendName: string, channel: string): string {
    const claims = ['no connection']
    if (channel === PRESENCE) {
        claims.push('no roster entry')
        if (backendName === 'fake Redis') claims.push('no control frame')
    }
    claims.push('no delivery')
    return claims.join(', ')
}

/**
 * Admit an observer, show every instrument moves for it, then run the
 * suspect's refused subscribe through `refuse` and read every instrument
 * back at zero.
 *
 * @param backend - The driver and its control-publish counter.
 * @param channel - The channel both connections subscribe to.
 * @param value - What the authorizer answers the suspect.
 * @param refuse - Asserts the suspect's subscribe rejects as the row claims.
 */
async function assertRefusalWritesNothing(
    backend: Backend,
    channel: string,
    value: () => unknown,
    refuse: (attempt: () => Promise<unknown>) => Promise<void>,
): Promise<void> {
    const m = new ChannelManager<User>({
        driver: backend.driver,
        authorize: suspectAuthorizer(value),
    })
    const publishesAtStart = backend.controlPublishes()
    // Both sockets are registered at open (#370). The refusal is read on the
    // channel's membership, which only a subscribe moves.
    const observer = conn('observer', OBSERVER)
    m.register(observer)
    const suspect = conn('suspect', SUSPECT)
    m.register(suspect)
    const membersAtStart = membersOn(m, channel)
    assertEquals((await m.subscribe(observer, channel)).ok, true)
    const membersBefore = membersOn(m, channel)
    const publishesBefore = backend.controlPublishes()
    // POSITIVE CONTROLS (#351): each instrument the refusal is read with
    // registers an admission.
    assertEquals(
        membersBefore,
        membersAtStart + 1,
        'CONTROL: an admitted subscribe moves the membership',
    )
    if (channel === PRESENCE) {
        assertEquals(
            await rosterIds(backend.driver, channel),
            [OBSERVER],
            'CONTROL: an admitted presence join is on the roster',
        )
        if (publishesAtStart !== undefined) {
            assertEquals(
                publishesBefore,
                publishesAtStart + 1,
                'CONTROL: an admitted presence join publishes one control frame',
            )
        }
    }

    await refuse(() => m.subscribe(suspect, channel))

    assertEquals(
        membersOn(m, channel),
        membersBefore,
        'no membership was written for the suspect',
    )
    if (channel === PRESENCE) {
        assertEquals(
            await rosterIds(backend.driver, channel),
            [OBSERVER],
            'no roster entry',
        )
        if (publishesBefore !== undefined) {
            assertEquals(
                backend.controlPublishes(),
                publishesBefore,
                'no control frame',
            )
        }
    }
    const observerFrames = observer.received.length
    m.broadcast(channel, 'secret', { n: 1 })
    await settle()
    assert(
        observer.received.length > observerFrames,
        'CONTROL: the admitted observer receives the broadcast',
    )
    assertEquals(
        suspect.received,
        [],
        'the refused connection receives nothing',
    )
}

/** Every backend × `channels`, each row titled by what it asserts. */
function refusalRows(
    channels: readonly string[],
    title: string,
    value: () => unknown,
    refuse: (attempt: () => Promise<unknown>) => Promise<void>,
): void {
    for (const [backendName, makeBackend] of BACKENDS) {
        for (const channel of channels) {
            Deno.test(
                `#353 ${backendName} ${channel}: ${title} — ${
                    refusalClaims(backendName, channel)
                }`,
                async () => {
                    const backend = makeBackend()
                    try {
                        await assertRefusalWritesNothing(
                            backend,
                            channel,
                            value,
                            refuse,
                        )
                    } finally {
                        await backend.close()
                    }
                },
            )
        }
    }
}

/** Asserts a rejection is exactly the error an application trap threw. */
async function rejectsWithTrapError(attempt: () => Promise<unknown>) {
    const error = await assertRejects(attempt, Error)
    assertEquals(error.constructor, Error, 'the trap error, not a wrapper')
    assertEquals(error.message, TRAP_MESSAGE)
}

// --- The classifier and the label -------------------------------------------

Deno.test('#353 classifyAuthorizeResult answers `invalid` for a value it cannot inspect, and never throws', () => {
    assertEquals(classifyAuthorizeResult(revokedProxy()), {
        verdict: 'invalid',
        type: UNINSPECTABLE,
    })
    assertEquals(classifyAuthorizeResult(throwingPrototypeProxy()), {
        verdict: 'invalid',
        type: UNINSPECTABLE,
    })
    // CONTROL: a Proxy as such is not refused — a transparent one is an
    // object like any other, and admits.
    const transparent = new Proxy({ id: 1 }, {})
    assertEquals(classifyAuthorizeResult(transparent), {
        verdict: 'admit',
        member: transparent,
    })
})

Deno.test('#353 typeLabel names a value it cannot inspect without running a trap', () => {
    assertEquals(typeLabel(revokedProxy()), UNINSPECTABLE)
    assertEquals(typeLabel(throwingPrototypeProxy()), UNINSPECTABLE)
    // CONTROL: a transparent Proxy is labelled through its target.
    assertEquals(typeLabel(new Proxy([], {})), 'array')
    assertEquals(typeLabel(new Proxy({}, {})), 'object')
})

Deno.test('#353 a prototype-less object is an object: classified `admit`, labelled `object`', () => {
    // `instanceof` walks a null prototype chain and answers `false` without
    // throwing, so nothing here is uninspectable.
    const member = nullPrototypeMember(1)
    assertEquals(classifyAuthorizeResult(member), { verdict: 'admit', member })
    assertEquals(typeLabel(Object.create(null)), 'object')
})

Deno.test('#353 an array is judged by what it IS, not the prototype its Proxy claims', () => {
    // `Array.isArray` sees through a Proxy to its target and runs no trap; the
    // wire sees the same thing, since `JSON.stringify` asks the same question.
    assertEquals(classifyAuthorizeResult(arrayDisguisedAsObject()), {
        verdict: 'invalid',
        type: 'array',
    })
    assertEquals(typeLabel(arrayDisguisedAsObject()), 'array')
    const dressed = objectDisguisedAsArray()
    assertEquals(classifyAuthorizeResult(dressed), {
        verdict: 'admit',
        member: dressed,
    })
    assertEquals(JSON.stringify(dressed), '{"id":1}', 'the wire sees an object')
})

// --- subscribe: the named errors, not a TypeError ---------------------------

refusalRows(
    [PRIVATE, PRESENCE],
    "an authorizer returning a Proxy whose getPrototypeOf trap throws gets AuthorizeResultError, not the trap's error",
    throwingPrototypeProxy,
    async (attempt) => {
        const error = await assertRejects(attempt, AuthorizeResultError)
        assert(
            error.message.includes(`returned ${UNINSPECTABLE} for`),
            `the label is named. Got: ${error.message}`,
        )
    },
)

// The rejection is the engine's, raised by `await` reading `then` — not the
// classifier's `IsArray`, whose wording is "Cannot perform 'IsArray'". If a
// future change classifies this value, these rows are the ones to update,
// deliberately. Sync and async: an `async` authorizer rejects on its own.
for (
    const [shape, value] of [
        ['returning', revokedProxy],
        ['resolving to', () => Promise.resolve(revokedProxy())],
    ] as const
) {
    refusalRows(
        [PRIVATE, PRESENCE],
        `an authorizer ${shape} a REVOKED Proxy rejects with the engine's TypeError "${REVOKED_GET}" — before Lockness holds the value`,
        value,
        async (attempt) => {
            await assertRejects(attempt, TypeError, REVOKED_GET)
        },
    )
}

refusalRows(
    [PRIVATE, PRESENCE],
    "an authorizer returning a Proxy whose get trap throws rejects with the trap's own error — `await` reads `then`",
    throwingGetProxy,
    rejectsWithTrapError,
)

refusalRows(
    [PRIVATE, PRESENCE],
    "an authorizer returning an object whose `then` getter throws rejects with the getter's own error",
    () => ({
        id: SUSPECT,
        get then() {
            throw new Error(TRAP_MESSAGE)
        },
    }),
    rejectsWithTrapError,
)

refusalRows(
    [PRESENCE],
    "an authorizer returning a Proxy whose ownKeys trap throws rejects with the trap's own error — the admission's one read of the member",
    throwingOwnKeysProxy,
    rejectsWithTrapError,
)

refusalRows(
    [PRESENCE],
    'a member whose id is a revoked Proxy throws PresenceMemberIdError',
    () => ({ id: revokedProxy() }),
    async (attempt) => {
        const error = await assertRejects(attempt, PresenceMemberIdError)
        assert(
            error.message.includes(`of type ${UNINSPECTABLE}`),
            `the id is named by its label. Got: ${error.message}`,
        )
    },
)

refusalRows(
    [PRESENCE],
    'a member whose id is a Proxy whose getPrototypeOf trap throws throws PresenceMemberIdError',
    () => ({ id: throwingPrototypeProxy() }),
    async (attempt) => {
        const error = await assertRejects(attempt, PresenceMemberIdError)
        assert(
            error.message.includes(`of type ${UNINSPECTABLE}`),
            `the id is named by its label. Got: ${error.message}`,
        )
    },
)

refusalRows(
    [PRESENCE],
    'a member whose id is a Proxy whose get and ownKeys traps throw throws PresenceMemberIdError — `typeof` runs no trap',
    () => ({
        id: new Proxy({}, {
            get() {
                throw new Error(TRAP_MESSAGE)
            },
            ownKeys() {
                throw new Error(TRAP_MESSAGE)
            },
        }),
    }),
    async (attempt) => {
        const error = await assertRejects(attempt, PresenceMemberIdError)
        assert(
            error.message.includes('of type object'),
            `the id is labelled through its target. Got: ${error.message}`,
        )
    },
)

refusalRows(
    [PRIVATE, PRESENCE],
    'an authorizer returning an array its Proxy dresses as a plain object gets AuthorizeResultError naming `array`',
    arrayDisguisedAsObject,
    async (attempt) => {
        const error = await assertRejects(attempt, AuthorizeResultError)
        assert(
            error.message.includes('returned array for'),
            `the array is named. Got: ${error.message}`,
        )
    },
)

Deno.test('#353 a prototype-less authorizer result admits on both kinds, and its member reaches the roster', async () => {
    for (const [backendName, makeBackend] of BACKENDS) {
        const backend = makeBackend()
        try {
            const m = new ChannelManager<User>({
                driver: backend.driver,
                authorize: answering(() => nullPrototypeMember(SUSPECT)),
            })
            const c1 = conn('c1', SUSPECT)
            m.register(c1)
            assertEquals(
                (await m.subscribe(c1, PRIVATE)).ok,
                true,
                `${backendName}: private`,
            )
            const c2 = conn('c2', SUSPECT)
            m.register(c2)
            assertEquals(
                (await m.subscribe(c2, PRESENCE)).ok,
                true,
                `${backendName}: presence`,
            )
            assertEquals(
                await rosterIds(backend.driver, PRESENCE),
                [SUSPECT],
                `${backendName}: the member is held`,
            )
        } finally {
            await backend.close()
        }
    }
})

// --- protocol.ts: the wire predicates answer, never throw -------------------

Deno.test('#353 isPresenceMemberInfoValue answers false for a revoked Proxy', () => {
    assertEquals(isPresenceMemberInfoValue(revokedProxy()), false)
    // CONTROL: the predicate still admits an object.
    assertEquals(isPresenceMemberInfoValue({ name: 'Ada' }), true)
})

Deno.test('#353 isPresenceMemberInfoValue runs no trap of a live Proxy, so a throwing one cannot make it throw', () => {
    // Its only inspection is `Array.isArray`, which reads a live Proxy's
    // target without a trap. Not refused: the one caller that hands it a
    // non-JSON value, the departure handler, announces through a path whose
    // every failure is a WARN — and on the wire no Proxy exists.
    assertEquals(isPresenceMemberInfoValue(throwingOwnKeysProxy()), true)
    assertEquals(isPresenceMemberInfoValue(throwingGetProxy()), true)
})

Deno.test('#353 isPresenceMemberWire answers false for a revoked Proxy, as the member or as its info', () => {
    assertEquals(isPresenceMemberWire(revokedProxy()), false)
    assertEquals(isPresenceMemberWire({ id: 1, info: revokedProxy() }), false)
    // CONTROL: a well-formed member passes.
    assertEquals(isPresenceMemberWire({ id: 1, info: { name: 'Ada' } }), true)
})

Deno.test('#353 isPresenceMemberWire answers false, and never throws, for a live Proxy whose ownKeys or get trap throws', () => {
    assertEquals(isPresenceMemberWire(throwingOwnKeysProxy()), false)
    assertEquals(isPresenceMemberWire(throwingGetProxy()), false)
    // CONTROL: the same target behind a transparent Proxy passes, so the
    // `false` above is the throwing trap's, not the Proxy's.
    assertEquals(isPresenceMemberWire(new Proxy({ id: 1 }, {})), true)
})

Deno.test('#353 isPresenceMemberWire judges a disguised or prototype-less member by its content', () => {
    assertEquals(isPresenceMemberWire(arrayDisguisedAsObject()), false)
    assertEquals(isPresenceMemberWire(objectDisguisedAsArray()), true)
    assertEquals(isPresenceMemberWire(nullPrototypeMember(1)), true)
    assertEquals(isPresenceMemberWire(nullPrototypeMember(null)), false)
})

Deno.test('#353 isPresenceMemberIdValue already answers false for a revoked Proxy — `typeof` runs no trap', () => {
    assertEquals(isPresenceMemberIdValue(revokedProxy()), false)
})

Deno.test('#353 a departure a driver reports with a value it cannot inspect is dropped with one WARN — the handler never throws', async () => {
    let handler: ((d: RosterDeparture) => void | Promise<void>) | undefined
    const published: ControlMessage[] = []
    const roster = new Map<string, PresenceMember>()
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl: () => {},
        publishControl: (control) => void published.push(control),
        holdMember: (_channel, member) => {
            const arrived = !roster.has(String(member.id))
            roster.set(String(member.id), member)
            return { arrived }
        },
        releaseMember: (_channel, id) => ({ gone: roster.delete(String(id)) }),
        readRoster: (_channel, limit, selfIds) =>
            asWindow([...roster.values()], limit, selfIds),
        onRosterDeparture: (registered) => void (handler = registered),
    }
    const m = new ChannelManager<User>({
        driver,
        authorize: (user) => user ? { id: user.id } : false,
    })
    const observer = conn('observer', 1)
    m.register(observer)
    await m.subscribe(observer, PRESENCE)
    assert(handler, 'precondition: a manager with a roster registers')
    const seen = observer.received.length
    published.length = 0

    const bad: ReadonlyArray<
        readonly [label: string, departure: RosterDeparture, names: string]
    > = [
        ['a revoked-Proxy member', {
            channel: PRESENCE,
            member: revokedProxy() as PresenceMember,
        }, PRESENCE],
        ['a member whose info is a revoked Proxy', {
            channel: PRESENCE,
            member: { id: 7, info: revokedProxy() } as PresenceMember,
        }, PRESENCE],
        // Live Proxies: `Object.keys` runs `ownKeys`, the field reads run
        // `get` — both used to throw out of the predicate.
        ['a member whose ownKeys trap throws', {
            channel: PRESENCE,
            member: throwingOwnKeysProxy() as PresenceMember,
        }, PRESENCE],
        ['a member whose get trap throws', {
            channel: PRESENCE,
            member: throwingGetProxy() as PresenceMember,
        }, PRESENCE],
        // The WARN names a non-string channel by `typeLabel`, which used to
        // throw on this value from inside the handler.
        ['a revoked-Proxy channel', {
            channel: revokedProxy() as unknown as string,
            member: { id: 7 },
        }, `a ${UNINSPECTABLE} channel`],
        // The departure object itself: its field reads used to sit outside
        // any guard, so a throwing getter escaped the handler.
        ['a departure whose channel getter throws', {
            get channel(): string {
                throw new Error('a getter the driver wrote')
            },
            member: { id: 7 },
        }, 'a undefined channel'],
        [
            'a revoked-Proxy departure',
            revokedProxy() as RosterDeparture,
            'a undefined channel',
        ],
    ]
    for (const [label, departure, names] of bad) {
        const lines: string[] = []
        const warn = console.warn
        console.warn = (...parts: unknown[]) => void lines.push(parts.join(' '))
        try {
            // A synchronous throw escapes before `await`, so this line is
            // itself the "never throws" assertion.
            await handler(departure)
        } finally {
            console.warn = warn
        }
        assertEquals(lines.length, 1, `${label}: one WARN`)
        assert(
            lines[0].startsWith(
                `realtime: dropped a roster departure the driver reported on ${names} — `,
            ),
            `${label}: the WARN names the channel only: ${lines[0]}`,
        )
        assertEquals(observer.received.length, seen, `${label}: no emit`)
        assertEquals(published, [], `${label}: no publish`)
    }

    // POSITIVE CONTROL (#351): a departure the handler can announce moves
    // both instruments the zeros above were read from.
    await handler({ channel: PRESENCE, member: { id: 7 } })
    assertEquals(
        observer.received.slice(seen),
        [{
            type: 'presence',
            channel: PRESENCE,
            action: 'left',
            member: { id: 7 },
        }],
        'CONTROL: a valid departure is emitted',
    )
    assertEquals(
        published.map((control) => control.kind),
        ['presence-leave'],
        'CONTROL: and published',
    )

    // ONE read per field: a `member` getter that answers a valid member first
    // and a malformed one after is checked and announced as the SAME value —
    // a second read could smuggle past the check what peers would refuse.
    let reads = 0
    const flipping = {
        channel: PRESENCE,
        get member(): PresenceMember {
            reads++
            return reads === 1
                ? { id: 8 }
                : { id: 8, smuggled: 'x' } as unknown as PresenceMember
        },
    }
    const before = observer.received.length
    await handler(flipping)
    assertEquals(reads, 1, 'the member is read once')
    assertEquals(
        observer.received.slice(before),
        [{
            type: 'presence',
            channel: PRESENCE,
            action: 'left',
            member: { id: 8 },
        }],
        'the announced member is the one that was checked',
    )
})

// --- The member invariant precedes every write -------------------------------

Deno.test('#353 a presence subscribe writes no binding of its own, admitted or refused (#370)', async () => {
    // What #353's ordering protected — a throwing subscribe leaving a
    // `connections` entry behind — can no longer happen: since #370 only
    // `register` binds. This pins that from both sides. The battery's M1 (the
    // member invariant moved below the caps) is recorded as an equivalent
    // mutant for the same reason; see its row.
    //
    // WHAT `connectionCount` CANNOT PIN (#403 review LOW). Restoring
    // `subscribe`'s own removed write — `register_only_admission_370.ts`'s
    // M9 — is an equivalent mutant BY CONSTRUCTION: by the time `subscribe`
    // would reach that write, admission has already proven `connection` is
    // the very object `connections` holds under its id, so re-adding
    // `connections.set(connection.id, connection)` there sets a key to the
    // value it already holds. `connectionCount` counts entries, not writes,
    // so it cannot and is not meant to distinguish "the write never ran"
    // from "the write ran and changed nothing" — that distinction has no
    // observable side effect anywhere in this class. This test's job is the
    // narrower, falsifiable one: the COUNT does not move. See M9's own
    // `expectSurvival` and `#assertBound`'s JSDoc for the full argument.
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: (user) => user ? { id: user.id } : false,
        maxWatchedChannels: 1,
        maxChannelsPerConnection: 1,
        anonymousHostingShare: 1,
    })
    const c1 = conn('c1', 1)
    m.register(c1)
    const c2 = conn('c2', 2)
    m.register(c2)
    assertEquals(m.connectionCount, 2, 'CONTROL: registering binds')

    assert((await m.subscribe(c1, PRESENCE)).ok, 'the admitted join')
    assertEquals(m.connectionCount, 2, 'an admitted join binds nothing more')

    await assertRejects(
        () => m.subscribe(c2, `${PRESENCE}-2`),
        ChannelLimitError,
    )
    assertEquals(m.connectionCount, 2, 'a refused join binds nothing more')
    await m.disconnect(c2)
    assertEquals(
        m.connectionCount,
        1,
        'CONTROL: the count moves when a binding is released',
    )
})
