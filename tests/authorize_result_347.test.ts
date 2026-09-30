/**
 * @fileoverview #347 — an authorizer result outside its contract never admits.
 *
 * `subscribe` used to read the authorizer's answer as a deny only when it was
 * exactly `false`. Every other value admitted: an authorizer returning
 * `undefined`, `null`, `0` or `''` put an authenticated stranger on a
 * `private-*` channel, and on a `presence-*` channel `0` produced an
 * **invisible listener** — delivery with no roster entry. The realistic trigger
 * is `return (await db.select()...)[0]`, which TypeScript does not catch.
 *
 * The contract is now three values: `true` and a non-array object admit,
 * `false` denies, and anything else throws `AuthorizeResultError`. Since #357
 * the object must be a `PresenceMember` on every channel kind —
 * `authorize_result_357.test.ts` pins that half. It is a
 * throw and not `{ ok: false }` because `{ ok: false }` means one thing — "not
 * authorized" (#331) — and a missing `return` is a bug, not a policy. Folding
 * it into a deny would turn it into an undiagnosable deny-all.
 *
 * **Every refusal row reads the state, not the return value.** It asserts the
 * throw, then that nothing was written (no `connections` entry, no roster
 * entry), nothing was published (no control frame) and nothing is delivered —
 * against a real {@link MemoryBroadcastDriver} and a {@link RedisBroadcastDriver}
 * over the fake broker. The delivery half carries a CONTROL: an admitted
 * observer on the same channel that does receive the broadcast, so "the refused
 * connection got nothing" cannot pass because nothing was delivered at all.
 *
 * **Every "nothing happened" counter is paired with a positive control (#351).**
 * The channel's membership and the control-publish count are each read before the
 * observer's admitted subscribe and shown to MOVE on it, in the same row, so a
 * counter that cannot register the event cannot pass for one that registered
 * none. The control-publish zero is asserted on presence rows only: an admitted
 * PRIVATE subscribe publishes no control frame at all (pinned below), so on a
 * private row that counter could not move whatever the manager did — there the
 * guards are the `connections` entry and the delivery control.
 *
 * @module @lockness/realtime/tests/authorize_result_347
 */

import {
    assert,
    assertEquals,
    assertInstanceOf,
    assertRejects,
} from '@std/assert'
import {
    type Authorizer,
    AuthorizeResultError,
    ChannelLimitError,
    ChannelManager,
    MemoryBroadcastDriver,
    type PresenceMember,
    RedisBroadcastDriver,
} from '../mod.ts'
import { classifyAuthorizeResult } from '../channel.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

interface User {
    id: number
}

const PRIVATE = 'private-orders'
const PRESENCE = 'presence-room'
const PREFIX = 'app:rt'
const CONTROL_TOPIC = `${PREFIX}__control`

/** The identity whose authorizer answer is the value under test. */
const SUSPECT = 1
/** The identity of the admitted observer — the delivery control. */
const OBSERVER = 2

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
    ['memory', () => {
        const driver = new MemoryBroadcastDriver()
        return {
            driver,
            controlPublishes: () => undefined,
            close: () => Promise.resolve(),
        }
    }],
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
 * admission for everyone else, cast the way a plain-JS app or an `any`-typed
 * query result reaches the manager.
 */
function suspectAuthorizer(value: () => unknown): Authorizer<User> {
    return ((identity: User | null, channel: string) => {
        if (identity?.id === SUSPECT) return value()
        return channel.startsWith('presence-') ? { id: identity?.id } : true
    }) as unknown as Authorizer<User>
}

/** The roster's member ids for a channel, as the driver reports them. */
async function rosterIds(driver: BroadcastDriver, channel: string) {
    const window = await driver.readRoster!(channel, 1_000, [])
    return window.members.map((m) => m.id)
}

/** The refused values, each with the type label its error must name. */
const REFUSED: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['undefined', () => undefined, 'undefined'],
    ['null', () => null, 'null'],
    ['0', () => 0, 'number'],
    ["''", () => '', 'string'],
    ['NaN', () => NaN, 'number'],
    ["'no'", () => 'no', 'string'],
    ['1', () => 1, 'number'],
    ['[]', () => [], 'array'],
    ['new Boolean(false)', () => new Boolean(false), 'boxed boolean'],
    ['a Symbol', () => Symbol('member'), 'symbol'],
    // #353: the classifier's `instanceof` runs this trap, so it used to throw
    // the application's own error instead of answering. A value it cannot
    // inspect is a defect like any other, named by a label no trap produced.
    // (A REVOKED Proxy never gets this far — `await` reads its `then` first;
    // see manager_debt_353.test.ts.)
    [
        'a Proxy whose getPrototypeOf trap throws',
        throwingPrototypeProxy,
        'uninspectable object',
    ],
]

/** A live Proxy whose `getPrototypeOf` trap throws — `instanceof` runs it. */
function throwingPrototypeProxy(): object {
    return new Proxy({ id: SUSPECT }, {
        getPrototypeOf() {
            throw new Error('a trap the application wrote')
        },
    })
}

/**
 * What a refusal row on `channel` over `backendName` actually asserts — the
 * row name claims exactly this. The roster is read on presence rows only, and
 * the control-publish zero only on a presence row over the backend that has a
 * control plane to count (the fake Redis).
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

for (const [backendName, makeBackend] of BACKENDS) {
    for (const channel of [PRIVATE, PRESENCE]) {
        for (const [valueName, value, label] of REFUSED) {
            const name =
                `#347 ${backendName} ${channel}: an authorizer returning ${valueName} throws AuthorizeResultError — ${
                    refusalClaims(backendName, channel)
                }`
            Deno.test(name, async () => {
                const backend = makeBackend()
                try {
                    const m = new ChannelManager<User>({
                        driver: backend.driver,
                        authorize: suspectAuthorizer(value),
                    })
                    const publishesAtStart = backend.controlPublishes()
                    // Both sockets are registered at open (#370). The
                    // refusal is read on the channel's membership, which only
                    // a subscribe moves.
                    const observer = conn('observer', OBSERVER)
                    m.register(observer)
                    const suspect = conn('suspect', SUSPECT)
                    m.register(suspect)
                    const membersAtStart = membersOn(m, channel)
                    assertEquals(
                        (await m.subscribe(observer, channel)).ok,
                        true,
                    )
                    const membersBefore = membersOn(m, channel)
                    const publishesBefore = backend.controlPublishes()
                    // POSITIVE CONTROLS (#351): the instruments the refusal
                    // is read with can register an admission at all.
                    assertEquals(
                        membersBefore,
                        membersAtStart + 1,
                        'CONTROL: an admitted subscribe moves the membership',
                    )
                    if (
                        channel === PRESENCE && publishesAtStart !== undefined
                    ) {
                        assertEquals(
                            publishesBefore,
                            publishesAtStart + 1,
                            'CONTROL: an admitted presence join publishes one control frame',
                        )
                    }

                    // The class argument is what rules out an accidental
                    // `TypeError` (a Symbol in a template literal): any other
                    // class fails the row here.
                    const error = await assertRejects(
                        () => m.subscribe(suspect, channel),
                        AuthorizeResultError,
                    )
                    assertEquals(
                        error.name,
                        'AuthorizeResultError',
                        'an `onError` log line names the error, not a bare `Error`',
                    )
                    assert(
                        error.message.includes(`returned ${label} for`),
                        `the message names the type label ${label}. Got: ${error.message}`,
                    )

                    assertEquals(
                        membersOn(m, channel),
                        membersBefore,
                        'no membership was written for the suspect',
                    )
                    if (channel === PRESENCE) {
                        assertEquals(
                            await rosterIds(backend.driver, channel),
                            [OBSERVER],
                            'no roster entry was written for the suspect',
                        )
                    }
                    if (
                        channel === PRESENCE && publishesBefore !== undefined
                    ) {
                        // Presence only — see the fileoverview: a private
                        // admission publishes nothing, so this zero would be
                        // vacuous there. And only with a control plane: with
                        // none, `undefined === undefined` could never fail.
                        assertEquals(
                            backend.controlPublishes(),
                            publishesBefore,
                            'no control frame was published',
                        )
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
                        'and the refused connection receives nothing',
                    )
                } finally {
                    await backend.close()
                }
            })
        }
    }
}

Deno.test('#347 fake Redis: an admitted private subscribe publishes no control frame, so a private refusal row does not read that counter', async () => {
    // The reason the refusal matrix asserts the control-publish zero on
    // presence rows only (#351). If a private admission ever starts
    // publishing, this fails — and the private rows should assert the counter
    // again, because it would then be able to move.
    const [, makeBackend] = BACKENDS[1]
    const backend = makeBackend()
    try {
        const m = new ChannelManager<User>({
            driver: backend.driver,
            authorize: suspectAuthorizer(() => true),
        })
        const before = backend.controlPublishes() ?? 0
        const c1 = conn('c1', SUSPECT)
        m.register(c1)
        assertEquals((await m.subscribe(c1, PRIVATE)).ok, true)
        const c2 = conn('c2', OBSERVER)
        m.register(c2)
        assertEquals(
            (await m.subscribe(c2, PRIVATE)).ok,
            true,
        )
        assertEquals(backend.controlPublishes(), before)
        // CONTROL: the same instrument, on the same backend, moves for a
        // presence join — the zero above is the private path's, not the
        // counter's.
        const c3 = conn('c3', OBSERVER)
        m.register(c3)
        assertEquals(
            (await m.subscribe(c3, PRESENCE)).ok,
            true,
        )
        assertEquals(backend.controlPublishes(), before + 1)
    } finally {
        await backend.close()
    }
})

// --- What admits, and what denies ------------------------------------------

Deno.test('#347 private `true` and a PresenceMember both admit', async () => {
    for (const value of [true, { id: 'u1' }]) {
        const m = new ChannelManager<User>({
            driver: new MemoryBroadcastDriver(),
            authorize: () => value,
        })
        const c = conn('c1', SUSPECT)
        m.register(c)
        assertEquals((await m.subscribe(c, PRIVATE)).ok, true)
        m.broadcast(PRIVATE, 'evt', { n: 1 })
        await settle()
        assertEquals(c.received.length, 1, 'and the admitted one is delivered')
    }
})

Deno.test('#347 presence `true` joins as the connection id', async () => {
    const driver = new MemoryBroadcastDriver()
    const m = new ChannelManager<User>({ driver, authorize: () => true })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    const result = await m.subscribe(c1, PRESENCE)
    assertEquals(result.ok, true)
    assertEquals(await rosterIds(driver, PRESENCE), ['c1'])
})

Deno.test('#347 presence `{ id, info }` joins as returned', async () => {
    const driver = new MemoryBroadcastDriver()
    const member: PresenceMember = { id: 7, info: { name: 'Ada' } }
    const m = new ChannelManager<User>({ driver, authorize: () => member })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    assertEquals((await m.subscribe(c1, PRESENCE)).ok, true)
    assertEquals(
        (await driver.readRoster(PRESENCE, 1_000, [])).members,
        [member],
    )
})

Deno.test('#347 presence: a class-instance member admits — the rule is "an object", not "a plain object"', async () => {
    class Member implements PresenceMember {
        constructor(readonly id: number) {}
    }
    const driver = new MemoryBroadcastDriver()
    const m = new ChannelManager<User>({
        driver,
        authorize: () => new Member(9),
    })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    assertEquals((await m.subscribe(c1, PRESENCE)).ok, true)
    assertEquals(await rosterIds(driver, PRESENCE), [9])
})

Deno.test('#347 `false` still denies with { ok: false }, on both kinds', async () => {
    for (const channel of [PRIVATE, PRESENCE]) {
        const driver = new MemoryBroadcastDriver()
        const m = new ChannelManager<User>({ driver, authorize: () => false })
        const c1 = conn('c1', SUSPECT)
        m.register(c1)
        assertEquals((await m.subscribe(c1, channel)).ok, false)
        assertEquals(
            m.connectionCount,
            1,
            'only the registration is counted; the denial bound nothing more',
        )
    }
})

Deno.test('#347 no authorizer still denies with { ok: false }', async () => {
    const m = new ChannelManager<User>({ driver: new MemoryBroadcastDriver() })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    for (const channel of [PRIVATE, PRESENCE]) {
        assertEquals(
            (await m.subscribe(c1, channel)).ok,
            false,
        )
    }
})

Deno.test('#347 a public channel never calls the authorizer, whatever it would return', async () => {
    let calls = 0
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: (() => {
            calls++
            return undefined
        }) as unknown as Authorizer<User>,
    })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    assertEquals((await m.subscribe(c1, 'news')).ok, true)
    assertEquals(calls, 0)
})

// --- Ordering, #331, and the message ----------------------------------------

Deno.test('#347 the result is classified BEFORE the caps: a full connection gets AuthorizeResultError, not ChannelLimitError', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        maxChannelsPerConnection: 1,
        authorize: suspectAuthorizer(() => undefined),
    })
    const c = conn('c1', SUSPECT)
    m.register(c)
    assertEquals((await m.subscribe(c, 'news')).ok, true)
    // CONTROL: the cap really is full for this connection — a public join,
    // which runs no authorizer, is refused by the cap.
    await assertRejects(() => m.subscribe(c, 'weather'), ChannelLimitError)
    const error = await assertRejects(
        () => m.subscribe(c, PRIVATE),
        Error,
    )
    assertInstanceOf(
        error,
        AuthorizeResultError,
        `an invalid result is refused on its own terms first. Got: ${error.name}`,
    )
})

Deno.test('#347 a held presence subscription whose authorizer flips to undefined throws AuthorizeResultError and keeps its entry and delivery (#331)', async () => {
    const driver = new MemoryBroadcastDriver()
    const state = { broken: false }
    const m = new ChannelManager<User>({
        driver,
        authorize: ((identity: User | null) =>
            state.broken
                ? undefined
                : { id: identity?.id }) as unknown as Authorizer<User>,
    })
    const c = conn('c1', SUSPECT)
    m.register(c)
    assertEquals((await m.subscribe(c, PRESENCE)).ok, true)

    state.broken = true
    await assertRejects(() => m.subscribe(c, PRESENCE), AuthorizeResultError)

    assertEquals(m.connectionCount, 1, 'the connection is still tracked')
    assertEquals(
        await rosterIds(driver, PRESENCE),
        [SUSPECT],
        'the roster entry stands — an invalid result removes nothing',
    )
    const before = c.received.length
    m.broadcast(PRESENCE, 'evt', { n: 1 })
    await settle()
    assertEquals(c.received.length, before + 1, 'and delivery continues')
})

Deno.test('#347 the message names the type and the channel, and never echoes the value', async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: suspectAuthorizer(() => 'sentinel-XYZ'),
    })
    const c1 = conn('c1', SUSPECT)
    m.register(c1)
    const error = await assertRejects(
        () => m.subscribe(c1, PRIVATE),
        AuthorizeResultError,
    )
    assert(
        error.message.includes('returned string for'),
        `the type label is named. Got: ${error.message}`,
    )
    assert(error.message.includes(PRIVATE), 'the channel is named')
    assert(error.message.includes('?? false'), 'and the fix is named')
    assert(
        !error.message.includes('sentinel-XYZ'),
        'the value is application data and this message reaches logs',
    )
    assertEquals(error.name, 'AuthorizeResultError')
})

// --- The classifier, directly ----------------------------------------------

Deno.test('#347 classifyAuthorizeResult: the three verdicts', () => {
    const member = { id: 3 }
    assertEquals(classifyAuthorizeResult(false), { verdict: 'deny' })
    assertEquals(classifyAuthorizeResult(true), {
        verdict: 'admit',
        member: undefined,
    })
    assertEquals(classifyAuthorizeResult(member), { verdict: 'admit', member })
    for (const [, value, label] of REFUSED) {
        assertEquals(classifyAuthorizeResult(value()), {
            verdict: 'invalid',
            type: label,
        })
    }
    for (
        const [value, label] of [
            [new Number(1), 'boxed number'],
            [new String('x'), 'boxed string'],
            [Object(1n), 'boxed bigint'],
            [Object(Symbol('s')), 'boxed symbol'],
            [1n, 'bigint'],
            [() => true, 'function'],
        ] as const
    ) {
        assertEquals(classifyAuthorizeResult(value), {
            verdict: 'invalid',
            type: label,
        })
    }
})
