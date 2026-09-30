/**
 * @fileoverview #357 — an object result admits only as a `PresenceMember`, on
 * every channel kind.
 *
 * #347 made the authorizer's contract three values: `true`, `false` or a
 * `PresenceMember`. On a PRESENCE channel the object has always been checked —
 * #350's `admitPresenceMember` parses it into exactly `{ id, info? }` or
 * throws. On a PRIVATE channel nothing read it: any non-null, non-array object
 * admitted, purely for being an object. A lookup that FOUND NOTHING routinely
 * is one — Deno KV's `{ key, value: null, versionstamp: null }`, a pg
 * `QueryResult` with `rows: []`, `{}`, a `Response` — so an untyped authorizer
 * returning its lookup instead of a boolean let every authenticated caller
 * read someone else's private channel.
 *
 * The fix gives the rule one home: the object goes through
 * `admitPresenceMember` whether the channel is private or presence, and the
 * kind decides only whether the admitted member is seated. The invariant this
 * suite pins (b): **for every authorizer result, `subscribe` gives the same
 * outcome on `private-X` as on `presence-X`** — `ok`, or the name of the error
 * it throws. `true` is the one value whose EFFECT differs (presence seats it
 * as the connection id); its outcome is `ok` on both.
 *
 * Every refusal row reads state, not only the thrown class: the membership
 * with a positive control that moves it (#351), and delivery with an observer
 * admitted by `true` that does receive the broadcast — on a real
 * {@link MemoryBroadcastDriver} and a {@link RedisBroadcastDriver} over the
 * fake broker.
 *
 * @module @lockness/realtime/tests/authorize_result_357
 */

import {
    assert,
    assertEquals,
    assertInstanceOf,
    assertRejects,
} from '@std/assert'
import {
    type Authorizer,
    ChannelLimitError,
    ChannelManager,
    decodeClientMessage,
    encodeServerMessage,
    MemoryBroadcastDriver,
    type PresenceMember,
    PresenceMemberIdError,
    PresenceMemberShapeError,
    PresenceMemberSizeError,
    RedisBroadcastDriver,
} from '../mod.ts'
import type { BroadcastDriver } from '../driver.ts'
import { buildEvents } from '../websocket.ts'
import type { Connection, WebSocketHooks, WSContext } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

interface User {
    id: number
}

const PRIVATE = 'private-orders'
const PRESENCE = 'presence-orders'
const PREFIX = 'app:rt'

/** The identity whose authorizer answer is the value under test. */
const SUSPECT = 1
/** The identity of the observer admitted with `true` — the delivery control. */
const OBSERVER = 2

/** The hint every member error carries (#357) — its literal text. */
const HINT =
    'On a `private-*` channel no member is used: return `true` to admit (#357).'

// A REAL Deno KV, not a hand-written lookalike: the miss wrapper is whatever
// `kv.get` actually resolves to, so the rows follow the runtime if it changes.
const kv = await Deno.openKv(':memory:')
const KV_MISS: unknown = await kv.get(['member', PRIVATE, SUSPECT])
const KV_SENTINEL_MISS: unknown = await kv.get(['sentinel-XYZ'])
await kv.set(['member', PRIVATE, OBSERVER], { role: 'owner' })
const KV_HIT: unknown = await kv.get(['member', PRIVATE, OBSERVER])
kv.close()

/** The shape of node-postgres's `Result`: a class instance, empty on a miss. */
class PgResult {
    command = 'SELECT'
    rowCount = 0
    oid = null
    rows: unknown[] = []
    fields: unknown[] = []
}

/** A class-instance member — legitimate: the rule is "a member", not "plain". */
class Member implements PresenceMember {
    constructor(readonly id: number) {}
}

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

/** One backend under test. */
interface Backend {
    driver: BroadcastDriver
    /** The channel's authoritative roster, as a reader of the driver sees it. */
    roster(channel: string): Promise<readonly PresenceMember[]>
    /**
     * Control-plane publishes so far — `undefined` on a driver with no control
     * plane, where there is nothing to count.
     */
    controlPublishes(): number | undefined
    close(): Promise<void>
}

/** The #347 backends: memory, and Redis over the fake broker. */
const BACKENDS: ReadonlyArray<readonly [string, () => Backend]> = [
    ['memory', () => {
        const driver = new MemoryBroadcastDriver()
        return {
            driver,
            roster: (channel) =>
                Promise.resolve(driver.readRoster(channel, 1_000, []).members),
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
        let publishes = 0
        const publishControl = driver.publishControl.bind(driver)
        driver.publishControl = (control) => {
            publishes++
            return publishControl(control)
        }
        return {
            driver,
            roster: async (channel) =>
                (await driver.readRoster(channel, 1_000, [])).members,
            controlPublishes: () => publishes,
            close: async () => {
                await driver.close()
                redis.assertNoRejections()
            },
        }
    }],
]

/**
 * An authorizer that answers `value()` for the suspect and `true` for everyone
 * else, cast the way a plain-JS app or an `any`-typed lookup reaches the
 * manager.
 */
function suspectAuthorizer(value: () => unknown): Authorizer<User> {
    return ((identity: User | null) =>
        identity?.id === SUSPECT ? value() : true) as unknown as Authorizer<
            User
        >
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

/** `ok`, `denied`, or the name of what `subscribe` threw. */
async function outcome(
    manager: ChannelManager<User>,
    connection: Connection<User>,
    channel: string,
): Promise<string> {
    try {
        return (await manager.subscribe(connection, channel)).ok
            ? 'ok'
            : 'denied'
    } catch (error) {
        return error instanceof Error ? error.name : typeof error
    }
}

// --- (a) Every refused object, on a private channel -------------------------

/**
 * Each refused value, the error class it must raise, and the keys named. The
 * miss wrappers first, then every other object (b) refuses — a refusal is read
 * for what it left behind, not only for what it threw.
 */
const REFUSED: ReadonlyArray<
    readonly [
        string,
        () => unknown,
        | typeof PresenceMemberShapeError
        | typeof PresenceMemberIdError
        | typeof PresenceMemberSizeError,
        readonly string[],
    ]
> = [
    [
        'a Deno KV miss entry',
        () => KV_MISS,
        PresenceMemberShapeError,
        ['key', 'value', 'versionstamp'],
    ],
    [
        'a QueryResult-shaped { rows: [], rowCount: 0 }',
        () => ({ rows: [], rowCount: 0 }),
        PresenceMemberShapeError,
        ['rows', 'rowCount'],
    ],
    [
        'a pg-shaped Result class instance',
        () => new PgResult(),
        PresenceMemberShapeError,
        ['command', 'rowCount', 'oid'],
    ],
    ['{}', () => ({}), PresenceMemberIdError, []],
    [
        'a 404 Response',
        () => new Response(null, { status: 404 }),
        PresenceMemberIdError,
        [],
    ],
    [
        'a found raw row { id, email }',
        () => ({ id: SUSPECT, email: 'a@b.c' }),
        PresenceMemberShapeError,
        ['email'],
    ],
    [
        'the identity object { id, name, roles }',
        () => ({ id: SUSPECT, name: 'Ada', roles: ['admin'] }),
        PresenceMemberShapeError,
        ['name', 'roles'],
    ],
    [
        'a Deno KV hit entry',
        () => KV_HIT,
        PresenceMemberShapeError,
        ['key', 'value', 'versionstamp'],
    ],
    ['a Date', () => new Date(0), PresenceMemberIdError, []],
    [
        'Object.create(null)',
        () => Object.create(null),
        PresenceMemberIdError,
        [],
    ],
    ['{ id: null }', () => ({ id: null }), PresenceMemberIdError, []],
    [
        'a 5 KiB member',
        () => ({ id: SUSPECT, info: { pad: 'x'.repeat(5 * 1024) } }),
        PresenceMemberSizeError,
        [],
    ],
]

for (const [backendName, makeBackend] of BACKENDS) {
    for (const [valueName, value, errorClass, keys] of REFUSED) {
        Deno.test(`#357 (a) ${backendName} ${PRIVATE}: ${valueName} throws ${errorClass.name} — no connection, no delivery`, async () => {
            const backend = makeBackend()
            try {
                const m = new ChannelManager<User>({
                    driver: backend.driver,
                    authorize: suspectAuthorizer(value),
                })
                // Both sockets are registered at open (#370). The refusal is
                // read on the channel's membership, which only a subscribe
                // moves.
                const observer = conn('observer', OBSERVER)
                m.register(observer)
                const suspect = conn('suspect', SUSPECT)
                m.register(suspect)
                const atStart = membersOn(m, PRIVATE)
                assertEquals((await m.subscribe(observer, PRIVATE)).ok, true)
                const before = membersOn(m, PRIVATE)
                assertEquals(
                    before,
                    atStart + 1,
                    'CONTROL: an admitted subscribe moves the membership',
                )

                const error = await assertRejects(
                    () => m.subscribe(suspect, PRIVATE),
                    Error,
                )
                assert(
                    error instanceof errorClass,
                    `a ${errorClass.name}. Got: ${error.name}: ${error.message}`,
                )
                for (const key of keys) {
                    assert(
                        error.message.includes(`"${key}"`),
                        `the message names the key ${key}. Got: ${error.message}`,
                    )
                }
                assertEquals(
                    membersOn(m, PRIVATE),
                    before,
                    'no membership was written for the suspect',
                )

                const observerFrames = observer.received.length
                m.broadcast(PRIVATE, 'secret', { n: 1 })
                await settle()
                assert(
                    observer.received.length > observerFrames,
                    'CONTROL: the observer admitted with `true` receives it',
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

// --- (b) Kind independence ---------------------------------------------------

/**
 * Probe 3's nineteen values, each with the outcome it gets on a PRESENCE
 * channel — the kind whose rule has always been checked. The row asserts that
 * outcome first, so the equality below cannot pass on two identical accidents.
 */
const KIND_TABLE: ReadonlyArray<readonly [string, () => unknown, string]> = [
    ['a Deno KV miss entry', () => KV_MISS, 'PresenceMemberShapeError'],
    ['a Deno KV hit entry', () => KV_HIT, 'PresenceMemberShapeError'],
    ['a pg-shaped Result', () => new PgResult(), 'PresenceMemberShapeError'],
    [
        '{ rows: [], rowCount: 0 }',
        () => ({ rows: [], rowCount: 0 }),
        'PresenceMemberShapeError',
    ],
    ['{}', () => ({}), 'PresenceMemberIdError'],
    ['a Date', () => new Date(0), 'PresenceMemberIdError'],
    [
        'a 404 Response',
        () => new Response(null, { status: 404 }),
        'PresenceMemberIdError',
    ],
    ['Object.create(null)', () => Object.create(null), 'PresenceMemberIdError'],
    [
        'a raw row',
        () => ({ id: SUSPECT, email: 'a@b.c' }),
        'PresenceMemberShapeError',
    ],
    ['{ id: null }', () => ({ id: null }), 'PresenceMemberIdError'],
    [
        '{ id, info: Date }',
        () => ({ id: SUSPECT, info: new Date(0) }),
        'PresenceMemberShapeError',
    ],
    [
        'a 5 KiB member',
        () => ({ id: SUSPECT, info: { pad: 'x'.repeat(5 * 1024) } }),
        'PresenceMemberSizeError',
    ],
    ['{ id }', () => ({ id: 'u1' }), 'ok'],
    ['{ id, info }', () => ({ id: 7, info: { n: 1 } }), 'ok'],
    ['a class-instance member', () => new Member(9), 'ok'],
    ['false', () => false, 'denied'],
    ['undefined', () => undefined, 'AuthorizeResultError'],
    ['[]', () => [], 'AuthorizeResultError'],
    ['true', () => true, 'ok'],
]

for (const [backendName, makeBackend] of BACKENDS) {
    for (const [valueName, value, expected] of KIND_TABLE) {
        Deno.test(`#357 (b) ${backendName}: ${valueName} gets the same outcome on private and presence (${expected})`, async () => {
            const backend = makeBackend()
            try {
                const m = new ChannelManager<User>({
                    driver: backend.driver,
                    authorize: (() => value()) as unknown as Authorizer<User>,
                })
                const presenceConn = conn('on-presence', SUSPECT)
                m.register(presenceConn)
                const privateConn = conn('on-private', SUSPECT)
                m.register(privateConn)
                const onPresence = await outcome(m, presenceConn, PRESENCE)
                const onPrivate = await outcome(m, privateConn, PRIVATE)
                assertEquals(
                    onPresence,
                    expected,
                    'CONTROL: the presence outcome is the one the table names',
                )
                assertEquals(
                    onPrivate,
                    onPresence,
                    'the channel kind does not change the outcome',
                )
            } finally {
                await backend.close()
            }
        })
    }
}

// --- (c) Negative controls: what still admits on a private channel ----------

/** The shared authorizer five existing tests use across both kinds. */
const shared = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

const ADMITTED: ReadonlyArray<readonly [string, Authorizer<User>]> = [
    ['true', () => true],
    ["{ id: 'u1' }", () => ({ id: 'u1' })],
    ['{ id: 7, info: { n: 1 } }', () => ({ id: 7, info: { n: 1 } })],
    ['a class-instance member', () => new Member(9)],
    [
        'a prototype-less member',
        () =>
            Object.assign(Object.create(null), { id: 'u1' }) as PresenceMember,
    ],
    ['the shared `identity ? { id } : false` authorizer', shared],
]

for (const [backendName, makeBackend] of BACKENDS) {
    for (const [valueName, authorize] of ADMITTED) {
        Deno.test(`#357 (c) ${backendName} ${PRIVATE}: ${valueName} admits, the broadcast is delivered, and the member is discarded`, async () => {
            const backend = makeBackend()
            try {
                const m = new ChannelManager<User>({
                    driver: backend.driver,
                    authorize,
                })
                const c = conn('c1', SUSPECT)
                m.register(c)
                const publishesBefore = backend.controlPublishes()
                const result = await m.subscribe(c, PRIVATE)
                await settle()
                assertEquals(result.ok, true)
                assertEquals(m.connectionCount, 1)
                // DISCARDED, not seated: the admission read the member and a
                // private channel keeps nothing of it.
                assertEquals(result.here, undefined, 'no `here` snapshot')
                assertEquals(
                    await backend.roster(PRIVATE),
                    [],
                    'no roster entry for the private channel',
                )
                assertEquals(
                    backend.controlPublishes(),
                    publishesBefore,
                    'no `presence-join` on the control plane',
                )
                m.broadcast(PRIVATE, 'evt', { n: 1 })
                await settle()
                assertEquals(c.received.length, 1, 'the admitted one hears it')

                // CONTROL, on the same three instruments: the same authorizer
                // on a PRESENCE channel seats its member, and each reading
                // moves — so each zero above is a reading, not a blind spot.
                const p1 = conn('p1', SUSPECT)
                m.register(p1)
                const seated = await m.subscribe(p1, PRESENCE)
                await settle()
                assert(
                    seated.here !== undefined,
                    'CONTROL: a presence admission returns `here`',
                )
                assertEquals(
                    (await backend.roster(PRESENCE)).length,
                    1,
                    'CONTROL: the roster read sees a seated member',
                )
                if (publishesBefore !== undefined) {
                    assertEquals(
                        backend.controlPublishes(),
                        publishesBefore + 1,
                        'CONTROL: a presence join publishes once',
                    )
                }
            } finally {
                await backend.close()
            }
        })
    }
}

// --- (d) Ordering, and #331 --------------------------------------------------

for (const [backendName, makeBackend] of BACKENDS) {
    Deno.test(`#357 (d) ${backendName}: a full connection whose private result is a KV miss hears PresenceMemberShapeError, not ChannelLimitError, and nothing is written`, async () => {
        const backend = makeBackend()
        try {
            const m = new ChannelManager<User>({
                driver: backend.driver,
                maxChannelsPerConnection: 1,
                authorize: suspectAuthorizer(() => KV_MISS),
            })
            const c = conn('c1', SUSPECT)
            m.register(c)
            assertEquals((await m.subscribe(c, 'news')).ok, true)
            // CONTROL: the cap really is full for this connection.
            await assertRejects(
                () => m.subscribe(c, 'weather'),
                ChannelLimitError,
            )
            const error = await assertRejects(
                () => m.subscribe(c, PRIVATE),
                Error,
            )
            assertInstanceOf(
                error,
                PresenceMemberShapeError,
                `the result is refused on its own terms first. Got: ${error.name}`,
            )

            const observer = conn('observer', OBSERVER)
            m.register(observer)
            assertEquals((await m.subscribe(observer, PRIVATE)).ok, true)
            const before = c.received.length
            m.broadcast(PRIVATE, 'secret', { n: 1 })
            await settle()
            assertEquals(observer.received.length, 1, 'CONTROL: delivered')
            assertEquals(
                c.received.length,
                before,
                'the refused connection was never subscribed',
            )
        } finally {
            await backend.close()
        }
    })

    Deno.test(`#357 (d) ${backendName}: a held private subscription whose authorizer flips to a KV miss throws, and keeps its subscription and delivery (#331)`, async () => {
        const backend = makeBackend()
        try {
            const state = { broken: false }
            const m = new ChannelManager<User>({
                driver: backend.driver,
                authorize: (() =>
                    state.broken ? KV_MISS : true) as unknown as Authorizer<
                        User
                    >,
            })
            const c = conn('c1', SUSPECT)
            m.register(c)
            assertEquals((await m.subscribe(c, PRIVATE)).ok, true)

            state.broken = true
            await assertRejects(
                () => m.subscribe(c, PRIVATE),
                PresenceMemberShapeError,
            )

            assertEquals(m.connectionCount, 1, 'the connection is still held')
            const before = c.received.length
            m.broadcast(PRIVATE, 'evt', { n: 1 })
            await settle()
            assertEquals(c.received.length, before + 1, 'delivery continues')
        } finally {
            await backend.close()
        }
    })
}

// --- (e) One read, one rule --------------------------------------------------

for (const [backendName, makeBackend] of BACKENDS) {
    Deno.test(`#357 (e) ${backendName} ${PRIVATE}: a counting Proxy result sees ownKeys once, get id once and get info once`, async () => {
        const backend = makeBackend()
        try {
            const counts = { ownKeys: 0, id: 0, info: 0 }
            const target = { id: 7, info: { n: 1 } }
            const proxy = new Proxy(target, {
                ownKeys(t) {
                    counts.ownKeys++
                    return Reflect.ownKeys(t)
                },
                get(t, key, receiver) {
                    if (key === 'id') counts.id++
                    if (key === 'info') counts.info++
                    return Reflect.get(t, key, receiver)
                },
            })
            const m = new ChannelManager<User>({
                driver: backend.driver,
                authorize: () => proxy,
            })
            const c = conn('c1', SUSPECT)
            m.register(c)
            assertEquals((await m.subscribe(c, PRIVATE)).ok, true)
            assertEquals(counts, { ownKeys: 1, id: 1, info: 1 })
        } finally {
            await backend.close()
        }
    })

    Deno.test(`#357 (e) ${backendName} ${PRIVATE}: { id: 7, info: new Date(0) } throws PresenceMemberShapeError naming string — the PARSED check, not an in-memory one`, async () => {
        const backend = makeBackend()
        try {
            const m = new ChannelManager<User>({
                driver: backend.driver,
                authorize: () =>
                    ({ id: 7, info: new Date(0) }) as unknown as PresenceMember,
            })
            const c1 = conn('c1', SUSPECT)
            m.register(c1)
            const error = await assertRejects(
                () => m.subscribe(c1, PRIVATE),
                PresenceMemberShapeError,
            )
            assert(
                error.message.includes('of type string'),
                `the parsed info's type is named. Got: ${error.message}`,
            )
            assertEquals(
                m.connectionCount,
                1,
                'only the registration is counted; the refusal bound nothing',
            )
        } finally {
            await backend.close()
        }
    })
}

// --- (f) No echo, and the hint ----------------------------------------------

Deno.test(`#357 (f) ${PRIVATE}: a KV miss names \`key\`, never echoes its content, and carries the private-channel hint`, async () => {
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: suspectAuthorizer(() => KV_SENTINEL_MISS),
    })
    const c = conn('c1', SUSPECT)
    m.register(c)
    const error = await assertRejects(
        () => m.subscribe(c, PRIVATE),
        PresenceMemberShapeError,
    )
    assert(error.message.includes('"key"'), `names key. Got: ${error.message}`)
    assert(
        !error.message.includes('sentinel-XYZ'),
        'the key content is application data and this message reaches logs',
    )
    assert(
        error.message.endsWith(` ${HINT}`),
        `the hint closes the message. Got: ${error.message}`,
    )
})

Deno.test('#357 (f) every member error carries the private-channel hint', () => {
    const errors: Error[] = [
        new PresenceMemberShapeError({ extraKeys: ['rows'] }),
        new PresenceMemberShapeError({ infoType: 'string' }),
        new PresenceMemberShapeError({ droppedInfoType: 'function' }),
        new PresenceMemberIdError(undefined),
        new PresenceMemberSizeError(5_000, 4_096),
    ]
    for (const error of errors) {
        assert(
            error.message.endsWith(` ${HINT}`),
            `${error.name} carries the hint. Got: ${error.message}`,
        )
    }
})

// --- (g) Public channels are unchanged ---------------------------------------

Deno.test('#357 (g) a public channel admits without calling the authorizer, even one that would return a KV miss', async () => {
    let calls = 0
    const m = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: (() => {
            calls++
            return KV_MISS
        }) as unknown as Authorizer<User>,
    })
    const c = conn('c1', SUSPECT)
    m.register(c)
    assertEquals((await m.subscribe(c, 'news')).ok, true)
    assertEquals(calls, 0)
})

// --- (h) WebSocket routing (the #352 harness) --------------------------------

Deno.test(`#357 (h) ${PRIVATE}: a KV-miss result reaches onError as PresenceMemberShapeError, sends nothing and keeps the socket open`, async () => {
    const manager = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: (() => KV_MISS) as unknown as Authorizer<User>,
    })
    const sent: string[] = []
    const closes: unknown[] = []
    const context = {
        send: (data: string) => void sent.push(data),
        close: (code?: number) => void closes.push(code),
    } as unknown as WSContext
    const errors: unknown[] = []
    const onMessage: NonNullable<WebSocketHooks<User>['onMessage']> = async (
        c,
        data,
    ) => {
        const message = decodeClientMessage(data as string)
        if (message.type !== 'subscribe') return
        const result = await manager.subscribe(c, message.channel)
        c.send(encodeServerMessage(
            result.ok
                ? { type: 'subscribed', channel: message.channel }
                : { type: 'error', message: 'forbidden' },
        ))
    }
    const events = buildEvents<User>(
        manager.handlerHooks({
            onMessage,
            onError: (_c, error) => void errors.push(error),
        }),
        { id: SUSPECT },
    )
    events.onOpen?.(new Event('open'), context)
    const subscribe = (channel: string) =>
        events.onMessage?.(
            {
                data: JSON.stringify({ type: 'subscribe', channel }),
            } as MessageEvent,
            context,
        )

    subscribe(PRIVATE)
    await settle()
    assertEquals(errors.length, 1, 'onError is called exactly once')
    assertInstanceOf(errors[0], PresenceMemberShapeError)
    assertEquals(sent, [], 'nothing is sent to the client')
    assertEquals(closes, [], 'the socket is not closed')

    // CONTROL: the socket still serves a valid frame.
    subscribe('news')
    await settle()
    assertEquals(
        sent.map((frame) => JSON.parse(frame)),
        [{ type: 'subscribed', channel: 'news' }],
    )
    assertEquals(errors.length, 1)
})
