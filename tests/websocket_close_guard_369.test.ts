/**
 * @fileoverview #369 — a rejection on the WebSocket close path is reported,
 * never fatal, and neither is a rejection from the sink that reports it.
 *
 * `buildEvents` runs the application's `onOpen` and `onMessage` through
 * `guard()`, which hands any throw to `reportError()`: the app's `onError`, or
 * the default `realtime: unhandled websocket error:` line. `onClose` skipped
 * it — `void hooks.onClose?.(…)` — and the package registers no
 * `unhandledrejection` listener, so on Deno, which terminates the process on an
 * unhandled rejection, one rejected close took down the server and every other
 * socket on it. `ChannelManager.handlerHooks().onClose` awaits `disconnect`,
 * which RE-THROWS its first teardown failure by contract, so a broker blip on
 * the last local member's unwatch was enough.
 *
 * The same held one level down: `guard()` awaited `reportError()` unguarded,
 * so an application `onError` that itself threw or rejected escaped from every
 * path the sink serves.
 *
 * Every row watches for an escape under the shared `watchingEscapes` (#374),
 * which records the reason and `preventDefault()`s it — so a regression FAILS
 * the row rather than killing the test runner — and is removed in a `finally`.
 *
 * @module @lockness/realtime/tests/websocket_close_guard_369
 */

import { assert, assertEquals, assertStrictEquals } from '@std/assert'
import {
    type BroadcastDriver,
    ChannelManager,
    decodeClientMessage,
    encodeServerMessage,
    MemoryBroadcastDriver,
} from '../mod.ts'
import { buildEvents } from '../websocket.ts'
import type { Connection, WebSocketHooks, WSContext } from '../types.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'

interface User {
    id: number
}

const PUBLIC = 'news'
const DEFAULT_LINE = 'realtime: unhandled websocket error: '

/**
 * The words of the WARN `handlerHooks` logs when the teardown fails after
 * the application's own `onClose` already threw (#379's combined witness).
 */
const CLOSE_DISCONNECT_FAILED = "after the application's onClose threw"

/** A fake socket recording every frame sent and every close. */
function fakeSocket() {
    const sent: string[] = []
    const closes: { code?: number; reason?: string }[] = []
    const methods = {
        send(data: string | ArrayBuffer | Uint8Array) {
            sent.push(data as string)
        },
        close(code?: number, reason?: string) {
            closes.push({ code, reason })
        },
    } satisfies Pick<WSContext, 'send' | 'close'>
    return { sent, closes, ...methods }
}

/** Capture `console.error` lines for the duration of `body`. */
async function capturingErrors(
    body: (lines: string[]) => Promise<void>,
): Promise<void> {
    const lines: string[] = []
    const original = console.error
    console.error = (...args: unknown[]) =>
        void lines.push(args.map(String).join(' '))
    try {
        await body(lines)
    } finally {
        console.error = original
    }
}

/** Capture `console.warn` lines for the duration of `body`. */
async function capturingWarnings(
    body: (lines: string[]) => Promise<void>,
): Promise<void> {
    const lines: string[] = []
    const original = console.warn
    console.warn = (...args: unknown[]) =>
        void lines.push(args.map(String).join(' '))
    try {
        await body(lines)
    } finally {
        console.warn = original
    }
}

/**
 * A channel-watching driver whose `unwatchChannel` REJECTS — the broker blip
 * #369 names. Delivery goes through a real in-process driver so the control
 * can prove a later subscriber is still served.
 */
function unwatchFailingDriver(): {
    driver: BroadcastDriver
    unwatches: string[]
} {
    const memory = new MemoryBroadcastDriver()
    const unwatches: string[] = []
    const driver: BroadcastDriver = {
        publish: (message) => memory.publish(message),
        onMessage: (handler) => memory.onMessage(handler),
        watchChannel: () => Promise.resolve(),
        unwatchChannel: (channel) => {
            unwatches.push(channel)
            return Promise.reject(new Error('broker unwatch failed'))
        },
    }
    return { driver, unwatches }
}

/** The application's `onMessage`, as `docs/realtime.md` writes it. */
function appOnMessage(
    manager: ChannelManager<User>,
): NonNullable<WebSocketHooks<User>['onMessage']> {
    return async (conn: Connection<User>, data: unknown) => {
        const message = decodeClientMessage(data as string)
        if (message.type !== 'subscribe') return
        const result = await manager.subscribe(conn, message.channel)
        conn.send(encodeServerMessage(
            result.ok
                ? { type: 'subscribed', channel: message.channel }
                : { type: 'error', message: 'forbidden' },
        ))
    }
}

/** Open a socket through `buildEvents` and return its drivers. */
function open(hooks: WebSocketHooks<User>) {
    const ws = fakeSocket()
    const context = ws as unknown as WSContext
    const events = buildEvents<User>(hooks, { id: 1 })
    events.onOpen?.(new Event('open'), context)
    return {
        ws,
        subscribe: (channel: string) =>
            events.onMessage?.(
                {
                    data: JSON.stringify({ type: 'subscribe', channel }),
                } as MessageEvent,
                context,
            ),
        message: (data: string) =>
            events.onMessage?.({ data } as MessageEvent, context),
        close: (code = 1000, reason = 'bye') =>
            events.onClose?.(
                new CloseEvent('close', { code, reason }),
                context,
            ),
        transportError: () =>
            events.onError?.(
                new ErrorEvent('error', { message: 'reset' }),
                context,
            ),
    }
}

function manager(driver: BroadcastDriver): ChannelManager<User> {
    return new ChannelManager<User>({ driver, authorize: () => true })
}

/**
 * The CONTROL: after the rejected close, the same manager still admits a new
 * socket, replies to its subscribe and delivers to it — the process, and the
 * manager in it, are alive.
 */
async function assertStillServing(
    channels: ChannelManager<User>,
    hooks: WebSocketHooks<User>,
): Promise<void> {
    const next = open(hooks)
    next.subscribe(PUBLIC)
    await settle()
    assertEquals(
        next.ws.sent.map((frame) => JSON.parse(frame)),
        [{ type: 'subscribed', channel: PUBLIC }],
        'CONTROL: a later subscribe is replied to',
    )
    channels.broadcast(PUBLIC, 'headline', { n: 1 })
    await settle()
    assertEquals(
        JSON.parse(next.ws.sent[1]),
        { type: 'event', channel: PUBLIC, event: 'headline', data: { n: 1 } },
        'CONTROL: and receives deliveries',
    )
}

// --- W1: handlerHooks, a disconnect that re-throws a teardown failure -------

Deno.test('#369 W1 handlerHooks: a rejected unwatch on the last close reaches onError exactly once, nothing escapes, and the manager still serves', async () => {
    await watchingEscapes(async (escaped) => {
        const { driver, unwatches } = unwatchFailingDriver()
        const channels = manager(driver)
        const errors: { conn: Connection<User>; error: unknown }[] = []
        const hooks = channels.handlerHooks({
            onMessage: appOnMessage(channels),
            onError: (conn, error) => void errors.push({ conn, error }),
        })
        const socket = open(hooks)
        socket.subscribe(PUBLIC)
        await settle()
        assertEquals(channels.connectionCount, 1, 'the open registered')

        socket.close()
        await settle()

        assertEquals(unwatches, [PUBLIC], 'the last member left: one unwatch')
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(errors.length, 1, 'onError is called exactly once')
        assertEquals(
            (errors[0].error as Error).message,
            'broker unwatch failed',
            'with the teardown failure disconnect re-threw',
        )
        assertEquals(
            channels.connectionCount,
            0,
            'the connection is still forgotten',
        )

        await assertStillServing(channels, hooks)
        assertEquals(escaped, [], 'and nothing escaped later either')
    })
})

Deno.test('#369 W1 handlerHooks: with no onError hook, a rejected unwatch on close is one default console.error line and nothing escapes', async () => {
    await watchingEscapes(async (escaped) => {
        await capturingErrors(async (lines) => {
            const { driver } = unwatchFailingDriver()
            const channels = manager(driver)
            const hooks = channels.handlerHooks({
                onMessage: appOnMessage(channels),
            })
            const socket = open(hooks)
            socket.subscribe(PUBLIC)
            await settle()

            socket.close()
            await settle()

            assertEquals(escaped, [], 'no rejection reaches the runtime')
            assertEquals(lines.length, 1, 'exactly one console.error line')
            assertEquals(
                lines[0],
                `${DEFAULT_LINE}Error: broker unwatch failed`,
                'the default line, rendered through renderError',
            )

            await assertStillServing(channels, hooks)
        })
    })
})

// --- W2: the application's own onClose --------------------------------------

const APP_CLOSE_FAILURES: [string, WebSocketHooks<User>['onClose']][] = [
    ['an async rejection', () => Promise.reject(new Error('app close failed'))],
    ['a synchronous throw', () => {
        throw new Error('app close failed')
    }],
]

for (const [shape, onClose] of APP_CLOSE_FAILURES) {
    Deno.test(`#369 W2 app onClose: ${shape} reaches onError exactly once with the connection and nothing escapes`, async () => {
        await watchingEscapes(async (escaped) => {
            await capturingErrors(async (lines) => {
                const errors: { conn: Connection<User>; error: unknown }[] = []
                let opened: Connection<User> | undefined
                const socket = open({
                    onOpen: (conn) => void (opened = conn),
                    onClose,
                    onError: (conn, error) => void errors.push({ conn, error }),
                })

                socket.close()
                await settle()

                assertEquals(escaped, [], 'no rejection reaches the runtime')
                assertEquals(errors.length, 1, 'onError is called exactly once')
                assert(opened !== undefined, 'onOpen ran')
                assertStrictEquals(
                    errors[0].conn,
                    opened,
                    'with the same connection the socket opened with',
                )
                assertEquals(
                    (errors[0].error as Error).message,
                    'app close failed',
                )
                assertEquals(
                    lines,
                    [],
                    'a working onError handled it: the default sink writes nothing',
                )
            })
        })
    })

    Deno.test(`#369 W2 app onClose: ${shape} with no onError hook is one default console.error line and nothing escapes`, async () => {
        await watchingEscapes(async (escaped) => {
            await capturingErrors(async (lines) => {
                const socket = open({ onClose })

                socket.close()
                await settle()

                assertEquals(escaped, [], 'no rejection reaches the runtime')
                assertEquals(lines, [`${DEFAULT_LINE}Error: app close failed`])
            })
        })
    })
}

// --- W3: the sink itself fails ----------------------------------------------

/**
 * The fallback line's fixed prefix, exactly as `docs/realtime.md` documents
 * it. The marker sits BEFORE any error text, so a client cannot forge it by
 * putting the same words in a frame.
 */
const FALLBACK_LINE =
    'realtime: unhandled websocket error (the onError hook failed too): '

const FAILING_SINKS: [string, WebSocketHooks<User>['onError']][] = [
    ['rejects', () => Promise.reject(new Error('sink failed'))],
    ['throws', () => {
        throw new Error('sink failed')
    }],
]

/**
 * Each path the sink serves, driven to a failure that reaches it, with the
 * rendered error it must report.
 */
const PATHS: [
    string,
    WebSocketHooks<User>,
    (s: ReturnType<typeof open>) => void,
    string,
][] = [
    [
        'onOpen',
        {
            onOpen: () => Promise.reject(new Error('hook failed')),
        },
        () => {},
        'Error: hook failed',
    ],
    [
        'onMessage',
        {
            onMessage: () => Promise.reject(new Error('hook failed')),
        },
        (s) => s.message('x'),
        'Error: hook failed',
    ],
    [
        'onClose',
        {
            onClose: () => Promise.reject(new Error('hook failed')),
        },
        (s) => s.close(),
        'Error: hook failed',
    ],
    [
        'the transport onError',
        {},
        (s) => s.transportError(),
        // The transport event rides as the cause, and `renderError` follows
        // the chain.
        'Error: websocket transport error: reset caused by: [object ErrorEvent]',
    ],
]

for (const [sinkShape, onError] of FAILING_SINKS) {
    for (const [path, hooks, trigger, reported] of PATHS) {
        Deno.test(`#369 W3 an onError that ${sinkShape}, on ${path}: falls back to the default line and nothing escapes`, async () => {
            await watchingEscapes(async (escaped) => {
                await capturingErrors(async (lines) => {
                    const socket = open({ ...hooks, onError })
                    trigger(socket)
                    await settle()

                    assertEquals(
                        escaped,
                        [],
                        'no rejection reaches the runtime',
                    )
                    assertEquals(
                        lines,
                        [
                            `${FALLBACK_LINE}${reported}; hook failure: Error: sink failed`,
                        ],
                        'exactly one line, in exactly the documented format',
                    )
                })
            })
        })
    }
}

/** CR, LF, an ANSI colour sequence and a U+202E right-to-left override. */
const HOSTILE = 'sink\r\nforged \x1b[31mred\x1b[0m ‮evil'
/** What `renderError` makes of {@link HOSTILE}: every one of them escaped. */
const HOSTILE_RENDERED =
    'Error: sink\\x0d\\x0aforged \\x1b[31mred\\x1b[0m \\u{202e}evil'

Deno.test('#369 W3 the hook failure is encoded like the original error: CR/LF, ANSI and U+202E are all escaped', async () => {
    await watchingEscapes(async (escaped) => {
        await capturingErrors(async (lines) => {
            const socket = open({
                onClose: () => Promise.reject(new Error(HOSTILE)),
                onError: () => {
                    throw new Error(HOSTILE)
                },
            })
            socket.close()
            await settle()

            assertEquals(escaped, [], 'no rejection reaches the runtime')
            assertEquals(lines.length, 1, 'exactly one console.error line')
            for (const raw of ['\r', '\n', '\x1b', '‮']) {
                assert(
                    !lines[0].includes(raw),
                    `no raw ${JSON.stringify(raw)} survives. Got: ${
                        JSON.stringify(lines[0])
                    }`,
                )
            }
            assertEquals(
                lines[0],
                `${FALLBACK_LINE}${HOSTILE_RENDERED}; hook failure: ${HOSTILE_RENDERED}`,
                'both halves rendered through renderError, identically',
            )
        })
    })
})

// --- W4: combined — the app's onClose throws AND the teardown fails --------

Deno.test('#379 W4 combined: the app onClose throws AND the teardown fails — one onError call carrying the app error, one WARN', async () => {
    await watchingEscapes(async (escaped) => {
        await capturingWarnings(async (warnLines) => {
            const APP = new Error('app close failed')
            const { driver } = unwatchFailingDriver()
            const channels = manager(driver)
            const errors: { conn: Connection<User>; error: unknown }[] = []
            const hooks = channels.handlerHooks({
                onMessage: appOnMessage(channels),
                onClose: () => {
                    throw APP
                },
                onError: (conn, error) => void errors.push({ conn, error }),
            })
            const socket = open(hooks)
            socket.subscribe(PUBLIC)
            await settle()
            assertEquals(channels.connectionCount, 1, 'the open registered')

            socket.close()
            await settle()

            assertEquals(escaped, [], 'no rejection reaches the runtime')
            assertEquals(errors.length, 1, 'onError is called exactly once')
            assertStrictEquals(
                errors[0].error,
                APP,
                "onError carries the app's onClose error, not the teardown " +
                    'failure that followed it',
            )
            assertEquals(
                warnLines.filter((l) => l.includes(CLOSE_DISCONNECT_FAILED))
                    .length,
                1,
                'exactly one WARN for the teardown failure, never a second ' +
                    'onError call',
            )
            assertEquals(
                channels.connectionCount,
                0,
                'the connection is still forgotten',
            )

            await assertStillServing(channels, hooks)
            assertEquals(escaped, [], 'and nothing escaped later either')
        })
    })
})
