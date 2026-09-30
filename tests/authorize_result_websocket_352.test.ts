/**
 * @fileoverview #352 — an `AuthorizeResultError` thrown from `onMessage`
 * reaches the operator exactly once, reaches the client never, and leaves the
 * socket open.
 *
 * #347 made `ChannelManager.subscribe` THROW for an authorizer result outside
 * its contract, and the design relies on the transport to route that throw:
 * `guard()` in `buildEvents` catches whatever the application's `onMessage`
 * throws and hands it to `onError` — or, with no hook, writes one
 * `console.error` line. The manager-level suite
 * (`authorize_result_347.test.ts`) proves the throw; nothing proved what the
 * socket does with it. A `buildEvents` that swallowed the error, sent a frame
 * or closed the socket passed every test.
 *
 * Driven end to end below the upgrade: `buildEvents` over
 * `manager.handlerHooks(...)`, a fake socket that records every `send` and
 * `close`, and an application `onMessage` written the way `docs/realtime.md`
 * shows — decode, `subscribe`, reply `subscribed`. The reply is sent only
 * AFTER `subscribe` returns, so on a refused frame any byte on the socket can
 * only have come from the framework.
 *
 * Each row carries a CONTROL: a following valid frame on the same socket is
 * handled — replied to, subscribed, delivered — so "nothing was sent" cannot
 * pass because the socket was dead.
 *
 * @module @lockness/realtime/tests/authorize_result_websocket_352
 */

import { assert, assertEquals, assertInstanceOf } from '@std/assert'
import {
    type Authorizer,
    AuthorizeResultError,
    ChannelManager,
    decodeClientMessage,
    encodeServerMessage,
    MemoryBroadcastDriver,
} from '../mod.ts'
import { buildEvents } from '../websocket.ts'
import type { Connection, WebSocketHooks, WSContext } from '../types.ts'

interface User {
    id: number
}

const PRIVATE = 'private-orders'
const PRESENCE = 'presence-room'
const PUBLIC = 'news'

/** A fake socket recording every frame sent and every close. */
function fakeSocket() {
    const sent: string[] = []
    const closes: { code?: number; reason?: string }[] = []
    // `satisfies` ties the two methods `buildEvents` drives to Hono's own
    // signatures, so a drift in `WSContext` fails type-checking here.
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

type FakeSocket = ReturnType<typeof fakeSocket>

/** Let `guard()`'s un-awaited promise chain run to completion. */
async function settle(): Promise<void> {
    for (let i = 0; i < 5; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0))
    }
}

/**
 * A manager whose authorizer returns `undefined` — the missing `return` #347
 * names — for every private and presence channel. Public channels never call
 * it, which is what makes the control frame admissible.
 */
function brokenManager(): ChannelManager<User> {
    return new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: (() => undefined) as unknown as Authorizer<User>,
    })
}

/**
 * The application's `onMessage`, as `docs/realtime.md` writes it: decode,
 * subscribe, reply. It sends nothing before `subscribe` settles.
 */
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

/** Open a socket through `buildEvents`; return it and a subscribe-frame sender. */
function open(hooks: WebSocketHooks<User>) {
    const ws = fakeSocket()
    // Still a cast: the events take a full `WSContext` (`readyState`, `raw`,
    // `url`, …) and the fake implements only the two methods checked above.
    const context = ws as unknown as WSContext
    const events = buildEvents<User>(hooks, { id: 1 })
    events.onOpen?.(new Event('open'), context)
    const subscribe = (channel: string) =>
        events.onMessage?.(
            {
                data: JSON.stringify({ type: 'subscribe', channel }),
            } as MessageEvent,
            context,
        )
    return { ws, subscribe }
}

/**
 * The CONTROL: a valid frame after the refused one is still handled on the
 * same socket — replied to, subscribed, and delivered to.
 */
async function assertStillServing(
    manager: ChannelManager<User>,
    ws: FakeSocket,
    subscribe: (channel: string) => void,
): Promise<void> {
    subscribe(PUBLIC)
    await settle()
    assertEquals(
        ws.sent.map((frame) => JSON.parse(frame)),
        [{ type: 'subscribed', channel: PUBLIC }],
        'CONTROL: the following valid frame is handled and replied to',
    )
    manager.broadcast(PUBLIC, 'headline', { n: 1 })
    await settle()
    assertEquals(
        JSON.parse(ws.sent[1]),
        { type: 'event', channel: PUBLIC, event: 'headline', data: { n: 1 } },
        'CONTROL: and the socket still receives deliveries',
    )
    assertEquals(ws.closes, [], 'the socket is still open afterwards')
}

for (const channel of [PRIVATE, PRESENCE]) {
    Deno.test(`#352 ${channel}: an AuthorizeResultError from onMessage reaches onError exactly once, sends nothing and keeps the socket open`, async () => {
        const manager = brokenManager()
        const errors: unknown[] = []
        const { ws, subscribe } = open(manager.handlerHooks({
            onMessage: appOnMessage(manager),
            onError: (_conn, error) => void errors.push(error),
        }))
        assertEquals(manager.connectionCount, 1, 'the open registered')

        subscribe(channel)
        await settle()

        assertEquals(errors.length, 1, 'onError is called exactly once')
        assertInstanceOf(errors[0], AuthorizeResultError)
        assert(
            (errors[0] as Error).message.includes(channel),
            'and it is the error for this frame',
        )
        assertEquals(ws.sent, [], 'nothing is sent to the client')
        assertEquals(ws.closes, [], 'the socket is not closed')
        assertEquals(
            manager.connectionCount,
            1,
            'the connection is still registered',
        )

        await assertStillServing(manager, ws, subscribe)
        assertEquals(errors.length, 1, 'and the valid frame reports nothing')
    })

    Deno.test(`#352 ${channel}: with no onError hook, exactly one console.error line names the AuthorizeResultError and the socket stays open`, async () => {
        const manager = brokenManager()
        const lines: string[] = []
        const originalConsoleError = console.error
        console.error = (...args: unknown[]) =>
            void lines.push(args.map(String).join(' '))
        try {
            const { ws, subscribe } = open(manager.handlerHooks({
                onMessage: appOnMessage(manager),
            }))

            subscribe(channel)
            await settle()

            assertEquals(lines.length, 1, 'exactly one console.error line')
            assert(
                lines[0].startsWith('realtime: unhandled websocket error: '),
                `the default sink's line. Got: ${lines[0]}`,
            )
            assert(
                lines[0].includes('AuthorizeResultError'),
                `the line names the error. Got: ${lines[0]}`,
            )
            assertEquals(ws.sent, [], 'nothing is sent to the client')
            assertEquals(ws.closes, [], 'the socket is not closed')

            await assertStillServing(manager, ws, subscribe)
            assertEquals(lines.length, 1, 'and the valid frame logs nothing')
        } finally {
            console.error = originalConsoleError
        }
    })
}
