/**
 * @fileoverview Tests for createWebSocketHandler — SC-002 at the handler level.
 *
 * A forbidden origin returns 403 without upgrading; an allowed origin upgrades
 * and threads the resolved identity into the connection. The upgrade function
 * is injected so no live Deno socket is needed.
 *
 * @module @lockness/realtime/tests/handler
 */

import { assert, assertEquals } from '@std/assert'
import { createWebSocketHandler } from '../websocket.ts'
import type { Context } from '@lockness/hono'
import type { WSContext, WSEvents } from '@lockness/hono/network'
import type { Connection } from '../types.ts'

function fakeContext(origin: string | null): Context {
    return {
        req: {
            header: (name: string) =>
                name.toLowerCase() === 'origin'
                    ? origin ?? undefined
                    : undefined,
        },
        text: (body: string, status: number) => ({ __body: body, status }),
    } as unknown as Context
}
const fakeNext = () => Promise.resolve()

interface User {
    id: number
}

Deno.test('SC-002 (handler): a forbidden origin returns 403 and does not upgrade', async () => {
    let upgraded = false
    const handler = createWebSocketHandler<User>({
        appUrl: 'https://app.example.com',
        hooks: {},
        // deno-lint-ignore no-explicit-any
        upgrade: (() => {
            upgraded = true
            return () => new Response('nope')
            // deno-lint-ignore no-explicit-any
        }) as any,
    })

    const res = await handler(
        fakeContext('https://evil.example.com'),
        fakeNext,
        // deno-lint-ignore no-explicit-any
    ) as any
    assertEquals(res.status, 403)
    assert(!upgraded) // never reached the upgrade
})

Deno.test('SC-002 (handler): an allowed origin upgrades and threads the resolved identity', async () => {
    let events: WSEvents | undefined
    let seenIdentity: User | null = null

    const handler = createWebSocketHandler<User>({
        appUrl: 'https://app.example.com',
        resolveIdentity: () => ({ id: 42 }),
        hooks: {
            onOpen: (conn: Connection<User>) =>
                void (seenIdentity = conn.identity),
        },
        // Capture the createEvents callback the handler hands to upgrade.
        // deno-lint-ignore no-explicit-any
        upgrade: ((createEvents: (c: Context) => WSEvents) => {
            events = createEvents({} as Context)
            return () => new Response('ok')
            // deno-lint-ignore no-explicit-any
        }) as any,
    })

    await handler(fakeContext('https://app.example.com'), fakeNext)
    assert(events !== undefined) // the upgrade was invoked (origin allowed)

    // Driving the built events proves the resolved identity was threaded in.
    events?.onOpen?.(
        new Event('open'),
        { send() {}, close() {} } as unknown as WSContext,
    )
    assertEquals(seenIdentity, { id: 42 })
})
