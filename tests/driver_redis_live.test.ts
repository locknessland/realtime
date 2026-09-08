/**
 * @fileoverview US1 — cross-process delivery over a REAL pub/sub socket (SC-001,
 * SC-006 oversized clause).
 *
 * Unlike `driver_redis.test.ts` (a fake in-memory bus), these tests wire two
 * `ChannelManager`s to real {@link RedisBroadcastDriver}s built through
 * {@link RedisBroadcastDriver.fromConfig} — the queue-mirror construction
 * (FR-012) that opens each instance's OWN `RedisSubscribeConnection` socket. The
 * sockets talk to the in-process RESP fake server (`packages/redis/tests`), whose
 * pub/sub push seam stands in for a live Redis fan-out. This proves the real
 * subscribe-mode push path end-to-end without a redis binary:
 *
 * - a broadcast fanned onto the bus reaches an authorized subscriber on a second
 *   instance through B's real socket (SC-001), and a locally-unauthorized
 *   connection on B receives nothing — B re-applies its own authorization (S6);
 * - an oversized pushed payload is rejected by the bounded `resp.ts` reader
 *   BEFORE `JSON.parse` + fan-out, so it never reaches a local subscriber
 *   (FR-019 / SC-006), and delivery self-heals afterwards.
 *
 * Resource/op-sanitizer discipline: every driver's sockets are closed and the
 * subscribe read loop awaited to unwind before the server stops.
 *
 * @module @lockness/realtime/tests/driver_redis_live
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { Connection } from '../types.ts'
// The redis package's loopback RESP fake with a pub/sub push + force-drop seam
// (#268 foundation). A test-only helper, so a relative reach into the sibling
// package's tests is intentional — it never ships (tests are publish-excluded).
import {
    type FakeServer,
    startFakeServer,
} from '../../redis/tests/fake_server.ts'

interface User {
    id: number
}

const PREFIX = 'app:rt'
/**
 * The subscription an instance hosting `private-room` holds.
 *
 * An EXACT topic, not `${PREFIX}__event:*` (#295). The driver no longer
 * subscribes a prefix-wide glob at `onMessage`; `ChannelManager` declares each
 * hosted channel through `watchChannel`, so this is the pattern the fake
 * server's push seam must name for a frame to reach the instance at all.
 */
const PATTERN = `${PREFIX}__event:private-room`

/** Poll `cond` until it holds or the deadline passes (a fake-socket race gate). */
async function waitFor(
    cond: () => boolean,
    message: string,
    timeoutMs = 2000,
): Promise<void> {
    const start = Date.now()
    while (!cond()) {
        if (Date.now() - start > timeoutMs) {
            throw new Error(`waitFor timed out: ${message}`)
        }
        await new Promise((r) => setTimeout(r, 5))
    }
}

/**
 * How many EXACT subscriptions the server has seen for `channel` (#295).
 *
 * `SUBSCRIBE`, not `PSUBSCRIBE`, and the verb is the point rather than an
 * implementation detail: Redis matches an ACL channel rule literally for
 * `PSUBSCRIBE` and by glob for `SUBSCRIBE`, so a driver issuing an exact topic
 * as a pattern is refused by every operator's `&prefix__event:*` rule. A
 * counter that accepted either verb would stay green through exactly that
 * regression.
 */
function psubscribeCount(server: FakeServer, channel: string): number {
    return server.commandLog.filter(
        (c) => c[0]?.toUpperCase() === 'SUBSCRIBE' && c[1] === channel,
    ).length
}

/** Run `body` with `console.warn` captured; restores it even if `body` throws. */
async function captureWarnings(body: () => Promise<void>): Promise<string[]> {
    const messages: string[] = []
    const real = console.warn
    console.warn = (...args: unknown[]) => {
        messages.push(args.map((a) => String(a)).join(' '))
    }
    try {
        await body()
    } finally {
        console.warn = real
    }
    return messages
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    const sent: string[] = []
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void sent.push(d as string),
        close: () => {},
        get _sent() {
            return sent
        },
    } as Connection<User> & { readonly _sent: string[] }
}
const sentOf = (c: Connection<User>) =>
    (c as unknown as { _sent: string[] })._sent

Deno.test("SC-001: a broadcast reaches an authorized subscriber on a second instance over a real pub/sub socket, re-bounded by B's local authorization (S6)", async () => {
    const server = await startFakeServer()
    const config = { hostname: '127.0.0.1', port: server.port }
    // Two independent instances, each with its OWN real subscribe socket.
    const driverA = RedisBroadcastDriver.fromConfig(config, { prefix: PREFIX })
    const driverB = RedisBroadcastDriver.fromConfig(config, { prefix: PREFIX })
    // A authorizes everyone; B authorizes user 1 but denies user 2 (S6).
    const a = new ChannelManager<User>({
        driver: driverA,
        authorize: () => true,
    })
    const b = new ChannelManager<User>({
        driver: driverB,
        authorize: (id) => id?.id === 1,
    })
    try {
        const subA = fakeConn('a1', { id: 1 })
        const subB = fakeConn('b1', { id: 1 }) // authorized on B
        const notOnB = fakeConn('b2', { id: 2 }) // denied on B

        await a.subscribe(subA, 'private-room')
        assertEquals((await b.subscribe(subB, 'private-room')).ok, true)
        assertEquals((await b.subscribe(notOnB, 'private-room')).ok, false)

        // Both instances have PSUBSCRIBEd over their own real socket.
        await waitFor(
            () => psubscribeCount(server, PATTERN) >= 2,
            'both instances subscribed over real sockets',
        )

        // Instance A broadcasts: on a live Redis a PUBLISH fans the frame to
        // every pattern subscriber's socket. The fake server's push seam performs
        // that fan-out with the exact payload shape the driver publishes.
        server.publishExact(
            PATTERN,
            JSON.stringify({ event: 'msg', data: { text: 'hello' } }),
        )

        await waitFor(
            () => sentOf(subB).length >= 1,
            "B's authorized subscriber received it over the real socket",
        )
        assertEquals(JSON.parse(sentOf(subB)[0]).channel, 'private-room')
        assertEquals(JSON.parse(sentOf(subB)[0]).event, 'msg')
        // S6: B never authorized this connection locally, so it is not in B's
        // subscription set and receives nothing.
        assertEquals(sentOf(notOnB).length, 0)
        // The publishing instance's own authorized subscriber receives it too.
        await waitFor(
            () => sentOf(subA).length >= 1,
            "the publishing instance's subscriber received it too",
        )
    } finally {
        await driverA.close()
        await driverB.close()
        server.stop()
    }
})

Deno.test('FR-019/SC-006: an oversized pushed payload is rejected by the bounded reader before fan-out, and delivery self-heals', async () => {
    const server = await startFakeServer()
    const config = { hostname: '127.0.0.1', port: server.port }
    const driver = RedisBroadcastDriver.fromConfig(config, { prefix: PREFIX })
    const manager = new ChannelManager<User>({
        driver,
        authorize: () => true,
    })
    const warnings = await captureWarnings(async () => {
        try {
            const sub = fakeConn('c1', { id: 1 })
            await manager.subscribe(sub, 'private-room')
            await waitFor(
                () => psubscribeCount(server, PATTERN) >= 1,
                'subscribed over a real socket',
            )
            const acceptsBefore = server.accepts()

            // A payload one byte over the reader's 10 MiB bulk bound. `resp.ts`
            // rejects it at the length header — before `readExact`, before
            // `JSON.parse`, before any fan-out — desyncing the socket.
            const oversized = 'x'.repeat(10 * 1024 * 1024 + 1)
            server.publishExact(
                PATTERN,
                oversized,
            )

            // The framing fault self-heals: reconnect + re-PSUBSCRIBE (WARN).
            await waitFor(
                () => server.accepts() > acceptsBefore,
                'the subscribe socket reconnected after the oversized frame',
            )
            await waitFor(
                () => psubscribeCount(server, PATTERN) >= 2,
                'the subscription was re-issued on the healed socket',
            )

            // A valid message on the healed socket IS delivered — proving the
            // earlier oversized frame was dropped, not merely delayed.
            server.publishExact(
                PATTERN,
                JSON.stringify({ event: 'ok', data: 1 }),
            )
            await waitFor(
                () => sentOf(sub).length >= 1,
                'delivery resumed after the oversized frame was rejected',
            )
            // Only the valid message, never the oversized one.
            assertEquals(sentOf(sub).length, 1)
            assertEquals(JSON.parse(sentOf(sub)[0]).event, 'ok')
        } finally {
            await driver.close()
            server.stop()
        }
    })
    assert(
        warnings.some((m) => m.toLowerCase().includes('reconnect')),
        'the oversized-frame fault reconnected and was logged at WARN, never silent',
    )
})

Deno.test('FR-007: on the fromConfig path, close() leaves nothing that a later socket fault could revive', async () => {
    const server = await startFakeServer()
    const config = { hostname: '127.0.0.1', port: server.port }
    // The OTHER construction path. Here `close()` owns and closes the subscribe
    // connection, so quiescence has two independent gates — the subscriber's own
    // `closed` flag and the driver's cleared revocation handler. The
    // injected-port sibling in `eviction_reconnect.test.ts` covers the path where
    // only the second exists.
    const driver = RedisBroadcastDriver.fromConfig(config, { prefix: PREFIX })
    let reconciles = 0
    const manager = new ChannelManager<User>({
        driver,
        authorize: () => true,
    })
    try {
        driver.onRevocationReconcile(() => {
            reconciles++
        })
        await manager.subscribe(fakeConn('a1', { id: 1 }), 'private-room')
        await waitFor(
            () => psubscribeCount(server, PATTERN) >= 1,
            'the real subscribe socket is up',
        )

        await driver.close()
        const psubscribesAtClose = psubscribeCount(server, PATTERN)
        const reconcilesAtClose = reconciles

        // Force the wire fault a live socket would take. A closed connection
        // must not re-dial at all, so no reconnect exists to fire the seam.
        server.dropConnections()
        await new Promise((r) => setTimeout(r, 60))

        assertEquals(
            psubscribeCount(server, PATTERN),
            psubscribesAtClose,
            'a closed subscribe connection does not re-dial',
        )
        assertEquals(
            reconciles,
            reconcilesAtClose,
            'and therefore nothing re-checks revocations after close()',
        )
    } finally {
        await driver.close()
        server.stop()
    }
})
