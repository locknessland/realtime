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
import {
    FLOOR_ANNOUNCE_RETRY_MS,
    RedisBroadcastDriver,
    REVOCATION_FLOOR_ANNOUNCE_FAILED,
} from '../drivers/redis.ts'
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

/**
 * Run `body` with `console.warn` captured; restores it even if `body` throws.
 * `body` receives the live list, so it can wait on a line as it lands.
 */
async function captureWarnings(
    body: (messages: readonly string[]) => Promise<void>,
): Promise<string[]> {
    const messages: string[] = []
    const real = console.warn
    console.warn = (...args: unknown[]) => {
        messages.push(args.map((a) => String(a)).join(' '))
    }
    try {
        await body(messages)
    } finally {
        console.warn = real
    }
    return messages
}

/** Whether a WARN line is the #380 floor announce's failure. */
const isAnnounceFailure = (line: string): boolean =>
    line.startsWith(REVOCATION_FLOOR_ANNOUNCE_FAILED)

/**
 * Count the #380 announce-failed WARNs from here on, and keep them off the
 * console; every other line passes through.
 *
 * The loopback server answers every `EVAL` with `ERR unknown command`, so the
 * first announce of each manager-backed driver is refused, exactly once per
 * driver. Each test pins that count exactly, so a new source of the WARN
 * fails it rather than adding one more line to the output. The retry this
 * failure arms (`FLOOR_ANNOUNCE_RETRY_MS`, 1 s) never fires inside these
 * tests at all (#407): every test body runs under
 * {@link withSuppressedFloorAnnounceRetry}, which is what keeps this count
 * from a wall-clock margin rather than "the driver is closed within 1 s".
 */
function countAnnounceFailures(): { count(): number; restore(): void } {
    const previous = console.warn
    let count = 0
    console.warn = (...args: unknown[]) => {
        if (isAnnounceFailure(args.map((a) => String(a)).join(' '))) {
            count++
            return
        }
        previous(...args)
    }
    return {
        count: () => count,
        restore: () => void (console.warn = previous),
    }
}

/**
 * The line between a real-time wait these tests still need and the #380
 * floor-announce retry {@link withSuppressedFloorAnnounceRetry} exists to
 * neutralize (#407, narrowed #415).
 *
 * Above it: `FLOOR_ANNOUNCE_RETRY_MS` (1 s, `drivers/redis.ts`) — and ONLY
 * that one timer, identified below by more than its delay. Below it: the
 * subscribe socket's own reconnect backoff (250 ms by default,
 * `packages/redis/subscriber.ts`, exercised by the FR-019/SC-006 test below)
 * and `waitFor`'s 5 ms poll — both stay real and wall-clock-bound exactly as
 * before #407. Also real since #415: `RECONCILE_RETRY_MS`'s reconnect retry
 * and `#armReconcile`'s recurring pass timer (both `drivers/redis.ts`) — #407
 * never meant to freeze those, and a floor on the delay alone did (#415).
 */
const NEVER_FIRES_MS = 24 * 60 * 60 * 1000

/**
 * The private field only {@link RedisBroadcastDriver}'s `#announceFloor`
 * retry closure assigns (`this.#announceRetry = …`, `drivers/redis.ts`) —
 * the fingerprint {@link isFloorAnnounceRetryHandler} looks for in a
 * `setTimeout` handler's own source text.
 *
 * **Coupling risk, read before touching `#announceFloor`:** this is coupled
 * to that closure's literal source, not to its behavior. If the closure is
 * refactored so it no longer assigns `this.#announceRetry` in its own body —
 * renamed field, extracted helper, assignment hoisted elsewhere — this
 * substring stops matching, `isFloorAnnounceRetryHandler` silently returns
 * `false` for the real retry, and the retry goes back to being wall-clock-bound
 * inside every test that wraps it (the exact regression #407 fixed). Nothing
 * red-flags the file at that point except {@link withSuppressedFloorAnnounceRetry}'s
 * own loud guard below — keep the two in sync, or drop the guard's `expectMatch`
 * default the moment this fingerprint no longer fits the retry it names.
 */
const ANNOUNCE_RETRY_FIELD_FINGERPRINT = '#announceRetry'

/**
 * Whether `handler`'s own literal source text is `#announceFloor`'s retry
 * closure (#415) — recognized by {@link ANNOUNCE_RETRY_FIELD_FINGERPRINT},
 * the one private field only that closure assigns.
 *
 * `Function#toString` on a native (never transpiled-away) private field
 * returns the closure's exact written source, so the substring survives
 * minification-free test runs unmodified. Matching on the scheduled delay
 * ALONE is not enough (#415): `RECONCILE_RETRY_MS`'s reconnect retry
 * (`#runRevocationReconcile`, `drivers/redis.ts`) arms the identical 1000 ms
 * through a bare `setTimeout` whose closure never touches this field.
 *
 * @param handler - The function passed to a `setTimeout` call. Typed to
 *   accept `never` arguments (never `any`, rule #3) — this only ever reads
 *   `.toString()`, so no call signature needs to line up with the real one.
 * @returns Whether `handler` is `#announceFloor`'s retry closure.
 */
function isFloorAnnounceRetryHandler(
    handler: (...args: never[]) => unknown,
): boolean {
    return handler.toString().includes(ANNOUNCE_RETRY_FIELD_FINGERPRINT)
}

/**
 * Run `body` with `#announceFloor`'s floor-announce retry — and ONLY that
 * timer — silently pushed a day out, so it cannot fire inside `body` no
 * matter how long `body` itself takes on a loaded machine (#407, narrowed
 * #415).
 *
 * A `setTimeout` is substituted when BOTH hold: its delay is exactly
 * `FLOOR_ANNOUNCE_RETRY_MS` (1 s, `drivers/redis.ts`), AND its handler is
 * {@link isFloorAnnounceRetryHandler}. Before #415 this matched on delay
 * alone (`>= 500 ms`), which also neutralized `RECONCILE_RETRY_MS`'s
 * reconnect retry and any future assertion inside `body` that itself armed a
 * long timer — the exact hazard this issue closes. The heartbeat uses
 * `setInterval`, never `setTimeout`, so it was already untouched either way.
 *
 * The substitution returns the REAL timer id every time, never a stand-in:
 * `clearTimeout` (bare, never patched here) and `Deno.unrefTimer`, both
 * called by production code on that id, keep working completely unmodified —
 * only the delay handed to the underlying, un-patched `setTimeout` changes.
 * `console.warn` capture happens independently via {@link countAnnounceFailures}
 * / {@link captureWarnings}; this helper only ever touches timing.
 *
 * **The loud guard against the fingerprint rotting silently** (#415): unless
 * `expectMatch` is `false`, this throws if `body` completed having never
 * matched a single timer — the signal that either the fingerprint stopped
 * naming the real closure (see {@link ANNOUNCE_RETRY_FIELD_FINGERPRINT}) or
 * the test no longer exercises the retry it claims to neutralize. A caller
 * that genuinely expects no match (proving the narrowing itself, never
 * exercising a real driver) passes `expectMatch: false` explicitly, with a
 * comment saying why.
 *
 * @param body - The test body to run with the substitution installed.
 * @param options - `expectMatch` (default `true`): whether `body` is expected
 *   to arm the floor-announce retry at least once.
 * @returns `body`'s own result.
 * @throws If `expectMatch` is `true` (the default) and `body` completed
 *   without ever suppressing a matching timer.
 * @example
 * ```ts
 * await withSuppressedFloorAnnounceRetry(async () => {
 *   // ... drive a real driver here; its 1 s+ retry never fires ...
 * })
 * ```
 */
async function withSuppressedFloorAnnounceRetry<T>(
    body: () => Promise<T>,
    options: { expectMatch?: boolean } = {},
): Promise<T> {
    const expectMatch = options.expectMatch ?? true
    const realSetTimeout = globalThis.setTimeout
    let suppressedCount = 0
    globalThis.setTimeout = (<Args extends unknown[]>(
        handler: (...args: Args) => void,
        timeout?: number,
        ...args: Args
    ): ReturnType<typeof setTimeout> => {
        const delay = timeout ?? 0
        const isFloorAnnounceRetry = delay === FLOOR_ANNOUNCE_RETRY_MS &&
            isFloorAnnounceRetryHandler(handler)
        if (isFloorAnnounceRetry) suppressedCount++
        return realSetTimeout(
            handler,
            isFloorAnnounceRetry ? NEVER_FIRES_MS : delay,
            ...args,
        )
        // `unknown`, not `any` (rule #3): the declared global `setTimeout`
        // types its rest-arg generic as `any[]` (the DOM/Deno lib signature,
        // not ours); this bridges our `unknown[]`-typed override to that
        // exact declared shape without introducing `any` in this file.
    }) as unknown as typeof setTimeout
    try {
        const result = await body()
        if (expectMatch && suppressedCount === 0) {
            throw new Error(
                '#415: withSuppressedFloorAnnounceRetry matched no timer ' +
                    'during this test body, but expectMatch was true (the ' +
                    "default). Either #announceFloor's retry closure no " +
                    'longer assigns `this.#announceRetry` in its own source ' +
                    '(the fingerprint in ANNOUNCE_RETRY_FIELD_FINGERPRINT has ' +
                    'rotted), or this test no longer exercises the retry it ' +
                    'claims to neutralize. Fix the fingerprint, or pass ' +
                    '{ expectMatch: false } if no match is genuinely expected.',
            )
        }
        return result
    } finally {
        globalThis.setTimeout = realSetTimeout
    }
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
    await withSuppressedFloorAnnounceRetry(async () => {
        const server = await startFakeServer()
        const config = { hostname: '127.0.0.1', port: server.port }
        const announces = countAnnounceFailures()
        // Two independent instances, each with its OWN real subscribe socket.
        const driverA = RedisBroadcastDriver.fromConfig(config, {
            prefix: PREFIX,
        })
        const driverB = RedisBroadcastDriver.fromConfig(config, {
            prefix: PREFIX,
        })
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
            a.register(subA)
            const subB = fakeConn('b1', { id: 1 }) // authorized on B
            b.register(subB)
            const notOnB = fakeConn('b2', { id: 2 }) // denied on B
            b.register(notOnB)

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
            await waitFor(
                () => announces.count() >= 2,
                "each instance's first floor announce was refused and WARNed",
            )
        } finally {
            await driverA.close()
            await driverB.close()
            server.stop()
            announces.restore()
        }
        assertEquals(
            announces.count(),
            2,
            'exactly one announce-failed WARN per instance (#380)',
        )
    })
})

Deno.test('FR-019/SC-006: an oversized pushed payload is rejected by the bounded reader before fan-out, and delivery self-heals', async () => {
    await withSuppressedFloorAnnounceRetry(async () => {
        const server = await startFakeServer()
        const config = { hostname: '127.0.0.1', port: server.port }
        const announces = countAnnounceFailures()
        const driver = RedisBroadcastDriver.fromConfig(config, {
            prefix: PREFIX,
        })
        const manager = new ChannelManager<User>({
            driver,
            authorize: () => true,
        })
        // The announce WARN lands in either list, depending on when it arrives.
        const announceFailures = (seen: readonly string[]) =>
            announces.count() + seen.filter(isAnnounceFailure).length
        const warnings = await captureWarnings(async (seen) => {
            try {
                const sub = fakeConn('c1', { id: 1 })
                manager.register(sub)
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
                await waitFor(
                    () => announceFailures(seen) >= 1,
                    "the instance's first floor announce was refused and WARNed",
                )
            } finally {
                await driver.close()
                server.stop()
            }
        }).finally(() => announces.restore())
        assertEquals(
            announceFailures(warnings),
            1,
            'exactly one announce-failed WARN for the one instance (#380)',
        )
        assert(
            warnings.some((m) => m.toLowerCase().includes('reconnect')),
            'the oversized-frame fault reconnected and was logged at WARN, never silent',
        )
    })
})

Deno.test('FR-007: on the fromConfig path, close() leaves nothing that a later socket fault could revive', async () => {
    await withSuppressedFloorAnnounceRetry(async () => {
        const server = await startFakeServer()
        const config = { hostname: '127.0.0.1', port: server.port }
        // The OTHER construction path. Here `close()` owns and closes the subscribe
        // connection, so quiescence has two independent gates — the subscriber's own
        // `closed` flag and the driver's cleared revocation handler. The
        // injected-port sibling in `eviction_reconnect.test.ts` covers the path where
        // only the second exists.
        const announces = countAnnounceFailures()
        const driver = RedisBroadcastDriver.fromConfig(config, {
            prefix: PREFIX,
        })
        let reconciles = 0
        const manager = new ChannelManager<User>({
            driver,
            authorize: () => true,
        })
        try {
            driver.onRevocationReconcile(() => {
                reconciles++
            })
            const a1 = fakeConn('a1', { id: 1 })
            manager.register(a1)
            await manager.subscribe(a1, 'private-room')
            await waitFor(
                () => psubscribeCount(server, PATTERN) >= 1,
                'the real subscribe socket is up',
            )
            // The manager's registration announced; the direct re-registration
            // above it did not (first registration only).
            await waitFor(
                () => announces.count() >= 1,
                "the driver's first floor announce was refused and WARNed",
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
            announces.restore()
        }
        assertEquals(
            announces.count(),
            1,
            'exactly one announce-failed WARN for the one driver (#380)',
        )
    })
})

Deno.test(
    '#415: withSuppressedFloorAnnounceRetry passes through a non-floor-announce timer armed at FLOOR_ANNOUNCE_RETRY_MS — proof the narrowing is by handler, not delay alone',
    async () => {
        // A spy standing in for the REAL setTimeout: installed BEFORE the
        // helper runs, so it is the `realSetTimeout` the helper captures and
        // forwards to — exactly the delay the helper decided to hand it,
        // without waiting for anything to actually fire.
        const forwardedDelays: number[] = []
        const spiedRealSetTimeout = globalThis.setTimeout
        globalThis.setTimeout = (<Args extends unknown[]>(
            handler: (...args: Args) => void,
            timeout?: number,
            ...args: Args
        ): ReturnType<typeof setTimeout> => {
            forwardedDelays.push(timeout ?? 0)
            // Never actually scheduled: the id is cleared immediately below,
            // so nothing here can outlive this test regardless of the delay
            // the helper decided on.
            return spiedRealSetTimeout(handler, 0, ...args)
            // `unknown`, not `any` (rule #3): same bridge as the helper's own
            // override, for the same reason.
        }) as unknown as typeof setTimeout
        try {
            // `expectMatch: false` (documented on the helper): this body
            // deliberately arms a timer that must NOT match the fingerprint,
            // so the helper's own loud guard is told not to expect one.
            await withSuppressedFloorAnnounceRetry(() => {
                // A plain closure at the EXACT same delay as the real
                // #announceFloor retry (`FLOOR_ANNOUNCE_RETRY_MS`), but one
                // that never assigns `#announceRetry` — the shape
                // `RECONCILE_RETRY_MS`'s reconnect retry takes in production.
                const id = setTimeout(() => {}, FLOOR_ANNOUNCE_RETRY_MS)
                clearTimeout(id)
                return Promise.resolve()
            }, { expectMatch: false })
        } finally {
            globalThis.setTimeout = spiedRealSetTimeout
        }
        assertEquals(
            forwardedDelays,
            [FLOOR_ANNOUNCE_RETRY_MS],
            'a same-delay, non-#announceFloor timer must reach the real ' +
                'setTimeout with its ORIGINAL delay — proof the narrowing ' +
                "checks the handler's fingerprint, not the delay value alone " +
                '(a mutant that matches on delay alone, or reverts to ' +
                'suppress-all-≥500ms, forwards NEVER_FIRES_MS instead and ' +
                'this assertion goes red)',
        )
    },
)
