/**
 * @fileoverview Tests for cross-process fan-out — SC-001 (S6), over the fake bus.
 *
 * A broadcast published by one instance reaches an authorized subscriber on a
 * second instance; the receiving instance re-applies its LOCAL authorization,
 * so a locally-unauthorized connection receives nothing. Proven with a fake
 * Redis pub/sub bus — the live-socket variant of SC-001 is qa-tester territory,
 * and SC-004 (subscribe-socket drop resumes) lives in `redis`'s subscriber test.
 *
 * @module @lockness/realtime/tests/driver_redis
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager } from '../manager.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { BroadcastMessage } from '../driver.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

/** A fake Redis pub/sub bus shared by driver instances. */
class FakeRedisBus {
    private readonly subscribers: Array<
        { pattern: string; handler: (topic: string, payload: string) => void }
    > = []
    /** The command client each driver publishes through. */
    readonly command = (...args: string[]): Promise<unknown> => {
        if (args[0] === 'PUBLISH') {
            const [, topic, payload] = args
            let delivered = 0
            for (const s of this.subscribers) {
                // NOT a glob — a prefix test. Good enough for the shapes this
                // file publishes, and a trap the moment a topic stops matching
                // the driver's pattern: an un-re-pointed topic reaches no
                // handler at all, so every `assertEquals(got.length, 0)` below
                // passes WITHOUT the code under test running. #288 measured it
                // — the three negative ingest tests were vacuous for exactly
                // one commit.
                if (topic.startsWith(s.pattern.replace(/\*$/, ''))) {
                    delivered++
                    s.handler(topic, payload)
                }
            }
            // THE FIX FOR THAT CLASS, and it belongs in the double rather than
            // in each test. A positive control in the test body proves the
            // TRANSPORT works; it cannot prove that THIS frame was routed, so
            // a stale topic still slips through. Refusing to publish into the
            // void catches it once, here, for every test in the file — present
            // and future.
            //
            // Nothing in this file ever publishes a topic no subscriber wants:
            // the driver's own `publish()` goes to its own pattern, and every
            // hand-written PUBLISH is aimed at a driver under test.
            if (delivered === 0) {
                throw new Error(
                    `FakeRedisBus: PUBLISH to "${topic}" matched no ` +
                        `subscriber (${
                            this.subscribers.map((s) => s.pattern).join(', ') ||
                            'none registered'
                        }). Every publish in this file is meant to reach a ` +
                        'driver, so this is a stale topic, not a scenario — ' +
                        'and it would otherwise make an ingest assertion pass ' +
                        'without the ingest code running.',
                )
            }
        }
        return Promise.resolve(0)
    }
    /**
     * A subscriber that deliberately does NOT implement the optional reconnect
     * seam (#271/FR-004). It is this file's standing proof that a subscriber
     * predating the seam — or any app-supplied one — still satisfies the port
     * and still drives a full revocation cycle through the periodic trigger.
     * Do not add `onReconnect` here; `tests/fake_redis.ts` is the fake that has
     * it.
     */
    subscriberFor() {
        return {
            psubscribe: (
                pattern: string,
                handler: (topic: string, payload: string) => void,
            ) => {
                this.subscribers.push({ pattern, handler })
            },
        }
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

function instance(bus: FakeRedisBus, authorize: (id: User | null) => boolean) {
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    return new ChannelManager<User>({ driver, authorize })
}

Deno.test('SC-001: a broadcast on instance A reaches an authorized subscriber on instance B', async () => {
    const bus = new FakeRedisBus()
    const a = instance(bus, () => true)
    const b = instance(bus, () => true)

    const subA = fakeConn('a1', { id: 1 })
    const subB = fakeConn('b1', { id: 2 })
    await a.subscribe(subA, 'private-room')
    await b.subscribe(subB, 'private-room')

    a.broadcast('private-room', 'msg', { text: 'hello' })

    // Both instances' subscribers received it (cross-process fan-out).
    assertEquals(sentOf(subA).length, 1)
    assertEquals(sentOf(subB).length, 1)
    assertEquals(JSON.parse(sentOf(subB)[0]).channel, 'private-room')
})

Deno.test('SC-001a: the receiving instance re-applies local authorization (S6)', async () => {
    const bus = new FakeRedisBus()
    const a = instance(bus, () => true)
    // Instance B denies everyone, so no local subscription exists on B.
    const b = instance(bus, () => false)

    const subA = fakeConn('a1', { id: 1 })
    const notOnB = fakeConn('b1', { id: 2 })
    await a.subscribe(subA, 'private-room')
    const okB = await b.subscribe(notOnB, 'private-room') // denied on B
    assertEquals(okB.ok, false)

    a.broadcast('private-room', 'secret', { s: 1 })

    assertEquals(sentOf(subA).length, 1) // A's authorized subscriber gets it
    assertEquals(sentOf(notOnB).length, 0) // B has no authorized local subscriber
})

Deno.test('cross-channel isolation: a broadcast to X is not delivered to a Y subscriber', async () => {
    const bus = new FakeRedisBus()
    const a = instance(bus, () => true)
    const onY = fakeConn('y', { id: 3 })
    await a.subscribe(onY, 'private-y')
    a.broadcast('private-x', 'e', {})
    assertEquals(sentOf(onY).length, 0)
})

Deno.test('S3 ingest: a Redis message with an out-of-charset event name is dropped', () => {
    const bus = new FakeRedisBus()
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    const got: unknown[] = []
    driver.onMessage((m) => got.push(m))
    // A peer publishes a poisoned event name — it must not reach local fan-out.
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: 'bad name!<x>', data: 1 }),
    )
    assertEquals(got.length, 0)
    // The positive control, in THIS body. The anti-vacuity guard used to live
    // in a sibling test, which is a guard for the file rather than for this
    // assertion: delete or skip that sibling and these three go back to
    // passing without the code under test running. A good frame on the same
    // topic proves the transport reached the driver.
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: 'control-ok', data: 1 }),
    )
    assertEquals(
        got.length,
        1,
        'a VALID frame on the same topic was not delivered either, so the ' +
            'assertion above says nothing about the ingest checks — it says ' +
            'the message never arrived',
    )
})

Deno.test('FR-019 ingest: a Redis message whose channel name is out of charset is dropped', () => {
    const bus = new FakeRedisBus()
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    const got: unknown[] = []
    driver.onMessage((m) => got.push(m))
    // The event name is valid, but the channel derived from the topic is not —
    // isValidName(channel) must gate it too, not only the event (FR-019).
    bus.command(
        'PUBLISH',
        'app:rt__event:bad chan!<x>',
        JSON.stringify({ event: 'ok', data: 1 }),
    )
    assertEquals(got.length, 0)
    // The positive control, in THIS body. The anti-vacuity guard used to live
    // in a sibling test, which is a guard for the file rather than for this
    // assertion: delete or skip that sibling and these three go back to
    // passing without the code under test running. A good frame on the same
    // topic proves the transport reached the driver.
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: 'control-ok', data: 1 }),
    )
    assertEquals(
        got.length,
        1,
        'a VALID frame on the same topic was not delivered either, so the ' +
            'assertion above says nothing about the ingest checks — it says ' +
            'the message never arrived',
    )
})

Deno.test('FR-019 ingest: a Redis message with a non-string event is dropped', () => {
    const bus = new FakeRedisBus()
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    const got: unknown[] = []
    driver.onMessage((m) => got.push(m))
    // A well-formed JSON payload with a wrong-shape event field — the shape
    // guard drops it before fan-out.
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: { nested: true }, data: 1 }),
    )
    assertEquals(got.length, 0)
    // The positive control, in THIS body. The anti-vacuity guard used to live
    // in a sibling test, which is a guard for the file rather than for this
    // assertion: delete or skip that sibling and these three go back to
    // passing without the code under test running. A good frame on the same
    // topic proves the transport reached the driver.
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: 'control-ok', data: 1 }),
    )
    assertEquals(
        got.length,
        1,
        'a VALID frame on the same topic was not delivered either, so the ' +
            'assertion above says nothing about the ingest checks — it says ' +
            'the message never arrived',
    )
})

Deno.test('FR-019 ingest: a valid channel + event name is delivered', () => {
    const bus = new FakeRedisBus()
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    const got: BroadcastMessage[] = []
    driver.onMessage((m) => got.push(m))
    bus.command(
        'PUBLISH',
        'app:rt__event:news',
        JSON.stringify({ event: 'published', data: { n: 1 } }),
    )
    assertEquals(got.length, 1)
    assertEquals(got[0].channel, 'news')
    assertEquals(got[0].event, 'published')
})

Deno.test('#293: a heartbeat that cannot outrun its own liveness TTL is rejected at construction', () => {
    const bus = new FakeRedisBus()
    const build = (presence: Record<string, number>) =>
        new RedisBroadcastDriver(
            { command: bus.command },
            bus.subscriberFor(),
            { prefix: 'app:rt', presence },
        )

    // The failure this refuses is not a crash — it is a HEALTHY instance being
    // swept. Its own liveness key lapses between beats, every peer reads
    // EXISTS 0, and its presence members are removed from every roster while
    // its sockets stay open.
    assertThrows(
        () => build({ heartbeatIntervalMs: 15_000, livenessTtlSeconds: 15 }),
        Error,
        'at most HALF',
    )
    // A MARGIN, not a strict inequality. One beat per window lands exactly on
    // the boundary and races the expiry — losing whenever the round-trip is
    // slower than the slack, which is when the broker is busy.
    assertThrows(
        () => build({ heartbeatIntervalMs: 14_999, livenessTtlSeconds: 15 }),
        Error,
        'at most HALF',
    )
    assertThrows(
        () => build({ heartbeatIntervalMs: 7_501, livenessTtlSeconds: 15 }),
        Error,
        'at most HALF',
    )
    // The message names BOTH values; a guard that says only "invalid" leaves
    // the operator to work out which of the two to move.
    assertThrows(
        () => build({ heartbeatIntervalMs: 15_000, livenessTtlSeconds: 15 }),
        Error,
        'heartbeatIntervalMs=15000ms and livenessTtlSeconds=15s',
    )

    // NaN is the dangerous input and it is easy to produce —
    // `Number(Deno.env.get('...'))` on an unset variable. Every comparison
    // against NaN is false, so without the finiteness check it would slip past
    // the relation above and disable the guard on a fresh process.
    assertThrows(
        () => build({ heartbeatIntervalMs: NaN, livenessTtlSeconds: 15 }),
        Error,
        'positive, finite',
    )
    assertThrows(
        () => build({ heartbeatIntervalMs: 5_000, livenessTtlSeconds: NaN }),
        Error,
        'positive, finite',
    )
    assertThrows(
        () => build({ heartbeatIntervalMs: 0, livenessTtlSeconds: 15 }),
        Error,
        'positive, finite',
    )

    // Exactly half is the boundary and it is ALLOWED — the guard is `> half`,
    // not `>= half`. Pinned so a later tightening cannot pass unnoticed.
    build({ heartbeatIntervalMs: 7_500, livenessTtlSeconds: 15 })
    // And the shipped defaults satisfy it with room to spare: 5000ms against a
    // 15s TTL is three beats per window, not two.
    new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    build({ heartbeatIntervalMs: 5_000, livenessTtlSeconds: 15 })
})

Deno.test('FR-015: a control secret below the 32-byte floor is rejected at construction', () => {
    const bus = new FakeRedisBus()
    // A short, guessable secret weakens the control-frame MAC — refuse it up
    // front rather than shipping a forgeable control plane.
    assertThrows(
        () =>
            new RedisBroadcastDriver(
                { command: bus.command },
                bus.subscriberFor(),
                { prefix: 'app:rt', control: { secret: 'too-short' } },
            ),
        Error,
        '32 bytes',
    )
    // A secret at or above the floor constructs cleanly.
    new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt', control: { secret: 'x'.repeat(32) } },
    )
})

Deno.test('publish sends PUBLISH with the reserved prefix and JSON payload', async () => {
    const bus = new FakeRedisBus()
    const calls: string[][] = []
    const driver = new RedisBroadcastDriver(
        { command: (...a) => (calls.push(a), Promise.resolve(0)) },
        bus.subscriberFor(),
        { prefix: 'app:rt' },
    )
    await driver.publish({ channel: 'news', event: 'e', data: { n: 1 } })
    assertEquals(calls[0][0], 'PUBLISH')
    assertEquals(calls[0][1], 'app:rt__event:news')
    assert(calls[0][2].includes('"event":"e"'))
})

Deno.test('FR-004: a subscriber without the reconnect seam constructs and arms its periodic trigger', async () => {
    const bus = new FakeRedisBus()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    // The fake above deliberately has no `onReconnect`. Registering the
    // revocation trigger must feature-detect it and fall back to the periodic
    // timer alone — no throw, no type error.
    const driver = new RedisBroadcastDriver(
        { command: bus.command },
        bus.subscriberFor(),
        { prefix: 'app:rt', presence: { reconcileIntervalMs: 1000 } },
    )
    let ticks = 0
    try {
        driver.onRevocationReconcile(() => {
            ticks++
        })
        await time.tickAsync(3_500)
        assertEquals(
            ticks,
            3,
            'the periodic trigger still fires on its cadence',
        )
    } finally {
        await driver.close()
        time.restore()
    }
})

Deno.test('#245: fromConfig actually forwards the subscribe cadences', () => {
    // The knobs README documents are only real if they reach the subscribe
    // connection. `fromConfig` builds that connection internally, so there is no
    // way to observe the value from outside — but an INVALID pair is observable,
    // because `RedisSubscribeConnection` validates at construction and throws.
    //
    // A "does it still deliver?" test would have been a false green here: if the
    // cadences were silently dropped, the production defaults would apply and
    // delivery would look identical. The throw is the only witness that
    // distinguishes forwarded from ignored.
    assertThrows(
        () =>
            RedisBroadcastDriver.fromConfig({
                hostname: '127.0.0.1',
                port: 6379,
                // livenessMs must be at least 2x keepaliveMs.
                keepaliveMs: 1000,
                livenessMs: 1000,
            }, { prefix: 'cadence-check' }),
        RangeError,
        'livenessMs',
    )
})

Deno.test('#245: fromConfig accepts a valid cadence set', async () => {
    // The other half, and the reason this test exists at all: before the config
    // type was widened, an object literal carrying `keepaliveMs` was a TS2353
    // excess-property error, so the documented knobs were unreachable from the
    // only production construction path. Nothing dials until first use.
    const driver = RedisBroadcastDriver.fromConfig({
        hostname: '127.0.0.1',
        port: 6379,
        keepaliveMs: 10_000,
        livenessMs: 30_000,
        retryBaseMs: 100,
        retryMaxMs: 5_000,
    }, { prefix: 'cadence-check' })
    await driver.close()
})
