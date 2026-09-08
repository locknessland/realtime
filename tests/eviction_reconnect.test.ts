/**
 * @fileoverview SC-001 (#271) — the durable revocation re-check runs at the
 * moment the subscribe socket reconnects, not only on the periodic tick.
 *
 * #268 shipped half of its own FR-014: the marker is re-checked on a periodic
 * timer, but not at the routine moment a pub/sub frame is lost — the instant the
 * owning instance's subscribe socket comes back. These tests drive `FakeTime`
 * and never advance it by a full reconcile interval, so a pass proves the
 * recovery came from the reconnect and not from the timer.
 *
 * @module @lockness/realtime/tests/eviction_reconnect
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { ChannelManager } from '../manager.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

interface User {
    id: number
    name: string
}

interface Spy {
    readonly _closed: number
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    let closed = 0
    return {
        id,
        identity,
        metadata: {},
        send: () => {},
        close: () => void closed++,
        get _closed() {
            return closed
        },
    } as Connection<User> & Spy
}
const closedOf = (c: Connection<User>) => (c as unknown as Spy)._closed

const authorize = (id: User | null): PresenceMember | false =>
    id ? { id: id.id, info: { name: id.name } } : false

/** One instance, with its subscriber kept in reach so a reconnect can be fired. */
function instance(redis: FakeRedis) {
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        subscriber,
        {
            prefix: 'app:rt',
            control: { secret: 'deployment-secret-with-enough-entropy' },
            presence: {
                livenessTtlSeconds: 30,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
    const manager = new ChannelManager<User>({ driver, authorize })
    return { manager, driver, subscriber }
}

/**
 * Drain the microtasks a reconcile pass queues.
 *
 * Every `FakeRedis` command resolves synchronously, so a reconcile is a chain of
 * already-settled promises rather than anything the clock can advance — a
 * `FakeTime` tick will not drain it. The bound is deliberately far above the
 * longest chain a pass can produce (`SMEMBERS`, then one `EXISTS` and one
 * roster/marker round-trip per revoked id) so it degrades into "drain until
 * quiet" rather than encoding a count that a future step would silently break.
 */
async function flushMicrotasks(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

Deno.test('SC-001: an evict missed while the socket was deaf is recovered at reconnect, before any periodic tick', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    const b = instance(redis)
    try {
        const x = fakeConn('x', { id: 1, name: 'Xavier' })
        await b.manager.subscribe(x, 'presence-lobby')

        // An evict is issued elsewhere and durably recorded; B's subscribe
        // socket was between reconnects, so the control frame never landed.
        await b.driver.markRevoked('x')
        assertEquals(closedOf(x), 0, 'X is not revoked yet')

        // Well short of reconcileIntervalMs — the periodic timer has NOT fired.
        await time.tickAsync(100)
        await flushMicrotasks()
        assertEquals(
            closedOf(x),
            0,
            'no periodic tick has elapsed, so nothing has recovered it yet',
        )

        // B's subscribe socket reconnects and re-issues its patterns.
        await b.subscriber.fireReconnect()
        await flushMicrotasks()

        assertEquals(
            closedOf(x),
            1,
            'the missed evict is recovered AT the reconnect, not on the next tick',
        )
        assertEquals(
            (await b.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            false,
            'X is dropped from the authoritative roster on recovery',
        )
    } finally {
        await b.driver.close()
        time.restore()
    }
})

Deno.test('SC-001: a presence-free instance also recovers at reconnect', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    const b = instance(redis)
    try {
        // Only private/public channels: the presence ghost-sweep timer never
        // starts. The reconnect trigger must not be coupled to it.
        const y = fakeConn('y', { id: 2, name: 'Yves' })
        b.manager.register(y)
        await b.driver.markRevoked('y')

        await b.subscriber.fireReconnect()
        await flushMicrotasks()

        assertEquals(
            closedOf(y),
            1,
            'recovered at reconnect with no presence member ever added',
        )
    } finally {
        await b.driver.close()
        time.restore()
    }
})

Deno.test('FR-007: a reconnect that fires after close() runs nothing, on the injected-port path', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    // Count every command the driver issues, so "quiesced" is proved by silence
    // on the wire and not only by the absence of a close().
    let commands = 0
    const counting = (...args: string[]): Promise<unknown> => {
        commands++
        return redis.command(...args)
    }
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver({ command: counting }, subscriber, {
        prefix: 'app:rt',
        control: { secret: 'deployment-secret-with-enough-entropy' },
        presence: { reconcileIntervalMs: 1000 },
    })
    const manager = new ChannelManager<User>({ driver, authorize })
    try {
        const z = fakeConn('z', { id: 3, name: 'Zoe' })
        manager.register(z)
        await driver.markRevoked('z')

        // Built through the PUBLIC CONSTRUCTOR with an injected subscriber, so
        // close() neither owns nor closes that subscriber — the subscriber
        // outlives the driver and can still fire. Quiescence must come from the
        // driver itself.
        await driver.close()
        const commandsAtClose = commands

        await subscriber.fireReconnect()
        await flushMicrotasks()

        assertEquals(closedOf(z), 0, 'a post-close reconnect revokes nothing')
        assertEquals(
            commands,
            commandsAtClose,
            'a post-close reconnect issues no Redis command',
        )
    } finally {
        time.restore()
    }
})

Deno.test('FR-003: a re-check that throws at reconnect does not disarm the periodic timer', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    // The fault is injected at the COMMAND BOUNDARY, not by monkey-patching the
    // driver under test: the first read of the revocation index rejects, exactly
    // as a Redis blip would, and everything downstream is the real code path.
    //
    // Matched on the COMMAND, not on a key literal. #276 renamed the index, and
    // a predicate pinned to the old key name would have silently stopped
    // matching — the test would still pass while injecting nothing.
    // Armed AFTER setup, so it catches the reconcile's read rather than the
    // markRevoked that seeds it — both are EVAL now.
    let failNextRead = false
    const flaky = (...args: string[]): Promise<unknown> => {
        if (failNextRead && args[0] === 'EVAL') {
            failNextRead = false
            return Promise.reject(new Error('reconcile exploded'))
        }
        return redis.command(...args)
    }
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver({ command: flaky }, subscriber, {
        prefix: 'app:rt',
        control: { secret: 'deployment-secret-with-enough-entropy' },
        presence: { reconcileIntervalMs: 1000 },
    })
    const b = {
        driver,
        subscriber,
        manager: new ChannelManager<User>({ driver, authorize }),
    }
    const realWarn = console.warn
    const warnings: string[] = []
    console.warn = (...args: unknown[]) => {
        warnings.push(args.map((a) => String(a)).join(' '))
    }
    try {
        const w = fakeConn('w', { id: 4, name: 'Wren' })
        b.manager.register(w)
        await b.driver.markRevoked('w')
        failNextRead = true

        await b.subscriber.fireReconnect()
        await flushMicrotasks()
        assertEquals(closedOf(w), 0, 'the failed re-check revoked nothing')

        // The containment did NOT disarm the retry: the periodic timer still
        // fires and recovers the revocation.
        await time.tickAsync(1_200)
        await flushMicrotasks()
        assertEquals(
            closedOf(w),
            1,
            'the periodic timer still recovers it after a failed reconnect pass',
        )
        assert(
            warnings.some((m) => m.includes('revocation reconcile failed')),
            'the failure was WARNed with its subject named',
        )
    } finally {
        console.warn = realWarn
        await b.driver.close()
        time.restore()
    }
})

Deno.test('FR-004: a seam-less subscriber still completes a full revocation cycle on the periodic trigger', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    // An app-supplied subscriber predating the seam: the optional member is
    // absent, so the driver must fall back to the periodic trigger alone —
    // exactly #268's shipped behaviour (SC-004).
    const full = redis.subscriberFor()
    const seamless = { psubscribe: full.psubscribe }
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        seamless,
        {
            prefix: 'app:rt',
            control: { secret: 'deployment-secret-with-enough-entropy' },
            presence: { reconcileIntervalMs: 1000 },
        },
    )
    const manager = new ChannelManager<User>({ driver, authorize })
    try {
        const s = fakeConn('s', { id: 5, name: 'Sam' })
        manager.register(s)
        await driver.markRevoked('s')

        await time.tickAsync(1_200)
        await flushMicrotasks()

        assertEquals(
            closedOf(s),
            1,
            'the periodic trigger alone still recovers the revocation',
        )
    } finally {
        await driver.close()
        time.restore()
    }
})
