/**
 * @fileoverview SC-003 — cross-process eviction revokes the owning socket.
 *
 * Two `ChannelManager`s wired to two `RedisBroadcastDriver`s share one fake
 * Redis (roster store + control bus). A server-only evict issued on the
 * instance that does NOT own the target socket reaches the owning instance,
 * which hard-closes the socket (Q2), stops delivering channel events to it, and
 * drops it from the authoritative roster; presence subscribers on BOTH instances
 * observe a `left` for the evicted member. The issuing (non-owning) instance
 * applies only the roster/`left` consequence — it never held the socket to close.
 *
 * @module @lockness/realtime/tests/eviction_control
 */

import { assert, assertEquals } from '@std/assert'
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
    readonly _frames: Record<string, unknown>[]
    readonly _closed: number
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    const frames: Array<Record<string, unknown>> = []
    let closed = 0
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void frames.push(JSON.parse(d as string)),
        close: () => void closed++,
        get _frames() {
            return frames
        },
        get _closed() {
            return closed
        },
    } as Connection<User> & Spy
}
const framesOf = (c: Connection<User>) => (c as unknown as Spy)._frames
const closedOf = (c: Connection<User>) => (c as unknown as Spy)._closed

const authorize = (id: User | null): PresenceMember | false =>
    id ? { id: id.id, info: { name: id.name } } : false

function instance(redis: FakeRedis) {
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        {
            prefix: 'app:rt',
            control: { secret: 'deployment-secret-with-enough-entropy' },
        },
    )
    const manager = new ChannelManager<User>({ driver, authorize })
    return { manager, driver }
}

const leftFor = (c: Connection<User>, memberId: number) =>
    framesOf(c).find((f) =>
        f.action === 'left' &&
        (f.member as PresenceMember | undefined)?.id === memberId
    )

/**
 * Drain the microtasks the owning instance's revoke queues. The evict reaches B
 * over the (synchronous) fake bus, but B's teardown — roster removal and the
 * cross-instance `presence-leave` — settles in the microtask queue right after
 * (every fake command resolves synchronously), so a bounded flush makes the
 * cross-process consequences deterministic.
 */
async function flushMicrotasks(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

Deno.test('SC-003: an evict on a non-owning instance revokes the owning socket cross-process', async () => {
    const redis = new FakeRedis()
    const a = instance(redis)
    const b = instance(redis)
    try {
        // W watches presence on A; V watches on B; X holds its socket on B.
        const w = fakeConn('w', { id: 10, name: 'Wendy' })
        const v = fakeConn('v', { id: 20, name: 'Victor' })
        const x = fakeConn('x', { id: 1, name: 'Xavier' })
        await a.manager.subscribe(w, 'presence-lobby')
        await b.manager.subscribe(v, 'presence-lobby')
        await b.manager.subscribe(x, 'presence-lobby')

        // X is authoritatively "here" before the evict.
        assert(
            (await b.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            'X should be on the roster before the evict',
        )

        // The evict is issued on A — the instance that does NOT own X's socket.
        await a.manager.evict('x')
        await flushMicrotasks()

        // The owning instance (B) hard-closed X's socket (Q2).
        assertEquals(closedOf(x), 1, 'B should have hard-closed X')

        // X is gone from the authoritative roster.
        assertEquals(
            (await b.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            false,
            'X should be absent from the roster after the evict',
        )

        // Presence subscribers on BOTH instances saw a `left` for X (member 1).
        assert(leftFor(w, 1), 'W (on A) should have seen X leave')
        assert(leftFor(v, 1), 'V (on B) should have seen X leave')

        // X stops receiving channel events after the evict.
        framesOf(x).length = 0
        a.manager.broadcast('presence-lobby', 'msg', { text: 'after' })
        await Promise.resolve()
        assertEquals(
            framesOf(x).some((f) => f.type === 'event'),
            false,
            'the evicted socket must receive no further channel events',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})
