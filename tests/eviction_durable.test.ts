/**
 * @fileoverview SC-007 — durable, reconciled revocation (FR-014).
 *
 * Eviction is fire-and-forget over at-most-once pub/sub while authorization is
 * cached at subscribe, so a lost evict control frame would silently reopen the
 * S7 disclosure gap. The Redis-backed revocation marker closes it: an evict
 * whose control frame never reached the owning instance (its subscribe socket
 * was between reconnects) is recovered by the owning instance's periodic
 * reconcile, which re-checks the durable marker and revokes any local member it
 * names. Driven by `FakeTime`, so no test waits on real elapsed time.
 *
 * @module @lockness/realtime/tests/eviction_durable
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
            presence: {
                livenessTtlSeconds: 30,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
    const manager = new ChannelManager<User>({ driver, authorize })
    return { manager, driver }
}

/** Drain the microtasks a reconcile pass queues (each fake command resolves sync). */
async function flushMicrotasks(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

Deno.test('SC-007: an evict lost while the owning socket was disconnected still revokes on the next reconcile', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    const b = instance(redis)
    try {
        const x = fakeConn('x', { id: 1, name: 'Xavier' })
        await b.manager.subscribe(x, 'presence-lobby')
        assert(
            (await b.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            'X should be on the roster before the evict',
        )

        // An evict is issued elsewhere and durably recorded, but its control
        // frame never reached B (B's subscribe socket was between reconnects).
        await b.driver.markRevoked('x')
        assertEquals(
            closedOf(x),
            0,
            'X is not revoked before the reconcile runs',
        )

        // B's periodic reconcile fires: it re-checks the durable marker and
        // revokes X — recovered, not lost.
        await time.tickAsync(1_200)
        await flushMicrotasks()

        assertEquals(
            closedOf(x),
            1,
            'the missed evict is recovered on reconcile',
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

Deno.test('SC-007: a presence-free instance still reconciles a durable revocation (no presence member ever added)', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    const b = instance(redis)
    try {
        // A deployment that only ever serves private/public channels: the
        // connection is owned locally but NO presence member is added, so the
        // presence ghost-sweep timer never starts. The revocation reconcile must
        // NOT be coupled to that timer, or this whole deployment class never
        // recovers a lost evict — reopening S7, the exact gap FR-014 closes.
        const y = fakeConn('y', { id: 2, name: 'Yves' })
        b.manager.register(y)

        // An evict is issued and durably recorded elsewhere, but its control
        // frame never reached B (B's subscribe socket was between reconnects).
        await b.driver.markRevoked('y')
        assertEquals(
            closedOf(y),
            0,
            'Y is not revoked before the reconcile runs',
        )

        // B's periodic revocation reconcile fires on its OWN cadence — with no
        // presence member ever added — re-checks the durable marker and revokes
        // Y. Recovered, not lost.
        await time.tickAsync(1_200)
        await flushMicrotasks()

        assertEquals(
            closedOf(y),
            1,
            'the missed evict is recovered even with no presence member',
        )
    } finally {
        await b.driver.close()
        time.restore()
    }
})
