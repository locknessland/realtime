/**
 * @fileoverview SC-005 — the instance-scoped ghost-member sweep (Q1/FR-008).
 *
 * Each instance refreshes ONE liveness key on a heartbeat; a surviving
 * instance's reconcile pass sweeps the roster members of any instance whose
 * liveness key has expired. When an instance "crashes" (stops heartbeating) its
 * members do not linger as permanent ghosts. Driven by `FakeTime`, so no test
 * waits on real elapsed time.
 *
 * @module @lockness/realtime/tests/presence_sweep
 */

import { assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { FakeRedis } from './fake_redis.ts'

function driver(redis: FakeRedis) {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        {
            prefix: 'app:rt',
            presence: {
                livenessTtlSeconds: 2,
                heartbeatIntervalMs: 500,
                reconcileIntervalMs: 1000,
            },
        },
    )
}

const ids = (members: { id: string | number }[]) =>
    members.map((m) => m.id).sort()

/**
 * Drain the resolved-promise microtasks a reconcile pass queues. `tickAsync`
 * fires the reconcile interval but does not await its multi-`await` body, so the
 * roster round-trips settle in the microtask queue right after — a bounded loop
 * flushes them deterministically (every fake command resolves synchronously).
 */
async function flushMicrotasks(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

Deno.test('SC-005: a crashed instance leaves no permanent ghost roster members', async () => {
    const redis = new FakeRedis()
    const time = new FakeTime(new Date('2026-09-05T10:00:00Z'))
    const a = driver(redis)
    const b = driver(redis)
    try {
        await a.addMember('presence-lobby', { id: 1, info: { name: 'A' } })
        await b.addMember('presence-lobby', { id: 2, info: { name: 'B' } })

        // Both members are authoritatively "here" from either instance's view.
        assertEquals(ids(await b.listMembers('presence-lobby')), [1, 2])

        // Instance A crashes: it stops heartbeating (close clears its timers) but
        // leaves its roster entries behind, exactly as a real crash would.
        await a.close()

        // Advance past A's liveness TTL: A's key expires and B's reconcile sweeps
        // A's ghost member (member 1) out of the authoritative roster.
        await time.tickAsync(3_500)
        await flushMicrotasks()

        assertEquals(ids(await b.listMembers('presence-lobby')), [2])
    } finally {
        await b.close()
        time.restore()
    }
})
