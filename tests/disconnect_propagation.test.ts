/**
 * @fileoverview US4 — a natural disconnect propagates the leave cross-process.
 *
 * A presence member's socket closes on its owning instance; that instance must
 * remove the member from the authoritative roster AND fan a `left` to presence
 * subscribers on EVERY instance, not only its own. Two `ChannelManager`s over
 * one fake Redis prove the cross-instance `left`: the disconnect on B is seen by
 * a presence subscriber on A through the authenticated control plane.
 *
 * @module @lockness/realtime/tests/disconnect_propagation
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

function fakeConn(id: string, identity: User | null): Connection<User> {
    const frames: Array<Record<string, unknown>> = []
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void frames.push(JSON.parse(d as string)),
        close: () => {},
        get _frames() {
            return frames
        },
    } as Connection<User> & { readonly _frames: Record<string, unknown>[] }
}
const framesOf = (c: Connection<User>) =>
    (c as unknown as { _frames: Record<string, unknown>[] })._frames

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

Deno.test('US4: a disconnect on the owning instance fans a `left` to presence subscribers on all instances', async () => {
    const redis = new FakeRedis()
    const a = instance(redis)
    const b = instance(redis)
    try {
        const w = fakeConn('w', { id: 10, name: 'Wendy' }) // watcher on A
        const x = fakeConn('x', { id: 1, name: 'Xavier' }) // member on B
        await a.manager.subscribe(w, 'presence-lobby')
        await b.manager.subscribe(x, 'presence-lobby')

        assert(
            (await a.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            'X should be on the authoritative roster before the disconnect',
        )

        // X's socket closes on its owning instance B.
        await b.manager.disconnect('x')

        // X is removed from the authoritative roster.
        assertEquals(
            (await a.driver.listMembers('presence-lobby')).some((m) =>
                m.id === 1
            ),
            false,
            'X should be gone from the authoritative roster',
        )

        // The watcher on the OTHER instance (A) saw X leave.
        assert(leftFor(w, 1), 'W (on A) should have seen X leave cross-process')
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})
