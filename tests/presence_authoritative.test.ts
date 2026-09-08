/**
 * @fileoverview SC-002 — consistent authoritative presence across instances.
 *
 * Two `ChannelManager`s wired to two `RedisBroadcastDriver`s sharing one fake
 * Redis (roster store + control bus) join the same presence channel. The roster
 * each client is handed lists every instance's members, and a join announced on
 * one instance reaches presence subscribers on the other. Every assertion reads
 * only `SubscribeResult.members` and the frames a connection received — never an
 * in-process map.
 *
 * @module @lockness/realtime/tests/presence_authoritative
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

const joinedFor = (c: Connection<User>, memberId: number) =>
    framesOf(c).find((f) =>
        f.action === 'joined' &&
        (f.member as PresenceMember | undefined)?.id === memberId
    )

Deno.test('SC-002: roster is cross-instance authoritative and a join crosses instances', async () => {
    const redis = new FakeRedis()
    const a = instance(redis)
    const b = instance(redis)
    try {
        const x = fakeConn('x', { id: 1, name: 'Xavier' })
        const y = fakeConn('y', { id: 2, name: 'Yolanda' })

        // X joins on instance A (first member of the channel).
        const rx = await a.manager.subscribe(x, 'presence-lobby')
        assertEquals(rx.members?.map((m) => m.id).sort(), [1])

        // Y joins on instance B — its roster snapshot lists BOTH X and Y, proving
        // instance B reads instance A's member from the authoritative store.
        const ry = await b.manager.subscribe(y, 'presence-lobby')
        assertEquals(ry.members?.map((m) => m.id).sort(), [1, 2])

        // X (on A) received a `joined` for Y (announced on B) — B → A direction.
        assert(joinedFor(x, 2), 'X should have seen Y join across instances')

        // A third client joins on A; Y (on B) sees it — A → B direction, and A's
        // fresh snapshot lists every instance's members (X, Y, Z).
        const z = fakeConn('z', { id: 3, name: 'Zoe' })
        const rz = await a.manager.subscribe(z, 'presence-lobby')
        assertEquals(rz.members?.map((m) => m.id).sort(), [1, 2, 3])
        assert(joinedFor(y, 3), 'Y should have seen Z join across instances')

        // A member never receives a `joined` for itself.
        assertEquals(joinedFor(x, 1), undefined)
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})
