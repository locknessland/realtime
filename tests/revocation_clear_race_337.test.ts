/**
 * @fileoverview #337 — a clear for an older revocation must never erase a newer
 * one for the same connection and channel.
 *
 * #332 clears a channel-scoped record once the owning instance has applied it,
 * so a record means *"a revocation the owner has not applied yet"*. The clear
 * used to be keyed on the pair alone. So a second `revokeChannel` for the same
 * pair, written **between** the first one's apply and its clear, was deleted by
 * that clear — and when the second one's control frame was lost, which is the
 * one case the record exists for, nothing ever enforced it. The caller of the
 * second `revokeChannel` saw it succeed.
 *
 * **The interleaving is driven, never hoped for.** Real `RedisBroadcastDriver`
 * instances share one `FakeRedis` and one control secret, so no stub stands in
 * for the code under test. Each instance's command client is wrapped so the
 * test can drop one chosen control `PUBLISH` and hold one `ZREM` behind a
 * deferred promise. Nothing waits on a clock: the reconcile is fired through
 * the subscriber's reconnect seam, which runs the same pass as the timer.
 *
 * **The witness never names a revocation id.** That is what lets the same file
 * run red against the pair-keyed clear and green against an exact one.
 *
 * @module @lockness/realtime/tests/revocation_clear_race_337
 */

import { assert, assertEquals } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

interface User {
    id: number
}

const SECRET = 'a-deployment-secret-with-more-than-enough-entropy'
const PREFIX = 'app:rt'
const INDEX = `${PREFIX}__revocations`
const CONTROL_TOPIC = `${PREFIX}__control`
const ROOM = 'private-room'

/** Run the microtask queue out; a control frame is applied as `void`. */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

function conn(id: string): Connection<User> & {
    readonly received: Record<string, unknown>[]
} {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Connection<User> & { readonly received: Record<string, unknown>[] }
}

/**
 * A command client over the shared fake that can drop the next control
 * `PUBLISH` and hold the next `ZREM`.
 *
 * Both are one-shot and armed explicitly, so every other command — the marks,
 * the reads, the event publishes — goes straight through. A held `ZREM` is
 * executed against the store only when released, which is exactly "a clear
 * issued before the newer write and landing after it".
 */
function interposed(redis: FakeRedis) {
    let dropNextControl = false
    let holdNextZrem = false
    let held: { run: () => void } | undefined

    const command = (...args: string[]): Promise<unknown> => {
        if (
            dropNextControl && args[0] === 'PUBLISH' &&
            args[1] === CONTROL_TOPIC
        ) {
            dropNextControl = false
            return Promise.resolve({ type: 'integer', value: 0 })
        }
        if (holdNextZrem && args[0] === 'ZREM') {
            holdNextZrem = false
            return new Promise((resolve, reject) => {
                held = {
                    run: () =>
                        void redis.command(...args).then(resolve, reject),
                }
            })
        }
        return redis.command(...args)
    }
    return {
        client: { command },
        dropNextControl: () => void (dropNextControl = true),
        holdNextZrem: () => void (holdNextZrem = true),
        /**
         * Whether a `ZREM` is being held. A boolean rather than a promise to
         * await: a mutant that never issues the clear would leave such a
         * promise pending forever, and the drivers' timers keep the event loop
         * alive, so the run would hang instead of failing.
         */
        zremHeld: () => held !== undefined,
        release: () => {
            assert(held !== undefined, 'a ZREM was held before release')
            held.run()
        },
    }
}

function instance(redis: FakeRedis) {
    const bus = interposed(redis)
    const subscriber = redis.subscriberFor()
    const driver = new RedisBroadcastDriver(bus.client, subscriber, {
        prefix: PREFIX,
        control: { secret: SECRET },
        revocationTtlSeconds: 300,
    })
    const manager = new ChannelManager<User>({
        driver,
        authorize: () => true,
    })
    return { bus, subscriber, driver, manager }
}

Deno.test('#337 WITNESS: an in-flight clear for an older revocation does not erase a newer one', async () => {
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const a = instance(redis)
    const b = instance(redis)
    try {
        // 1. B owns c1 in ROOM.
        const victim = conn('c1')
        b.manager.register(victim)
        assertEquals((await b.manager.subscribe(victim, ROOM)).ok, true)

        // 2. A revokes it. The frame reaches B, B applies with 'left', and B's
        //    clear is HELD before it reaches the store.
        b.bus.holdNextZrem()
        assertEquals(await a.manager.revokeChannel('c1', ROOM), 'not-owned')
        await settle()
        assert(
            b.bus.zremHeld(),
            'precondition: B issued its clear, and it is held',
        )
        assertEquals(
            victim.received.filter((f) => f.type === 'unsubscribed').length,
            1,
            'precondition: B applied the first revocation',
        )

        // 3. c1 re-subscribes on B. A revocation is not a ban.
        assertEquals((await b.manager.subscribe(victim, ROOM)).ok, true)
        // Positive control for step 7: a broadcast DOES reach a member, so
        // "it did not arrive" there cannot pass because delivery is broken.
        b.manager.broadcast(ROOM, 'probe', {})
        await settle()
        assertEquals(
            victim.received.filter((f) => f.event === 'probe').length,
            1,
            'precondition: a ROOM broadcast reaches a subscribed c1',
        )

        // 4. A revokes it AGAIN, and this frame is lost on the bus.
        a.bus.dropNextControl()
        assertEquals(await a.manager.revokeChannel('c1', ROOM), 'not-owned')
        await settle()
        assertEquals(
            victim.received.filter((f) => f.type === 'unsubscribed').length,
            1,
            'precondition: the second frame was lost, so B has not acted on it',
        )

        // 5. B's clear for the FIRST revocation finally lands.
        b.bus.release()
        await settle()

        // 6. B's reconcile runs — the backstop for exactly this lost frame.
        await b.subscriber.fireReconnect()
        await settle()

        // 7. The second revocation is enforced, and nothing is left behind.
        const before = victim.received.length
        b.manager.broadcast(ROOM, 'tick', { n: 1 })
        await settle()
        assertEquals(
            victim.received.slice(before).filter((f) => f.event === 'tick')
                .length,
            0,
            "THE SECOND REVOCATION WAS NEVER ENFORCED: the first revocation's " +
                'clear erased its durable record, and its frame was lost — so ' +
                'c1 is still in the room although revokeChannel succeeded',
        )
        assertEquals(
            redis.zcard(INDEX),
            0,
            'and every applied record was cleared',
        )
        redis.assertNoRejections()
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('#337 two marks for one pair in the SAME second: clearing the first leaves the second', async () => {
    // `TIME` is whole seconds and both marks use the same TTL, so the two
    // records share a score. That is what rules out comparing the score as
    // the clear's guard: the compare would match and delete the newer record.
    // Only an identity inside the member tells the two apart.
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, revocationTtlSeconds: 300 },
    )
    try {
        const first = { target: 'c1', channel: ROOM, id: crypto.randomUUID() }
        const second = { target: 'c1', channel: ROOM, id: crypto.randomUUID() }
        await driver.markRevocation(first)
        await driver.markRevocation(second)
        assertEquals(
            redis.zcard(INDEX),
            2,
            'two revocations of one pair are TWO records — `ZADD GT` adds a ' +
                'new member; GT only governs an update',
        )

        await driver.clearRevocation(first)

        assertEquals(
            await driver.listRevocations(),
            [second],
            'the clear removed exactly the first record; the second, written ' +
                'in the same second with the same expiry, survives',
        )
        redis.assertNoRejections()
    } finally {
        await driver.close()
    }
})

Deno.test('#337 two records for one pair: ONE kick, both cleared, and no kick on the next tick', async () => {
    // Both frames lost, so the reconcile meets two records for one pair. One
    // leave settles both. Applied one record at a time, the first leaves and
    // clears its own record, the second finds 'not-subscribed' and survives —
    // and the next tick kicks a client that has legitimately re-subscribed.
    const redis = new FakeRedis()
    redis.setTime(1_000)
    const a = instance(redis)
    const b = instance(redis)
    try {
        const victim = conn('c1')
        b.manager.register(victim)
        await b.manager.subscribe(victim, ROOM)
        for (let i = 0; i < 2; i++) {
            a.bus.dropNextControl()
            assertEquals(await a.manager.revokeChannel('c1', ROOM), 'not-owned')
        }
        await settle()
        assertEquals(redis.zcard(INDEX), 2, 'precondition: two records')

        await b.subscriber.fireReconnect()
        await settle()
        const kicks = () =>
            victim.received.filter((f) => f.type === 'unsubscribed').length
        assertEquals(kicks(), 1, 'the pair left once, and was told once')
        assertEquals(
            redis.zcard(INDEX),
            0,
            'EVERY id listed for the pair was cleared by that one leave',
        )

        assertEquals((await b.manager.subscribe(victim, ROOM)).ok, true)
        await b.subscriber.fireReconnect()
        await settle()
        assertEquals(
            kicks(),
            1,
            'the re-subscribed client is NOT kicked again on the next tick',
        )
        const before = victim.received.length
        b.manager.broadcast(ROOM, 'tick', {})
        await settle()
        assertEquals(
            victim.received.slice(before).filter((f) => f.event === 'tick')
                .length,
            1,
            'and it is really back in the room',
        )
        redis.assertNoRejections()
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('#337 a revoke-channel frame with NO revocation id is dropped', async () => {
    // A frame that names no record gives the owner nothing exact to clear,
    // and clearing by pair is the defect #337 removed. The durable record, if
    // one was written, carries its id and is recovered by the reconcile.
    let deliver: ((control: ControlMessage) => void) | undefined
    const cleared: unknown[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: (revocation) => {
            cleared.push(revocation)
            return Promise.resolve()
        },
    }
    const m = new ChannelManager<User>({ driver, authorize: () => true })
    const victim = conn('c1')
    m.register(victim)
    await m.subscribe(victim, ROOM)
    assert(deliver !== undefined, 'the control seam was registered')
    const send = deliver as (control: ControlMessage) => void

    send({ kind: 'revoke-channel', target: 'c1', channel: ROOM })
    await settle()

    assertEquals(cleared, [], 'nothing was cleared')
    assertEquals(
        await m.unsubscribe('c1', ROOM),
        'left',
        'and the membership was not touched — the frame was dropped',
    )
})
