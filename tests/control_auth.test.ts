/**
 * @fileoverview SC-008 — the control-plane forgery matrix (FR-015/FR-019).
 *
 * The reserved control topic is isolation-by-convention only; anyone with bus
 * `PUBLISH` can put a well-formed frame on it. Authenticity is the sole gate: a
 * frame whose MAC is absent, forged, or signed with the wrong secret — and a
 * frame whose routing names are out of charset — is dropped on ingest and NEVER
 * actioned. This proves the matrix end-to-end by publishing raw frames directly
 * onto the bus (bypassing `publishControl`, exactly as an attacker would) and
 * asserting neither the driver `onControl` seam nor the manager ever obeys them:
 * no forged evict closes a socket, no spoofed `presence-join` injects a member.
 *
 * @module @lockness/realtime/tests/control_auth
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { ControlMessage } from '../driver.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'
import { FakeRedis } from './fake_redis.ts'

const CONTROL_TOPIC = 'app:rt__control'

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

function driver(
    redis: FakeRedis,
    secret = 'deployment-secret-with-enough-entropy',
) {
    return new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: 'app:rt', control: { secret } },
    )
}

/**
 * Publish a raw wire frame directly onto the control topic, as an attacker would.
 *
 * `ts` and `nonce` are filled in with VALID values unless the caller overrides
 * them (#272/FR-014). Without that, every forgery below would be dropped at the
 * shape gate for missing those fields and would never reach the MAC check it
 * exists to exercise — the tests would keep passing for a completely different
 * reason, and the FR-015 forgery matrix would be silently lost.
 */
function forge(
    redis: FakeRedis,
    wire: Record<string, unknown>,
): Promise<unknown> {
    const framed = {
        ts: Date.now(),
        nonce: 'a1b2c3d4'.repeat(4),
        ...wire,
    }
    return redis.command('PUBLISH', CONTROL_TOPIC, JSON.stringify(framed))
}

/** Run `body` with `console.warn` captured, restoring it even on a throw. */
async function warningsFrom(body: () => Promise<void>): Promise<string[]> {
    const captured: string[] = []
    const real = console.warn
    console.warn = (...args: unknown[]) => void captured.push(String(args[0]))
    try {
        await body()
    } finally {
        console.warn = real
    }
    return captured
}

Deno.test('SC-008: an absent-MAC control frame is dropped and never obeyed', async () => {
    const redis = new FakeRedis()
    const b = driver(redis)
    try {
        const got: ControlMessage[] = []
        b.onControl((c) => got.push(c))
        const warnings = await warningsFrom(async () => {
            await forge(redis, {
                kind: 'evict',
                target: 'victim',
                origin: 'atk',
            })
        })
        assertEquals(got.length, 0)
        // An absent MAC fails the SHAPE gate, not the MAC gate — `mac` must be
        // a string before there is anything to compare. Stated so the file's
        // five tests read as a matrix of distinct gates rather than five ways
        // of saying "nothing happened".
        assert(
            warnings.some((w) => w.includes('invalid shape')),
            `dropped at the SHAPE gate. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await b.close()
    }
})

Deno.test('SC-008/FR-014: a forged-MAC evict is dropped AT THE MAC GATE', async () => {
    const redis = new FakeRedis()
    const b = driver(redis)
    try {
        const got: ControlMessage[] = []
        b.onControl((c) => got.push(c))
        const warnings = await warningsFrom(async () => {
            await forge(redis, {
                kind: 'evict',
                target: 'victim',
                origin: 'atk',
                mac: 'deadbeef'.repeat(8),
            })
        })
        assertEquals(got.length, 0)
        // FR-014: asserting the REASON, not just the outcome. #272 added a
        // shape gate above the MAC check, and without this assertion a forgery
        // that started failing there instead would look identical from here —
        // green, and no longer testing forgery at all.
        assert(
            warnings.some((w) =>
                w.includes('absent/invalid') && w.includes('MAC')
            ),
            `dropped at the MAC gate, not an earlier one. Got: ${
                warnings.join(' | ')
            }`,
        )
    } finally {
        await b.close()
    }
})

Deno.test('SC-008: a spoofed presence-join member is dropped and never obeyed', async () => {
    const redis = new FakeRedis()
    const b = driver(redis)
    try {
        const got: ControlMessage[] = []
        b.onControl((c) => got.push(c))
        const warnings = await warningsFrom(async () => {
            await forge(redis, {
                kind: 'presence-join',
                target: 'ghost',
                channel: 'presence-lobby',
                member: { id: 999, info: { name: 'Impostor' } },
                origin: 'atk',
                mac: 'f'.repeat(64),
            })
        })
        assertEquals(got.length, 0)
        // FR-014: pin the GATE, not just the outcome. One more constraint on
        // the pre-MAC shape check would move this frame there, and this test
        // would keep passing while no longer testing forgery at all.
        assert(
            warnings.some((w) =>
                w.includes('absent/invalid') && w.includes('MAC')
            ),
            `dropped at the MAC gate. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await b.close()
    }
})

Deno.test('SC-008: end-to-end — a forged evict on the bus never closes an owned socket', async () => {
    const redis = new FakeRedis()
    const b = driver(redis)
    const manager = new ChannelManager<User>({ driver: b, authorize })
    try {
        const x = fakeConn('x', { id: 1, name: 'Xavier' })
        await manager.subscribe(x, 'presence-lobby')

        // A forged evict for X's socket, published straight onto the bus.
        const warnings = await warningsFrom(async () => {
            await forge(redis, {
                kind: 'evict',
                target: 'x',
                origin: 'atk',
                mac: '0'.repeat(64),
            })
            await Promise.resolve()
        })
        assert(
            warnings.some((w) =>
                w.includes('absent/invalid') && w.includes('MAC')
            ),
            `dropped at the MAC gate. Got: ${warnings.join(' | ')}`,
        )

        assertEquals(closedOf(x), 0, 'a forged evict must never close a socket')
        assertEquals(
            (await b.listMembers('presence-lobby')).some((m) => m.id === 1),
            true,
            'a forged evict must never drop a roster member',
        )
    } finally {
        await b.close()
    }
})

Deno.test('SC-008/FR-019: a validly-signed evict with an out-of-charset target is dropped', async () => {
    const redis = new FakeRedis()
    const b = driver(redis)
    try {
        const got: ControlMessage[] = []
        b.onControl((c) => got.push(c))
        // Signed by a legitimate sender, but the target id is out of charset —
        // the ingest name check drops it even though the MAC verifies.
        const warnings = await warningsFrom(async () => {
            await driver(redis).publishControl({
                kind: 'evict',
                target: 'bad target!<script>',
            })
        })
        assertEquals(got.length, 0)
        // This one must reach the NAME gate — past the shape gate and past a
        // MAC that genuinely verifies. Pinning it proves the name check is
        // still the thing doing the work.
        assert(
            warnings.some((w) => w.includes('invalid name')),
            `dropped at the NAME gate. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await b.close()
    }
})
