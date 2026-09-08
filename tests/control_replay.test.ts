/**
 * @fileoverview US1/US2/US3 (#272) — a captured control frame is refused on
 * re-publish, and legitimate traffic is not.
 *
 * These drive the whole ingest path through a real {@link RedisBroadcastDriver}
 * over the in-process bus, rather than exercising `ControlReplayWindow`
 * directly — that class has its own unit tests. What is under test here is the
 * wiring: that the window is consulted, consulted in the right place (after the
 * MAC), and that its verdict actually stops the frame.
 *
 * The attacker's move is modelled honestly. They do not forge anything: they
 * capture the exact bytes a legitimate instance published and PUBLISH them
 * again. Every replay below is a byte-identical copy of a frame that was
 * accepted the first time.
 *
 * @module @lockness/realtime/tests/control_replay
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import type { ControlMessage } from '../driver.ts'
import { FakeRedis } from './fake_redis.ts'

const PREFIX = 'app:rt'
const CONTROL_TOPIC = `${PREFIX}__control`
const SECRET = 'deployment-secret-with-enough-entropy'

/** A receiving instance, with everything its `onControl` seam was handed. */
function receiver(redis: FakeRedis, windowMs?: number, maxEntries?: number) {
    const driver = new RedisBroadcastDriver(
        { command: redis.command },
        redis.subscriberFor(),
        { prefix: PREFIX, control: { secret: SECRET, windowMs, maxEntries } },
    )
    const obeyed: ControlMessage[] = []
    driver.onControl((control) => obeyed.push(control))
    return { driver, obeyed }
}

/** A publishing instance whose control frames are captured off the wire. */
function publisher(redis: FakeRedis) {
    const captured: string[] = []
    const command = (...args: string[]): Promise<unknown> => {
        if (args[0] === 'PUBLISH' && args[1] === CONTROL_TOPIC) {
            captured.push(args[2])
        }
        return redis.command(...args)
    }
    const driver = new RedisBroadcastDriver(
        { command },
        redis.subscriberFor(),
        { prefix: PREFIX, control: { secret: SECRET } },
    )
    return { driver, captured }
}

/** Re-publish captured bytes verbatim — the attacker's entire capability. */
function replay(redis: FakeRedis, frame: string): Promise<unknown> {
    return redis.command('PUBLISH', CONTROL_TOPIC, frame)
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

Deno.test('US1: a captured frame is obeyed ONCE — the replay is refused', async () => {
    const redis = new FakeRedis()
    const a = publisher(redis)
    const b = receiver(redis)
    try {
        await a.driver.publishControl({ kind: 'evict', target: 'victim' })
        assertEquals(a.captured.length, 1, 'one frame reached the wire')
        assertEquals(b.obeyed.length, 1, 'and B obeyed it, as it should')

        // Byte-for-byte the same frame. No forgery, no secret required.
        const warnings = await warningsFrom(async () => {
            await replay(redis, a.captured[0])
            await replay(redis, a.captured[0])
            await replay(redis, a.captured[0])
        })

        assertEquals(
            b.obeyed.length,
            1,
            'three replays of a frame B already obeyed change nothing',
        )
        assert(
            warnings.some((w) => w.includes('DUPLICATE')),
            `the drop names duplication. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('US2: a frame older than the window is refused by an instance that never saw it', async () => {
    const redis = new FakeRedis()
    const a = publisher(redis)
    try {
        await a.driver.publishControl({ kind: 'evict', target: 'victim' })
        const frame = a.captured[0]

        // A receiver built AFTER the frame was issued, with an empty store and
        // a window so short the frame is already outside it. This is the case
        // the nonce cannot cover — a restarted or newly-started instance holds
        // no nonce for it, so only the timestamp protects it (S3).
        await new Promise((resolve) => setTimeout(resolve, 15))
        const late = receiver(redis, 10)
        try {
            const warnings = await warningsFrom(async () => {
                await replay(redis, frame)
            })
            assertEquals(
                late.obeyed.length,
                0,
                'an instance with an EMPTY replay store still refuses it',
            )
            assert(
                warnings.some((w) => w.includes('STALE')),
                `the drop names staleness. Got: ${warnings.join(' | ')}`,
            )
        } finally {
            await late.driver.close()
        }
    } finally {
        await a.driver.close()
    }
})

Deno.test('US3: two instances publishing concurrently are both obeyed', async () => {
    // The regression that would fail CLOSED — worse than the replay this
    // feature prevents. Keyed on the nonce alone rather than on
    // (origin, nonce), the second publisher's frame is discarded as the
    // first's duplicate, silently and with no retry.
    const redis = new FakeRedis()
    const a = publisher(redis)
    const b = publisher(redis)
    const c = receiver(redis)
    try {
        await a.driver.publishControl({ kind: 'evict', target: 'from-a' })
        await b.driver.publishControl({ kind: 'evict', target: 'from-b' })

        assertEquals(
            c.obeyed.map((m) => m.target).sort(),
            ['from-a', 'from-b'],
            'both instances got through',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
        await c.driver.close()
    }
})

Deno.test('US3: a legitimate stream of frames is never dropped', async () => {
    const redis = new FakeRedis()
    const a = publisher(redis)
    const b = receiver(redis)
    try {
        for (let i = 0; i < 25; i++) {
            await a.driver.publishControl({ kind: 'evict', target: `t${i}` })
        }
        assertEquals(
            b.obeyed.length,
            25,
            'every distinct frame is obeyed — no nonce collision across a run',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('FR-011: an oversized control payload is dropped before it is parsed', async () => {
    const redis = new FakeRedis()
    const b = receiver(redis)
    try {
        // Costs the receiver a length check and nothing else — no parse, no
        // re-serialise, and above all no synchronous pure-JS SHA-256 over
        // attacker-chosen bytes.
        const warnings = await warningsFrom(async () => {
            await replay(redis, JSON.stringify({ pad: 'x'.repeat(20_000) }))
        })
        assertEquals(b.obeyed.length, 0)
        assert(
            warnings.some((w) => w.includes('oversized')),
            `the drop names its size. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await b.driver.close()
    }
})

Deno.test('FR-012: a non-integer timestamp is refused at the shape gate', async () => {
    const redis = new FakeRedis()
    const b = receiver(redis)
    try {
        // `1e400` is valid JSON and parses to Infinity, which JSON.stringify
        // then collapses to `null` — so Infinity, -Infinity and null would all
        // canonicalise identically and share one MAC.
        const warnings = await warningsFrom(async () => {
            await redis.command(
                'PUBLISH',
                CONTROL_TOPIC,
                '{"kind":"evict","target":"v","origin":"atk","mac":"' +
                    'f'.repeat(64) + '","ts":1e400,"nonce":"' +
                    'a'.repeat(32) + '"}',
            )
        })
        assertEquals(b.obeyed.length, 0)
        // Asserting the REASON, not just the outcome. This frame also carries a
        // bogus MAC, so "not obeyed" is satisfied by the MAC gate whether or not
        // the shape gate ever looked at `ts` — which is exactly how this test
        // passed against a mutation that replaced Number.isInteger with a plain
        // typeof check.
        assert(
            warnings.some((w) => w.includes('invalid shape')),
            `dropped at the SHAPE gate, before the MAC. Got: ${
                warnings.join(' | ')
            }`,
        )
    } finally {
        await b.driver.close()
    }
})

Deno.test('FR-012: an object nonce is refused at the shape gate', async () => {
    const redis = new FakeRedis()
    const b = receiver(redis)
    try {
        // Used as a store key an object compares by identity, so every replay
        // would be a fresh key: duplicate detection fails silently while the
        // store grows without bound.
        const warnings = await warningsFrom(async () => {
            await replay(
                redis,
                JSON.stringify({
                    kind: 'evict',
                    target: 'v',
                    origin: 'atk',
                    mac: 'f'.repeat(64),
                    ts: Date.now(),
                    nonce: { toString: 'not-a-string' },
                }),
            )
        })
        assertEquals(b.obeyed.length, 0)
        assert(
            warnings.some((w) => w.includes('invalid shape')),
            `dropped at the SHAPE gate, before the MAC. Got: ${
                warnings.join(' | ')
            }`,
        )
    } finally {
        await b.driver.close()
    }
})

// ---------------------------------------------------------------------------
// FR-011 — isPlainMember, the pre-MAC guard on the one field that can be large
// ---------------------------------------------------------------------------

Deno.test('FR-011: a member that is not a plain object is refused at the shape gate', async () => {
    // Every rejection branch of `isPlainMember`. Without these, replacing the
    // whole conjunct with `true` leaves the suite green — the guard exists to
    // stop attacker-chosen bytes reaching a synchronous pure-JS SHA-256, and an
    // untested guard is one that can be deleted by accident.
    const cases: ReadonlyArray<[string, unknown]> = [
        ['an array', [1, 2, 3]],
        ['a string', 'not-an-object'],
        ['a number', 42],
        ['null id', { id: null }],
        ['a missing id', { info: { name: 'x' } }],
        ['an array info', { id: 1, info: [1, 2, 3] }],
        ['an extra key', { id: 1, info: {}, smuggled: 'x'.repeat(100) }],
    ]

    for (const [label, member] of cases) {
        const redis = new FakeRedis()
        const b = receiver(redis)
        try {
            const warnings = await warningsFrom(async () => {
                await replay(
                    redis,
                    JSON.stringify({
                        kind: 'presence-join',
                        target: 'v',
                        channel: 'presence-lobby',
                        member,
                        origin: 'atk',
                        mac: 'f'.repeat(64),
                        ts: Date.now(),
                        nonce: 'a'.repeat(32),
                    }),
                )
            })
            assertEquals(b.obeyed.length, 0, `${label} was obeyed`)
            assert(
                warnings.some((w) => w.includes('invalid shape')),
                `${label} must be refused at the SHAPE gate, before any ` +
                    `hashing. Got: ${warnings.join(' | ')}`,
            )
        } finally {
            await b.driver.close()
        }
    }
})

Deno.test('FR-011: a legitimate member shape still passes the guard', async () => {
    // The negative control. A guard that rejected everything would satisfy the
    // test above perfectly and break the entire presence control plane.
    const redis = new FakeRedis()
    const a = publisher(redis)
    const b = receiver(redis)
    try {
        await a.driver.publishControl({
            kind: 'presence-join',
            target: 'conn-1',
            channel: 'presence-lobby',
            member: { id: 7, info: { name: 'Ada', role: 'admin' } },
        })
        assertEquals(b.obeyed.length, 1, 'a real presence member gets through')
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('FR-011: an oversized frame is refused at PUBLISH, not only at ingest', async () => {
    // The failure surfaces where it can be fixed. Without the publish-side
    // check, an app whose member info grew past the ceiling would publish
    // happily and every remote instance would drop it silently.
    const redis = new FakeRedis()
    const a = publisher(redis)
    try {
        const warnings = await warningsFrom(async () => {
            await a.driver.publishControl({
                kind: 'presence-join',
                target: 'conn-1',
                channel: 'presence-lobby',
                member: { id: 1, info: { blob: 'x'.repeat(20_000) } },
            })
        })
        assertEquals(a.captured.length, 0, 'nothing reached the wire')
        assert(
            warnings.some((w) =>
                w.includes('refusing to publish an oversized')
            ),
            `the publisher names its own refusal. Got: ${warnings.join(' | ')}`,
        )
    } finally {
        await a.driver.close()
    }
})

Deno.test('FR-007: an invalid windowMs is refused at construction, not silently ignored', () => {
    // `Math.abs(x) > NaN` is false for every frame, so a NaN window disables the
    // freshness check outright — and `Number(Deno.env.get('UNSET'))` is NaN.
    for (const windowMs of [NaN, 0, -1, Infinity]) {
        let threw = false
        try {
            new RedisBroadcastDriver(
                { command: () => Promise.resolve({ type: 'nil' as const }) },
                { psubscribe: () => {} },
                { prefix: PREFIX, control: { secret: SECRET, windowMs } },
            )
        } catch {
            threw = true
        }
        assert(threw, `windowMs=${windowMs} must be refused at construction`)
    }
})

// ---------------------------------------------------------------------------
// #283 — the entry cap reaches the window from the driver's own config
// ---------------------------------------------------------------------------

Deno.test('#283: control.maxEntries is FORWARDED to the replay window, not merely accepted', async () => {
    // The trap this is shaped to avoid is the same one #245's cadence test
    // names: a driver that silently DROPS the option behaves identically to one
    // that forwards it, because the default (10 000) would never be reached by
    // a test. Accepting the option proves nothing.
    //
    // So the assertion is behavioural and only possible if the value arrived: a
    // cap of 2 means the third distinct frame evicts the first, and the first
    // becomes admissible again. At the default cap it would still be a
    // duplicate and stay refused.
    const redis = new FakeRedis()
    const a = publisher(redis)
    const b = receiver(redis, undefined, 2)
    try {
        await a.driver.publishControl({ kind: 'evict', target: 'v1' })
        await a.driver.publishControl({ kind: 'evict', target: 'v2' })
        await a.driver.publishControl({ kind: 'evict', target: 'v3' })
        assertEquals(b.obeyed.length, 3, 'three distinct frames were obeyed')

        await warningsFrom(async () => {
            // The FIRST frame's nonce was evicted by the third admission.
            await replay(redis, a.captured[0])
        })
        assertEquals(
            b.obeyed.length,
            4,
            'the evicted nonce is admissible again — which can only be true ' +
                'if maxEntries:2 reached the window. At the 10 000 default ' +
                'this replay stays a duplicate and the count is still 3',
        )

        // The other half: what is still remembered is still refused, so the
        // window did not simply stop working.
        await warningsFrom(async () => {
            await replay(redis, a.captured[2])
        })
        assertEquals(
            b.obeyed.length,
            4,
            'the most recent frame is still remembered and still refused',
        )
    } finally {
        await a.driver.close()
        await b.driver.close()
    }
})

Deno.test('#283: an invalid control.maxEntries is refused at construction', async () => {
    const redis = new FakeRedis()
    for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
        assertThrows(
            () =>
                new RedisBroadcastDriver(
                    { command: redis.command },
                    redis.subscriberFor(),
                    {
                        prefix: PREFIX,
                        control: { secret: SECRET, maxEntries: bad },
                    },
                ),
            Error,
            'maxEntries',
            `maxEntries: ${bad} must be refused, not silently coerced`,
        )
    }
    await Promise.resolve()
})
