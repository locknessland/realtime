/**
 * @fileoverview #332 — the revocation trio is detected as a SET, and a driver
 * of the previous generation is refused at construction.
 *
 * Two separate rules, deliberately in two separate units, and this file pins
 * both **and** pins that they are separate.
 *
 * `revocationStore` is a *total, non-throwing* narrowing function, exactly like
 * `presenceRoster` and `channelWatcher`: a driver either has the whole
 * capability or it does not. "Which driver generations are admitted" is a
 * different question with a different answer, and fusing it into the probe
 * would give that function two reasons to change — after which a later pass
 * restoring consistency with its two non-throwing siblings deletes the refusal
 * without noticing what it was for. That is not hypothetical: it is the mirror
 * image of the risk already recorded for someone re-adding `markRevoked` "for
 * compatibility".
 *
 * **Why the refusal is a throw and not a warning.** The old pair was
 * `markRevoked(target)` / `listRevoked()`. A driver that still presents them
 * and not the new trio narrows to `undefined` — "no revocation store" — so
 * `evict` silently loses its durability on a driver that plainly implements
 * revocation, and a lost control frame is never recovered. Nothing is logged
 * and every same-version test passes.
 *
 * @module @lockness/realtime/tests/revocation_seam_332
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { ChannelManager, revocationStore } from '../manager.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { BroadcastDriver } from '../driver.ts'

/** The bare transport every driver below builds on. */
const transport = (): BroadcastDriver => ({
    publish: () => {},
    onMessage: () => {},
})

const trio = () => ({
    markRevocation: () => Promise.resolve(),
    listRevocations: () => Promise.resolve([]),
    clearRevocation: () => Promise.resolve(),
})

Deno.test('#332 the revocation trio is narrowed as a SET, never member by member', () => {
    assertEquals(
        revocationStore({ ...transport(), ...trio() }) !== undefined,
        true,
        'all three present → a store',
    )

    // Each PAIR is refused, and the middle one is the reason the rule exists:
    // a driver that can mark but not clear re-applies a channel-scoped
    // revocation at every reconcile tick for the whole record TTL, kicking a
    // client that legitimately re-subscribed — with the roster correct
    // throughout and nothing logged.
    const { markRevocation, listRevocations, clearRevocation } = trio()
    const pairs: [string, BroadcastDriver][] = [
        ['mark + list, no clear', {
            ...transport(),
            markRevocation,
            listRevocations,
        }],
        ['mark + clear, no list', {
            ...transport(),
            markRevocation,
            clearRevocation,
        }],
        ['list + clear, no mark', {
            ...transport(),
            listRevocations,
            clearRevocation,
        }],
    ]
    for (const [label, driver] of pairs) {
        assertEquals(
            revocationStore(driver),
            undefined,
            `${label} is not a revocation store — two of three is not a ` +
                'partial capability, it is a different and worse one',
        )
    }
})

Deno.test('#332 the probe is TOTAL — a driver with no revocation surface is fine', () => {
    // And this is what the throw must never fire for. `MemoryBroadcastDriver`
    // is the single-process path; it implements none of the three, and it owes
    // no durability because it has no bus on which to lose a frame.
    const memory = new MemoryBroadcastDriver()
    assertEquals(revocationStore(memory), undefined)
    assertEquals(revocationStore(transport()), undefined)

    const m = new ChannelManager({ driver: memory })
    assert(m instanceof ChannelManager, 'and it constructs')
})

Deno.test('#332 a driver on the PREVIOUS seam is refused at construction', () => {
    // The failure this replaces is silent: narrowed to `undefined`, such a
    // driver would make `evict` non-durable while looking entirely healthy.
    for (
        const legacy of [
            { markRevoked: () => {}, listRevoked: () => [] },
            // EITHER member is enough. A driver presenting one half was always
            // broken; refusing only the complete pair would let the more
            // broken driver through.
            { markRevoked: () => {} },
            { listRevoked: () => [] },
        ]
    ) {
        const error = assertThrows(
            () => new ChannelManager({ driver: { ...transport(), ...legacy } }),
            Error,
        )
        assert(
            error.message.includes('markRevocation'),
            `the error must NAME the migration, not merely refuse it. Got: ${error.message}`,
        )
    }
})

Deno.test('#332 the refusal is the ASSERTION, not the probe — and stays separable', () => {
    // The regression this guards is a tidy-up, not a bug: someone notices the
    // probe throws where its two siblings return `undefined`, "restores
    // consistency", and the migration refusal disappears with no test failing.
    //
    // So this pins the split directly: the probe must answer `undefined` for a
    // legacy driver — it has no store — WITHOUT throwing, because the throwing
    // is somebody else's job.
    const legacy: BroadcastDriver = {
        ...transport(),
        ...({ markRevoked: () => {}, listRevoked: () => [] } as object),
    }
    assertEquals(
        revocationStore(legacy),
        undefined,
        'the probe reports absence and does not editorialise',
    )
    assertThrows(
        () => new ChannelManager({ driver: legacy }),
        Error,
        'markRevocation',
    )
})

Deno.test('#332 a driver with the new trio constructs and is used', async () => {
    const marked: unknown[] = []
    const driver: BroadcastDriver = {
        ...transport(),
        ...trio(),
        markRevocation: (revocation) => {
            marked.push(revocation)
            return Promise.resolve()
        },
    }
    const m = new ChannelManager({ driver })
    await m.evict('c1')
    assertEquals(
        marked,
        [{ target: 'c1' }],
        'evict records a CONNECTION-scoped revocation through the narrowed ' +
            'store — no channel, because it revokes the whole socket',
    )
})
