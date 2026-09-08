/**
 * @fileoverview #314 — the channel name is asserted at the boundary too.
 *
 * `subscribe` asserted two of the three values it received and skipped this
 * one: `connection.id` (#304) and a presence `member.id` (#306) were checked,
 * the channel was not. The only channel validation in the package guarded the
 * WebSocket wire, so the framework's own socket path refused a name the public
 * API accepted — #304's asymmetry, one value over.
 *
 * These tests pin the boundary AND the reason it matters, which is not the
 * throw: without it the join succeeded locally and every peer dropped the
 * control frame, with `{ ok: true }` returned and nothing logged on the
 * instance that made the mistake.
 *
 * @module @lockness/realtime/tests/channel_name_boundary
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, ChannelNameError } from '../manager.ts'
import { isValidName } from '../protocol.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

const conn = (id: string): Connection<User> => ({
    id,
    identity: { id: 1 },
    metadata: {},
    send: () => {},
    close: () => {},
})

/** Names the WebSocket wire already refuses, which the public API accepted. */
const REFUSED = [
    'presence-my room', // a space — the owned-set separator
    'chat:\u0007', // a control character
    'room#1', // outside the allowlist
    'x'.repeat(201), // over the length cap
    '', // empty
]

Deno.test('#314 subscribe refuses a channel the WebSocket wire would refuse', async () => {
    for (const channel of REFUSED) {
        assert(!isValidName(channel), `fixture is wrong: ${channel} is valid`)
        const m = new ChannelManager<User>({ authorize: () => true })
        const c = conn(crypto.randomUUID())
        m.register(c)
        let threw: unknown
        try {
            await m.subscribe(c, channel)
        } catch (error) {
            threw = error
        }
        assert(
            threw instanceof ChannelNameError,
            `${JSON.stringify(channel)} was accepted (got ${threw})`,
        )
    }
})

Deno.test('#314 the refusal happens BEFORE the authorizer runs', async () => {
    // The authorizer may be a DB read, an audit write or a rate-limit
    // increment. Running it for a channel that can never work spends that side
    // effect on nothing — the same reasoning #304 gives for the id assertion
    // sitting ahead of it.
    let authorizerRuns = 0
    const m = new ChannelManager<User>({
        authorize: () => {
            authorizerRuns++
            return true
        },
    })
    const c = conn(crypto.randomUUID())
    m.register(c)
    try {
        await m.subscribe(c, 'presence-my room')
    } catch { /* expected */ }
    assertEquals(
        authorizerRuns,
        0,
        'the authorizer ran for an unusable channel',
    )
})

Deno.test('#314 an ordinary channel is unaffected, including one containing `:`', async () => {
    // `:` is inside the charset and is load-bearing for namespaced channels —
    // a guard that refused it would break far more than it fixed.
    for (const channel of ['chat', 'presence-eu:orders', 'private-a.b_c-d']) {
        const m = new ChannelManager<User>({ authorize: () => true })
        const c = conn(crypto.randomUUID())
        m.register(c)
        const result = await m.subscribe(c, channel)
        assertEquals(
            result.ok,
            true,
            `a legitimate channel was refused: ${channel}`,
        )
    }
})

Deno.test('#314 UNSUBSCRIBE is not guarded — cleanup must be total', async () => {
    // Deliberate asymmetry: creation is guarded, removal is not. Refusing a
    // removal strands the state it would have removed, and `disconnect`
    // iterates every known channel — so on a process holding a name created
    // before the guard, throwing here would fail the disconnect and leak every
    // channel after it.
    const m = new ChannelManager<User>({ authorize: () => true })
    const c = conn(crypto.randomUUID())
    m.register(c)
    await m.unsubscribe(c.id, 'presence-my room')
    await m.disconnect(c.id)
})

Deno.test('#314 the owned-set separator rule holds: the channel has no space, the member id may', () => {
    // What the boundary buys, stated as the invariant `OWNED_SEP`'s docstring
    // asserts and used to only assume. The entry is `<channel> <field>` parsed
    // on the FIRST space, so the split is unambiguous exactly while a channel
    // cannot contain one — and a member id, which #306 deliberately left
    // charset-free, may contain as many as it likes.
    const channel = 'presence-room'
    const memberId = 'Ada Lovelace Jr'
    const entry = `${channel} ${memberId}`
    const sep = entry.indexOf(' ')
    assertEquals(entry.slice(0, sep), channel)
    assertEquals(entry.slice(sep + 1), memberId)
    assert(isValidName(channel), 'the channel half must be charset-bounded')
    assert(!isValidName(memberId), 'the member half deliberately need not be')

    // And the pre-#314 failure, pinned so the reason the guard exists survives:
    // a channel with a space splits in the wrong place, and the sweep then
    // HDELs a roster key that does not exist.
    const broken = `presence-my room ${memberId}`
    const badSep = broken.indexOf(' ')
    assertEquals(broken.slice(0, badSep), 'presence-my')
    assert(
        broken.slice(0, badSep) !== 'presence-my room',
        'a spaced channel name must be shown to mis-split — that is the bug',
    )
})
