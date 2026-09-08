/**
 * @fileoverview #306 — the bound on `PresenceMember.id`, and what it is NOT.
 *
 * `PresenceMember.id` is the other id-shaped value on the control frame, and
 * #304 bounded only `Connection.id`. The asymmetry is closed here — but with a
 * LENGTH bound, not the charset one, and these tests pin both halves of that
 * decision. A test suite that only proved the refusals would let someone
 * "tighten" this to `isValidName` later and break every deployment keying
 * presence on an email address.
 *
 * @module @lockness/realtime/tests/presence_member_id
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, PresenceMemberIdError } from '../manager.ts'
import { MAX_NAME_LENGTH } from '../protocol.ts'
import type { Connection } from '../types.ts'
import type { PresenceMember } from '../channel.ts'

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

/** A manager whose authorizer returns whatever `member` says. */
function managerReturning(member: PresenceMember | true) {
    return new ChannelManager<User>({ authorize: () => member })
}

Deno.test('#306 the FRAMEWORK path is unaffected — authorize() returning true', async () => {
    // The acceptance criterion that matters most: `authorize: () => true`
    // yields `{ id: connection.id }`, and a connection id is already charset-
    // and length-bounded by #304. A guard that tripped on the framework's own
    // member would be a regression dressed as hardening.
    const m = managerReturning(true)
    const c = conn(crypto.randomUUID())
    m.register(c)
    const result = await m.subscribe(c, 'presence-room')
    assertEquals(result.ok, true)
})

Deno.test('#306 an application id the charset would REJECT is accepted', async () => {
    // The decision, pinned. `isValidName` rejects every one of these, and all
    // three are ordinary presence identities. Borrowing Connection.id's
    // charset here would break real deployments to buy nothing — RESP bulk
    // strings are length-prefixed, control frames carry a MAC, and the
    // owned-set parse anchors on a charset-bounded channel.
    for (const id of ['user@example.com', 'Ada Lovelace', 'user+tag@x.co']) {
        const m = managerReturning({ id })
        const c = conn(crypto.randomUUID())
        m.register(c)
        const result = await m.subscribe(c, 'presence-room')
        assertEquals(result.ok, true, `a legitimate id was refused: ${id}`)
    }
})

Deno.test('#306 an OVERSIZED member id is refused before anything is written', async () => {
    // Length is the one thing no layer below bounds: the caps upstream are a
    // 10 MiB RESP frame and an 8 KiB control payload, and the roster write
    // happens BEFORE the control publish — so without this guard an oversized
    // id lands in the Redis hash while the frame announcing it is dropped with
    // a warning, and subscribe still answers ok. A partial write, reported as
    // a success.
    const m = managerReturning({ id: 'x'.repeat(MAX_NAME_LENGTH + 1) })
    const c = conn(crypto.randomUUID())
    m.register(c)
    let threw: unknown
    try {
        await m.subscribe(c, 'presence-room')
    } catch (error) {
        threw = error
    }
    assert(
        threw instanceof PresenceMemberIdError,
        `expected PresenceMemberIdError, got ${threw}`,
    )
})

Deno.test('#306 the boundary is EXACT — 200 passes, 201 does not', async () => {
    // Pinned so a later "round it down for safety" edit is a visible change of
    // contract rather than a silent one.
    const ok = managerReturning({ id: 'x'.repeat(MAX_NAME_LENGTH) })
    const a = conn(crypto.randomUUID())
    ok.register(a)
    assertEquals((await ok.subscribe(a, 'presence-room')).ok, true)

    const over = managerReturning({ id: 'x'.repeat(MAX_NAME_LENGTH + 1) })
    const b = conn(crypto.randomUUID())
    over.register(b)
    let threw = false
    try {
        await over.subscribe(b, 'presence-room')
    } catch {
        threw = true
    }
    assert(threw, `${MAX_NAME_LENGTH + 1} characters was accepted`)
})

Deno.test('#306 an EMPTY member id is refused', async () => {
    const m = managerReturning({ id: '' })
    const c = conn(crypto.randomUUID())
    m.register(c)
    let threw = false
    try {
        await m.subscribe(c, 'presence-room')
    } catch {
        threw = true
    }
    assert(threw, 'an empty member id was accepted as a roster field name')
})

Deno.test('#306 a NON-FINITE numeric id is refused, a large finite one is not', async () => {
    // `String(NaN)` is "NaN" — an ordinary-looking hash field that every
    // NaN-identified member would silently SHARE, collapsing them into one
    // roster entry. Infinity is the same shape of problem.
    for (const id of [Number.NaN, Number.POSITIVE_INFINITY, -Infinity]) {
        const m = managerReturning({ id })
        const c = conn(crypto.randomUUID())
        m.register(c)
        let threw = false
        try {
            await m.subscribe(c, 'presence-room')
        } catch {
            threw = true
        }
        assert(threw, `a non-finite numeric id was accepted: ${id}`)
    }

    // And the case that forced length-over-charset: String(1e21) is "1e+21",
    // whose `+` is outside isValidName. A charset predicate could not have been
    // applied to this type without rejecting a legitimate large integer.
    assertEquals(String(1e21), '1e+21')
    const m = managerReturning({ id: 1e21 })
    const c = conn(crypto.randomUUID())
    m.register(c)
    assertEquals((await m.subscribe(c, 'presence-room')).ok, true)
})
