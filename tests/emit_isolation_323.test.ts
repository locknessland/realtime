/**
 * @fileoverview #323/FR-009 — one unusable socket must not silence the rest.
 *
 * `emitPresence` fans a frame to every local subscriber in a bare `for` loop.
 * `Connection.send` returns `void` and is uncaught, so a socket that throws —
 * a `WebSocket.send` on a CLOSING connection raises `InvalidStateError`, which
 * is a normal race rather than an exotic one — aborts the loop **mid-fan-out**.
 * Every subscriber the iteration had not reached yet is silently skipped, and
 * the throw propagates out of `subscribe` over a join that already committed.
 *
 * This becomes load-bearing under #323: once the announcement is the LAST thing
 * a join does, a throwing socket is a failure after the authoritative write,
 * and the roster and the frames disagree for everyone downstream of the dead
 * connection in iteration order — an order nothing defines.
 *
 * The fan-out is therefore per-connection: a socket that cannot receive is
 * skipped and logged, never fatal to its neighbours.
 *
 * @module @lockness/realtime/tests/emit_isolation_323
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

/** A connection that records what it received, or refuses to receive at all. */
function conn(
    id: string,
    userId: number,
    options: { deaf?: boolean } = {},
): Connection<User> & { readonly received: string[] } {
    const received: string[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => {
            if (options.deaf) throw new TypeError('socket is closing')
            received.push(data as string)
        },
        close: () => {},
        received,
    } as Connection<User> & { readonly received: string[] }
}

const authorize = (identity: User | null): PresenceMember | false =>
    identity ? { id: identity.id } : false

const joinFrames = (c: { readonly received: string[] }) =>
    c.received.map((raw) => JSON.parse(raw)).filter((f) =>
        f.action === 'joined'
    )

Deno.test('#323/FR-009 a throwing socket does not silence the subscribers after it', async () => {
    const m = new ChannelManager<User>({ authorize })
    const first = conn('c1', 1)
    m.register(first)
    const deaf = conn('c2', 2, { deaf: true })
    m.register(deaf)
    const last = conn('c3', 3)
    m.register(last)

    // Iteration follows Set insertion order, so `last` sits AFTER the socket
    // that throws — which is the only arrangement that can observe the abort.
    assertEquals((await m.subscribe(first, 'presence-room')).ok, true)
    assertEquals((await m.subscribe(deaf, 'presence-room')).ok, true)
    assertEquals((await m.subscribe(last, 'presence-room')).ok, true)

    const newcomer = conn('c4', 4)
    m.register(newcomer)
    const result = await m.subscribe(newcomer, 'presence-room')

    assertEquals(result.ok, true, 'the join itself must still succeed')
    assertEquals(
        joinFrames(last).length,
        1,
        'the subscriber AFTER the throwing socket must still have been told',
    )
})

Deno.test('#323/FR-009 a throwing socket does not fail the join it is announcing', async () => {
    const m = new ChannelManager<User>({ authorize })
    const deaf = conn('c1', 1, { deaf: true })
    m.register(deaf)
    assertEquals((await m.subscribe(deaf, 'presence-room')).ok, true)

    // The join is committed by the time the announcement runs; a socket that
    // cannot hear it must not turn a completed join into a rejection.
    const newcomer = conn('c2', 2)
    m.register(newcomer)
    const result = await m.subscribe(newcomer, 'presence-room')
    assertEquals(result.ok, true)
    assertEquals(
        result.here?.members.map((x) => x.id).sort(),
        [1, 2],
        'and the roster holds both members',
    )
})
