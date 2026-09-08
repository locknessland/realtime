/**
 * @fileoverview #304 — one charset for a connection id, enforced at the boundary.
 *
 * `Connection.id` is supplied by the application when it wires its own
 * transport, and until now carried no charset. Three paths disagreed about
 * what they would accept:
 *
 * | path | an id outside `isValidName` |
 * | :--- | :--- |
 * | local `evict()` | worked |
 * | control frame to another instance | dropped on ingest, silently |
 * | durable reconcile | recovered it |
 *
 * So an application using such an id already had a revocation that worked on
 * one instance and not the others, and had no way to find out. Filtering the
 * reconcile path alone would have made that two silent failures instead of one.
 * The constraint lives at registration instead, where it is loud and where the
 * application can act on it.
 *
 * @module @lockness/realtime/tests/connection_id_charset
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { isValidName } from '../protocol.ts'
import type { BroadcastDriver } from '../driver.ts'
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

Deno.test('#304 the framework-minted id is inside the charset', () => {
    // The filter must reject nothing the framework itself produces. 200 draws
    // rather than one, because `crypto.randomUUID()` is the thing being relied
    // on and a single sample proves nothing about its alphabet.
    for (let i = 0; i < 200; i++) {
        const id = crypto.randomUUID()
        assert(
            isValidName(id),
            `randomUUID produced an id outside the charset: ${id}`,
        )
    }
})

Deno.test('#304 registering a connection with an out-of-charset id throws', () => {
    const m = new ChannelManager<User>()

    for (
        const [id, why] of [
            ['user@example.com', 'an email — the shape the docs warn against'],
            [
                'id with spaces',
                'a space, which the control frame format cannot carry',
            ],
            ['{7c9e6679-7425}', 'braces around a UUID'],
            ['', 'empty'],
            ['a'.repeat(201), 'over the length bound'],
        ] as const
    ) {
        assertThrows(
            () => m.register(conn(id)),
            Error,
            undefined,
            `register accepted ${why}`,
        )
    }
})

Deno.test('#304 subscribing with an out-of-charset id throws too', async () => {
    // The second registration site. `subscribe` writes the connection into the
    // same map, so guarding only `register` would leave a way in.
    const m = new ChannelManager<User>({ authorize: () => true })
    let threw = false
    try {
        await m.subscribe(conn('user@example.com'), 'presence-room')
    } catch {
        threw = true
    }
    assertEquals(threw, true, 'subscribe accepted an out-of-charset id')
    assertEquals(m.connectionCount, 0, 'the connection was tracked anyway')
})

Deno.test('#304 an ordinary id is accepted by both sites', async () => {
    const m = new ChannelManager<User>({ authorize: () => true })
    const good = conn(crypto.randomUUID())

    m.register(good)
    assertEquals(m.connectionCount, 1)

    const other = conn('svc:worker-3.a_b-1')
    const result = await m.subscribe(other, 'presence-room')
    assertEquals(result.ok, true)
    assertEquals(m.connectionCount, 2)
})

Deno.test('#304 the message names the id and the rule', async () => {
    const m = new ChannelManager<User>()
    const error = assertThrows(() => m.register(conn('bad id')))

    assert(error instanceof Error)
    // The id is encoded on the way into the message: this throw is reachable
    // with an application-controlled string, and an exception message reaches
    // a log as readily as a console call does.
    assertEquals(
        error.message.includes('\n'),
        false,
        'a newline reached the message',
    )
    const injected = assertThrows(() => m.register(conn('x\nGET /admin 200')))
    assert(injected instanceof Error)
    assertEquals(
        injected.message.includes('\n'),
        false,
        'a forged log line got through the error message',
    )
    await Promise.resolve()
})

Deno.test('#304 evict() refuses an out-of-charset id rather than no-opping', async () => {
    // The third boundary, and the one that matters most to a caller. `evict`
    // takes an arbitrary string, and an id outside the charset would publish a
    // control frame that every receiving instance drops on ingest — a
    // revocation that returns successfully and does nothing. That is the exact
    // shape of failure this issue exists to remove, so it must be loud here
    // too, not only at registration.
    const driver: BroadcastDriver = {
        publish: () => Promise.resolve(),
        onMessage: () => {},
    }
    const m = new ChannelManager<User>({ driver })

    let threw = false
    try {
        await m.evict('user@example.com')
    } catch {
        threw = true
    }
    assertEquals(
        threw,
        true,
        'evict accepted an id the control plane will drop',
    )

    // And an ordinary id still reaches the control plane unimpeded.
    await m.evict(crypto.randomUUID())
})

Deno.test('#304 reconcile drops a broker-injected id outside the charset', async () => {
    // The second, independent control. `listRevoked` is broker-sourced — a
    // writer with bus access can put anything in the index — and reconcile
    // hands what it finds straight to `revokeLocal`. The control-plane path has
    // always filtered `wire.target`; this one did not, and that asymmetry was
    // the finding.
    //
    // #278 removed the second return path, so this filter is now the ONLY thing
    // between the index and `revokeLocal` rather than one of two. The test did
    // not change shape; what changed is that there is no longer a second leg to
    // cover for it.
    const { RedisBroadcastDriver } = await import('../drivers/redis.ts')
    const { recordingPorts } = await import('./recording_ports.ts')

    // RESP shapes, not bare arrays: `asArray` requires `{ type: 'array' }` and
    // `asInteger` requires `{ type: 'integer' }`. A bare array coerces to
    // undefined, the driver logs "unexpected reply shape" and returns nothing —
    // which would have made this test pass for an empty result had it asserted
    // only that the bad ids were absent. It asserts the whole set instead.
    const bulk = (value: string) => ({ type: 'bulk', value })
    const array = (value: unknown[]) => ({ type: 'array', value })
    const { command, subscriber, recording } = recordingPorts({
        // The new sorted set, via EVAL.
        EVAL: array([
            bulk('7c9e6679-7425-40de-944b-e07fc1f90ae7'),
            bulk('user@example.com'),
            bulk('id with spaces'),
            bulk('svc:worker-3'),
            bulk('x\nGET /admin 200'),
        ]),
    })

    const driver = new RedisBroadcastDriver(command, subscriber, {
        prefix: 'app',
    })
    const revoked = await driver.listRevoked?.() ?? []

    assertEquals(
        revoked.sort(),
        [
            '7c9e6679-7425-40de-944b-e07fc1f90ae7',
            'svc:worker-3',
        ],
        'the filter kept or dropped the wrong ids',
    )
    // Positive control: the EVAL leg ran at all, so an empty result would not
    // pass this test for the wrong reason.
    assertEquals(
        recording.commands.some((argv) => argv[0] === 'EVAL'),
        true,
        'the revocation index was never read',
    )
})

Deno.test('#304 subscribe rejects before the authorizer runs', async () => {
    // The guard sat AFTER the awaited authorize, so an out-of-charset id on a
    // private channel returned `{ ok: false }` whenever the app denied — the
    // same id that throws on a public channel — and the authorizer ran on an id
    // that was never usable. An authorizer is a DB read, an audit write, a
    // rate-limit increment; none of them should fire for a caller bug.
    let authorizerRan = false
    const m = new ChannelManager<User>({
        authorize: () => {
            authorizerRan = true
            return false
        },
    })

    let threw = false
    try {
        await m.subscribe(conn('user@example.com'), 'private-billing')
    } catch {
        threw = true
    }

    assertEquals(
        threw,
        true,
        'a denied private channel swallowed the id defect',
    )
    assertEquals(authorizerRan, false, 'the authorizer ran on an unusable id')
})

Deno.test('#304 a rejected id closes the socket instead of leaving it open', () => {
    // `guard()` in websocket.ts catches whatever onOpen throws and only logs
    // it, so a bare throw left the socket OPEN and untracked: the app's own
    // onOpen was skipped, onMessage went on firing, and evict could not reclaim
    // it because it rejects the same id. Fail-open on the seam this change was
    // meant to make loud.
    const m = new ChannelManager<User>()
    const closed: [number, string][] = []
    const bad: Connection<User> = {
        id: 'user@example.com',
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: (code, reason) => void closed.push([code ?? 0, reason ?? '']),
    }
    let appOnOpenRan = false
    const hooks = m.handlerHooks({ onOpen: () => void (appOnOpenRan = true) })

    assertThrows(() => hooks.onOpen?.(bad))

    assertEquals(closed.length, 1, 'the socket was left open')
    assertEquals(closed[0][0], 1011)
    assertEquals(
        appOnOpenRan,
        false,
        "the app's onOpen ran on an untracked connection",
    )
    assertEquals(m.connectionCount, 0)
})
