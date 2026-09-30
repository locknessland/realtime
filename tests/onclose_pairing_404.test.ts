/**
 * @fileoverview #404 — `handlerHooks` runs the app's `onClose` exactly once for
 * each socket whose `onOpen` ran: evicted ones included, refused ones never.
 *
 * Before this, the app's `onClose` ran for a socket whose `register` was
 * refused — on a custom transport that reuses connection ids, a second socket
 * presenting a live id. An id-form verb there (`unsubscribe(conn.id, …)`,
 * `disconnect(conn.id)`) then landed on the LIVE owner of that id, and a
 * per-identity counter the app keeps in `onOpen` / `onClose` was decremented
 * for a socket it never counted.
 *
 * The pairing is a closure-local weak set of opened objects, cleared on close.
 * It pairs open with close and nothing else: ownership is still `#isOwner`'s
 * (row 17 of the #370 plan), which is why W2 — an evicted socket, no longer
 * the owner — must still get its hook.
 *
 * Every "the holder kept it" is paired with a positive broadcast control, so a
 * broken broadcast cannot pass as silence. Test names start `#404 W<n> ` with a
 * trailing space, so a battery naming `W1 ` never matches a later `W1x`.
 *
 * @module @lockness/realtime/tests/onclose_pairing_404
 */

import { assert, assertEquals, assertThrows } from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import {
    ChannelManager,
    ConnectionDisconnectedError,
    ConnectionIdError,
    ConnectionIdInUseError,
} from '../manager.ts'
import type { Connection, WebSocketHooks } from '../types.ts'

const PRIVATE = 'private-x'

interface User {
    id: number
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

/** A fake transport socket that records the frames it is sent. */
function conn(id: string, user: User): Recording {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: user,
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Recording
}

/** The manager's subscription map, read through its TS-private field. */
const subscriptions = (manager: ChannelManager<User>) =>
    (manager as unknown as { subscriptions: Map<string, Set<string>> })
        .subscriptions

/** A macrotask: every queued microtask chain has run when it resolves. */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

/** How many event frames named `event` `c` received. */
const heard = (c: Recording, event: string) =>
    c.received.filter((f) => f.type === 'event' && f.event === event).length

/**
 * A custom transport driving `handlerHooks`, whose app keeps the counter the
 * issue names — `n++` on open, `n--` on close — and, on close, leaves
 * `private-x` by id: the id-form verb a refused socket must never reach.
 *
 * @param opens - Run by the app's `onOpen` after its increment — W4 throws.
 */
function fixture(opens: () => void = () => {}) {
    const manager = new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: () => true,
    })
    const app = { n: 0, closes: 0 }
    const user: WebSocketHooks<User> = {
        onOpen: () => {
            app.n++
            opens()
        },
        onClose: async (c) => {
            app.closes++
            app.n--
            await manager.unsubscribe(c.id, PRIVATE)
        },
    }
    return { manager, app, hooks: manager.handlerHooks(user) }
}

Deno.test("#404 W1 (i) an id-in-use socket's close runs no app hook — the counter holds and the live owner keeps its channel", async () => {
    const { manager, app, hooks } = fixture()
    const a = conn('c1', { id: 1 })
    await hooks.onOpen?.(a)
    assert((await manager.subscribe(a, PRIVATE)).ok)

    const b = conn('c1', { id: 3 })
    assertThrows(() => hooks.onOpen?.(b), ConnectionIdInUseError)
    await hooks.onClose?.(b, 1011, '')

    assertEquals(app.closes, 0, "the app's onClose never ran for B")
    assertEquals(app.n, 1, 'the counter still counts A, and only A')
    assert(
        subscriptions(manager).get(PRIVATE)?.has('c1'),
        'A still holds private-x',
    )
    manager.broadcast(PRIVATE, 'still', 1)
    await tick()
    assertEquals(heard(a, 'still'), 1, 'A still receives')
})

Deno.test('#404 W1 (ii) a socket refused for an unusable id runs no app hook on close', async () => {
    const { app, hooks } = fixture()
    const bad = conn('not a usable id', { id: 3 })
    assertThrows(() => hooks.onOpen?.(bad), ConnectionIdError)
    await hooks.onClose?.(bad, 1011, '')

    assertEquals(app.closes, 0, "the app's onClose never ran")
    assertEquals(app.n, 0, 'CONTROL: its onOpen never ran either')
})

Deno.test('#404 W1 (iii) a socket refused as disconnected runs no app hook on its later close', async () => {
    // A opens and closes, which retires it; the same object presented again
    // is refused with ConnectionDisconnectedError and must not count as opened.
    const { app, hooks } = fixture()
    const a = conn('c1', { id: 1 })
    await hooks.onOpen?.(a)
    await hooks.onClose?.(a, 1000, '')
    assertEquals(app.closes, 1, 'CONTROL: the first open was paired')

    assertThrows(() => hooks.onOpen?.(a), ConnectionDisconnectedError)
    await hooks.onClose?.(a, 1011, '')

    assertEquals(app.closes, 1, 'the refused re-open got no close')
    assertEquals(app.n, 0)
})

Deno.test("#404 W2 an evicted socket's close still runs the app hook, once", async () => {
    const { manager, app, hooks } = fixture()
    const a = conn('c1', { id: 1 })
    await hooks.onOpen?.(a)
    await manager.evict(a.id)

    await hooks.onClose?.(a, 4403, 'evicted')

    assertEquals(app.closes, 1, 'the evicted socket was opened: it is closed')
    assertEquals(app.n, 0)
})

Deno.test('#404 W3 a second close of the same socket runs the app hook no more', async () => {
    const { app, hooks } = fixture()
    const a = conn('c1', { id: 1 })
    await hooks.onOpen?.(a)

    await hooks.onClose?.(a, 1000, '')
    await hooks.onClose?.(a, 1000, '')

    assertEquals(app.closes, 1, 'one open, one close')
    assertEquals(app.n, 0)
})

Deno.test('#404 W4 an app onOpen that throws still gets its onClose, once', async () => {
    // The socket was admitted: `register` succeeded before the app's hook
    // threw, so the pairing is already recorded and the close must run the
    // hook — which is what lets a counter incremented first in onOpen balance.
    const { app, hooks } = fixture(() => {
        throw new Error('app onOpen failed')
    })
    const a = conn('c1', { id: 1 })
    assertThrows(() => hooks.onOpen?.(a), Error, 'app onOpen failed')

    await hooks.onClose?.(a, 1011, '')

    assertEquals(app.closes, 1, "the admitted socket's close ran the hook")
    assertEquals(app.n, 0, 'the counter balances')
})
