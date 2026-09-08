/**
 * @fileoverview US1 regression — local re-authorization has exactly one home
 * (FR-011, decision-table S6).
 *
 * A message received off the bus fans **only** to `subscriptions.get(channel)`,
 * the set `ChannelManager.subscribe` populated with connections the LOCAL
 * authorizer approved at subscribe time. The broadcast driver performs no
 * authorization of its own — it hands every received message straight to the
 * manager, which is the single place that decides who on this instance may see
 * it. These assertions lock that invariant so a future change cannot grow a
 * second authorization check in the driver, nor fan a received message to a
 * connection the manager never authorized.
 *
 * @module @lockness/realtime/tests/deliver_local_reauth
 */

import { assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type { BroadcastDriver, BroadcastMessage } from '../driver.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

/**
 * A driver whose received-message handler is captured so a test can push a
 * message onto the manager exactly as the transport would — without any bus,
 * socket, or authorization of its own.
 */
class ControllableDriver implements BroadcastDriver {
    private handler: ((message: BroadcastMessage) => void) | undefined
    publish(): void {}
    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.handler = handler
    }
    /** Deliver a received message to the manager, as the transport would. */
    receive(message: BroadcastMessage): void {
        this.handler?.(message)
    }
}

function fakeConn(id: string, identity: User | null): Connection<User> {
    const sent: string[] = []
    return {
        id,
        identity,
        metadata: {},
        send: (d) => void sent.push(d as string),
        close: () => {},
        get _sent() {
            return sent
        },
    } as Connection<User> & { readonly _sent: string[] }
}
const sentOf = (c: Connection<User>) =>
    (c as unknown as { _sent: string[] })._sent

Deno.test('FR-011/S6: a received message fans only to the local subscription set — never to an unauthorized connection', async () => {
    const driver = new ControllableDriver()
    // Deny user 2; approve everyone else.
    const manager = new ChannelManager<User>({
        driver,
        authorize: (id) => id?.id !== 2,
    })

    const authorized = fakeConn('ok', { id: 1 })
    const denied = fakeConn('no', { id: 2 })
    // Both connections are registered/known to the manager…
    manager.register(denied)
    // …but only the authorized one is in the channel's subscription set.
    assertEquals((await manager.subscribe(authorized, 'private-room')).ok, true)
    assertEquals((await manager.subscribe(denied, 'private-room')).ok, false)

    driver.receive({ channel: 'private-room', event: 'secret', data: { s: 1 } })

    // The single home (deliverLocal) fanned to subscriptions.get(channel) only.
    assertEquals(sentOf(authorized).length, 1)
    assertEquals(sentOf(denied).length, 0)
})

Deno.test('FR-011/S6: a received message on a channel with no local subscribers reaches no one', () => {
    const driver = new ControllableDriver()
    const manager = new ChannelManager<User>({ driver, authorize: () => true })

    const bystander = fakeConn('by', { id: 1 })
    // Registered but never subscribed to this channel.
    manager.register(bystander)

    driver.receive({ channel: 'private-empty', event: 'e', data: {} })

    assertEquals(sentOf(bystander).length, 0)
})
