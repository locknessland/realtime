/**
 * @fileoverview #393 — one teardown per object: a second `disconnect` of an
 * already-retiring connection joins the first call's promise instead of
 * computing its own copy of the reverse-index loop.
 *
 * Before this, `#retired` was a `WeakSet`: two overlapping teardowns of the
 * SAME object — `evict`'s id-form call racing the transport's own close
 * event with the object form, both entered while the object still owned the
 * id — each ran their own snapshot of `#channelsByClient` and their own
 * `finally`. Whichever's snapshot happened to find nothing left (the OTHER
 * copy's sync delete had already run) still reached its `finally` and freed
 * `connections` for the id — early, while the other copy was still mid-loop.
 * A fresh registration under that id then raced the stale copy's own resume.
 *
 * `#retired` is now a `WeakMap<Connection, Promise<DisconnectOutcome>>`. A
 * second call for an object already present joins its promise; the id stays
 * bound to the retiring object until that one, single teardown ends.
 *
 * Every wait below is on a gate's promise or a bounded poll of an observable
 * condition, never a fixed microtask count (docs/testing.md). W2 wraps
 * `watchingEscapes` (#374) because it drives three overlapping calls
 * (`disconnect`, `evict`, `register`) and an escaped rejection must fail the
 * row by name rather than kill the runner.
 *
 * Committed red first against the pre-fix `manager.ts`: W1 and W2 both fail
 * — W1 because two independent calls return two different promises, W2
 * because `evict` frees the id early while `disconnect(a0)` is still stuck,
 * letting A1 register before the stale copy resumes. Each is paired with a
 * mutant in `mutations/joined_teardown_393.ts`.
 *
 * @module @lockness/realtime/tests/joined_teardown_393
 */

import {
    assert,
    assertEquals,
    assertStrictEquals,
    assertThrows,
} from '@std/assert'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { ChannelManager, ConnectionIdInUseError } from '../manager.ts'
import type { BroadcastDriver, BroadcastMessage } from '../driver.ts'
import type { Connection } from '../types.ts'
import { watchingEscapes } from './escape_watcher.ts'

interface User {
    id: number
}

type Recording = Connection<User> & {
    readonly received: Record<string, unknown>[]
}

/** A connection that records every frame it is sent. */
function conn(id: string, userId = 1): Recording {
    const received: Record<string, unknown>[] = []
    return {
        id,
        identity: { id: userId },
        metadata: {},
        send: (data) => void received.push(JSON.parse(data as string)),
        close: () => {},
        received,
    } as Recording
}

/**
 * A watch-capable driver, over a {@link MemoryBroadcastDriver} for delivery
 * and roster ops, whose `unwatchChannel` for one named channel suspends until
 * the test opens it — the mid-loop gate every witness here shares.
 */
class GatedUnwatchDriver implements BroadcastDriver {
    readonly memory = new MemoryBroadcastDriver()
    readonly watched: string[] = []
    readonly unwatched: string[] = []
    #gated?: string
    #open!: () => void
    #gate = new Promise<void>((resolve) => (this.#open = resolve))
    #reach!: () => void
    /** Resolves once the gated channel's unwatch has been asked for. */
    readonly reached = new Promise<void>((resolve) => (this.#reach = resolve))

    publish(message: BroadcastMessage): void {
        this.memory.publish(message)
    }

    onMessage(handler: (message: BroadcastMessage) => void): void {
        this.memory.onMessage(handler)
    }

    watchChannel(channel: string): void {
        this.watched.push(channel)
    }

    unwatchChannel(channel: string): void | Promise<void> {
        this.unwatched.push(channel)
        if (channel !== this.#gated) return
        this.#gated = undefined
        this.#reach()
        return this.#gate
    }

    /** Suspend the next unwatch of `channel`, and only that one. */
    gate(channel: string): void {
        this.#gated = channel
    }

    /** Let the suspended unwatch finish. */
    open(): void {
        this.#open()
    }
}

/** A macrotask: every queued microtask chain has run when it resolves. */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

function managerOver(driver: BroadcastDriver): ChannelManager<User> {
    return new ChannelManager<User>({ driver, authorize: () => true })
}

Deno.test(
    "#393 W1 a same-turn double disconnect of one object joins — the second call IS the first call's promise, and the driver unwatches once",
    async () => {
        const driver = new GatedUnwatchDriver()
        const manager = managerOver(driver)
        const a0 = conn('c1')
        manager.register(a0)
        assert((await manager.subscribe(a0, 'news')).ok)

        driver.gate('news')
        // Issued back to back, in the same synchronous turn: the object form
        // first, then the id form — no await between them.
        const first = manager.disconnect(a0)
        const second = manager.disconnect('c1')
        assertStrictEquals(
            second,
            first,
            'the second call joined the first — not a race',
        )

        driver.open()
        assertEquals(await first, 'disconnected')
        assertEquals(await second, 'disconnected')
        assertEquals(
            driver.unwatched.filter((c) => c === 'news').length,
            1,
            'only one teardown ever ran — a second, independent copy would ' +
                'unwatch twice',
        )
    },
)

Deno.test(
    "#393 W2 the acceptance witness: overlapping teardowns of one object never remove a new holder's membership mid-loop",
    async () => {
        await watchingEscapes(async () => {
            const driver = new GatedUnwatchDriver()
            const manager = managerOver(driver)
            const a0 = conn('c1')
            manager.register(a0)
            assert((await manager.subscribe(a0, 'news')).ok)

            // The object form: A0's own close, entered while A0 still owns
            // the id. Suspended mid-loop on the shared channel's unwatch.
            driver.gate('news')
            const closing = manager.disconnect(a0)
            await driver.reached

            // The id form: an evict racing it, entered while A0 still owns
            // the id too (#393's exact trigger — evict → revokeLocal's
            // id-form call racing the transport's close, the object form).
            // It must JOIN, not free the id early.
            const evicting = manager.evict('c1')
            // Give evict's own internal id-form call the chance it needs to
            // run: on the pre-fix tree its independent copy finds `news`
            // already gone (A0's own sync delete, above) and settles right
            // here, freeing the id early.
            await tick()
            assertThrows(
                () => manager.register(conn('c1', 2)),
                ConnectionIdInUseError,
                undefined,
                "the id is still A0's — evict joined A0's own teardown " +
                    'instead of freeing it early',
            )

            driver.open()
            await closing
            await evicting

            // Only now is the id free — a genuinely new registration, not a
            // window opened while a teardown was still mid-loop.
            const a1 = conn('c1', 2)
            manager.register(a1)
            assert((await manager.subscribe(a1, 'news')).ok)

            manager.broadcast('news', 'ping', 1)
            await tick()
            assertEquals(
                a1.received.filter((f) => f.type === 'event').length,
                1,
                "A1 holds `news` — the resumed teardown's own (earlier) " +
                    "unwatch never reached A1's fresh membership",
            )
        })
    },
)

Deno.test(
    '#393 W3 a different object re-registering under a settled id gets its OWN teardown — the join is keyed by object, not by clientId',
    async () => {
        const driver = new GatedUnwatchDriver()
        const manager = managerOver(driver)
        const a0 = conn('c1')
        manager.register(a0)
        assert((await manager.subscribe(a0, 'news')).ok)
        assertEquals(await manager.disconnect(a0), 'disconnected')

        const a1 = conn('c1', 2)
        manager.register(a1)
        assert((await manager.subscribe(a1, 'weather')).ok)

        assertEquals(
            await manager.disconnect(a1),
            'disconnected',
            "A1's own teardown ran for real, not a silent join of A0's " +
                'settled one',
        )
        assert(
            driver.unwatched.includes('weather'),
            "A1's channel was actually torn down",
        )
    },
)
