/**
 * @fileoverview #392 — `disconnect`'s id form raises a deprecation notice,
 * once per manager instance, for application callers only.
 *
 * `disconnect(target)` still accepts a connection id — `evict`'s own
 * `revokeLocal` needs it, and the issue defers narrowing the signature to a
 * later breaking step (architect-expert disposition,
 * https://github.com/locknessland/lockness-monorepo/issues/392). What changes
 * is visibility: an application passing a bare id now gets one
 * `triggerDeprecation` notice per `ChannelManager`, never a second one for the
 * same instance, and never one at all for the object form or for the
 * framework's own internal id-form caller (`revokeLocal`, reached through
 * `evict`).
 *
 * @module @lockness/realtime/tests/deprecate_disconnect_id_392
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import {
    type DeprecationEntry,
    getCollector,
    registerCollector,
    unregisterCollector,
} from '@lockness/deprecation-contracts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import { ChannelManager } from '../manager.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

function conn(id: string): Connection<User> {
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: () => {},
    }
}

function managerOver(): ChannelManager<User> {
    return new ChannelManager<User>({
        driver: new MemoryBroadcastDriver(),
        authorize: () => true,
    })
}

/** A collector that just remembers every entry it was handed. */
function recordingCollector(): { entries: DeprecationEntry[] } {
    const entries: DeprecationEntry[] = []
    registerCollector({ addDeprecation: (entry) => void entries.push(entry) })
    return { entries }
}

Deno.test(
    '#392 W1 disconnect(id) raises exactly one deprecation notice per manager, even across two calls',
    async () => {
        const { entries } = recordingCollector()
        try {
            const manager = managerOver()
            const a0 = conn('c1')
            manager.register(a0)

            assertEquals(await manager.disconnect('c1'), 'disconnected')
            assertEquals(
                entries.length,
                1,
                'the first id-form call raises the notice',
            )

            // A different, later id — still the SAME manager instance.
            const a1 = conn('c2')
            manager.register(a1)
            assertEquals(await manager.disconnect('c2'), 'disconnected')
            assertEquals(
                entries.length,
                1,
                'a second id-form call on the same manager raises nothing more',
            )
        } finally {
            unregisterCollector()
        }
    },
)

Deno.test(
    '#392 W2 disconnect(connectionObject) raises no deprecation notice',
    async () => {
        const { entries } = recordingCollector()
        try {
            const manager = managerOver()
            const a0 = conn('c1')
            manager.register(a0)

            assertEquals(await manager.disconnect(a0), 'disconnected')
            assertEquals(
                entries.length,
                0,
                'the object form never raises the notice',
            )
        } finally {
            unregisterCollector()
        }
    },
)

Deno.test(
    '#392 W3 evict (and its internal revokeLocal id-form call) raises no deprecation notice',
    async () => {
        const { entries } = recordingCollector()
        try {
            const manager = managerOver()
            const a0 = conn('c1')
            manager.register(a0)
            assert((await manager.subscribe(a0, 'news')).ok)

            await manager.evict('c1')
            assertEquals(
                entries.length,
                0,
                "evict's own id-form call is framework-internal and stays silent",
            )
        } finally {
            unregisterCollector()
        }
    },
)

Deno.test(
    '#392 W4 under STRICT_DEPRECATIONS, disconnect(id) rejects with the deprecation error AND the connection is retired and its channels released (review HIGH)',
    async () => {
        const manager = managerOver()
        const a0 = conn('c1')
        manager.register(a0)
        assert((await manager.subscribe(a0, 'news')).ok)

        Deno.env.set('STRICT_DEPRECATIONS', 'true')
        try {
            await assertRejects(
                () => manager.disconnect('c1'),
                Error,
                '[DEPRECATION] Since @lockness/realtime 0.4.0',
                'the notice becomes a rejection of THIS call, not a swallowed side effect',
            )
        } finally {
            Deno.env.delete('STRICT_DEPRECATIONS')
        }

        // The teardown ran to completion despite the rejection above: the id
        // is free — a second disconnect reports not-owned rather than tearing
        // down again, and a fresh registration under it succeeds, which only
        // holds if the first connection's channel was actually released.
        assertEquals(
            await manager.disconnect('c1'),
            'not-owned',
            'the connection was retired and forgotten even though the notice rejected',
        )
        const a1 = conn('c1')
        manager.register(a1)
        assert(
            (await manager.subscribe(a1, 'news')).ok,
            "a fresh registration under the freed id succeeds — the old one's channel was released",
        )
    },
)

Deno.test(
    '#392 W5 a throwing collector cannot prevent the teardown that already started (review HIGH)',
    async () => {
        registerCollector({
            addDeprecation: () => {
                throw new Error('boom: a buggy collector')
            },
        })
        try {
            const manager = managerOver()
            const a0 = conn('c1')
            manager.register(a0)
            assert((await manager.subscribe(a0, 'news')).ok)

            await assertRejects(
                () => manager.disconnect('c1'),
                Error,
                'boom: a buggy collector',
                "the collector's own error still reaches this call's caller",
            )
            assertEquals(
                await manager.disconnect('c1'),
                'not-owned',
                'teardown still ran to completion despite the collector throwing',
            )
        } finally {
            unregisterCollector()
        }
    },
)

Deno.test(
    '#392 W6 under STRICT_DEPRECATIONS a failing teardown is WARNed, never dropped, while the deprecation error wins',
    async () => {
        // A per-channel driver: `unwatchChannel` is only called when
        // `watchChannel` exists too (driver.ts), so both are declared.
        class FailingUnwatch extends MemoryBroadcastDriver {
            watchChannel(): void {}
            unwatchChannel(): Promise<void> {
                return Promise.reject(new Error('BROKER_UNWATCH_FAILED'))
            }
        }
        const manager = new ChannelManager<User>({
            driver: new FailingUnwatch(),
            authorize: () => true,
        })
        const a0 = conn('c1')
        manager.register(a0)
        assert((await manager.subscribe(a0, 'news')).ok)

        const warned: string[] = []
        const realWarn = console.warn
        console.warn = (...args: unknown[]) => void warned.push(args.join(' '))
        Deno.env.set('STRICT_DEPRECATIONS', 'true')
        try {
            await assertRejects(
                () => manager.disconnect('c1'),
                Error,
                '[DEPRECATION] Since @lockness/realtime 0.4.0',
                'the deprecation error still wins the rejection',
            )
        } finally {
            Deno.env.delete('STRICT_DEPRECATIONS')
            console.warn = realWarn
        }

        assertEquals(
            warned.filter((line) =>
                line.includes('a disconnect teardown failed') &&
                line.includes('BROKER_UNWATCH_FAILED')
            ).length,
            1,
            'the teardown failure the rejection does not carry is WARNed exactly once',
        )
    },
)

Deno.test(
    '#392 pin — the id form still tears the connection down exactly as before',
    async () => {
        assertEquals(getCollector(), null, 'no collector leaked from above')
        const manager = managerOver()
        const a0 = conn('c1')
        manager.register(a0)
        assert((await manager.subscribe(a0, 'news')).ok)

        assertEquals(await manager.disconnect('c1'), 'disconnected')
        assertEquals(
            await manager.disconnect('c1'),
            'not-owned',
            'a second disconnect of a forgotten id reports not-owned, unchanged',
        )
    },
)
