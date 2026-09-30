/**
 * @fileoverview #371 — `RosterMaintenanceRun`, the driver-internal class that
 * decides WHEN the owed-release drain runs and how that stops, tested on its
 * own — `LapseRun`'s own precedent (`lapse_run_349.test.ts`).
 *
 * The Redis driver reports a successful, connection-proving heartbeat by
 * calling `trigger()` and never awaits it. `RosterMaintenanceRun` owns
 * everything after that: at most one run in flight, however many ticks arrive
 * during it exactly one trailing run, none once closed (#371's W6: no drain
 * runs after `close()` has begun, and `close()` waits for one already in
 * flight rather than leaking it), and a run that never throws (a failure is
 * one WARN, with a marked-fallback line if even that WARN cannot be written).
 * No broker double: these are properties of the scheduler alone, so they are
 * witnessed without one — exactly `lapse_run_349.test.ts`'s own reasoning.
 *
 * Committed red first: the module did not exist.
 *
 * @module @lockness/realtime/tests/roster_maintenance_run_371
 */

import { assert, assertEquals } from '@std/assert'
import {
    ROSTER_MAINTENANCE_RUN_LOG_FAILED,
    RosterMaintenanceRun,
} from '../drivers/roster_maintenance_run.ts'
import { everyChannelThrows, watchingEscapes } from './escape_watcher.ts'

/** Run the microtask queue out. */
async function settle(times = 50): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve()
}

/** Collect every `console.warn` line until `restore()`. */
function captureWarnings() {
    const warn = console.warn
    const lines: string[] = []
    console.warn = (...parts: unknown[]) => void lines.push(parts.join(' '))
    return { lines, restore: () => void (console.warn = warn) }
}

/** A handler whose runs stay in flight until released, one at a time. */
function gatedHandler() {
    const releases: Array<() => void> = []
    let calls = 0
    const handler = () => {
        calls++
        return new Promise<void>((resolve) => void releases.push(resolve))
    }
    return {
        handler,
        calls: () => calls,
        releaseNext: () => releases.shift()?.(),
    }
}

const RUN_FAILED = 'a roster-maintenance run failed — the next successful ' +
    'heartbeat retries'

Deno.test(
    '#371 W6 close() while a run is gated waits for it, and no run starts afterwards',
    async () => {
        const run = new RosterMaintenanceRun()
        const gated = gatedHandler()
        run.register(gated.handler)
        run.trigger()
        await settle()
        assertEquals(gated.calls(), 1)

        let closed = false
        const closing = run.close().then(() => void (closed = true))
        await settle()
        assertEquals(closed, false, 'close() waits for the run in flight')

        gated.releaseNext()
        await closing
        run.trigger()
        await settle()
        assertEquals(gated.calls(), 1, 'no run starts after close()')
    },
)

Deno.test(
    '#371 W6 a tick during the run is not run again once close() began',
    async () => {
        const run = new RosterMaintenanceRun()
        const gated = gatedHandler()
        run.register(gated.handler)
        run.trigger()
        await settle()
        run.trigger()
        const closing = run.close()
        gated.releaseNext()
        await closing
        await settle()
        assertEquals(
            gated.calls(),
            1,
            'the trailing run was dropped by close()',
        )
    },
)

Deno.test(
    'three ticks during a gated run give exactly one trailing run',
    async () => {
        const run = new RosterMaintenanceRun()
        const gated = gatedHandler()
        run.register(gated.handler)
        run.trigger()
        await settle()
        run.trigger()
        run.trigger()
        run.trigger()
        await settle()
        assertEquals(gated.calls(), 1, 'one run in flight at a time')

        gated.releaseNext()
        await settle()
        assertEquals(gated.calls(), 2, 'exactly one trailing run')
        gated.releaseNext()
        await settle()
        assertEquals(gated.calls(), 2, 'and no more')
        await run.close()
    },
)

Deno.test('trigger() after close() never calls the handler', async () => {
    const run = new RosterMaintenanceRun()
    let calls = 0
    run.register(() => void calls++)
    const closing = run.close()
    run.trigger()
    await closing
    run.trigger()
    await settle()
    assertEquals(calls, 0)
})

Deno.test(
    'a handler that throws synchronously: one WARN, nothing escapes',
    async () => {
        await watchingEscapes(async (escaped) => {
            const warnings = captureWarnings()
            try {
                const run = new RosterMaintenanceRun()
                run.register(() => {
                    throw new Error('drain refused')
                })
                run.trigger()
                await settle()
                assertEquals(warnings.lines.length, 1, 'exactly one WARN')
                assert(warnings.lines[0].includes(RUN_FAILED))
                assert(
                    warnings.lines[0].includes('drain refused'),
                    'the WARN renders the error',
                )

                // The failed run is over: a later tick runs again.
                run.trigger()
                await settle()
                assertEquals(warnings.lines.length, 2)
                await run.close()
                await new Promise((resolve) => setTimeout(resolve, 0))
                assertEquals(escaped, [], 'nothing escaped')
            } finally {
                warnings.restore()
            }
        })
    },
)

Deno.test('a rejecting handler: one WARN, nothing escapes', async () => {
    await watchingEscapes(async (escaped) => {
        const warnings = captureWarnings()
        try {
            const run = new RosterMaintenanceRun()
            run.register(() => Promise.reject(new Error('EVAL refused')))
            run.trigger()
            await settle()
            assertEquals(warnings.lines.length, 1)
            assert(warnings.lines[0].includes(RUN_FAILED))
            await run.close()
            await new Promise((resolve) => setTimeout(resolve, 0))
            assertEquals(escaped, [])
        } finally {
            warnings.restore()
        }
    })
})

Deno.test('a successful run logs nothing', async () => {
    const warnings = captureWarnings()
    try {
        const run = new RosterMaintenanceRun()
        run.register(async () => {})
        run.trigger()
        await settle()
        assertEquals(warnings.lines, [])
        await run.close()
    } finally {
        warnings.restore()
    }
})

Deno.test(
    'register() replaces the handler, and trigger() with none registered starts nothing',
    async () => {
        const run = new RosterMaintenanceRun()
        run.trigger()
        await settle()

        const called: string[] = []
        run.register(() => void called.push('first'))
        run.register(() => void called.push('second'))
        run.trigger()
        await settle()
        assertEquals(called, ['second'])
        await run.close()
    },
)

Deno.test(
    'close() with no run in flight resolves, and drops the handler',
    async () => {
        const run = new RosterMaintenanceRun()
        let calls = 0
        run.register(() => void calls++)
        await run.close()
        await run.close()
        run.trigger()
        await settle()
        assertEquals(calls, 0)
    },
)

Deno.test(
    'a throwing console.warn on a failed run: one marked fallback line, nothing escapes',
    async () => {
        await watchingEscapes(async (escaped) => {
            using channels = everyChannelThrows()
            const run = new RosterMaintenanceRun()
            run.register(() => {
                throw new Error('drain refused')
            })
            run.trigger()
            await settle()
            assertEquals(channels.errorCalls(), 1, 'one ERROR line attempted')
            assert(
                channels.errorLines()[0].includes(
                    ROSTER_MAINTENANCE_RUN_LOG_FAILED,
                ),
                channels.errorLines()[0],
            )
            await run.close()
            await new Promise((resolve) => setTimeout(resolve, 0))
            assertEquals(escaped, [], 'nothing escaped')
        })
    },
)
