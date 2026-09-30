/**
 * @fileoverview #349 — `LapseRun`, the driver-internal class that decides WHEN
 * the lapse handler runs and how that stops, tested on its own (plan A6).
 *
 * The Redis driver reports a lapse by calling `trigger()` from its heartbeat
 * and never awaits it. `LapseRun` owns everything after that: at most one run
 * in flight, however many lapses arrive during it exactly one trailing run,
 * none once closed, a run that never throws (a failure is one WARN plus the
 * driver's `onFailure`), and a `close()` that aborts the signal and waits for
 * the run in flight. No broker double: these are properties of the scheduler
 * alone, so they are witnessed without one.
 *
 * Committed red first: the module did not exist.
 *
 * @module @lockness/realtime/tests/lapse_run_349
 */

import { assert, assertEquals } from '@std/assert'
import { LapseRun } from '../drivers/lapse_run.ts'
import { watchingEscapes } from './escape_watcher.ts'

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
    const signals: AbortSignal[] = []
    const releases: Array<() => void> = []
    const handler = (signal: AbortSignal) => {
        signals.push(signal)
        return new Promise<void>((resolve) => void releases.push(resolve))
    }
    return {
        handler,
        signals,
        calls: () => signals.length,
        releaseNext: () => releases.shift()?.(),
    }
}

const RUN_FAILED = "re-asserting this instance's presence holds after a " +
    'liveness lapse failed — the next successful heartbeat retries'

Deno.test('#349 W11 close() while a run is gated aborts its signal, waits for it, and no run starts afterwards', async () => {
    const run = new LapseRun(() => {})
    const gated = gatedHandler()
    run.register(gated.handler)
    run.trigger()
    await settle()
    assertEquals(gated.calls(), 1)
    assertEquals(gated.signals[0].aborted, false)

    let closed = false
    const closing = run.close().then(() => void (closed = true))
    assertEquals(gated.signals[0].aborted, true, 'close() aborts the signal')
    await settle()
    assertEquals(closed, false, 'close() waits for the run in flight')

    gated.releaseNext()
    await closing
    run.trigger()
    await settle()
    assertEquals(gated.calls(), 1, 'no run starts after close()')
})

Deno.test('#349 W11 a lapse during the run is not run again once close() began', async () => {
    const run = new LapseRun(() => {})
    const gated = gatedHandler()
    run.register(gated.handler)
    run.trigger()
    await settle()
    run.trigger()
    const closing = run.close()
    gated.releaseNext()
    await closing
    await settle()
    assertEquals(gated.calls(), 1, 'the trailing run was dropped by close()')
})

Deno.test('#349 W12 three lapses during a gated run give exactly one trailing run', async () => {
    const run = new LapseRun(() => {})
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
})

Deno.test('#349 W14 trigger() after close() never calls the handler', async () => {
    const run = new LapseRun(() => {})
    let calls = 0
    run.register(() => void calls++)
    // Called, not yet resolved: the handler is still registered until close()
    // resumes, so only the closed state keeps this trigger from running it —
    // the window a heartbeat reply landing during close() falls into.
    const closing = run.close()
    run.trigger()
    await closing
    run.trigger()
    await settle()
    assertEquals(calls, 0)
})

Deno.test('#349 WS2 a handler that throws synchronously: one WARN, onFailure once, nothing escapes', async () => {
    await watchingEscapes(async (escaped) => {
        const warnings = captureWarnings()
        let failures = 0
        try {
            const run = new LapseRun(() => void failures++)
            run.register(() => {
                throw new Error('slot write refused')
            })
            run.trigger()
            await settle()
            assertEquals(warnings.lines.length, 1, 'exactly one WARN')
            assert(warnings.lines[0].includes(RUN_FAILED), warnings.lines[0])
            assert(
                warnings.lines[0].includes('slot write refused'),
                'the WARN renders the error',
            )
            assertEquals(failures, 1, 'onFailure called once')

            // The failed run is over: a later lapse runs again.
            run.trigger()
            await settle()
            assertEquals(failures, 2)
            await run.close()
            // A macrotask, so an unhandled rejection would have been dispatched.
            await new Promise((resolve) => setTimeout(resolve, 0))
            assertEquals(escaped, [], 'nothing escaped')
        } finally {
            warnings.restore()
        }
    })
})

Deno.test('#349 a rejecting handler: one WARN and onFailure', async () => {
    await watchingEscapes(async (escaped) => {
        const warnings = captureWarnings()
        let failures = 0
        try {
            const run = new LapseRun(() => void failures++)
            run.register(() => Promise.reject(new Error('2 slot(s) failed')))
            run.trigger()
            await settle()
            assertEquals(warnings.lines.length, 1)
            assert(warnings.lines[0].includes(RUN_FAILED))
            assertEquals(failures, 1)
            await run.close()
            await new Promise((resolve) => setTimeout(resolve, 0))
            assertEquals(escaped, [])
        } finally {
            warnings.restore()
        }
    })
})

Deno.test('#349 a successful run calls no onFailure and logs nothing', async () => {
    const warnings = captureWarnings()
    let failures = 0
    try {
        const run = new LapseRun(() => void failures++)
        run.register(async () => {})
        run.trigger()
        await settle()
        assertEquals(failures, 0)
        assertEquals(warnings.lines, [])
        await run.close()
    } finally {
        warnings.restore()
    }
})

Deno.test('#349 register() replaces the handler, and trigger() with none registered starts nothing', async () => {
    const run = new LapseRun(() => {})
    run.trigger()
    await settle()

    const called: string[] = []
    run.register(() => void called.push('first'))
    run.register(() => void called.push('second'))
    run.trigger()
    await settle()
    assertEquals(called, ['second'])
    await run.close()
})

Deno.test('#349 close() with no run in flight resolves, and drops the handler', async () => {
    const run = new LapseRun(() => {})
    let calls = 0
    run.register(() => void calls++)
    await run.close()
    await run.close()
    run.trigger()
    await settle()
    assertEquals(calls, 0)
})
