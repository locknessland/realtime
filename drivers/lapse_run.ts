/**
 * @fileoverview When the Redis driver's lapse handler runs, and how that stops
 * (#349, plan A6).
 *
 * The driver detects a liveness lapse in its heartbeat and calls
 * {@link LapseRun.trigger}; it never awaits the handler, because a heartbeat
 * that waited behind K slot writes would cause the next lapse. Everything
 * after the trigger lives here:
 *
 * - at most one run in flight;
 * - however many lapses arrive during a run, exactly ONE trailing run;
 * - none once closed;
 * - a run never throws: a failure is one WARN plus the driver's `onFailure`,
 *   which marks the lapse suspected so the next successful heartbeat retries —
 *   there is no retry timer;
 * - {@link LapseRun.close} aborts the handler's signal and waits for the run in
 *   flight before it drops the handler.
 *
 * Concrete, internal and lapse-specific on purpose: no interface and no
 * generic runner. The ghost sweep is level-triggered and re-arms itself; this
 * hook is edge-triggered and coalesces. They share no scheduling.
 * Package-internal — not exported from `mod.ts`.
 *
 * @module @lockness/realtime/drivers/lapse_run
 */

import { renderError } from '@lockness/contract'
import {
    markedFallbackMarker,
    writeMarkedFallback,
} from '../marked_fallback.ts'

/**
 * The marker that starts the one ERROR line written when a failed run's WARN
 * could not be, because `console.warn` threw (#395). Nothing awaits a run, so
 * that throw would reach the runtime as an unhandled rejection. The line
 * carries the run's failure and the sink's, each rendered; the marker is the
 * fixed prefix, so an error text cannot forge it. Exported for the test suite
 * only.
 */
export const LAPSE_RUN_LOG_FAILED = markedFallbackMarker(
    'realtime: a lapse-run failure could not be logged (#395):',
)

/** The lapse handler: writes this process's holds again, stopping on `signal`. */
export type LapseHandler = (signal: AbortSignal) => void | Promise<void>

/**
 * The single home of when the lapse handler runs and how the run is shut down
 * (#349 decision table, rows 5 and 6).
 *
 * @example
 * ```ts
 * const lapse = new LapseRun(() => { suspected = true })
 * lapse.register((signal) => reassert(signal))
 * lapse.trigger() // from the heartbeat, never awaited
 * await lapse.close() // from the driver's close()
 * ```
 */
export class LapseRun {
    readonly #onFailure: () => void
    readonly #abort = new AbortController()
    #handler?: LapseHandler
    /** The run in flight; it never rejects. */
    #run?: Promise<void>
    /** Set by a trigger during a run: exactly one more run follows it. */
    #trailing = false
    #closed = false

    /**
     * @param onFailure - Called once for every run whose handler threw or
     *   rejected, after its WARN. The Redis driver marks the lapse suspected,
     *   so its next successful heartbeat triggers a run again.
     */
    constructor(onFailure: () => void) {
        this.#onFailure = onFailure
    }

    /**
     * Register the handler. One handler: registering again replaces it.
     *
     * @param handler - Called with this run's abort signal.
     */
    register(handler: LapseHandler): void {
        this.#handler = handler
    }

    /**
     * Report a lapse. Starts a run unless one is in flight — then exactly one
     * trailing run follows it, however many lapses arrive meanwhile. Does
     * nothing once closed, or while no handler is registered. Never awaits the
     * run and never throws.
     */
    trigger(): void {
        if (this.#closed || !this.#handler) return
        if (this.#run) {
            this.#trailing = true
            return
        }
        const handler = this.#handler
        this.#run = this.#invoke(handler).finally(() => {
            this.#run = undefined
            if (this.#trailing && !this.#closed) {
                this.#trailing = false
                this.trigger()
            }
        })
    }

    /**
     * One run, contained: a handler that throws synchronously or rejects is one
     * WARN, carrying no member id or channel, then `onFailure`. Never rejects,
     * not even when the WARN itself throws: that is one
     * {@link LAPSE_RUN_LOG_FAILED} line instead (#395).
     */
    async #invoke(handler: LapseHandler): Promise<void> {
        try {
            await handler(this.#abort.signal)
        } catch (error) {
            try {
                console.warn(
                    "realtime: re-asserting this instance's presence holds " +
                        'after a liveness lapse failed — the next successful ' +
                        `heartbeat retries: ${renderError(error)}`,
                )
            } catch (sink) {
                // #395: `trigger()` never awaits this run, so a throwing sink
                // would escape as an unhandled rejection. One marked line.
                writeMarkedFallback(LAPSE_RUN_LOG_FAILED, error, {
                    label: 'sink failure',
                    error: sink,
                })
            }
            this.#onFailure()
        }
    }

    /**
     * Stop: mark closed and abort the signal — both synchronously, before the
     * first await — then wait for the run in flight, then drop the handler.
     * No run starts once this has been called. Idempotent.
     *
     * @returns Settles once the run in flight, if any, has settled.
     */
    async close(): Promise<void> {
        this.#closed = true
        this.#abort.abort()
        await this.#run
        this.#handler = undefined
    }
}
