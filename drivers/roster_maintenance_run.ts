/**
 * @fileoverview When the Redis driver's roster-maintenance handler runs, and
 * how that stops (#371).
 *
 * The driver fires {@link RosterMaintenanceRun.trigger} after every
 * successful heartbeat; it never awaits the handler, on `LapseRun`'s own
 * reasoning (`drivers/lapse_run.ts`): a heartbeat that waited behind the
 * drain's slot writes would cause the very lapse #349's remedy exists to
 * repair. Everything after the trigger lives here:
 *
 * - at most one run in flight;
 * - however many ticks arrive during a run, exactly ONE trailing run;
 * - none once closed;
 * - a run never throws: a failure is one WARN, with a marked-fallback line
 *   if even that WARN cannot be written (#391);
 * - {@link RosterMaintenanceRun.close} waits for the run in flight before it
 *   drops the handler.
 *
 * Concrete and internal, on `LapseRun`'s own precedent: a shared abstraction
 * is warranted only once a SECOND production driver needs three or more of
 * `BroadcastDriver`'s notification hooks (ADR 007 §2) — not before. Unlike
 * `LapseRun`, the handler here takes no `AbortSignal`: `onRosterMaintenance`'s
 * contract is "no arguments" (its payload is nothing, the whole reason it is
 * its own hook), so a run in flight is waited out rather than cut short.
 * Package-internal — not exported from `mod.ts`.
 *
 * @module @lockness/realtime/drivers/roster_maintenance_run
 */

import { renderError } from '@lockness/contract'
import {
    markedFallbackMarker,
    writeMarkedFallback,
} from '../marked_fallback.ts'

/**
 * The marker that starts the one ERROR line written when a failed run's WARN
 * could not be, because `console.warn` threw (#391). Nothing awaits a run, so
 * that throw would reach the runtime as an unhandled rejection. The line
 * carries the run's failure and the sink's, each rendered; the marker is the
 * fixed prefix, so an error text cannot forge it. Exported for the test suite
 * only.
 */
export const ROSTER_MAINTENANCE_RUN_LOG_FAILED = markedFallbackMarker(
    'realtime: a roster-maintenance run failure could not be logged (#391):',
)

/** The maintenance handler: drains the owed-release ledger. */
export type RosterMaintenanceHandler = () => void | Promise<void>

/**
 * The single home of when the roster-maintenance handler runs and how the run
 * is shut down (#371) — `LapseRun`'s shape, without an `AbortSignal`.
 *
 * @example
 * ```ts
 * const maintenance = new RosterMaintenanceRun()
 * maintenance.register(() => drain())
 * maintenance.trigger() // from the heartbeat, never awaited
 * await maintenance.close() // from the driver's close()
 * ```
 */
export class RosterMaintenanceRun {
    #handler?: RosterMaintenanceHandler
    /** The run in flight; it never rejects. */
    #run?: Promise<void>
    /** Set by a trigger during a run: exactly one more run follows it. */
    #trailing = false
    #closed = false

    /**
     * Register the handler. One handler: registering again replaces it.
     *
     * @param handler - Called with no arguments, once per run.
     */
    register(handler: RosterMaintenanceHandler): void {
        this.#handler = handler
    }

    /**
     * Report a tick. Starts a run unless one is in flight — then exactly one
     * trailing run follows it, however many ticks arrive meanwhile. Does
     * nothing once closed, or while no handler is registered. Never awaits
     * the run and never throws.
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
     * One run, contained: a handler that throws synchronously or rejects is
     * one WARN, naming no channel or member. Never rejects, not even when the
     * WARN itself throws: that is one
     * {@link ROSTER_MAINTENANCE_RUN_LOG_FAILED} line instead (#391).
     */
    async #invoke(handler: RosterMaintenanceHandler): Promise<void> {
        try {
            await handler()
        } catch (error) {
            try {
                console.warn(
                    'realtime: a roster-maintenance run failed — the next ' +
                        `successful heartbeat retries: ${renderError(error)}`,
                )
            } catch (sink) {
                // #391: `trigger()` never awaits this run, so a throwing sink
                // would escape as an unhandled rejection. One marked line.
                writeMarkedFallback(ROSTER_MAINTENANCE_RUN_LOG_FAILED, error, {
                    label: 'sink failure',
                    error: sink,
                })
            }
        }
    }

    /**
     * Stop: mark closed, synchronously, then wait for the run in flight, then
     * drop the handler. No run starts once this has been called. Idempotent.
     *
     * @returns Settles once the run in flight, if any, has settled.
     */
    async close(): Promise<void> {
        this.#closed = true
        await this.#run
        this.#handler = undefined
    }
}
