/**
 * @fileoverview The one bound on how long `RedisBroadcastDriver.close()`
 * waits for the work it has in flight (#368).
 *
 * `close()` awaits its ghost-sweep pass, then its lapse run, then its
 * roster-maintenance drain (#349's and #371's order). All three sit behind
 * the same serialised command port, so a command that never settles — a port
 * that violates the {@link RedisCommandClient} contract — used to make
 * `close()` hang forever: a graceful shutdown that never returns, with the
 * owned ports never closed. This module is the one bound: `close()` waits
 * for at most one liveness TTL, then carries on with its teardown regardless
 * of what is still pending.
 *
 * `close()` never cancels a command and never frees the sweep slot — past
 * the budget, a still-stalled wait behaves exactly like a crash, which is
 * the failure the ghost sweep was built to survive (ADR 005, ADR 006). The
 * bound is not a new timeout: it is `Math.min(livenessTtlSeconds * 1000,
 * MAX_TIMER_MS)`, the driver's own existing definition of when a silent
 * instance counts as dead, computed by `close()` and handed in here as
 * `budgetMs`. There is no new tuning constant.
 *
 * Concrete, internal and close-specific on purpose, following the precedent
 * of `lapse_run.ts`: one function, no interface, no generic runner.
 * Package-internal — not exported from `mod.ts`.
 *
 * @module @lockness/realtime/drivers/close_drain
 */

/**
 * Which of the three awaited works were still pending when the budget
 * expired. All `false` means every one settled before the budget ran out.
 */
export interface CloseDrainPending {
    /** The ghost-sweep pass had not settled. */
    readonly sweepPass: boolean
    /** The lapse run had not settled. */
    readonly lapseRun: boolean
    /** The roster-maintenance drain had not settled (#371). */
    readonly maintenanceDrain: boolean
}

/**
 * Await `sweepPass`, then `lapseRun`, then `maintenanceDrain` (#349's and
 * #371's order), all against ONE shared expiry armed for `budgetMs` from this
 * call — never a budget per step, or a command port that stalls on its own
 * tail would multiply the wait for nothing, on a driver that serialises every
 * exchange.
 *
 * **The timer is deliberately ref'd**, unlike every other timer this driver
 * holds. Its job is to let an awaited call finish: a stall that holds no
 * I/O — a command whose port never settles it and never schedules anything
 * else — must still let `close()` resolve, and an unref'd timer is not
 * guaranteed to fire when it is the only thing left keeping the process
 * running. It is cleared the moment all three works have settled, so a
 * healthy drain leaves nothing behind.
 *
 * Never awaits any of the three past the expiry: once the budget is spent,
 * this returns immediately, naming whichever had not yet settled. None of the
 * three promises is cancelled — there is no such thing on this driver's
 * command port (#362) — so a late settlement is still possible after this
 * returns; `close()` decides what that means.
 *
 * @param budgetMs - The shared expiry, in milliseconds, from this call.
 * @param sweepPass - The ghost-sweep pass in flight, or `undefined` when none
 *   is running.
 * @param lapseRun - The lapse run's stop promise (`LapseRun.close()`),
 *   always present: closing it is unconditional.
 * @param maintenanceDrain - The roster-maintenance run's stop promise
 *   (`RosterMaintenanceRun.close()`, #371), always present: closing it is
 *   unconditional.
 * @returns Which of the three, if any, were still pending at expiry.
 *
 * @example
 * ```ts
 * const pending = await awaitCloseDrain(
 *     budgetMs,
 *     this.#reconcilePass,
 *     stopped,
 *     maintenanceStopped,
 * )
 * if (pending.sweepPass || pending.lapseRun || pending.maintenanceDrain) {
 *     // log once, then tear down regardless.
 * }
 * ```
 */
export async function awaitCloseDrain(
    budgetMs: number,
    sweepPass: Promise<void> | undefined,
    lapseRun: Promise<void>,
    maintenanceDrain: Promise<void>,
): Promise<CloseDrainPending> {
    let resolveExpiry = (): void => {}
    const expiry = new Promise<void>((resolve) => {
        resolveExpiry = resolve
    })
    const timer = setTimeout(resolveExpiry, budgetMs)

    let sweepPassSettled = sweepPass === undefined
    if (sweepPass !== undefined) {
        await Promise.race([
            sweepPass.then(() => {
                sweepPassSettled = true
            }),
            expiry,
        ])
    }

    let lapseRunSettled = false
    await Promise.race([
        lapseRun.then(() => {
            lapseRunSettled = true
        }),
        expiry,
    ])

    let maintenanceDrainSettled = false
    await Promise.race([
        maintenanceDrain.then(() => {
            maintenanceDrainSettled = true
        }),
        expiry,
    ])

    clearTimeout(timer)
    return {
        sweepPass: !sweepPassSettled,
        lapseRun: !lapseRunSettled,
        maintenanceDrain: !maintenanceDrainSettled,
    }
}
