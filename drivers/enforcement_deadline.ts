/**
 * @fileoverview The revocation enforcement deadline (#362): one timer that
 * says so, once, when no revocation pass has completed without failures
 * within `revocationTtlSeconds` of the last clean pass's start (#384).
 *
 * The Redis driver promises that a revocation whose one-shot control frame
 * was lost is still applied within a bound — the one home of that bound is
 * `onRevocationReconcile` in `drivers/redis.ts`, and nothing here restates it.
 * The constructor refuses a timing that could never keep the promise; this
 * module watches the promise at runtime. It is revocation-specific,
 * internal, and not exported from `mod.ts`.
 *
 * **What it judges.** Only the guarantee, never a performance value: a
 * record lives at least `revocationTtlSeconds`, so the guarantee is broken
 * exactly when no pass has completed without failures within one TTL of the
 * START of the last clean pass. The deadline is armed from that start,
 * re-armed only by a clean pass, and never by any other (#384): which pass is
 * clean is the driver's decision, made at its pass's end site. Every other
 * pass that settles says so, verdict-free, through
 * {@link EnforcementDeadline.passEnded}, so an expiry after it is `MISSED` and
 * never blames a pass that is merely in flight.
 *
 * **What it never does.** It never frees the pass slot, never starts or
 * abandons a pass, never re-arms on EXPIRY (a stall of any length is ONE
 * line), and never reads a clock of its own: `now` is the driver's pass
 * clock. **Only {@link EnforcementDeadline.close} drops a line that is
 * decided but not yet written**: an arm that finds one writes it first, on a
 * 0 ms timer, then arms the remaining time. Whether a timer may be armed at
 * all is the driver's `#closing` decision, so {@link EnforcementDeadline.close}
 * is not terminal.
 *
 * **How it writes.** In its own timer callback, never from the driver's pass
 * end site, and in the #369 shape: a `console.warn` that throws becomes one
 * marked `console.error` line, the marker in the fixed prefix and both halves
 * rendered — written by `writeMarkedFallback`, which never throws (#391).
 *
 * @module @lockness/realtime/drivers/enforcement_deadline
 */

import {
    markedFallbackMarker,
    writeMarkedFallback,
} from '../marked_fallback.ts'

/**
 * The WARN written when the deadline expires with a pass still in flight and
 * no pass ended since the last clean one. It is followed by the pass's
 * trigger and age, and points at the command port's contract, because a pass
 * that outlives a TTL is almost always a command that never settled.
 */
export const REVOCATION_DEADLINE_STALLED =
    'realtime: revocation deadline STALLED (#362): no revocation pass ' +
    'completed without failures within revocationTtlSeconds of the last ' +
    "clean pass's start, and the pass in flight has not settled"

/**
 * The WARN written when the deadline expires with no pass in flight, or after
 * a pass ended that was not clean (#384), and by every overdue arm. Passes are
 * failing, or completing with failures (their own WARNs precede it), or are
 * slower than the bound allows.
 */
export const REVOCATION_DEADLINE_MISSED =
    'realtime: revocation deadline MISSED (#362): no revocation pass ' +
    'completed without failures within revocationTtlSeconds of the last ' +
    "clean pass's start"

/**
 * The WARN written when the broker's clock advanced by at least one TTL
 * between two successful passes, while the local deadline was still
 * pending: records live on the broker's clock, so some may have expired
 * unapplied although no local window was broken. It names both reap times.
 */
export const REVOCATION_DEADLINE_SKEWED =
    "realtime: revocation deadline SKEWED (#362): the broker's clock " +
    'advanced by at least revocationTtlSeconds between two successful ' +
    'revocation passes'

/**
 * The marked ERROR line written when a revocation log line could not be:
 * a throwing `console.warn` at the deadline's fire, or a rejection reaching
 * the end of the driver's pass chain. The marker is the fixed prefix, so an
 * error text cannot forge it.
 */
export const REVOCATION_LOG_FAILED = markedFallbackMarker(
    'realtime: a revocation log line could not be written (#362):',
)

/** Where every deadline line sends a reader. */
const SEE =
    'The enforcement bound: onRevocationReconcile in packages/realtime/drivers/redis.ts.'

/** A revocation pass in flight, as the deadline reads it. */
export interface PassInFlight {
    /** What started the pass: the driver's own trigger name. */
    readonly trigger: string
    /** When it started, on the pass clock. */
    readonly startedAt: number
}

/** How an {@link EnforcementDeadline} is built. */
export interface EnforcementDeadlineOptions {
    /** `revocationTtlSeconds`, in milliseconds; it fits one timer. */
    readonly ttlMs: number
    /** The driver's pass clock, a monotonic millisecond reading. */
    readonly now: () => number
    /** The pass in flight, if any; read only by a timer that expires. */
    readonly inFlight: () => PassInFlight | undefined
}

/**
 * The revocation enforcement deadline: one timer, re-armed by a clean pass,
 * anchored at that pass's start, and writing at most one line per
 * episode. An expiry never re-arms it; a carried line is followed by the arm
 * it was carried through.
 *
 * @example
 * ```ts
 * const clock = { now: 0 } // the driver passes its monotonic pass clock
 * const deadline = new EnforcementDeadline({
 *     ttlMs: 300_000,
 *     now: () => clock.now,
 *     inFlight: () => undefined,
 * })
 * deadline.arm(300_000)
 * deadline.passSucceeded(0, 40, 1_800_000_000)
 * deadline.close()
 * ```
 */
export class EnforcementDeadline {
    readonly #ttlMs: number
    readonly #now: () => number
    readonly #inFlight: () => PassInFlight | undefined
    /** The one pending timer, if any. */
    #timer?: ReturnType<typeof setTimeout>
    /**
     * Lines decided but not yet written (an overdue `MISSED`, a `SKEWED`).
     * Only {@link close} drops them: a 0 ms timer lands after a few
     * milliseconds, and a fast pass ending before it must not erase the only
     * report of a loss.
     */
    #unwritten: string[] = []
    /** The reap time of the previous successful pass, in broker seconds. */
    #previousReadAt?: number
    /**
     * Whether a pass settled without being clean since the last clean one
     * (#384). Set by {@link passEnded}; cleared by {@link passSucceeded} and
     * {@link close}. An expiry while it is set writes `MISSED`, even with a
     * pass in flight.
     */
    #ended = false

    /**
     * Build a deadline; nothing is armed until {@link arm}.
     *
     * @param options - The TTL, the pass clock and the in-flight getter.
     */
    constructor(options: EnforcementDeadlineOptions) {
        this.#ttlMs = options.ttlMs
        this.#now = options.now
        this.#inFlight = options.inFlight
    }

    /**
     * Arm the deadline `delayMs` from now, replacing any pending timer.
     *
     * **A decided line is carried, never dropped**: when one is still
     * unwritten, a 0 ms timer writes it first and then arms the time that
     * remains. A timer that expires on its own decides its line AT FIRE:
     * `STALLED`, naming the pass in flight, or `MISSED` when there is none.
     * **An overdue arm (`delayMs ≤ 0`) decides `MISSED` now**, and writes it
     * on a 0 ms timer: a pass started in between — a trailing pass the
     * driver's end site started before this call — must not be named as the
     * stalled one.
     *
     * @param delayMs - How long from now, in milliseconds.
     */
    arm(delayMs: number): void {
        const armedAt = this.#now()
        this.#clearTimer()
        if (this.#unwritten.length > 0) {
            this.#setTimer(0, () => {
                this.#flush()
                this.arm(delayMs - (this.#now() - armedAt))
            })
            return
        }
        if (delayMs > 0) {
            this.#setTimer(delayMs, () => this.#write(this.#expired()))
            return
        }
        this.#unwritten.push(this.#missed())
        this.#setTimer(0, () => this.#flush())
    }

    /**
     * Record a successful pass: re-arm the deadline one TTL after its start.
     *
     * **The broker-clock check runs first** (S1). When the previous success's
     * reap time is known, this one's is at least one TTL later, and the local
     * deadline is still pending, the records may have expired on the broker's
     * clock although no local window was broken: `SKEWED` is decided now, and
     * the arm below carries it (see {@link arm}). A step that lands after the
     * local deadline already fired adds nothing to that episode. An
     * `undefined` reap time skips the check, and so does a decided line
     * already sitting in {@link #unwritten} (#383): a pending carry timer also
     * satisfies "the local deadline is still pending", and an overdue `MISSED`
     * already reports the broken guarantee for this episode — the broader of
     * the two signals — so a coinciding `SKEWED` would be a second line for
     * one episode, which {@link EnforcementDeadline}'s own contract forbids.
     *
     * @param startedAt - The pass's start, on the pass clock.
     * @param endedAt - The pass's end, on the pass clock.
     * @param readAt - The reap time of its completed enumeration, in broker
     *   seconds, or `undefined` when none has completed.
     */
    passSucceeded(
        startedAt: number,
        endedAt: number,
        readAt: number | undefined,
    ): void {
        this.#ended = false
        const previous = this.#previousReadAt
        this.#previousReadAt = readAt
        if (
            previous !== undefined && readAt !== undefined &&
            this.#timer !== undefined &&
            this.#unwritten.length === 0 &&
            readAt - previous >= this.#ttlMs / 1000
        ) {
            this.#unwritten.push(
                `${REVOCATION_DEADLINE_SKEWED} — reap times ` +
                    `${previous}s and ${readAt}s. ${SEE}`,
            )
        }
        this.arm(this.#ttlMs - (endedAt - startedAt))
    }

    /**
     * Record that a pass settled without being clean (#384) — failed, or
     * completed with a failure or a malformed tally. **Verdict-free**: it
     * never arms, re-arms or writes. Which pass is clean is the driver's
     * decision, at its pass's end site. It only records that the window was
     * broken by a pass that ENDED, so an expiry after it writes `MISSED`
     * rather than naming the pass then in flight as `STALLED` and blaming the
     * command port. {@link passSucceeded} and {@link close} forget it.
     *
     * @example
     * ```ts
     * deadline.passEnded() // a pass completed with failures
     * ```
     */
    passEnded(): void {
        this.#ended = true
    }

    /**
     * Clear the pending timer, and drop any line decided but not yet
     * written. Not terminal: whether a timer may be armed again is the
     * driver's decision.
     */
    close(): void {
        this.#clearTimer()
        this.#unwritten = []
        this.#ended = false
    }

    /**
     * The line a timer that expired on its own writes, decided now: `MISSED`
     * when no pass is in flight, or when a pass ended since the last clean
     * one (#384); `STALLED`, naming the pass in flight, otherwise.
     */
    #expired(): string {
        const pass = this.#inFlight()
        if (pass === undefined || this.#ended) return this.#missed()
        return `${REVOCATION_DEADLINE_STALLED} — trigger ${pass.trigger}, ` +
            `age ${this.#now() - pass.startedAt}ms. A command on the port ` +
            'never settled: every command settles is the RedisCommandClient ' +
            `contract, and the driver does not cancel one. ${SEE}`
    }

    /** The `MISSED` line. */
    #missed(): string {
        return `${REVOCATION_DEADLINE_MISSED} — passes are failing or are ` +
            'completing with failures (see the WARNs before this), or are ' +
            'slower than the bound allows, so a ' +
            'revocation whose control frame was lost may expire unapplied. ' +
            SEE
    }

    /** Write every decided line, in the order it was decided. */
    #flush(): void {
        const lines = this.#unwritten
        this.#unwritten = []
        for (const line of lines) this.#write(line)
    }

    /**
     * Write one line in the #369 shape: the sink never throws past itself,
     * and neither does its marked fallback (#391). The marker is the fixed
     * prefix; both halves are rendered.
     */
    #write(text: string): void {
        try {
            console.warn(text)
        } catch (failure) {
            writeMarkedFallback(REVOCATION_LOG_FAILED, text, {
                label: 'sink failure',
                error: failure,
            })
        }
    }

    /** Clear the pending timer, if any. */
    #clearTimer(): void {
        if (this.#timer === undefined) return
        clearTimeout(this.#timer)
        this.#timer = undefined
    }

    /**
     * Replace the pending timer with one that clears its own handle, then
     * runs `fire`. The one timer the deadline holds; unref'd, so it is never
     * the reason a process stays alive.
     */
    #setTimer(delayMs: number, fire: () => void): void {
        this.#clearTimer()
        const id = setTimeout(() => {
            this.#timer = undefined
            fire()
        }, delayMs)
        Deno.unrefTimer(id)
        this.#timer = id
    }
}
