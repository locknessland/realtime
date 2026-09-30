/**
 * @fileoverview The one home of the marked fallback line (#369, #391) —
 * internal to `@lockness/realtime`, never re-exported from `mod.ts`.
 *
 * Several sinks in this package end a chain that has no caller left to hand a
 * failure to: a WARN that could not be written, an `onError` hook that threw,
 * a pass chain's last `.catch`. Each writes ONE marked line instead, and on
 * Deno a throw from that line would be the very unhandled rejection (or
 * uncaught timer exception) it exists to stop — the process terminates. So
 * the line is written here, and this function never throws.
 *
 * **The format is fixed**: the marker is the fixed prefix, so no error text
 * can forge it; every variable part goes through `renderError`, so no error
 * text can break the line or smuggle a control character:
 *
 * ```text
 * <marker> <renderError(subject)>[; <label>: <renderError(failure.error)>]
 * ```
 *
 * **The channels, in order.** `console.error` first, so an application that
 * patched its console still receives the line. If that throws, the same line
 * and a newline go to `Deno.stderr.writeSync`, which bypasses the console
 * entirely. If THAT throws too, nothing is left to write to, and the line is
 * dropped by the one final catch — see the comment there. What this does not
 * solve: a line that reaches neither channel is lost, and a partial write to
 * stderr is not retried.
 *
 * @module @lockness/realtime/marked_fallback
 */

import { renderError } from '@lockness/contract'

/** The second half of a two-part marked line: what ALSO failed, and how. */
export interface MarkedFallbackFailure {
    /** The words before the second rendered error. */
    readonly label: 'hook failure' | 'sink failure'
    /** What the hook or the log sink threw. */
    readonly error: unknown
}

/**
 * A brand carried by every marker this module hands out (#399) — assignable
 * to `string` (it IS one; nothing here is boxed or copied), but not the other
 * way around, so `writeMarkedFallback`'s `marker` parameter rejects a string
 * built ad hoc at a call site and accepts only a value that passed through
 * {@link markedFallbackMarker}.
 *
 * **Why a brand and not a literal union of every marker constant.** This
 * module is imported by every sink in the package (websocket.ts, manager.ts,
 * every driver); a union would need a type-only import of each of their
 * marker constants IN RETURN, so the one home of the marked-fallback line
 * would name every caller it has, and adding a caller's marker would mean
 * editing this file too. The brand keeps the invariant — no ad hoc string
 * reaches `writeMarkedFallback` — without that inversion: a new marker is
 * declared once, at its own call site, through {@link markedFallbackMarker}.
 * What it does not solve: two constants with identical text still both
 * type-check as valid markers — nominal branding narrows the TYPE a marker
 * must have, not the VALUE two different call sites choose. A literal union
 * would carry the same residual.
 */
export type MarkedFallbackMarker = string & {
    readonly __markedFallbackMarker: unique symbol
}

/**
 * Declare a marker line prefix — the only way to produce
 * {@link MarkedFallbackMarker}, so every constant `writeMarkedFallback` ever
 * receives is declared, once, next to the text it names.
 *
 * @param text - The fixed prefix, written verbatim by
 *   {@link writeMarkedFallback}; never rendered, so no error text can forge
 *   it.
 * @returns `text`, unchanged at runtime — branding is a compile-time-only
 *   cast, not a wrapper.
 *
 * @example
 * ```ts
 * export const SWEEP_LOG_FAILED = markedFallbackMarker(
 *     'realtime: a ghost-sweep log line could not be written (#360):',
 * )
 * ```
 */
export function markedFallbackMarker(text: string): MarkedFallbackMarker {
    return text as MarkedFallbackMarker
}

/**
 * Write one marked fallback line, and never throw.
 *
 * **Relies on `renderError` never throwing.** Both calls below run inside the
 * one `try` that also guards `console.error`, so if `renderError` itself
 * threw, this function would still contain it the same way — but the stderr
 * fallback's own text (built from `line`, computed before `console.error` is
 * even reached) would then still be the marker alone rather than the full
 * line. `renderError` is `@lockness/contract`'s own guarantee never to throw;
 * this module trusts it and does not re-guard it here.
 *
 * @param marker - The fixed prefix; written verbatim, never rendered. Only
 *   {@link markedFallbackMarker} produces one.
 * @param subject - What the line reports, rendered after the marker.
 * @param failure - The second half, when the line carries two failures.
 * @returns Nothing: it cannot fail, only fail to be seen.
 *
 * @example
 * ```ts
 * writeMarkedFallback(SWEEP_LOG_FAILED, error)
 * writeMarkedFallback(PASS_SAMPLE_LOG_FAILED, failure, {
 *     label: 'sink failure',
 *     error: sink,
 * })
 * ```
 */
export function writeMarkedFallback(
    marker: MarkedFallbackMarker,
    subject: unknown,
    failure?: MarkedFallbackFailure,
): void {
    let line: string = marker
    try {
        line = `${marker} ${renderError(subject)}` +
            (failure === undefined
                ? ''
                : `; ${failure.label}: ${renderError(failure.error)}`)
        console.error(line)
    } catch (consoleFailure) {
        // The console refused the ERROR line: write it past the console,
        // naming what the console itself threw (#399) — the prior line named
        // only what console.error was given, never why it refused it.
        try {
            Deno.stderr.writeSync(
                new TextEncoder().encode(
                    `${line}; console failure: ${
                        renderError(consoleFailure)
                    }\n`,
                ),
            )
        } catch {
            // #391 THE LAST RESORT: the console and stderr both refused, so
            // no channel is left to log this on, and a re-throw would reach
            // a caller that has none — an unhandled rejection or an uncaught
            // timer exception, which terminates the process on Deno. Dropping
            // one log line is the lesser harm.
        }
    }
}
