/**
 * @fileoverview The `unhandledrejection` watcher, and the every-channel-throws
 * console, that the containment witnesses share (#374, #376, #391, #395).
 *
 * On Deno an unhandled rejection terminates the process, so a witness that
 * let one through would kill the runner instead of failing by name. The
 * watcher records each escape and `preventDefault()`s it, and the witness
 * asserts on what it recorded.
 *
 * @module @lockness/realtime/tests/escape_watcher
 */

import { AssertionError } from '@std/assert'

/**
 * The real `setTimeout`, captured at module load — before any witness installs
 * FakeTime — so {@link settle} always yields a real macrotask.
 */
const REAL_SET_TIMEOUT = globalThis.setTimeout

/**
 * Yield five real macrotasks: a rejection's `unhandledrejection` event is
 * dispatched after one, and a chain a few hops long needs the rest.
 *
 * @returns A promise that resolves after the fifth macrotask.
 */
export async function settle(): Promise<void> {
    for (let i = 0; i < 5; i++) {
        await new Promise((resolve) => REAL_SET_TIMEOUT(resolve, 0))
    }
}

/**
 * Run `body` while recording every rejection that reaches the runtime
 * unhandled — the one `unhandledrejection` watcher this package's tests share
 * (#374). Its contract, which no caller re-derives:
 *
 * - **preventDefault.** Every escape is `preventDefault()`ed, so on Deno, which
 *   terminates the process on an unhandled rejection, a regression fails the
 *   row on its assertion instead of killing the runner.
 * - **record.** Every escape's reason is pushed onto the live `escaped` list
 *   `body` receives, in dispatch order, for the row to assert on.
 * - **removed in `finally`, one macrotask late.** The listener is removed
 *   whatever happens, but only after one real macrotask. A rejection nobody
 *   handled is dispatched once the microtask queue drains; removed in the same
 *   turn as a failing assertion, the listener would be gone before the event
 *   arrived, and the file would die as an `(uncaught error)` rather than fail
 *   that row by name. The macrotask is real (captured at module load), so it
 *   runs under FakeTime too.
 * - **a late escape fails.** An escape that lands during that macrotask, after
 *   `body` returned normally, was never asserted on; the watcher throws an
 *   `AssertionError` naming it rather than swallow it. When `body` threw, its
 *   error is the one reported.
 *
 * A row that deliberately needs a different shape — its own listener, say, to
 * witness this helper — installs one and says why in a comment.
 *
 * @param body - The witness; it receives the live list of escaped reasons.
 * @returns A promise that settles as `body` does.
 * @throws {AssertionError} When a rejection escaped after `body` returned.
 *
 * @example
 * ```ts
 * await watchingEscapes(async (escaped) => {
 *     fireAndForget()
 *     await settle()
 *     assertEquals(escaped, [], 'no rejection escaped')
 * })
 * ```
 */
export async function watchingEscapes(
    body: (escaped: unknown[]) => Promise<void>,
): Promise<void> {
    const escaped: unknown[] = []
    const listener = (event: PromiseRejectionEvent) => {
        event.preventDefault()
        escaped.push(event.reason)
    }
    globalThis.addEventListener('unhandledrejection', listener)
    let asserted: number
    try {
        await body(escaped)
        asserted = escaped.length
    } finally {
        await new Promise((resolve) => REAL_SET_TIMEOUT(resolve, 0))
        globalThis.removeEventListener('unhandledrejection', listener)
    }
    const late = escaped.slice(asserted)
    if (late.length > 0) {
        throw new AssertionError(
            `${late.length} rejection(s) escaped after the witness returned: ` +
                late.map(String).join('; '),
        )
    }
}

/** Every log channel refusing, as {@link everyChannelThrows} installs it. */
export interface ThrowingChannels extends Disposable {
    /** How many times `console.error` was attempted. */
    errorCalls(): number
    /** Each attempted `console.error` line, its parts joined by a space. */
    errorLines(): readonly string[]
}

/**
 * `console.warn` and `console.error` refusing, but `Deno.stderr.writeSync`
 * left working and recording — the one shape a witness needs to prove the
 * fallback reaches PAST the console (#391, #399), not merely that
 * `console.error` was attempted. `everyChannelThrows` cannot answer that: it
 * makes stderr throw too, so no row driven under it ever observes what
 * reached stderr. Restored on scope exit.
 */
export interface ConsoleRefusesChannels extends Disposable {
    /** Each line `Deno.stderr.writeSync` actually received, decoded UTF-8. */
    stderrLines(): readonly string[]
}

/**
 * Make `console.warn` and `console.error` throw, and record every write
 * `Deno.stderr.writeSync` receives instead — so a witness can tell that the
 * marked fallback line reached stderr, and read what it carried.
 *
 * @returns The recorder; dispose it (`using`) to restore the channels.
 */
export function consoleRefuses(): ConsoleRefusesChannels {
    const realWarn = console.warn
    const realError = console.error
    const realWrite = Deno.stderr.writeSync
    const decoder = new TextDecoder()
    const lines: string[] = []
    console.warn = () => {
        throw new Error('warn sink down')
    }
    console.error = () => {
        throw new Error('error sink down')
    }
    Deno.stderr.writeSync = (chunk: Uint8Array) => {
        lines.push(decoder.decode(chunk))
        return chunk.length
    }
    return {
        stderrLines: () => lines,
        [Symbol.dispose]: () => {
            console.warn = realWarn
            console.error = realError
            Deno.stderr.writeSync = realWrite
        },
    }
}

/**
 * Make `console.warn`, `console.error` and `Deno.stderr.writeSync` all throw,
 * recording each `console.error` attempt before it throws — so a witness can
 * tell the fallback it reached from one it did not. Restored on scope exit.
 *
 * @returns The recorder; dispose it (`using`) to restore the channels.
 */
export function everyChannelThrows(): ThrowingChannels {
    const realWarn = console.warn
    const realError = console.error
    const realWrite = Deno.stderr.writeSync
    const lines: string[] = []
    console.warn = () => {
        throw new Error('warn sink down')
    }
    console.error = (...parts: unknown[]) => {
        lines.push(parts.map(String).join(' '))
        throw new Error('error sink down')
    }
    Deno.stderr.writeSync = () => {
        throw new Error('stderr down')
    }
    return {
        errorCalls: () => lines.length,
        errorLines: () => lines,
        [Symbol.dispose]: () => {
            console.warn = realWarn
            console.error = realError
            Deno.stderr.writeSync = realWrite
        },
    }
}
