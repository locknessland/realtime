/**
 * @fileoverview #391 — `writeMarkedFallback`, the one home of the marked
 * fallback line, never throws and never changes a byte of it.
 *
 * Three channels, tried in order: `console.error` (so an application that
 * patched its console is respected), then `Deno.stderr.writeSync` with the
 * same line and a newline, then nothing — the one final catch, justified
 * because no channel is left and a re-throw would terminate the process.
 *
 * Each row is run twice: with a second half (`; sink failure: …`) and
 * without, because the package's sinks use both shapes.
 *
 * @module @lockness/realtime/tests/marked_fallback_391
 */

import { assertEquals } from '@std/assert'
import { renderError } from '@lockness/contract'
import {
    markedFallbackMarker,
    writeMarkedFallback,
} from '../marked_fallback.ts'

const MARKER = markedFallbackMarker(
    'realtime: a test line could not be written (#391):',
)
/** What `channels()` throws from `console.error`, rendered (#399). */
const CONSOLE_FAILURE_RENDERED = renderError(
    new Error('console refused (#391)'),
)
/** A subject whose text would break the line if it were not rendered. */
const SUBJECT = new Error('refused\r\nrealtime: forged line')
const FAILURE = {
    label: 'sink failure' as const,
    error: new Error('warn sink down ‮'),
}

/** The two line shapes, and the bytes each must produce. */
const SHAPES = [
    {
        name: 'marker and subject',
        failure: undefined,
        line: `${MARKER} ${renderError(SUBJECT)}`,
    },
    {
        name: 'marker, subject and a second half',
        failure: FAILURE,
        line: `${MARKER} ${renderError(SUBJECT)}; sink failure: ${
            renderError(FAILURE.error)
        }`,
    },
]

/**
 * Stub `console.error` and `Deno.stderr.writeSync`, each recording what it was
 * given and throwing when told to. Restored on scope exit.
 */
function channels(fail: { console: boolean; stderr: boolean }) {
    const realError = console.error
    const realWrite = Deno.stderr.writeSync
    const errors: unknown[][] = []
    const writes: string[] = []
    console.error = (...args: unknown[]) => {
        errors.push(args)
        if (fail.console) throw new Error('console refused (#391)')
    }
    Deno.stderr.writeSync = (bytes: Uint8Array) => {
        writes.push(new TextDecoder().decode(bytes))
        if (fail.stderr) throw new Error('stderr refused (#391)')
        return bytes.length
    }
    return {
        errors,
        writes,
        [Symbol.dispose]: () => {
            console.error = realError
            Deno.stderr.writeSync = realWrite
        },
    }
}

for (const shape of SHAPES) {
    Deno.test(`#391 H1 (${shape.name}) a working console gets the one line, and stderr nothing`, () => {
        using sink = channels({ console: false, stderr: false })
        writeMarkedFallback(MARKER, SUBJECT, shape.failure)
        assertEquals(sink.errors, [[shape.line]])
        assertEquals(sink.writes, [])
    })

    Deno.test(`#391 H2 (${shape.name}) a throwing console: one stderr line naming what it threw, the same original bytes`, () => {
        using sink = channels({ console: true, stderr: false })
        writeMarkedFallback(MARKER, SUBJECT, shape.failure)
        assertEquals(sink.errors, [[shape.line]], 'the console was tried first')
        assertEquals(sink.writes, [
            `${shape.line}; console failure: ${CONSOLE_FAILURE_RENDERED}\n`,
        ])
    })

    Deno.test(`#391 H3 (${shape.name}) console and stderr both throwing: it still returns`, () => {
        using sink = channels({ console: true, stderr: true })
        assertEquals(
            writeMarkedFallback(MARKER, SUBJECT, shape.failure),
            undefined,
        )
        assertEquals(sink.errors.length, 1, 'the console was tried')
        assertEquals(sink.writes, [
            `${shape.line}; console failure: ${CONSOLE_FAILURE_RENDERED}\n`,
        ], 'then stderr, naming what the console threw')
    })
}
