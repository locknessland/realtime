/**
 * @fileoverview #352's mutation battery — a throw from `onMessage` reaches the
 * operator exactly once, the client never, and leaves the socket open.
 *
 * Each row puts back one way `guard()` / `reportError()` in `buildEvents` can
 * get the #347 refusal wrong at the transport: the error swallowed (M1), a
 * framework frame sent to the client (M2 — a maintainer decision on #347 says
 * none is), the socket closed although the client did nothing wrong (M3), the
 * error reported twice (M4), and the no-hook default sink demoted below
 * `console.error` (M5a) or its rendering downgraded (M5b). Every row must die
 * on a `#352` test.
 *
 * **M5 split in two (#399).** The original M5 changed the sink to
 * `console.debug` AND swapped `renderError(error)` for a raw `${error}`
 * template — two changes bundled under one label. M5a now changes only the
 * sink; M5b changes only the rendering, keeping `console.error`.
 *
 * The witness each row dies on — the assertion that fails, not only the test:
 * M1 `onError is called exactly once` (0 calls), M2 `nothing is sent to the
 * client`, M3 `the socket is not closed`, M4 `onError is called exactly once`
 * (2 calls), M5a `exactly one console.error line` (0 lines), M5b `the line
 * names the error` (the class name is gone, only its message survives raw
 * `.message` access). Each is named again on its row below.
 *
 * Every row was proven LIVE before it was trusted: a marker was placed at the
 * row's anchor and seen to execute under the killing witness — the `catch`
 * anchor (M1–M4) under `private-orders: an AuthorizeResultError from
 * onMessage`, the default-sink anchor (M5a, M5b) under `private-orders: with
 * no onError hook`. A row whose line never runs reports a kill it did not
 * cause.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/websocket_error_routing_352.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/websocket_error_routing_352
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const WEBSOCKET = new URL('../../websocket.ts', import.meta.url)
const SUITES = [
    new URL('../authorize_result_websocket_352.test.ts', import.meta.url)
        .pathname,
]

const CATCH = '        } catch (error) {\n' +
    '            await reportError(conn, error)\n' +
    '        }\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the error swallowed: no onError, no log line',
        file: WEBSOCKET,
        edits: [[
            CATCH,
            '        } catch (error) {\n' +
            '            void error\n' +
            '        }\n',
        ]],
        // Witness: `onError is called exactly once` — it is called 0 times.
        killedBy: 'private-orders: an AuthorizeResultError from onMessage',
    },
    {
        label: 'M2 — a framework error frame sent to the client',
        file: WEBSOCKET,
        edits: [[
            CATCH,
            '        } catch (error) {\n' +
            `            conn.send('{"type":"error","message":"internal error"}')\n` +
            '            await reportError(conn, error)\n' +
            '        }\n',
        ]],
        // Witness: `nothing is sent to the client` — one frame was.
        killedBy: 'private-orders: an AuthorizeResultError from onMessage',
    },
    {
        label: 'M3 — the socket closed on an application defect',
        file: WEBSOCKET,
        edits: [[
            CATCH,
            '        } catch (error) {\n' +
            "            conn.close(1011, 'internal error')\n" +
            '            await reportError(conn, error)\n' +
            '        }\n',
        ]],
        // Witness: `the socket is not closed` — one close, code 1011.
        killedBy: 'private-orders: an AuthorizeResultError from onMessage',
    },
    {
        label: 'M4 — the error reported twice',
        file: WEBSOCKET,
        edits: [[
            CATCH,
            '        } catch (error) {\n' +
            '            await reportError(conn, error)\n' +
            '            await reportError(conn, error)\n' +
            '        }\n',
        ]],
        // Witness: `onError is called exactly once` — it is called twice.
        killedBy: 'private-orders: an AuthorizeResultError from onMessage',
    },
    {
        label: 'M5a — the no-hook default sink demoted below console.error',
        // Re-anchored by #391: the default line is written through
        // `writeMarkedFallback`; the demotion puts a `console.debug` back.
        // #399: keeps `renderError` so this changes ONLY the console method,
        // not the encoding — M5b below is the encoding half, split out.
        file: WEBSOCKET,
        edits: [
            [
                'import { markedFallbackMarker, writeMarkedFallback } ' +
                "from './marked_fallback.ts'\n",
                "import { renderError } from '@lockness/contract'\n" +
                'import { markedFallbackMarker, writeMarkedFallback } ' +
                "from './marked_fallback.ts'\n",
            ],
            [
                '        writeMarkedFallback(UNHANDLED_WEBSOCKET_ERROR, error)\n',
                '        console.debug(' +
                '`${UNHANDLED_WEBSOCKET_ERROR} ${renderError(error)}`)\n',
            ],
        ],
        // Witness: `exactly one console.error line` — none reaches it.
        killedBy: 'private-orders: with no onError hook',
    },
    {
        label:
            "M5b — the no-hook default line's rendering downgraded, dropping the error's name",
        // #399: keeps `console.error` (through the real writeMarkedFallback
        // path this time — only the SUBJECT rendering is swapped for the raw
        // `.message`, which drops AuthorizeResultError's `name`), so this
        // changes ONLY the encoding, not the sink.
        file: WEBSOCKET,
        edits: [[
            '        writeMarkedFallback(UNHANDLED_WEBSOCKET_ERROR, error)\n',
            '        writeMarkedFallback(\n' +
            '            UNHANDLED_WEBSOCKET_ERROR,\n' +
            '            error instanceof Error ? error.message : error,\n' +
            '        )\n',
        ]],
        // Witness: `the line names the error` — the class name is gone.
        killedBy: 'private-orders: with no onError hook',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#352 — websocket routing of an onMessage throw',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
