/**
 * @fileoverview #291's mutation battery.
 *
 * Same contract as `prefix_288.ts` and `contract/tests/mutations/bidi_292.ts`:
 * each anchor must match exactly once, the file is re-read to prove it changed,
 * a mutant that does not type-check is DEAD rather than a survivor, an uncaught
 * module error is a kill checked BEFORE the summary, and every mutated file is
 * restored on SIGINT/SIGTERM with any failure named rather than swallowed.
 *
 * The rows that matter are the three that revert an object-passing site. Those
 * were invisible to this package's whole suite before #291, because every
 * console-capturing test in it records `String(args[0])` and the error lives in
 * `args[1]`.
 *
 * Two rows mutate the TEST file rather than the source. That is deliberate: a
 * guard whose own failure nothing observes is not a guard, and the dispose
 * restore is exactly that shape — every test installs its own recorder, so a
 * failed restore is invisible inside this file and corrupts every file that
 * runs after it.
 *
 * The compile guard reaches every row because the suite imports `manager.ts`,
 * `websocket.ts` and `events_bridge.ts`. It did not when this battery was
 * written: the bridge rows were never type-checked, so a bridge mutant with a
 * syntax error would have been reported SURVIVED rather than DEAD.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/log_encoding_291.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/log_encoding_291
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const BRIDGE = new URL('../../events_bridge.ts', import.meta.url)
const SOCKET = new URL('../../websocket.ts', import.meta.url)
const SELF = new URL('../log_encoding_291.test.ts', import.meta.url)
const SUITES = [
    new URL('../log_encoding_291.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'evict-teardown back to a raw error.message',
        file: MANAGER,
        edits: [[
            '`after hard-close: ${renderError(error)}`',
            '`after hard-close: ${String(error)}`',
        ]],
        killedBy:
            'evict-teardown WARN renders the error and encodes the client id',
    },
    {
        label: 'evict-teardown stops encoding the client id',
        file: MANAGER,
        // DISAMBIGUATED (#295). `${safeForLog(clientId)}` now appears twice in
        // the manager — the watched-channel cap WARN uses it too — and a bare
        // substring anchor matched both, which the harness refuses rather than
        // guessing. Carrying the surrounding words is what keeps this row
        // pointed at the evict-teardown line specifically.
        edits: [[
            'evict teardown for ${safeForLog(clientId)} failed',
            'evict teardown for ${clientId} failed',
        ]],
        killedBy:
            'evict-teardown WARN renders the error and encodes the client id',
        expectSurvival:
            'RETIRED BY #304, deliberately and not by neglect. That issue moved ' +
            'the connection-id charset to the boundary: `register`, `subscribe` ' +
            'and `evict` now refuse anything outside `isValidName`, so no id ' +
            'reaching this WARN can carry a control character and ' +
            '`safeForLog(x) === x` for every id that can. No assertion can ' +
            'distinguish the encoder from its absence here any more. The ' +
            'encoder STAYS as the second of two independent controls — the ' +
            'same argument the driver writes down for its own WARNs — and this ' +
            'row stays visible rather than being deleted, so the next reader ' +
            'learns why it cannot be killed instead of wondering why it is gone.',
    },
    {
        label: 'durable-revocation WARN back to passing the error object',
        file: MANAGER,
        edits: [[
            '                    `by reconcile: ${renderError(error)}`,\n            )',
            "                    'by reconcile',\n                error,\n            )",
        ]],
        killedBy: 'durable-revocation WARN renders the error and stays a WARN',
    },
    {
        label: 'the default publish sink back to passing the error object',
        file: MANAGER,
        edits: [[
            '`realtime: broadcast publish failed: ${renderError(error)}`,',
            "'realtime: broadcast publish failed',\n                    error,",
        ]],
        killedBy: 'the default onPublishError renders the error',
    },
    {
        label:
            'events_bridge converted — the edit this whole guard exists to stop',
        file: BRIDGE,
        edits: [[
            'const message = error instanceof Error ? error.message : String(error)',
            'const message = error instanceof Error\n            ? safeForLog(error.message)\n            : String(error)',
        ]],
        killedBy: 'events_bridge stays control flow, not a log line',
        // `safeForLog`, not `renderError`: the first version of this row
        // injected a call to a name `events_bridge.ts` does not import, so it
        // did not type-check and no test ran. It read as KILLED only because
        // the compile guard could not see the bridge at all back then — the
        // suite did not import it. That is the difference between a mutation
        // and a typo, and this battery is supposed to be able to tell.
    },
    {
        label: 'the websocket default sink back to passing the error object',
        file: SOCKET,
        edits: [[
            '`realtime: unhandled websocket error: ${renderError(error)}`,',
            "'realtime: unhandled websocket error',\n                error,",
        ]],
        killedBy: 'the default websocket error sink renders the error',
    },
    {
        label: 'the transport detail dropped — the line renders to nothing',
        file: SOCKET,
        edits: [[
            '`websocket transport error: ${detail}`',
            "'websocket transport error'",
        ]],
        killedBy: 'a transport error keeps its detail',
    },
    {
        label: 'the public-channel warning stops encoding the event name',
        file: BRIDGE,
        edits: [['${safeForLog(name)}', '${name}']],
        killedBy:
            'the public-channel warning encodes the event and channel names',
    },
    {
        label: 'the public-channel warning stops encoding the channel',
        file: BRIDGE,
        edits: [['${safeForLog(channel)}', '${channel}']],
        killedBy:
            'the public-channel warning encodes the event and channel names',
    },
    {
        label: 'the durable-revocation WARN raised to console.error',
        file: MANAGER,
        edits: [[
            "console.warn(\n                'realtime: the durable revocation write failed",
            "console.error(\n                'realtime: the durable revocation write failed",
        ]],
        killedBy: 'durable-revocation WARN renders the error and stays a WARN',
    },
    {
        label: "captureConsole's dispose emptied — a patched console leaks out",
        file: SELF,
        edits: [[
            '        [Symbol.dispose]() {\n            console.warn = realWarn\n            console.error = realError\n        },',
            '        [Symbol.dispose]() {},',
        ]],
        killedBy: 'captureConsole restores both sinks on dispose',
    },
    {
        label: "events_bridge's CONTROL FLOW label removed",
        file: BRIDGE,
        edits: [['// CONTROL FLOW, NOT A LOG LINE.', '// A note.']],
        killedBy: 'events_bridge stays control flow, not a log line',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery('#291 — log encoding', SUITES, MUTATIONS) > 0 ? 1 : 0,
    )
}
