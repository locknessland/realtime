/**
 * @fileoverview #291 — every error this package logs is rendered, never raw.
 *
 * #277 converted `drivers/redis.ts` and settled the convention: interpolate
 * `renderError(error)`, never hand the error object to `console.*`. This file
 * covers the four sites that had not been converted — three in `manager.ts`,
 * one in `websocket.ts` — plus the one site in `events_bridge.ts` that must
 * deliberately stay raw, and the helper-reached warning in the same file that
 * a `console.` grep does not find.
 *
 * **Why the capture joins EVERY argument and records the sink.** Every other
 * console-capturing test in this package records `String(args[0])`, and that is
 * exactly why two of the four disclosure sites were invisible to a green
 * suite: `console.warn('message', error)` puts the error in `args[1]`, so the
 * test sees a clean string while the sink receives the message *and the stack*.
 * A test that cannot observe the defect is not evidence of its absence.
 *
 * @module @lockness/realtime/tests/log_encoding_291
 */

import { assert, assertEquals, assertStringIncludes } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import { buildEvents } from '../websocket.ts'
import { forwardEvent } from '../events_bridge.ts'
import type { BroadcastDriver } from '../driver.ts'
import type { Connection, WSContext } from '../types.ts'

/** A DSN-bearing failure — the shape a driver teardown actually produces. */
const DSN_FAILURE = () =>
    new Error('teardown failed: postgres://svc:S3cr3t@db.internal:5432/app')

interface Captured {
    /** Which sink the line went to. Severity is behaviour, so it is recorded. */
    sink: 'warn' | 'error'
    text: string
}

/**
 * Capture `console.warn` and `console.error`, joining **all** arguments and
 * keeping them apart by sink.
 *
 * `Symbol.dispose` rather than a `try/finally` in each test: a helper that
 * restores only on the happy path leaves the real console patched for every
 * later test in the process when an assertion throws.
 */
function captureConsole(): { lines: Captured[] } & Disposable {
    const lines: Captured[] = []
    const realWarn = console.warn
    const realError = console.error
    const record = (sink: 'warn' | 'error') => (...args: unknown[]) =>
        void lines.push({
            sink,
            // `Deno.inspect`, not `String`. This file's premise is that
            // `String(args[0])` blinded the suite to a leak in `args[1]`, and
            // `String(a)` on a non-Error carrier has the same shape of hole:
            // `console.warn('msg', { dsn })` records `[object Object]`, so a
            // leak inside a plain object, an AggregateError's `errors`, or a
            // cause chain stays invisible. Reproducing the blind spot one
            // argument over would be the same mistake in a new place.
            text: args.map((a) =>
                typeof a === 'string' ? a : Deno.inspect(a, { depth: 4 })
            ).join(' '),
        })
    console.warn = record('warn')
    console.error = record('error')
    return {
        lines,
        [Symbol.dispose]() {
            console.warn = realWarn
            console.error = realError
        },
    }
}

/**
 * Assert one captured line carries a RENDERED error, not a raw one.
 *
 * The three checks are chosen so that none of them is satisfied by the raw
 * forms this file rejects:
 *
 * - `postgres://***:***@` is the positive oracle. `Error:` is **not** one — it
 *   is `Error.prototype.toString()`'s own prefix and the first token of a
 *   stack, so it survives every revert these tests aim at. Without a positive
 *   check, a `renderError` reduced to `error.name` keeps an absence-only test
 *   green while destroying the diagnostic value the line exists for.
 * - the password must be gone.
 * - the line must be SINGLE-LINE. That is the stack oracle, and unlike
 *   `includes('at file:')` it cannot go inert: `renderError` escapes newlines,
 *   so more than one line means raw content got through. `at file:` is present
 *   only when some frame happens to be anonymous, which is a property of the
 *   call chain rather than of the code under test.
 */
function assertRendered(line: Captured | undefined, why: string): void {
    assert(line, `${why} — the line never fired, so the test proves nothing`)
    assertStringIncludes(line.text, 'postgres://***:***@', why)
    assertEquals(
        line.text.includes('S3cr3t'),
        false,
        `${why}: the password reached the sink`,
    )
    assertEquals(
        line.text.split('\n').length,
        1,
        `${why}: the line is multi-line, so a stack reached the sink`,
    )
}

interface User {
    id: number
}

function fakeConn(id: string): Connection<User> {
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: () => {},
    }
}

const fakeSocket = () => ({ send() {}, close() {} })
const tick = () => new Promise((r) => setTimeout(r, 0))

Deno.test('#291 evict-teardown WARN renders the error and encodes the client id', async () => {
    // A roster whose removeMember rejects makes disconnect() throw, which is
    // the only way into revokeLocal's catch.
    const driver: BroadcastDriver = {
        publish: () => Promise.resolve(),
        onMessage: () => {},
        addMember: () => Promise.resolve(),
        listMembers: () => Promise.resolve([]),
        removeMember: () => Promise.reject(DSN_FAILURE()),
    }
    // #304 made a hostile id UNCONSTRUCTIBLE: `register` refuses anything
    // outside `isValidName`, so a bidi override can no longer BE a connection
    // id. The `safeForLog(clientId)` at the WARN below therefore has no
    // reachable hostile input any more. It stays as the second of two
    // independent controls — the same argument the driver's own WARNs make for
    // their encoders — and the assertion it used to carry now lives in
    // `connection_id_charset.test.ts` as a throw rather than an escape.
    const hostile = 'client-gnp.txt'
    // `authorize` defaults to DENY, so without one the subscribe below is
    // refused, no presence member is recorded, unsubscribe short-circuits and
    // the WARN never fires — a green test asserting nothing.
    const m = new ChannelManager<User>({ driver, authorize: () => true })
    const conn = fakeConn(hostile)
    m.handlerHooks({}).onOpen?.(conn)
    const sub = await m.subscribe(conn, 'presence-room')
    assertEquals(sub.ok, true, 'the fixture must actually subscribe')

    using captured = captureConsole()
    await m.evict(hostile)

    const warn = captured.lines.find((l) => l.text.includes('evict teardown'))
    assertRendered(warn, 'evict teardown')
    assertEquals(
        warn?.sink,
        'warn',
        'a teardown failure is a WARN, not an ERROR',
    )
    // The id still reaches the line. This does NOT keep the encoder honest —
    // `safeForLog(x) === x` for every id that can now get here, so no assertion
    // at this site can tell the encoder from its absence. The claim that it
    // could was wrong, and the battery proved it by going red. What this
    // asserts is only that the id is still interpolated at all.
    assertStringIncludes(warn?.text ?? '', 'client-gnp.txt')
})

Deno.test('#291 durable-revocation WARN renders the error and stays a WARN', async () => {
    const driver: BroadcastDriver = {
        publish: () => Promise.resolve(),
        onMessage: () => {},
        markRevoked: () => Promise.reject(DSN_FAILURE()),
    }
    const m = new ChannelManager<User>({ driver })

    using captured = captureConsole()
    await m.evict('c1').catch(() => {}) // the durability error is re-thrown by design

    const warn = captured.lines.find((l) =>
        l.text.includes('durable revocation')
    )
    assertRendered(warn, 'durable revocation')
    // The severity is deliberate — the evict still proceeds, so this is a WARN
    // and not an ERROR. Nothing pinned it before, because the capture aliased
    // both sinks into one array and no test could tell them apart.
    assertEquals(
        warn?.sink,
        'warn',
        'the durable-write failure must stay a WARN',
    )
})

Deno.test('#291 the default onPublishError renders the error', async () => {
    const driver: BroadcastDriver = {
        publish: () => Promise.reject(DSN_FAILURE()),
        onMessage: () => {},
    }
    // No onPublishError supplied — the framework's own default is under test.
    const m = new ChannelManager<User>({ driver })

    using captured = captureConsole()
    m.broadcast('news', 'e', {})
    await tick()

    const line = captured.lines.find((l) =>
        l.text.includes('broadcast publish failed')
    )
    assertRendered(line, 'default publish sink')
    assertEquals(line?.sink, 'error', 'a lost broadcast is an ERROR')
})

Deno.test('#291 the default websocket error sink renders the error', async () => {
    // The fourth site, and the hottest: guard() routes every throw from the
    // application's onOpen/onMessage here whenever no hooks.onError is given.
    const events = buildEvents<User>(
        {
            onMessage: () =>
                void (() => {
                    throw DSN_FAILURE()
                })(),
        },
        { id: 1 },
    )

    using captured = captureConsole()
    events.onMessage?.(
        { data: '{}' } as MessageEvent,
        fakeSocket() as unknown as WSContext,
    )
    await tick()

    const line = captured.lines.find((l) =>
        l.text.includes('unhandled websocket error')
    )
    assertRendered(line, 'default websocket sink')
    assertEquals(
        line?.sink,
        'error',
        'an unhandled websocket error is an ERROR',
    )
})

Deno.test('#291 a client-controlled frame field cannot forge or reorder a log line', async () => {
    // The source half of the same site: decodeClientMessage interpolates the
    // frame's own `type` into `unknown frame type: ...`. Encoding happens at
    // the SINK rather than at that throw, because the thrown message also goes
    // to the app's onError hook and escaping it there would corrupt what the
    // application sees to fix a problem that exists only at the log.
    const payload = '\nGET /admin 200 forged‮gnp.txt'
    const events = buildEvents<User>({
        onMessage: () => {
            throw new Error(`unknown frame type: ${payload}`)
        },
    }, { id: 1 })

    using captured = captureConsole()
    events.onMessage?.(
        { data: '{}' } as MessageEvent,
        fakeSocket() as unknown as WSContext,
    )
    await tick()

    const line = captured.lines.find((l) =>
        l.text.includes('unhandled websocket error')
    )
    assert(line, 'the websocket sink did not fire')
    // Positive control: the payload reached the line at all, encoded.
    assertStringIncludes(line.text, 'GET /admin 200 forged')
    assertEquals(
        line.text.includes('\n'),
        false,
        'a forged log line got through',
    )
    assertEquals(line.text.includes('‮'), false, 'a bidi override got through')
})

Deno.test('#291 a transport error keeps its detail, which renderError would drop', async () => {
    // renderError renders `name: message` and drops `cause` entirely, so an
    // error whose whole content is its cause renders to nothing useful. The
    // onError path deliberately attaches the transport event as a cause; the
    // detail is mirrored into the message so the encoder has something to carry.
    const events = buildEvents<User>({}, { id: 1 })

    using captured = captureConsole()
    events.onError?.(
        new ErrorEvent('error', { message: 'ECONNRESET' }),
        fakeSocket() as unknown as WSContext,
    )
    await tick()

    const line = captured.lines.find((l) =>
        l.text.includes('unhandled websocket error')
    )
    assert(line, 'the websocket sink did not fire')
    assertStringIncludes(
        line.text,
        'ECONNRESET',
        'the transport detail was lost — the line carries no information',
    )
})

Deno.test('#291 the public-channel warning encodes the event and channel names', () => {
    // Reached through the `warn` helper rather than a direct console.*, which
    // is why a grep of events_bridge.ts for `console.` does not find it.
    const lines: string[] = []
    const manager = { broadcast: () => {} }
    // `data` is the event instance; the bridge reads broadcastOn/broadcastAs
    // off it and falls back to `payload.event` for the name.
    forwardEvent(
        manager as never,
        {
            event: 'unused',
            data: {
                broadcastAs: () => 'order\u202Egnp.txt',
                broadcastOn: () => ['public-\nGET /admin 200 forged'],
            },
        },
        (m: string) => void lines.push(m),
    )

    const warned = lines.find((l) => l.includes('public channel'))
    assert(warned, 'the public-channel warning did not fire')
    assertStringIncludes(warned, 'order', 'the event name was dropped entirely')
    assertStringIncludes(
        warned,
        'GET /admin 200 forged',
        'the channel was dropped',
    )
    assertEquals(warned.includes('\n'), false, 'a forged log line got through')
    assertEquals(
        warned.includes('\u202E'),
        false,
        'a bidi override got through',
    )
})

Deno.test('#291 events_bridge stays control flow, not a log line', async () => {
    // THE ONE SITE THAT MUST NOT BE CONVERTED, and the only test here that
    // reads source rather than behaviour. That is deliberate and stated rather
    // than disguised: `loadDispatcher` is module-private and hardcodes its
    // specifier, so with `@lockness/events` present in this workspace no
    // behavioural test can reach the branch at all. The choice is a source
    // assertion or no guard, and no guard is how the next audit converts it.
    //
    // What conversion would break: the string is substring-matched to decide
    // whether the optional package is absent (return null, the soft edge stays
    // soft) or something else went wrong (rethrow). `renderError` prefixes the
    // error name and truncates at 200 code points, so a resolver message long
    // enough to push `Module not found` past the boundary would stop matching
    // — and an absent optional dependency would start THROWING instead of
    // degrading.
    const source = await Deno.readTextFile(
        new URL('../events_bridge.ts', import.meta.url),
    )
    const start = source.indexOf('async function loadDispatcher')
    assert(
        start !== -1,
        'loadDispatcher was renamed or reshaped — this guard now reads the ' +
            'wrong region and must be re-anchored, not deleted.',
    )
    const end = source.indexOf('\n}', start)
    assert(end !== -1, 'could not find the end of loadDispatcher')
    const region = source.slice(start, end)

    // Comments stripped — BLOCK comments as well as `//` lines. The comment
    // this test protects necessarily names both encoders in order to say not
    // to use them, so a check over raw source fails on its own warning
    // (measured). Stripping only `//` was the first attempt, and one docs pass
    // reflowing that comment into a `/* */` block would have re-broken it.
    const guard = region
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .split('\n')
        .map((line) => line.replace(/\/\/.*$/, ''))
        .join('\n')

    // `error.message`, not `message.includes(` — the property this test owns is
    // "no encoder is applied here", and pinning the match's spelling would fail
    // a behaviour-preserving extraction to `isMissingModule(message)` for no
    // reason.
    assertStringIncludes(guard, 'error.message')
    for (const encoder of ['renderError', 'safeForLog']) {
        assertEquals(
            guard.includes(encoder),
            false,
            `loadDispatcher must not use ${encoder}: it decides control flow ` +
                'on this string, and encoding it changes what it matches.',
        )
    }
    // The AC asks for a comment naming this as control flow, so the next
    // audit's grep stops here. A marker, not a sentence: the wording is free to
    // improve, the label is what carries.
    assertStringIncludes(region, 'CONTROL FLOW, NOT A LOG LINE')
})

Deno.test('#291 captureConsole restores both sinks on dispose', () => {
    // Nothing else in this file can observe a failed restore: each test
    // installs its own recorder and asserts on its own array. But Deno runs
    // test files in one process, so an unrestored recorder swallows console
    // output for every file that runs after this one — including this
    // package's other console-capturing tests. The helper's doc comment gives
    // that as the reason it uses Symbol.dispose; this is what checks it.
    const realWarn = console.warn
    const realError = console.error
    {
        using _captured = captureConsole()
        assertEquals(
            console.warn === realWarn,
            false,
            'the capture did not install',
        )
    }
    assertEquals(console.warn, realWarn, 'console.warn was left patched')
    assertEquals(console.error, realError, 'console.error was left patched')
})
