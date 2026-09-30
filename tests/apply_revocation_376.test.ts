/**
 * @fileoverview #376 — a control-frame revocation whose own WARN cannot be
 * written never reaches the runtime as an unhandled rejection.
 *
 * `ChannelManager` applies an `evict` and a `revoke-channel` control frame
 * fire-and-forget: the switch that receives them has no caller to hand a
 * rejection to. `#applyRevocation` contains every failure of the apply itself,
 * but not the failure of its own catch — the `console.warn` it writes there. A
 * sink that throws (a patched console, a logger transport that refuses a line)
 * therefore turned one failed apply into a rejection with nowhere to go, and on
 * Deno an unhandled rejection terminates the process. Any peer instance that
 * publishes one of those frames could reach it: the #369 class.
 *
 * **The seam is driven directly.** The fake driver hands the test the manager's
 * `onControl` handler, so each frame reaches the switch exactly as an
 * authenticated frame off the bus would.
 *
 * **The failure is a real one.** `evict` fails its teardown on a broker whose
 * `unwatchChannel` rejects (the #369 blip); `revoke-channel` fails to clear the
 * record it applied. Each failure is written at WARN, which is where the
 * throwing sink bites — so the rejection starts inside the apply, crosses the
 * apply's own catch, and reaches the call site.
 *
 * Every row watches for an escape with an `unhandledrejection` listener that
 * records the reason and `preventDefault()`s it — so a regression FAILS the row
 * rather than killing the runner — and is removed in a `finally`.
 *
 * @module @lockness/realtime/tests/apply_revocation_376
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager, REVOCATION_APPLY_LOG_FAILED } from '../manager.ts'
import type { BroadcastDriver, ControlMessage } from '../driver.ts'
import { MemoryBroadcastDriver } from '../drivers/memory.ts'
import type { Connection } from '../types.ts'
import { settle, watchingEscapes } from './escape_watcher.ts'

interface User {
    id: number
}

const ROOM = 'private-room'

/**
 * Replace `console.warn` with a sink that THROWS (counting every attempt, so a
 * row can prove the sink was actually reached) and capture `console.error`.
 * Both are restored on scope exit.
 */
function throwingWarnSink(failure = 'warn sink refused the line') {
    const realWarn = console.warn
    const realError = console.error
    const attempts: string[] = []
    const errors: string[] = []
    console.warn = (...args: unknown[]) => {
        attempts.push(args.map(String).join(' '))
        throw new Error(failure)
    }
    console.error = (...args: unknown[]) =>
        void errors.push(args.map(String).join(' '))
    return {
        attempts,
        errors,
        [Symbol.dispose]: () => {
            console.warn = realWarn
            console.error = realError
        },
    }
}

/** A connection recording the close codes it was sent. */
function conn(id: string): Connection<User> & { readonly closes: number[] } {
    const closes: number[] = []
    return {
        id,
        identity: { id: 1 },
        metadata: {},
        send: () => {},
        close: (code?: number) => void closes.push(code ?? 1000),
        closes,
    } as Connection<User> & { readonly closes: number[] }
}

/**
 * A manager owning one subscribed socket, over a driver whose control seam the
 * test holds. Delivery goes through a real in-process driver; the durable
 * store's `clearRevocation` REJECTS, and so — unless `unwatchFails` is false —
 * does the channel watcher's `unwatchChannel`, so each revocation scope has a
 * failure to log.
 */
async function ownerOf(target: string, { unwatchFails = true } = {}) {
    const memory = new MemoryBroadcastDriver()
    let deliver: ((control: ControlMessage) => void) | undefined
    const driver: BroadcastDriver = {
        publish: (message) => memory.publish(message),
        onMessage: (handler) => memory.onMessage(handler),
        watchChannel: () => Promise.resolve(),
        unwatchChannel: () =>
            unwatchFails
                ? Promise.reject(new Error('broker unwatch failed'))
                : Promise.resolve(),
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: () => Promise.reject(new Error('broker clear failed')),
    }
    const manager = new ChannelManager<User>({
        driver,
        authorize: () => true,
    })
    const victim = conn(target)
    manager.register(victim)
    assertEquals((await manager.subscribe(victim, ROOM)).ok, true)
    // Narrowed, not optional-chained: an unregistered seam would make every
    // assertion below pass for the wrong reason.
    assert(deliver !== undefined, 'the control seam was registered')
    return {
        manager,
        victim,
        send: deliver as (control: ControlMessage) => void,
    }
}

/** The marked lines among the captured `console.error` output. */
const marked = (errors: string[]) =>
    errors.filter((line) => line.startsWith(REVOCATION_APPLY_LOG_FAILED))

Deno.test('#376 W1 evict: a throwing WARN sink on a failed teardown never escapes', async () => {
    await watchingEscapes(async (escaped) => {
        const { victim, send } = await ownerOf('c1')
        using sink = throwingWarnSink()

        send({ kind: 'evict', target: 'c1' })
        await settle()

        assertEquals(victim.closes, [4403], 'the socket was hard-closed')
        assert(
            sink.attempts.some((line) => line.includes('failed')),
            `the throwing WARN sink was reached: ${sink.attempts}`,
        )
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const lines = marked(sink.errors)
        assertEquals(
            lines.length,
            1,
            `exactly one marked ERROR line: ${sink.errors}`,
        )
        assert(
            lines[0].includes('warn sink refused the line'),
            `the line carries the sink's failure: ${lines[0]}`,
        )
    })
})

Deno.test('#376 W2 revoke-channel: a throwing WARN sink on a failed clear never escapes', async () => {
    await watchingEscapes(async (escaped) => {
        // The leave succeeds, so the failure logged is the CLEAR's — a second
        // path to the same sink, distinct from W1's teardown.
        const { manager, victim, send } = await ownerOf('c1', {
            unwatchFails: false,
        })
        using sink = throwingWarnSink()

        send({
            kind: 'revoke-channel',
            target: 'c1',
            channel: ROOM,
            revocationId: 'r-1',
        })
        await settle()

        assertEquals(victim.closes, [], 'a channel revocation keeps the socket')
        assert(
            sink.attempts.some((line) => line.includes('could not be cleared')),
            `the throwing WARN sink was reached: ${sink.attempts}`,
        )
        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assertEquals(
            marked(sink.errors).length,
            1,
            `exactly one marked ERROR line: ${sink.errors}`,
        )
        assertEquals(
            await manager.unsubscribe('c1', ROOM),
            'not-subscribed',
            'the revocation itself was applied',
        )
    })
})

Deno.test('#376 W3 the sink failure is rendered, not interpolated raw', async () => {
    await watchingEscapes(async (escaped) => {
        const { send } = await ownerOf('c1')
        using sink = throwingWarnSink('refused\r\nrealtime: forged line')

        send({ kind: 'evict', target: 'c1' })
        await settle()

        assertEquals(escaped, [], 'no rejection reaches the runtime')
        const lines = marked(sink.errors)
        assertEquals(lines.length, 1, `one marked line: ${sink.errors}`)
        assert(!lines[0].includes('\r'), `no raw "\\r" survives: ${lines[0]}`)
        assert(!lines[0].includes('\n'), `no raw "\\n" survives: ${lines[0]}`)
    })
})

Deno.test('#376 CONTROL: a working WARN sink writes the WARN and no marked line', async () => {
    // Without this, W1 could pass because the marked line is written on every
    // apply, not only when the WARN itself failed.
    await watchingEscapes(async (escaped) => {
        const { send } = await ownerOf('c1')
        const realWarn = console.warn
        const realError = console.error
        const warns: string[] = []
        const errors: string[] = []
        console.warn = (...args: unknown[]) =>
            void warns.push(args.map(String).join(' '))
        console.error = (...args: unknown[]) =>
            void errors.push(args.map(String).join(' '))
        try {
            send({ kind: 'evict', target: 'c1' })
            await settle()
        } finally {
            console.warn = realWarn
            console.error = realError
        }

        assertEquals(escaped, [], 'no rejection reaches the runtime')
        assert(
            warns.some((line) => line.includes('evict teardown')),
            `the teardown failure is logged at WARN: ${warns}`,
        )
        assertEquals(marked(errors), [], 'no marked ERROR line')
    })
})

Deno.test('#376 CONTROL: the watcher records a real unhandled rejection', async () => {
    // Without this, every `escaped` assertion above could pass because the
    // listener never fires, not because nothing escaped.
    await watchingEscapes(async (escaped) => {
        void Promise.reject(new Error('deliberately unhandled'))
        await settle()
        assertEquals(escaped.length, 1, 'the watcher saw the escape')
    })
})
