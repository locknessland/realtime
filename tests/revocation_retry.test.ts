/**
 * @fileoverview #308 — a failed seam-triggered revocation reconcile gets one
 * retry, and says which trigger it was.
 *
 * The two triggers are not equivalent on failure, and that asymmetry is the
 * whole issue. The timer's next pass is already scheduled, so a failed timer
 * pass costs only latency. The seam fires ONCE per outage and its intent is
 * consumed by the activation that fired it, so a failed seam pass was retried
 * by nothing — enforcement silently reverted to the periodic timer, which is
 * the pre-#271 exposure the seam exists to remove.
 *
 * @module @lockness/realtime/tests/revocation_retry
 */

import { assert, assertEquals } from '@std/assert'
import { FakeTime } from '@std/testing/time'
import { RedisBroadcastDriver } from '../drivers/redis.ts'

/** A command port that answers nothing — no reconcile here touches it. */
const command = () => Promise.resolve(null)

/** A subscriber that exposes the reconnect seam so a test can fire it. */
function seamSubscriber() {
    let fire: (() => void | Promise<void>) | undefined
    return {
        port: {
            psubscribe: () => {},
            onReconnect: (handler: () => void | Promise<void>) => {
                fire = handler
            },
        },
        /** Drive the seam exactly as the subscribe socket would. */
        reconnect: () => fire?.(),
    }
}

/** Capture `console.warn`, restoring on scope exit. */
function captureWarnings() {
    const real = console.warn
    const messages: string[] = []
    console.warn = (...args: unknown[]) => {
        messages.push(args.map((a) => String(a)).join(' '))
    }
    return {
        messages,
        [Symbol.dispose]: () => void (console.warn = real),
    }
}

function driverWith(seam: ReturnType<typeof seamSubscriber>) {
    return new RedisBroadcastDriver(
        { command },
        seam.port,
        { prefix: 'app:rt', presence: { reconcileIntervalMs: 10_000 } },
    )
}

Deno.test('#308 the WARN names WHICH trigger failed', async () => {
    // Both lines read identically before this, so an operator watching a WARN
    // stream could not tell the fast path had been lost from a routine pass
    // having failed — and those want different responses.
    const seam = seamSubscriber()
    const driver = driverWith(seam)
    const time = new FakeTime(new Date('2026-09-07T10:00:00Z'))
    using warn = captureWarnings()
    try {
        driver.onRevocationReconcile(() => {
            throw new Error('EVAL refused')
        })
        await seam.reconnect()
        assert(
            warn.messages.some((m) => m.includes('(reconnect)')),
            `the seam-triggered failure did not name itself: ${warn.messages}`,
        )

        warn.messages.length = 0
        await time.tickAsync(10_000)
        assert(
            warn.messages.some((m) => m.includes('(timer)')),
            `the timer-triggered failure did not name itself: ${warn.messages}`,
        )
    } finally {
        await driver.close()
        time.restore()
    }
})

Deno.test('#308 a failed SEAM reconcile is retried exactly once', async () => {
    const seam = seamSubscriber()
    const driver = driverWith(seam)
    const time = new FakeTime(new Date('2026-09-07T10:00:00Z'))
    using warn = captureWarnings()
    void warn
    let attempts = 0
    try {
        driver.onRevocationReconcile(() => {
            attempts++
            throw new Error('EVAL refused')
        })
        await seam.reconnect()
        assertEquals(attempts, 1, 'the seam pass ran')

        // The retry lands well inside the periodic cadence — that is what makes
        // it a fast path rather than a second timer.
        await time.tickAsync(1_500)
        assertEquals(attempts, 2, 'the failed seam pass was retried')

        // AND ONLY ONCE. Chaining would turn a broker that keeps refusing into
        // a hot loop against the command socket, which is the opposite of what
        // a bounded enforcement window needs. Nine more seconds, still inside
        // the 10s periodic tick, must add nothing.
        await time.tickAsync(8_000)
        assertEquals(
            attempts,
            2,
            'the retry retried itself — one failing broker now loops',
        )
    } finally {
        await driver.close()
        time.restore()
    }
})

Deno.test('#308 a failed TIMER reconcile is NOT retried — its next pass is already due', async () => {
    // The asymmetry, pinned. Retrying the timer would double its rate for as
    // long as the broker is unhealthy, which is a self-inflicted load spike at
    // precisely the wrong moment.
    const seam = seamSubscriber()
    const driver = driverWith(seam)
    const time = new FakeTime(new Date('2026-09-07T10:00:00Z'))
    using warn = captureWarnings()
    void warn
    let attempts = 0
    try {
        driver.onRevocationReconcile(() => {
            attempts++
            throw new Error('EVAL refused')
        })
        await time.tickAsync(10_000)
        assertEquals(attempts, 1, 'the timer pass ran')
        await time.tickAsync(2_000)
        assertEquals(attempts, 1, 'a failed timer pass scheduled a retry')
    } finally {
        await driver.close()
        time.restore()
    }
})

Deno.test('#308 close() leaves no pending retry behind', async () => {
    const seam = seamSubscriber()
    const driver = driverWith(seam)
    const time = new FakeTime(new Date('2026-09-07T10:00:00Z'))
    using warn = captureWarnings()
    void warn
    let attempts = 0
    try {
        driver.onRevocationReconcile(() => {
            attempts++
            throw new Error('EVAL refused')
        })
        await seam.reconnect()
        assertEquals(attempts, 1)
        await driver.close()
        await time.tickAsync(5_000)
        assertEquals(attempts, 1, 'a retry ran after close()')
    } finally {
        time.restore()
    }
})
