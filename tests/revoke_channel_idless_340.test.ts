/**
 * @fileoverview #340 — an id-less `revoke-channel` frame is still not applied,
 * and it is no longer dropped silently.
 *
 * #337 made the owner clear exactly the record a frame names, by its
 * `revocationId`. A frame without one names no record, so it is dropped. A
 * custom driver that serialises control frames from a fixed field list strips
 * that field, and until #340 nothing said so: the revocation waited for the
 * reconcile tick and no log line told the operator or the driver author why.
 *
 * **The seam is driven directly.** The fake driver hands the test the manager's
 * `onControl` handler, so the frame reaches the switch exactly as an
 * authenticated frame off the bus would, and nothing in between is stubbed.
 *
 * @module @lockness/realtime/tests/revoke_channel_idless_340
 */

import { assert, assertEquals } from '@std/assert'
import { ChannelManager } from '../manager.ts'
import type {
    BroadcastDriver,
    ChannelRevocation,
    ControlMessage,
} from '../driver.ts'
import type { Connection } from '../types.ts'

interface User {
    id: number
}

const ROOM = 'private-room'

/** Run the microtask queue out; a control frame is applied as `void`. */
const settle = async () => {
    for (let i = 0; i < 50; i++) await Promise.resolve()
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

/** A manager over a driver whose control seam the test holds. */
async function ownerOf(target: string) {
    let deliver: ((control: ControlMessage) => void) | undefined
    const cleared: ChannelRevocation[] = []
    const driver: BroadcastDriver = {
        publish: () => {},
        onMessage: () => {},
        onControl(handler) {
            deliver = handler
        },
        publishControl: () => Promise.resolve(),
        markRevocation: () => Promise.resolve(),
        listRevocations: () => Promise.resolve([]),
        clearRevocation: (r) => void cleared.push(r),
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
        cleared,
        send: deliver as (control: ControlMessage) => void,
    }
}

Deno.test('#340 WITNESS: an id-less revoke-channel frame logs a WARN and is still not applied', async () => {
    const { manager, cleared, send } = await ownerOf('c1')
    using warn = captureWarnings()

    send({ kind: 'revoke-channel', target: 'c1', channel: ROOM })
    await settle()

    const lines = warn.messages.filter((m) => m.includes('revocationId'))
    assertEquals(
        lines.length,
        1,
        `exactly one WARN names the missing revocationId: ${warn.messages}`,
    )
    assert(lines[0].includes(ROOM), `the WARN names the channel: ${lines[0]}`)
    assert(
        lines[0].includes('was ignored'),
        `the WARN says the frame was ignored: ${lines[0]}`,
    )
    assert(
        lines[0].includes('reconcile'),
        `the WARN says enforcement falls to the reconcile tick: ${lines[0]}`,
    )

    // No `victim.closes` assertion: an APPLIED channel revocation keeps the
    // socket open too (#332), so it could never tell the two apart. The
    // membership and the cleared records can — and the control test below
    // proves they move within this same settle when a frame IS applied.
    assertEquals(cleared, [], 'no revocation record is cleared')
    assertEquals(
        await manager.unsubscribe('c1', ROOM),
        'left',
        'and the membership is untouched — the frame was not applied',
    )
})

Deno.test('#340 CONTROL: the same frame WITH an id is applied within the same settle', async () => {
    // Without this, the witness's "not applied" half could pass because the
    // apply is merely slower than the settle, not because it never ran.
    const { manager, cleared, send } = await ownerOf('c1')
    using warn = captureWarnings()

    send({
        kind: 'revoke-channel',
        target: 'c1',
        channel: ROOM,
        revocationId: 'r-1',
    })
    await settle()

    assertEquals(warn.messages, [], 'an id-bearing frame logs nothing')
    assertEquals(cleared.map((r) => r.id), ['r-1'], 'its record is cleared')
    assertEquals(
        await manager.unsubscribe('c1', ROOM),
        'not-subscribed',
        'and the membership is gone — the frame was applied',
    )
})

Deno.test('#340 the channel name in the WARN is log-encoded', async () => {
    // Channel names are attacker-influenced. A raw newline in the WARN lets a
    // name forge a second log line.
    const { send } = await ownerOf('c1')
    using warn = captureWarnings()

    send({
        kind: 'revoke-channel',
        target: 'c1',
        channel: 'private-room\nrealtime: forged line',
    })
    await settle()

    const lines = warn.messages.filter((m) => m.includes('revocationId'))
    assertEquals(lines.length, 1, `one WARN: ${warn.messages}`)
    assert(!lines[0].includes('\n'), `the name is encoded: ${lines[0]}`)
})

Deno.test('#340 an instance that does not own the target stays silent', async () => {
    // The frame reaches every instance. Only the owner's enforcement is
    // delayed, so only the owner says so — one WARN per frame, not one per
    // instance in the fleet.
    const { send } = await ownerOf('c1')
    using warn = captureWarnings()

    send({ kind: 'revoke-channel', target: 'owned-elsewhere', channel: ROOM })
    await settle()

    assertEquals(warn.messages, [], 'a non-owner logs nothing')
})
