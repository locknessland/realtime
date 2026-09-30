/**
 * @fileoverview #404's mutation battery — `handlerHooks` runs the app's
 * `onClose` exactly once for each socket whose `onOpen` ran: evicted ones
 * included, refused ones never.
 *
 * The decision lives in one home, `handlerHooks` in `manager.ts`: a
 * closure-local `WeakSet` of opened objects, added to after `register`
 * succeeds and before the app's `onOpen`, and `delete`d by the gate in front
 * of the app's `onClose`.
 *
 * - M1 the gate removed: every close runs the app's hook.
 * - M2 the gate asks `#isOwner` instead of the set — the ownership shape the
 *   disposition rejected: an evicted socket no longer owns its id, so it loses
 *   its hook.
 * - M3 the gate asks `has` instead of `delete`: a second close runs it again.
 * - M4 the `add` moved above `register`: a refused socket is recorded as
 *   opened before its `register` throws.
 * - M5 the `add` moved below the app's `onOpen`: an app hook that throws
 *   leaves an admitted socket unrecorded, so its close skips the app's
 *   `onClose` (#404 review, MEDIUM).
 *
 * Every row was proven LIVE: the harness ran the mutant and its named witness
 * went red, attributed. Every `killedBy` ends in a space, so `W1 ` is not a
 * prefix of a later `W1x`.
 *
 * ```bash
 * deno task mutate onclose_pairing_404
 * ```
 *
 * @module @lockness/realtime/tests/mutations/onclose_pairing_404
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../onclose_pairing_404.test.ts', import.meta.url).pathname,
]

/** The app's hook, behind the gate, as shipped. */
const GATED = '                    if (opened.delete(conn)) {\n' +
    '                        await userHooks.onClose?.(conn, code, reason)\n' +
    '                    }\n'

/** `onOpen`'s register, then the `add` after it, as shipped. */
const REGISTER_THEN_ADD = '                try {\n' +
    '                    this.register(conn)\n' +
    '                } catch (error) {\n' +
    "                    conn.close(1011, 'unusable connection id')\n" +
    '                    throw error\n' +
    '                }\n' +
    "                // After the register succeeded, before the app's hook: a\n" +
    '                // refused socket never reaches this line, so it never gets\n' +
    "                // the app's onClose either — and an app onOpen that throws\n" +
    '                // or closes the socket still gets its onClose (#404 W4).\n' +
    '                opened.add(conn)\n'

const MUTATIONS: Mutation[] = [
    {
        label: "M1 — the gate removed: every close runs the app's onClose",
        file: MANAGER,
        edits: [[
            GATED,
            '                    await userHooks.onClose?.(conn, code, reason)\n',
        ]],
        killedBy: '#404 W1 (i) ',
    },
    {
        label: 'M2 — the gate asks #isOwner instead of the set',
        file: MANAGER,
        edits: [[
            '                    if (opened.delete(conn)) {\n',
            '                    if (this.#isOwner(conn)) {\n',
        ]],
        killedBy: '#404 W2 ',
    },
    {
        label: 'M3 — the gate asks has instead of delete',
        file: MANAGER,
        edits: [[
            '                    if (opened.delete(conn)) {\n',
            '                    if (opened.has(conn)) {\n',
        ]],
        killedBy: '#404 W3 ',
    },
    {
        label: 'M4 — the add moved above register',
        file: MANAGER,
        edits: [[
            REGISTER_THEN_ADD,
            '                opened.add(conn)\n' +
            '                try {\n' +
            '                    this.register(conn)\n' +
            '                } catch (error) {\n' +
            "                    conn.close(1011, 'unusable connection id')\n" +
            '                    throw error\n' +
            '                }\n',
        ]],
        killedBy: '#404 W1 (i) ',
    },
    {
        label: "M5 — the add moved below the app's onOpen",
        file: MANAGER,
        edits: [[
            '                opened.add(conn)\n' +
            '                return userHooks.onOpen?.(conn)\n',
            '                const result = userHooks.onOpen?.(conn)\n' +
            '                opened.add(conn)\n' +
            '                return result\n',
        ]],
        killedBy: '#404 W4 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                "#404 — the app's onClose runs once per opened socket",
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
