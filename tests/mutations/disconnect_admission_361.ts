/**
 * @fileoverview #361's mutation battery — a disconnected connection is refused
 * at admission, `unsubscribe` forgets before it leaves, and every manager
 * collector records a failure by a flag.
 *
 * The remedy's decisions live in these homes (plan §5), all in `manager.ts`:
 * `#retired` and its one writer at `disconnect`'s entry; `#assertAdmissible`,
 * its one reader, with two clauses and two classes; its askers (`register`
 * directly and first; `subscribe` through `#assertBound`, before its
 * authorizer and after the result is classified — #370); `unsubscribe`'s
 * forget-before-leave and its release on a failed leave; the flag collectors;
 * and `handlerHooks.onClose`.
 *
 * - N1 the post-check removed.
 * - N2 the pre-check removed — only the authorizer's call count sees it.
 * - N3 the retirement moved from `disconnect`'s entry into its `finally`.
 * - N4 the post-check moved below `#checkChannelCaps` (#370 D3): a retired
 *   connection on a full instance hears the cap's refusal instead of its own.
 *   Rewritten when `subscribe`'s implicit binding was deleted, which took the
 *   old anchor with it; killed by `#361 W13`, the pin added for it.
 * - N5 `register`'s check removed.
 * - N6 `unsubscribe`'s forget moved back after the awaited leave — killed by
 *   W9, the racing subscribe. The plan named W6 too, but the leave's failure
 *   is now caught by the flag collector, so the forget after it still runs and
 *   W6 cannot see the order (measured: `MISATTRIBUTED` against W6).
 * - N7 the release skipped when the leave failed.
 * - N9 the split collapsed: clause 2 throws `ConnectionDisconnectedError`.
 * - N10 the post-check hoisted above the deny `return`, straight after the
 *   awaited authorizer: a denial becomes a throw.
 * - N11 `disconnect`'s per-channel `try`/`catch` removed.
 * - N12 `handlerHooks.onClose` back to the unprotected order.
 * - N13 `disconnect`'s collector back to `failure === undefined`.
 * - N13b `#revokeChannelLocal`'s clear collector back to
 *   `clearError === undefined`, its returned flag dropped.
 * - N14 (#379) `handlerHooks.onClose`'s teardown-failure catch re-throws
 *   unconditionally instead of WARNing: the app's error is displaced by the
 *   teardown's. Killed only through `websocket_close_guard_369.test.ts`'s W4,
 *   added to this battery's `SUITES` for it — the combined witness wires
 *   `buildEvents` on top of `handlerHooks`, which this file's own suite does
 *   not.
 * - N15 (#379) the same catch's WARN dropped, the swallow kept.
 *
 * **Anchors.** Since #370 `subscribe` asks `#assertBound(connection)` at both
 * of its checks, and `#assertAdmissible(connection)` is asked by `register` and
 * by `#assertBound` itself — each call appears twice, so every row that touches
 * one anchors on a neighbouring line as well. N1, N2 and N10 were re-anchored
 * on the new method name, N9 on the widened clause 2 (#363), and N12 on
 * `onClose`'s object-form `disconnect(conn)`; their killers are unchanged.
 * N12's anchor and its replacement carry #404's open/close pairing, so the
 * mutant still changes only the order it names.
 * Every `killedBy` ends in a space, so `W1 ` is not a prefix of `W10`–`W13`.
 *
 * **Subsumed for #401.** N8 — "the bound-object clause removed from
 * `#assertAdmissible`" — mutated per-field guards that #363 widened into the
 * single clause 2 both this row and `register_only_admission_370.ts`'s M4 now
 * mutate: same anchor, same edit, same deletion, byte for byte. That is the
 * case `docs/testing.md`'s subsumption rule names — several per-field guards
 * became one, so N8's row had nothing left to mutate that M4 does not already
 * mutate. N8 is deleted; M4 covers it, and this paragraph is where the reason
 * lives (N8 carried no `expectSurvival` text to carry forward — it was a real
 * kill, not an equivalent mutant).
 * N6 and N7 were re-anchored again for #373: `unsubscribe`'s own flag-and-
 * try/catch leave collection moved into `#collectLeaveOutcome`, the helper it
 * now shares with `#joinPresence`'s compensation, so N6's `LEAVE` constant and
 * N7's `leaveFailed` reference moved with it — the mutants still change only
 * the order and the release-skip they always named; their killers are
 * unchanged.
 *
 * **Re-anchored for #393.** `#retired` moved from a `WeakSet` to a
 * `WeakMap<Connection, Promise<DisconnectOutcome>>`, and its one write moved
 * from `disconnect`'s own body into a private `#teardown` it now delegates
 * to. N3's anchor follows: the mutant still records "retired" too late —
 * inside `#teardown`'s `finally`, after the loop has already run, instead of
 * synchronously at entry — so W1's racing subscribe still finds nothing
 * retired and wrongly succeeds. It writes a placeholder settled promise, not
 * a real one, because the mutant's whole point is that nothing should have
 * been recorded yet; the value is never read on this path.
 *
 * **Re-anchored for #392.** `disconnect` is now a thin wrapper: the body N3
 * describes above — the join, the retired-write and the loop's `finally` —
 * moved verbatim into a private `#teardown(target)`, and the loop+`finally`
 * itself (what N3's text still calls "`#teardown`'s `finally`") is now
 * `#teardownChannels`, called from inside `#teardown`. Both of N3's edits are
 * plain source text, unmoved and unchanged, so the mutant still matches and
 * still kills the same way — only the surrounding method names moved, which
 * this note records rather than rewriting N3's label.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate disconnect_admission_361
 * ```
 *
 * @module @lockness/realtime/tests/mutations/disconnect_admission_361
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../disconnect_admission_361.test.ts', import.meta.url).pathname,
    // #379's combined close-path witness (W4) lives here, not in this
    // battery's own suite: it wires `buildEvents` on top of `handlerHooks`,
    // which this file's suite does not.
    new URL('../websocket_close_guard_369.test.ts', import.meta.url).pathname,
]

/** The post-check, with the comment above it and the one below it. */
const POST_CHECK =
    '        // The post-check (#361): the disconnect may have begun while the\n' +
    "        // authorizer ran. No await from here to the join's adds.\n" +
    '        this.#assertBound(connection)\n' +
    '\n' +
    '        // BEFORE any membership mutation'

/** The same, without the check. */
const NO_POST_CHECK = '        // BEFORE any membership mutation'

/** The cap check, then the line after it (#370: the post-check sits above). */
const CAPS = '            connection.identity !== null,\n' +
    '        )\n' +
    '\n' +
    '        // `member` is set on a presence admission'

/**
 * `unsubscribe`'s leave, with its comment — collected through
 * `#collectLeaveOutcome`, the helper #373 shares with `#joinPresence`'s
 * compensation. Re-anchored here for #373: the flag-and-try/catch this used to
 * match moved into that shared helper, so N6's anchor moved with it.
 */
const LEAVE =
    '        // Collected through `#collectLeaveOutcome`, the one helper this\n' +
    "        // shares with `#joinPresence`'s #323 compensation (#373) — a\n" +
    '        // rejection here must not skip the release below.\n' +
    '        const outcome = await this.#collectLeaveOutcome(channel, clientId)\n'

/** `handlerHooks.onClose`'s body, from the app flag to the re-throw. */
const ON_CLOSE_BODY = '                let appFailed = false\n' +
    '                let appError: unknown\n' +
    '                try {\n' +
    '                    // Once per opened socket (#404): never for one `register`\n' +
    '                    // refused, never twice. An evicted socket was opened, so\n' +
    '                    // it still gets its hook — ownership is not the question.\n' +
    '                    if (opened.delete(conn)) {\n' +
    '                        await userHooks.onClose?.(conn, code, reason)\n' +
    '                    }\n' +
    '                } catch (error) {\n' +
    '                    appFailed = true\n' +
    '                    appError = error\n' +
    '                }\n' +
    '                try {\n' +
    '                    await this.disconnect(conn)\n' +
    '                } catch (error) {\n' +
    '                    if (!appFailed) throw error\n' +
    '                    console.warn(\n' +
    '                        `realtime: disconnecting ${\n' +
    '                            safeForLog(conn.id)\n' +
    "                        } after the application's onClose threw also ` +\n" +
    '                            `failed: ${renderError(error)}`,\n' +
    '                    )\n' +
    '                }\n' +
    "                // The app's error first: it is the one its own code raised.\n" +
    '                if (appFailed) throw appError\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'N1 — the post-check removed',
        file: MANAGER,
        edits: [[POST_CHECK, NO_POST_CHECK]],
        killedBy: '#361 W1 ',
    },
    {
        label: 'N2 — the pre-check removed',
        file: MANAGER,
        edits: [[
            '        const kind = channelKind(channel)\n' +
            '        this.#assertBound(connection)\n',
            '        const kind = channelKind(channel)\n',
        ]],
        killedBy: '#361 W4 ',
    },
    {
        label:
            "N3 — the retirement moved from disconnect's entry into #teardown's finally",
        file: MANAGER,
        edits: [
            [
                '        if (bound !== undefined) this.#retired.set(bound, teardown)\n' +
                '        return teardown\n',
                '        return teardown\n',
            ],
            [
                '            if (bound !== undefined && this.#isOwner(bound)) {\n' +
                '                this.#channelsByClient.delete(clientId)\n' +
                '                this.connections.delete(clientId)\n' +
                '            }\n',
                '            if (bound !== undefined) {\n' +
                "                this.#retired.set(bound, Promise.resolve('not-owned'))\n" +
                '            }\n' +
                '            if (bound !== undefined && this.#isOwner(bound)) {\n' +
                '                this.#channelsByClient.delete(clientId)\n' +
                '                this.connections.delete(clientId)\n' +
                '            }\n',
            ],
        ],
        killedBy: '#361 W1 ',
    },
    {
        label: 'N4 — the post-check moved below #checkChannelCaps',
        file: MANAGER,
        edits: [
            [POST_CHECK, NO_POST_CHECK],
            [
                CAPS,
                '            connection.identity !== null,\n' +
                '        )\n' +
                '        this.#assertBound(connection)\n' +
                '\n' +
                '        // `member` is set on a presence admission',
            ],
        ],
        killedBy: '#361 W13 ',
    },
    {
        label: "N5 — register's check removed",
        file: MANAGER,
        edits: [[
            '        this.#assertAdmissible(connection)\n' +
            '        this.#assertUsableId(connection.id)\n',
            '        this.#assertUsableId(connection.id)\n',
        ]],
        killedBy: '#361 W5 ',
    },
    {
        label: "N6 — unsubscribe's forget moved back after the awaited leave",
        file: MANAGER,
        edits: [[
            '        const member = this.#forgetPresenceMember(channel, clientId)\n' +
            LEAVE,
            LEAVE +
            '        const member = this.#forgetPresenceMember(channel, clientId)\n',
        ]],
        killedBy: '#361 W9 ',
    },
    {
        label: 'N7 — the release skipped when the leave failed',
        // Re-anchored for #373: the flag `leaveFailed` moved into
        // `#collectLeaveOutcome`'s returned `outcome.failed`.
        file: MANAGER,
        edits: [[
            '        if (member) {\n' +
            '            // Released through the per-slot projection (#330), WHETHER OR NOT\n',
            '        if (member && !outcome.failed) {\n' +
            '            // Released through the per-slot projection (#330), WHETHER OR NOT\n',
        ]],
        killedBy: '#361 W6 (i) ',
    },
    {
        label:
            'N9 — the split collapsed: clause 2 throws ConnectionDisconnectedError',
        file: MANAGER,
        edits: [[
            '            throw new ConnectionIdInUseError()\n',
            '            throw new ConnectionDisconnectedError(connection.id)\n',
        ]],
        killedBy: '#361 W8 ',
    },
    {
        label: 'N10 — the post-check hoisted above the deny return',
        file: MANAGER,
        edits: [
            [POST_CHECK, NO_POST_CHECK],
            [
                '                ? await this.authorize(connection.identity, channel)\n' +
                '                : false\n',
                '                ? await this.authorize(connection.identity, channel)\n' +
                '                : false\n' +
                '            this.#assertBound(connection)\n',
            ],
        ],
        killedBy: '#361 W3 (ii) ',
    },
    {
        label: "N11 — disconnect's per-channel try/catch removed",
        file: MANAGER,
        edits: [[
            '                try {\n' +
            '                    await this.unsubscribe(clientId, channel)\n' +
            '                } catch (error) {\n',
            '                {\n' +
            '                    await this.unsubscribe(clientId, channel)\n' +
            '                }\n' +
            '                for (const error of [] as unknown[]) {\n',
        ]],
        killedBy: '#361 W6 (i) ',
    },
    {
        label: 'N12 — handlerHooks.onClose back to the unprotected order',
        file: MANAGER,
        edits: [[
            ON_CLOSE_BODY,
            '                if (opened.delete(conn)) {\n' +
            '                    await userHooks.onClose?.(conn, code, reason)\n' +
            '                }\n' +
            '                await this.disconnect(conn)\n',
        ]],
        killedBy: '#361 W12 (i) ',
    },
    {
        label: "N13 — disconnect's collector back to failure === undefined",
        file: MANAGER,
        edits: [
            [
                '                    if (!failed) {\n' +
                '                        failed = true\n' +
                '                        failure = error\n' +
                '                    } else {\n',
                '                    if (failure === undefined) failure = error\n' +
                '                    else {\n',
            ],
            [
                '        if (failed) throw failure\n',
                '        if (failure !== undefined) throw failure\n',
            ],
        ],
        killedBy: '#361 W6 (ii) disconnect',
    },
    {
        label:
            "N13b — #revokeChannelLocal's clear collector back to clearError === undefined",
        file: MANAGER,
        edits: [
            [
                '                if (!clearFailed && cleared.failed) {\n' +
                '                    clearFailed = true\n' +
                '                    clearError = cleared.error\n' +
                '                }\n',
                '                if (clearError === undefined) clearError = cleared.error\n',
            ],
            [
                '            if (!durabilityFailed && applied.clearFailed) {\n',
                '            if (!durabilityFailed && applied.clearError !== undefined) {\n',
            ],
        ],
        killedBy: '#361 W6 (ii) revokeChannel clear',
    },
    {
        label:
            "N14 — the teardown failure re-thrown unconditionally, past the app's",
        file: MANAGER,
        edits: [[
            '                } catch (error) {\n' +
            '                    if (!appFailed) throw error\n' +
            '                    console.warn(\n' +
            '                        `realtime: disconnecting ${\n' +
            '                            safeForLog(conn.id)\n' +
            "                        } after the application's onClose threw also ` +\n" +
            '                            `failed: ${renderError(error)}`,\n' +
            '                    )\n' +
            '                }\n',
            '                } catch (error) {\n' +
            '                    console.warn(\n' +
            '                        `realtime: disconnecting ${\n' +
            '                            safeForLog(conn.id)\n' +
            "                        } after the application's onClose threw also ` +\n" +
            '                            `failed: ${renderError(error)}`,\n' +
            '                    )\n' +
            '                    throw error\n' +
            '                }\n',
        ]],
        killedBy: '#379 W4 ',
    },
    {
        label: 'N15 — the teardown-failure WARN dropped',
        file: MANAGER,
        edits: [[
            '                } catch (error) {\n' +
            '                    if (!appFailed) throw error\n' +
            '                    console.warn(\n' +
            '                        `realtime: disconnecting ${\n' +
            '                            safeForLog(conn.id)\n' +
            "                        } after the application's onClose threw also ` +\n" +
            '                            `failed: ${renderError(error)}`,\n' +
            '                    )\n' +
            '                }\n',
            '                } catch (error) {\n' +
            '                    if (!appFailed) throw error\n' +
            '                }\n',
        ]],
        killedBy: '#379 W4 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#361 — a disconnected connection is refused at admission',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
