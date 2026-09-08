/**
 * @fileoverview #304's mutation battery — one charset, three boundaries.
 *
 * Runs under `@lockness/contract`'s shared harness: green baseline before
 * anything is mutated, an atomic per-file lock, anchors matched exactly once,
 * a non-compiling mutant reported DEAD, and every kill attributed to the test
 * that claims it.
 *
 * The three boundary rows exist because the constraint used to live in only one
 * of three paths. A control frame naming an out-of-charset id was already
 * dropped on ingest, while a local evict worked and reconcile recovered it — so
 * an application using such an id had a revocation that worked on one instance
 * and silently nowhere else. Removing any one of these guards puts that back.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/connection_id_304.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/connection_id_304
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const DRIVER = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../connection_id_charset.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'the register() boundary guard removed',
        file: MANAGER,
        edits: [[
            '        this.#assertUsableId(connection.id)\n        this.connections.set(connection.id, connection)\n    }',
            '        this.connections.set(connection.id, connection)\n    }',
        ]],
        killedBy: 'registering a connection with an out-of-charset id throws',
    },
    {
        label: 'the subscribe() boundary guard removed',
        file: MANAGER,
        // Anchored on the comment that belongs to THIS call site, not on the
        // line that followed it. The old anchor spanned the gap to
        // `const kind = channelKind(channel)` to disambiguate from
        // `register()`'s identical call — and #314 inserted the channel
        // assertion into that gap, so the row went DEAD and reported nothing.
        // A battery row is only as durable as what its anchor assumes will
        // stay adjacent.
        edits: [[
            '        // rate-limit increment) ran on an id that was never usable.\n        this.#assertUsableId(connection.id)\n',
            '',
        ]],
        killedBy: 'subscribing with an out-of-charset id throws too',
    },
    {
        label: 'the evict() boundary guard removed',
        file: MANAGER,
        edits: [['        this.#assertUsableId(clientId)\n', '']],
        killedBy: 'evict() refuses an out-of-charset id',
    },
    {
        label: 'the guard inverted — only VALID ids are refused',
        file: MANAGER,
        edits: [[
            'if (isValidName(id)) return',
            'if (!isValidName(id)) return',
        ]],
        killedBy: 'an ordinary id is accepted by both sites',
    },
    {
        label: 'the id is no longer encoded into the throw message',
        file: MANAGER,
        // `safeForLog(id)` alone stopped being unique when #306 added
        // `PresenceMemberIdError`, which encodes its own id the same way — so
        // this row has been DEAD since that merge, reporting nothing while
        // looking like a row. Anchored on the surrounding message text now,
        // which names the error this row is actually about.
        edits: [[
            'realtime: connection id ${safeForLog(id)} is outside the ',
            'realtime: connection id ${id} is outside the ',
        ]],
        killedBy: 'the message names the id and the rule',
    },
    {
        label: 'the socket is left open when register() rejects the id',
        file: MANAGER,
        edits: [[
            "                    conn.close(1011, 'unusable connection id')\n",
            '',
        ]],
        killedBy: 'a rejected id closes the socket',
    },
    {
        label: 'the app onOpen runs anyway after a rejected id',
        file: MANAGER,
        edits: [[
            "                    conn.close(1011, 'unusable connection id')\n                    throw error",
            "                    conn.close(1011, 'unusable connection id')",
        ]],
        killedBy: 'a rejected id closes the socket',
    },
    {
        // THREE rows stood here and two are gone (#278). One anchored on
        // `#legacyRevoked`'s own filter; the other was a recorded
        // `expectSurvival` whose whole justification was that the real guard
        // had moved INSIDE that method, making this outer one belt-and-braces
        // over a set already clean. #278 deleted the method, so both anchors
        // address code that no longer exists — and, more importantly, the
        // redundancy the survivor recorded is gone with it.
        //
        // So this row's meaning CHANGED without its text changing: it is now
        // the only thing standing between a broker-sourced member and
        // `revokeLocal`. Removing the filter is a kill, not an equivalence.
        // The reason is transcribed into `listRevoked`'s docstring, where a
        // reader of the code finds it rather than a reader of this battery.
        label: 'the reconcile filter dropped on the sorted-set path',
        file: DRIVER,
        edits: [[
            'if (id && isValidName(id)) live.add(id)',
            'if (id) live.add(id)',
        ]],
        killedBy: 'reconcile drops a broker-injected id',
    },
]

Deno.exit(
    await runBattery(
        '#304 mutation battery — connection-id charset at three boundaries',
        SUITES,
        MUTATIONS,
    ),
)
