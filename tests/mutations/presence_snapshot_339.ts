/**
 * @fileoverview #339's mutation battery — a subscribe's presence snapshot is
 * bounded, keeps its joiner, counts the room before the cut, and is bounded on
 * both roster sources.
 *
 * Eight rows, one per rule the plan's decision table gives a single home. Each
 * mutant is written to COMPILE: a mutant the type-checker refuses is recorded
 * dead and proves nothing about the suite.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_snapshot_339.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_snapshot_339
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const SNAPSHOT = new URL('../../presence_snapshot.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_snapshot_bound_339.test.ts', import.meta.url)
        .pathname,
    new URL('../presence_snapshot_unit_339.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#339 self is no longer kept (a bare slice)',
        file: SNAPSHOT,
        edits: [[
            '        selfId === undefined ||\n',
            '        limit > 0 || selfId === undefined ||\n',
        ]],
        // Re-anchored for #341, where the keep-self block became an early
        // return: a guard that always holds rather than a deleted block, so
        // the code below stays reachable to the type-checker and `selfId`
        // keeps its narrowing, and the mutant compiles. This is the shape that
        // ships a joiner a roster it is not in — and on a shared barrier read
        // it is every joiner but the first K. The witness's shared-read row is the
        // one named; the ceiling row dies too.
        killedBy: 'each keep THEMSELVES, never each other',
    },
    {
        label: '#339 the cut keeps one member too many (limit + 1)',
        file: SNAPSHOT,
        edits: [[
            '        : window.members.slice(0, limit)\n',
            '        : window.members.slice(0, limit + 1)\n',
        ]],
        // The off-by-one a byte ceiling exists to catch: 101 members, and a
        // published ceiling that is no longer true.
        //
        // Re-attributed for #341. The ceiling witness reads through the memory
        // driver, which now bounds the read to K itself, so the cut never
        // fires there and the mutant survives it — correctly: the driver is
        // not trusted to honour K (plan §5), and the cut is what a local
        // fallback or a driver that ignores `limit` meets. The unit row that
        // hands the cut an oversized window is the one that proves it.
        killedBy: 'an oversized roster is cut to the first K',
    },
    {
        label: '#339 `total` is computed AFTER the cut',
        file: SNAPSHOT,
        edits: [[
            '    const { total } = window\n',
            '    const total = Math.min(window.total, limit)\n',
        ]],
        // Re-anchored for #341: `total` is taken from the window once, and
        // every return reuses it — so "counted after the cut" is now the
        // window's population clamped to the bound, on every path.
        // Then `members.length < total` is never true, and a partial snapshot
        // is indistinguishable from a whole room — the one thing removing the
        // flat `members` field was meant to prevent.
        killedBy: 'the default ceiling is pinned',
    },
    {
        label: '#339 the cut is applied only on the AUTHORITATIVE branch',
        file: MANAGER,
        edits: [[
            '            this.presence.get(channel)?.get(clientId)?.id,\n' +
            '            this.#maxPresenceSnapshotMembers,\n',
            '            this.presence.get(channel)?.get(clientId)?.id,\n' +
            "            source === 'local'\n" +
            '                ? Infinity\n' +
            '                : this.#maxPresenceSnapshotMembers,\n',
        ]],
        // The fallback is exactly the path nobody watches: the broker is down,
        // a WARN is logged, and the local list goes out whole. An unbounded
        // local view is small on most instances and not on a hub instance.
        killedBy: 'the local fallback is bounded',
    },
    {
        label: '#339 the default bound drifts (100 → 101)',
        file: MANAGER,
        edits: [[
            'export const MAX_PRESENCE_SNAPSHOT_MEMBERS = 100\n',
            'export const MAX_PRESENCE_SNAPSHOT_MEMBERS = 101\n',
        ]],
        // Why the witness pins literals: a test that measured against the
        // constant would pass this.
        killedBy: 'the default ceiling is pinned',
    },
    {
        label: '#339 truncation logs',
        file: MANAGER,
        edits: [[
            '        return { ok: true, here: { ...here, source } }\n',
            '        if (here.members.length < here.total) {\n' +
            "            console.warn('realtime: presence snapshot cut')\n" +
            '        }\n' +
            '        return { ok: true, here: { ...here, source } }\n',
        ]],
        // FR-008: a cut is the designed reply. A WARN per join into a large
        // room is a log line an attacker buys with one frame.
        killedBy: 'the default ceiling is pinned',
    },
    {
        label: '#339 sort inside boundPresenceSnapshot',
        file: SNAPSHOT,
        edits: [[
            '        ? window.members\n',
            '        ? window.members.sort((a, b) =>\n' +
            '            String(a.id).localeCompare(String(b.id))\n' +
            '        )\n',
        ]],
        // Re-anchored for #341 on the fitting arm of the cut, which replaced
        // the `total <= limit` early return.
        // Driver order is kept: a sort is O(N log N) per caller on a shared
        // read, and it mutates the array it was handed.
        killedBy: 'a room within the bound is returned whole',
    },
    {
        label: '#339 self is captured BEFORE the closing read settles',
        file: MANAGER,
        edits: [[
            '            this.presence.get(channel)?.get(clientId)?.id,\n' +
            '            this.#maxPresenceSnapshotMembers,\n',
            '            fetchSelfId,\n' +
            '            this.#maxPresenceSnapshotMembers,\n',
        ]],
        // Re-anchored for #341: the pre-await id now EXISTS (`fetchSelfId`,
        // what the read fetches), so the mutant no longer has to invent one —
        // it passes that id to the cut instead of the post-await lookup.
        // FR-006: a leave that overtakes the read leaves no member to keep; a
        // self captured early hands the reply a member the connection dropped.
        killedBy: 'a join overtaken by its own leave',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#339 — a bounded snapshot that keeps its joiner',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
