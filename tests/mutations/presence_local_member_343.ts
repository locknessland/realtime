/**
 * @fileoverview #343's mutation battery — the local presence view counts
 * members, not connections, by one rule in one place.
 *
 * - (a) and (b) put a raw spread of the `presence` map back at each of the two
 *   READ sites `#localRoster` now serves: the `here` local fallback and the
 *   roster-less branch of `rosterSnapshot`. Each dies to its own witness row,
 *   which is why the two rows exist separately.
 * - (c) and (d) break the rule itself: last occurrence wins (a second tab's
 *   `info` overwrites the first, so the local view no longer matches the slot
 *   #330 wrote), and a key without `String()` (`1` and `'1'` become two
 *   members where the roster hash holds one).
 *
 * **Equivalent mutant, run as a KNOWN SURVIVOR (row e).** Reverting the
 * `#syncRosterMember` scan to `[...(this.presence.get(channel)?.values() ?? [])]`
 * cannot change behaviour: `.find` over the raw values returns the FIRST
 * connection whose member id matches the slot, in map order, and
 * `uniqueMembers` keeps exactly that first occurrence. Same member, same write.
 * The scan goes through `#localRoster` so the local entry and the roster slot
 * come from one rule by construction, not by coincidence of two iteration
 * orders — a property no public surface can observe, so row (e) is expected to
 * survive, and the battery says so instead of hiding it.
 *
 * Every row was proven LIVE before it was trusted: the edited line was probed
 * (a throwing marker in its place) and seen to execute under the killing
 * witness. A row whose line never runs reports a kill it did not cause.
 *
 * Each mutant compiles: (d) widens the `Map` key type in the same row, or the
 * type-checker would refuse it and the row would prove nothing.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_local_member_343.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_local_member_343
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const SNAPSHOT = new URL('../../presence_snapshot.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_local_member_343.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#343 (a) the here fallback reads the map raw again',
        file: MANAGER,
        edits: [[
            '            window = localWindow(this.#localRoster(channel))\n',
            '            window = localWindow([...(this.presence.get(channel)?.values() ?? [])])\n',
        ]],
        // Re-anchored for #341: the fallback now wraps the local roster in a
        // `localWindow`; the raw read is the same defect inside it.
        // The shipped defect on the fallback: member 7 twice, total 3.
        killedBy: 'the local fallback lists one entry per member',
    },
    {
        label: '#343 (b) the roster-less branch reads the map raw again',
        file: MANAGER,
        edits: [[
            '        return localWindow(this.#localRoster(channel))\n',
            '        return localWindow([...(this.presence.get(channel)?.values() ?? [])])\n',
        ]],
        // Re-anchored for #341: `rosterSnapshot` returns a window now.
        // The shipped defect on a driver with no roster ops.
        killedBy: 'a roster-less driver lists one entry per member',
    },
    {
        label: '#343 (c) the last occurrence wins',
        file: SNAPSHOT,
        edits: [[
            '        if (!byId.has(key)) byId.set(key, member)\n',
            '        byId.set(key, member)\n',
        ]],
        // `Map.set` on an existing key keeps its position, so order survives
        // and only the `info` flips to the latest tab — the local view then
        // disagrees with the slot the roster holds.
        killedBy: 'the local fallback lists one entry per member',
    },
    {
        label: '#343 (d) the key is member.id, not String(member.id)',
        file: SNAPSHOT,
        edits: [
            [
                '    const byId = new Map<string, PresenceMember>()\n',
                '    const byId = new Map<string | number, PresenceMember>()\n',
            ],
            [
                '        const key = String(member.id)\n',
                '        const key = member.id\n',
            ],
        ],
        // `1` and `'1'` are one slot in the roster hash and two here.
        killedBy: 'ids 1 and "1" are one member on the local view',
    },
    {
        label:
            '#343 (f) the local view is sorted instead of kept in join order',
        file: SNAPSHOT,
        edits: [[
            '    return [...byId.values()]\n',
            '    return [...byId.values()].sort((x, y) =>\n' +
            '        String(x.id).localeCompare(String(y.id))\n' +
            '    )\n',
        ]],
        // Survived the first review: every order asserted was ascending.
        killedBy: 'join order is kept, not sorted',
    },
    {
        label: '#343 (e) the slot scan reads the map raw again',
        file: MANAGER,
        edits: [[
            '            const desired = this.#localRoster(channel)\n',
            '            const desired = [...(this.presence.get(channel)?.values() ?? [])]\n',
        ]],
        killedBy: 'the winning connection leaves',
        expectSurvival:
            "KNOWN SURVIVOR — equivalent mutant. `.find` over the raw values returns the first connection with the slot's member id, which is exactly the occurrence uniqueMembers keeps, so the write is identical. See the file header.",
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#343 — the local presence view counts members',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
