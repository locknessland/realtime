/**
 * @fileoverview #341's mutation battery — the roster read a presence subscribe
 * ingests is bounded in the driver, its `total` is counted in that same read,
 * and a shared read still carries every caller's own member.
 *
 * One row per rule the plan's decision table gives a home, plus the two test
 * doubles those rules lean on (the fake's `HMGET`). Each mutant is written to
 * COMPILE — a mutant the type-checker refuses is recorded dead and proves
 * nothing — and each was probed LIVE: the mutated line executes under the test
 * it names (coverage of that test alone, recorded in the #341 completion
 * report), so a KILLED here is not a kill by a test that never reached it.
 *
 * **One declared survivor.** The memory driver walks at most `limit` values
 * instead of copying the whole room and slicing it. The two produce the same
 * window, so no behavioural test can tell them apart; the walk is a cost
 * property, and the plan names it as the residue a mutant cannot pin.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/presence_read_bound_341.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_read_bound_341
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const BARRIER = new URL('../../roster_read_barrier.ts', import.meta.url)
const SNAPSHOT = new URL('../../presence_snapshot.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const MEMORY = new URL('../../drivers/memory.ts', import.meta.url)
const FAKE = new URL('../fake_redis.ts', import.meta.url)
const SUITES = [
    // T005 / T010 / T012 — the byte pin, the gated shared reads, small rooms.
    new URL('../presence_read_bound_341.test.ts', import.meta.url).pathname,
    // T006 / T007 — both drivers' windows and input asserts, on FakeRedis.
    new URL('../roster_window_341.test.ts', import.meta.url).pathname,
    // T004 — the capability and the construction-time refusal.
    new URL('../presence_roster_guard.test.ts', import.meta.url).pathname,
    // T002 — the fake's `HMGET`, which is what makes the padding row bite.
    new URL('../fake_redis_conformance.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: '#341 the manager asks the driver for the whole room',
        file: MANAGER,
        edits: [[
            '        const limit = this.#maxPresenceSnapshotMembers\n',
            '        const limit = Number.MAX_SAFE_INTEGER\n',
        ]],
        // THE DEFECT, restored at its one call site: a positive integer, so
        // the driver's own assert lets it through, and `HRANDFIELD` with a
        // count past the size returns the whole hash. The reply is still cut
        // to K afterwards — nothing a client sees changes, only what the
        // instance ingested. Which is why the witness pins bytes, not members.
        killedBy: 'ingests the same bytes from a room of 1 000 as from 10 000',
    },
    {
        label: '#341 `total` is the window length, not the roster count',
        file: REDIS,
        edits: [[
            '        return { members, total, selves }\n',
            '        return { members, total: members.length, selves }\n',
        ]],
        // `HLEN` still runs inside the script; only its answer is dropped. A
        // bounded window then reports itself as the whole room — K, never N.
        killedBy: 'ingests the same bytes from a room of 1 000 as from 10 000',
    },
    {
        label: "#341 a queued batch's read carries only its first caller's id",
        file: BARRIER,
        edits: [[
            '        const promise = ahead.then(\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '            () => this.#issue(channel, [...ids]),\n' +
            '        )\n',
            '        const promise = ahead.then(\n' +
            '            () => this.#issue(channel, [...ids].slice(0, 1)),\n' +
            '            () => this.#issue(channel, [...ids].slice(0, 1)),\n' +
            '        )\n',
        ]],
        // The read is still shared and still counted as two — the count-only
        // witness of #333 passes it. The second caller of the batch is simply
        // answered by a read that never fetched its member.
        killedBy: 'B and C outside the window each keep their OWN self',
    },
    {
        label: '#341 the cut ignores `selves`',
        file: SNAPSHOT,
        edits: [[
            '    const self = window.members.find(isSelf) ?? window.selves.find(isSelf)\n',
            '    const self = window.members.find(isSelf)\n',
        ]],
        // #339's self rule without #341's widening: a joiner outside the
        // sampled window is fetched, transferred, and then not kept.
        killedBy: 'ingests the same bytes from a room of 1 000 as from 10 000',
    },
    {
        label: '#341 the cut keeps the id captured BEFORE the read',
        file: MANAGER,
        edits: [[
            '            this.presence.get(channel)?.get(clientId)?.id,\n' +
            '            this.#maxPresenceSnapshotMembers,\n',
            '            fetchSelfId,\n' +
            '            this.#maxPresenceSnapshotMembers,\n',
        ]],
        // The pre-await id may only WIDEN the read. Deciding with it hands a
        // caller that unsubscribed during the read a member the roster no
        // longer holds — `selves` was read before the leave committed. #339's
        // battery carries the same mutant against its own witness.
        killedBy: 'an unsubscribe during the gated read drops self',
    },
    {
        label: '#341 a new id joins the newest batch past the cap',
        file: BARRIER,
        edits: [[
            '        const batch = newest && newest.ids.size < this.#maxSelfIds\n',
            '        const batch = newest\n',
        ]],
        // No read ever opens because of the cap: one read carries every id a
        // storm brings, and the per-read bound becomes the storm's size.
        killedBy: 'past the cap a caller starts a NEW batch',
    },
    {
        label: '#341 batches count frames, not distinct ids',
        file: BARRIER,
        edits: [
            [
                '        const queued = slot.pending.get(id)\n' +
                '        if (queued) return queued\n',
                '',
            ],
            [
                '        const ids = new Set<string>()\n',
                '        const ids = new (class extends Set<string> {\n' +
                '            frames = 0\n' +
                '            override add(value: string): this {\n' +
                '                this.frames++\n' +
                '                return super.add(value)\n' +
                '            }\n' +
                '        })()\n',
            ],
            [
                '        const batch = newest && newest.ids.size < this.#maxSelfIds\n',
                '        const batch = newest &&\n' +
                '                (newest.ids as Set<string> & { frames: number }).frames <\n' +
                '                    this.#maxSelfIds\n',
            ],
        ],
        // S1: a pipelined re-join storm from ONE socket (the framework meters
        // no frame, #329) fills batch after batch with the same id — six reads
        // of one entry for 5 000 frames, where #333 cost two.
        killedBy: '5 000 frames from ONE member id cost two reads',
    },
    {
        label: "#341 a started batch's ids stay in `pending`",
        file: BARRIER,
        edits: [[
            '            for (const id of head.ids) slot.pending.delete(id)\n',
            '',
        ]],
        // H1: a caller whose id rode a batch that has ALREADY been issued finds
        // that batch in `pending` and joins it — answered by a read older than
        // its ask, which is exactly the leading-edge sharing the barrier bans.
        killedBy:
            'a late same-id caller does not ride a read that already started',
    },
    {
        label:
            '#341 a new batch chains on the running read, not the batch ahead',
        file: BARRIER,
        edits: [[
            '        const ahead = slot.queue.at(-1)?.promise ?? slot.running\n',
            '        const ahead = slot.running\n',
        ]],
        // H2: every queued batch is issued the moment the running read settles,
        // so a cap overflow puts two reads in flight at once — the per-channel
        // bound becomes ⌈S / cap⌉ concurrent reads.
        killedBy: 'waits for the batch AHEAD of it, not the running read',
    },
    {
        label: '#341 batch ids are keyed by the raw id, not String(id)',
        file: BARRIER,
        edits: [[
            '        const id = String(selfId)\n',
            '        const id = selfId as string\n',
        ]],
        // `1` and `'1'` name one member (the roster keys by String(id)); keyed
        // raw, they occupy two slots and a read carries the member twice.
        killedBy: "ids dedupe by String(id): 1 and '1' share one batch slot",
    },
    {
        label: '#341 an id-less caller joins the newest queued batch',
        file: BARRIER,
        edits: [[
            '            return slot.queue[0] ?? this.#open(channel, slot)\n',
            '            return slot.queue.at(-1) ?? this.#open(channel, slot)\n',
        ]],
        // Freshness holds either way; the pin is latency — an id-less caller is
        // answered by the earliest read that satisfies its ask, not the last.
        killedBy: 'an id-less caller joins the OLDEST queued batch',
    },
    {
        label: '#341 the legacy roster guard returns silently',
        file: MANAGER,
        edits: [[
            // RE-ANCHORED by #345: one guard now refuses every retired roster
            // member at once, so "returns silently" is its empty-list exit.
            '    if (present.length === 0) return\n',
            '    if (present.length === 0 || legacy) return\n',
        ]],
        // A pre-0.4.0 driver constructs, is narrowed to "no roster" because it
        // lacks `readRoster`, and every presence room silently becomes this
        // instance's local view.
        killedBy:
            'a driver still carrying listMembers is refused at construction',
    },
    {
        label: "#341 the read script's `''` padding is removed",
        file: REDIS,
        edits: [
            [
                '                String(limit),\n' +
                "                '',\n" +
                '                ...wanted,\n',
                '                String(limit),\n' +
                '                ...wanted,\n',
            ],
        ],
        // Selves are matched against a Set of wanted ids, not by slot index, so
        // dropping the padding needs no second edit to stay consistent.
        // A1, the one HIGH of the plan audit: consistent on every read that
        // carries an id, and an arity error on real Redis for the one that
        // carries none (a superseded join) — which only bites because the fake
        // refuses `HMGET` with no field, as a broker does.
        killedBy: 'readRoster with zero self ids still issues a valid HMGET',
    },
    {
        label: "#341 the Redis driver trusts the caller's limit",
        file: REDIS,
        edits: [[
            '        if (!Number.isInteger(limit) || limit < 1) {\n' +
            '            throw new Error(\n',
            '        if (limit !== limit) {\n' +
            '            throw new Error(\n',
        ]],
        // S2: the seam is exported. A negative `HRANDFIELD` count returns
        // |count| entries WITH repeats; `1.5` is a broker error mid-script.
        killedBy:
            'redis readRoster refuses bad input before issuing any command',
    },
    {
        label: '#341 the memory driver accepts more self ids than the cap',
        file: MEMORY,
        edits: [[
            '        if (selfIds.length > MAX_ROSTER_READ_SELF_IDS) {\n',
            '        if (selfIds.length > MAX_ROSTER_READ_SELF_IDS * 2) {\n',
        ]],
        // The same S2 assert, on the other shipped driver — the cap bounds one
        // read's transfer only if every driver refuses to exceed it.
        killedBy: 'memory readRoster refuses more self ids than',
    },
    {
        label: '#341 the memory driver returns a self once per requested id',
        file: MEMORY,
        edits: [[
            '            if (!self || seen.has(id)) continue\n',
            '            if (!self) continue\n',
        ]],
        // `[7, '7']` would hand back two copies of one member — the Redis
        // driver dedupes, so the two shipped drivers disagreed on one seam.
        killedBy: 'readRoster contract — memory',
    },
    {
        label: '#341 the Redis driver matches selves against the wanted set',
        file: REDIS,
        edits: [[
            '            if (id !== wanted[i - 1] || seen.has(id)) continue\n',
            '            if (!wanted.includes(id) || seen.has(id)) continue\n',
        ]],
        // S3: an entry under field `7` naming `9` would be accepted as `9`'s
        // self whenever `9` is also requested — vouched for by the wrong field.
        killedBy: "refuses an entry under another requested id's field",
    },
    {
        label: "#341 the fake's HMGET answers '' for an absent field",
        file: FAKE,
        edits: [[
            "                            ? { type: 'nil' }\n",
            "                            ? { type: 'bulk', value: '' }\n",
        ]],
        // A broker answers nil. A fake answering an empty string would make the
        // driver's nil-skip untested and its parse path the one exercised.
        killedBy: 'HMGET answers in field order with nil for an absent field',
    },
    {
        label: '#341 the memory driver copies the whole room, then slices',
        file: MEMORY,
        edits: [[
            '        for (const member of room.values()) {\n' +
            '            if (members.length === limit) break\n' +
            '            members.push(member)\n' +
            '        }\n',
            '        members.push(...[...room.values()].slice(0, limit))\n',
        ]],
        killedBy: '(none — equivalent)',
        expectSurvival:
            'KNOWN SURVIVOR — equivalent mutant. The full copy and the O(limit) walk return the same first `limit` members in join order; only the cost differs, and a behavioural test cannot observe it. Named as residue in the #341 plan (§12).',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#341 — the roster read a presence subscribe ingests is bounded',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
