/**
 * @fileoverview #350's mutation battery — a presence member is the JSON round
 * trip of `{ id, info }`, parsed once at admission, checked with the one
 * predicate every receiver runs.
 *
 * - M1 the raw candidate is returned: the #350 defect itself.
 * - M2 the extra-key check removed — the "strip" variant. Killed ONLY by the
 *   refusal rows ((a), (g) and the unit key rows); every LEAK row ((b), (c))
 *   survives it, because the stripped copy still ships nothing but
 *   `{ id, info }`. That is the recorded proof that the security property
 *   does not depend on the product answer (refuse vs. correct).
 * - M3 the draft re-reads `id`: a getter's second answer is what ships.
 * - M4 the draft re-reads `info`: same, for `info`.
 * - M5 `admitted = draft` — the filed direction, a shallow copy checked in
 *   memory: a `Date` `info` passes as an object and arrives as a string.
 * - M6 the join's wire check removed: `null` / `Date` `info` join locally and
 *   every peer drops them.
 * - M7 the ingest back to a key COUNT (`<= 2`): a signed `{ id, smuggled }`
 *   is re-emitted by every peer.
 * - M8 the roster read's `info` check removed: a non-object `info` is read
 *   back.
 * - M9 the shape error echoes a value beside its key.
 * - M10 admission moved below `#checkChannelCaps`: a full connection hears
 *   `ChannelLimitError`, and a refusal is a partial write. Re-anchored by
 *   #370, which deleted the `connections` write below the caps.
 * - M11 the non-object-`info` refusal echoes the parsed value after its
 *   message: every wording assertion still passes, so only a row whose `info`
 *   carries the sentinel can see it.
 * - M12 the serializes-to-nothing refusal echoes the supplied value after its
 *   message: a function's source or a symbol's description reaches logs.
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant ran
 * and turned its named witness red.
 *
 * ```bash
 * deno task mutate presence_member_admission_350
 * ```
 *
 * @module @lockness/realtime/tests/mutations/presence_member_admission_350
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MEMBER = new URL('../../presence_member.ts', import.meta.url)
const PROTOCOL = new URL('../../protocol.ts', import.meta.url)
const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../presence_member_admission_350.test.ts', import.meta.url)
        .pathname,
]

const DRAFT =
    '        text = JSON.stringify(info === undefined ? { id } : { id, info })\n'
// RE-ANCHORED by #357: the object result is admitted on EVERY kind before
// the presence branch, and `true`'s `{ id: connection.id }` inside it, so
// taking the admission out of `subscribe` is two edits now. The source
// moved; the guard remains.
const ADMISSION_CALL =
    '            const returned = verdict.member === undefined\n' +
    '                ? undefined\n' +
    '                : admitPresenceMember(\n' +
    '                    verdict.member,\n' +
    '                    this.#maxPresenceMemberBytes,\n' +
    '                )\n'
const RAW_RETURNED =
    '            const returned = verdict.member as PresenceMember | undefined\n'
const SEAT_CALL =
    '                member = returned ?? admitPresenceMember(\n' +
    '                    { id: connection.id },\n' +
    '                    this.#maxPresenceMemberBytes,\n' +
    '                )\n'
const RAW_MEMBER =
    '                member = returned ?? { id: connection.id }\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the raw candidate is returned (the #350 defect)',
        file: MEMBER,
        // #354 moved the anchor: the return now freezes the parsed copy. The
        // source moved, the guard remains — the raw candidate still ships.
        edits: [[
            '    return freezePresenceMember(admitted)\n}',
            '    return candidate as PresenceMember\n}',
        ]],
        killedBy: '#350 (b) memory: a member whose toJSON',
    },
    {
        label: 'M2 — the extra-key check removed: the strip variant',
        file: MEMBER,
        edits: [[
            '    if (extraKeys.length > 0) throw new PresenceMemberShapeError({ extraKeys })\n',
            '',
        ]],
        // Only refusal rows die; the leak rows (b)/(c) stay green. Measured
        // when this row was recorded: six failures — (a) on both drivers,
        // (g), and the three unit key rows — and every (b)/(c) row passed.
        killedBy: '#350 (a) memory',
    },
    {
        label: 'M3 — the draft re-reads id',
        file: MEMBER,
        edits: [[
            DRAFT,
            '        text = JSON.stringify(info === undefined ? { id: fields.id } : { id: fields.id, info })\n',
        ]],
        killedBy: "#350 (c) memory: a getter id answering 'ok' then 500",
    },
    {
        label: 'M4 — the draft re-reads info',
        file: MEMBER,
        edits: [[
            DRAFT,
            '        text = JSON.stringify(info === undefined ? { id } : { id, info: fields.info })\n',
        ]],
        killedBy: '#350 (c) memory: an info getter',
    },
    {
        label: 'M5 — admitted = draft: the filed in-memory copy',
        file: MEMBER,
        edits: [[
            '    const admitted: unknown = JSON.parse(text)\n',
            '    const admitted: unknown = info === undefined ? { id } : { id, info }\n',
        ]],
        killedBy: '#350 (d) refused info: new Date(0)',
    },
    {
        label: "M6 — the join's wire check removed",
        file: MEMBER,
        edits: [
            [
                '    if (!isPresenceMemberWire(admitted)) {\n',
                '    if (false as boolean) {\n',
            ],
            // Without the predicate `admitted` is not narrowed: the two reads
            // below it are cast so the mutant compiles and RUNS.
            [
                '    if (info !== undefined && admitted.info === undefined) {\n',
                '    if (info !== undefined && (admitted as PresenceMember).info === undefined) {\n',
            ],
            [
                '    return freezePresenceMember(admitted)\n}',
                '    return freezePresenceMember(admitted as PresenceMember)\n}',
            ],
        ],
        killedBy: '#350 (d) refused info: null',
    },
    {
        label: 'M7 — the ingest back to a key count',
        file: PROTOCOL,
        edits: [[
            '    if (!Object.keys(value).every(isPresenceMemberKey)) return false\n',
            '    if (Object.keys(value).length > 2) return false\n',
        ]],
        killedBy: '#350 (e) frame ingest',
    },
    {
        label: "M8 — the roster read's info check removed",
        file: REDIS,
        edits: [[
            '            if (!isPresenceMemberInfoValue(member.info)) {\n',
            '            if (false as boolean) {\n',
        ]],
        killedBy: '#350 (f) roster read',
    },
    {
        label: 'M9 — the shape error echoes a value',
        file: MEMBER,
        edits: [[
            '    if (extraKeys.length > 0) throw new PresenceMemberShapeError({ extraKeys })\n',
            '    if (extraKeys.length > 0) {\n' +
            '        throw new PresenceMemberShapeError({\n' +
            '            extraKeys: extraKeys.map((key) =>\n' +
            '                `${key}=${String((candidate as Record<string, unknown>)[key])}`\n' +
            '            ),\n' +
            '        })\n' +
            '    }\n',
        ]],
        killedBy: '#350 (a) memory',
    },
    {
        label: 'M10 — admission below #checkChannelCaps',
        file: MANAGER,
        edits: [
            [ADMISSION_CALL, RAW_RETURNED],
            [SEAT_CALL, RAW_MEMBER],
            [
                '            connection.identity !== null,\n' +
                '        )\n' +
                '\n' +
                '        // `member` is set',
                '            connection.identity !== null,\n' +
                '        )\n' +
                '        if (member !== undefined) {\n' +
                '            member = admitPresenceMember(member, this.#maxPresenceMemberBytes)\n' +
                '        }\n' +
                '\n' +
                '        // `member` is set',
            ],
        ],
        killedBy: '#350 (g)',
    },
    {
        // The value is appended AFTER the intact message, so every wording
        // assertion still passes and only the no-echo assertion can kill it.
        label: 'M11 — the info-type refusal echoes the parsed value',
        file: MEMBER,
        edits: [[
            '        throw new PresenceMemberShapeError({\n' +
            '            infoType: typeLabel((admitted as { info?: unknown }).info),\n' +
            '        })\n',
            '        const echoed = new PresenceMemberShapeError({\n' +
            '            infoType: typeLabel((admitted as { info?: unknown }).info),\n' +
            '        })\n' +
            '        echoed.message += JSON.stringify((admitted as { info?: unknown }).info)\n' +
            '        throw echoed\n',
        ]],
        killedBy: '#350 (d) refused info: [SENTINEL]',
    },
    {
        label: 'M12 — the serializes-to-nothing refusal echoes the value',
        file: MEMBER,
        edits: [[
            '        throw new PresenceMemberShapeError({ droppedInfoType: typeof info })\n',
            '        const echoed = new PresenceMemberShapeError({ droppedInfoType: typeof info })\n' +
            '        echoed.message += String(info)\n' +
            '        throw echoed\n',
        ]],
        killedBy: '#350 admit: an info that serializes to nothing',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#350 — a presence member is parsed once at admission',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
