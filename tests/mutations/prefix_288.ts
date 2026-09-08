/**
 * @fileoverview #288's mutation battery, as a runnable script.
 *
 * **A mutation table in a comment is a claim; this is the thing that checks
 * it.** The counts in `tests/prefix_anchoring.test.ts`'s header came from a
 * script that lived in a scratch directory, so a reader could not reproduce
 * them and a later change could not invalidate them. The review gate flagged
 * exactly that. It lives in the tree now.
 *
 * Runs under the shared harness (#305), which replaced this file's own runner.
 * That runner read a failure COUNT and printed the failing test names without
 * ever asserting them — printing is not attribution, and a row could be killed
 * by an unrelated control while the control it was written to prove was absent.
 * It also THREW when no summary line appeared, so a mutant that failed to
 * type-check aborted the whole battery mid-run instead of being reported DEAD.
 * The harness adds a green baseline, a per-file lock, and `killedBy`.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/prefix_288.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/prefix_288
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const DRIVER = new URL('../../drivers/redis.ts', import.meta.url)
const SUITE = new URL('../prefix_anchoring.test.ts', import.meta.url)
const SUITES = [SUITE.pathname]

const MUTATIONS: Mutation[] = [
    {
        label: 'separator reverted to the pre-#288 `:`',
        file: DRIVER,
        edits: [[
            'return `${this.prefix}${RESERVED_SEPARATOR_LEAD}event:`',
            'return `${this.prefix}:`',
        ]],
        killedBy: 'EVERY derived name carries the reserved lead-in',
    },
    {
        label: 'control topic given a `:` separator',
        file: DRIVER,
        edits: [[
            'return `${this.prefix}${RESERVED_SEPARATOR_LEAD}control`',
            'return `${this.prefix}:control`',
        ]],
        killedBy: 'EVERY derived name carries the reserved lead-in',
    },
    {
        label: 'presenceKey un-anchored',
        file: DRIVER,
        edits: [[
            'return `${this.prefix}${RESERVED_SEPARATOR_LEAD}presence:${channel}`',
            'return `${this.prefix}:presence:${channel}`',
        ]],
        killedBy: 'EVERY derived name carries the reserved lead-in',
    },
    {
        label: 'instancesKey un-anchored',
        file: DRIVER,
        edits: [[
            'return `${this.prefix}${RESERVED_SEPARATOR_LEAD}instances`',
            'return `${this.prefix}:instances`',
        ]],
        killedBy: 'EVERY derived name carries the reserved lead-in',
    },
    {
        label: 'the `__` refusal disabled',
        file: DRIVER,
        edits: [[
            '    if (prefix.includes(RESERVED_SEPARATOR_LEAD)) {',
            '    if (false && prefix.includes(RESERVED_SEPARATOR_LEAD)) {',
        ]],
        killedBy: 'an unusable prefix is refused, by the RIGHT guard',
    },
    {
        label: 'the guard refuses an UNRELATED sequence',
        file: DRIVER,
        edits: [[
            '    if (prefix.includes(RESERVED_SEPARATOR_LEAD)) {',
            "    if (prefix.includes('::')) {",
        ]],
        killedBy: 'an unusable prefix is refused, by the RIGHT guard',
    },
    {
        label: 'the charset allowlist disabled',
        file: DRIVER,
        edits: [[
            '    if (!PREFIX_RE.test(prefix)) {',
            '    if (false && !PREFIX_RE.test(prefix)) {',
        ]],
        killedBy: 'an unusable prefix is refused, by the RIGHT guard',
    },
    {
        label: 'the glob-character loop disabled',
        file: DRIVER,
        edits: [[
            '        if (prefix.includes(char)) {',
            '        if (false && prefix.includes(char)) {',
        ]],
        killedBy: 'an unusable prefix is refused, by the RIGHT guard',
    },
    {
        label: 'the length cap loosened to 999',
        file: DRIVER,
        edits: [[
            '/^[A-Za-z0-9:._-]{1,64}$/',
            '/^[A-Za-z0-9:._-]{1,999}$/',
        ]],
        killedBy: 'an unusable prefix is refused, by the RIGHT guard',
    },
    {
        label: "onMessage's shape-mismatch drop removed",
        file: DRIVER,
        edits: [[
            '            if (!topic.startsWith(marker)) {',
            '            if (false) {',
        ]],
        killedBy: 'a topic that does not carry the event marker is DROPPED',
    },
    {
        label: 'the strip changed to `split(marker)[1]`',
        file: DRIVER,
        edits: [[
            '            const channel = topic.slice(marker.length)',
            "            const channel = topic.split(marker)[1] ?? ''",
        ]],
        killedBy:
            'a channel containing the reserved separator round-trips whole',
    },
    {
        label: 'the strip reverted to the pre-#288 offset',
        file: DRIVER,
        edits: [[
            '            const channel = topic.slice(marker.length)',
            '            const channel = topic.slice(this.prefix.length + 1)',
        ]],
        killedBy:
            'a channel containing the reserved separator round-trips whole',
    },
    {
        label: 'the strip changed to `replace(marker, ...)`',
        file: DRIVER,
        edits: [[
            '            const channel = topic.slice(marker.length)',
            "            const channel = topic.replace(marker, '')",
        ]],
        killedBy:
            'a channel containing the reserved separator round-trips whole',
        expectSurvival:
            'The `startsWith` guard immediately above has already established ' +
            'that the marker occurs at position 0, so "remove the first " ' +
            'occurrence" and "drop that many characters" cannot disagree. ' +
            'Recorded rather than deleted: an earlier comment claimed this ' +
            'mutation WOULD corrupt the channel, and the battery is what ' +
            'disproved it.',
    },
    {
        label: 'globMatches neutered',
        file: SUITE,
        edits: [[
            "    return new RegExp(`^${out}$`, 's').test(topic)",
            '    return false',
        ]],
        killedBy: 'globMatches models the broker well enough to be trusted',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery('#288 — prefix anchoring', SUITES, MUTATIONS) > 0
            ? 1
            : 0,
    )
}
