/**
 * @fileoverview #280's mutation battery — the fake audited against real Redis.
 *
 * **This battery mutates a TEST DOUBLE, which is the point.** #276 shipped a
 * security fix that was wrong twice with a green suite, both times because this
 * double modelled a command incorrectly in exactly the spot that mattered. A
 * double is production code for every test that depends on it, and nothing was
 * checking it.
 *
 * Every row restores a divergence actually present on `main` before this
 * branch — a silently-ignored option token, a second key argument dropped, a
 * store the delete never reached, a second clock. None is hypothetical.
 *
 * Runs under `@lockness/contract`'s shared harness: green baseline first, an
 * atomic per-file lock, anchors matched exactly once, and every kill attributed
 * to the test that claims it.
 *
 * **What this battery does NOT show.** `SUITES` is the conformance file alone,
 * which is the right choice for attribution — a row killed by an unrelated
 * consumer suite would prove nothing about the conformance test that claims it.
 * The consequence is worth saying out loud: these rows prove the conformance
 * tests pin the fake, not that the fake's correctness is load-bearing for the
 * thirteen suites that import it. That second question is #285's.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/fake_redis_280.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/fake_redis_280
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const FAKE = new URL('../fake_redis.ts', import.meta.url)
const SUITES = [
    new URL('../fake_redis_conformance.test.ts', import.meta.url).pathname,
]

const MUTATIONS: Mutation[] = [
    {
        label: 'SET accepts an unmodelled option again',
        file: FAKE,
        edits: [["if (opts[i].toUpperCase() !== 'EX') {", 'if (false) {']],
        killedBy: 'SET rejects an option it does not model',
    },
    {
        label: 'SET stops checking its EX argument',
        file: FAKE,
        edits: [[
            "                    if (\n                        raw === undefined || raw === '' ||\n                        !Number.isFinite(seconds)\n                    ) {",
            '                    if (false) {',
        ]],
        killedBy: 'SET refuses an EX with a missing or unparseable value',
    },
    {
        label: 'a plain SET stops clearing the TTL',
        file: FAKE,
        edits: [[
            'if (expireAt === undefined) this.#keyExpiry.delete(key)\n                else this.#keyExpiry.set(key, expireAt)',
            'if (expireAt !== undefined) this.#keyExpiry.set(key, expireAt)',
        ]],
        killedBy: 'a plain SET clears an existing TTL',
    },
    {
        label: 'EXISTS stops seeing sets',
        file: FAKE,
        edits: [[
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
            'return this.#strings.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
        ]],
        killedBy: 'EXISTS takes many keys and sees every type',
    },
    {
        label: 'EXISTS stops seeing hashes',
        file: FAKE,
        edits: [[
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#zsets.has(key)',
        ]],
        killedBy: 'EXISTS takes many keys and sees every type',
    },
    {
        label: 'EXISTS stops seeing sorted sets',
        file: FAKE,
        edits: [[
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key)',
        ]],
        killedBy: 'EXISTS takes many keys and sees every type',
    },
    {
        label: 'EXISTS stops honouring the key-level TTL',
        file: FAKE,
        edits: [[
            'if (this.#expired(key)) {\n            this.#dropKey(key)\n            return false\n        }\n        return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
            'return this.#strings.has(key) || this.#sets.has(key) ||\n            this.#hashes.has(key) || this.#zsets.has(key)',
        ]],
        killedBy: 'key-level TTL is authoritative for every type',
    },
    {
        label: 'DEL takes only the first key again',
        file: FAKE,
        edits: [[
            'for (const key of rest) if (this.#dropKey(key)) removed++',
            'for (const key of rest.slice(0, 1)) if (this.#dropKey(key)) removed++',
        ]],
        killedBy: 'DEL takes many keys',
    },
    {
        label: 'DEL stops reaching sorted sets again',
        file: FAKE,
        edits: [[
            '        this.#zsets.delete(key)\n        this.#keyExpiry.delete(key)\n        return existed',
            '        this.#keyExpiry.delete(key)\n        return existed',
        ]],
        killedBy: 'DEL removes a sorted set',
    },
    {
        label: "DEL stops clearing the key's TTL, so a re-create inherits it",
        file: FAKE,
        edits: [[
            '        this.#zsets.delete(key)\n        this.#keyExpiry.delete(key)\n        return existed',
            '        this.#zsets.delete(key)\n        return existed',
        ]],
        killedBy: 'DEL clears the key TTL',
    },
    {
        label: 'an emptied collection lingers as an existing key',
        file: FAKE,
        edits: [[
            'for (const m of members) if (set?.delete(m)) removed++\n                this.#dropIfEmpty(key)',
            'for (const m of members) if (set?.delete(m)) removed++',
        ]],
        killedBy: 'an emptied collection stops existing',
    },
    {
        label: 'EXPIRE arms a key that does not exist',
        file: FAKE,
        edits: [[
            "if (!this.#keyExists(key)) return { type: 'integer', value: 0 }",
            "if (false) return { type: 'integer', value: 0 }",
        ]],
        killedBy: 'EXPIRE on a missing key changes nothing',
    },
    {
        label: 'ZADD writes only the first score/member pair again',
        file: FAKE,
        edits: [[
            'for (let i = 0; i < pairs.length; i += 2) {\n                    const score = Number(pairs[i])',
            'for (let i = 0; i < 2; i += 2) {\n                    const score = Number(pairs[i])',
        ]],
        killedBy: 'ZADD writes every score/member pair',
    },
    {
        label: 'ZADD scans the whole tail for flags again',
        file: FAKE,
        edits: [[
            "if (!['GT', 'LT', 'NX', 'XX', 'CH'].includes(token)) break",
            "if (!['GT', 'LT', 'NX', 'XX', 'CH'].includes(token)) { cursor++; continue }",
        ]],
        killedBy: 'ZADD reads its flags by position',
    },
    {
        label: 'ZADD half-applies an odd argument list again',
        file: FAKE,
        edits: [[
            'const pairs = tail.slice(cursor)\n                if (pairs.length === 0 || pairs.length % 2 !== 0) {',
            'const pairs = tail.slice(cursor)\n                if (pairs.length === 0) {',
        ]],
        killedBy: 'ZADD writes every score/member pair',
    },
    {
        label: 'HSET writes only the first pair again',
        file: FAKE,
        edits: [[
            'for (let i = 0; i < pairs.length; i += 2) {\n                    if (!h.has(pairs[i])) added++',
            'for (let i = 0; i < 2; i += 2) {\n                    if (!h.has(pairs[i])) added++',
        ]],
        killedBy: 'HSET and HDEL take many field/value pairs',
    },
    {
        label: 'HSET half-applies an odd argument list again',
        file: FAKE,
        edits: [[
            'const [key, ...pairs] = rest\n                if (pairs.length === 0 || pairs.length % 2 !== 0) {',
            'const [key, ...pairs] = rest\n                if (pairs.length === 0) {',
        ]],
        killedBy: 'an odd HSET argument list is refused',
    },
    {
        label: 'HDEL removes only the first field again',
        file: FAKE,
        edits: [[
            'for (const field of fields) if (h?.delete(field)) removed++',
            'for (const field of fields.slice(0, 1)) if (h?.delete(field)) removed++',
        ]],
        killedBy: 'HSET and HDEL take many field/value pairs',
    },
    {
        label: 'ZRANGEBYSCORE ignores its options again',
        file: FAKE,
        edits: [['if (opts.length > 0) {', 'if (false) {']],
        killedBy: 'ZRANGEBYSCORE rejects an option it does not model',
    },
    {
        label: 'ZRANGEBYSCORE breaks ties by insertion order again',
        file: FAKE,
        edits: [['a[1] - b[1] || (a[0] < b[0] ? -1 : 1)', 'a[1] - b[1]']],
        killedBy: 'ZRANGEBYSCORE breaks a score tie lexicographically',
    },
    {
        label: 'SMEMBERS accepts extra arguments again',
        file: FAKE,
        edits: [[
            'if (rest.length !== 1) {\n                    this.#reject(\n                        `FakeRedis: SMEMBERS takes one key',
            'if (false) {\n                    this.#reject(\n                        `FakeRedis: SMEMBERS takes one key',
        ]],
        killedBy: 'the arms with no options refuse extra arguments',
    },
    {
        label:
            'a refusal stops being recorded, so a swallowed throw goes silent',
        file: FAKE,
        edits: [[
            '        this.#rejections.push(message)\n        throw new Error(message)',
            '        throw new Error(message)',
        ]],
        killedBy: 'a rejection survives a driver that swallows it',
    },
]

Deno.exit(
    await runBattery(
        '#280 mutation battery — the fake Redis against real semantics',
        SUITES,
        MUTATIONS,
    ),
)
