/**
 * @fileoverview #282 — every name the Redis driver derives from `prefix` is
 * anchored under it, proved at the port boundary.
 *
 * **Two mechanisms were designed for this and both were shown blind before a
 * line was written.** `SCAN MATCH ${prefix}*` returns only keys already under
 * the prefix, so it cannot observe an unanchored one. A whole-keyspace diff
 * observes a name only if that name *becomes a key* — and five of the driver's
 * ten never do: `topic` is a `PUBLISH` argument, `controlTopic` a
 * `PSUBSCRIBE`/`PUBLISH` argument, `${prefix}:*` is a subscription, and the two
 * legacy names are documented at `drivers/redis.ts:889` as "read, never reaped
 * and never written".
 *
 * The port sees all ten. Both dependencies are constructor-injected
 * (`drivers/redis.ts:450-453`), so a recorder over them needs no broker.
 *
 * **Which strings count as prefix-derived is decided differentially**, not by a
 * table of key positions per command: the same exercise runs under two distinct
 * prefixes, and every captured string that differs between the runs is derived
 * from the prefix by construction. There is no model of Redis here to drift.
 *
 * ## Mutations run against this file (#282 FR-007)
 *
 * **Twenty-three, all red.** The count is stated because it was twice wrong:
 * this table said sixteen after the count had grown, and the merge report said
 * twenty-four. A mutation table is evidence, so a number in it that nobody can
 * reproduce is worse than no number — the script that produces this figure is
 * the one below, and it prints its own total.
 *
 * | Mutation | Caught by |
 * | :--- | :--- |
 * | `isAnchored` weakened to `startsWith` | `FR-003` |
 * | Each of the seven members un-anchored, in turn (7) | `SC-001` |
 * | The inline subscribe pattern un-anchored | `SC-001`, `FR-001` |
 * | A `psubscribe` site dropped from the recorder | `FR-001` |
 * | A member removed from the pinned roster | `SC-004` |
 * | The glob guard removed entirely | `SC-005` |
 * | Each of the five guard characters dropped, in turn (5) | `SC-005` |
 * | The guard's `includes` weakened to `startsWith` | `SC-005` |
 * | `globMatches` neutered to `return true` | `globMatches models the broker` |
 * | `globMatches`'s trailing-escape bound flipped | `globMatches models the broker` |
 * | `topic` dropped from the exercise, `presenceKey` duplicated | `SC-001` |
 *
 * **Five of these were GREEN before a reviewer caught them**, and each was the
 * same class — an observation that cannot see its own violation:
 *
 * - `ownedKey` and `aliveKey` embed a per-driver `crypto.randomUUID()`, so the
 *   differential's normalise-match dropped them: `SC-001` covered eight of ten
 *   while appearing to cover all.
 * - `globMatches` had never executed, because its only caller was an ignored
 *   test — so both of its mutations were green.
 * - The bad-prefix fixture was `'app[1]'`, carrying `[` **and** `]`, so dropping
 *   `[` from the guard still threw on `]`.
 * - The last row was green against a pinned COUNT: drop one name, add one shape,
 *   the total is unchanged. It is red only against the exact set.
 *
 * ## Mutations run for #288 (the nested-prefix fix)
 *
 * **Fourteen run, zero survivors** (thirteen killed, one equivalent). The
 * battery is **in the tree and runnable** — `tests/mutations/prefix_288.ts`,
 * `deno run -A` it, and its exit code is the survivor count. It lived in a
 * scratch directory until the review gate pointed out that a table nobody can
 * reproduce is a claim, not evidence. It asserts each anchor matches exactly
 * once, writes, re-reads to prove the file actually changed, and only then
 * reads the suite summary.
 *
 * | Mutation | Killed by |
 * | :--- | :--- |
 * | separator reverted to the pre-#288 `:` | `FR-001`, `FR-002`, `FR-006`, `SC-001`, `SC-002` |
 * | control topic given a `:` separator | `FR-001`, `FR-006`, `SC-001` |
 * | `presenceKey` un-anchored | `FR-006`, `SC-001` |
 * | `instancesKey` un-anchored | `FR-006`, `SC-001` |
 * | the `__` refusal disabled | `FR-004`, `SC-005` |
 * | the charset allowlist disabled | `SC-005` |
 * | the glob-character loop disabled | `SC-005` |
 * | the length cap loosened to 999 | `SC-005` |
 * | the guard refuses an UNRELATED sequence | `FR-004`, `SC-005` |
 * | `onMessage`'s shape-mismatch drop removed | `FR-002` |
 * | the strip changed to `split(marker)[1]` | `FR-002` |
 * | the strip reverted to the pre-#288 offset | `FR-002` |
 * | `globMatches` neutered | `SC-002`, `globMatches models the broker` |
 * | the strip changed to `replace(...)` | **EQUIVALENT — see below** |
 * | a name added without the lead-in | `FR-004 source` |
 * | a family nested under the event pattern | `FR-004` |
 * | a key un-anchored, colliding across prefixes | `FR-012` |
 * | a negative ingest test's topic left stale | `FakeRedisBus` refuses it |
 *
 * **`('app','app_')` earns its place by argument, not by measurement**, and
 * that is stated rather than dressed up: no product mutation distinguishes it
 * from `('app','app:eu')`. It pins the proof's boundary step — those two
 * patterns agree for `|p|+2` characters and diverge at one offset — and a row
 * claiming a kill it cannot demonstrate would be worse than no row.
 *
 * **Three readings that were wrong before they were right**, kept because each
 * is a way to believe a result you did not measure:
 *
 * - `the __ refusal disabled` first read GREEN, and it was: `SC-005` asserted
 *   only that a bad prefix *threw*, and the new charset allowlist threw for it.
 *   The fix was to pin the expected MESSAGE per fixture, so each one tests the
 *   guard it was written for.
 * - `strip -> replace` reads GREEN and is an **equivalent mutant**. After the
 *   `startsWith` check the marker's first occurrence is position 0, so "remove
 *   the first occurrence" and "drop that many characters" cannot disagree. An
 *   earlier comment claimed `replace` would corrupt the channel. It does not.
 * - The first `split` fixture used a channel of `__event:x`, which does not
 *   contain the WHOLE marker, so `slice`, `replace` and `split` all agreed and
 *   the row proved nothing. The channel has to repeat the marker in full.
 *
 * ## Mutations run for #315 (binding by identity, not by glob shape)
 *
 * **Two run, both killed** — `tests/mutations/subscription_identity_315.ts`,
 * whose exit code is the survivor count. The number comes from that script, not
 * from a hand count: the two figures above were each wrong once.
 *
 * | Mutation | Killed by |
 * | :--- | :--- |
 * | the event subscription opened on the control topic | `SC-002` |
 * | a THIRD subscription, in neither family | `SC-002` |
 *
 * **This is the sixth entry in the catalogue above, and the only one caught
 * before it cost anything.** Four sites told the two subscriptions apart with
 * `pattern.endsWith('*')` — a fact about neither of them. Nothing was vacuous
 * on `main`: both patterns had their expected shapes and the suite was checking
 * what it claimed. But an event subscription that stopped ending in a star made
 * `filter(endsWith('*'))` return `[]`, so `SC-002`'s US2 loop ran zero times
 * and the suite stayed green having stopped checking that routing alone can
 * never hand a control frame to `onMessage`, where `#verifyAndDecode` is not.
 * `assert(ownControl !== undefined)` read like a guard against that and was
 * not one: it asserted that *some* pattern lacks a trailing star, which the
 * wrong binding satisfies.
 *
 * Both rows also break `FR-001`'s exact-set pin, so a bare kill would say
 * nothing. The evidence is the ATTRIBUTION: each row names `SC-002`, and run
 * against the pre-#315 file the same two rows report MISATTRIBUTED — `SC-002`
 * passed in both. That one test moving from pass to fail is the whole change.
 *
 * @module @lockness/realtime/tests/prefix_anchoring
 */

import { assert, assertEquals, assertRejects } from '@std/assert'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import { isAnchored, recordingPorts } from './recording_ports.ts'

/**
 * The members of `RedisBroadcastDriver` whose body interpolates `this.prefix`,
 * plus the inline pattern.
 *
 * **Names, not a count.** A count moves when a harmless refactor reads the
 * prefix into a local, and fails to move when a new getter does the same — the
 * exact slip-past this list exists to prevent. `SC-004` diffs this against the
 * source, so a getter added later fails by name.
 */
const PREFIX_MEMBERS: readonly string[] = [
    'topic',
    // The PATTERN-context sibling of `topic` (#295). Two members, same bytes
    // today, because `PUBLISH` is a literal context and `PSUBSCRIBE` a pattern
    // one — and it is driven by `exercise`'s `watchChannel` call, not merely
    // listed here.
    'eventPattern',
    'controlTopic',
    'presenceKey',
    'ownedKey',
    'aliveKey',
    'instancesKey',
    'revocationIndexKey',
]

/**
 * Members that interpolate the prefix but derive **no name of their own** —
 * fragments other members build on.
 *
 * `eventTopicPrefix` is everything an event topic has before the channel
 * (#288). It reaches no port by itself: what crosses is `topic()`'s output and
 * `onMessage`'s pattern, both of which are already pinned. Listing it here
 * rather than in {@link PREFIX_MEMBERS} keeps `FR-006`'s exact-string matching
 * honest — a fragment has no exact string to look for, and giving it one would
 * mean matching by substring, which is the discipline `FR-006`'s own comment
 * exists to forbid — `alpha__owned:` is contained in `alpha__owned:<id>`, so a
 * member that was never driven would still "be found" inside another name.
 *
 * **It is a declared list, not an exclusion rule.** A getter added later that
 * is quietly a fragment fails `SC-004` by name until someone decides which of
 * the two lists it belongs in — which is the whole point of pinning either.
 */
const PREFIX_FRAGMENTS: readonly string[] = ['eventTopicPrefix']

/**
 * Pinned members that build on a {@link PREFIX_FRAGMENTS} entry rather than
 * interpolating `prefix` themselves.
 *
 * `FR-004 source` counts DIRECT interpolation sites, so a member that reaches
 * the prefix through a fragment contributes no site of its own. Declared rather
 * than inferred: the alternative is a count that quietly stops matching, which
 * is how that assertion would go from a check to a formality.
 *
 * `topic` and `eventPattern` are the pair — a `PUBLISH` argument and a
 * `PSUBSCRIBE` pattern, same bytes today, deliberately two builders (#295).
 */
const FRAGMENT_DERIVED: readonly string[] = ['topic', 'eventPattern']

/**
 * Canned replies that let `exercise` reach every read path without a store.
 *
 * They are `RespReply`-shaped, not raw JS values — the driver narrows every
 * reply through `asArray`/`asBulk`/`asInteger`, and a plain array is rejected by
 * all three. A first draft returned raw arrays, and a name the exercise was
 * supposed to derive was simply never reached; `FR-006` is the test that said
 * so, which is the whole reason it asserts reach separately from anchoring.
 */
const CANNED = {
    HGETALL: { type: 'array', value: [] },
    ZRANGEBYSCORE: { type: 'array', value: [] },
    EVAL: { type: 'array', value: [] },
    TIME: {
        type: 'array',
        value: [
            { type: 'bulk', value: '1757000000' },
            { type: 'bulk', value: '0' },
        ],
    },
}

/**
 * Drive every prefix-deriving path in the driver and return what crossed the
 * ports.
 *
 * No timer and no `FakeTime`: `addMember` awaits `#ensureSweepStarted()`, which
 * awaits `#heartbeat()` before installing either interval
 * (`drivers/redis.ts:733`, `:1116`), so the instance and liveness keys are
 * created synchronously on the first join.
 */
async function exercise(prefix: string) {
    const { command, subscriber, recording } = recordingPorts(CANNED)
    const driver = new RedisBroadcastDriver(command, subscriber, {
        prefix,
        control: { secret: 'a-secret-long-enough-for-the-32-byte-floor!!' },
    })
    try {
        driver.onMessage(() => {})
        // DRIVES `eventPattern` (#295). Under per-channel subscribe `onMessage`
        // registers the decoder and subscribes nothing; the event topic reaches
        // the wire through `watchChannel`, so an exercise that stopped at
        // `onMessage` would leave the suite asserting anchoring on a pattern
        // the driver no longer issues.
        await driver.watchChannel('room')
        driver.onControl(() => {})
        driver.onRevocationReconcile(() => {})
        await driver.publish({ channel: 'room', event: 'e', data: {} })
        await driver.publishControl({ kind: 'evict', target: 'conn-1' })
        // addMember reaches instancesKey and aliveKey too: it awaits
        // #ensureSweepStarted() -> #heartbeat() before any interval exists.
        await driver.addMember('presence-room', { id: 'u1', info: {} })
        await driver.listMembers('presence-room')
        await driver.removeMember('presence-room', 'u1')
        await driver.markRevoked('conn-1')
        await driver.listRevoked()
    } finally {
        await driver.close()
    }
    return recording
}

Deno.test('FR-001: BOTH psubscribe sites are captured, by count', async () => {
    // A recorder that catches only the events subscription would satisfy every
    // "is it anchored" assertion below while missing `controlTopic` — the one
    // name US3 exists for, and the only one that uses no `:` separator.
    const recording = await exercise('app')
    assertEquals(
        recording.subscriptions.length,
        2,
        'the driver subscribes twice — one WATCHED CHANNEL and control. ' +
            'Capturing one and asserting on it proves half.',
    )
    const patterns = recording.subscriptions.map((s) => s.pattern).sort()
    // `app__event:room`, not `app__event:*`. Under #295 the events side is one
    // exact topic per hosted channel rather than one glob for the whole prefix,
    // which makes this suite stronger rather than weaker: it now anchors the
    // string the broker actually matches against, and the channel name is the
    // half a nested prefix could once reach into.
    assertEquals(patterns, ['app__control', 'app__event:room'])
})

Deno.test('FR-003: anchoring is prefix PLUS a separator, not startsWith', () => {
    assert(isAnchored('app', 'app'), 'the prefix itself')
    assert(isAnchored('app:room', 'app'), 'the `:` separator')
    assert(isAnchored('app__control', 'app'), 'the `__` separator, no colon')

    // The case that makes this predicate worth having. `appx` "begins with"
    // `app`, and a `startsWith` definition would call it contained.
    assert(
        !isAnchored('appx', 'app'),
        'appx continues the prefix without a separator and is NOT anchored',
    )
    assert(
        !isAnchored('other:app:room', 'app'),
        'a prefix appearing anywhere but the start is not an anchor',
    )
    assert(!isAnchored('ap', 'app'), 'a truncation is not an anchor')
})

Deno.test('SC-001: every prefix-derived name is anchored', async () => {
    // The differential: run the same exercise under two prefixes, and take the
    // strings that CHANGED. Those are prefix-derived by construction — no
    // per-command key-position table, and nothing to drift from Redis.
    const alpha = await exercise('alpha')
    const beta = await exercise('beta')

    // A raw "differs between runs" filter OVER-captures: a signed control frame
    // carries a fresh `origin`, `ts`, `nonce` and `mac` every time.
    //
    // But normalising only the PREFIX under-captures, and that is worse. Two of
    // the ten names embed `instanceId` — a `crypto.randomUUID()` fixed per
    // driver (`drivers/redis.ts:470`) — so `alpha:owned:<uuid-A>` and
    // `beta:owned:<uuid-B>` never match however the prefix is normalised, and
    // both were silently dropped from the assertion. The review gate proved it:
    // an unanchored `ownedKey` left this whole suite green.
    //
    // That is this plan's own invariant 2 — "the observation must be capable of
    // seeing its violation" — failing for the THIRD time on this branch, after
    // the namespaced scan and the keyspace diff. So the count is now pinned:
    // under-capture cannot hide again.
    const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g
    const normalise = (value: string, prefix: string) =>
        value.replaceAll(prefix, '\u0000P\u0000').replace(
            UUID,
            '\u0000ID\u0000',
        )
    const betaNormalised = new Set(
        beta.strings().map((value) => normalise(value, 'beta')),
    )
    const derived = alpha.strings().filter((value) =>
        value.includes('alpha') &&
        betaNormalised.has(normalise(value, 'alpha'))
    )

    // The exact SET, not `> 0` and not a count.
    //
    // `> 0` let two names vanish. A pinned COUNT then let them vanish in pairs:
    // dropping `topic` from the exercise while a second `listMembers` call adds
    // one more `presenceKey` shape keeps the total at ten, and the assertion
    // stays green with `topic` asserted by nothing. Verified — that mutation was
    // GREEN against the count and is RED against this set.
    //
    // Three instruments, three blind spots, same class each time: an
    // observation that cannot see its own violation. The set is the first one
    // that can.
    const distinct = [...new Set(derived.map((d) => d.replace(UUID, '<id>')))]
    assertEquals(
        distinct.sort(),
        [
            // ALL of them behind `__`, with no exception left. #288 anchored
            // five; #295 split the event pattern out; #278 removed the last
            // two — `alpha:revoked` and `alpha:revoked:<id>`, which were
            // unanchored because they read what a pre-#276 instance wrote at
            // those exact strings. Deleting the reader deleted the exception,
            // which is why `FR-004 source` below has no escape hatch any more.
            'alpha__alive:<id>',
            'alpha__control',
            // `alpha__event:*` USED TO BE HERE and is gone (#295): the events
            // side is now one exact topic per hosted channel, so the only
            // event name the driver derives is the channel's own. Removing it
            // narrows what this set covers by exactly one glob and widens what
            // the suite proves — an exact topic is the string the broker
            // matches against, and the channel half is the part a nested prefix
            // could once reach into.
            'alpha__event:room',
            'alpha__instances',
            'alpha__owned:<id>',
            'alpha__presence:presence-room',
            'alpha__revocations',
        ],
        'the differential captured a different set of derived names than the ' +
            'pinned members plus the inline pattern. A name missing here is a ' +
            'name this test says nothing about; a name added here is one the ' +
            'roster does not know about.',
    )
    for (const name of derived) {
        assert(
            isAnchored(name, 'alpha'),
            `"${name}" crossed the port unanchored. On a shared broker that is ` +
                'residue #273 teardown cannot reap, or a subscription reading ' +
                "another deployment's traffic.",
        )
    }
})

Deno.test('SC-004: the pinned roster matches the driver source', async () => {
    // Completeness is a SOURCE fact and exercise is a RUNTIME fact; one
    // criterion cannot carry both. This is the source half.
    const source = await Deno.readTextFile(
        new URL('../drivers/redis.ts', import.meta.url),
    )
    // Members whose body interpolates the prefix — the shape, not a count.
    // Both spellings. Since #288 a member may derive its name from
    // `this.eventTopicPrefix` instead of `this.prefix` directly — `topic()`
    // does — and a regex that knows only the second silently drops it from the
    // roster while the assertion goes green on a shorter list.
    const found = [
        ...source.matchAll(
            /(?:private |protected )?(\w+)\([^)]*\)(?::[^{]+)?\{\s*return `\$\{this\.(?:prefix|eventTopicPrefix)\}/g,
        ),
    ].map((m) => m[1]).filter((name) => !PREFIX_FRAGMENTS.includes(name)).sort()

    assertEquals(
        found,
        [...PREFIX_MEMBERS].sort(),
        'the driver gained or lost a prefix-deriving member. Update ' +
            'PREFIX_MEMBERS **and** confirm the exercise drives the new one — ' +
            'a roster that is edited to go green is a roster that tracks nothing.',
    )
})

Deno.test('FR-006: every pinned member is actually driven by the exercise', async () => {
    // A name can exist, be anchored, and never be reached. Then the containment
    // claim covers a smaller set than it appears to.
    const recording = await exercise('alpha')
    // EXACT strings, not a joined blob. A substring search over the join looks
    // right and is not: `alpha__owned:` is contained in `alpha__owned:<id>`,
    // and `alpha__alive:` in `alpha__alive:<id>`, so a member that was never
    // driven would still "be found" inside another name. This guard is the one
    // that caught the first recorder bug; it would not have caught a second.
    const seen = new Set(recording.strings())
    const seenList = [...seen]

    const shapes: Record<string, string> = {
        topic: 'alpha__event:room',
        controlTopic: 'alpha__control',
        presenceKey: 'alpha__presence:presence-room',
        ownedKey: 'alpha__owned:',
        aliveKey: 'alpha__alive:',
        instancesKey: 'alpha__instances',
        revocationIndexKey: 'alpha__revocations',
    }
    // `eventPattern` is checked SEPARATELY, and the reason is the point of
    // splitting it from `topic` at all: the two produce the same bytes today,
    // so a string-blob search cannot tell which builder made the hit and would
    // report `eventPattern` as driven even if `watchChannel` never ran. The
    // SUBSCRIPTION list can tell — that string reaches it only through a watch.
    assert(
        recording.subscriptions.some((sub) =>
            sub.pattern === 'alpha__event:room'
        ),
        'eventPattern was not driven: no subscription carries the channel ' +
            'topic, so the exercise never reached watchChannel',
    )
    assertEquals(
        [...Object.keys(shapes), 'eventPattern'].sort(),
        [...PREFIX_MEMBERS].sort(),
        'every pinned member has a shape to look for',
    )
    for (const [member, shape] of Object.entries(shapes)) {
        // A name with a variable tail (`owned:<uuid>`) is matched by prefix
        // against a WHOLE captured string; a fixed name must be present whole.
        const hit = shape.endsWith(':')
            ? seenList.some((value) => value.startsWith(shape))
            : seen.has(shape)
        assert(
            hit,
            `${member} was never driven — the exercise does not reach it, so ` +
                'SC-001 says nothing about it',
        )
    }
})

/**
 * Redis glob matching, for the shapes this driver produces.
 *
 * Hand-rolled because the assertion below is about what a broker WOULD deliver,
 * and no broker is involved. It handles `*` (any run, `:` included), `?` (one
 * character) and `\` (escape) — an earlier version let `?` through unescaped, so
 * it reached the regex as a quantifier and meant something else entirely, and
 * inverted `\`. It does not model `[…]` classes; the driver emits none, and
 * `assertUsablePrefix` refuses a prefix that could introduce one.
 */
function globMatches(pattern: string, topic: string): boolean {
    let out = ''
    for (let i = 0; i < pattern.length; i++) {
        const c = pattern[i]
        if (c === '\\' && i + 1 < pattern.length) {
            out += pattern[++i].replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        } else if (c === '*') out += '.*'
        else if (c === '?') out += '.'
        else out += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    }
    // The `s` flag matters, and its absence errs in the dangerous direction.
    // `.` does not match a line terminator by default, so `*` would model Redis
    // as NOT spanning a channel containing `\n` — and since SC-002 asserts an
    // EMPTY reach, a model that under-matches produces false PASSES. Redis's
    // `stringmatchlen` is byte-wise and has no such exception. A channel may
    // not contain `\n` today (NAME_RE forbids it), but the topic side of this
    // model is not bounded by NAME_RE and a broker can deliver anything.
    return new RegExp(`^${out}$`, 's').test(topic)
}

Deno.test('globMatches models the broker well enough to be trusted', () => {
    // This helper is #288's evidence. If it is wrong, the P0 is misfiled — which
    // is worse than not filing it. It ran nowhere until the review gate pointed
    // out that its only caller was an ignored test.
    assert(globMatches('app:*', 'app:room'))
    assert(globMatches('app:*', 'app:eu:orders'), '`*` spans `:` — the leak')
    assert(!globMatches('app:*', 'other:room'))
    assert(
        globMatches('app?x', 'appzx'),
        '`?` is one character, not a quantifier',
    )
    assert(!globMatches('app?x', 'appx'), '`?` requires a character')
    assert(globMatches('a\\*b', 'a*b'), 'an escaped star is a literal star')
    assert(!globMatches('a\\*b', 'azzb'), 'and matches nothing else')
    assert(!globMatches('app__control', 'app:eu__control'))
    // The trailing-escape branch, which nothing reached: flipping its bound to
    // `<=` left the suite green even though the branch is correct.
    assert(globMatches('a\\', 'a\\'), 'a trailing backslash is a literal')
})

/**
 * The prefix pairs SC-002 quantifies over.
 *
 * **Both directions of each pair.** The claim is "for every pair of prefixes
 * the driver accepts", and it was once tested with exactly one pair —
 * `('app','app:eu')` — the pair the leak was reported with rather than the pair
 * that could still break the fix.
 *
 * **`('app','app_')` USED to be here, and it is gone because the pair can no
 * longer be constructed** (#278). It was the only near-miss in the isolation
 * proof: the two event patterns are `app__event:*` and `app___event:*`, which
 * agree for the first `|p|+2` characters and diverge at one offset, and SC-002
 * proved they do not reach each other.
 *
 * That proof was about the driver's own PATTERNS and it was correct. What it
 * did not cover is the ACL: the grant this project documents for `app` is
 * `~app__*`, and `app_`'s names begin `app___`, which that glob matches. So the
 * two never collided and never cross-subscribed, and the `app` credential could
 * still read every key the `app_` deployment wrote. `assertUsablePrefix` now
 * refuses a trailing `_`, which closes it structurally.
 *
 * **Record the loss, not just the fix.** This table is now the easy half of the
 * proof — every remaining pair differs by more than a separator run. The hard
 * half moved to `SC-005`, where the refusal is asserted directly: an
 * unconstructible configuration is a stronger guarantee than a constructible
 * one shown not to reach, but it is proven somewhere else, by a different kind
 * of test, and a reader of this table should be told where.
 */
const ISOLATION_PAIRS: readonly (readonly [string, string])[] = [
    ['app', 'app:eu'],
    ['app:eu', 'app'],
]

/**
 * Every topic a deployment at `prefix` publishes, derived from what it
 * SUBSCRIBES to rather than from a literal.
 *
 * Replacing the `*` in a subscription yields a topic that deployment really
 * publishes: the event pattern becomes a channel topic, and the control
 * "pattern" carries no `*` so it is already a topic. Deriving both from the
 * recording is what keeps this test honest when the wire shape changes — a
 * literal here would have to be edited to go green, which is the edit that
 * makes a wire-format test stop testing the wire format.
 */
function topicsOf(recording: { subscriptions: { pattern: string }[] }) {
    return recording.subscriptions.map((s) => s.pattern.replace('*', 'orders'))
}

/**
 * Split a recording's subscriptions into the control one and the event ones,
 * **by identity** — never by glob shape (#315).
 *
 * The driver opens exactly two: a control topic (`${prefix}__control`,
 * glob-free) and an event glob (`${prefix}__event:*`). Four sites in this file
 * told them apart with `pattern.endsWith('*')`, which separates them *today*
 * and is a property of neither. If the event subscription ever stops ending in
 * a star, `filter(endsWith('*'))` returns `[]`, the US2 loop below never runs,
 * and the suite goes green **having stopped checking the thing it exists to
 * check** — that routing alone can never hand a control frame to `onMessage`,
 * where `#verifyAndDecode` is not.
 *
 * That is the sixth entry in this file's own catalogue of observations that
 * cannot see their own violation (see the header). The old
 * `assert(ownControl !== undefined)` read like a guard against it and was not
 * one: it asserted that *some* pattern lacks a trailing star, which the wrong
 * binding satisfies.
 *
 * **The two names are literals here on purpose**, and it is the opposite of the
 * literal `topicsOf` avoids. That one would have to be edited to go green when
 * the wire shape changes, which is how a wire-format test stops testing the
 * wire format. These are the identities the #282/#288 property is *stated in
 * terms of*, so a driver that stops using them must make this file fail — which
 * is what the three assertions below do, loudly, instead of binding to the
 * other subscription and continuing.
 *
 * @param prefix - The deployment prefix the recording was made under.
 * @param subscriptions - Everything the recording port captured.
 * @returns The one subscription that IS the control topic.
 * @throws {AssertionError} If it is absent or duplicated.
 */
function controlSubscription<T extends { pattern: string }>(
    prefix: string,
    subscriptions: readonly T[],
): T {
    const controlTopic = `${prefix}__control`
    const found = subscriptions.filter((s) => s.pattern === controlTopic)
    assertEquals(
        found.length,
        1,
        `exactly one subscription must BE "${controlTopic}"; saw ` +
            `${found.length} in ${patternsIn(subscriptions)}`,
    )
    return found[0]
}

/**
 * Every event subscription in a recording, by identity — anchored under
 * `${prefix}__event:`. See {@link controlSubscription}.
 *
 * @param prefix - The deployment prefix the recording was made under.
 * @param subscriptions - Everything the recording port captured.
 * @returns The event subscriptions, in recorded order.
 * @throws {AssertionError} If none is anchored under the event head.
 */
function eventSubscriptions<T extends { pattern: string }>(
    prefix: string,
    subscriptions: readonly T[],
): T[] {
    const eventHead = `${prefix}__event:`
    const found = subscriptions.filter((s) => s.pattern.startsWith(eventHead))
    assert(
        found.length > 0,
        `no subscription is anchored under "${eventHead}"; saw ` +
            patternsIn(subscriptions),
    )
    return found
}

/**
 * Both of the above, plus the claim that they account for EVERYTHING recorded.
 *
 * Only for a recording where both seams are registered. `deliver` below never
 * calls `onControl` — which is what opens the control subscription — so it asks
 * for the event subscriptions alone rather than relaxing this into an optional
 * control, which would put the absence back out of sight.
 *
 * @param prefix - The deployment prefix the recording was made under.
 * @param subscriptions - Everything the recording port captured.
 * @returns The control subscription and every event subscription.
 * @throws {AssertionError} If either is missing, or if any subscription is
 * neither.
 */
function splitSubscriptions<T extends { pattern: string }>(
    prefix: string,
    subscriptions: readonly T[],
): { control: T; events: T[] } {
    const control = controlSubscription(prefix, subscriptions)
    const events = eventSubscriptions(prefix, subscriptions)
    // Nothing unaccounted for. Without this the two claims above hold while a
    // third subscription — the one that would carry the leak — goes unexamined.
    assertEquals(
        1 + events.length,
        subscriptions.length,
        'a subscription is neither the control topic nor anchored under the ' +
            `event head: ${patternsIn(subscriptions)}`,
    )
    return { control, events }
}

/** The recorded patterns, for an assertion message. */
function patternsIn(subscriptions: readonly { pattern: string }[]): string {
    return JSON.stringify(subscriptions.map((s) => s.pattern))
}

Deno.test('SC-002: no accepted prefix can reach another, in either direction', async () => {
    for (const [self, other] of ISOLATION_PAIRS) {
        const mine = await exercise(self)
        const theirs = await exercise(other)
        const myPatterns = mine.subscriptions.map((s) => s.pattern)
        const myTopics = topicsOf(mine)
        const theirTopics = topicsOf(theirs)

        // THE POSITIVE CONTROL, and it is not decoration.
        //
        // `assertEquals(reached, [])` is satisfied by a neutered `globMatches`,
        // by an `exercise` that returns zero subscriptions, and by a deleted
        // loop body. This file records five mutations that were GREEN for
        // exactly that reason (see the header). An emptiness claim is worth
        // nothing without a demonstration, in the same body, that the machinery
        // producing it can still produce a match.
        assertEquals(
            myTopics.filter((t) =>
                myPatterns.some((pattern) => globMatches(pattern, t))
            ).sort(),
            myTopics.slice().sort(),
            `positive control failed for "${self}": a deployment must match ` +
                'its OWN topics. If this line fails, the emptiness assertion ' +
                'below proves nothing and must not be read as a pass.',
        )

        // A deployment's own event pattern must not reach its own control
        // topic either (US2) — routing alone must never be able to hand a
        // control frame to `onMessage`, where `#verifyAndDecode` is not.
        const { control, events } = splitSubscriptions(self, mine.subscriptions)
        for (const { pattern } of events) {
            assert(
                !globMatches(pattern, control.pattern),
                `"${pattern}" matches this deployment's OWN control topic ` +
                    `"${control.pattern}". A control frame routed to onMessage ` +
                    'never reaches the MAC check.',
            )
        }

        const reached: string[] = []
        for (const pattern of myPatterns) {
            for (const topic of theirTopics) {
                if (globMatches(pattern, topic)) {
                    reached.push(`${pattern} -> ${topic}`)
                }
            }
        }
        assertEquals(
            reached.sort(),
            [],
            `deployment "${self}" reaches deployment "${other}". Both ` +
                'prefixes are accepted by assertUsablePrefix, so this is a ' +
                'configuration a user can reach — #288.',
        )
    }
})

/**
 * Drive one event-topic delivery straight into the driver's ingest handler.
 *
 * `FakeRedisBus` cannot be used for this: it routes by
 * `topic.startsWith(pattern minus '*')`, so it can only ever deliver a topic
 * that already matches. These two tests are about what happens to a topic that
 * does NOT — which no fake that models routing can produce.
 */
async function deliver(prefix: string, topic: string, payload: string) {
    const { command, subscriber, recording } = recordingPorts(CANNED)
    const driver = new RedisBroadcastDriver(command, subscriber, { prefix })
    const got: { channel: string; event: string }[] = []
    driver.onMessage((m) => got.push({ channel: m.channel, event: m.event }))
    // The event subscription now comes from a WATCH, not from `onMessage`
    // (#295). Which channel is watched is irrelevant here — the decoder derives
    // the channel from the DELIVERED topic, which is the property these two
    // tests exist to pin, and a helper that watched the topic under test would
    // hide exactly that.
    await driver.watchChannel('room')
    // Events only: this helper never registers `onControl`, and that seam is
    // what opens the control subscription (drivers/redis.ts, `onControl`).
    const events = eventSubscriptions(prefix, recording.subscriptions)
    events[0].handler(topic, payload)
    await driver.close()
    return got
}

Deno.test('FR-002: a topic that does not carry the event marker is DROPPED', async () => {
    // Deny by default. This branch used to read `channel = topic`, so a topic
    // the subscription could not have produced became a channel name — and a
    // plausible one: `isValidName('app__control')` is TRUE, because `_` and `:`
    // are both in NAME_RE. The ingest checks below it would have passed it
    // through on any payload carrying a string `event`.
    //
    // Unreachable from a correct broker under a literal-anchored pattern, which
    // is exactly why it must not be the branch that decides anything: a faulty
    // port, a fake, or a re-subscription bug feeds it directly.
    const got = await deliver(
        'app',
        'app__control',
        JSON.stringify({ event: 'e', data: 1 }),
    )
    assertEquals(
        got,
        [],
        'a topic outside the event marker reached local fan-out under a ' +
            'channel name nobody chose',
    )
})

Deno.test('FR-002: a channel containing the reserved separator round-trips whole', async () => {
    // The fixture that separates a fixed-offset slice from a `replace`. Both
    // strip the marker from `app__event:room`; only the slice survives a
    // channel that CONTAINS the marker, and NAME_RE permits one to
    // (`_` and `:` are both in the charset).
    //
    // Measured, not assumed. `replace(marker, '')` is in fact **equivalent** to
    // the slice here, and the reason is worth writing down rather than
    // discovering twice: the `startsWith` guard above has already established
    // that the marker's first occurrence is position 0, so "remove the first
    // occurrence" and "drop that many characters" cannot disagree. An earlier
    // note on this test claimed `replace` would corrupt the channel; it does
    // not, and a mutation battery said so before anyone shipped the claim.
    //
    // `split(marker)[1]` is the one that really differs — it returns the empty
    // string for the fixture below, and the driver would then fan out under a
    // channel no subscriber asked for. And a `slice(this.prefix.length + 1)`
    // left over from the pre-#288 offset returns `_event:...`.
    // The channel must repeat the WHOLE marker, prefix included — that is what
    // separates the three candidate implementations. A channel of `__event:x`
    // does not: the marker's first occurrence is still position 0, so `slice`,
    // `replace` and `split` all agree, and a fixture built on it proves nothing
    // about which one is in the file.
    const got = await deliver(
        'app',
        'app__event:app__event:y',
        JSON.stringify({ event: 'e', data: 1 }),
    )
    assertEquals(
        got,
        [{ channel: 'app__event:y', event: 'e' }],
        'the strip must be a fixed-offset slice. `split(marker)[1]` returns ' +
            'the empty string here, and `replace` is only safe by accident — ' +
            'see the note above this test.',
    )

    // And the plain `:` case, which is the one that has always been legal.
    const colon = await deliver(
        'app',
        'app__event:presence-room.1:v2',
        JSON.stringify({ event: 'e', data: 1 }),
    )
    assertEquals(colon, [{ channel: 'presence-room.1:v2', event: 'e' }])
})

/**
 * The literal source text every anchored getter interpolates.
 *
 * `FR-004 source` reads the driver as TEXT, so what a tail begins with is this
 * identifier's spelling, not the two underscores it evaluates to. The length is
 * how the check reaches the character AFTER the separator.
 */
const RESERVED_LEAD_LITERAL = '${RESERVED_SEPARATOR_LEAD}'

Deno.test('FR-004 source: EVERY derived name carries the reserved lead-in', async () => {
    // The review gate's finding, and it was right: the isolation invariant was
    // enforced by a comment. Seven builders each re-write the interpolation
    // shape independently, `this.prefix` is `private readonly` rather than
    // `#private` so it is interpolable at every site, and an EIGHTH name
    // spelled `${this.prefix}:queue:` would pass fmt, lint, check and the whole
    // suite while silently reopening #288 for that family.
    //
    // A comment cannot fail. This can.
    const source = await Deno.readTextFile(
        new URL('../drivers/redis.ts', import.meta.url),
    )
    const sites = [
        ...source.matchAll(
            /(\w+)\([^)]*\)(?::[^{]+)?\{\s*return `\$\{this\.prefix\}([^`]*)`/g,
        ),
    ].map((m) => ({ member: m[1], tail: m[2] }))

    const expectedSites = PREFIX_MEMBERS.length - FRAGMENT_DERIVED.length +
        PREFIX_FRAGMENTS.length
    assert(
        sites.length >= expectedSites,
        `only ${sites.length} direct interpolation sites found, expected at ` +
            `least ${expectedSites}; the regex has ` +
            'drifted from the source and this check is not looking at the ' +
            'driver any more',
    )
    for (const { member, tail } of sites) {
        // NO exemption branch, and that is the point. #278 removed the last two
        // unanchored names, so this check is unconditional — there is no list
        // to add a name to in order to make it pass.
        assert(
            tail.startsWith('${RESERVED_SEPARATOR_LEAD}'),
            `${member} derives "\${prefix}${tail}", which does NOT begin with ` +
                'the reserved lead-in. The isolation proof holds only because ' +
                'every separator starts with the sequence assertUsablePrefix ' +
                'refuses — see RESERVED_SEPARATOR_LEAD. Anchor it.',
        )
        // The proof rests on a SECOND property nothing used to assert: no tail
        // may begin with a third `_`. `assertUsablePrefix` refuses only the
        // two-character sequence, and `_` is inside PREFIX_RE, so a member
        // spelled `${prefix}___queue:` passes the check above and lets the
        // accepted prefixes `app` and `app_` derive the same key. That is the
        // collision the anchoring exists to close, reopened past the test
        // written to prevent it.
        // POSITIVE CONTROL for the line below: if the regex ever stops
        // capturing tails, `tail[26]` is `undefined` for every site and the
        // assertion passes for every input — an emptiness claim with nothing
        // behind it. A tail must exist and must be non-empty to be checkable.
        assert(
            tail.length > RESERVED_LEAD_LITERAL.length,
            `${member}'s captured tail is "${tail}", which is too short to ` +
                'carry a separator — the source regex has drifted and the ' +
                'underscore check below cannot fail for any input',
        )
        assert(
            tail[RESERVED_LEAD_LITERAL.length] !== '_',
            `${member} derives "\${prefix}${tail}", whose separator begins ` +
                'with a THIRD underscore. Two accepted prefixes differing by ' +
                'one trailing `_` then derive the same name — see the ' +
                'containment proof in the plan for #278. Use exactly two.',
        )
    }
})

Deno.test('FR-004: a subscription reaches ONLY its own family, within one prefix', async () => {
    // The second half of the invariant, and the first draft of this test
    // checked a hazard that does not exist. It asserted no family name is a
    // prefix of another, reasoning that `__events:` beside `__event:` would
    // collide. It would not: `app__event:` is not a prefix of `app__events:`,
    // because the `:` and the `s` differ at the same offset. The mutation
    // proved it — the test passed with the family added.
    //
    // The real hazard is a family NESTED under a subscribed one: `__event:sub:`
    // yields `app__event:sub:x`, which `app__event:*` matches exactly. So the
    // property is not about the names, it is about what each SUBSCRIPTION
    // reaches — checked here against the driver's actual recorded output rather
    // than against a regex over its source.
    const recording = await exercise('alpha')
    const patterns = recording.subscriptions.map((s) => s.pattern)
    const names = [...new Set(recording.strings())].filter((value) =>
        value.startsWith('alpha') && !patterns.includes(value)
    )

    // By identity, not by shape (#315): which subscription this is decides what
    // it may reach, and `endsWith('*')` is a fact about neither.
    const { control, events } = splitSubscriptions(
        'alpha',
        recording.subscriptions,
    )
    const expectations: [string, string[]][] = [
        // The control topic is glob-free and reaches only itself, which is not
        // in `names` — so nothing.
        [control.pattern, []],
        // An event subscription now reaches NOTHING outside itself, and that
        // is a stronger result than the one this line used to assert (#295).
        //
        // Under the prefix-wide glob it reached a SET — every event topic under
        // the prefix — and the hazard this test guards was a family nested
        // under it: `__event:sub:` yields `alpha__event:sub:x`, which
        // `alpha__event:*` matches. An exact-topic subscription has no such
        // reach: it matches one string, its own, which the `names` filter
        // excludes because it is also a recorded pattern.
        //
        // The guard is kept rather than deleted. It still covers the control
        // subscription, and it is what would fire the day any glob comes back.
        ...events.map((s): [string, string[]] => [s.pattern, []]),
    ]

    for (const [pattern, expected] of expectations) {
        const reached = names.filter((name) => globMatches(pattern, name))
        assertEquals(
            reached.sort(),
            expected,
            `the subscription "${pattern}" reaches names outside its own ` +
                'family. Every derived name is anchored, so this is not #288 ' +
                'across prefixes — it is the same defect WITHIN one prefix, ' +
                'which a new family nested under a subscribed one (say ' +
                '`__event:sub:`) introduces without touching anything else.',
        )
    }
})

Deno.test('FR-004: the refused sequence and the separator lead-in are ONE decision', () => {
    // The invariant the whole isolation proof rests on, and the one thing that
    // can break silently: the guard refuses `__`, and every reserved separator
    // begins with `__`. Neither half is worth anything alone.
    //
    // This is what SC-002's pair table cannot reach. A prefix that EMBEDS the
    // event marker is the case that reopens #288 one level down — and it cannot
    // appear in that table, because `exercise()` would throw before recording
    // anything. So the two halves are asserted against each other directly:
    // first that such a prefix really would reach, then that it is refused.
    const hostile = 'app__event:z'
    assert(
        globMatches('app__event:*', `${hostile}__event:room`),
        'if this stops being true the fixture is stale, not safe: it is only ' +
            'a hazard while the outer pattern really does span it',
    )
    let refused = false
    try {
        const { command, subscriber } = recordingPorts()
        new RedisBroadcastDriver(command, subscriber, { prefix: hostile })
    } catch {
        refused = true
    }
    assert(
        refused,
        `a prefix of "${hostile}" was accepted. It embeds the event marker, ` +
            'so a deployment at "app" reaches every one of its channels — ' +
            '#288, one level down. The guard MUST refuse whatever sequence ' +
            'the separators begin with; refusing some other sequence leaves ' +
            'this open while every other test stays green.',
    )
})

/** The presence key a driver at `prefix` derives for `channel`, read off the port. */
async function presenceKeyOf(prefix: string, channel: string): Promise<string> {
    const { command, subscriber, recording } = recordingPorts(CANNED)
    const driver = new RedisBroadcastDriver(command, subscriber, { prefix })
    await driver.addMember(channel, { id: 'u1', info: {} })
    await driver.close()
    const hset = recording.commands.find((argv) => argv[0] === 'HSET')
    assert(hset !== undefined, 'addMember did not HSET')
    return hset[1]
}

Deno.test('FR-012: two accepted prefixes cannot derive the same KEY', async () => {
    // SC-002 covers what a SUBSCRIPTION reaches. Keys are never glob-matched,
    // so their risk is not reach — it is COLLISION, and the two are not the
    // same property. Before #288 this pair really did collide, and no test in
    // this file could have seen it:
    //
    //   prefix "app"             + channel "eu:presence:room"
    //   prefix "app:presence:eu" + channel "room"
    //     -> both "app:presence:eu:presence:room"
    //
    // One deployment's `listMembers` returned the other's roster, with
    // `member.info`. Both prefixes are accepted and the channel is a valid
    // name, so it was reachable by configuration alone.
    //
    // The fix makes it structural: the client-controlled channel sits after a
    // `__`-anchored infix that no accepted prefix can reproduce.
    const outer = await presenceKeyOf('app', 'eu:presence:room')
    const inner = await presenceKeyOf('app:presence:eu', 'room')
    assert(
        outer !== inner,
        `both prefixes derive "${outer}" — one deployment's roster IS the ` +
            "other's. This is #288's key half, and it does not show up in " +
            'any reach test because keys are never glob-matched.',
    )

    // And the same pair one level out, so the fixture is not a single lucky
    // string: any channel that reproduces the other prefix's tail.
    const a = await presenceKeyOf('app', 'eu__presence:room')
    const b = await presenceKeyOf('app:eu', 'room')
    assert(a !== b, `both prefixes derive "${a}"`)
})

Deno.test('#278/SC-003: the last two colliding prefix pairs are closed', async () => {
    // #288 anchored five names and left TWO out — the legacy revocation keys,
    // which existed only to read what a pre-#276 instance wrote at those exact
    // strings. Anchoring them would have addressed a key nothing ever wrote,
    // so the collision stayed open by design and was documented as such.
    //
    // #278 deleted the reader, which closes it the only way available: the
    // names stop existing. `app` and `app:revoked` are both accepted by
    // PREFIX_RE — the `:` is in the charset — so before this they could derive
    // the same key.
    const pairs: readonly (readonly [string, string, string])[] = [
        // The pair the legacy names made reachable.
        ['app', 'revoked', 'app:revoked'],
    ]
    for (const [left, channel, right] of pairs) {
        const a = await presenceKeyOf(left, channel)
        const b = await presenceKeyOf(right, channel)
        assert(
            a !== b,
            `prefixes "${left}" and "${right}" both derive "${a}"`,
        )
    }

    // The OTHER half, and this test is how it was found. `SC-005` pins the
    // guard and its message; this asserts the consequence. `app` and `app_` do
    // not collide — they derive different keys — but the ACL grant this guide
    // recommends for `app`, `~app__*`, MATCHES `app_`'s names, because `app_`
    // plus the two-character separator is `app___`. Different keys, shared
    // credential boundary. `__` was already refused; a single trailing `_` was
    // not, and now is, which is what makes the containment argument exact.
    await assertRejects(
        async () => {
            await presenceKeyOf('app_', 'room')
        },
        Error,
        'must not end with',
    )
})

Deno.test('SC-005: an unusable prefix is refused, by the RIGHT guard', () => {
    // Not hygiene. Such a prefix is trivially "anchored" under ANY definition —
    // every name it derives begins with it — so SC-001 passes while the driver
    // subscribes to traffic it does not own. The containment test and this
    // guard cover different halves of the same property.
    //
    // ONE property each, and the expected MESSAGE is pinned, not just "it
    // threw". A fixture of 'app[1]' once carried both `[` and `]`, so dropping
    // `[` from the guard still threw on `]` and the whole suite stayed green.
    // Asserting only `threw` has the same shape of blind spot one level up:
    // after #288 added a charset allowlist, EVERY glob character is also
    // outside the allowlist, so a `threw`-only fixture would be satisfied by
    // the allowlist while the guard it was written for had been deleted.
    // Pinning the message is what makes each fixture test its own guard.
    const cases: readonly (readonly [string, string])[] = [
        ['', 'must not be empty'],
        // `app\\` is the one a first version missed: Redis reads `app\\:*` as the
        // literal `app:*`, so the deployment reads another's whole stream while
        // its own traffic stays invisible to that deployment.
        ['app*', 'glob character "*"'],
        ['ap?p', 'glob character "?"'],
        ['app[x', 'glob character "["'],
        ['appx]', 'glob character "]"'],
        ['app\\', 'glob character "\\"'],
        // #288. `_` is inside the allowlist and is not a glob character, so
        // these three are refused by the separator guard and by nothing else —
        // which is what makes them the fixtures that kill its removal. ONE
        // occurrence each, in three positions.
        ['app__x', 'reserved separator'],
        ['__app', 'reserved separator'],
        ['app__', 'reserved separator'],
        // #278, and it is the half #288 left open. `app_` collides with
        // nothing and cross-subscribes to nothing — SC-002 proved that with
        // this exact pair. What it shares with `app` is a CREDENTIAL boundary:
        // the ACL grant this project documents for `app` is `~app__*`, and
        // `app_`'s own names begin `app___`, which that glob matches. Refused
        // by this guard and by nothing else — `_` is inside the allowlist, one
        // trailing occurrence is not the reserved sequence, and it is not a
        // glob character.
        ['app_', 'must not end with'],
        ['a_', 'must not end with'],
        // The allowlist's own half: characters no other guard mentions.
        ['app rt', 'must match'],
        ['app\nrt', 'must match'],
        ['app\u0000rt', 'must match'],
        ['\u202eapp', 'must match'],
        ['a'.repeat(65), 'must match'],
    ]
    for (const [bad, expected] of cases) {
        let message: string | undefined
        try {
            const { command, subscriber } = recordingPorts()
            new RedisBroadcastDriver(command, subscriber, { prefix: bad })
        } catch (error) {
            message = error instanceof Error ? error.message : String(error)
        }
        assert(
            message !== undefined,
            `a prefix of ${JSON.stringify(bad)} was accepted; it reaches ` +
                'PSUBSCRIBE at two pattern contexts and every derived key',
        )
        assert(
            message.includes(expected),
            `a prefix of ${JSON.stringify(bad)} was refused by the WRONG ` +
                `guard. Expected a message containing ${
                    JSON.stringify(expected)
                }, ` +
                `got: ${message}`,
        )
    }

    // The default must survive all four checks — 17 characters, a `:`, no `__`.
    const { command, subscriber } = recordingPorts()
    new RedisBroadcastDriver(command, subscriber, {}).close()
})

Deno.test('SC-005: an ordinary prefix is still accepted', async () => {
    // The negative control. Without it, a guard that rejected EVERYTHING would
    // satisfy the test above.
    const recording = await exercise('lockness:realtime')
    assert(recording.subscriptions.length === 2, 'a normal prefix still works')
})
