/**
 * @fileoverview #273 — the realtime bus against a **live Redis broker**.
 *
 * Distinct from `driver_redis_live.test.ts`, which is named for a live *socket*:
 * that suite talks to the in-process RESP fake and proves the subscribe-mode
 * push path end-to-end over TCP. It cannot prove anything else, because the fake
 * answers ten commands of which exactly two (`SET`, `DEL`) are ones the driver's
 * command connection issues. Presence, eviction and the durable revocation index
 * have, until this file, only ever been checked against `fake_redis.ts` — an
 * in-process model of Redis written by the same hand as the code it validates.
 *
 * That is not a theoretical gap. During #276 the revocation index's `EXPIRE`
 * line was wrong twice in a row — first unconditional (deleting live
 * revocations), then `GT` alone (inert, because Redis reads a key with no TTL as
 * infinite) — and the full suite was green both times, because the fake modelled
 * the option flags wrongly in the same place.
 *
 * **What this suite is, and is not.** `fake_redis.ts` was corrected in the same
 * change that fixed the driver, so it now catches that specific mutation too —
 * this suite is not "the only thing that catches it". What it is, is the only
 * check that does not depend on the fake being right. Both were wrong together
 * twice, and nothing inside the repository could tell. Reverting the driver's
 * `EXPIRE … NX` line and watching *this* file go red is a statement about
 * Redis; watching the fake go red is a statement about the fake.
 *
 * Gated behind `LOCKNESS_REDIS_INTEGRATION=1`; see
 * `packages/redis/tests/live_broker.ts` for the gate, the connection contract
 * and the preflight, and `live_realtime.ts` for the key layout and read-backs.
 *
 * @module @lockness/realtime/tests/redis_broker_integration
 */

import { assert, assertEquals } from '@std/assert'
import {
    brokerConfig,
    LIVE_BROKER,
    preflight,
    runNamespace,
} from '../../redis/tests/live_broker.ts'
import { hmacSha256Hex } from '../../redis/mod.ts'
import { RedisBroadcastDriver } from '../drivers/redis.ts'
import {
    awaitSubscribers,
    connection,
    controlSecret,
    keys,
    type Reader,
    waitFor,
    withFaultyInstance,
    withInstances,
    withReader,
} from './live_realtime.ts'

/** Register a gated `[integration]` test that owns a fresh namespace. */
function integrationTest(
    name: string,
    fn: (namespace: string, reader: Reader) => Promise<void>,
): void {
    Deno.test({
        name: `[integration] ${name}`,
        ignore: !LIVE_BROKER,
        async fn() {
            await preflight(brokerConfig())
            const namespace = runNamespace()
            await withReader(namespace, (reader) => fn(namespace, reader))
        },
    })
}

// ---------------------------------------------------------------------------
// #294 — the harness's own edges
// ---------------------------------------------------------------------------

integrationTest(
    '#294: scanMatch refuses a pattern that escapes the run namespace',
    async (namespace, reader) => {
        // `scanKeys` builds `<namespace>*` itself and structurally cannot
        // escape; `scanMatch` takes a caller's glob and, until #294, could not
        // enforce the same thing. Every caller today derives its pattern from
        // `keys(namespace)`, so this guards the caller that has not been
        // written yet — on a SHARED broker an unanchored glob reads another
        // run's keys and reports them as this run's, which is the failure that
        // makes a green assertion mean nothing.
        const anchored = await reader.scanMatch(`${namespace}*`)
        assert(Array.isArray(anchored), 'an anchored pattern still works')

        let refused = ''
        try {
            await reader.scanMatch('*')
        } catch (error) {
            refused = error instanceof Error ? error.message : String(error)
        }
        assert(
            refused.includes('not anchored'),
            `an unanchored pattern was accepted: ${refused || '(no throw)'}`,
        )
        // The message has to name the namespace, or the next person to hit
        // this cannot tell which run refused them.
        assert(
            refused.includes(namespace),
            'the refusal names the run namespace it expected',
        )
    },
)

// ---------------------------------------------------------------------------
// US1 — cross-process delivery (SC-001)
// ---------------------------------------------------------------------------

integrationTest(
    'US1: a broadcast on one instance reaches an authorized subscriber on another',
    async (namespace, reader) => {
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)

            const listener = connection('b-listener', { id: 1, name: 'Bea' })
            await b.manager.subscribe(listener, 'private-orders')

            a.manager.broadcast('private-orders', 'created', { id: 42 })

            await waitFor(
                () => listener.sawEvent('created'),
                'the event to cross the broker to instance B',
            )
            assert(
                listener.frames.some((f) => f.includes('42')),
                'the payload crossed intact, not just the event name',
            )
        })
    },
)
integrationTest(
    'US1: a connection B’s OWN authorizer rejects receives nothing from the bus',
    async (namespace, reader) => {
        // B refuses user 99 outright. A knows nothing about that refusal — the
        // point of S6 is that authorization is re-applied by the RECEIVING
        // instance, so a message crossing the bus carries no authority with it.
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)

            const rejected = connection('b-rejected', { id: 99, name: 'Mal' })
            await b.manager.subscribe(rejected, 'private-orders')
            const allowed = connection('b-allowed', { id: 3, name: 'Dee' })
            await b.manager.subscribe(allowed, 'private-orders')

            a.manager.broadcast('private-orders', 'created', { id: 7 })
            await waitFor(
                () => allowed.sawEvent('created'),
                'delivery to the connection B authorized',
            )

            assertEquals(
                rejected.frames.some((frame) => frame.includes('created')),
                false,
                'B’s authorizer rejected this connection, so no bus message ' +
                    'reaches it — A’s broadcast carries no authority of its own',
            )
        }, {
            authorize: (index) => (identity) => {
                if (index === 1 && identity?.id === 99) return false
                return identity
                    ? { id: identity.id, info: { name: identity.name } }
                    : false
            },
        })
    },
)

// ---------------------------------------------------------------------------
// US2 — authoritative presence (SC-002)
// ---------------------------------------------------------------------------

integrationTest(
    'US2: the roster read OUT OF REDIS lists both instances’ members',
    async (namespace, reader) => {
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)

            const onA = connection('a-1', { id: 11, name: 'Ana' })
            const onB = connection('b-1', { id: 22, name: 'Bo' })
            await a.manager.subscribe(onA, 'presence-lobby')
            await b.manager.subscribe(onB, 'presence-lobby')

            // Read back with a RAW HGETALL on a client the suite owns — never
            // through driver.listMembers(), which would route the assertion
            // back through the parsing layer under test (FR-008).
            await waitFor(
                async () =>
                    (await reader.roster(namespace, 'presence-lobby')).size >=
                        2,
                'both members to reach the authoritative roster',
            )
            const roster = await reader.roster(namespace, 'presence-lobby')

            assertEquals(
                [...roster.keys()].sort(),
                ['11', '22'],
                'both instances’ members are in the Redis roster',
            )

            // Each entry is tagged with its OWNING instance, which is what the
            // ghost sweep keys off. Two members on two instances must carry two
            // distinct owners — a single owner would mean the roster cannot
            // tell whose socket died.
            const owners = [...roster.values()].map((entry) =>
                (entry as { owner?: string }).owner
            )
            assert(
                owners.every((owner) => typeof owner === 'string' && owner),
                `every roster entry is owner-tagged: ${JSON.stringify(owners)}`,
            )
            assertEquals(
                new Set(owners).size,
                2,
                'the two members are tagged to two DIFFERENT instances',
            )
        })
    },
)

integrationTest(
    'US2: each client observed a joined for the other, across the broker',
    async (namespace, reader) => {
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)

            const onA = connection('a-2', { id: 33, name: 'Cai' })
            await a.manager.subscribe(onA, 'presence-lobby')
            const onB = connection('b-2', { id: 44, name: 'Di' })
            await b.manager.subscribe(onB, 'presence-lobby')

            // Asserted on the ACTION, not a bare id: a `left` frame carries the
            // same member id, so `includes('44')` alone would have been
            // satisfied by the opposite event.
            //
            // One direction only, and that is correct rather than a gap. A
            // `joined` is announced at the moment of joining; A was already
            // subscribed when B joined, so A receives it across the broker. B
            // subscribed second and there is no replay of past joins — a later
            // joiner learns who is already here from the AUTHORITATIVE ROSTER,
            // which US2's first test asserts by reading Redis directly. Plan §2
            // says "each observed a joined for the other", which is idealised:
            // it only holds if both joined after both were subscribed.
            await waitFor(
                () =>
                    onA.frames.some((f) =>
                        f.includes('"joined"') && f.includes('44')
                    ),
                'A to observe B’s member JOINING across the broker',
            )
            assertEquals(
                onB.frames.some((f) =>
                    f.includes('"joined"') && f.includes('33')
                ),
                false,
                'and B does NOT get a replayed join for the member that was ' +
                    'already there — it reads the roster instead',
            )
        })
    },
)

// ---------------------------------------------------------------------------
// US3 — cross-process eviction (SC-003)
// ---------------------------------------------------------------------------

integrationTest(
    'US3: an evict issued on the instance that does NOT own the socket closes it',
    async (namespace, reader) => {
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)

            const watcherOnA = connection('watch-a', { id: 51, name: 'Wa' })
            const watcherOnB = connection('watch-b', { id: 52, name: 'Wb' })
            await a.manager.subscribe(watcherOnA, 'presence-lobby')
            await b.manager.subscribe(watcherOnB, 'presence-lobby')

            const target = connection('owned-by-a', { id: 55, name: 'Eli' })
            await a.manager.subscribe(target, 'presence-lobby')

            // Positive read FIRST, so the absence assertion below cannot pass
            // vacuously against a mistyped key (FR-009).
            await waitFor(
                async () =>
                    (await reader.roster(namespace, 'presence-lobby')).has(
                        '55',
                    ),
                'the member to appear in the roster before it is evicted',
            )
            const before = await reader.roster(namespace, 'presence-lobby')
            assert(
                before.has('55'),
                'the member IS in the roster to begin with',
            )

            // B evicts a socket it does not own.
            await b.manager.evict('owned-by-a')

            await waitFor(
                () => target.closed() > 0,
                'the owning instance to close the evicted socket',
            )
            await waitFor(
                async () =>
                    !(await reader.roster(namespace, 'presence-lobby')).has(
                        '55',
                    ),
                'the member to leave the authoritative roster',
            )

            // The third clause of US3, which had no assertion at all: presence
            // subscribers on BOTH instances see the `left`. On B that frame can
            // only have arrived over the bus, as a presence-leave control frame
            // from the owning instance — there is no local path to it.
            await waitFor(
                () =>
                    watcherOnA.frames.some((f) => f.includes('"left"')) &&
                    watcherOnB.frames.some((f) => f.includes('"left"')),
                'both instances’ presence subscribers to observe the `left`',
            )
            const after = await reader.roster(namespace, 'presence-lobby')
            assertEquals(
                after.has('55'),
                false,
                'the evicted member is gone from the Redis roster',
            )
        })
    },
)

// ---------------------------------------------------------------------------
// US4 — the durable revocation index under REAL Redis semantics
// ---------------------------------------------------------------------------

integrationTest(
    'US4: markRevoked ARMS a TTL on the index — the inert EXPIRE GT would not',
    async (namespace, reader) => {
        await withInstances(1, namespace, async ([a]) => {
            await a.driver.markRevoked('victim-1')

            const ttl = await reader.ttlOf(keys(namespace).revocations)
            assert(
                ttl > 0,
                'the revocation index carries a TTL. A -1 here is the exact ' +
                    'defect shipped mid-#276: EXPIRE ... GT alone can never arm ' +
                    'a TTL, because Redis reads a key with no TTL as infinite. ' +
                    `Got ${ttl}.`,
            )
        })
    },
)

integrationTest(
    'US4: the index TTL extends but never shrinks',
    async (namespace, reader) => {
        const key = keys(namespace).revocations
        await withInstances(1, namespace, async ([short]) => {
            await short.driver.markRevoked('victim-2')
            const armed = await reader.ttlOf(key)
            assert(armed > 0, `armed on first write, got ${armed}`)

            await withInstances(1, namespace, async ([long]) => {
                await long.driver.markRevoked('victim-3')
                const extended = await reader.ttlOf(key)
                // A numeric FLOOR, not `>=`. Reviewed and confirmed by
                // mutation: with `extended >= armed`, deleting the driver's
                // `EXPIRE ... GT` line — whose only job is extension — left
                // every test in this suite green, because an unmoved TTL
                // satisfies `>=` perfectly.
                assert(
                    extended > armed + 300,
                    'a 900s write must actually EXTEND a 300s TTL, not merely ' +
                        `fail to shrink it: ${armed} -> ${extended}`,
                )
            }, { revocationTtlSeconds: 900 })

            const afterLong = await reader.ttlOf(key)
            await withInstances(1, namespace, async ([shorter]) => {
                await shorter.driver.markRevoked('victim-4')
                const afterShort = await reader.ttlOf(key)
                assert(
                    afterShort >= afterLong - 5,
                    'a SHORTER-TTL instance never shrinks the index TTL ' +
                        `(EXPIRE ... GT): ${afterLong} -> ${afterShort}`,
                )
            }, { revocationTtlSeconds: 30 })
        }, { revocationTtlSeconds: 300 })
    },
)

integrationTest(
    'US4: listRevoked returns live entries, read back raw',
    async (namespace, reader) => {
        await withInstances(1, namespace, async ([a]) => {
            await a.driver.markRevoked('victim-5')
            await a.driver.markRevoked('victim-6')

            const live = await reader.revoked(namespace)
            assertEquals(
                live.sort(),
                ['victim-5', 'victim-6'],
                'both revocations are in the index, read via a raw ' +
                    'ZRANGEBYSCORE rather than through driver.listRevoked()',
            )
        })
    },
)

integrationTest(
    'US4: listRevoked REAPS expired entries and keeps live ones',
    async (namespace, reader) => {
        // The only test that makes LIST_REVOKED_SCRIPT actually execute against
        // a real Redis. `listRevoked()` is the ACTION here, not the assertion —
        // every claim below is read back raw (FR-008).
        await withInstances(1, namespace, async ([a]) => {
            const index = keys(namespace).revocations
            const now = await reader.now()

            // One entry already expired, planted directly at a past score.
            await reader.command('ZADD', index, String(now - 60), 'stale-one')
            // One live entry, written by the driver itself.
            await a.driver.markRevoked('live-one')

            assertEquals(
                (await reader.revokedAtAnyScore(namespace)).sort(),
                ['live-one', 'stale-one'],
                'both are present before the reaper runs',
            )

            await a.driver.listRevoked?.()

            assertEquals(
                (await reader.revokedAtAnyScore(namespace)).sort(),
                ['live-one'],
                'the expired entry is REAPED from the index, not merely ' +
                    'filtered out of the reply',
            )
            assertEquals(
                await reader.revoked(namespace),
                ['live-one'],
                'and the live entry’s score is genuinely in the future — a ' +
                    '-inf read cannot tell these two cases apart, which is how ' +
                    'a revocation scored into the past would ship green',
            )
        })
    },
)

integrationTest(
    'US4: the index is bounded — re-revoking the same target adds no member',
    async (namespace, reader) => {
        await withInstances(1, namespace, async ([a]) => {
            await a.driver.markRevoked('victim-7')
            await a.driver.markRevoked('victim-7')
            await a.driver.markRevoked('victim-7')

            assertEquals(
                await reader.zcard(keys(namespace).revocations),
                1,
                'ZADD ... GT updates the score in place; it never appends',
            )
        })
    },
)

integrationTest(
    'US4: the legacy revoked SET is never written (dual-read only)',
    async (namespace, reader) => {
        await withInstances(1, namespace, async ([a]) => {
            await a.driver.markRevoked('victim-8')

            // Enumerate what the driver ACTUALLY created, rather than probing a
            // key name this file supplies. A seed-and-re-read probe on the same
            // helper constant proves only that the key is writable: mistype the
            // constant and the probe still passes, because both halves are
            // mistyped together. The driver never writes this key, so there is
            // nothing to observe its spelling from — the honest assertion is
            // over the whole namespace.
            const created = await reader.scanKeys(namespace)
            assert(
                created.length > 0,
                'the driver created SOMETHING — otherwise every absence ' +
                    `assertion below is vacuous. Found: ${created.join(', ')}`,
            )
            assert(
                // Via `keys()`, not an inline literal. This file already
                // reads the index through it three times above; a fourth
                // spelling built here is the second home the harness exists to
                // prevent, and #288 caught it by moving the key.
                created.includes(keys(namespace).revocations),
                'the sorted-set index is the key that WAS written: ' +
                    created.join(', '),
            )
            // #278 deleted the READ as well, so the claim is stronger and the
            // filter is weaker: `:revoked` cannot appear because no member
            // derives it, which means this assertion can no longer fail for the
            // reason it was written — it used to guard "read but never
            // written". What is worth pinning now is that nothing under the run
            // sits outside the anchored namespace at all.
            assertEquals(
                created.filter((key) => !key.startsWith(`${namespace}__`)),
                [],
                'a key was created outside the anchored namespace — after ' +
                    '#278 every derived name is behind the reserved lead-in, ' +
                    `with no exception: ${created.join(', ')}`,
            )
        })
    },
)

// ---------------------------------------------------------------------------
// #272 FR-010 — anti-replay, against a live broker
// ---------------------------------------------------------------------------

integrationTest(
    '#272: a control frame replayed on a LIVE broker is obeyed once, not twice',
    async (namespace, reader) => {
        // Deliberately a presence-join and NOT an evict. A replayed evict is a
        // no-op on its own merits — the target is already gone, so the socket
        // stays closed whether anti-replay works or not — and an earlier draft
        // of this test asserted exactly that. It passed with the replay check
        // fully disabled. A presence-join re-emits to every subscriber on every
        // delivery, so the count is a real signal.
        const secret = controlSecret()
        await withInstances(1, namespace, async ([a]) => {
            await awaitSubscribers(reader, namespace, 1)

            const watcher = connection('watcher', { id: 1, name: 'Wat' })
            await a.manager.subscribe(watcher, 'presence-lobby')
            const joinsSeen = () =>
                watcher.frames.filter((f) =>
                    f.includes('"joined"') && f.includes('4242')
                ).length

            const wire = {
                kind: 'presence-join',
                target: 'peer-conn',
                channel: 'presence-lobby',
                member: { id: 4242, info: { name: 'Ghost' } },
                origin: 'peer-instance',
                ts: Date.now(),
                nonce: Array.from(
                    crypto.getRandomValues(new Uint8Array(16)),
                    (b) => b.toString(16).padStart(2, '0'),
                ).join(''),
            }
            const mac = hmacSha256Hex(
                new TextEncoder().encode(secret),
                new TextEncoder().encode(JSON.stringify(wire)),
            )
            const frame = JSON.stringify({ ...wire, mac })
            const topic = keys(namespace).controlTopic

            // First delivery: a legitimate frame from a peer, obeyed.
            await reader.command('PUBLISH', topic, frame)
            await waitFor(
                () => joinsSeen() === 1,
                'the legitimate join to land',
            )

            // The attacker's entire capability: the same bytes, again.
            await reader.command('PUBLISH', topic, frame)
            await reader.command('PUBLISH', topic, frame)
            await new Promise((resolve) => setTimeout(resolve, 150))

            assertEquals(
                joinsSeen(),
                1,
                'two replays of the exact frame over a real broker are refused ' +
                    '— without the check this would be 3',
            )
        }, { secret })
    },
)

// ---------------------------------------------------------------------------
// US5 — the ghost sweep (#268 Q1/FR-008), against a real broker (#281)
//
// This is the path that removes a CRASHED instance's roster members. Every
// other scenario above tears its instances down cleanly, so none of them
// reaches it: the members simply never become ghosts.
//
// `driver.close()` is the crash model, and it is an honest one — it stops the
// timers and drops the sockets but deliberately does NOT `SREM` the instance
// or `DEL` its liveness key, precisely because a real crash cannot. The
// surviving instance has to notice the expiry and clean up after it.
//
// The cadences are the floor the broker allows, not values picked for speed:
// `EX` granularity is one second, so a sweep cannot be observed faster than
// that no matter what the reconcile interval says.
// ---------------------------------------------------------------------------

// Mutation battery — every one applied to `drivers/redis.ts`, run against a
// real broker, and observed before being reverted. A sweep test that only ever
// runs green proves the sweep happened, not that the test would notice if it
// stopped.
//
//   | # | Mutation                                          | Observed |
//   | - | ------------------------------------------------- | -------- |
//   | 1 | `#reconcile` never calls `#sweepInstance`          | RED      |
//   | 2 | the heartbeat `SET` drops its `EX` argument        | RED      |
//   | 3 | `#sweepInstance` skips the `SREM`                  | RED      |
//   | 4 | `#sweepInstance` skips the owned-set `DEL`         | RED      |
//   | 5 | the sweep `DEL`s the whole presence hash           | RED      |
//   | 6 | the `if (alive === 0)` liveness gate is removed    | RED      |
//   | 7 | the `id === this.instanceId` self-skip is removed  | RED      |
//   | 8 | the owned-entry parse splits on the LAST space      | RED      |
//   | 9 | `if (sep < 0) continue` becomes `sep <= 0`          | GREEN    |
//
// **Rows 8 and 9 are #316**, and row 8 is the reason `GHOST_MEMBER_ID` exists.
// The parse (`entry.indexOf(OWNED_SEP)`) ran on every pass of this scenario for
// the whole of #281 and could not be observed, because the fixture id was `2`:
// with one space in the entry, `indexOf` and `lastIndexOf` return the same
// index. Executing a line is not covering it. With a two-space id the mutation
// splits `presence-ops Boris Ivanov Jr` into channel `presence-ops Boris
// Ivanov` and field `Jr`, the `HDEL` hits a key that does not exist, and the
// ghost is never reclaimed — #314's second consequence, reached from the sweep
// side instead of the guard side.
//
// Row 9 is GREEN and is an EQUIVALENT mutant, recorded rather than dropped: the
// two forms differ only at `sep === 0`, an entry that BEGINS with a space, which
// means an empty channel name. `ChannelManager.subscribe` refuses that
// (#314's `#assertUsableChannel`, via `isValidName`), so no `addMember` can
// write one. The guard being unreachable from a valid input is the desired
// state, exactly as for row 7.
//
// Both rows are automated in `tests/mutations/sweep_parse_316.ts`.
//
// **Row 6 is why there are three instances.** It was GREEN — in 269ms — when
// this scenario had only a survivor and a corpse, and it is the DESTRUCTIVE
// direction: a driver that sweeps live peers evicts connected users from every
// presence channel. Two instances cannot catch it, because an instance never
// sweeps itself, so "sweep the dead one" and "sweep every peer" produce the
// same roster. The bystander's member is the assertion that separates them.
//
// **Row 7 was recorded GREEN twice, and is RED since #310.** It is worth
// keeping the whole path, because each reading was correct about the suite it
// was measured against and wrong about the guard.
//
// It was first recorded as an EQUIVALENT mutant: without the self-skip,
// `#reconcile` evaluates `EXISTS` on its own liveness key, which is present
// while the instance heartbeats, so no sweep follows. #293 was then filed
// expecting to make it RED, and moved it the other way — the configuration that
// would let an instance's own key lapse (`heartbeatIntervalMs` at or above half
// the TTL) is now refused at construction, so refusing the bad state moved the
// mutant FURTHER from killable. Nor was there a boot window: `start()` awaits
// one `#heartbeat()` before arming the interval. Re-measured on a real broker
// with the guard landed: 14 passed, 0 failed.
//
// What both readings missed is that "unreachable" was a claim about the
// CONFIGURATION, and the divergent path is TRANSIENT — the liveness `SET`
// failing for a window while the instance is otherwise healthy. `#heartbeat`
// catches and logs at WARN, so nothing else stops: the instance keeps accepting
// joins and writing rosters while its own key expires underneath it, and
// without the self-skip it then reads `EXISTS 0` on ITSELF and evicts its own
// connected users from every presence channel it holds.
//
// `US5/#310` below injects exactly that and nothing else, through
// `withFaultyInstance`. Measured both ways on a real broker: the row SURVIVES
// against this suite without that scenario, and is KILLED with it. The battery
// is `tests/mutations/self_skip_310.ts`.
//
// The self-skip would have stayed either way — a guard unreachable from a valid
// configuration is the desired state, not a redundancy. What changed is that
// "unreachable" is no longer the reason given for leaving it untested.
//
// Row 5 is why all three members share ONE channel: with a single owner,
// deleting the dead owner's field and deleting the whole hash are
// indistinguishable. Row 2 is why the TTL is asserted rather than the key's
// presence: a `SET` without `EX` means a crashed instance is never swept at
// all, and every presence-only assertion still passes.

/** Liveness TTL for the sweep scenarios — Redis `EX` cannot go below 1s. */
const SWEEP_LIVENESS_SECONDS = 1
/** Well under the TTL, or a LIVE instance would let its own key lapse. */
const SWEEP_HEARTBEAT_MS = 250
/** Fast enough that the sweep is bounded by the TTL, not by this. */
const SWEEP_RECONCILE_MS = 250
/** Generous: the TTL is 1s, so this is ~8 chances to observe the sweep. */
const SWEEP_TIMEOUT_MS = 8_000

/** Which instance `withInstances` hands to the crash — the second of three. */
const DOOMED_INDEX = 1
/**
 * The doomed instance's presence member id, carrying TWO spaces (#316).
 *
 * Not decoration. The owned-set entry is `` `${channel} ${memberId}` `` and the
 * sweep splits it on the FIRST space — a choice that is **unobservable** while
 * the id has no space of its own, because `indexOf` and `lastIndexOf` then
 * return the same index. This scenario ran for the whole of #281 with the id
 * `2`, executing the parse on every pass and unable to notice if it reversed.
 *
 * Two spaces, not one: with one, `lastIndexOf` still yields a channel that is
 * wrong but a field that is right, and the `HDEL` misses for a reason harder to
 * read. With `Boris Ivanov Jr` the split lands at channel
 * `presence-ops Boris Ivanov`, field `Jr`, and the ghost is simply never
 * reclaimed.
 *
 * A spaced id is a SUPPORTED input, not a hostile one: #306 left
 * `PresenceMember.id` charset-free on purpose (an email, a display name, an
 * external id), and #314's `OWNED_SEP` docstring promises in as many words that
 * a member id after the first space may contain more. This fixture is what
 * makes that promise a test rather than a claim.
 */
const GHOST_MEMBER_ID = 'Boris Ivanov Jr'

integrationTest(
    'US5/FR-008: a crashed instance’s members are swept, and a LIVE peer’s are not',
    async (namespace, reader) => {
        // THREE instances, not two. Two proves the sweep fires; it cannot prove
        // the sweep is SELECTIVE about which instance it fires against, because
        // with only a survivor and a corpse "sweep the dead one" and "sweep
        // every peer" have identical outcomes — an instance never sweeps itself
        // (`id === this.instanceId`, drivers/redis.ts:1238), so the survivor's
        // own member survives either way.
        //
        // The third instance stays alive and heartbeating, and its member is
        // the assertion that pins the `if (alive === 0)` liveness gate. Deleting
        // that gate is the DESTRUCTIVE failure — a driver that sweeps live peers
        // silently evicts connected users from every presence channel — and the
        // two-instance version of this test passed it in 269ms.
        await withInstances(
            3,
            namespace,
            async ([survivor, doomed, bystander]) => {
                await awaitSubscribers(reader, namespace, 3)

                // All three in the SAME channel. One channel, three owners: a sweep
                // that deletes the whole presence hash rather than the dead
                // instance's fields would pass a single-owner test perfectly.
                await survivor.manager.subscribe(
                    connection('survivor-conn', { id: 1, name: 'Ada' }),
                    'presence-ops',
                )
                await doomed.manager.subscribe(
                    connection('doomed-conn', { id: 2, name: 'Boris' }),
                    'presence-ops',
                )
                await bystander.manager.subscribe(
                    connection('bystander-conn', { id: 3, name: 'Cleo' }),
                    'presence-ops',
                )

                await waitFor(
                    async () =>
                        (await reader.roster(namespace, 'presence-ops'))
                            .size === 3,
                    'all three members to reach the authoritative roster',
                )

                // Each liveness key is BOUNDED. A `SET` without `EX` leaves a key
                // that never expires, so a crashed instance is never swept at all —
                // and every presence-only assertion still passes.
                //
                // `>= 0`, not `> 0`: at a 1s TTL Redis legitimately reports 0 for a
                // key with under half a second left, and rejecting that would be a
                // flake of this test's own making. What is being excluded is `-1`
                // (no expiry set — the actual defect) and `-2` (absent).
                const aliveKeys = await reader.scanMatch(
                    keys(namespace).alivePattern,
                )
                assertEquals(
                    aliveKeys.length,
                    3,
                    'each instance registered exactly one liveness key',
                )
                for (const key of aliveKeys) {
                    const ttl = await reader.ttlOf(key)
                    assert(
                        ttl >= 0 && ttl <= SWEEP_LIVENESS_SECONDS,
                        `the liveness key ${key} carries a bounded TTL, not -1 ` +
                            `(no expiry) or -2 (absent); saw ${ttl}`,
                    )
                }

                // Identify the doomed instance's owned-set by its CONTENTS, before
                // the crash. Counting the sets is not enough: three exist and two
                // must remain, but "two remain" is satisfied just as well by
                // deleting the survivor's set as by deleting the ghost's. The
                // instance id is a `crypto.randomUUID()` inside the driver and is
                // not reachable from a test, so its own member is what names it.
                const ownedBefore = await reader.scanMatch(
                    keys(namespace).ownedPattern,
                )
                assertEquals(
                    ownedBefore.length,
                    3,
                    'each instance owns its own member-set before the crash',
                )
                const ownerOf = async (entry: string): Promise<string> => {
                    for (const key of ownedBefore) {
                        const reply = await reader.command('SMEMBERS', key)
                        const held = reply.type === 'array'
                            ? reply.value.flatMap((r) =>
                                r.type === 'bulk' ? [r.value] : []
                            )
                            : []
                        if (held.includes(entry)) return key
                    }
                    throw new Error(`no owned-set holds ${entry}`)
                }
                // `${channel} ${field}` — OWNED_SEP is a space, and the sweep
                // splits on the FIRST one (drivers/redis.ts, `OWNED_SEP`).
                // `GHOST_MEMBER_ID` holds two more spaces after it, so this
                // lookup is also the fixture that makes that choice observable.
                const ghostSet = await ownerOf(
                    `presence-ops ${GHOST_MEMBER_ID}`,
                )

                // The crash. No SREM, no DEL — the liveness key must lapse on its
                // own and a survivor must act on that.
                await doomed.driver.close()

                await waitFor(
                    async () =>
                        (await reader.roster(namespace, 'presence-ops'))
                            .size === 2,
                    'a survivor to sweep the dead instance’s ghost member',
                    SWEEP_TIMEOUT_MS,
                )

                // Settle past several more reconcile ticks before asserting what
                // SURVIVED. Without this the assertions race the first tick and a
                // sweep that over-reaches has not yet had a chance to do it — which
                // is exactly how the two-instance version of this test let the
                // liveness gate be deleted and still passed.
                await new Promise((resolve) =>
                    setTimeout(resolve, SWEEP_RECONCILE_MS * 4)
                )

                const roster = await reader.roster(namespace, 'presence-ops')
                assertEquals(
                    [...roster.keys()].sort(),
                    ['1', '3'],
                    'the ghost is gone and BOTH live members remain — member 3 is ' +
                        'the liveness gate: without `if (alive === 0)` the sweep ' +
                        'runs against a heartbeating peer and evicts it too',
                )

                // The dead instance is forgotten, so the reconcile stops re-checking
                // it: its owned-set is deleted and its id is out of the instances
                // set. Both inside one waitFor — the `SREM` is issued a round-trip
                // AFTER the `DEL`, so asserting it outside the wait checks a command
                // that may not have landed yet.
                await waitFor(
                    async () => {
                        const owned = await reader.scanMatch(
                            keys(namespace).ownedPattern,
                        )
                        const reply = await reader.command(
                            'SMEMBERS',
                            keys(namespace).instances,
                        )
                        const ids = reply.type === 'array'
                            ? reply.value.length
                            : 0
                        return !owned.includes(ghostSet) &&
                            owned.length === 2 &&
                            ids === 2
                    },
                    'the dead instance’s owned-set to be DELeted (its own, not a ' +
                        'live peer’s) and its id SREMed from the instances set',
                    SWEEP_TIMEOUT_MS,
                )
            },
            {
                reconcileIntervalMs: SWEEP_RECONCILE_MS,
                livenessTtlSeconds: SWEEP_LIVENESS_SECONDS,
                heartbeatIntervalMs: SWEEP_HEARTBEAT_MS,
                // Only the DOOMED instance gets the spaced id. The survivor and
                // the bystander keep numeric ids so the surviving-roster
                // assertion below stays an exact `['1', '3']` — the ghost is the
                // one whose reclamation the separator governs, and giving all
                // three spaced ids would test the parse three times while making
                // the liveness-gate assertion harder to read.
                authorize: (index) => (identity) =>
                    identity
                        ? {
                            id: index === DOOMED_INDEX
                                ? GHOST_MEMBER_ID
                                : identity.id,
                            info: { name: identity.name },
                        }
                        : false,
            },
        )
    },
)

integrationTest(
    'US5/#310: an instance whose OWN liveness key lapses does not sweep ITSELF',
    async (namespace, reader) => {
        // Row 7 of the table above, made reachable. The self-skip
        // (`id === this.instanceId`) cannot be reached from any valid
        // CONFIGURATION — #293's guard makes a heartbeat slower than half the
        // TTL unconstructible, and `start()` awaits one `#heartbeat()` before
        // arming the interval, so there is no boot window. The remaining path
        // is TRANSIENT and can only be injected: the liveness `SET` failing for
        // a window while the instance is otherwise healthy.
        //
        // ONE instance, deliberately. A peer would sweep this one the moment
        // its key lapsed — correctly, since from the peer's side that is
        // indistinguishable from a crash — and the roster would empty with or
        // without the self-skip. The claim "its OWN members survive" only means
        // something when nobody else can remove them.
        await withFaultyInstance(
            namespace,
            async ({ manager }, fault) => {
                await manager.subscribe(
                    connection('resident-conn', { id: 1, name: 'Ada' }),
                    'presence-ops',
                )
                await waitFor(
                    async () =>
                        (await reader.roster(namespace, 'presence-ops'))
                            .size === 1,
                    'the member to reach the authoritative roster',
                )
                assertEquals(
                    (await reader.scanMatch(keys(namespace).alivePattern))
                        .length,
                    1,
                    'the instance registered its liveness key before the fault',
                )

                fault.breakLivenessWrites()
                await waitFor(
                    async () =>
                        (await reader.scanMatch(keys(namespace).alivePattern))
                            .length === 0,
                    'the instance’s OWN liveness key to lapse',
                    SWEEP_TIMEOUT_MS,
                )
                assert(
                    fault.refused() > 0,
                    'the fault must actually have refused a write — a lapse ' +
                        'with zero refusals means the key expired for some ' +
                        'other reason and this scenario proves nothing',
                )

                // Several reconcile ticks with its own key gone. This is the
                // window in which a driver without the self-skip reads
                // `EXISTS 0` on ITSELF and sweeps its own members out of the
                // roster.
                await new Promise((resolve) =>
                    setTimeout(resolve, SWEEP_RECONCILE_MS * 5)
                )

                // STILL SERVING, and that is half the point: an instance that
                // had simply stopped would also keep its roster. A fresh join
                // has to reach the authoritative roster while the fault is
                // still on — only the liveness `SET` is refused, every other
                // command goes to the broker.
                await manager.subscribe(
                    connection('late-conn', { id: 2, name: 'Boris' }),
                    'presence-ops',
                )
                await waitFor(
                    async () =>
                        (await reader.roster(namespace, 'presence-ops'))
                            .size === 2,
                    'a join accepted DURING the fault to reach the roster',
                    SWEEP_TIMEOUT_MS,
                )

                await new Promise((resolve) =>
                    setTimeout(resolve, SWEEP_RECONCILE_MS * 5)
                )
                const roster = await reader.roster(namespace, 'presence-ops')
                assertEquals(
                    [...roster.keys()].sort(),
                    ['1', '2'],
                    'an instance never sweeps itself. Without the self-skip it ' +
                        'reads EXISTS 0 on its own liveness key and evicts its ' +
                        'own connected users from every presence channel it ' +
                        'holds — while still serving them.',
                )
            },
            {
                reconcileIntervalMs: SWEEP_RECONCILE_MS,
                livenessTtlSeconds: SWEEP_LIVENESS_SECONDS,
                heartbeatIntervalMs: SWEEP_HEARTBEAT_MS,
            },
        )
    },
)

integrationTest(
    "US1/#288: a deployment does NOT receive a NESTED deployment's frames, on a REAL broker",
    async (namespace, reader) => {
        // The one assertion no recording double can make. `SC-002` in
        // prefix_anchoring.test.ts proves the patterns and topics cannot match
        // under a hand-rolled `globMatches`; this proves the BROKER agrees —
        // that the model of Redis matching the whole fix rests on is the model
        // Redis actually implements.
        //
        // Nested on purpose: `<ns>` and `<ns>:eu` is the exact configuration
        // #288 was filed for, and the one an operator reaches by reading
        // "prefix" as "tenant".
        const outerPrefix = namespace
        const innerPrefix = `${namespace}:eu`
        const outer = RedisBroadcastDriver.fromConfig(brokerConfig(), {
            prefix: outerPrefix,
            control: { secret: controlSecret() },
        })
        const inner = RedisBroadcastDriver.fromConfig(brokerConfig(), {
            prefix: innerPrefix,
            control: { secret: controlSecret() },
        })
        const outerGot: string[] = []
        const innerGot: string[] = []
        try {
            outer.onMessage((m) => outerGot.push(`${m.channel}/${m.event}`))
            inner.onMessage((m) => innerGot.push(`${m.channel}/${m.event}`))
            // Instrumented, not stubbed. An empty handler proves the seam
            // exists; it cannot say whether a control frame CROSSED. #288's
            // second half is that routing must never hand a control frame to
            // the wrong deployment — so the handler has to count.
            const outerControl: string[] = []
            const innerControl: string[] = []
            outer.onControl((c) => outerControl.push(`${c.kind}/${c.target}`))
            inner.onControl((c) => innerControl.push(`${c.kind}/${c.target}`))
            // EVERY CHANNEL THIS SCENARIO USES IS WATCHED EXPLICITLY (#295).
            //
            // These two drivers are built directly rather than through
            // `withInstances`, so nothing watches on their behalf. Under
            // per-channel subscribe `onMessage` subscribes nothing at all, and
            // an unwatched channel would make the isolation assertions below
            // pass for the emptiest possible reason — nobody receiving
            // anything, which the positive controls exist to rule out.
            //
            // `orders` on BOTH: the scenario publishes it from each deployment
            // in turn, and the point is that each receives only its own.
            for (const channel of [keys(outerPrefix).probeChannel, 'orders']) {
                await outer.watchChannel(channel)
            }
            for (const channel of [keys(innerPrefix).probeChannel, 'orders']) {
                await inner.watchChannel(channel)
            }
            await outer.watchChannel('own')
            await awaitSubscribers(reader, outerPrefix, 1)
            await awaitSubscribers(reader, innerPrefix, 1)
            // The readiness gate PUBLISHes a real event on each deployment's
            // OWN topic and counts receivers, so both handlers have already
            // fired once by now — `probe-ready/probe-ready`, from itself, not
            // from the other. Dropping it here keeps the assertion below an
            // exact set rather than a filter, which is what makes an extra
            // arrival impossible to explain away.
            outerGot.length = 0
            innerGot.length = 0

            await inner.publish({
                channel: 'orders',
                event: 'created',
                data: { id: 1 },
            })
            await inner.publishControl({ kind: 'evict', target: 'conn-1' })

            // THE POSITIVE CONTROL. Without it an empty `outerGot` is equally
            // explained by "isolated" and by "the broker delivered nothing to
            // anyone" — a broker that is up but routing nothing would pass the
            // isolation assertion perfectly.
            await waitFor(
                () => innerGot.length >= 1,
                'the inner deployment received its OWN event — without this ' +
                    'the isolation assertion below is vacuous',
            )
            // Give a leak the same wall-clock the delivery above needed.
            await outer.publish({
                channel: 'own',
                event: 'ping',
                data: null,
            })
            await waitFor(
                () => outerGot.includes('own/ping'),
                'the outer deployment received its OWN event — the second ' +
                    'half of the control, and the barrier that makes the ' +
                    'absence below a measurement rather than a race',
            )

            assertEquals(
                outerGot,
                ['own/ping'],
                'the outer deployment received a frame published under the ' +
                    'nested prefix. Both prefixes are accepted, so this is a ' +
                    `configuration a user can reach — #288. Got: ${outerGot}`,
            )
            assertEquals(
                outerControl,
                [],
                'a CONTROL frame published by the nested deployment reached ' +
                    "the outer one's control handler. The MAC would not save " +
                    'it: both deployments here hold the same secret, and on a ' +
                    'shared broker that is the normal case for one operator ' +
                    'running two apps.',
            )

            // THE OTHER DIRECTION. Isolation is a claim about a PAIR, and a
            // test that only ever publishes from the inner one proves half of
            // it — the half where the nesting is deepest and a leak is most
            // likely, but still half.
            await outer.publish({
                channel: 'orders',
                event: 'created',
                data: { id: 2 },
            })
            await outer.publishControl({ kind: 'evict', target: 'conn-2' })
            await waitFor(
                () => outerGot.length >= 2,
                'the outer deployment received its own second event — the ' +
                    'barrier for the reverse-direction assertions below',
            )
            assertEquals(
                innerGot,
                ['orders/created'],
                "the nested deployment received the OUTER one's event. Its " +
                    'own is the only entry that belongs here.',
            )
            assertEquals(
                innerControl,
                [],
                "the nested deployment received the OUTER one's control frame.",
            )
        } finally {
            await outer.close()
            await inner.close()
        }
    },
)

// ---------------------------------------------------------------------------
// #295 — per-channel subscribe: the fan-out claim, measured on a real broker
// ---------------------------------------------------------------------------

integrationTest(
    '#295/SC-001: an instance receives NO frame for a channel it does not host',
    async (namespace, reader) => {
        // The whole point of the feature, and the measurement is the broker's
        // own: `PUBLISH` returns how many subscribers it delivered to. That is
        // exact, needs no client instrumentation, and — unlike counting frames
        // at a socket — cannot be satisfied by timing or by sampling at the
        // wrong moment. The pre-change figure is recorded in `baseline.md`:
        // an instance hosting `alpha` only still counted as a receiver for a
        // publish to `beta`.
        await withInstances(2, namespace, async ([a, b]) => {
            await awaitSubscribers(reader, namespace, 2)
            const topic = (channel: string) => `${namespace}__event:${channel}`
            const receivers = async (channel: string): Promise<number> => {
                const reply = await reader.command(
                    'PUBLISH',
                    topic(channel),
                    JSON.stringify({ event: 'e', data: null }),
                )
                return reply.type === 'integer' ? reply.value : -1
            }

            await a.manager.subscribe(
                connection('a1', { id: 1, name: 'a1' }),
                'alpha',
            )
            await b.manager.subscribe(
                connection('b1', { id: 2, name: 'b1' }),
                'beta',
            )
            // Both watches have to have LANDED before a count means anything.
            // The probe channel every instance holds is already proof the
            // sockets are up; this waits for these two specific subscriptions.
            await waitFor(
                async () =>
                    await receivers('alpha') === 1 &&
                    await receivers('beta') === 1,
                'each channel is hosted by exactly ONE instance',
            )

            assertEquals(
                await receivers('alpha'),
                1,
                'a publish to `alpha` must reach exactly the instance hosting ' +
                    'it — 2 would mean the prefix-wide glob is still live, 0 ' +
                    'that the watch never landed',
            )
            assertEquals(
                await receivers('beta'),
                1,
                'and `beta` likewise, on the other instance',
            )
            assertEquals(
                await receivers('gamma-nobody-hosts-this'),
                0,
                'a channel NO instance hosts must reach nobody at all. Before ' +
                    'this feature every instance under the prefix counted as a ' +
                    'receiver for it (see baseline.md).',
            )
        })
    },
)

integrationTest(
    '#295/SC-006: after the last leaver, the channel reaches nobody',
    async (namespace, reader) => {
        // Measured at the BROKER, not at delivery. A subscription with no
        // handler delivers nothing and still costs bandwidth on every publish,
        // so "the handler stopped firing" is the wrong instrument — it is green
        // for a channel whose subscription is still live.
        await withInstances(1, namespace, async ([a]) => {
            await awaitSubscribers(reader, namespace, 1)
            const receivers = async (): Promise<number> => {
                const reply = await reader.command(
                    'PUBLISH',
                    `${namespace}__event:alpha`,
                    JSON.stringify({ event: 'e', data: null }),
                )
                return reply.type === 'integer' ? reply.value : -1
            }

            await a.manager.subscribe(
                connection('a1', { id: 1, name: 'a1' }),
                'alpha',
            )
            await a.manager.subscribe(
                connection('a2', { id: 3, name: 'a2' }),
                'alpha',
            )
            await waitFor(
                async () => await receivers() === 1,
                'alpha is hosted',
            )

            await a.manager.unsubscribe('a1', 'alpha')
            assertEquals(
                await receivers(),
                1,
                'a NON-last leaver must not unsubscribe — a2 still holds it',
            )
            await a.manager.unsubscribe('a2', 'alpha')
            await waitFor(
                async () => await receivers() === 0,
                'the LAST leaver stopped the traffic at the broker',
            )
        })
    },
)

integrationTest(
    '#295/US4: a channel dropped BEFORE a fault is not resurrected by the reconnect',
    async (namespace, reader) => {
        // A subscription a reconnect brings back is a leak that only appears
        // under fault — the worst moment to discover anything — and it decays
        // the fan-out win silently, because delivery stays correct throughout.
        //
        // Measured with the broker's receiver count, not with delivery: a
        // resurrected subscription with no local subscriber delivers nothing
        // and still costs bandwidth on every publish, so "the handler stopped
        // firing" is green for exactly the defect this forbids.
        await withInstances(1, namespace, async ([a]) => {
            await awaitSubscribers(reader, namespace, 1)
            const receivers = async (channel: string): Promise<number> => {
                const reply = await reader.command(
                    'PUBLISH',
                    `${namespace}__event:${channel}`,
                    JSON.stringify({ event: 'e', data: null }),
                )
                return reply.type === 'integer' ? reply.value : -1
            }

            await a.manager.subscribe(
                connection('keep', { id: 1, name: 'keep' }),
                'kept',
            )
            await a.manager.subscribe(
                connection('drop', { id: 2, name: 'drop' }),
                'dropped',
            )
            await waitFor(
                async () =>
                    await receivers('kept') === 1 &&
                    await receivers('dropped') === 1,
                'both channels are hosted',
            )

            // The last leaver of `dropped` goes; `kept` keeps its subscriber.
            await a.manager.unsubscribe('drop', 'dropped')
            await waitFor(
                async () => await receivers('dropped') === 0,
                'the unwatch landed before the fault',
            )

            // Force the subscribe socket to fault. `CLIENT KILL TYPE pubsub`
            // reaches only subscribe-mode connections, so the reader issuing it
            // is not killing itself.
            await reader.command('CLIENT', 'KILL', 'TYPE', 'pubsub')

            // The reconnect re-issues what is still hosted…
            await waitFor(
                async () => await receivers('kept') === 1,
                'the reconnect restored the channel that is still hosted',
                15_000,
            )
            // …and only that.
            assertEquals(
                await receivers('dropped'),
                0,
                'the reconnect resurrected a channel whose last subscriber had ' +
                    'left — the re-issue set and the hosted set have diverged, ' +
                    'and nothing would have shown it until the next fault',
            )
        })
    },
)
