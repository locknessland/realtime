/**
 * @fileoverview The realtime half of the live-broker harness (#273) — the
 * pieces that know what `@lockness/realtime` writes into Redis.
 *
 * Paired with `packages/redis/tests/live_broker.ts`, which owns everything
 * broker-generic (the gate, the config, the preflight, the namespace, the
 * teardown, `waitFor`). The split follows reason-to-change: this file changes
 * when the realtime key layout or instance construction changes, that one when
 * the broker contract does.
 *
 * This module is the **single home** for four decisions (plan §5): the Redis key
 * layout the suite reads back, what counts as an authoritative read-back, where
 * the control secret comes from, and how an instance is created and disposed.
 *
 * **On read-backs.** `RedisBroadcastDriver.listMembers()` and `listRevoked()`
 * both read from Redis, so asserting through them would satisfy a naive reading
 * of "assert cross-process state" while routing every assertion straight back
 * through the parsing and semantics layer this suite exists to backstop. A suite
 * that asserts through the type under test is a unit test with a real socket
 * attached. Everything here issues raw commands on a client the suite owns.
 *
 * @module @lockness/realtime/tests/live_realtime
 */

import {
    RedisClient,
    RedisSubscribeConnection,
    type RespReply,
} from '../../redis/mod.ts'
import {
    brokerConfig,
    teardown,
    waitFor,
} from '../../redis/tests/live_broker.ts'
import { ChannelManager } from '../manager.ts'
import {
    RedisBroadcastDriver,
    type RedisCommandClient,
} from '../drivers/redis.ts'
import type { PresenceMember } from '../channel.ts'
import type { Connection } from '../types.ts'

export { waitFor }

/** The identity shape every live-broker scenario uses. */
export interface TestUser {
    id: number
    name: string
}

/**
 * The Redis key layout the suite reads back — the **single home** for it.
 *
 * A second home is forced here and must be named rather than scattered: every
 * key getter on the driver is `private` (`packages/realtime/drivers/redis.ts`
 * lines 427–462), so a raw read-back has to re-derive the layout. The failure
 * direction is the dangerous one — a test asserting a member is *absent* from
 * a mistyped key passes vacuously — which is why `roster()` below is always
 * paired with a positive read in the same test.
 *
 * @param prefix - The run's namespace, used as the driver's `prefix`.
 * @returns The key names and patterns the suite reads.
 */
/** The channel every live instance watches so the readiness gate has a target. */
const PROBE_CHANNEL = 'probe-ready'

export function keys(prefix: string): {
    presence: (channel: string) => string
    instances: string
    revocations: string
    ownedPattern: string
    alivePattern: string
    controlTopic: string
    probeTopic: string
    probeChannel: string
} {
    return {
        presence: (channel: string) => `${prefix}__presence:${channel}`,
        instances: `${prefix}__instances`,
        revocations: `${prefix}__revocations`,
        // The owning instance id is `crypto.randomUUID()` inside the driver and
        // is not reachable from here, so these two are patterns, not names.
        ownedPattern: `${prefix}__owned:*`,
        alivePattern: `${prefix}__alive:*`,
        // Topics, not keys — and they belong here for the same reason the keys
        // do. The driver derives both from `prefix`, so a suite that builds a
        // topic string inline carries a second spelling of the layout.
        //
        // An `eventTopic(channel)` helper used to sit here too and was deleted
        // by #288: a repo-wide grep found the declaration and the definition
        // and no caller. That is worse than a duplicate — a second spelling
        // nothing executes cannot go red when the layout changes, so it would
        // have survived this very rename as a silent lie. `probeTopic` below is
        // the one that is load-bearing.
        controlTopic: `${prefix}__control`,
        // THE READINESS GATE for every live test in this package.
        // `awaitSubscribers` PUBLISHes here and counts receivers, so this must
        // be a real event topic under the driver's current marker. Left on the
        // pre-#288 `${prefix}:probe-ready` it matches no subscription, every
        // count stays 0, and the whole live suite fails on a 10s timeout with
        // nothing to say why.
        //
        // Since #295 it must also be a channel each instance actually WATCHES —
        // `withInstances` does that — because a prefix-wide glob no longer
        // covers it. The failure mode is identical, which is why the warning
        // above is left standing rather than reworded.
        probeTopic: `${prefix}__event:${PROBE_CHANNEL}`,
        // The CHANNEL, not the topic — what `withInstances` watches so the
        // topic above has a subscriber at all. One home for the name: a second
        // spelling here and in the watch call is how the gate goes silently
        // dead again.
        probeChannel: PROBE_CHANNEL,
    }
}

/**
 * Mint the per-deployment control secret for one run — the **single home**.
 *
 * Generated, never a literal. `RedisBroadcastDriver` enforces a 32-byte floor
 * and refuses to publish an unsigned control frame, so the harness must supply
 * one; the tempting shortcut is a constant in a test body that then gets copied
 * into a README and shipped by an integrator as their real MAC key. A secret in
 * somebody else's deployment cannot be rotated, because you do not know whose.
 *
 * @returns 32 random bytes, hex-encoded.
 */
export function controlSecret(): string {
    const bytes = crypto.getRandomValues(new Uint8Array(32))
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

/** A connection double that records what was sent to it and whether it closed. */
export interface RecordingConnection extends Connection<TestUser> {
    /** Every frame the manager delivered, in order. */
    readonly frames: string[]
    /** How many times the manager closed this socket. */
    readonly closed: () => number
    /** Whether any delivered frame carries `event`. */
    readonly sawEvent: (event: string) => boolean
}

/**
 * Build a recording connection double.
 *
 * @param id - The transport id.
 * @param identity - The verified identity, or `null`.
 * @returns The connection, with its recorded frames reachable.
 */
export function connection(
    id: string,
    identity: TestUser | null,
): RecordingConnection {
    const frames: string[] = []
    let closes = 0
    return {
        id,
        identity,
        metadata: {},
        frames,
        send: (data) => void frames.push(String(data)),
        close: () => void closes++,
        closed: () => closes,
        sawEvent: (event: string) =>
            frames.some((frame) => frame.includes(`"${event}"`)),
    }
}

/** One live instance: a driver over its own sockets, plus its manager. */
export interface LiveInstance {
    /** The driver, over its own command and subscribe sockets. */
    readonly driver: RedisBroadcastDriver
    /** The manager wired to it. */
    readonly manager: ChannelManager<TestUser>
}

/** How `withInstances` builds its instances. */
export interface InstanceOptions {
    /** Per-instance authorizer, by index. Defaults to "any identity passes". */
    authorize?: (index: number) => (
        identity: TestUser | null,
        channel: string,
    ) => PresenceMember | false
    /** Ghost-sweep and reconcile cadence, in ms. */
    reconcileIntervalMs?: number
    /**
     * Instance-liveness key TTL, in seconds.
     *
     * The ghost sweep waits for a dead instance's `{prefix}:alive:<id>` key to
     * EXPIRE, and Redis's `EX` granularity is one second — so a sweep scenario
     * cannot run faster than this, and the default of 15 s would make it a
     * 15-second test. One second is the floor the broker allows, not a value
     * chosen for speed.
     */
    livenessTtlSeconds?: number
    /**
     * Liveness heartbeat cadence, in ms. Must stay at or under HALF
     * `livenessTtlSeconds * 1000` — the driver refuses anything slower (#293),
     * because a LIVE instance would let its own liveness key lapse between
     * beats.
     *
     * When that happens the instance does NOT sweep itself: `#reconcile` skips
     * `id === this.instanceId`, so an instance never sweeps its own members.
     * Its PEERS sweep it — each of them reads `EXISTS 0` on its liveness key
     * and removes its presence members from every roster they hold. The
     * distinction matters because that self-skip is a guard under active
     * discussion, and a comment that misattributes the sweep sends the next
     * reader to the wrong function.
     */
    heartbeatIntervalMs?: number
    /** Durable revocation marker TTL, in seconds. */
    revocationTtlSeconds?: number
    /**
     * The control-plane secret for this run. Defaults to a fresh per-run value.
     *
     * A test passes its own only when it needs to **sign a frame the way a peer
     * instance would** — #272's replay test has to produce a genuinely valid
     * frame before it can prove that replaying it is refused. Forging is not the
     * threat being tested; capture-and-repeat is.
     */
    secret?: string
}

const defaultAuthorize = (
    identity: TestUser | null,
): PresenceMember | false =>
    identity ? { id: identity.id, info: { name: identity.name } } : false

/**
 * Run `body` with `count` live instances, and close every one afterwards — the
 * **single home** for instance construction and disposal.
 *
 * Each instance is built through `RedisBroadcastDriver.fromConfig`, so it opens
 * its own command socket and its own subscribe socket, exactly as a separate
 * process would. Two managers in one Deno process is enough to prove the bus:
 * every assertion still travels through the broker, and the alternative — real
 * subprocesses — would prove nothing further while being far harder to keep
 * clean under the op sanitizer.
 *
 * Disposal is unconditional. CI runs `deno task test:leaks`, so a driver left
 * open is a red build rather than a warning, and the run that most needs
 * closing is the failing one.
 *
 * @param count - How many instances to build.
 * @param namespace - The run namespace, used as each driver's `prefix`.
 * @param body - The scenario, receiving the instances in construction order.
 * @param options - Per-instance authorizers and cadences.
 * @returns Whatever `body` returns.
 * @example
 * ```typescript
 * await withInstances(2, ns, async ([a, b]) => {
 *   await b.manager.subscribe(conn, 'private-orders')
 *   a.manager.broadcast('private-orders', 'created', { id: 1 })
 * })
 * ```
 */
export async function withInstances<T>(
    count: number,
    namespace: string,
    body: (instances: LiveInstance[]) => Promise<T>,
    options: InstanceOptions = {},
): Promise<T> {
    const secret = options.secret ?? controlSecret()
    const config = brokerConfig()
    const instances: LiveInstance[] = []
    try {
        for (let index = 0; index < count; index++) {
            const driver = RedisBroadcastDriver.fromConfig(config, {
                prefix: namespace,
                control: { secret },
                presence: {
                    reconcileIntervalMs: options.reconcileIntervalMs ?? 60_000,
                    // No `?? 15` / `?? 5_000` here (#293). Those restated the
                    // driver's own defaults in a second place, and the two are
                    // now bound by a constructor guard — a copy that drifts
                    // would make this helper build configurations the driver
                    // refuses, or worse, ones it accepts for the wrong reason.
                    // Passing `undefined` lets the single owner decide.
                    livenessTtlSeconds: options.livenessTtlSeconds,
                    heartbeatIntervalMs: options.heartbeatIntervalMs,
                },
                revocationTtlSeconds: options.revocationTtlSeconds ?? 300,
            })
            const authorize = options.authorize?.(index) ?? defaultAuthorize
            // The manager's constructor registers the delivery handler, which
            // `watchChannel` requires — so the order below is a contract, not a
            // convenience.
            const manager = new ChannelManager<TestUser>({ driver, authorize })
            // THE READINESS PROBE IS A WATCHED CHANNEL NOW (#295/FR-024).
            //
            // `awaitSubscribers` publishes to `probeTopic` and waits for the
            // receiver count to reach N. That worked while every instance held
            // one prefix-wide glob; under per-channel subscribe no instance
            // hosts `probe-ready` until a client asks for it, so the count
            // would stay 0 and every live test in this package would time out
            // at ten seconds — the symptom `keys()`'s own docstring predicts.
            //
            // Watched on the DRIVER rather than through `manager.subscribe`:
            // the gate needs a subscription on the wire, not a membership, and
            // a fake connection in `subscriptions` would leak into every
            // roster and fan-out assertion the scenarios make. A driver whose
            // subscriber cannot do per-pattern returns early here and keeps its
            // glob, so both paths reach the same gate.
            await driver.watchChannel(keys(namespace).probeChannel)
            instances.push({ driver, manager })
        }
        return await body(instances)
    } finally {
        for (const instance of instances) {
            await instance.driver.close().catch((error) =>
                console.warn(
                    `[live-realtime] a driver failed to close: ${error}`,
                )
            )
        }
    }
}

/**
 * A switch over one instance's liveness writes, handed to the scenario (#310).
 *
 * The self-skip in `#reconcile` (`id === this.instanceId`) is unreachable from
 * any valid CONFIGURATION — #293's guard makes a heartbeat slower than half the
 * TTL unconstructible, and `start()` awaits one `#heartbeat()` before arming the
 * interval, so there is no boot window either. The one path that still reaches
 * it is TRANSIENT: the liveness `SET` failing for a window while the instance is
 * otherwise healthy and still serving. That cannot be configured, only injected.
 */
export interface LivenessFault {
    /**
     * Start refusing `SET {prefix}__alive:*`. Every other command — the
     * heartbeat's own `SADD`, the reconcile's `EXISTS`, every roster write —
     * still goes to the broker, which is what keeps the instance *alive* rather
     * than merely stopped.
     */
    breakLivenessWrites(): void
    /** Stop refusing them. */
    healLivenessWrites(): void
    /** How many liveness writes have been refused so far. */
    refused(): number
}

/**
 * Run `body` with ONE live instance whose liveness writes can be broken.
 *
 * Built from injected ports rather than `fromConfig`, because the command port
 * has to be wrapped — so this owns its two sockets explicitly (`fromConfig`'s
 * `owned` list is what `close()` normally walks, and an injected-port driver
 * has none).
 *
 * **One instance, not two, and that is the whole design.** A peer would sweep
 * this instance the moment its liveness key lapsed — correctly, since from the
 * peer's side it is indistinguishable from a crash — and the roster would empty
 * whether or not the self-skip is present. The assertion "its OWN members
 * survive" is only meaningful with nobody else able to remove them.
 *
 * The refusal is a THROW, not a silent success: `#heartbeat` catches and logs
 * at WARN, which is exactly why the instance carries on serving while its own
 * key expires underneath it.
 *
 * @param namespace - The run namespace, used as the driver's `prefix`.
 * @param body - The scenario, receiving the instance and the fault switch.
 * @param options - Cadences, as for {@link withInstances}.
 * @returns Whatever `body` returns.
 * @example
 * ```typescript
 * await withFaultyInstance(ns, async ({ manager }, fault) => {
 *   await manager.subscribe(conn, 'presence-ops')
 *   fault.breakLivenessWrites()
 * })
 * ```
 */
export async function withFaultyInstance<T>(
    namespace: string,
    body: (instance: LiveInstance, fault: LivenessFault) => Promise<T>,
    options: InstanceOptions = {},
): Promise<T> {
    const config = brokerConfig()
    const client = new RedisClient(config)
    const subscriber = new RedisSubscribeConnection(config)
    const alivePrefix = `${namespace}__alive:`
    let broken = false
    let refused = 0
    const command: RedisCommandClient = {
        command: (...args: string[]) => {
            if (
                broken && args[0] === 'SET' && args[1]?.startsWith(alivePrefix)
            ) {
                refused++
                return Promise.reject(
                    new Error('injected: liveness write refused (#310)'),
                )
            }
            return client.command(...args)
        },
    }
    const driver = new RedisBroadcastDriver(command, subscriber, {
        prefix: namespace,
        control: { secret: options.secret ?? controlSecret() },
        presence: {
            reconcileIntervalMs: options.reconcileIntervalMs ?? 60_000,
            livenessTtlSeconds: options.livenessTtlSeconds,
            heartbeatIntervalMs: options.heartbeatIntervalMs,
        },
        revocationTtlSeconds: options.revocationTtlSeconds ?? 300,
    })
    const authorize = options.authorize?.(0) ?? defaultAuthorize
    const instance: LiveInstance = {
        driver,
        manager: new ChannelManager<TestUser>({ driver, authorize }),
    }
    try {
        return await body(instance, {
            breakLivenessWrites: () => void (broken = true),
            healLivenessWrites: () => void (broken = false),
            refused: () => refused,
        })
    } finally {
        await driver.close().catch((error) =>
            console.warn(
                `[live-realtime] the faulty driver failed to close: ${error}`,
            )
        )
        await subscriber.close().catch(() => {})
        await client.close().catch(() => {})
    }
}

/**
 * Block until `count` instances are actually subscribed to the run's patterns.
 *
 * Redis pub/sub is at-most-once and `psubscribe` is fire-and-forget, so a
 * broadcast issued between construction and the `PSUBSCRIBE` landing on the
 * wire is simply lost — and the test that follows fails for a reason that has
 * nothing to do with the behaviour it meant to check. `PUBLISH` returns the
 * number of clients that received the message, which is the broker's own answer
 * to "is everyone listening yet".
 *
 * Both probes stay inside the run's namespace, so this never touches a topic
 * the run does not own. The control probe is deliberately unsigned: the driver
 * drops it after counting it, which is exactly what is wanted — the count is
 * the signal, the payload is not.
 *
 * That drop raises `realtime: dropped a control message of invalid shape` in the
 * log, once per probe round, and it is left there deliberately. Since #272 the
 * shape gate also requires an integer `ts` and a fixed-width `nonce`, so `'{}'`
 * still trips it — earlier now, and for more reasons, but with the same
 * message. If that message ever changes, this comment and the probe's rationale
 * go stale together. Suppressing it
 * cannot be done honestly — the WARN is raised on the subscribe socket's read
 * loop, after this function has already returned — so an assignment to
 * `console.warn` here would look like it worked while doing nothing. The line is
 * expected output, and it is evidence the FR-015 MAC check is live against a
 * real broker.
 *
 * @param reader - A raw client to publish the probes on.
 * @param namespace - The run namespace.
 * @param count - How many instances must be listening.
 * @param timeoutMs - How long to wait before giving up.
 * @throws {Error} When they are not all listening before the deadline.
 */
export async function awaitSubscribers(
    reader: Reader,
    namespace: string,
    count: number,
    timeoutMs = 10_000,
): Promise<void> {
    const receivers = async (
        topic: string,
        payload: string,
    ): Promise<number> => {
        const reply = await reader.command('PUBLISH', topic, payload)
        return reply.type === 'integer' ? reply.value : 0
    }
    const topics = keys(namespace)
    let events = 0
    let control = 0
    try {
        // The project's one poll-until-true home (plan §5 row 6). A second loop
        // here would be a second spelling of the same decision.
        await waitFor(
            async () => {
                events = await receivers(
                    topics.probeTopic,
                    JSON.stringify({ event: 'probe-ready', data: null }),
                )
                control = await receivers(topics.controlTopic, '{}')
                return events >= count && control >= count
            },
            `${count} instance(s) to subscribe under ${namespace}`,
            timeoutMs,
        )
    } catch {
        throw new Error(
            `[live-realtime] ${count} instance(s) never subscribed under ` +
                `${namespace}: last seen ${events} event and ${control} ` +
                'control subscriber(s)',
        )
    }
}

/**
 * A raw client for read-backs and teardown, closed by {@link withReader}.
 *
 * Separate from every driver's own client on purpose: an assertion issued on
 * the socket under test is not an independent observation of it.
 */
export interface Reader {
    /** Issue a raw command. */
    command(...args: string[]): Promise<RespReply>
    /** The channel's authoritative roster, by member id, via `HGETALL`. */
    roster(prefix: string, channel: string): Promise<Map<string, unknown>>
    /**
     * The revocation index members whose expiry score is still in the FUTURE,
     * via `ZRANGEBYSCORE key <now> +inf`.
     *
     * Bounded by score on purpose. A `-inf +inf` read returns every member
     * regardless of expiry, so it cannot tell a live revocation from one the
     * driver scored into the past — and scoring them into the past is a
     * fail-open defect, revocations dead on arrival. Reviewed and confirmed by
     * mutation: with `-inf +inf`, flipping the driver's `t + ARGV[1]` to
     * `t - ARGV[1]` left every test in this suite green.
     */
    revoked(prefix: string): Promise<string[]>
    /** Every member regardless of score — for asserting what was REAPED. */
    revokedAtAnyScore(prefix: string): Promise<string[]>
    /** The broker's own clock, in epoch seconds (`TIME`). */
    now(): Promise<number>
    /**
     * Every key that exists under the run's namespace, via `SCAN … MATCH`.
     *
     * The only honest way to assert a key was NOT written. A probe that seeds
     * and re-reads the same helper constant proves the key is writable, not
     * that it is the name the driver uses — a consistently mistyped constant
     * satisfies it perfectly. Enumerating what the driver actually created is
     * independent of what this file believes the layout to be.
     */
    scanKeys(namespace: string): Promise<string[]>
    /**
     * Every key matching a glob, via `SCAN … MATCH` — the read-back for the
     * patterns {@link keys} exposes.
     *
     * `ownedPattern` and `alivePattern` are patterns rather than names because
     * the owning instance id is a `crypto.randomUUID()` inside the driver and
     * is not reachable from a test. Without this, a caller re-spells the layout
     * inline as `` `${prefix}:owned:` `` — a second home for the one decision
     * this module exists to hold.
     *
     * **The pattern MUST be anchored to the run's namespace, and this is
     * enforced rather than asked for.** Unlike {@link Reader.scanKeys}, which
     * builds `<namespace>*` itself and structurally cannot escape, this takes
     * a caller's glob — so on a SHARED broker an unanchored one would scan
     * another run's keys and report them as this run's. Every current caller
     * passes a `keys(namespace).*Pattern` and is unaffected; the check exists
     * for the caller that has not been written yet.
     *
     * @throws {Error} If `pattern` does not begin with the run's namespace.
     */
    scanMatch(pattern: string): Promise<string[]>
    /** A key's TTL in seconds: `-1` = no TTL, `-2` = absent. */
    ttlOf(key: string): Promise<number>
    /** How many members a sorted set holds. */
    zcard(key: string): Promise<number>
}

/**
 * Run `body` with a raw reader, then tear the run's namespace down.
 *
 * The teardown is in a `finally`, so a failing assertion still cleans up. That
 * is the run it matters for: a suite that only tidies after a green run leaves
 * residue on somebody else's broker exactly when it has just gone wrong.
 *
 * @param namespace - The run namespace to read and then remove.
 * @param body - The scenario, receiving the reader.
 * @returns Whatever `body` returns.
 */
export async function withReader<T>(
    namespace: string,
    body: (reader: Reader) => Promise<T>,
): Promise<T> {
    const client = new RedisClient(brokerConfig())
    /** The BROKER's clock — never the test process's, which may differ by seconds. */
    const readNow = async (): Promise<number> => {
        const reply = await client.command('TIME')
        const seconds = reply.type === 'array' ? reply.value[0] : undefined
        return seconds?.type === 'bulk' ? Number(seconds.value) : 0
    }
    const readZRange = async (key: string, min: string): Promise<string[]> => {
        const reply = await client.command('ZRANGEBYSCORE', key, min, '+inf')
        if (reply.type !== 'array') return []
        return reply.value
            .map((r) => (r.type === 'bulk' ? r.value : undefined))
            .filter((r): r is string => r !== undefined)
    }
    /**
     * One `SCAN … MATCH` loop, shared by both read-backs.
     *
     * Cursor-based rather than `KEYS`: `KEYS` blocks the broker for the length
     * of the scan, and a test suite that does it to a shared development
     * instance is indistinguishable from an outage.
     */
    const scan = async (pattern: string): Promise<string[]> => {
        let cursor = '0'
        const found: string[] = []
        do {
            const reply = await client.command(
                'SCAN',
                cursor,
                'MATCH',
                pattern,
                'COUNT',
                '500',
            )
            if (reply.type !== 'array' || reply.value.length !== 2) break
            const next = reply.value[0]
            cursor = next.type === 'bulk' ? next.value : '0'
            const batch = reply.value[1]
            if (batch.type !== 'array') continue
            for (const entry of batch.value) {
                if (entry.type === 'bulk') found.push(entry.value)
            }
        } while (cursor !== '0')
        return found.sort()
    }

    const reader: Reader = {
        command: (...args: string[]) => client.command(...args),
        roster: async (prefix: string, channel: string) => {
            const reply = await client.command(
                'HGETALL',
                keys(prefix).presence(channel),
            )
            const out = new Map<string, unknown>()
            if (reply.type !== 'array') return out
            for (let i = 0; i + 1 < reply.value.length; i += 2) {
                const field = reply.value[i]
                const value = reply.value[i + 1]
                if (field.type !== 'bulk' || value.type !== 'bulk') continue
                out.set(field.value, JSON.parse(value.value))
            }
            return out
        },
        revoked: async (prefix: string) => {
            const now = await readNow()
            return await readZRange(keys(prefix).revocations, String(now))
        },
        revokedAtAnyScore: (prefix: string) =>
            readZRange(keys(prefix).revocations, '-inf'),
        now: () => readNow(),
        scanKeys: (ns: string) => scan(`${ns}*`),
        scanMatch: (pattern: string) => {
            // See the port's JSDoc: anchored by CHECK, not by convention. The
            // namespace is in scope here because `withReader` takes it, so no
            // caller has to pass it twice.
            if (!pattern.startsWith(namespace)) {
                throw new Error(
                    `scanMatch pattern ${JSON.stringify(pattern)} is not ` +
                        `anchored to the run namespace ${
                            JSON.stringify(namespace)
                        }. On a shared broker it would scan another run's ` +
                        "keys and report them as this one's — derive it from " +
                        '`keys(namespace)`.',
                )
            }
            return scan(pattern)
        },
        ttlOf: async (key: string) => {
            const reply = await client.command('TTL', key)
            return reply.type === 'integer' ? reply.value : -2
        },
        zcard: async (key: string) => {
            const reply = await client.command('ZCARD', key)
            return reply.type === 'integer' ? reply.value : 0
        },
    }
    try {
        return await body(reader)
    } finally {
        // Never silent: this is the only thing standing between a failing run
        // and residue on somebody else's broker, and a swallowed failure here
        // is invisible precisely when it matters.
        await teardown(client, namespace).catch((error) =>
            console.error(
                `[live-realtime] TEARDOWN FAILED for ${namespace} — keys may ` +
                    `remain on the broker. Remove them with: ` +
                    `SCAN 0 MATCH ${namespace}* . Cause: ${error}`,
            )
        )
        await client.close()
    }
}
