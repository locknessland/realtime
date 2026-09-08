/**
 * @fileoverview #282 — a recording double over both of the Redis driver's ports.
 *
 * **It models no Redis semantics.** It records the argv of every `command(...)`
 * and the pattern of every `psubscribe`, and answers from a canned-reply table.
 * That is deliberate and it is the whole reason this file exists rather than a
 * `FakeRedis`: a double that models command behaviour can model it *wrongly*,
 * and a test whose reach is decided by a wrong model is a test agreeing with a
 * second bug ([#280](https://github.com/locknessland/lockness-monorepo/issues/280)
 * documents that happening twice, over a security control, with a green suite).
 * Nothing here can be wrong about Redis, because nothing here claims anything
 * about Redis.
 *
 * **Why the port and not the broker.** Containment — "every name the driver
 * derives sits under its prefix" — has to be observed somewhere that sees every
 * derived name. A keyspace scan does not: five of the driver's ten names never
 * become a key at all (two `PUBLISH`/`PSUBSCRIBE` arguments, the subscribe
 * pattern itself, and two keys `drivers/redis.ts:889` documents as read and
 * never written). Both dependencies are constructor-injected, so the port sees
 * all ten — and needs no broker to do it.
 *
 * @module @lockness/realtime/tests/recording_ports
 */

import type { RedisCommandClient, RedisSubscriber } from '../drivers/redis.ts'

/** One recorded `psubscribe`, with the handler kept so a test can drive it. */
export interface RecordedSubscription {
    pattern: string
    handler: (topic: string, payload: string) => void
}

/** What the recorder saw. */
export interface PortRecording {
    /** Every `command(...)` argv, in order. */
    commands: string[][]
    /** Every `psubscribe`, in order. */
    subscriptions: RecordedSubscription[]
    /** Every string that crossed either port — argv elements and patterns. */
    strings(): string[]
}

/**
 * A canned reply table: exact command name (upper-cased) to the reply to return.
 *
 * A function receives the argv, so a test can vary a reply without the double
 * acquiring state. Anything unlisted answers `null`, which every driver read
 * path treats as "absent".
 */
export type CannedReplies = Record<
    string,
    unknown | ((args: string[]) => unknown)
>

/** Both ports plus the recording they share. */
export interface RecordingPorts {
    command: RedisCommandClient
    subscriber: RedisSubscriber
    recording: PortRecording
}

/**
 * Build a recording pair of ports.
 *
 * @param canned - Replies by command name, upper-cased. Unlisted commands answer
 *   `null`.
 * @returns The two ports and the recording they write into.
 * @example
 * ```typescript
 * const { command, subscriber, recording } = recordingPorts({
 *   SMEMBERS: [{ type: 'bulk', value: 'conn-1' }],
 * })
 * const driver = new RedisBroadcastDriver(command, subscriber, { prefix: 'app' })
 * await driver.publish('room', { event: 'e', data: {} })
 * recording.strings() // every string the driver put on either port
 * ```
 */
export function recordingPorts(canned: CannedReplies = {}): RecordingPorts {
    const commands: string[][] = []
    const subscriptions: RecordedSubscription[] = []

    const recording: PortRecording = {
        commands,
        subscriptions,
        strings: () => [
            ...commands.flat(),
            ...subscriptions.map((s) => s.pattern),
        ],
    }

    return {
        command: {
            command: (...args: string[]) => {
                commands.push([...args])
                const reply = canned[(args[0] ?? '').toUpperCase()]
                return Promise.resolve(
                    typeof reply === 'function'
                        ? (reply as (a: string[]) => unknown)(args)
                        : reply ?? null,
                )
            },
        },
        subscriber: {
            psubscribe: (pattern, handler) => {
                subscriptions.push({ pattern, handler })
            },
            // The per-pattern pair (#295), recorded into the SAME list. Every
            // anchoring assertion in `prefix_anchoring.test.ts` reads
            // `subscriptions`, so a second list would put the per-channel
            // topics — the ones this feature actually puts on the wire —
            // outside the suite that exists to prove they are anchored.
            //
            // Both members present, deliberately: the driver detects them as a
            // set, so offering one would silently keep the prefix-wide glob and
            // every test here would exercise the path this feature replaces.
            subscribeOne: (pattern, handler) => {
                subscriptions.push({ pattern, handler })
            },
            unsubscribeOne: (pattern) => {
                const i = subscriptions.findIndex((s) => s.pattern === pattern)
                if (i >= 0) subscriptions.splice(i, 1)
            },
        },
        recording,
    }
}

/**
 * The separators the driver uses after its prefix, enumerated.
 *
 * `:` for keys and event topics, `__` for the control topic — which is exactly
 * why a containment check may not assume `:`. Anything else is unanchored.
 */
export const ANCHOR_SEPARATORS: readonly string[] = [':', '__']

/**
 * The separator-aware anchoring predicate — the single home for what "anchored"
 * means (#282, plan §5 row 1).
 *
 * **Not `startsWith`.** That definition passed `${prefix}:*`, which matched a
 * NESTED deployment's topics: with `app` and `app:eu` on one broker, `app:*`
 * matched `app:eu:orders`, and the inner deployment's event payloads reached
 * the outer one's subscribers. (It matched `app:eu__control` too, but those
 * frames were dropped on ingest for carrying no `event` field — the MAC was
 * never *bypassed*, only never *reached*.) A containment test built on
 * `startsWith` went green on exactly that. #288 closed it by moving every
 * derived name behind a `__`-leading separator no accepted prefix may contain.
 *
 * **Anchoring is not exclusivity, and this predicate only decides anchoring.**
 * `isAnchored('app___event:x', 'app')` is `true` — that name belongs to a
 * deployment at `app_`, and it is still anchored under `app` by this
 * definition. Whether two deployments can reach each other is a property of the
 * MATCH, tested by `SC-002`'s pair table. Reading a green `SC-001` as evidence
 * of isolation is the mistake this paragraph exists to prevent.
 *
 * A name is anchored when it **is** the prefix, or continues it with one of the
 * separators the driver actually uses.
 *
 * **What this does NOT decide.** `isAnchored('app:*', 'app')` is `true`, and
 * that is correct: the pattern *is* anchored. Its defect is that its glob
 * reaches into a nested deployment, which is a property of the **match**, not of
 * the prefix — so it is asserted separately (US2/SC-002) rather than folded in
 * here. Conflating the two would give one predicate two jobs and let a green
 * result be read as covering both.
 *
 * @param name - The derived name to judge.
 * @param prefix - The configured prefix.
 * @returns Whether `name` is anchored under `prefix`.
 * @example
 * ```typescript
 * isAnchored('app:room', 'app')      // true
 * isAnchored('app__control', 'app')  // true  — no `:`, still anchored
 * isAnchored('appx', 'app')          // false — continues without a separator
 * isAnchored('app', 'app')           // true  — the prefix itself
 * ```
 */
export function isAnchored(name: string, prefix: string): boolean {
    if (name === prefix) return true
    for (const separator of ANCHOR_SEPARATORS) {
        if (name.startsWith(`${prefix}${separator}`)) return true
    }
    return false
}
