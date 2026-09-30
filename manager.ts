/**
 * @fileoverview `ChannelManager` — the broadcaster: subscribe/unsubscribe with
 * authorization, channel fan-out over a driver, presence, eviction, and the
 * per-client `send` that satisfies `@lockness/notification`'s `BroadcasterLike`.
 *
 * The subscription set for a channel holds **only authorized** connections
 * (subscribe gates it), so both local and Redis-received delivery fan out only
 * to authorized subscribers on this instance (security S1/S6). Per-client
 * `send(clientId,…)` is a targeted send, never a fan-to-all.
 *
 * @module @lockness/realtime/manager
 */

import { renderError, safeForLog } from '@lockness/contract'
import { triggerDeprecation } from '@lockness/deprecation-contracts'
import { isPresenceMemberWire, isValidName } from './protocol.ts'
import { admitPresenceMember } from './presence_member.ts'
import { markedFallbackMarker, writeMarkedFallback } from './marked_fallback.ts'

/**
 * The DEFAULT per-instance watched-channel cap (#295/FR-017, #322).
 *
 * **The couplings belong to this default, not to the cap.** This number is also
 * the N that #295's SC-007 proves a full reconnect re-issue at, and the bound
 * its R-8 puts on the post-outage revocation window. A deployment that raises
 * `maxWatchedChannels` past it voids **both**: the re-issue is proven at 1 000,
 * not at whatever was chosen, and the revocation window widens with the set.
 * That is a legitimate trade — it is not a free one, and `docs/realtime.md`'s
 * upgrade note says so where an operator actually picks the number.
 */
export const MAX_WATCHED_CHANNELS = 1_000

/** The DEFAULT per-connection watched-channel cap (#295/FR-017, #322). */
export const MAX_CHANNELS_PER_CONNECTION = 100

/**
 * The DEFAULT ceiling on a serialized {@link PresenceMember}, in bytes (#326).
 *
 * **It is half the default control-payload ceiling, and that is the whole
 * relationship.** A member is announced inside a control frame that also
 * carries `kind`, `target` and `channel`; `target` and `channel` are each
 * bounded at `MAX_NAME_LENGTH` characters and `kind` is a short literal,
 * so the envelope around the member cannot exceed roughly 1 KiB even when every
 * character escapes. Against the driver's 8 KiB default that leaves the
 * envelope four times the room it can ever need, so **a member admitted here
 * can always be announced**.
 *
 * That invariant is the point, not the number. `member.id` is bounded because
 * the roster write happens BEFORE the control publish, so an unbounded value
 * lands in the authoritative hash while the frame announcing it is dropped —
 * a member present in the room and invisible to every peer. `info` is the
 * larger of the two fields and the one an end user typically controls, through
 * an ordinary profile edit, so without this it is a self-service cloak.
 *
 * **Raising `control.maxPayloadBytes` does not raise this.** They are separate
 * numbers on separate objects — the ceiling is a driver option, this is a
 * manager option — and a deployment that raises one must raise the other, or
 * admit members it cannot announce.
 */
export const MAX_PRESENCE_MEMBER_BYTES = 4 * 1024

/**
 * The DEFAULT bound on how many members one presence `subscribe` returns
 * (#339).
 *
 * A room's population has no ceiling, so without this a subscribe's reply was
 * the whole room: 10 000 members at {@link MAX_PRESENCE_MEMBER_BYTES} put
 * 40 970 001 bytes into one result, on every join and every re-join. With it,
 * one reply holds at most K·(M+1)+1 bytes of member JSON — 409 701 at the
 * defaults — however large the room.
 *
 * **The reply and the read are both bounded.** The reply holds at most K
 * members. The driver's read (#341) fetches at most K members plus the own
 * entries of the callers that read serves — one per distinct member id, at
 * most `MAX_ROSTER_READ_SELF_IDS` — so what the instance ingests does not grow
 * with the room either.
 */
export const MAX_PRESENCE_SNAPSHOT_MEMBERS = 100

/**
 * The DEFAULT bound on {@link ChannelManager}'s pending owed-release ledger —
 * the roster releases a failed write left un-applied, retried by the
 * driver's `onRosterMaintenance` drain (#371).
 *
 * **Sized like this file's other named bounds** (`MAX_WATCHED_CHANNELS`,
 * `MAX_ROSTER_READ_SELF_IDS`): a round number an operator can reason about,
 * not a measurement. Past it, a NEW slot's failure is refused rather than
 * queued — the ghost sweep is what recovers it instead — while a slot
 * ALREADY queued keeps coalescing onto its newest failure regardless of the
 * count, so the cap bounds distinct pending slots, never retries of one.
 */
export const MAX_PENDING_ROSTER_RELEASES = 1_000

/**
 * The marker that starts the one ERROR line written when a control-frame
 * revocation's own WARN could not be (#376): a `console.warn` that throws
 * inside the apply's catch, so the fire-and-forget apply rejected. The
 * rejection is rendered after it; the marker is the fixed prefix, so an error
 * text cannot forge it (#369). Exported for the test suite only — not
 * re-exported from `mod.ts`.
 */
export const REVOCATION_APPLY_LOG_FAILED = markedFallbackMarker(
    'realtime: a control-frame revocation log line could not be written (#376):',
)

/**
 * The marker that starts the default `onPublishError` line: a broadcast's
 * publish failed and the caller supplied no sink. The error is rendered after
 * it. `broadcast` discards the publish promise, so the line is written by
 * `writeMarkedFallback`, which never throws (#395). Exported for the test
 * suite only — not re-exported from `mod.ts`.
 */
export const PUBLISH_FAILED = markedFallbackMarker(
    'realtime: broadcast publish failed:',
)

/**
 * The marker that starts the one ERROR line written when a #323 join
 * compensation's own WARN could not be (#373): a `console.warn` that throws
 * inside `#joinPresence`'s catch, for either the leave's failure or the
 * reclaim's. That catch always ends by re-throwing the ORIGINAL roster
 * error, so a throwing sink must not be allowed to replace it with a WARN
 * line's own exception — the line carries the compensation failure and the
 * sink's, each rendered. Exported for the test suite only — not re-exported
 * from `mod.ts`.
 */
export const JOIN_COMPENSATION_LOG_FAILED = markedFallbackMarker(
    'realtime: a #323 join-compensation failure could not be logged (#373):',
)

/**
 * The marker that starts the one ERROR line written when an owed-release
 * ledger enqueue's own WARN could not be (#371): a `console.warn` that throws
 * inside `#recordOwedRelease`, called from `unsubscribe`'s and
 * `#joinPresence`'s catches — both end by re-throwing a roster failure of
 * their own, so a throwing sink here must not replace it. Exported for the
 * test suite only — not re-exported from `mod.ts`.
 */
export const OWED_RELEASE_LOG_FAILED = markedFallbackMarker(
    'realtime: an owed-release enqueue log line could not be written (#371):',
)

/**
 * The marker that starts the one ERROR line written when a `disconnect(id)`
 * teardown failed while its id-form deprecation notice ALSO threw (#392,
 * `STRICT_DEPRECATIONS`), and the WARN that reports the teardown failure could
 * not be written either. The deprecation error wins the call's rejection, so
 * the teardown failure is never re-thrown; it is logged instead, and a
 * throwing sink must not drop it silently. Exported for the test suite only —
 * not re-exported from `mod.ts`.
 */
export const DISCONNECT_TEARDOWN_LOG_FAILED = markedFallbackMarker(
    'realtime: a disconnect teardown failure could not be logged (#392):',
)

/**
 * The marker that starts the one ERROR line written when `evict`'s own
 * durable-revocation WARN could not be (#395 part 2): a `console.warn` that
 * throws inside the catch around `markRevocation`, before the revocation is
 * applied locally or published to the owning instance. Without a guard, that
 * throw would abort `evict` right there — the durability write already
 * failed, and skipping the apply too would leave the connection revoked
 * NOWHERE, local or remote, breaking the very contract this method documents:
 * a failed durability write never cancels the revocation. Exported for the
 * test suite only — not re-exported from `mod.ts`.
 */
export const EVICT_DURABILITY_LOG_FAILED = markedFallbackMarker(
    'realtime: an evict durability-failure WARN could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when `revokeChannel`'s own
 * durable-revocation WARN could not be (#395 part 2) — the same hazard as
 * {@link EVICT_DURABILITY_LOG_FAILED}, one channel scope over: a throwing sink
 * here must not skip the local apply or the control-frame publish that follow.
 * Exported for the test suite only — not re-exported from `mod.ts`.
 */
export const REVOKE_CHANNEL_DURABILITY_LOG_FAILED = markedFallbackMarker(
    'realtime: a revokeChannel durability-failure WARN could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when `#recheckRevocations`'s
 * own wrapper WARN could not be (#395, security review HIGH): a `console.warn`
 * that throws inside the `apply` closure's catch, after `#applyRevocation`
 * itself already failed (its own WARN included). Without a guard, that throw
 * would escape `apply`, abort the `for` loop mid-pass, and skip every
 * revocation still queued behind the failing one — breaking #349's "one
 * revocation that throws never stops the ones after it". Exported for the
 * test suite only — not re-exported from `mod.ts`.
 */
export const RECHECK_REVOCATION_LOG_FAILED = markedFallbackMarker(
    'realtime: a durable-revocation recheck failure could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when `#teardownChannels`'s
 * "also failed" WARN could not be (#395, security review LOW): a
 * `console.warn` that throws inside the catch for a SECOND (or later) channel
 * whose teardown failed. Without a guard, that throw would abort the loop
 * before any later channel's roster release / `left` announcement / cap
 * release ran. Exported for the test suite only — not re-exported from
 * `mod.ts`.
 */
export const TEARDOWN_CHANNEL_LOG_FAILED = markedFallbackMarker(
    'realtime: a teardown channel-failure WARN could not be logged (#395):',
)

/**
 * The marker that starts the one ERROR line written when `emitPresence`'s
 * per-connection delivery WARN could not be (#395, security review LOW): a
 * `console.warn` that throws inside the catch around one socket's `send`.
 * Without a guard, that throw would abort the fan-out loop before any later
 * local subscriber received the frame. Exported for the test suite only — not
 * re-exported from `mod.ts`.
 */
export const EMIT_PRESENCE_LOG_FAILED = markedFallbackMarker(
    'realtime: a presence fan-out WARN could not be logged (#395):',
)

/**
 * Refuse a cap that is not a positive integer, at construction.
 *
 * @param option - The option's name, so the message names what to fix.
 * @param value - The resolved value.
 * @throws {Error} If `value` is not a positive integer.
 */
function assertCap(option: string, value: number): void {
    if (!Number.isInteger(value) || value <= 0) {
        throw new Error(
            `realtime: ${option} must be a positive integer, received ` +
                `${
                    JSON.stringify(value)
                }. A cap discovered at the thousandth ` +
                'subscribe is a misconfiguration discovered in production.',
        )
    }
}

/**
 * Refuse a reservation share outside `(0, 1]`, at construction.
 *
 * A fraction, not a count — {@link assertCap}'s integer test would refuse every
 * legitimate value.
 *
 * @param option - The option's name.
 * @param value - The resolved value.
 * @throws {Error} If `value` is not a finite number in `(0, 1]`.
 */
function assertShare(option: string, value: number): void {
    if (!Number.isFinite(value) || value <= 0 || value > 1) {
        throw new Error(
            `realtime: ${option} must be a number greater than 0 and at most ` +
                `1, received ${JSON.stringify(value)}. Use 1 to disable the ` +
                'reservation.',
        )
    }
}

/**
 * Render a cap breach for a human, without the numbers.
 *
 * The count and the limit stay on the error as properties; they are deliberately
 * absent from the message, which many applications forward to the client as a
 * close-frame reason. The instance-wide count is a load signal covering every
 * other user of the deployment, and the caller that triggers this ran no
 * authorizer when the channel is public.
 *
 * @param scope - The breached scope; unknown values render generically.
 * @returns The message body.
 */
function renderLimitBreach(scope: string): string {
    if (scope === 'connection') {
        return 'this connection is at its watched-channel limit'
    }
    if (scope === 'instance-anonymous') {
        return 'connections with no identity have used their share of this ' +
            "instance's watched-channel budget, which is reserved so an " +
            'anonymous socket cannot deny hosting to identified connections'
    }
    if (scope === 'instance') {
        return 'this instance is at its watched-channel limit'
    }
    return `this ${scope} is at its watched-channel limit`
}

/**
 * The scopes a cap breach can carry: `'instance'`, `'connection'` and
 * `'instance-anonymous'` today.
 *
 * **Deliberately `string`, not a union of those three.** This set is OPEN — it
 * gained `'instance-anonymous'` in #322 and may gain more — and a closed union
 * would let an application write an exhaustive `switch` that the next addition
 * silently breaks at every catch site. Typing it as `string` makes that
 * mistake unwriteable: handle an unrecognised scope as a generic cap breach.
 *
 * @see {@link CHANNEL_LIMIT_SCOPES} for the values known at this version.
 */
export type ChannelLimitScope = string

/**
 * The cap-breach scopes this version raises.
 *
 * Provided so an application can compare against a named constant rather than
 * a magic string, without gaining a closed type it could switch on
 * exhaustively — see {@link ChannelLimitScope}.
 */
export const CHANNEL_LIMIT_SCOPES = [
    'instance',
    'connection',
    'instance-anonymous',
] as const

/**
 * Raised when a subscribe would take this instance or this connection past its
 * watched-channel cap (#295/FR-017, #322).
 *
 * **A throw, not `{ ok: false }`.** `subscribe`'s contract is that a denied
 * subscribe answers `{ ok: false }` and a caller bug throws; a cap breach is
 * neither — it is resource exhaustion driven by a legitimate client. Answering
 * `{ ok: false }` would make it indistinguishable from an authorization denial
 * in every application's client code.
 *
 * **The numbers are on the error, not in the message.** `count` and `limit` are
 * what server-side logging should read; the message omits them because an
 * application that forwards it to the client would hand an unauthenticated
 * caller a live load signal for the whole deployment.
 *
 * @example
 * ```ts
 * try {
 *     await manager.subscribe(connection, 'public-feed')
 * } catch (error) {
 *     if (error instanceof ChannelLimitError) {
 *         log.warn('cap breach', { scope: error.scope, count: error.count })
 *     }
 * }
 * ```
 */
export class ChannelLimitError extends Error {
    /** Always `'ChannelLimitError'`, for logs and `onError`. */
    override readonly name = 'ChannelLimitError'

    /**
     * Build the refusal. The numbers ride on the error, never in the message.
     *
     * @param scope - Which cap was breached. An open set — see
     *   {@link ChannelLimitScope}.
     * @param count - The count at the moment of the breach.
     * @param limit - The cap that was reached.
     */
    constructor(
        readonly scope: ChannelLimitScope,
        readonly count: number,
        readonly limit: number,
    ) {
        super(
            `realtime: ${
                renderLimitBreach(scope)
            }. Each watched channel is a ` +
                'broker subscription re-issued on every reconnect, so the set ' +
                'is bounded deliberately — read `count` and `limit` for the ' +
                'numbers, raise the limit knowing the reconnect cost, or ' +
                'shard the deployment.',
        )
    }
}

/**
 * A channel name the control plane cannot carry (#314).
 *
 * `ChannelManager` raises it from `subscribe` — before the authorizer runs and
 * before anything is written — and from `revokeChannel`, when the name is
 * outside {@link isValidName}'s charset (letters, digits and `: . _ -`, at most
 * 200 characters). The WebSocket wire already refused such a name; the
 * programmatic API did not, so a presence join succeeded on this instance while
 * every peer dropped the control frame announcing it, and the ghost sweep
 * mis-parsed the roster's owned-member entry at the first space.
 *
 * Named, like its siblings, so an `onError` handler can tell a bug in the
 * application's own channel naming — which no retry fixes — from a dead socket
 * with `instanceof`.
 *
 * @example
 * ```ts
 * import { ChannelNameError } from '@lockness/realtime'
 *
 * try {
 *     await manager.subscribe(connection, `presence-${room.title}`)
 * } catch (error) {
 *     if (error instanceof ChannelNameError) {
 *         // Name the channel by the room's id, not its title — do not retry.
 *     }
 * }
 * ```
 */
export class ChannelNameError extends Error {
    /** Always `'ChannelNameError'`, for logs and `onError`. */
    override readonly name = 'ChannelNameError'

    /**
     * Build the refusal, naming the channel through `safeForLog`.
     *
     * @param channel - The offending name, encoded before it reaches the message.
     */
    constructor(channel: string) {
        super(
            `realtime: channel name ${safeForLog(channel)} is outside the ` +
                'supported charset (letters, digits and : . _ -, at most 200 ' +
                'characters). The control plane drops frames naming a channel ' +
                'outside it, so a presence join would succeed on this instance ' +
                'and be silently dropped by every other one.',
        )
    }
}

/**
 * The app's authorizer returned a value outside its contract (#347).
 *
 * The contract is `true`, `false` or a {@link PresenceMember} object. Anything
 * else — `undefined` from a missing `return`, `null` or `undefined` from an
 * empty query row, `0`, `''`, `'yes'`, `1`, an array, a boxed primitive — is a
 * defect in the authorizer, and `subscribe` refuses it with this error before
 * anything is written, published or delivered.
 *
 * **A throw, not `{ ok: false }`.** `{ ok: false }` means one thing, "not
 * authorized" (#331). Read as a deny, a forgotten `return` would become a
 * deny-all nobody could tell from policy; read as an admit — which is what the
 * manager did before — it put authenticated strangers on private channels.
 * Named, like its siblings, so an `onError` handler can tell "a bug in my own
 * code that no retry fixes" from a dead socket with `instanceof`.
 *
 * The message carries the channel (log-encoded) and the value's TYPE label
 * only — never the value, which is application data. Nothing is sent to the
 * client; the application's `onMessage` owns any reply.
 *
 * @example
 * ```ts
 * import { AuthorizeResultError } from '@lockness/realtime'
 *
 * try {
 *     await manager.subscribe(connection, 'private-orders')
 * } catch (error) {
 *     if (error instanceof AuthorizeResultError) {
 *         // Fix the authorizer — end it with `?? false` — do not retry.
 *     }
 * }
 * ```
 */
export class AuthorizeResultError extends Error {
    /** Always `'AuthorizeResultError'`, for logs and `onError`. */
    override readonly name = 'AuthorizeResultError'

    /**
     * Build the refusal from the result's type label, never its value.
     *
     * @param channel - The channel being subscribed, encoded before it
     *   reaches the message.
     * @param type - The result's type label (`undefined`, `null`, `number`,
     *   `string`, `array`…), never the value itself.
     */
    constructor(channel: string, type: string) {
        super(
            `realtime: authorize() returned ${type} for ` +
                `${safeForLog(channel)} — it must return true, false or a ` +
                'PresenceMember object (#347). The value is not echoed; it is ' +
                'application data and this message reaches logs. The ' +
                'subscribe was refused and nothing was written. Any other ' +
                'result is treated as a bug in the authorizer rather than a ' +
                'denial, so a missing return or an absent query row cannot ' +
                'pass as a policy decision: end the authorizer with `?? false` ' +
                'where the value may be absent, and return an explicit member ' +
                'rather than a raw row.',
        )
    }
}

/**
 * A connection id the control plane cannot carry.
 *
 * `ChannelManager` raises it from `register`, `subscribe` (before the
 * authorizer runs), `evict` and `revokeChannel` — each time before anything is
 * written or published — when a connection id is outside the control plane's
 * charset (letters, digits and `: . _ -`, at most 200 characters). The control
 * plane drops a frame naming such an id, so eviction would work on this
 * instance and silently fail on every other one (#304).
 *
 * A named type rather than a bare `Error` because this reaches the application
 * through the same `onError` hook as a transport failure and a driver failure,
 * and those want different handling: a dead socket is operational, an unusable
 * id is a bug in the caller's own code that no retry will fix.
 *
 * @example
 * ```ts
 * import { ConnectionIdError } from '@lockness/realtime'
 *
 * try {
 *     await manager.subscribe(connection, 'public-feed')
 * } catch (error) {
 *     if (error instanceof ConnectionIdError) {
 *         // Mint connection ids with `crypto.randomUUID()` — do not retry.
 *     }
 * }
 * ```
 */
export class ConnectionIdError extends Error {
    /** Always `'ConnectionIdError'`, for logs and `onError`. */
    override readonly name = 'ConnectionIdError'

    /**
     * Build the refusal, naming the id through `safeForLog`.
     *
     * @param id - The offending id, encoded before it reaches the message.
     */
    constructor(id: string) {
        super(
            `realtime: connection id ${safeForLog(id)} is outside the ` +
                'supported charset (letters, digits and : . _ -, at most 200 ' +
                'characters). Mint connection ids with `crypto.randomUUID()`. ' +
                'The control plane drops frames naming an id outside it, so ' +
                'eviction would work on this instance and silently fail on ' +
                'every other one.',
        )
    }
}

/**
 * A connection that was disconnected was presented again (#361).
 *
 * `ChannelManager` raises it from `register`, and from `subscribe` both before
 * the authorizer runs and again after it, when the connection **object** is one
 * a `disconnect` has already begun for — by the socket's close, by `evict`, or
 * by a direct call. It is always raised before anything is written: no slot is
 * taken, no channel is watched, no roster entry is held and no frame is sent.
 * Its usual cause is an `onMessage` that awaits something (a body read, a
 * database lookup) before calling `subscribe`, while the socket closes.
 *
 * **A named type, not `{ ok: false }`.** `{ ok: false }` means the authorizer
 * denied (#331), a policy outcome an application may answer the client with.
 * Nobody decided anything about the channel here: the socket has gone. Folding
 * the two together would put a lifecycle race and a policy decision behind one
 * branch.
 *
 * **Not a field on `SubscribeResult` either.** A field every caller must
 * remember to read is one a caller that checks only `ok` silently ignores, and
 * `register` has no result to carry it at all.
 *
 * No client is left to answer and no retry can succeed, so the one right
 * handling is to drop the frame. It is one of three lifecycle refusals, with
 * {@link ConnectionIdInUseError} and {@link ConnectionNotRegisteredError}, and
 * shares no base class with them — ADR 010 §7 records why.
 *
 * @example
 * ```ts
 * import { ConnectionDisconnectedError } from '@lockness/realtime'
 *
 * const hooks = manager.handlerHooks({
 *     onMessage: async (conn, data) => {
 *         const channel = await channelFrom(data) // the socket may close here
 *         try {
 *             await manager.subscribe(conn, channel)
 *         } catch (error) {
 *             // No client is left to answer: drop the frame.
 *             if (error instanceof ConnectionDisconnectedError) return
 *             throw error
 *         }
 *     },
 * })
 * ```
 */
export class ConnectionDisconnectedError extends Error {
    /** Always `'ConnectionDisconnectedError'`, for logs and `onError`. */
    override readonly name = 'ConnectionDisconnectedError'

    /**
     * Build the refusal, naming the id through `safeForLog`.
     *
     * @param id - The disconnected connection's id, encoded before it reaches
     *   the message.
     */
    constructor(id: string) {
        super(
            `realtime: connection ${safeForLog(id)} was disconnected, so ` +
                'nothing was subscribed or registered for it. No retry will ' +
                'help: the socket is gone.',
        )
    }
}

/**
 * A different connection object presented an id another object already holds
 * (#361, widened by #363).
 *
 * `ChannelManager` raises it from `register` and `subscribe`, before anything
 * is written and before any authorizer runs, whenever the id is bound to a
 * DIFFERENT object — live, or still being torn down. The {@link Connection.id}
 * contract forbids reusing an id, so this is a breach of that contract in the
 * caller's transport, not a race to wait out. Before #363 only a retiring
 * holder was refused, and a second object under a live id took over every
 * channel the first one held.
 *
 * The same object registered twice is not refused: re-registration is a no-op.
 *
 * A sibling of {@link ConnectionDisconnectedError} and
 * {@link ConnectionNotRegisteredError}, with no base class shared between the
 * three, because each calls for a different remedy: that one means *this*
 * socket has gone, this one means two sockets were given one id — mint a fresh
 * id per socket. ADR 010 §7
 * (`docs/adr/010-realtime-disconnect-retires-the-connection-object.md`) records
 * why. The message carries no connection id: a refusal that echoed it would be
 * one more place an id reaches a log.
 *
 * @example
 * ```ts
 * import { ConnectionIdInUseError } from '@lockness/realtime'
 *
 * try {
 *     manager.register(connection)
 * } catch (error) {
 *     if (error instanceof ConnectionIdInUseError) {
 *         // Mint a fresh id per socket with `crypto.randomUUID()`.
 *     }
 *     throw error
 * }
 * ```
 */
export class ConnectionIdInUseError extends Error {
    /** Always `'ConnectionIdInUseError'`, for logs and `onError`. */
    override readonly name = 'ConnectionIdInUseError'

    /** Build the refusal. It takes no id, and its message names none. */
    constructor() {
        super(
            'realtime: a different connection object already holds this ' +
                'connection id. Mint a fresh id per socket; an id is never ' +
                'reused, not even across a reconnect.',
        )
    }
}

/**
 * A connection that was never registered was presented to `subscribe` (#370).
 *
 * `register` is the only way a connection object becomes bound to its id, so
 * `subscribe` admits only an object `register` bound. It raises this at its
 * first check — before the authorizer runs, and before anything is written: no
 * slot is taken, no channel is watched, no roster entry is held and nothing is
 * counted. Before #370 `subscribe` bound the object itself, and a first
 * subscribe that outlived its socket's close bound a connection no
 * `disconnect` would ever reach.
 *
 * **The remedy is to register at open.** Call `manager.register(conn)` from the
 * transport's open hook, with the object the socket will present for its whole
 * life. {@link ChannelManager.handlerHooks} does that for you, which is the
 * zero-work path: an application on it never sees this error.
 *
 * It is the third lifecycle refusal beside {@link ConnectionDisconnectedError}
 * and {@link ConnectionIdInUseError}, with no base class shared between them:
 * ADR 010 §7 (`docs/adr/010-realtime-disconnect-retires-the-connection-object.md`)
 * records why. The message carries no connection id.
 *
 * @example
 * ```ts
 * import { ChannelManager, type WebSocketHooks } from '@lockness/realtime'
 *
 * const manager = new ChannelManager()
 * const hooks: WebSocketHooks = {
 *     // Register at open, with the object this socket keeps for its life.
 *     onOpen: (conn) => manager.register(conn),
 *     onClose: async (conn) => void (await manager.disconnect(conn)),
 * }
 * ```
 */
export class ConnectionNotRegisteredError extends Error {
    /** Always `'ConnectionNotRegisteredError'`, for logs and `onError`. */
    override readonly name = 'ConnectionNotRegisteredError'

    /** Build the refusal. It takes no id, and its message names none. */
    constructor() {
        super(
            'realtime: this connection was never registered, so nothing was ' +
                "subscribed. Call `register` from the transport's open hook, " +
                'or use `handlerHooks`, which registers for you.',
        )
    }
}

/**
 * A scoped revocation was asked for on a driver that can route it but cannot
 * record it durably.
 *
 * `ConnectionIdError`-shaped, and named for the same reason: a caller acting on
 * a failed revocation needs `instanceof`, not string matching on a message.
 *
 * **Why this refuses rather than degrades.** A driver with a control plane and
 * no revocation store could still publish the frame — and that is precisely the
 * undurable path this package rejected: a lost or MAC-refused frame would mean
 * a revocation that reported success and did nothing, which is the defect
 * {@link ChannelManager.revokeChannel} exists to remove. A single-process driver
 * (no control plane, no store) is a different case and is allowed: there is no
 * bus on which to lose a frame, so no durability is owed.
 */
export class RevocationScopeError extends Error {
    /** Always `'RevocationScopeError'`, for logs and `onError`. */
    override readonly name = 'RevocationScopeError'

    /**
     * Build the refusal, naming the channel through `safeForLog`.
     *
     * @param channel - The channel the revocation was scoped to, encoded before
     *   it reaches the message.
     */
    constructor(channel: string) {
        super(
            `realtime: cannot revoke ${safeForLog(channel)} durably — this ` +
                'driver has a control plane but no revocation store, so the ' +
                'revoke would depend on a single control frame arriving. A ' +
                'lost frame would leave a revocation that reported success ' +
                'and did nothing. Implement markRevocation / listRevocations ' +
                '/ clearRevocation on the driver, or use evict, which every ' +
                'driver obeys.',
        )
    }
}
import type { Connection, WebSocketHooks } from './types.ts'
import type {
    BroadcastDriver,
    BroadcastMessage,
    ChannelRevocation,
    ChannelWatchCapableDriver,
    ConnectionRevocation,
    ControlMessage,
    PresenceCapableDriver,
    RevocationStoreDriver,
    RevocationTally,
    RosterDeparture,
    RosterWindow,
} from './driver.ts'
import { MemoryBroadcastDriver } from './drivers/memory.ts'
import {
    type Authorizer,
    type AuthorizeResult,
    type AuthorizeVerdict,
    channelKind,
    classifyAuthorizeResult,
    type PresenceMember,
    type PresenceSnapshot,
    typeLabel,
} from './channel.ts'
import type { ServerMessage } from './protocol.ts'
import { RosterReadBarrier } from './roster_read_barrier.ts'
import {
    boundPresenceSnapshot,
    localWindow,
    sameMemberId,
    uniqueMembers,
} from './presence_snapshot.ts'

/**
 * Every revocation of ONE connection from ONE channel that an apply answers
 * for (#337): the pair, and the id of each record it will clear on a `'left'`.
 *
 * One leave settles all of them, so they are applied as a group. Applied one
 * record at a time, the first leave returns `'left'` and clears its record, and
 * every later one finds `'not-subscribed'` and survives — to kick the client
 * again at the next tick if it has legitimately re-subscribed.
 */
interface ChannelRevocationGroup {
    readonly target: string
    readonly channel: string
    readonly ids: readonly string[]
}

/**
 * The connection a queued roster write announces as, and the member it names
 * (#344). It never carries the desired state: that is read from the local map
 * inside the slot's tail.
 */
interface PresenceOrigin {
    readonly clientId: string
    readonly member: PresenceMember
}

/**
 * The single feature-detect guard for a driver's optional presence-state ops
 * (A5). The roster-aware methods route through this **one** helper rather than
 * repeating `if (driver.holdMember)` per call site: a driver either owns the
 * authoritative roster (all three ops present) or it does not, and this narrows
 * once to {@link PresenceCapableDriver} accordingly.
 *
 * **The three are `holdMember`, `releaseMember` and `readRoster`** (#341,
 * #345). A driver offering only the pre-`0.4.0` roster names is simply not
 * presence-capable to this probe — refusing it is a different question, and it
 * lives in {@link assertNotLegacyRosterDriver}.
 *
 * @param driver - The broadcast driver to probe.
 * @returns The driver narrowed to {@link PresenceCapableDriver} when it exposes
 *   the full presence-state surface, otherwise `undefined` (single-process
 *   driver — the manager keeps its in-process roster).
 *
 * @example
 * ```ts
 * const roster = presenceRoster(driver)
 * if (roster) await roster.holdMember(channel, member)
 * ```
 */
export function presenceRoster(
    driver: BroadcastDriver,
): PresenceCapableDriver | undefined {
    return typeof driver.holdMember === 'function' &&
            typeof driver.releaseMember === 'function' &&
            typeof driver.readRoster === 'function'
        ? driver as PresenceCapableDriver
        : undefined
}

/**
 * Narrow a driver to one that subscribes per channel, or `undefined`.
 *
 * **The single feature-detect guard for the watch pair**, on `presenceRoster`'s
 * precedent and for the same reason: repeating `if (driver.watchChannel)` at
 * each call site is how one site gets it and another does not.
 *
 * **Detected as a SET, never member by member.** A driver offering
 * `watchChannel` without `unwatchChannel` would accumulate one permanent
 * subscription per channel it ever hosted — monotonic over the process
 * lifetime, strictly worse than the prefix-wide subscription it replaces, and
 * invisible because delivery stays correct throughout. Such a driver keeps
 * today's behaviour instead.
 *
 * @param driver - The broadcast driver to probe.
 * @returns The driver narrowed to {@link ChannelWatchCapableDriver} when it
 *   exposes both ops, otherwise `undefined`.
 *
 * @example
 * ```ts
 * const watcher = channelWatcher(driver)
 * if (watcher) await watcher.watchChannel(channel)
 * ```
 */
export function channelWatcher(
    driver: BroadcastDriver,
): ChannelWatchCapableDriver | undefined {
    return typeof driver.watchChannel === 'function' &&
            typeof driver.unwatchChannel === 'function'
        ? driver as ChannelWatchCapableDriver
        : undefined
}

/**
 * Narrow a driver to one that can durably record revocations, or `undefined`.
 *
 * **The single feature-detect guard for the revocation trio**, on
 * {@link presenceRoster} and {@link channelWatcher}'s precedent and for the same
 * reason: repeating `if (driver.markRevocation)` at each call site is how one
 * site gets it and another does not.
 *
 * **Detected as a SET.** A driver that can mark but not clear would re-apply a
 * channel-scoped revocation at every reconcile tick for the whole record TTL —
 * kicking a client that has legitimately re-subscribed, repeatedly, with the
 * roster correct throughout and nothing logged. That is the same shape of
 * defect the watch pair is detected as a set to avoid.
 *
 * **Total and non-throwing, exactly like both siblings.** A driver that
 * implements none of the trio is a single-process driver and gets `undefined`.
 * Refusing a driver of an older generation is a DIFFERENT question with a
 * different answer, and it lives in {@link assertNotLegacyRevocationDriver} —
 * fusing the two would give this function two reasons to change, and a later
 * pass restoring consistency with its two non-throwing siblings would delete
 * the refusal without noticing what it was for.
 *
 * @param driver - The broadcast driver to probe.
 * @returns The driver narrowed to `RevocationStoreDriver` when it exposes all
 *   three ops, otherwise `undefined`.
 *
 * @example
 * ```ts
 * const store = revocationStore(driver)
 * if (store) {
 *     await store.markRevocation({ target, channel, id: crypto.randomUUID() })
 * }
 * ```
 */
export function revocationStore(
    driver: BroadcastDriver,
): RevocationStoreDriver | undefined {
    return typeof driver.markRevocation === 'function' &&
            typeof driver.listRevocations === 'function' &&
            typeof driver.clearRevocation === 'function'
        ? driver as RevocationStoreDriver
        : undefined
}

/**
 * Refuse a driver written against the revocation seam this package published
 * before `0.4.0`.
 *
 * **It throws rather than warning, and that is the whole point.** The old pair
 * was `markRevoked(target)` / `listRevoked()`; a driver that still presents them
 * and not the new trio would be silently treated as having no revocation store
 * at all — so `evict` would lose its durability on a driver that plainly
 * implements revocation, and a lost control frame would never be recovered.
 * Nothing would be logged, and every same-version test would pass.
 *
 * **It tests for the OLD members' presence, never the new ones' absence.** A
 * driver that never implemented revocation — `MemoryBroadcastDriver` implements
 * none of them — is not this function's business and constructs unaffected.
 *
 * At `0.x` the built-in drivers are the contract: third-party realtime drivers
 * are not a supported extension point before `1.0`, so this is a hard refusal
 * with no deprecation window.
 *
 * @param driver - The broadcast driver being wired into a manager.
 * @throws {Error} If the driver presents the pre-`0.4.0` revocation pair.
 */
function assertNotLegacyRevocationDriver(driver: BroadcastDriver): void {
    const legacy = driver as {
        markRevoked?: unknown
        listRevoked?: unknown
    }
    if (
        typeof legacy.markRevoked !== 'function' &&
        typeof legacy.listRevoked !== 'function'
    ) {
        return
    }
    throw new Error(
        'realtime: this driver implements the pre-0.4.0 revocation seam ' +
            '(markRevoked / listRevoked), which no longer exists. Replace it ' +
            'with markRevocation(revocation) / listRevocations() / ' +
            'clearRevocation(channelRevocation) over a Revocation record ' +
            '({ target } for a whole connection, { target, channel, id } for ' +
            'one channel — clear removes exactly that id). Keeping the old ' +
            'pair would silently disable durable revocation on a driver that ' +
            'plainly implements it: evict would still close the socket ' +
            'locally, and a lost control frame would never be recovered.',
    )
}

/**
 * The upgrade-notes section a refused roster driver is sent to — by **title**,
 * never by item number, so renumbering the notes cannot send an author to the
 * wrong migration.
 */
const ROSTER_SEAM_UPGRADE_SECTION =
    'The driver roster seam is replaced, and the old names throw'

/**
 * The roster members this package published before `0.4.0` and retired: the
 * unbounded read (#341) and the add/remove pair that could not tell a holder
 * from the slot (#345).
 */
const RETIRED_ROSTER_MEMBERS = [
    'listMembers',
    'addMember',
    'removeMember',
] as const

/**
 * Refuse a driver that still offers any roster member this package retired
 * before `0.4.0` — the unbounded read (#341) or the add/remove pair (#345).
 *
 * **It throws rather than warning**, for the reason
 * {@link assertNotLegacyRevocationDriver} does. A driver presenting the old
 * names and not the new ones would be narrowed to "no roster" and silently lose
 * its authoritative presence; one presenting both would keep a retired
 * behaviour public for any caller that reaches the driver directly. A stale
 * add/remove pair left to run would fail later, with a `TypeError` inside the
 * #323 rollback, instead of here.
 *
 * **Once, naming every retired member present.** A `0.3.0` driver carries all
 * three; two sequential refusals would send its author through two upgrades.
 *
 * **Beside, and separate from, the revocation refusal.** Each tests one
 * retired seam and names one migration; fusing them would give one function
 * two reasons to change.
 *
 * @param driver - The broadcast driver being wired into a manager.
 * @throws {Error} If the driver presents any member of
 *   {@link RETIRED_ROSTER_MEMBERS}, naming all of those present.
 */
function assertNotLegacyRosterDriver(driver: BroadcastDriver): void {
    const legacy = driver as unknown as Record<string, unknown>
    const present = RETIRED_ROSTER_MEMBERS.filter((name) =>
        typeof legacy[name] === 'function'
    )
    if (present.length === 0) return
    throw new Error(
        `realtime: this driver implements pre-0.4.0 roster members ` +
            `(${present.join(', ')}), which no longer exist — see ` +
            `"${ROSTER_SEAM_UPGRADE_SECTION}" in the v0.4.0 upgrade notes. ` +
            'The roster seam is now readRoster(channel, limit, selfIds) → ' +
            '{ members, total, selves }, holdMember(channel, member) → ' +
            '{ arrived } and releaseMember(channel, memberId) → { gone }: a ' +
            'slot is held per process, `arrived` is true only when no process ' +
            'held it, and `gone` only when this process held it and no holder ' +
            'is left. Remove the retired members.',
    )
}

/**
 * A frame the manager sends to a connection — the `event` / `presence` /
 * `unsubscribed` subset of the wire protocol's {@link ServerMessage} (one
 * shape, not a second copy).
 *
 * `unsubscribed` joined this union with {@link ChannelManager.revokeChannel}
 * (#332), and it had to: the frame goes through the application's own `encode`
 * hook like every other, and a frame the framework sent around that hook would
 * reach a custom codec's peer in a format the codec never produced. An encoder
 * annotated with the older union stops compiling — deliberately, because the
 * alternative is discovering the new shape from a client that cannot parse it.
 */
export type OutboundFrame = Extract<
    ServerMessage,
    { type: 'event' } | { type: 'presence' } | { type: 'unsubscribed' }
>

/**
 * A presence frame announcing one member's arrival or departure — the only
 * frame {@link ChannelManager}'s `emitPresence` sends (#349). Internal: it
 * narrows {@link OutboundFrame} so the member the self-exclusion reads is
 * always there.
 */
type PresenceTransitionFrame =
    & Extract<OutboundFrame, { type: 'presence' }>
    & { action: 'joined' | 'left'; member: PresenceMember }

/**
 * What a leave verb did, on the instance it was called on.
 *
 * Three outcomes that used to share one representation — `undefined`:
 *
 * - `'left'` — a membership was removed here.
 * - `'not-subscribed'` — this instance owns the socket, and it was not in that
 *   channel. Idempotent and correct; nothing to do.
 * - `'not-owned'` — the socket lives on **another instance**, so nothing local
 *   could have been removed and nothing was announced anywhere. Use
 *   {@link ChannelManager.revokeChannel}, which reaches the owner.
 *
 * **This is a SERVER-side value.** It is never relayed to a client, and the
 * `clientId` it is derived from is never taken from a client frame — the three
 * states together would otherwise tell a caller whether an arbitrary connection
 * id is live in the fleet, whether this instance owns it, and whether it is in
 * a given room.
 */
export type LeaveOutcome = 'left' | 'not-subscribed' | 'not-owned'

/**
 * What {@link ChannelManager}'s private `#collectLeaveOutcome` got from one
 * `#leaveLocal` call — a rejection collected rather than thrown, so a caller
 * can still run its own follow-up write. Internal: never a public return
 * shape, unlike {@link LeaveOutcome}.
 */
type LocalLeaveOutcome =
    | { readonly left: boolean; readonly failed: false }
    | { readonly left: false; readonly failed: true; readonly error: unknown }

/**
 * What {@link ChannelManager.disconnect} did.
 *
 * `'not-owned'` carries the same meaning, the same warning and the same remedy
 * as it does on {@link LeaveOutcome}.
 */
export type DisconnectOutcome = 'disconnected' | 'not-owned'

/**
 * What {@link ChannelManager.revokeChannel} did on the instance that owns the
 * socket.
 *
 * `'not-owned'` here means something different from the local verbs': the
 * revocation **was** recorded durably and routed to the owner, and this
 * instance simply is not it. It is reported rather than hidden so a caller on a
 * single-process driver — where there is no owner to route to — cannot mistake
 * "published nothing, applied nothing" for success.
 */
export type RevokeChannelOutcome = 'revoked' | 'not-subscribed' | 'not-owned'

/** The outcome of a subscribe attempt. */
export interface SubscribeResult {
    /** Whether the subscription was authorized. */
    ok: boolean
    /**
     * For an authorized presence channel: the bounded "here" snapshot — at most
     * `maxPresenceSnapshotMembers` members, the joiner's own among them, the
     * roster's `total`, and its `source` (#339).
     *
     * Replaces the 0.3.0 `members` and `rosterSource` fields, which were
     * removed rather than deprecated: `members` used to be the whole room, and a
     * consumer counting `members.length` as the room size must fail to compile
     * rather than silently read a bounded list as everyone. Use `here.total`.
     */
    here?: PresenceSnapshot
}

/** Options for a {@link ChannelManager}. */
export interface ChannelManagerOptions<Identity = unknown> {
    /** The broadcast driver (defaults to in-process memory). */
    driver?: BroadcastDriver
    /** The app authorizer for private/presence channels. */
    authorize?: Authorizer<Identity>
    /** Frame encoder (defaults to JSON; `#213`'s protocol codec replaces it). */
    encode?: (frame: OutboundFrame) => string
    /** Sink for a failed cross-process publish (defaults to `console.error`). */
    onPublishError?: (error: unknown) => void
    /**
     * The per-instance watched-channel cap (default {@link MAX_WATCHED_CHANNELS}).
     *
     * A positive integer. Each watched channel is a broker subscription
     * re-issued on every reconnect, so raising this raises the cost of a
     * reconnect storm in direct proportion — see the upgrade note in
     * `docs/realtime.md`.
     */
    maxWatchedChannels?: number
    /**
     * The per-connection watched-channel cap (default
     * {@link MAX_CHANNELS_PER_CONNECTION}).
     *
     * A positive integer, and never greater than {@link maxWatchedChannels} —
     * a per-connection cap above the instance cap lets one connection consume
     * the whole instance budget, which is refused at construction.
     */
    maxChannelsPerConnection?: number
    /**
     * The share of {@link maxWatchedChannels} that connections with no identity
     * may cause to be hosted (default `0.8`; `1` disables the reservation).
     *
     * `subscribe` runs no authorizer for a public channel, so without this an
     * anonymous socket can drive the instance to its cap and deny every other
     * connection — authenticated ones included — the ability to host a new
     * channel. An anonymous connection may always JOIN an already-hosted
     * channel; the reservation bounds only 0 -> 1 transitions.
     *
     * A deployment that authenticates nobody sets this to `1` and carries the
     * original exposure knowingly.
     */
    anonymousHostingShare?: number
    /**
     * The ceiling on a serialized {@link PresenceMember}, in bytes. Defaults to
     * {@link MAX_PRESENCE_MEMBER_BYTES}.
     *
     * A positive integer, validated at construction. Enforced at the admission
     * boundary — an oversized member is refused with
     * {@link PresenceMemberSizeError} before any local join, roster write or
     * announcement exists, so a refusal leaves nothing behind. It bounds an
     * object result on a private channel too (#357), which runs the same
     * admission and discards the member.
     *
     * **Reconcile it with the driver's `control.maxPayloadBytes`** whenever you
     * change either: a member larger than the control ceiling can be written to
     * the roster and never announced. {@link MAX_PRESENCE_MEMBER_BYTES}
     * documents the headroom the default leaves.
     */
    maxPresenceMemberBytes?: number
    /**
     * How many members one presence `subscribe` returns at most, self
     * included. Defaults to {@link MAX_PRESENCE_SNAPSHOT_MEMBERS}.
     *
     * A positive integer, validated at construction. It limits the size of ONE
     * reply and keeps no state across frames — it is not a meter, a budget or
     * a rationing policy (#329): every subscribe is answered, and `ok` and the
     * number of driver reads never change. Cutting is silent.
     *
     * One reply carries at most K·(M+1)+1 bytes of member JSON, where M is
     * {@link maxPresenceMemberBytes} — or the largest value any instance of the
     * fleet runs, since peers write the roster too.
     */
    maxPresenceSnapshotMembers?: number
}

/**
 * The channel manager / broadcaster.
 *
 * @typeParam Identity - The app's connection-identity shape.
 *
 * @example
 * ```ts
 * const manager = new ChannelManager({ authorize: (id, ch) => id != null })
 * manager.register(conn) // from the transport's open hook
 * await manager.subscribe(conn, 'private-orders')
 * manager.broadcast('private-orders', 'created', { id: 1 })
 * ```
 */
export class ChannelManager<Identity = unknown> {
    private readonly driver: BroadcastDriver
    private readonly authorize?: Authorizer<Identity>
    private readonly encode: (frame: OutboundFrame) => string
    private readonly onPublishError: (error: unknown) => void
    /** Resolved caps and reservation — read by `#checkChannelCaps` and nowhere else. */
    readonly #maxWatchedChannels: number
    readonly #maxChannelsPerConnection: number
    readonly #anonymousHostingShare: number
    readonly #maxPresenceMemberBytes: number
    readonly #maxPresenceSnapshotMembers: number
    /**
     * One serial tail per roster slot, keyed by {@link #rosterSlotKey} (#330).
     * Entries live only while a reconciliation for that slot is queued or in
     * flight — {@link #syncRosterMember} deletes its own once it settles, so
     * this cannot grow with a cardinality a client chooses. A roster-less
     * driver has tails too: the reconciliation reads the local map and writes
     * nothing (#342).
     */
    readonly #rosterTails = new Map<string, Promise<unknown>>()
    /**
     * The serial tail of durable revocation re-checks (#359 FR-009a): the
     * last run queued, settled either way. Written only by
     * {@link reconcileRevocations}.
     */
    #revocationTail: Promise<unknown> = Promise.resolve()
    /**
     * The roster slots this instance holds on a roster-less driver, keyed like
     * {@link #rosterTails} (#344). Arrived/gone on such a driver is this set's
     * transition, read and written only inside {@link #syncRosterMember}'s
     * queued run and updated before the announcement. An entry lives while a
     * local connection holds the member, so it is bounded by the presence map.
     */
    readonly #heldSlots = new Set<string>()
    /**
     * Roster slots whose last release attempt rejected — a presence leave's
     * own release, or the #323/#373 join compensation's reclaim — retried by
     * the driver's `onRosterMaintenance` drain rather than left to the ghost
     * sweep alone (#371). Keyed like {@link #rosterTails}, by
     * {@link #rosterSlotKey}. The VALUE is the origin to retry with, never a
     * desired state — the retry still reads `presence` fresh, through
     * {@link #syncRosterMember}, so a slot re-claimed by a fresh join before
     * the drain runs is re-derived as held, not released. A second failure on
     * the same slot overwrites its entry rather than growing it; bounded at
     * {@link MAX_PENDING_ROSTER_RELEASES} distinct slots by
     * {@link #recordOwedRelease}.
     */
    readonly #owedReleases = new Map<string, PresenceOrigin>()
    /** The instance cap an anonymous connection may reach, precomputed once. */
    readonly #anonymousWatchedCeiling: number
    /**
     * Whether that ceiling is actually below the cap.
     *
     * `false` when `anonymousHostingShare` is 1 (or rounds up to the cap), and
     * then an anonymous caller is refused by the INSTANCE cap rather than by a
     * reservation — which is what the breach must say, or an operator is sent
     * to tune a dial that is already at its maximum.
     */
    readonly #anonymousReservationActive: boolean
    private readonly connections = new Map<string, Connection<Identity>>()
    private readonly subscriptions = new Map<string, Set<string>>()
    /**
     * Reverse index: which channels each connection holds.
     *
     * A different question from `subscriptions`, not a second answer to the same
     * one — hosting is read from `subscriptions` and nowhere else. Written only
     * inside {@link #joinLocal} / {@link #leaveLocal}, so it cannot drift.
     */
    readonly #channelsByClient = new Map<string, Set<string>>()
    /**
     * The connection objects a `disconnect` has begun for — **retired** (#361)
     * — mapped to that teardown's own settling promise (#393).
     *
     * This is the definition's one home
     * ([ADR 010](../../docs/adr/010-realtime-disconnect-retires-the-connection-object.md)
     * links here rather than restating it):
     *
     * - **Retired** means a `disconnect` has begun for this connection
     *   **object**. {@link #teardown} is the only writer, at its entry — the
     *   public {@link disconnect} and `revokeLocal`'s direct internal call
     *   (#392) both reach it, and neither writes here any other way.
     * - It is **terminal** and **per-manager**: nothing ever removes an entry,
     *   and another manager knows nothing of it.
     * - It is **keyed by object**, so an entry lives exactly as long as someone
     *   can still present that object, then dies with it — bounded by
     *   construction, with no TTL and no sweep. An id-keyed record would have
     *   to outlive the teardown, one entry per socket ever closed.
     * - It is **not a spelling of ownership.** A retiring connection is still
     *   owned — `connections` answers that, for every reader that asks it —
     *   and is only no longer admissible. {@link #assertAdmissible} is the one
     *   reader, and it still asks only `.has(connection)` — the value stored
     *   alongside is {@link #teardown}'s own business.
     * - **The value is the retiring teardown's promise, not a boolean.** A
     *   second `disconnect` of an object already present here joins that
     *   promise instead of running its own copy of the reverse-index loop
     *   (#393) — one teardown per object, ever. Nothing removes an entry once
     *   settled, rejected included: a later joiner sees the same outcome the
     *   first caller did.
     */
    readonly #retired = new WeakMap<
        Connection<Identity>,
        Promise<DisconnectOutcome>
    >()
    /**
     * Whether {@link disconnect}'s id-form deprecation notice has already
     * fired for THIS manager instance (#392). Written only by
     * {@link #warnIdForm}, `disconnect`'s own helper. A line per socket close
     * under connection churn would be worse than one line ever, so the notice
     * is per-manager, not per-call — unlike `#retired`, this flag is never
     * per-object: every id-form call after the first, for any id, sees it
     * already `true`.
     */
    #idFormWarned = false
    /** The driver's per-channel watch ops, or `undefined` — one guard (#295). */
    #watcher: ChannelWatchCapableDriver | undefined
    /**
     * The driver's durable revocation store, narrowed ONCE at construction —
     * never member-by-member at a call site. `undefined` means a single-process
     * driver, which owes no durability because it has no bus to lose a frame
     * on.
     */
    #revocations: RevocationStoreDriver | undefined
    /**
     * The **local** presence members this instance's sockets own, per channel
     * (`clientId → member`). NOT the authoritative roster (that is the driver,
     * possibly remote — decision-table §5): this map only records what THIS
     * instance added, so `unsubscribe`/`disconnect` know which member's slot to
     * release in the driver roster, and which member a `left` names when that
     * release empties the slot (#344).
     *
     * **Keyed by connection, read by member (#343).** One member with two tabs
     * is two values here and one slot in the roster. Never read `.values()`
     * directly: {@link #localRoster} is the only reader, and it deduplicates by
     * `String(id)`, earliest-joined connection first. Do not re-key this map by
     * member either — the #327 claim, the #334 last-member delete and the self
     * lookup in {@link #closingRead} all key on the connection.
     */
    private readonly presence = new Map<
        string,
        Map<string, PresenceMember>
    >()
    /** The driver's roster ops when it owns the authoritative roster (else `undefined`). */
    private readonly roster: PresenceCapableDriver | undefined
    /**
     * Collapses concurrent authoritative reads of one channel onto the trailing
     * edge (#333). Constructed with {@link roster} and `undefined` with it, so
     * the two never disagree about whether this driver has a roster at all.
     *
     * `private`, matching {@link roster} beside it rather than the `#` fields
     * further up, and for a reason this class has already used: the barrier's
     * `size` is the only way to observe that sharing a read does not also
     * RETAIN one, and that property has no behavioural consequence to assert
     * on — exactly the shape #334 records for the presence map.
     */
    private readonly rosterReads: RosterReadBarrier | undefined

    /**
     * Build a manager over a broadcast driver — an in-memory one when none is
     * given, which is single-process.
     *
     * @param options - The driver, authorizer, and encoder.
     */
    constructor(options: ChannelManagerOptions<Identity> = {}) {
        this.driver = options.driver ?? new MemoryBroadcastDriver()
        this.authorize = options.authorize
        this.encode = options.encode ?? ((frame) => JSON.stringify(frame))
        this.onPublishError = options.onPublishError ??
            // The framework's DEFAULT sink, so it is the framework's job to
            // make it safe: the error is rendered, never handed with its stack
            // to a log store, and the line never throws — `broadcast` discards
            // the publish promise, so a throw here would escape (#395).
            ((error) => writeMarkedFallback(PUBLISH_FAILED, error))
        // `??`, never `||`: a supplied 0 must reach the assertion below rather
        // than be silently repaired into the default. A cap that repairs itself
        // is the shape the plan's decision table forbids.
        this.#maxWatchedChannels = options.maxWatchedChannels ??
            MAX_WATCHED_CHANNELS
        this.#maxChannelsPerConnection = options.maxChannelsPerConnection ??
            MAX_CHANNELS_PER_CONNECTION
        this.#anonymousHostingShare = options.anonymousHostingShare ?? 0.8
        this.#maxPresenceMemberBytes = options.maxPresenceMemberBytes ??
            MAX_PRESENCE_MEMBER_BYTES
        this.#maxPresenceSnapshotMembers = options.maxPresenceSnapshotMembers ??
            MAX_PRESENCE_SNAPSHOT_MEMBERS
        assertCap('maxWatchedChannels', this.#maxWatchedChannels)
        assertCap('maxChannelsPerConnection', this.#maxChannelsPerConnection)
        assertShare('anonymousHostingShare', this.#anonymousHostingShare)
        assertCap('maxPresenceMemberBytes', this.#maxPresenceMemberBytes)
        assertCap(
            'maxPresenceSnapshotMembers',
            this.#maxPresenceSnapshotMembers,
        )
        if (this.#maxChannelsPerConnection > this.#maxWatchedChannels) {
            throw new Error(
                `realtime: maxChannelsPerConnection ` +
                    `(${this.#maxChannelsPerConnection}) exceeds ` +
                    `maxWatchedChannels (${this.#maxWatchedChannels}), which ` +
                    'lets a single connection consume the whole instance ' +
                    'budget. Lower it, or raise the instance cap.',
            )
        }
        this.#anonymousWatchedCeiling = Math.floor(
            this.#maxWatchedChannels * this.#anonymousHostingShare,
        )
        this.#anonymousReservationActive =
            this.#anonymousWatchedCeiling < this.#maxWatchedChannels
        // A ceiling of ZERO denies anonymous hosting outright for the life of
        // the process, and it is reachable from two individually valid values —
        // `{ maxWatchedChannels: 1 }` with the default share floors 0.8 to 0.
        // Fail-closed, so nothing leaks; but a deployment that meant to reserve
        // a fifth of its budget and instead disabled anonymous hosting entirely
        // deserves to hear about it at construction rather than from a support
        // ticket. Set the share to 1 to disable it on purpose.
        if (this.#anonymousWatchedCeiling === 0) {
            throw new Error(
                `realtime: maxWatchedChannels (${this.#maxWatchedChannels}) × ` +
                    `anonymousHostingShare (${this.#anonymousHostingShare}) ` +
                    'floors to 0, so no connection without an identity could ' +
                    'ever host a channel. Raise either, or set the share to 1 ' +
                    'to disable the reservation deliberately.',
            )
        }
        // BEFORE the probe, not after: a driver of the previous generation
        // must fail loudly here rather than be narrowed to `undefined` and
        // silently treated as having no revocation store at all.
        assertNotLegacyRevocationDriver(this.driver)
        // Same placement, separate refusal: a driver still offering the
        // unbounded read must not be narrowed to "no roster" (#341).
        assertNotLegacyRosterDriver(this.driver)
        const roster = presenceRoster(this.driver)
        this.roster = roster
        // ONE barrier, at construction, for every authoritative read (#333).
        // It takes a FUNCTION rather than the driver, so the unit cannot drift
        // with `BroadcastDriver`'s optional-member surface. The read is
        // bounded to K in the driver (#341); K itself stays in the cut.
        const limit = this.#maxPresenceSnapshotMembers
        this.rosterReads = roster
            ? new RosterReadBarrier((channel, selfIds) =>
                roster.readRoster(channel, limit, selfIds)
            )
            : undefined
        // ONE guard, at construction, for the whole watch pair (#295).
        this.#watcher = channelWatcher(this.driver)
        // ONE guard, at construction, for the whole revocation trio (#332).
        this.#revocations = revocationStore(this.driver)
        // Local + cross-process delivery share this one path.
        this.driver.onMessage((message) => this.deliverLocal(message))
        // A cross-process driver's control plane is a DISTINCT seam (A2/FR-016):
        // control frames drive roster/eviction consequences, never event fan-out.
        this.driver.onControl?.((control) => this.handleControl(control))
        // The durable revocation re-check (S1/FR-014): on every reconcile pass
        // the owning instance recovers an evict whose control frame was lost.
        this.driver.onRevocationReconcile?.(() => this.reconcileRevocations())
        // A slot the driver emptied for ANOTHER process — on Redis, the ghost
        // sweep of a crashed instance — is announced here (#348). Only with a
        // roster: without one there is no slot a driver could have emptied.
        if (roster) {
            this.driver.onRosterDeparture?.((departure) =>
                this.#announceDeparture(departure)
            )
            // Holds the driver may have lost on this process's behalf — on
            // Redis, a lapsed liveness key a peer swept — are written again
            // (#349). Only with a roster, like the departure above.
            this.driver.onRosterLapse?.((signal) =>
                this.#reassertRoster(signal)
            )
            // The owed-release drain (#371): a roster release this instance
            // could not commit — a presence leave's, or the #323/#373
            // reclaim's — is retried here rather than left to the ghost
            // sweep alone. Fired unconditionally after every successful
            // heartbeat, never gated on a detected fault (contrast
            // `onRosterLapse` above), and never folded into the revocation
            // reconcile pass, which would corrupt its #362/#384 deadline
            // measurement.
            this.driver.onRosterMaintenance?.(() => this.#drainOwedReleases())
        }
    }

    /**
     * Compose lifecycle hooks that register the connection on open and
     * disconnect it on close — the framework-owned teardown seam, so a forgotten
     * app wire cannot leave ghost presence members or dead-socket references.
     *
     * **VERB RATE IS THE APPLICATION'S** (#329). `onOpen` and `onClose` below
     * are composed; `onMessage` is passed through unmetered — its one wrapper
     * is the ownership gate described further down (#363), which counts
     * nothing and refuses nothing an owner sends. That is a decision, not an
     * omission, and this is the one place it is recorded — so read it before
     * adding anything to that wrapper.
     *
     * **The framework has no charge target a reconnect does not rotate.**
     * `Connection.id` is minted per socket and by contract never reused, so a
     * returning attacker and a reconnecting client are indistinguishable. Any
     * budget large enough to let a legitimate client re-issue its whole channel
     * set as one burst — up to {@link maxChannelsPerConnection} — is a budget a
     * reconnect hands back for free. The only key that does not rotate is
     * `Connection.identity`, which is the application's own type: hence the
     * seam is here, and the meter belongs in your `onMessage`, keyed on a
     * **stable string** derived from that identity (an object identity keys a
     * `Map` by reference and the meter silently accumulates nothing).
     *
     * **But the decisive reason is revocation, not that arithmetic.** A budget
     * that covers the whole churn cycle has to sit on
     * {@link ChannelManager.unsubscribe} — and six paths reach that method, of
     * which exactly ONE comes from a client: `disconnect` on socket close,
     * `revokeLocal` from a local `evict`, `handleControl`'s evict arm from
     * ANOTHER instance, the durable revocation reconcile, and a direct
     * programmatic call. A refusal there charges six and means one: a client
     * that spent its budget makes its own eviction leave permanent roster
     * ghosts, because `disconnect` re-throws, `revokeLocal` catches and warns,
     * the socket is already closed, and only a ghost sweep of a **dead**
     * instance would reclaim the entries. An unreliable revoke is a worse
     * outcome than the amplification it was meant to bound.
     *
     * A subscribe-ONLY budget escapes that objection — `subscribe` has no
     * non-client callers — and is on the record as considered and declined: it
     * is still not a bound, and it would still add statements to the #323
     * co-turn.
     *
     * **`authorize` is not the seam.** It gates ADMISSION. It never runs for a
     * public channel or for `unsubscribe` at all, and per #331 a denial on a
     * held channel changes nothing — so a budget placed there meters the cheap
     * path and cannot refuse the expensive one. `docs/realtime.md` carries the
     * per-frame cost table and a worked example.
     *
     * **`onClose` always disconnects** (#361). The app's `onClose` runs first,
     * and the teardown runs whatever it did — so the connection is torn down
     * and retired even when the app's hook throws. The app's error is then
     * what the close rejects with; a teardown failure after it is one WARN.
     * It passes the connection OBJECT (#363), so a socket that does not own
     * its id — one `onOpen` refused, or one whose id was re-registered after
     * an evict — tears down nothing that belongs to the socket that does.
     *
     * **Your `onClose` runs exactly once for each socket whose `onOpen` ran**
     * (#404) — evicted ones included, refused ones never. If your transport
     * reuses ids, an evicted socket's id may already be someone else's, so
     * still never act on `conn.id` there. An `onOpen` of yours that throws,
     * or that closes the socket itself, still gets its `onClose`, so a
     * counter kept across the two must be incremented first. The pairing is a
     * weak set of the admitted objects, cleared on close; it answers "did
     * this socket open?" and never "who owns this id?". `onError` still hears
     * a refused socket, by design.
     *
     * **`onMessage` runs the app's hook only for the socket that owns its id**
     * (#363). A frame from a socket `onOpen` refused, or from one already torn
     * down, is dropped before any app code runs — so an app that calls
     * `unsubscribe(conn.id, …)` there cannot strip the live holder. The drop
     * logs nothing: the socket has no owner to answer, and a line per frame
     * would be a flooding vector. Apart from that gate the hook is passed
     * through as it is.
     *
     * @param userHooks - The app's own hooks (run alongside the teardown).
     * @returns Hooks to pass to `createWebSocketHandler`.
     *
     * @example
     * ```ts
     * createWebSocketHandler({ hooks: manager.handlerHooks({ onMessage }) })
     * ```
     */
    handlerHooks(
        userHooks: WebSocketHooks<Identity> = {},
    ): WebSocketHooks<Identity> {
        // PAIRS OPEN WITH CLOSE, AND NOTHING ELSE (#404): the sockets whose
        // app `onOpen` is about to run, each cleared by its first close. Never
        // asked who owns an id — `#isOwner` is the one authority on that
        // (#370 plan, row 17). Weak, so a transport that never closes a socket
        // leaks nothing here.
        const opened = new WeakSet<Connection<Identity>>()
        return {
            onOpen: (conn) => {
                // CLOSE FIRST, then rethrow. `guard()` in websocket.ts catches
                // whatever this throws and merely logs it, so a bare throw left
                // the socket OPEN and untracked: the app's own onOpen — where a
                // per-SOCKET rate limit or an explicit unauthorized-close
                // lives, which is a different budget from the per-VERB one the
                // docstring above routes to `onMessage` — was skipped,
                // onMessage went on firing, and `evict` could
                // not reclaim it because it rejects the same id. Fail-open on
                // the seam this breaking change was supposed to make loud.
                try {
                    this.register(conn)
                } catch (error) {
                    conn.close(1011, 'unusable connection id')
                    throw error
                }
                // After the register succeeded, before the app's hook: a
                // refused socket never reaches this line, so it never gets
                // the app's onClose either — and an app onOpen that throws
                // or closes the socket still gets its onClose (#404 W4).
                opened.add(conn)
                return userHooks.onOpen?.(conn)
            },
            onMessage: (conn, data) => {
                // Only the owner reaches app code (#363): a refused or retired
                // socket shares an id with, at most, a socket it must not act
                // for. Dropped without a log line — one per frame would flood.
                if (!this.#isOwner(conn)) return
                return userHooks.onMessage?.(conn, data)
            },
            onError: userHooks.onError,
            onClose: async (conn, code, reason) => {
                // THE TEARDOWN RUNS WHATEVER THE APP'S HOOK DID (#361). An app
                // `onClose` that threw used to skip `disconnect`: no teardown,
                // no retirement, and every leak retirement removes. Each
                // failure is recorded by a flag — a rejection may carry
                // `undefined` — and never by a bare `try/finally`, which would
                // drop the app's error if the teardown failed too.
                let appFailed = false
                let appError: unknown
                try {
                    // Once per opened socket (#404): never for one `register`
                    // refused, never twice. An evicted socket was opened, so
                    // it still gets its hook — ownership is not the question.
                    if (opened.delete(conn)) {
                        await userHooks.onClose?.(conn, code, reason)
                    }
                } catch (error) {
                    appFailed = true
                    appError = error
                }
                try {
                    await this.disconnect(conn)
                } catch (error) {
                    if (!appFailed) throw error
                    console.warn(
                        `realtime: disconnecting ${
                            safeForLog(conn.id)
                        } after the application's onClose threw also ` +
                            `failed: ${renderError(error)}`,
                    )
                }
                // The app's error first: it is the one its own code raised.
                if (appFailed) throw appError
            },
        }
    }

    /**
     * Reject a connection id the rest of the framework cannot carry.
     *
     * **Loud, at the boundary, once.** Before this, three paths disagreed about
     * an id outside {@link isValidName}: a local `evict()` worked, a control
     * frame to another instance was dropped on ingest (`drivers/redis.ts`), and
     * the durable reconcile recovered it. An application using such an id had a
     * revocation that worked on one instance and not the others, with nothing
     * to tell it so — and filtering the reconcile path alone would have made
     * that two silent failures rather than one.
     *
     * The charset is not new; it is what the control plane has always required.
     * What is new is saying so at the moment the id enters, where an
     * application can act on it, instead of at a revocation nobody is watching.
     *
     * @param id - The connection id to check.
     * @throws If the id is outside the charset the control plane accepts.
     */
    #assertUsableId(id: string): void {
        if (isValidName(id)) return
        throw new ConnectionIdError(id)
    }

    /**
     * Assert a channel name can cross the control plane and be parsed back out
     * of the roster's owned-member set (#314).
     *
     * **`subscribe` asserted two of the three values it received and skipped
     * this one.** `connection.id` goes through `#assertUsableId` (#304) and a
     * presence `member.id` through the member-id check (#306, now inside
     * `admitPresenceMember`, `presence_member.ts`); the channel
     * went through nothing. The only channel validation in the package was
     * `decodeClientMessage`, which guards the WebSocket wire — so the
     * framework's own socket path refused a name the public programmatic API
     * accepted, which is the asymmetry #304 was opened about, one value over.
     *
     * Two consequences were live, both reachable with a channel containing a
     * space:
     *
     * - **Cross-instance presence stopped working for that channel.** The
     *   `presence-join` control frame carries it, and every receiving instance
     *   drops the frame on ingest for exactly this charset. The join succeeded
     *   locally, `subscribe` answered `{ ok: true }`, and no peer ever learned.
     * - **The ghost sweep mis-parsed.** An owned-set entry is
     *   `` `${channel} ${field}` `` and the sweep splits on the FIRST space, so
     *   `presence-my room` + `u1` split to channel `presence-my` and field
     *   `room u1`. The `HDEL` then hit a key that does not exist and the members
     *   were never reclaimed. Only the death-recovery path broke — the ordinary
     *   leave path re-joins the full string — which is why nothing caught it.
     *
     * Two shipped docstrings already asserted this invariant
     * (`OWNED_SEP`'s and the member-id check's), which is worse than a
     * gap: the next reader takes it as settled. Both now cite this method.
     *
     * @param channel - The channel name to check.
     * @throws {ChannelNameError} If the name is outside `isValidName`.
     */
    #assertUsableChannel(channel: string): void {
        if (isValidName(channel)) return
        throw new ChannelNameError(channel)
    }

    /**
     * Refuse a connection that may no longer be admitted (#361, widened by
     * #363) — the one reader of {@link #retired}, and the admission rule every
     * asker shares.
     *
     * **Who asks, and what each asks** (the single home of that list):
     *
     * - **`register` asks this method directly, first**, before the id
     *   charset. A transport re-registering a socket it already closed would
     *   otherwise re-add a zombie that `connectionCount` counts, and a second
     *   object under a held id would otherwise take the binding over.
     * - **`subscribe` asks {@link #assertBound}, twice** — which asks this
     *   method first and then whether the object is registered:
     *   - **before the authorizer**, so a retired, foreign or unregistered
     *     connection's authorizer — a database read, an audit write — never
     *     runs, and no cap or anonymous share is spent on it (windows (b) and
     *     (c));
     *   - **after the authorizer's result is classified**, in the synchronous
     *     turn it shares with the caps and the join's adds. The disconnect may
     *     have begun while the authorizer ran (window (a)); refusing here,
     *     before every write, is what leaves nothing to undo. Below the denial
     *     `return` and every result check, so a denial is still a denial and a
     *     defect is still a defect.
     *
     * No other site asks: `register` never asks {@link #assertBound} (it is
     * what binds), and `subscribe` never asks this method directly (it must
     * also refuse an object nothing registered).
     *
     * One predicate, two clauses, two classes — because the two call for
     * different handling:
     *
     * 1. **This object was retired** — its socket is gone, and no retry helps.
     * 2. **A different object holds its id** — live, or still being torn down
     *    (#363). The `Connection.id` contract is breached. The same object
     *    presenting itself again passes, which is what makes a second
     *    `register` of one object a no-op. The clause reads the binding, so it
     *    retains nothing once the id is released.
     *
     * @param connection - The connection being admitted.
     * @throws {ConnectionDisconnectedError} If this object was retired.
     * @throws {ConnectionIdInUseError} If a different object holds its id.
     */
    #assertAdmissible(connection: Connection<Identity>): void {
        if (this.#retired.has(connection)) {
            throw new ConnectionDisconnectedError(connection.id)
        }
        const bound = this.connections.get(connection.id)
        if (bound !== undefined && bound !== connection) {
            throw new ConnectionIdInUseError()
        }
    }

    /**
     * Refuse a connection `subscribe` may not act for (#370) — `subscribe`'s
     * decider, asked at both of its checks.
     *
     * **Admissibility first, then registration.** A retired object whose id
     * is already unbound is a socket that has gone, and it hears
     * {@link ConnectionDisconnectedError}; only an admissible object that
     * nothing bound hears {@link ConnectionNotRegisteredError}. The other order
     * would tell a closed socket to go and register.
     *
     * **Why two mutations of `subscribe` are equivalent** (the reasons the
     * #370 battery's two survival rows point to):
     *
     * - **The registration clause is unreachable at the post-check** while
     *   `disconnect` deletes only its owner's binding. An object that passed
     *   the pre-check was bound; between the two checks only a `disconnect`
     *   can unbind it, and every `disconnect` that unbinds this object
     *   retires it first — so the admissibility clause refuses it before the
     *   registration clause is read. Asking {@link #assertAdmissible} alone
     *   there is therefore equivalent. It stops being equivalent the moment a
     *   teardown can delete a binding it does not own, which is what the
     *   owner guard in `disconnect`'s `finally` prevents.
     * - **Writing the binding back inside `subscribe`**, below the caps, is
     *   equivalent too: both checks have just established that this very
     *   object is the one bound under its id, so the write stores what is
     *   already there.
     *
     * @param connection - The connection being subscribed.
     * @throws {ConnectionDisconnectedError} If this object was retired.
     * @throws {ConnectionIdInUseError} If a different object holds its id.
     * @throws {ConnectionNotRegisteredError} If nothing registered it.
     */
    #assertBound(connection: Connection<Identity>): void {
        this.#assertAdmissible(connection)
        if (!this.connections.has(connection.id)) {
            throw new ConnectionNotRegisteredError()
        }
    }

    /**
     * Whether `connection` is the object that owns its id (#363) — the one
     * spelling of that question in this class.
     *
     * Its askers: `disconnect`'s object form, before it retires or tears down
     * anything; `disconnect`'s loop, before each channel's leave, so a
     * teardown whose object was replaced mid-loop stops; `disconnect`'s
     * `finally`, before it forgets the binding and its reverse index; and
     * `handlerHooks`' `onMessage`, before any app code runs. Synchronous, so
     * it adds nothing to the #323 turn.
     *
     * @param connection - The connection object to test.
     * @returns `true` when the binding under its id is this very object.
     */
    #isOwner(connection: Connection<Identity>): boolean {
        return this.connections.get(connection.id) === connection
    }

    /**
     * Register a live connection — the only way a connection object becomes
     * bound to its id (#370).
     *
     * **A transport must call this from its open hook** (#361), with the
     * connection object it will present for the socket's whole life — the
     * first of the three lifecycle duties `docs/realtime.md` states
     * (§ *Your connection ids and your transport's lifecycle*); the
     * same-object duty is {@link Connection}'s. It is enforced: `subscribe`
     * refuses an object this method never bound with
     * {@link ConnectionNotRegisteredError}, before its authorizer runs.
     * `handlerHooks` registers from `onOpen` for you.
     *
     * Registering the same object twice is a no-op. A different object under
     * an id another object holds — live or still being torn down — is refused.
     *
     * @param connection - The connection to track.
     * @throws {ConnectionDisconnectedError} If a `disconnect` has already
     *   begun for this object.
     * @throws {ConnectionIdInUseError} If a different object holds the same
     *   id, live or still being disconnected.
     * @throws {ConnectionIdError} If `connection.id` is outside the supported
     *   charset.
     */
    register(connection: Connection<Identity>): void {
        this.#assertAdmissible(connection)
        this.#assertUsableId(connection.id)
        this.connections.set(connection.id, connection)
    }

    /** The count of tracked connections. */
    get connectionCount(): number {
        return this.connections.size
    }

    /**
     * The **effective** per-connection watched-channel cap this instance was
     * constructed with (#329).
     *
     * {@link MAX_CHANNELS_PER_CONNECTION} is only its DEFAULT, and reading the
     * default where the effective value was meant is how a correct rule
     * produces a wrong number: a deployment passing `maxChannelsPerConnection`
     * gets a different cap, and `docs/realtime.md` shows exactly that.
     *
     * It is exposed because an application's own verb budget — which is where
     * verb-rate policy lives, see {@link ChannelManager.handlerHooks} — has to
     * clear this value as its burst, or it refuses the reconnect of a client
     * re-issuing a channel set the framework itself permitted. A budget sized
     * against the default is wrong for every configured deployment, and wrong
     * in the direction that refuses legitimate traffic.
     *
     * @returns The cap, as a positive integer.
     *
     * @example
     * ```ts
     * // A token bucket whose burst can never refuse a legitimate reconnect.
     * const burst = manager.maxChannelsPerConnection
     * ```
     */
    get maxChannelsPerConnection(): number {
        return this.#maxChannelsPerConnection
    }

    /**
     * Subscribe a connection to a channel, enforcing authorization for
     * private/presence channels.
     *
     * @param connection - The subscribing connection.
     * @param channel - The channel name.
     * @returns Whether it was authorized, plus — for a presence channel — the
     *   bounded `here` snapshot: at most `maxPresenceSnapshotMembers` members
     *   with the joiner's own among them, the roster's `total`, and whether
     *   that roster is every instance's or only this one's — see
     *   {@link SubscribeResult}. The members are deep-frozen (#354) and may be
     *   the very objects other callers receive: copy one before changing it
     *   (`{ ...m, info: { ...m.info, extra } }` or `structuredClone(m)`). A
     *   write throws `TypeError`. `here` and its `members` array are yours.
     * @throws {ConnectionIdError} If `connection.id` is outside the supported
     *   charset. That is a caller bug, not an authorization outcome — a denied
     *   subscribe answers `{ ok: false }`, and folding the two together would
     *   put a policy decision and a defect behind the same branch.
     * @throws {AuthorizeResultError} If the authorizer returned anything but
     *   `true`, `false` or a non-array object (#347) — `undefined`, `null`,
     *   `0`, `''`, `'yes'`, an array, a boxed primitive. The same reasoning
     *   as the line above: that is a defect in the authorizer, not a denial.
     *   Raised before the member id check, the caps and every write, so
     *   nothing is written, published or delivered; on a channel already held
     *   it removes nothing (#331).
     * @throws {ChannelNameError} If `channel` is not a usable channel name.
     * @throws {ConnectionDisconnectedError} If a `disconnect` has begun for
     *   this connection object (#361). Raised before the authorizer when the
     *   disconnect began first — the authorizer then never runs — and again
     *   after it, when the disconnect began while it ran; always before
     *   anything is written. A denial is still `{ ok: false }` and a result
     *   outside the contract still throws its own error: the refusal
     *   replaces only an admission.
     * @throws {ConnectionIdInUseError} If a different object presents an id
     *   still bound to a connection being disconnected (#361), at the same
     *   two points.
     * @throws {PresenceMemberShapeError} If an object result has an own key
     *   other than `id` and `info`, or an `info` that does not serialize to
     *   a JSON object — one that serializes to nothing (a function, a
     *   symbol) included (#350). **On a private channel too** (#357): an
     *   object admits only as a `PresenceMember` on every kind, so a lookup
     *   wrapper such as a Deno KV entry or a pg `QueryResult` is refused here
     *   instead of admitted; a private channel then discards the member. The
     *   member a presence room receives is exactly the JSON round trip of
     *   `{ id, info }`, read once from the authorizer's object — never that
     *   object, and never any other key of it.
     * @throws {PresenceMemberIdError} If an object result's id is not a string
     *   or a finite number (#346), or is empty or too long (#306) — on either
     *   channel kind (#357); `{}` is the usual private-channel cause.
     * @throws {PresenceMemberSizeError} If an object result serializes past
     *   the configured byte bound (#326), on either channel kind (#357).
     * @throws {ConnectionNotRegisteredError} If `register` never bound this
     *   connection object (#370). Raised at the first check, always before
     *   the authorizer runs and before anything is written.
     * @throws {ConnectionIdInUseError} If a different object holds this
     *   connection's id, live or still being torn down (#363). Raised before
     *   the authorizer runs.
     * @throws {ConnectionDisconnectedError} If a `disconnect` has begun for
     *   this object — before the authorizer, or while it ran (#361).
     * @throws {ChannelLimitError} If the join would take this instance or this
     *   connection past a watched-channel cap, or past the share reserved for
     *   connections with no identity. Raised only AFTER authorization, so an
     *   unauthorized caller is denied on its own terms and never learns the
     *   instance is full.
     * @throws Whatever the authorizer itself throws or rejects with,
     *   propagated unchanged — Lockness wraps only a result it can read. That
     *   includes an error raised while `await` reads the result's `then` (a
     *   revoked Proxy's `TypeError`, a throwing `get` trap or `then` getter)
     *   and an error an object result's own `ownKeys` or `get` trap throws
     *   during the one read of its `id` and `info`, on either channel kind
     *   (#353, #357). All of them
     *   propagate before anything is written, published or delivered.
     * @throws If the authoritative roster refuses a presence join. The
     *   rejection is propagated, and the instance is left as it was: no
     *   subscriber received a `joined`, no local membership survives, and a
     *   0→1 channel subscription taken by the attempt is released. The same
     *   pair can be retried and yields one member (#323/FR-003).
     *
     *   **A `joined` frame from THIS instance follows the successful roster
     *   write that filled the member's slot** — a connection of a member
     *   already held anywhere sends none (#344). It is not an authorization
     *   token: a frame re-emitted on
     *   another instance comes from the control plane without a roster read
     *   ({@link handleControl}), and an application must re-authorize an action
     *   rather than infer permission from a presence frame or a snapshot.
     *
     * **A RE-SUBSCRIBE to a presence channel this connection already holds is a
     * roster READ.** It writes nothing, emits no `joined` to anyone, publishes
     * nothing to other instances, and never throws once the authorizer's result
     * and member are accepted — and it returns the same
     * `SubscribeResult` a first join returns, so a client re-subscribing after
     * a network blip cannot tell the difference and is never refused.
     * **Zero writes is not zero cost** (#329): the read is one authoritative
     * roster fetch per inbound frame — bounded to K members plus the
     * joiners' own (#341), and its reply bounded to K (#339).
     * `docs/realtime.md` carries the per-frame table. `joined`
     * records a transition and membership is a set, so a connection already in
     * the room transitions nothing (#327).
     *
     * Its `member` payload is **discarded**: an authorizer returning different
     * `info` on the second call leaves the original entry standing and
     * broadcasts nothing. There is no "member updated" event in this protocol
     * and `joined` must not be pressed into service as one; detecting a change
     * would mean deep-equality over unbounded application `info` on every
     * inbound frame.
     *
     * **A DENIAL NEVER REVOKES** (#331). The authorizer runs on every
     * private/presence subscribe, re-subscribes included — and when one that
     * previously approved now denies, this answers `{ ok: false }` and changes
     * nothing: the connection keeps its subscription, its roster entry and its
     * delivery. `authorize` gates admission; this method adds or does nothing,
     * and never removes. To act on a revoked entitlement call
     * {@link ChannelManager.unsubscribe} for one channel or
     * {@link ChannelManager.evict} for the connection — the latter is durable
     * and crosses processes, which a denial-driven removal would not be.
     *
     * The authorizer runs BEFORE the re-join guard above, and that order is
     * load-bearing: an unauthorized caller is denied on its own terms and never
     * learns whether the channel exists or who is in it. Its consequence is
     * that a denied re-subscribe returns `{ ok: false }` and **not** the roster.
     */
    async subscribe(
        connection: Connection<Identity>,
        channel: string,
    ): Promise<SubscribeResult> {
        // FIRST, before `channelKind` and before the awaited authorize. It
        // sat after both, so an out-of-charset id returned `{ ok: false }`
        // whenever the app's authorizer denied — the same id that throws on a
        // public channel — and the authorizer (a DB read, an audit write, a
        // rate-limit increment) ran on an id that was never usable.
        this.#assertUsableId(connection.id)
        // BEFORE `channelKind` and before the awaited authorizer, for the same
        // reason the id assertion is (#314): the authorizer may be a DB read, an
        // audit write or a rate-limit increment, and running it for a channel
        // that can never work spends that side effect on nothing.
        this.#assertUsableChannel(channel)
        const kind = channelKind(channel)
        this.#assertBound(connection)

        let member: PresenceMember | undefined
        if (kind !== 'public') {
            // A private/presence channel needs a verified identity (S1) and the
            // app's approval — before any event is ever delivered.
            if (connection.identity === null) return { ok: false }
            const result: AuthorizeResult = this.authorize
                ? await this.authorize(connection.identity, channel)
                : false
            // STRAIGHT AFTER the awaited authorizer, and ahead of the member
            // id check, the size check, the caps and every write (#347). The
            // authorizer is application code and the type does not reach it:
            // `(await select())[0]`, an `any` row or plain JS hand this any
            // value. Only `true` or a member admits and only `false` denies;
            // anything else is a defect, thrown rather than folded into
            // `{ ok: false }` (#331 gives that one meaning), and refused before
            // anything exists to undo. On a channel already held it throws
            // and removes nothing, exactly as a denial does.
            const verdict: AuthorizeVerdict = classifyAuthorizeResult(result)
            if (verdict.verdict === 'invalid') {
                throw new AuthorizeResultError(channel, verdict.type)
            }
            if (verdict.verdict === 'deny') return { ok: false }
            // An object result is admitted as a PresenceMember on EVERY kind
            // (#357). On a private channel it used to admit for being an
            // object at all — and a lookup that found nothing usually is one:
            // a Deno KV `{ key, value: null, versionstamp: null }`, a pg
            // `QueryResult` with `rows: []`, `{}`. One rule, one home, and the
            // same outcome for a value on `private-X` as on `presence-X`.
            //
            // THE ONE READ of the authorizer's object (#350): its keys, its
            // `id` and its `info` are each read once, and what comes back is
            // the JSON round trip of `{ id, info }`. BEFORE the member
            // invariant, the caps and every write (#306, #326): refusing after
            // the roster write is a partial write, not a refusal. Stays
            // synchronous — nothing may be awaited between the verdict and
            // `#checkChannelCaps` (#323/#327).
            const returned = verdict.member === undefined
                ? undefined
                : admitPresenceMember(
                    verdict.member,
                    this.#maxPresenceMemberBytes,
                )
            // The kind decides only what the admitted member is FOR: presence
            // seats it (or the connection id, for `true`); private discards it
            // — a frozen copy (#354) that nothing holds.
            if (kind === 'presence') {
                member = returned ?? admitPresenceMember(
                    { id: connection.id },
                    this.#maxPresenceMemberBytes,
                )
            }
        }

        // An INVARIANT since #347, not a filter. It read
        // `kind === 'presence' && member`, and a falsy "member" such as `0`
        // fell through to `#joinLocal`: delivery with no roster entry, an
        // invisible listener. Every presence admission now carries a member
        // (the `admitPresenceMember` admission above), so reaching here
        // without one is a bug in this method, and it must not degrade into
        // that listener. Checked HERE, above the caps and every write (#353):
        // it is a refusal like the others, and a refusal after a write would
        // leave that write behind — the partial write #306 and #347 ordered
        // everything else to avoid.
        if (kind === 'presence' && member === undefined) {
            throw new Error(
                'realtime: a presence admission reached the join without ' +
                    'a member — an invariant of subscribe is broken (#347).',
            )
        }
        // The post-check (#361): the disconnect may have begun while the
        // authorizer ran. No await from here to the join's adds.
        this.#assertBound(connection)

        // BEFORE any membership mutation, and after authorization: an
        // unauthorized subscribe is denied on its own terms, and a cap breach
        // is not an authorization outcome (#295/FR-017, §5 row 14).
        this.#checkChannelCaps(
            channel,
            connection.id,
            connection.identity !== null,
        )

        // `member` is set on a presence admission and nowhere else, and the
        // invariant above guarantees it there, so it IS the presence
        // discriminator here — and it narrows without a cast.
        if (member !== undefined) {
            return await this.#joinPresence(connection, channel, member)
        }

        await this.#joinLocal(channel, connection.id)
        return { ok: true }
    }

    /**
     * The presence half of {@link subscribe} — the re-join guard, the local
     * claim, the authoritative roster write with its compensation, and the
     * closing read (#328). It announces nothing itself: the roster write
     * announces the member's arrival when, and only when, it fills the slot
     * (#344).
     *
     * **Extracted, and the extraction is the whole point.** `subscribe` had
     * grown to 198 lines carrying seven responsibilities, and it grew during
     * #323 rather than shrinking: both HIGH fixes that review demanded — the
     * compensation and the roster-source discriminator — added statements
     * here. The branch that made the behaviour correct made the structure
     * worse.
     *
     * **The call boundary does NOT break the cap's synchronous turn.** An
     * `async` body runs synchronously until its first `await`, so
     * `#checkChannelCaps` in the caller, the re-join guard's read and claim
     * here, and `#joinLocal`'s set/index adds all still land before anything
     * yields. That is the #323 invariant and the #327 one at once, and
     * `presence_cap_concurrency_323.test.ts` is what proves the boundary did
     * not cost it. Anything added above the first `await` of this method must
     * keep that true.
     *
     * Returns the full {@link SubscribeResult} rather than the members array
     * the issue proposed: `here.source` cannot ride on a bare array, and
     * re-wrapping it in the caller would put the authoritative-versus-local
     * distinction back in the method this extraction exists to shrink.
     *
     * @param connection - The joining connection.
     * @param channel - The presence channel.
     * @param member - The member the authorizer returned, already asserted.
     * @returns The join's result, with the roster and its source.
     * @throws If the authoritative roster refuses the write; the local state
     *   is compensated first, and nothing was announced for the refused hold.
     */
    async #joinPresence(
        connection: Connection<Identity>,
        channel: string,
        member: PresenceMember,
    ): Promise<SubscribeResult> {
        // BOOKKEEPING FIRST, ANNOUNCEMENT LAST (#323). `#joinLocal` and the
        // `presence` write are state no subscriber can observe; the frame is
        // the only visible effect, and it must not claim a membership the
        // authoritative roster has not accepted.
        //
        // The authoritative write does NOT move above this. `#joinLocal`'s
        // set/index adds run in the same synchronous turn as
        // `#checkChannelCaps` above, and that pairing is what keeps the cap
        // exact — an awaited round-trip between them lets concurrent joins
        // read one count and all act on it. `#joinLocal` says so itself.
        let members = this.presence.get(channel)
        if (!members) this.presence.set(channel, members = new Map())
        // A RE-JOIN IS NOT A JOIN (#327). `joined` is a domain event and
        // must record a transition; membership is a set, so a subscribe to
        // a channel this connection already holds transitions nothing. It
        // announces nothing, writes nothing, publishes nothing — though
        // NOT nothing at all: the closing read below is one authoritative
        // roster fetch per frame, bounded to K plus the joiners' own entries
        // (#329, #341) —
        // and returns the same bounded snapshot a first join returns (#339),
        // because a client re-subscribing after a network blip is
        // legitimate traffic and must not be able to tell the difference.
        //
        // Announcing it told every local subscriber AND — through
        // `publishControl`, which `handleControl` re-emits — every
        // subscriber on every OTHER instance that a member already in the
        // room had joined it. The cost was the room's cluster-wide
        // population per inbound frame, charged by neither cap, because a
        // cap that meters set growth cannot meter an operation that grows
        // no set. The defect was never only the missing meter: the event
        // itself was false, and a budget bounds how often a wrong event is
        // produced without making it right.
        //
        // THE PAYLOAD IS DISCARDED, deliberately. Detecting a changed
        // `info` means deep-equality over unbounded application data
        // (#326) — per-frame cost proportional to what an attacker
        // controls. The domain has no "member updated" event, `joined` is
        // not one, and pressing it into service as one abuses the
        // vocabulary. That is `presence:update`, if a caller ever needs it.
        if (members.has(connection.id)) {
            return await this.#closingRead(channel, connection.id)
        }
        // CLAIMED IN THE SAME SYNCHRONOUS TURN as the check above, and
        // before `#joinLocal`'s first `await`. The guard is otherwise a
        // check-then-act across a suspension: `#joinLocal` awaits `#watch`,
        // and `onMessage` is dispatched as `void guard(...)`, so K
        // pipelined subscribe frames would all read "not a member" and all
        // perform a full join. This is the #323 cap invariant applied to
        // the second check-then-act pair in this method, not a competing
        // rule — `#checkChannelCaps` reads, this claims, `#joinLocal`
        // spends, and all three land before the method's first await.
        members.set(connection.id, member)
        await this.#joinLocal(channel, connection.id)
        // The authoritative roster is the driver's (FR-005/FR-006), and the
        // write goes through the per-slot projection (#330) rather than
        // straight at the driver: by the time this resolves, an `unsubscribe`
        // that overtook this join may already have removed the membership, and
        // the projection then issues that removal instead of re-adding a
        // member nothing local holds.
        //
        // THE ANNOUNCEMENT IS THE PROJECTION'S, not this method's (#344). Only
        // the queued write that observes the slot fill (`arrived`) announces
        // `joined`, so a second tab of a member already present, and a join
        // superseded by the leave that overtook it, announce nothing — and
        // there is no "superseded" branch left here to keep in step with it.
        const origin = { clientId: connection.id, member }
        {
            try {
                await this.#syncRosterMember(channel, origin)
            } catch (error) {
                // NOTHING WAS ANNOUNCED, so nothing is retracted — the
                // compensation is internal only, and no `left` goes out for
                // a member no subscriber was ever told about. That is the
                // whole reason this beats rolling back a visible join.
                // UNCONDITIONAL, and only because the guard above makes
                // it so (#327). This used to capture `wasSubscribed` and
                // `priorMember` and restore rather than delete, because a
                // re-join could reach this write and an undo would evict a
                // membership the call never created. A re-join can no
                // longer get here at all — it returns before `#joinLocal`
                // — so this call is provably the one that created both the
                // subscription and the member entry, and undoing exactly
                // what it did is the whole compensation. Mirrors
                // `unsubscribe` THROUGH THE ONE HELPER both leave paths share,
                // so the 1→0 delete cannot hold for one of them and not the
                // other — this rollback is the only other way a presence
                // membership is taken out, and it creates the entry it is
                // undoing.
                this.#forgetPresenceMember(channel, connection.id)
                // COLLECTED, NOT AWAITED-THEN-THROWN (#373). The leave's own
                // `unwatchChannel` round trip can reject on the 1→0
                // transition, and a broker connection drop is exactly what
                // ALSO loses the roster reply the `catch` above is reacting
                // to — the two faults are correlated, not independent. An
                // `await` here that let the leave's rejection propagate skips
                // the reclaim below entirely and surfaces the unwatch failure
                // in place of the roster failure that actually refused the
                // join. `#collectLeaveOutcome` is the one helper this shares
                // with `unsubscribe`'s #361 ordering, which runs its own
                // roster release the same way: unconditionally, after
                // collecting rather than awaiting-then-throwing the leave.
                const leaveOutcome = await this.#collectLeaveOutcome(
                    channel,
                    connection.id,
                )
                if (leaveOutcome.failed) {
                    // Reported, never thrown: the reclaim below still must
                    // run, and the ORIGINAL roster error is still what this
                    // branch ends on.
                    try {
                        console.warn(
                            `realtime: the local leave during a #323 join ` +
                                `compensation on ${safeForLog(channel)} ` +
                                `failed, before the roster reclaim: ${
                                    renderError(leaveOutcome.error)
                                }`,
                        )
                    } catch (sink) {
                        // The `throw error` below must survive a throwing
                        // sink too — `writeMarkedFallback` never throws
                        // (#391).
                        writeMarkedFallback(
                            JOIN_COMPENSATION_LOG_FAILED,
                            leaveOutcome.error,
                            { label: 'sink failure', error: sink },
                        )
                    }
                }
                // The write is atomic, but its REPLY can still be lost: a
                // connection dropped after the script commits looks exactly
                // like one that never ran. Ask for the removal rather than
                // leave an orphan carrying the member's `info` until the
                // ghost sweep reaches it — and it only reaches it once this
                // instance has been declared dead, which for a healthy
                // process is never.
                //
                // THROUGH THE SAME SERIALIZED PATH (#330), not a third
                // unordered write. The local delete above already ran, so the
                // projection computes "absent" and issues the removal on its
                // own — the reclaim falls out of the design rather than being
                // a separate call that could race the very write it undoes.
                //
                // A reclaim that finds a committed hold returns `gone` and
                // announces a truthful `left` for a member no `joined` was
                // sent for — the accepted cost of a reply lost after commit.
                //
                // RUNS UNCONDITIONALLY (#373), whether or not the leave above
                // failed: a failed unwatch does not mean the local membership
                // was not already removed, and this reclaim's desired state
                // is read from that removal, not from the leave's outcome.
                //
                // A FAILED RECLAIM IS QUEUED, NOT JUST WARNED (#371): the
                // ghost sweep used to be the only backstop for a reclaim that
                // itself rejects; `#recordOwedRelease` retries it after every
                // successful heartbeat instead, through the same
                // `#syncRosterMember` this reclaim already calls.
                try {
                    await this.#syncRosterMember(channel, origin)
                } catch (cleanupError) {
                    this.#recordOwedRelease(channel, origin, cleanupError)
                }
                // ALWAYS THE ORIGINAL ERROR (#373): neither the leave's
                // failure nor the reclaim's replaces it. Both are WARNed,
                // never thrown, so the caller learns exactly what refused the
                // join — not a symptom of the compensation that followed it.
                throw error
            }
        }
        return await this.#closingRead(channel, connection.id)
    }

    /**
     * The authoritative roster snapshot a presence subscribe returns, with its
     * local fallback — the closing READ both exits of the presence branch
     * share (FR-006, #327).
     *
     * Extracted so the re-join guard and a committed first join answer with one
     * implementation rather than two that drift. A re-join must be
     * indistinguishable from a first join to the caller; two copies of this
     * block is how "indistinguishable" quietly stops being true.
     *
     * **It never throws.** By the time either caller reaches it the join has
     * fully committed — roster, local view, and whatever the queued write
     * announced (#344) — so a failure
     * here has no residue to compensate and nothing wrong to report. Throwing
     * would leave the caller believing a completed join was rejected. It
     * degrades to what this instance knows and says which it gave, so a caller
     * that cares can tell an authoritative answer from a local one.
     *
     * **The one place the snapshot is bounded (#339)**, once, after the read
     * settles, on whichever roster the read produced — authoritative or local.
     * Not at each of the three exits, where one would be forgotten; not in
     * `rosterSnapshot` or the barrier, where a cut shared between callers
     * would hand one joiner's self to another. The cut is silent, and its
     * `total` is a snapshot-time number that costs no driver command — it is
     * never added to `joined`/`left` frames, because a live count per frame is
     * the state #329 declined. `rosterSnapshot`'s spread is still what gives
     * each caller its own array: the cut returns its input when the room fits.
     *
     * **Self is looked up here, after the await, in the same statement as the
     * cut** — so the roster, the local fallback and self describe one moment.
     * The id looked up BEFORE the await (#341) is only what the read fetches.
     * Not passed from the exits: at the re-join exit the authorizer's `member`
     * is the discarded new payload, and on a superseded join the connection
     * holds no member at all, so there is correctly no self to keep.
     *
     * @param channel - The presence channel to read the roster of.
     * @param clientId - The subscribing connection, whose member is kept in
     *   the snapshot when the roster holds it.
     * @returns `{ ok: true }` with the bounded snapshot.
     */
    async #closingRead(
        channel: string,
        clientId: string,
    ): Promise<SubscribeResult> {
        // `window` is what the source reported; `members` is taken by the
        // LOCAL map at every call site — two different rosters, and naming
        // them alike is how a fallback quietly becomes the source.
        let window: RosterWindow
        let source: PresenceSnapshot['source'] = 'authoritative'
        // The id to FETCH, taken before the await (#341). It only widens what
        // the read returns, so a self outside the sampled window can still be
        // kept; it never decides what is kept — that is the post-await lookup
        // below.
        const fetchSelfId = this.presence.get(channel)?.get(clientId)?.id
        try {
            window = await this.rosterSnapshot(channel, fetchSelfId)
        } catch (error) {
            window = localWindow(this.#localRoster(channel))
            source = 'local'
            console.warn(
                `realtime: the here-roster for ${
                    safeForLog(channel)
                } could not be read; the join is committed and the ` +
                    `snapshot falls back to this instance's own members: ${
                        renderError(error)
                    }`,
            )
        }
        const here = boundPresenceSnapshot(
            window,
            this.presence.get(channel)?.get(clientId)?.id,
            this.#maxPresenceSnapshotMembers,
        )
        return { ok: true, here: { ...here, source } }
    }

    /**
     * Refuse a subscribe that would take this instance or this connection past
     * its watched-channel cap.
     *
     * **Only a join that GROWS a set counts.** A second client on a hosted
     * channel adds no subscription, and a client re-joining a channel it
     * already holds adds nothing either; charging for those would refuse work
     * that costs the broker nothing.
     *
     * **`isIdentified`, not the `Connection`.** The cap decision needs exactly
     * one bit about identity — whether this caller may reach into the reserved
     * share — and handing it the whole connection would let a later change ask
     * identity a second question here, which is how a decision acquires a
     * second home.
     *
     * Called BEFORE any membership mutation and AFTER authorization, so a
     * refusal mutates nothing and a cap breach is never mistaken for a denial.
     *
     * @param channel - The channel being joined.
     * @param clientId - The joining connection.
     * @param isIdentified - Whether the connection carries a verified identity.
     * @throws {ChannelLimitError} If either cap, or the anonymous reservation,
     *   would be exceeded.
     */
    #checkChannelCaps(
        channel: string,
        clientId: string,
        isIdentified: boolean,
    ): void {
        if (!this.subscriptions.has(channel)) {
            // Growing the hosted set is the only thing the instance cap
            // charges for, and it is the only thing the anonymous reservation
            // bounds — an anonymous connection joins an ALREADY-hosted channel
            // freely, at any size.
            // The reservation only exists while it is BELOW the cap. With the
            // share at 1 an anonymous caller is refused by the instance cap
            // itself, and the breach must say so.
            const reserved = !isIdentified && this.#anonymousReservationActive
            const limit = reserved
                ? this.#anonymousWatchedCeiling
                : this.#maxWatchedChannels
            if (this.subscriptions.size >= limit) {
                throw new ChannelLimitError(
                    reserved ? 'instance-anonymous' : 'instance',
                    this.subscriptions.size,
                    limit,
                )
            }
        }
        const owned = this.#channelsByClient.get(clientId)
        if (
            !owned?.has(channel) &&
            (owned?.size ?? 0) >= this.#maxChannelsPerConnection
        ) {
            throw new ChannelLimitError(
                'connection',
                owned?.size ?? 0,
                this.#maxChannelsPerConnection,
            )
        }
    }

    /**
     * Add a connection to a channel's local set — **the only writer**, with
     * {@link #leaveLocal}, of `subscriptions` (#295).
     *
     * Two add sites and no funnel is how a `watchChannel` gets written at one
     * and forgotten at the other, leaving a channel hosted-but-unwatched: every
     * message dropped while `subscribe` answers `{ ok: true }`, and nothing
     * logged.
     *
     * **The 0→1 test is computed in the SAME SYNCHRONOUS TURN as the add.** The
     * only `await` is the wire op that follows the decision. Reading the
     * transition after an await is what let a join during a leave's roster
     * round-trip unwatch a channel with a live authorized subscriber —
     * permanently, since the reconnect that heals every other deafness is
     * guaranteed not to re-issue a channel that left the re-issue set.
     *
     * **That rule now also holds the channel cap** (#323). `#checkChannelCaps`
     * reads `subscriptions.size` and this method spends it, with no `await`
     * between them — which is why #323 moved the presence ANNOUNCEMENT behind
     * the authoritative roster write and left the write itself below this call.
     * Hoisting a broker round-trip above these adds lets concurrent subscribes
     * read one count and all act on it, and the cap it overshoots is what
     * bounds the post-outage revocation window, not merely memory.
     *
     * **`set.size > 0` and `subscriptions.has(channel)` are ONE answer**, and
     * that is what lets `#checkChannelCaps` ask the second while this asks the
     * first. They agree only because {@link #leaveLocal} deletes the empty
     * `Set` rather than keeping it for reuse. Keep an emptied `Set` here as an
     * allocation tidy-up and the cap starts counting channels with no
     * subscribers — refusing a subscribe on an instance nowhere near its limit,
     * with an error naming a count nobody can reproduce.
     *
     * @param channel - The channel being joined.
     * @param clientId - The joining connection.
     */
    async #joinLocal(channel: string, clientId: string): Promise<void> {
        let set = this.subscriptions.get(channel)
        if (!set) this.subscriptions.set(channel, set = new Set())
        const wasHosted = set.size > 0
        set.add(clientId)
        // The reverse index. NOT a second counter — hosting is still read from
        // `subscriptions` and only from there. This answers a different
        // question, "which channels does THIS connection hold", which
        // `Map<channel, Set<clientId>>` can only answer by scanning every
        // channel. `disconnect` and the per-connection cap both need it.
        let owned = this.#channelsByClient.get(clientId)
        if (!owned) this.#channelsByClient.set(clientId, owned = new Set())
        owned.add(channel)
        if (wasHosted) return
        await this.#watch(channel)
    }

    /**
     * Remove a connection from a channel's local set, and stop hosting the
     * channel when it was the last one.
     *
     * Returns without a wire op when the connection was not a member — which is
     * what keeps `disconnect` from firing an unwatch for every channel this
     * instance has ever hosted.
     *
     * **The empty `Set` is DELETED**, so `subscriptions.has(channel)` is the one
     * spelling of "this instance hosts it". Leaving an empty set behind gives
     * "not hosted" two spellings and grows the map without bound.
     *
     * @param channel - The channel being left.
     * @param clientId - The leaving connection.
     * @returns Whether a membership was actually removed — the fact
     *   {@link unsubscribe}'s outcome is built from.
     */
    async #leaveLocal(channel: string, clientId: string): Promise<boolean> {
        const set = this.subscriptions.get(channel)
        // THE ONE MEMBERSHIP PREDICATE, and the one the report is taken from.
        //
        // There are three exits below and only this one decides whether a
        // membership existed; the other two decide whether the channel is
        // still hosted. Reporting from the END of this method instead would
        // answer `false` for every leave from a channel that still holds
        // another subscriber — the common case — because the tail is reached
        // only on the 1→0 transition. Every fixture in this package is
        // single-member, so that mistake passes the whole suite.
        if (!set?.delete(clientId)) return false
        this.#channelsByClient.get(clientId)?.delete(channel)
        if (set.size > 0) return true
        this.subscriptions.delete(channel)
        await this.#watcher?.unwatchChannel(channel)
        return true
    }

    /**
     * Await {@link #leaveLocal} once, collected rather than thrown — the
     * caller decides what to do with a failure, instead of its rejection
     * skipping whatever the caller meant to run next.
     *
     * **The one helper {@link unsubscribe} (#361) and {@link #joinPresence}'s
     * #323 compensation (#373) share.** Both run a second roster write (a
     * release, a reclaim) after the leave regardless of whether it rejected —
     * the 1→0 transition's `unwatchChannel` round trip is exactly the call
     * that can fail there, and a broker fault that fails it is the same fault
     * likely to have caused the roster write these callers are compensating
     * for in the first place. Letting the rejection propagate before either
     * caller reaches its second write would skip that write instead of
     * running it.
     *
     * **Awaited immediately, inside this method's own `try`.** A promise held
     * aside and only awaited later, after other work runs first, is what Deno
     * reports as an unhandled rejection if it settles rejected before that
     * later `await` — collecting it here, in the same turn `#leaveLocal` is
     * called, is what keeps this method's own return the only thing either
     * caller awaits across that gap.
     *
     * @param channel - The channel to leave locally.
     * @param clientId - The connection leaving it.
     * @returns The leave's own result, or its rejection collected instead of
     *   thrown.
     */
    async #collectLeaveOutcome(
        channel: string,
        clientId: string,
    ): Promise<LocalLeaveOutcome> {
        try {
            return {
                left: await this.#leaveLocal(channel, clientId),
                failed: false,
            }
        } catch (error) {
            return { left: false, failed: true, error }
        }
    }

    /**
     * Remove one member from a presence channel's local map, and forget the map
     * itself when it was the last one.
     *
     * **The empty `Map` is DELETED**, for both of the reasons
     * {@link #leaveLocal} gives for its `Set`, and for a third this map has on
     * its own.
     *
     * *Unbounded growth.* Every unique presence name this instance had ever
     * hosted used to retain an empty inner `Map` for the life of the process,
     * so a `subscribe` / `unsubscribe` cycle over fresh names was not
     * cost-neutral at rest — which is the premise #329's cost accounting rests
     * on when it calls the net set delta zero.
     *
     * *One spelling.* `presence.has(channel)` is now the single answer to "does
     * this instance hold members here", exactly as `subscriptions.has(channel)`
     * is the single answer to "does it host the channel". Keeping an emptied
     * map gives "no members" two spellings and lets a later reader pick the
     * one that is only accidentally right.
     *
     * *And it is READ on every leave, not merely retained.*
     * {@link #syncRosterMember} derives its desired state by scanning
     * `presence.get(channel)` inside its serial tail, so a stranded entry is a
     * scan the projection pays for on each slot write — the cost is no longer
     * purely memory. An absent entry and an empty one compute the same absent
     * state there, which is what makes deleting behaviour-preserving rather
     * than merely tidy.
     *
     * @param channel - The presence channel being left.
     * @param clientId - The leaving connection.
     * @returns The member that was removed, or `undefined` when this connection
     *   held no membership here — the fact {@link unsubscribe} gates its
     *   roster release on (the release, not `unsubscribe`, decides whether a
     *   `left` is announced, #344).
     */
    #forgetPresenceMember(
        channel: string,
        clientId: string,
    ): PresenceMember | undefined {
        const members = this.presence.get(channel)
        const member = members?.get(clientId)
        // THE MEMBERSHIP PREDICATE, and the 1→0 delete below may not be reached
        // without it. A call that removed nothing must not drop a map that
        // still holds somebody else's membership — `members.size === 0` is only
        // the right question once this call has actually taken one out.
        if (!members || !member) return undefined
        members.delete(clientId)
        if (members.size === 0) this.presence.delete(channel)
        return member
    }

    /**
     * Ask the driver to subscribe to `channel`, and survive its refusal.
     *
     * **A rejection keeps the membership.** The driver records the channel in
     * whatever set it re-issues, so a self-healing driver restores delivery on
     * its next successful activation; dropping the membership here would turn a
     * transient write failure into permanent local deafness. The join window is
     * then the driver's retry backoff, which is a documented guarantee change
     * rather than a silent one.
     *
     * `{ ok: true }` still, deliberately: the join succeeded and delivery
     * resumes. `{ ok: false }` would make resource-level trouble
     * indistinguishable from an authorization denial in every application's
     * client code — the same argument `ChannelLimitError` makes from the other
     * side.
     *
     * @param channel - The channel to begin receiving.
     */
    async #watch(channel: string): Promise<void> {
        const watcher = this.#watcher
        if (!watcher) return
        try {
            await watcher.watchChannel(channel)
        } catch (error) {
            console.warn(
                `realtime: the driver could not subscribe to ${
                    safeForLog(channel)
                } — the membership is kept and the driver's own retry is what ` +
                    `restores delivery: ${renderError(error)}`,
            )
        }
    }

    /**
     * Build the `<channel>\0<memberId>` roster slot key (#330, #408).
     *
     * **The one place the separator and its assembly are decided.** Every
     * site that builds or parses this key — {@link #syncRosterMember}'s tail,
     * {@link #recordOwedRelease}'s ledger, {@link #drainOwedReleases}'s
     * decode — routes through this pair rather than agreeing by convention.
     * The NUL is safe as a separator only because a channel name's charset
     * (`isValidName`) can never contain one; a member id's own charset is
     * unconstrained (type only, #346) and may embed a NUL of its own without
     * breaking the split — see {@link #rosterSlotChannel}.
     *
     * @param channel - The presence channel owning the slot.
     * @param memberId - The member id the slot belongs to, already
     *   `String()`-coerced by the caller.
     * @returns The composite key used by {@link #rosterTails},
     *   {@link #heldSlots} and {@link #owedReleases}.
     */
    #rosterSlotKey(channel: string, memberId: string): string {
        return `${channel}\0${memberId}`
    }

    /**
     * Recover the channel from a slot key built by {@link #rosterSlotKey}
     * (#408).
     *
     * Only the channel is ever re-derived here — the field after the first
     * NUL is never re-parsed. A channel can never contain one (`isValidName`'s
     * charset), so slicing at the first NUL always recovers it exactly,
     * whatever the member id itself contains, including a NUL of its own.
     *
     * @param key - A key built by {@link #rosterSlotKey}.
     * @returns The channel prefix, up to the first NUL.
     */
    #rosterSlotChannel(key: string): string {
        return key.slice(0, key.indexOf('\0'))
    }

    /**
     * Write one authoritative roster slot to match this instance's local
     * `presence` map, with at most one write in flight for that slot (#330).
     *
     * **The desired state is DERIVED, not passed.** The caller names a slot;
     * this reads what the local map says about it **at issue time**, inside the
     * serial tail, and writes that. So a join whose membership was removed
     * while its write was queued issues a release, not the hold it set out to
     * make — and there is no version, epoch or tombstone to keep in step with
     * anything, because nothing is remembered.
     *
     * **Why the ad-hoc calls it replaces were wrong.** `#joinLocal` and
     * `#leaveLocal` compute their transition and issue their wire op in the
     * SAME synchronous turn, so racing verbs issue `watchChannel` /
     * `unwatchChannel` in decision order. The roster write was the one place
     * that did not: `#joinPresence` claims, suspends at `#watch`, and only then
     * issues the hold — from a state that no longer holds the membership.
     * With `RedisClient.command` chaining onto its tail synchronously at call
     * time, commit order is enqueue order, so a removal enqueued first and an
     * add enqueued second left the member in the authoritative roster with no
     * local membership and its `left` already announced. Only the ghost sweep
     * reclaims that, and a live instance never sweeps its own owned set.
     *
     * The slot is keyed by `member.id`, not by connection id, because that is
     * what the roster hash is keyed by — two connections sharing one member id
     * are one slot, and the projection writes the one {@link #localRoster}
     * keeps: the earliest-joined connection still subscribed (#343), the same
     * entry the local view shows.
     *
     * **The write that observes the transition announces it, and only it**
     * (#344). A hold that fills the slot (`arrived`) announces `joined` with
     * the entry it wrote; a release that empties a slot this instance held
     * (`gone`) announces `left` as `origin.member`. Every other write — a
     * second connection of a present member, a leave while another holder
     * remains, a join superseded by its own leave — announces nothing. Running
     * inside the tail is what makes that exact: two writes for one slot never
     * observe the same transition.
     *
     * **A roster-less driver decides from {@link #heldSlots}**, updated before
     * the announcement runs, so a write queued behind this one already sees the
     * slot as held or released.
     *
     * @param channel - The presence channel owning the slot.
     * @param origin - The connection the write announces as, and the member it
     *   names. It names the slot and who to announce as, never the desired
     *   state, which is read from the local map at issue time.
     * @returns Settles once the slot is written and any announcement is done.
     * @throws Whatever the driver throws; the tail still advances. An
     *   announcement failure is never thrown (see {@link #announcePresence}).
     */
    #syncRosterMember(
        channel: string,
        origin: PresenceOrigin,
    ): Promise<void> {
        const roster = this.roster
        const field = String(origin.member.id)
        const key = this.#rosterSlotKey(channel, field)
        const prior = this.#rosterTails.get(key) ?? Promise.resolve()
        const run = prior.then(async () => {
            const desired = this.#localRoster(channel)
                .find((candidate) => sameMemberId(candidate.id, field))
            if (desired) {
                // A roster-less driver still gets the projection, just no
                // write (#342); its holder count is this instance's own.
                let arrived: boolean
                if (roster) {
                    arrived =
                        (await roster.holdMember(channel, desired)).arrived
                } else {
                    arrived = !this.#heldSlots.has(key)
                    this.#heldSlots.add(key)
                }
                if (arrived) {
                    await this.#announcePresence(
                        'joined',
                        channel,
                        desired,
                        origin.clientId,
                    )
                }
                return
            }
            let gone: boolean
            if (roster) {
                gone = (await roster.releaseMember(channel, field)).gone
            } else {
                gone = this.#heldSlots.delete(key)
            }
            if (gone) {
                await this.#announcePresence(
                    'left',
                    channel,
                    origin.member,
                    origin.clientId,
                )
            }
        })
        // The tail must always settle so the next write for this slot runs;
        // `run` still rejects to this caller, so nothing is swallowed.
        const tail = run.catch(() => {})
        this.#rosterTails.set(key, tail)
        void tail.then(() => {
            // ONLY if nothing queued behind it, or the next write would lose
            // its predecessor and the two could overlap again.
            if (this.#rosterTails.get(key) === tail) {
                this.#rosterTails.delete(key)
            }
        })
        return run
    }

    /**
     * Announce a member's arrival or departure — locally first, then to every
     * other instance over the control plane (#344).
     *
     * **Two callers, and each announces only a bit a roster write returned**:
     * {@link #syncRosterMember}'s queued run, by the write that observed the
     * transition, and {@link #announceDeparture}, for a slot the driver
     * emptied while releasing another process's hold (#348). Any `joined` /
     * `left` emit or `presence-join` / `presence-leave` publish elsewhere (the
     * receive side in {@link handleControl} excepted) would announce per
     * connection again.
     *
     * **Local, then remote.** `joined` carries the entry the roster now holds
     * (`member` = the write's desired entry); `left` carries the departed
     * member. The local emit excludes every local connection of that member
     * id, for both actions ({@link emitPresence}, #349). `target` is
     * informational on a presence frame (see `ControlMessage.target`): the
     * origin connection for a queued write, the channel name for a reported
     * departure.
     *
     * **Each half fails on its own, as one WARN, and is never rethrown.** This
     * runs inside the slot's tail: a throw here would reject the queued write of
     * whichever call awaits it — and a join that sees its write reject rolls
     * back a hold that committed. A local emit that throws (the application's
     * `encode` refusing the frame) still lets the control publish run, so other
     * instances are not silenced by this instance's codec; a frame both halves
     * cannot carry is two WARNs, one per audience. The WARN carries the
     * channel, the action, the audience and the error only: never the member id
     * or `info`, which may be application PII.
     *
     * @param action - Which transition to announce.
     * @param channel - The presence channel.
     * @param member - The member the frame carries.
     * @param target - The control frame's informational `target`.
     */
    async #announcePresence(
        action: 'joined' | 'left',
        channel: string,
        member: PresenceMember,
        target: string,
    ): Promise<void> {
        const lost = (audience: string, error: unknown) =>
            console.warn(
                `realtime: a presence ${action} on ${
                    safeForLog(channel)
                } was not announced to ${audience} — the roster is written ` +
                    `and a roster read is correct; only the frame is lost: ${
                        renderError(error)
                    }`,
            )
        try {
            this.emitPresence(channel, {
                type: 'presence',
                channel,
                action,
                member,
            })
        } catch (error) {
            lost('local subscribers', error)
        }
        try {
            await this.publishControl({
                kind: action === 'joined' ? 'presence-join' : 'presence-leave',
                target,
                channel,
                member,
            })
        } catch (error) {
            lost('other instances', error)
        }
    }

    /**
     * Write every local roster slot again, after the driver reported through
     * `onRosterLapse` that its holds may have been released on this process's
     * behalf (#349) — on Redis, a lapsed liveness key a peer's sweep took for
     * a crash.
     *
     * **Durable revocations are applied first** ({@link reconcileRevocations},
     * the A2 / S2 ruling). A revoke issued during the lapse, whose frame this
     * instance never received, is enforced before anything is re-held, so the
     * room never hears the revoked member come back. A failed re-check is this
     * method's one WARN, naming no target or member; the re-assert still runs,
     * and the failure never becomes its rejection.
     *
     * **Through {@link #syncRosterMember}, one slot at a time**, and nothing
     * else. The desired state is read inside each slot's tail at issue time
     * (ADR 003), so a leave or a join queued on that slot is ordered with the
     * re-write; the hold's `arrived` bit decides the frame (ADR 004), so a
     * slot nobody swept announces nothing and a swept one announces one
     * `joined`. This method never announces anything itself. One at a time,
     * never `Promise.all`: K writes queued at once would sit in front of the
     * next heartbeat and cause the next lapse.
     *
     * **The pairs are snapshotted first**: the channels are the `presence`
     * keys, each channel's members are {@link #localRoster}'s (the one dedupe
     * rule, #343), and each member's origin is the connection whose entry IS
     * that member object — the one `#localRoster` kept, found by identity.
     *
     * **Every slot is tried.** A slot that rejects is recorded and the loop
     * goes on; afterwards one `Error` carries the count and the first
     * failure, never a member id or `info`. The driver logs it and retries on
     * its next successful heartbeat. The signal is checked before each slot:
     * once the driver shuts down, this resolves before the next write.
     *
     * @param signal - Aborted by the driver's shutdown.
     * @returns Settles once every slot was tried, or the signal stopped it.
     * @throws {Error} If any slot's write rejected.
     */
    async #reassertRoster(signal: AbortSignal): Promise<void> {
        // Revocations FIRST (#349 A2 / S2): a revoke whose control frame was
        // lost while this instance was partitioned would otherwise be undone
        // here — re-held, announced `joined`, then revoked again. A failed
        // re-check is one WARN and the re-assert goes on: it never joins the
        // rejection below, or a broken store would re-assert every beat.
        try {
            await this.reconcileRevocations()
        } catch (error) {
            console.warn(
                'realtime: re-checking durable revocations before ' +
                    "re-asserting this instance's presence holds failed — " +
                    'the holds are re-asserted anyway, and the periodic ' +
                    `revocation reconcile applies any it missed: ${
                        renderError(error)
                    }`,
            )
        }
        if (signal.aborted) return
        const slots: { channel: string; origin: PresenceOrigin }[] = []
        for (const [channel, members] of this.presence) {
            // One pass per channel: the entries `#localRoster` kept, and for
            // each the FIRST connection in presence order holding that very
            // object — `delete` answers true once per kept entry.
            const kept = new Set(this.#localRoster(channel))
            for (const [clientId, member] of members) {
                if (kept.delete(member)) {
                    slots.push({ channel, origin: { clientId, member } })
                }
            }
        }
        const failures: unknown[] = []
        for (const { channel, origin } of slots) {
            if (signal.aborted) return
            try {
                await this.#syncRosterMember(channel, origin)
            } catch (error) {
                failures.push(error)
            }
        }
        if (failures.length > 0) {
            throw new Error(
                `realtime: ${failures.length} presence slot(s) could not be ` +
                    `re-held after a liveness lapse: ${
                        renderError(failures[0])
                    }`,
            )
        }
    }

    /**
     * Queue a roster slot's release for retry, after a write this instance
     * could not commit (#371) — a presence leave's own release, or the
     * #323/#373 join compensation's reclaim.
     *
     * **The ledger records a TRIGGER, never a desired state** (ADR 003 §7,
     * amended by #371): draining a slot re-issues it through
     * {@link #syncRosterMember}, which re-derives what to write from
     * `presence` at drain time, exactly as every other call through that one
     * writer does. Nothing here is remembered that could fall out of step
     * with it.
     *
     * **Bounded at {@link MAX_PENDING_ROSTER_RELEASES} distinct slots.** A
     * second failure on a slot already queued always coalesces onto this
     * origin, never refused; only a genuinely NEW slot can be refused, once
     * the ledger is at its cap — an honest, named degradation under
     * sustained failure, never a silently unbounded `Map`. The ghost sweep
     * stays the backstop of last resort for a refused slot, exactly as it
     * was before #371.
     *
     * **Always logs once, and never throws** (#391): both callers sit inside
     * a `catch` that ends by re-throwing a roster error of their own, and a
     * throwing sink here must not replace it.
     *
     * @param channel - The presence channel owning the slot.
     * @param origin - The connection to announce as, and the member a `left`
     *   would name, if the retried release ever empties the slot.
     * @param error - What the failed write threw, rendered into the WARN.
     */
    #recordOwedRelease(
        channel: string,
        origin: PresenceOrigin,
        error: unknown,
    ): void {
        const key = this.#rosterSlotKey(channel, String(origin.member.id))
        const atCapacity = !this.#owedReleases.has(key) &&
            this.#owedReleases.size >= MAX_PENDING_ROSTER_RELEASES
        if (!atCapacity) this.#owedReleases.set(key, origin)
        try {
            console.warn(
                atCapacity
                    ? `realtime: could not reclaim a possibly-written ` +
                        `roster entry on ${
                            safeForLog(channel)
                        } — the pending-release ledger is at its cap ` +
                        `(${MAX_PENDING_ROSTER_RELEASES}); the ghost sweep ` +
                        `is the remaining backstop: ${renderError(error)}`
                    : `realtime: releasing a presence member's roster slot ` +
                        `on ${
                            safeForLog(channel)
                        } failed; queued for retry after the next ` +
                        `successful heartbeat: ${renderError(error)}`,
            )
        } catch (sink) {
            writeMarkedFallback(OWED_RELEASE_LOG_FAILED, error, {
                label: 'sink failure',
                error: sink,
            })
        }
    }

    /**
     * Retry every queued owed release, one slot at a time (#371) — the
     * handler {@link ChannelManager}'s constructor registers on the driver's
     * `onRosterMaintenance` hook.
     *
     * **Sequential, never `Promise.all`**, on {@link #reassertRoster}'s own
     * reasoning: K writes issued at once would sit in front of the next
     * heartbeat and manufacture the very lapse this mechanism must not cause.
     *
     * **Through {@link #syncRosterMember} alone**, which re-derives the
     * desired state from `presence` at issue time — never from the ledger's
     * stale `origin` — so a slot re-claimed by a fresh join before its turn
     * is re-derived as held, and the release this call asked for is not the
     * one it gets (#344's `arrived` decides the frame either way).
     *
     * **Silent on success** (matching every other roster write in this
     * package): a slot whose retry settles is simply removed. A slot that
     * rejects again stays queued and is logged again through
     * {@link #recordOwedRelease}, the same helper the original failure used.
     *
     * @returns Settles once every slot queued when this run began was tried.
     */
    async #drainOwedReleases(): Promise<void> {
        for (const [key, origin] of [...this.#owedReleases]) {
            const channel = this.#rosterSlotChannel(key)
            try {
                await this.#syncRosterMember(channel, origin)
                // ONLY if nothing overwrote this slot's entry while the write
                // was in flight — the same guard `#syncRosterMember` uses for
                // its own tail, and for the same reason: a fresher failure
                // recorded during this await must survive the delete below.
                if (this.#owedReleases.get(key) === origin) {
                    this.#owedReleases.delete(key)
                }
            } catch (error) {
                this.#recordOwedRelease(channel, origin, error)
            }
        }
    }

    /**
     * Announce a departure the driver reported through
     * `onRosterDeparture` — a roster slot it emptied while releasing another
     * process's hold, such as the ghost sweep of a crashed instance (#348).
     *
     * **What a driver reports is checked before anything is emitted** (S3).
     * A departure whose channel is not a valid name, or whose member fails
     * `isPresenceMemberWire` (the #346 id rule, a plain-object `info`, no key
     * but `id` and `info` — what every peer's ingest asks of the same
     * member), is
     * dropped with one WARN naming the channel only: every peer would refuse
     * that frame at ingest, and this instance must not show its own
     * subscribers what the rest of the fleet cannot. Never throws.
     *
     * **Not queued on the slot's roster tail, and nothing is awaited before
     * {@link #announcePresence}** (A1, W8). A hold of the same slot issued
     * while the sweep's release was outstanding commits after that release;
     * its reply is behind the release's on the one command client, so this
     * `left` is emitted and its publish issued before that hold's `joined`.
     * Chained on the tail instead, it would wait for the hold and its
     * announcement, and the room would hear `joined` then `left` for a member
     * who is present.
     *
     * @param departure - What the driver reported.
     * @returns Settles once the announcement is done; never rejects.
     */
    #announceDeparture(departure: RosterDeparture): Promise<void> {
        // Each field is read ONCE, and a throwing getter or trap on the
        // departure itself is a malformed report like any other — dropped,
        // never thrown: the check and the announcement must see one value.
        let channel: unknown
        let member: unknown
        try {
            channel = departure?.channel
            member = departure?.member
        } catch {
            channel = undefined
            member = undefined
        }
        // What every peer's ingest requires of a presence frame's member — the
        // one wire-member rule, never a copy here.
        if (
            typeof channel !== 'string' || !isValidName(channel) ||
            !isPresenceMemberWire(member)
        ) {
            console.warn(
                `realtime: dropped a roster departure the driver reported on ${
                    typeof channel === 'string'
                        ? safeForLog(channel)
                        : `a ${typeLabel(channel)} channel`
                } — it is not a departure this instance can announce. The ` +
                    'driver must report a valid channel and a member it could ' +
                    'have admitted.',
            )
            return Promise.resolve()
        }
        return this.#announcePresence('left', channel, member, channel)
    }

    /**
     * The authoritative "here" roster for a presence channel — the driver's when
     * it owns one (every instance's members, FR-006), otherwise this instance's
     * local members, one entry per member (#343) — a driver with no roster
     * capability is single-process.
     *
     * **Every authoritative read in this class goes through here**, and #333 is
     * why that mattered: this is the only caller of the barrier, so one edit
     * shapes the read for every entry point. It is reached once per presence
     * `subscribe` frame, re-joins included — which is why the read is a
     * bounded window (#341): K members plus the callers' own, whatever the
     * room's size.
     *
     * **Returns a window, authoritative or local**, never a bare list: the
     * local path goes through `localWindow`, so there is one read shape.
     *
     * **The spread is load-bearing, not defensive.** The barrier hands ONE
     * window to every caller sharing a read; without this copy they would share
     * a mutable `members` list. The members inside it are still shared by
     * reference, and deliberately so — a per-caller deep copy would restore a
     * per-caller cost, in CPU instead of bytes — which is safe because every
     * member is deep-frozen where it is minted (#354).
     *
     * @param channel - The presence channel.
     * @param selfId - The caller's member id for the read to fetch, if any.
     */
    private async rosterSnapshot(
        channel: string,
        selfId?: string | number,
    ): Promise<RosterWindow> {
        // Branching on the BARRIER, not on `roster`: they are constructed
        // together, and reading the thing actually used leaves no second
        // spelling of "this driver owns a roster" to fall out of step.
        const reads = this.rosterReads
        if (reads) {
            const window = await reads.snapshot(channel, selfId)
            return { ...window, members: [...window.members] }
        }
        return localWindow(this.#localRoster(channel))
    }

    /**
     * This instance's own members of a presence channel, ONE entry per member
     * — the only way this class reads the `presence` map's values (#343).
     *
     * The map is keyed by connection id; the roster by member id. Reading the
     * values raw counted a member with two tabs twice on the local fallback
     * and on a roster-less driver, and let the slot projection pick a
     * different connection than the local view showed. Routing every read
     * through {@link uniqueMembers} keeps one rule for all three.
     *
     * Deduplicating HERE, not in `boundPresenceSnapshot`: the authoritative
     * roster is already one entry per member, and a dedupe on the cut would
     * add an O(room) pass to every authoritative read while hiding a driver
     * that returned duplicates. The map is not re-keyed either — the #327
     * claim, the #334 last-member delete and the self lookup all key on the
     * connection.
     *
     * @param channel - The presence channel.
     * @returns A fresh array, earliest-joined connection's member first.
     */
    #localRoster(channel: string): PresenceMember[] {
        return uniqueMembers(this.presence.get(channel)?.values() ?? [])
    }

    /**
     * Publish a control message to other instances when the driver exposes the
     * control plane; a single-process driver (no `publishControl`) has none, and
     * relies on the manager's direct local `emitPresence` instead.
     */
    private async publishControl(control: ControlMessage): Promise<void> {
        await this.driver.publishControl?.(control)
    }

    /**
     * Unsubscribe a connection from a channel (eviction primitive, S7).
     *
     * `async` because a presence leave releases this instance's hold on the
     * member's slot in the driver's authoritative roster, which for the Redis
     * driver is a round-trip (FR-017). **A `left` is announced only when that
     * release empties the slot** (#344) — this instance's local presence
     * subscribers get it, and every OTHER instance re-emits it from the
     * control plane (US4). A leave while another connection or instance still
     * holds the member announces nothing, and a failed announcement is a WARN:
     * this still resolves `'left'`. A failed release rejects.
     *
     * **The presence member is forgotten before the leave** (#361), and its
     * roster slot is released even when the leave rejects (a failed unwatch):
     * the leave's failure is then the rejection, and a release failure after
     * it is a WARN. A failed unwatch therefore leaves no presence member for a
     * lapse re-assert to bring back.
     *
     * **This is a LOCAL verb, and its `clientId` argument makes it look like an
     * addressed one.** It acts only on sockets this instance owns. Called with
     * an id owned by another instance it removes nothing, announces nothing and
     * reports `'not-owned'` — it does not reach across. {@link revokeChannel}
     * is the addressed verb for one channel; {@link evict} is the addressed
     * verb for a whole connection.
     *
     * **The outcome is a SERVER-side value.** Never relay it to a client, and
     * never take `clientId` from a client frame — pass `connection.id` from a
     * socket you own. The three states together would otherwise tell whoever
     * receives them whether an arbitrary id is live in the fleet, whether this
     * instance owns it, and whether it is in a given room.
     *
     * @param clientId - The connection id.
     * @param channel - The channel to leave.
     * @returns `'left'` when a membership was removed, `'not-subscribed'` when
     *   this instance owns the socket and it was not in that channel, and
     *   `'not-owned'` when the socket lives elsewhere.
     * @example
     * ```ts
     * if (await manager.unsubscribe(connection.id, channel) === 'not-owned') {
     *     // Another instance holds this socket — reach it with revokeChannel.
     *     await manager.revokeChannel(connection.id, channel)
     * }
     * ```
     */
    // DELIBERATELY NOT CHANNEL-ASSERTED (#314). This is a REMOVAL path, and
    // refusing a removal strands the state it would have removed. It is also
    // reached from `disconnect`, which iterates `#channelsByClient` — so on a
    // process that predates the boundary guard, throwing here would make every
    // disconnect fail on the first legacy name and leak every channel after
    // it. Accepting a name we would no longer create is the correct asymmetry:
    // creation is guarded, cleanup is total.
    //
    // `revokeChannel` asserts BOTH its arguments and the two verbs disagree on
    // purpose: that one mints a name onto the control plane and into a durable
    // record, this one cleans up a name the framework already admitted.
    async unsubscribe(
        clientId: string,
        channel: string,
    ): Promise<LeaveOutcome> {
        // READ BEFORE THE FIRST AWAIT. `connections` is the sole spelling of
        // ownership in this class, and `disconnect`'s `finally` deletes from
        // it — so a value sampled after the leave could report `'not-owned'`
        // for a connection this instance had just finished tearing down.
        const owned = this.connections.has(clientId)
        // FORGET BEFORE THE LEAVE (#361), the order the #323 compensation
        // already uses. The leave awaits an unwatch that may reject; forgetting
        // after it let that rejection skip the forget and the release, leaving
        // a roster ghost the lapse re-assert (#349) re-held forever. And a
        // subscribe racing a suspended leave read the member still here, took
        // the re-join guard and answered `ok` for a membership being removed.
        const member = this.#forgetPresenceMember(channel, clientId)
        // Collected through `#collectLeaveOutcome`, the one helper this
        // shares with `#joinPresence`'s #323 compensation (#373) — a
        // rejection here must not skip the release below.
        const outcome = await this.#collectLeaveOutcome(channel, clientId)
        if (member) {
            // Released through the per-slot projection (#330), WHETHER OR NOT
            // the leave failed. The local delete above is what the projection
            // reads, so this issues a release — and a join racing it can no
            // longer re-hold the member behind it, because its own write is
            // chained after this one and computes the same absent state. The
            // `left` is the projection's to send, and only when this release
            // empties the slot (#344). A failed announcement does not reject.
            try {
                await this.#syncRosterMember(channel, { clientId, member })
            } catch (error) {
                // QUEUED EITHER WAY (#371): a release this instance could not
                // commit is retried after every successful heartbeat, whether
                // or not the leave itself also failed.
                this.#recordOwedRelease(channel, { clientId, member }, error)
                // The leave's failure came first and is the one re-thrown
                // when it also failed; this release's own failure is only
                // ever reported through the ledger's WARN above.
                if (!outcome.failed) throw error
            }
        }
        if (outcome.failed) throw outcome.error
        // `left` first: something WAS removed, whatever `connections` says
        // about a socket that may already have been pruned around it.
        return outcome.left ? 'left' : owned ? 'not-subscribed' : 'not-owned'
    }

    /**
     * Disconnect a connection entirely — unsubscribe it from every channel
     * (releasing its presence holds, which announce `left` for any slot they
     * empty) and forget it (eviction primitive, S7).
     *
     * `async` (FR-017): it awaits each channel's roster release so a caller — the
     * handler's `onClose` — can await teardown before the socket is gone.
     *
     * **Local, like {@link unsubscribe}, and misaddressable the same way.**
     * Called with an id this instance does not own it iterates an empty channel
     * set and deletes two absent map entries — so it reports `'not-owned'`
     * rather than resolving as if it had torn something down. {@link evict} is
     * the addressed verb.
     *
     * The outcome is a **server**-side value; the warning on
     * {@link LeaveOutcome} applies here unchanged.
     *
     * **It retires the connection object first** (#361), in the synchronous
     * turn that copies the connection's channels: from then on `register` and
     * `subscribe` refuse that object with {@link ConnectionDisconnectedError},
     * and a different object under its id with {@link ConnectionIdInUseError}
     * while the teardown runs. A join that committed before this call is torn
     * down with the rest; one resolving after it is refused before it writes.
     * The connection stays in `connections` — still owned — until the teardown
     * ends. An id this instance does not own retires nothing.
     *
     * **Pass the connection object from your close hook** (#363) — the third
     * of the transport lifecycle duties `docs/realtime.md` states, and the one
     * this JSDoc is the home of: call `disconnect(conn)` when the socket
     * closes, with the object you registered. The object form acts only for
     * the object that owns the id: any other object — a socket `register`
     * refused, or one whose id was re-registered after an evict — gets
     * `'not-owned'` before anything is retired, copied or awaited. The id form
     * acts on whoever holds the id when it runs, which is what `evict` needs
     * and what a late close must not do.
     *
     * **The teardown acts only while its object owns the id.** Each channel's
     * leave is keyed by id, so the loop stops as soon as the object it is
     * tearing down no longer owns the id, and the `finally` deletes the binding
     * and the reverse index only while it still does. A teardown whose object
     * was replaced while it ran — an evict, then a fast reconnect under the
     * same id — therefore leaves the new binding, its index and its channels
     * alone, including a channel both objects held.
     *
     * **One teardown per object, ever** (#393). A second call for an object
     * already retiring — `evict`'s id form racing the transport's own close
     * event with the object form, both entered while the object still owned
     * the id — joins the first call's promise instead of computing its own
     * copy of the reverse index and running a second, independent loop. Two
     * independent loops each believed the id still theirs to finish, so a
     * copy that reached the last of its channels only after the OTHER copy had
     * already forgotten the id (making it free for a new registration) tore
     * down whatever a fresh registration had since built there — a real
     * teardown, not a no-op, because that copy had captured the channel before
     * the other one ever touched it. Joining removes the second copy
     * entirely: the id stays bound to the one retiring object until its own,
     * single teardown ends.
     *
     * One channel's failure never aborts the rest: the first failure is
     * re-thrown once the loop above ends — whether it tried every channel or
     * stopped early because the object lost the id (#370), whichever failure
     * that already accumulated wins — later failures are WARNed, and a
     * failure is recorded by a flag, so a rejection carrying `undefined` is
     * re-thrown too. Whether the connection itself is forgotten follows the
     * `finally` rule above: only while the object still owns the id.
     *
     * **The id form is deprecated for application callers** (#392). Passing a
     * bare id — instead of the `Connection` object your close hook received —
     * now raises one `triggerDeprecation` notice
     * (`@lockness/deprecation-contracts`) per `ChannelManager` instance, never
     * a second one for that instance and never one for the object form. This
     * is a visibility change only: the id form still does exactly what it did
     * above, tearing down whoever holds that id when it runs — narrowing the
     * signature, or fixing that hazard itself, is later work the issue
     * defers. `evict`'s own internal id-form call bypasses this method
     * entirely (see {@link #teardown}) and never raises the notice: it is for
     * application code choosing the id form, not for the shape the framework
     * still uses itself.
     *
     * **The teardown is started BEFORE the notice, and its outcome never
     * depends on the notice** (#392 review HIGH). `STRICT_DEPRECATIONS=true`
     * makes `@lockness/deprecation-contracts` throw synchronously
     * (`trigger.ts`'s `triggerWithHandler`), and that mode exists precisely so
     * a deprecated call surfaces loudly in CI — but a caller who then never
     * reaches {@link #teardown} at all would leave the connection registered,
     * still owning its channels, with nothing torn down: strict mode would
     * have turned a visibility notice into a functional regression. So
     * `#teardown(target)` is called FIRST — which keeps #393's synchronous,
     * same-turn retirement write exactly where it always was, since nothing
     * here runs before it — and the notice is raised after, wrapped in a
     * `try`. When it throws, this call's own promise rejects with that
     * deprecation error once the teardown settles, whichever way it settles:
     * the deprecation error wins even over a teardown failure. The
     * alternative — the teardown's failure winning when both occur — would
     * make `STRICT_DEPRECATIONS` unreliable for the CI use it exists for: an
     * unrelated, co-occurring teardown fault would silently swallow the one
     * signal strict mode promises to never miss. A rejection is used rather
     * than a synchronous throw so this method's contract stays uniform
     * (a `Promise` always, never a throw before one exists) and so the
     * teardown's own promise is never left with only a rejection reaction —
     * both of `.then`'s callbacks are supplied, and both discard the
     * teardown's settlement in favour of the deprecation error, so neither
     * settlement goes unhandled.
     *
     * @param target - The registered connection object (from a close hook),
     *   or a connection id (the form `evict` uses — deprecated for
     *   application callers, #392).
     * @returns `'disconnected'` when this instance owned the socket and tore it
     *   down, `'not-owned'` when the socket lives elsewhere, or when the object
     *   passed is not the one that owns its id — nothing local was touched.
     * @throws Whatever the first channel teardown threw, when the id-form
     *   notice did not also throw — unchanged; the connection is still
     *   forgotten, and the outcome is not reported in that case because the
     *   throw is the report. A second, joining call throws the same rejection
     *   the first call did.
     * @throws The id-form deprecation error, when `STRICT_DEPRECATIONS=true`
     *   and `target` is a string (#392) — in preference to a teardown failure,
     *   which is WARNed instead (with {@link DISCONNECT_TEARDOWN_LOG_FAILED}
     *   as the fallback when the WARN sink itself throws); the teardown itself
     *   still ran to completion first.
     * @example
     * ```ts
     * const hooks: WebSocketHooks<Identity> = {
     *     onOpen: (conn: Connection<Identity>) => manager.register(conn),
     *     // The object you registered, not `conn.id` — passing the id here
     *     // still works, but now logs a one-time deprecation notice (#392).
     *     onClose: (conn: Connection<Identity>) => manager.disconnect(conn),
     * }
     * ```
     */
    disconnect(
        target: string | Connection<Identity>,
    ): Promise<DisconnectOutcome> {
        // STARTED FIRST (#392 review HIGH): #teardown's own synchronous
        // prefix — the #393 retirement write included — must run whatever the
        // notice below does. Nothing above this line can throw.
        const teardown = this.#teardown(target)
        if (typeof target === 'string') {
            try {
                this.#warnIdForm()
            } catch (deprecationError) {
                // Both reactions re-throw the SAME error: it wins even over a
                // teardown failure (see the JSDoc above for why), and
                // supplying both means `teardown` never carries an
                // unhandled rejection either way.
                return teardown.then(
                    () => {
                        throw deprecationError
                    },
                    (teardownError: unknown) => {
                        // The deprecation error wins the rejection, but the
                        // teardown's own failure is never dropped silently:
                        // it is WARNed, and a throwing sink falls back to the
                        // marked line, which never throws (#391).
                        try {
                            console.warn(
                                'realtime: a disconnect teardown failed while ' +
                                    'its id-form deprecation notice also ' +
                                    `threw (#392): ${
                                        renderError(teardownError)
                                    }`,
                            )
                        } catch (sink) {
                            writeMarkedFallback(
                                DISCONNECT_TEARDOWN_LOG_FAILED,
                                teardownError,
                                { label: 'sink failure', error: sink },
                            )
                        }
                        throw deprecationError
                    },
                )
            }
        }
        return teardown
    }

    /**
     * Fires {@link disconnect}'s id-form deprecation notice (#392) — at most
     * ONCE per manager instance, never per call. A line per socket close under
     * connection churn would be worse than one line ever, so `#idFormWarned`
     * is checked and set here, and nowhere else. {@link disconnect} is the
     * only caller; `revokeLocal`'s own id-form call reaches {@link #teardown}
     * directly and never this method, which is what keeps the framework's own
     * use of the shape silent.
     *
     * **Called AFTER `disconnect` has already started the teardown** (#392
     * review HIGH) — this method's own possible throw must never be able to
     * pre-empt it; see `disconnect`'s JSDoc for why and how the two settle
     * together.
     *
     * @returns void
     * @throws Whatever `triggerDeprecation` throws when
     *   `STRICT_DEPRECATIONS=true` — a plain `Error`, unmodified.
     */
    #warnIdForm(): void {
        if (this.#idFormWarned) return
        this.#idFormWarned = true
        triggerDeprecation(
            '@lockness/realtime',
            '0.4.0',
            'disconnect() called with a connection id is deprecated — pass ' +
                'the Connection object your close hook received instead',
        )
    }

    /**
     * `disconnect`'s engine, and `revokeLocal`'s own id-form entry point
     * (#392) — split out of the public method's body, unchanged, so the
     * public wrapper can raise its deprecation notice without this, the part
     * that actually retires and tears down, ever running twice for one call.
     *
     * **The object form acts only for the owner** (#363), asked before
     * anything is retired, copied or awaited: a socket that does not own its
     * id must not tear down the one that does.
     *
     * **It retires the connection object first** (#361), in the synchronous
     * turn that copies the connection's channels: from then on `register` and
     * `subscribe` refuse that object with {@link ConnectionDisconnectedError},
     * and a different object under its id with {@link ConnectionIdInUseError}
     * while the teardown runs. A join that committed before this call is torn
     * down with the rest; one resolving after it is refused before it writes.
     * The connection stays in `connections` — still owned — until the teardown
     * ends. An id this instance does not own retires nothing.
     *
     * **One teardown per object, ever** (#393). A second call for an object
     * already retiring — `evict`'s id form racing the transport's own close
     * event with the object form, both entered while the object still owned
     * the id — joins the first call's promise instead of computing its own
     * copy of the reverse index and running a second, independent loop: the id
     * stays bound to the one retiring object until its own, single teardown
     * ends.
     *
     * @param target - The registered connection object (from a close hook,
     *   or `disconnect`'s object form), or a connection id (`disconnect`'s id
     *   form, and `revokeLocal`'s own direct call).
     * @returns `'disconnected'` when this instance owned the socket and tore it
     *   down, `'not-owned'` when the socket lives elsewhere, or when the object
     *   passed is not the one that owns its id — nothing local was touched.
     * @throws Whatever the first channel teardown threw — unchanged; the
     *   connection is still forgotten, and the outcome is not reported in that
     *   case because the throw is the report. A second, joining call throws
     *   the same rejection the first call did.
     */
    #teardown(
        target: string | Connection<Identity>,
    ): Promise<DisconnectOutcome> {
        // THE OBJECT FORM ACTS ONLY FOR THE OWNER (#363), asked before
        // anything is retired, copied or awaited: a socket that does not own
        // its id must not tear down the one that does.
        if (typeof target !== 'string' && !this.#isOwner(target)) {
            return Promise.resolve('not-owned')
        }
        const clientId = typeof target === 'string' ? target : target.id
        // READ BEFORE ANY AWAIT, exactly as #361 always did: this is the one
        // read every later decision in this call (and #teardownChannels's) is
        // taken from.
        const bound = this.connections.get(clientId)
        // JOIN, RATHER THAN RUN (#393): an object already retiring has a
        // teardown in flight, keyed by the object itself — never by
        // `clientId`, which a later registration would reuse for someone
        // else entirely. No new snapshot, no new loop; this call's caller
        // awaits (or throws with) exactly what the first caller does.
        if (bound !== undefined) {
            const joining = this.#retired.get(bound)
            if (joining !== undefined) return joining
        }
        const teardown = this.#teardownChannels(clientId, bound)
        // RETIRED, synchronously, in the same turn that read `bound` above
        // (the #361 rule, unchanged): `#teardownChannels` already ran
        // synchronously up to its own first await, so nothing has run between
        // that read and this write that could have observed `bound` retiring
        // with no entry here — a second, same-turn call for this very object
        // still finds it and joins.
        if (bound !== undefined) this.#retired.set(bound, teardown)
        return teardown
    }

    /**
     * The reverse-index loop and forgetting `finally` for one teardown call,
     * extracted so a second teardown of an already-retiring object can await
     * this very run instead of executing a copy of its own (#393) —
     * {@link #teardown} is the only caller, and the only writer of
     * {@link #retired}.
     *
     * @param clientId - The id `#teardown` was called with.
     * @param bound - The object `#teardown` read `connections` as holding
     *   `clientId`, or `undefined` when this instance did not own it.
     * @returns `'disconnected'` when `bound` was owned and torn down here,
     *   `'not-owned'` otherwise.
     * @throws Whatever the first channel's teardown threw — unchanged.
     */
    async #teardownChannels(
        clientId: string,
        bound: Connection<Identity> | undefined,
    ): Promise<DisconnectOutcome> {
        const owned = bound !== undefined
        // THIS CONNECTION'S channels, not every channel this instance has ever
        // hosted. The old loop walked `subscriptions.keys()` and called
        // `unsubscribe` for all of them, which was harmless only because a
        // non-member delete is a no-op — and stopped being harmless the moment
        // a 1→0 transition acquired a wire op. It was also O(channels under the
        // prefix) per disconnect.
        //
        // A failure is recorded by a FLAG beside its value (#361): a
        // rejection may carry `undefined`, and testing the value lost it.
        let failed = false
        let failure: unknown
        try {
            for (
                const channel of [...this.#channelsByClient.get(clientId) ?? []]
            ) {
                // STOP ONCE THE OBJECT NO LONGER OWNS THE ID (#363). Every
                // leave below is keyed by id, so after an await in which a
                // racing teardown forgot this object and a new one registered
                // under the id, the rest of this copy would strip the NEW
                // owner of any channel both held. The remaining leaves are
                // skipped: the teardown that forgot this object already left
                // them.
                if (bound !== undefined && !this.#isOwner(bound)) break
                // ONE CHANNEL'S TEARDOWN CANNOT ABORT THE REST.
                //
                // `unsubscribe` awaits two rejectable calls — the driver's
                // unwatch and the roster release (a failed control publish is
                // a WARN since #344) — and a single transient fault used to
                // throw straight out of this loop, leaving every later channel
                // unwatched AND the two
                // deletes below unreached. The connection then sat in
                // `connections`, in the reverse index and in `subscriptions`
                // for the life of the process, permanently charged against its
                // own cap.
                //
                // The JOIN path already contains driver faults deliberately
                // (`#watch`); the leave path did not, and a leave is exactly
                // where giving up is least affordable.
                try {
                    await this.unsubscribe(clientId, channel)
                } catch (error) {
                    // COLLECTED, NOT SWALLOWED. The first failure is re-thrown
                    // below so `disconnect`'s contract is unchanged — the evict
                    // path's own WARN is what reports it, and swallowing here
                    // made that line unreachable. Only the SUBSEQUENT ones are
                    // logged here, because they are the ones no caller will
                    // ever see.
                    if (!failed) {
                        failed = true
                        failure = error
                    } else {
                        try {
                            console.warn(
                                `realtime: tearing ${
                                    safeForLog(clientId)
                                } out of ` +
                                    `${safeForLog(channel)} also failed: ` +
                                    renderError(error),
                            )
                        } catch (sink) {
                            // #395 (security review LOW): a throwing sink
                            // must not abort this loop — a later channel's
                            // roster release, `left` announcement and cap
                            // release must still run. One marked line
                            // instead, which never throws (#391).
                            writeMarkedFallback(
                                TEARDOWN_CHANNEL_LOG_FAILED,
                                error,
                                { label: 'sink failure', error: sink },
                            )
                        }
                    }
                }
            }
        } finally {
            // IN A `finally`: forgetting the connection is the one part of a
            // disconnect that must happen whatever else did not — and ONLY
            // while the object this call tore down still owns the id (#363). A
            // different object registered under it while the loop ran (an
            // evict, then a fast reconnect) keeps its binding and its index.
            if (bound !== undefined && this.#isOwner(bound)) {
                this.#channelsByClient.delete(clientId)
                this.connections.delete(clientId)
            }
        }
        // AFTER the teardown completed and the connection was forgotten. The
        // caller still learns the disconnect was not clean; what it no longer
        // does is decide how much of the teardown ran.
        if (failed) throw failure
        return owned ? 'disconnected' : 'not-owned'
    }

    /**
     * Server-only eviction of a connection (FR-009, S7). Revokes the connection
     * wherever its socket lives: the durable marker is recorded first (FR-014,
     * so a lost control frame is recovered on reconnect/reconcile), then — if
     * this instance owns the socket — it is revoked locally; otherwise an
     * authenticated `evict` control message is published so the owning instance
     * revokes it. Per Q2 a revocation-driven evict **hard-closes** the socket,
     * unlike a plain channel leave ({@link unsubscribe}).
     *
     * This is NOT reachable from a client frame — `decodeClientMessage`'s
     * allowlist is unchanged (deny-by-default). It is called by server code (an
     * admin action, a revoked-token hook).
     *
     * @param clientId - The connection id to evict.
     * @returns Resolves once the durable marker is set and the revocation has
     *   been applied locally or published to the owning instance.
     * @throws {ConnectionIdError} If `clientId` is outside the supported
     *   charset. Without this the call would publish a control frame that every
     *   receiving instance drops on ingest, and return successfully having
     *   revoked nothing.
     * @throws Whatever the durable revocation write rejected with, after the
     *   revocation itself was applied or published — recorded by a flag, so a
     *   rejection carrying `undefined` is re-thrown too (#361). A local evict
     *   retires the connection through `revokeLocal`'s direct `#teardown` call
     *   (#392), so a later `subscribe` with that object is refused.
     * @example
     * ```ts
     * // A revoked token: kick the connection off every instance.
     * // Throws ConnectionIdError if the id is not one this framework minted.
     * await manager.evict(connectionId)
     * ```
     */
    async evict(clientId: string): Promise<void> {
        // The third boundary. `evict` takes an arbitrary string from the
        // application, and an id outside the charset would publish a control
        // frame that every receiving instance drops on ingest — a revocation
        // that reports success and does nothing. Loud here, for the same reason
        // it is loud at registration.
        this.#assertUsableId(clientId)
        // Durable first (S1/FR-014): even if the control frame is lost, the
        // owning instance recovers the evict on its next reconcile.
        //
        // A failure here must NOT cancel the eviction. The local hard-close
        // needs no Redis at all, so letting a durability write reject out of
        // this method would skip the one revocation that was still possible —
        // failing open on the framework's only revocation path. The error is
        // re-thrown after the revocation has been applied, so the caller still
        // learns that durability was lost and this evict will not survive a
        // reconcile (#276 review HIGH-2). Recorded by a FLAG (#361): a
        // rejection may carry `undefined`.
        let durabilityFailed = false
        let durabilityError: unknown
        try {
            await this.#revocations?.markRevocation({ target: clientId })
        } catch (error) {
            durabilityFailed = true
            durabilityError = error
            // Rendered, not passed as a separate console argument. The old
            // comment here reasoned that not interpolating meant no encoder was
            // needed — which treats the hazard as log INJECTION when it is
            // DISCLOSURE. `console.warn(msg, error)` prints the error's message
            // AND its stack, so the object form leaks strictly more than the
            // interpolation it was preferred over: measured, a DSN-bearing
            // failure reached the sink in cleartext with its stack. Teardown is
            // exactly where credential-bearing errors are produced.
            try {
                console.warn(
                    'realtime: the durable revocation write failed — revoking ' +
                        'anyway, but a lost control frame will NOT be recovered ' +
                        `by reconcile: ${renderError(error)}`,
                )
            } catch (sink) {
                // #395 part 2: a throwing sink must not abort `evict` here —
                // the durable write already failed, and skipping the apply
                // below too would leave the connection revoked NOWHERE, local
                // or remote. One marked line instead, which never throws
                // (#391).
                writeMarkedFallback(EVICT_DURABILITY_LOG_FAILED, error, {
                    label: 'sink failure',
                    error: sink,
                })
            }
        }
        // The revocation itself. Its failure is the more serious of the two, so
        // it propagates in preference to the durability error — never from a
        // `finally`, which would mask it.
        if (this.connections.has(clientId)) {
            await this.revokeLocal(clientId)
        } else {
            // The socket lives on another instance — reach it over the control
            // plane.
            await this.publishControl({ kind: 'evict', target: clientId })
        }
        if (durabilityFailed) throw durabilityError
    }

    /**
     * Revoke a connection this instance owns: hard-close its socket (Q2 — a
     * revocation-driven evict, not a plain leave) then disconnect it from every
     * channel, which releases its holds in the authoritative roster and
     * announces `left` on every instance for each slot a release empties. A
     * failure to tear down is logged at WARN, never
     * swallowed — the socket is closed regardless.
     *
     * **The hard-close is outside the `try`, on purpose** (#384). A
     * `Connection.close` that throws leaves the socket open AND still owned,
     * so the durable record reaches it again on every pass: that failure must
     * reach the caller, where the revocation re-check counts it, rather than
     * become a WARN that reads like the contained teardown failure below.
     *
     * **Calls {@link #teardown} directly, never the public `disconnect`**
     * (#392). This is the framework's own id-form caller — the shape
     * `disconnect`'s id form now warns application code about — so it must
     * stay silent: going through the public wrapper here would raise a
     * deprecation notice for the framework's own use of its own primitive.
     *
     * @param clientId - The owned connection id to revoke.
     * @returns `true` once the teardown fulfilled — `'disconnected'`, or
     *   `'not-owned'` when nothing is left to tear down here — and `false`
     *   when the teardown threw, after its WARN (#384). `evict` and the
     *   control-frame path drop it; the revocation re-check counts it.
     * @throws Whatever the connection's `close` threw.
     */
    private async revokeLocal(clientId: string): Promise<boolean> {
        // Hard-close first so delivery stops immediately, even before the async
        // roster teardown settles (Q2 — safe even if `authorize()` lags).
        this.connections.get(clientId)?.close(4403, 'evicted')
        try {
            await this.#teardown(clientId)
        } catch (error) {
            console.warn(
                `realtime: evict teardown for ${safeForLog(clientId)} failed ` +
                    `after hard-close: ${renderError(error)}`,
            )
            return false
        }
        return true
    }

    /**
     * Whether this driver has a control plane — the ONE spelling, so
     * {@link revokeChannel}'s two questions ("may I proceed without a durable
     * store?" and "is there anyone to route to?") ask the same thing.
     */
    get #hasControlPlane(): boolean {
        return typeof this.driver.publishControl === 'function'
    }

    /**
     * Revoke a connection from **one channel**, wherever its socket lives
     * (#332) — the channel-scoped sibling of {@link evict}.
     *
     * The connection leaves that channel and keeps every other it holds; **the
     * socket stays open.** That is the difference from `evict`, which
     * hard-closes with 4403 and takes every still-authorized channel with it —
     * and, for a client with no reconnect logic, ends the realtime session.
     *
     * Same durable path as `evict`, not a second weaker one: the record is
     * written first, then either applied here or published to the owner, and
     * the reconcile pass recovers a frame the bus lost. It is cleared once
     * applied, so a record means exactly *a revocation the owner has not
     * applied yet* — an uncleared one would re-kick a client that legitimately
     * re-subscribed, once per reconcile tick, for the whole record TTL.
     *
     * **A revocation is not a ban.** The connection may re-subscribe
     * immediately if the application's `authorize` admits it; this framework
     * owns no deny list, and `subscribe` does not consult the revocation index.
     *
     * Not reachable from a client frame — `decodeClientMessage`'s allowlist is
     * unchanged. Server code calls this.
     *
     * @param clientId - The connection id to revoke.
     * @param channel - The channel to revoke it from.
     * @returns `'revoked'` when the membership was removed here,
     *   `'not-subscribed'` when this instance owns the socket and it was not in
     *   that channel, `'not-owned'` when the socket lives elsewhere (the record
     *   is written and the frame published) or when there is no owner to route
     *   to at all.
     * @throws {ConnectionIdError} If `clientId` is outside the supported
     *   charset.
     * @throws {ChannelNameError} If `channel` is outside it. **Both** are
     *   asserted, unlike {@link unsubscribe}: these two values are minted onto
     *   the control plane and into a durable record, where an unusable name
     *   means a frame every peer drops and a revocation that reported success
     *   having revoked nothing.
     * @throws {RevocationScopeError} If the driver can route the frame but
     *   cannot record it durably.
     * @example
     * ```ts
     * // Remove one member from one room, everywhere. Their other rooms and
     * // their socket are untouched.
     * await manager.revokeChannel(connectionId, 'private-orders')
     * ```
     */
    async revokeChannel(
        clientId: string,
        channel: string,
    ): Promise<RevokeChannelOutcome> {
        this.#assertUsableId(clientId)
        this.#assertUsableChannel(channel)
        const store = this.#revocations
        // A driver that can ROUTE but cannot RECORD is the undurable path: the
        // revoke would rest on one control frame arriving, and a lost or
        // MAC-refused frame is a revocation that reported success and did
        // nothing. Refuse before anything is published. A single-process driver
        // (no control plane, no store) is a different case and is allowed —
        // there is no bus on which to lose a frame.
        if (!store && this.#hasControlPlane) {
            throw new RevocationScopeError(channel)
        }
        // ONE CALL, ONE RECORD (#337). The id is minted here rather than by the
        // driver so a failed mark still publishes a frame the owner can clear
        // by, and so uniqueness is promised in one place instead of by every
        // driver. Without it, a clear for an earlier revocation of this pair
        // erases this one's record in flight — and if this frame is lost,
        // nothing enforces it.
        const revocation: ChannelRevocation = {
            target: clientId,
            channel,
            id: crypto.randomUUID(),
        }
        // Durable first, and a failure NEVER cancels the revocation — the same
        // sequencing `evict` records at length: the local apply needs no broker
        // at all, so letting a durability write reject out of this method would
        // skip the one revocation still possible. Re-thrown after the apply, so
        // the caller learns durability was lost. Recorded by a FLAG (#361):
        // a rejection may carry `undefined`.
        let durabilityFailed = false
        let durabilityError: unknown
        try {
            await store?.markRevocation(revocation)
        } catch (error) {
            durabilityFailed = true
            durabilityError = error
            try {
                console.warn(
                    `realtime: the durable revocation write for ${
                        safeForLog(channel)
                    } failed — revoking anyway, but a lost control frame ` +
                        `will NOT be recovered by reconcile: ${
                            renderError(error)
                        }`,
                )
            } catch (sink) {
                // #395 part 2: same hazard as `evict`'s durability WARN — a
                // throwing sink must not skip the local apply or the
                // control-frame publish that follow. One marked line instead,
                // which never throws (#391).
                writeMarkedFallback(
                    REVOKE_CHANNEL_DURABILITY_LOG_FAILED,
                    error,
                    { label: 'sink failure', error: sink },
                )
            }
        }
        let outcome: RevokeChannelOutcome = 'not-owned'
        if (this.connections.has(clientId)) {
            const applied = await this.#revokeChannelLocal({
                target: clientId,
                channel,
                ids: [revocation.id],
            })
            outcome = applied.outcome
            // Only HERE is a clear failure re-thrown: this is the one apply
            // path with a caller to receive it. The control-frame and reconcile
            // paths have none, so re-throwing there would be an unhandled
            // rejection rather than a signal (FR-019).
            if (!durabilityFailed && applied.clearFailed) {
                durabilityFailed = true
                durabilityError = applied.clearError
            }
        } else if (this.#hasControlPlane) {
            await this.publishControl({
                kind: 'revoke-channel',
                target: clientId,
                channel,
                // The owner clears exactly this record, never the pair.
                revocationId: revocation.id,
            })
        }
        if (durabilityFailed) throw durabilityError
        return outcome
    }

    /**
     * Apply a channel-scoped revocation to a socket this instance owns.
     *
     * **Reuses the whole existing leave path** — the roster release, and the
     * `left` / `presence-leave` its queued write announces when it empties the
     * slot (#344) — rather than growing a second announcement mechanism beside
     * it.
     *
     * @param group - The pair, and the id of every record this one leave
     *   answers for. Only these ids are cleared (#337): a record for the same
     *   pair written after the caller read it has an id nobody here has seen,
     *   so no clear can reach it.
     * @returns The outcome, and the first error from clearing a durable record
     *   — returned rather than thrown so each caller decides, since only one of
     *   the callers has anyone to tell — with `clearFailed` beside it, because
     *   a rejection may carry `undefined` (#361).
     */
    async #revokeChannelLocal(
        group: ChannelRevocationGroup,
    ): Promise<
        {
            outcome: RevokeChannelOutcome
            clearFailed: boolean
            clearError: unknown
        }
    > {
        const { target, channel } = group
        const left = await this.unsubscribe(target, channel)
        if (left === 'left') {
            // WITHOUT THIS THE TARGET NEVER LEARNS. Any `left` fans only to the
            // channel's remaining subscribers, and the leaver was removed from
            // that set before it ran — so it does not even receive its own
            // departure, and no `left` is sent at all while another of the
            // member's connections holds the slot (#344). `evict`'s target at
            // least gets close code 4403; this one would get silence.
            //
            // A CLIENT-initiated unsubscribe still sends nothing: the
            // application owns that reply, and this is gated on a leave the
            // server asked for.
            this.#tell(target, { type: 'unsubscribed', channel })
        }
        // CLEARED ONLY WHEN SOMETHING WAS ACTUALLY REMOVED.
        //
        // `subscribe` suspends at the application authorizer before
        // `#joinLocal` runs, so there is a real window in which this instance
        // owns the socket and the membership has not landed. A revoke inside
        // it marks the record, gets `'not-subscribed'` here, and — clearing
        // unconditionally — would delete the record it wrote moments earlier.
        // The authorizer then resolves, the membership lands, and the
        // reconcile has nothing left to find: the connection stays in the room
        // permanently, and the caller was told the revoke was a no-op.
        //
        // A record that found nothing therefore survives to its TTL, which is
        // exactly what `evict` has always done. Re-applying it costs one
        // no-op leave per reconcile tick and sends the client nothing, because
        // the frame below is gated on the same predicate.
        //
        // EVERY id in the group, each by its own exact id (#337). One leave
        // settled all of them; a clear that named the pair instead would also
        // take a record written after this group was read.
        //
        // The first failure is recorded by a FLAG (#361), carried across the
        // return: a rejection may carry `undefined`.
        let clearFailed = false
        let clearError: unknown
        if (left === 'left') {
            for (const id of group.ids) {
                const cleared = await this.#clearRevocation({
                    target,
                    channel,
                    id,
                })
                if (!clearFailed && cleared.failed) {
                    clearFailed = true
                    clearError = cleared.error
                }
            }
        }
        return {
            outcome: left === 'left'
                ? 'revoked'
                : left === 'not-subscribed'
                ? 'not-subscribed'
                : 'not-owned',
            clearFailed,
            clearError,
        }
    }

    /**
     * Forget exactly one channel revocation this instance has applied.
     *
     * @param revocation - The applied revocation, by its exact id.
     * @returns Whether it failed, and the failure — never thrown from here,
     *   because one of the callers is a fire-and-forget control-frame dispatch
     *   where a rejection has nowhere to go. A flag beside the value (#361),
     *   because a rejection may carry `undefined`.
     */
    async #clearRevocation(
        revocation: ChannelRevocation,
    ): Promise<{ failed: boolean; error: unknown }> {
        try {
            await this.#revocations?.clearRevocation(revocation)
            return { failed: false, error: undefined }
        } catch (error) {
            console.warn(
                `realtime: the revocation record for ${
                    safeForLog(revocation.channel)
                } was applied but could not be cleared — reconcile will ` +
                    `re-apply it until it expires: ${renderError(error)}`,
            )
            return { failed: true, error }
        }
    }

    /**
     * Send one frame to one connection, through the application's own encoder.
     *
     * @param clientId - The connection to tell.
     * @param frame - The frame to encode and send.
     */
    #tell(clientId: string, frame: OutboundFrame): void {
        const connection = this.connections.get(clientId)
        if (!connection) return
        try {
            connection.send(this.encode(frame))
        } catch (error) {
            console.warn(
                `realtime: could not tell ${safeForLog(clientId)} it was ` +
                    `revoked — the socket is skipped: ${renderError(error)}`,
            )
        }
    }

    /**
     * **What a revocation's scope does to the socket** — the one place that
     * mapping is made.
     *
     * No channel means the whole connection: hard-close 4403 and tear it out of
     * every room. A channel means one room, socket open. The two entry points
     * that have no caller — the `revoke-channel` control frame and the
     * reconcile pass — both **call** this rather than each testing the scope
     * themselves. Two spellings of it, reached by different routes, is how a
     * third scope later gets added to one and not the other.
     *
     * Contained, never re-thrown: a control frame is dispatched fire-and-forget
     * and the reconcile is invoked by the driver's timer, so neither has anyone
     * to receive a rejection (FR-019). The one thing it cannot contain is its
     * own WARN throwing; each caller ends that — the control frame through
     * {@link #dispatchRevocation} (#376), the reconcile inside its own `try`
     * (#349).
     *
     * **Whether the apply completed is decided here and nowhere else**
     * (#384): the connection scope reports {@link revokeLocal}'s answer, and
     * a hard-close that threw reaches the catch; the channel scope completed
     * once its leave resolved, **whatever its clear did** — the revocation
     * was applied, and only the record outlives it. What a failure means to
     * an operator is {@link RevocationTally}'s to say.
     *
     * @param revocation - A whole-connection revocation, or every channel
     *   revocation of one pair, to apply to a socket this instance owns.
     * @returns `true` when the apply completed, `false` from the catch, after
     *   its WARN. {@link #dispatchRevocation} drops it; the revocation
     *   re-check counts it.
     * @throws Only what its own WARN throws.
     */
    async #applyRevocation(
        revocation: ConnectionRevocation | ChannelRevocationGroup,
    ): Promise<boolean> {
        try {
            if (revocation.channel === undefined) {
                // Connection scope. The record is NOT cleared: it becomes moot
                // the instant the socket dies, so it is left to its TTL — which
                // is what `evict` has always done.
                return await this.revokeLocal(revocation.target)
            }
            await this.#revokeChannelLocal(revocation)
            return true
        } catch (error) {
            console.warn(
                `realtime: applying a revocation for ${
                    safeForLog(revocation.target)
                } failed: ${renderError(error)}`,
            )
            return false
        }
    }

    /**
     * Apply a revocation from a control frame, **fire-and-forget and fully
     * contained** (#376) — the only way the control-frame switch reaches
     * {@link #applyRevocation}.
     *
     * The apply contains every failure but its own WARN: a `console.warn` that
     * throws inside its catch (a patched console, a logger transport refusing
     * the line) rejects the apply. The switch has no caller to hand that
     * rejection to, and on Deno an unhandled rejection terminates the process —
     * reachable by any peer that publishes an `evict` or `revoke-channel`
     * frame. So the chain ends here, in the #369 shape: one marked ERROR line
     * ({@link REVOCATION_APPLY_LOG_FAILED}), never a re-throw. The marker is the
     * fixed prefix and the rejection is rendered, so its text can neither
     * forge the marker nor break the line.
     *
     * @param revocation - The revocation the frame names, for a socket this
     *   instance owns.
     */
    #dispatchRevocation(
        revocation: ConnectionRevocation | ChannelRevocationGroup,
    ): void {
        this.#applyRevocation(revocation).catch((error: unknown) =>
            writeMarkedFallback(REVOCATION_APPLY_LOG_FAILED, error)
        )
    }

    /**
     * The durable revocation re-check (S1/FR-014) — **the gate that runs one
     * re-check at a time** (#359 FR-009a, A1), whoever calls it: the driver's
     * pass and the lapse run's re-check (#349) are its two callers.
     *
     * Each call appends one run of {@link #recheckRevocations} to a private
     * serial tail (the ADR 003 slot-tail idiom) and returns that run's
     * promise, so its caller still sees a rejection. A run starts only after
     * the previous one settled, so every run reads the index afresh — a
     * record written while a run waited is applied before anything after it
     * (#349 A2) — and a pair one run left is never re-kicked by another's
     * older snapshot (#337). The tail continues on **both** settle branches,
     * so a rejected run never stops the next. No coalescing: the callers are
     * each one at a time, so the tail is at most two deep.
     *
     * @returns This call's run's {@link RevocationTally}, once it settled —
     *   what the driver's pass handler resolves to (#384).
     * @throws Whatever this call's run throws.
     */
    private reconcileRevocations(): Promise<RevocationTally> {
        const run = this.#revocationTail.then(() => this.#recheckRevocations())
        // The tail must always settle so the next run starts; `run` still
        // rejects to this caller, so nothing is swallowed.
        this.#revocationTail = run.then(() => {}, () => {})
        return run
    }

    /**
     * One durable revocation re-check (S1/FR-014), run by
     * {@link reconcileRevocations}'s tail. Any live revocation whose socket
     * this instance owns is applied here — recovering a revoke whose one-shot
     * control frame was lost while the owning socket was between reconnects.
     *
     * **Nothing is applied before the enumeration ends** (#359): the driver
     * pages the index, and this reads its whole result before grouping — a
     * pair whose records sit on different pages still leaves once.
     *
     * **Scope is dispatched, not decided**: see {@link #applyRevocation}.
     *
     * **Channel records are grouped by pair, and each pair leaves ONCE**
     * (#337). Two `revokeChannel` calls for one pair are two records, and one
     * leave settles both — so on `'left'` every id listed for that pair is
     * cleared. Applied one record at a time, the second finds
     * `'not-subscribed'`, survives, and kicks the client again at the next
     * tick if it has legitimately re-subscribed.
     *
     * **One revocation that throws never stops the ones after it** (#349).
     * {@link #applyRevocation} contains everything but its own WARN; a log
     * sink that refuses that line would otherwise end the pass there, and —
     * the socket still open, the record still live — end every later pass at
     * the same place, starving each revocation listed behind it. Each is
     * applied inside its own `try`, and a throw is one WARN naming no target
     * and no member.
     *
     * **The tally is counted in the `apply` wrapper and nowhere else**
     * (#384): one attempt per call — so a pair is one, and a record the
     * ownership check below drops is never attempted — and one failure per
     * call whose apply reported `false` or threw. What the counts mean is
     * {@link RevocationTally}'s to say.
     *
     * @returns How many applies this run attempted, and how many failed.
     * @throws Whatever the index read throws: there is no tally then.
     */
    async #recheckRevocations(): Promise<RevocationTally> {
        let attempted = 0
        let failed = 0
        const apply = async (
            revocation: ConnectionRevocation | ChannelRevocationGroup,
        ): Promise<void> => {
            attempted++
            try {
                if (!await this.#applyRevocation(revocation)) failed++
            } catch (error) {
                // Counted BEFORE the WARN, as the sweep counts: a sink that
                // throws cannot skip it.
                failed++
                try {
                    console.warn(
                        'realtime: a durable revocation could not be applied — ' +
                            'the reconcile goes on with the next one: ' +
                            renderError(error),
                    )
                } catch (sink) {
                    // #395 (security review HIGH): a throwing sink must not
                    // abort this `apply` — the loop above awaits it one
                    // revocation at a time, so an uncontained throw here would
                    // escape past this closure and skip every revocation
                    // still queued behind the failing one, breaking #349's
                    // "one revocation that throws never stops the ones after
                    // it". One marked line instead, which never throws (#391).
                    writeMarkedFallback(RECHECK_REVOCATION_LOG_FAILED, error, {
                        label: 'sink failure',
                        error: sink,
                    })
                }
            }
        }
        // The driver is ASKED which targets are local, so it can drop foreign
        // records as it pages (#359); the check below still decides.
        const revocations = await this.#revocations?.listRevocations(
            (target) => this.connections.has(target),
        ) ?? []
        // Keyed by pair; the ids list is appended to while the index is read,
        // then handed to the apply as a read-only `ChannelRevocationGroup`.
        const groups = new Map<
            string,
            ChannelRevocationGroup & { ids: string[] }
        >()
        for (const revocation of revocations) {
            if (!this.connections.has(revocation.target)) continue
            if (revocation.channel === undefined) {
                await apply(revocation)
                continue
            }
            // A JSON pair, so no delimiter a third-party driver's names could
            // contain can fuse two different pairs into one group.
            const key = JSON.stringify([revocation.target, revocation.channel])
            const group = groups.get(key)
            if (group) {
                group.ids.push(revocation.id)
            } else {
                groups.set(key, {
                    target: revocation.target,
                    channel: revocation.channel,
                    ids: [revocation.id],
                })
            }
        }
        for (const group of groups.values()) {
            await apply(group)
        }
        return { attempted, failed }
    }

    /**
     * Broadcast an event to a channel (fanned to authorized subscribers on
     * every instance via the driver).
     *
     * @param channel - The channel name.
     * @param event - The event name.
     * @param data - The payload.
     */
    // DELIBERATELY NOT CHANNEL-ASSERTED (#314). A broadcast reaches only a
    // PUBLISH, which is a literal context, and `deliverLocal` re-keys on the
    // channel recovered from the delivered topic. With `subscribe` guarded
    // nothing can be subscribed to an unusable name, so a broadcast to one
    // fans out to an empty set — inert, not incorrect. Guarding it would add a
    // throw on a path that cannot produce the failure this issue is about,
    // while breaking a caller mid-flight during an upgrade.
    broadcast(channel: string, event: string, data: unknown): void {
        try {
            const result = this.driver.publish({ channel, event, data })
            if (result instanceof Promise) result.catch(this.onPublishError)
        } catch (error) {
            this.onPublishError(error)
        }
    }

    /**
     * Send an event directly to one connection — satisfies
     * `@lockness/notification`'s `BroadcasterLike`. Per-client, never fan-to-all.
     *
     * @param clientId - The target connection id.
     * @param event - The event name.
     * @param data - The payload.
     * @returns Whether a live connection received it.
     */
    send(clientId: string, event: string, data: unknown): boolean {
        const connection = this.connections.get(clientId)
        if (!connection) return false
        connection.send(this.encode({ type: 'event', event, data }))
        return true
    }

    /** Deliver a received message to this instance's authorized subscribers. */
    private deliverLocal(message: BroadcastMessage): void {
        const set = this.subscriptions.get(message.channel)
        if (!set) return
        const frame = this.encode({
            type: 'event',
            channel: message.channel,
            event: message.event,
            data: message.data,
        })
        for (const clientId of set) {
            this.connections.get(clientId)?.send(frame)
        }
    }

    /**
     * Emit a presence frame to this instance's LOCAL presence subscribers (A5).
     *
     * Fans to `subscriptions.get(channel)` — the connections THIS instance holds
     * — never to the driver roster, which now holds remote members this instance
     * cannot reach. Cross-instance presence is carried by the control plane
     * ({@link handleControl}), not by iterating a roster of unreachable sockets.
     *
     * **No connection ever hears presence about its own member id** (#323,
     * #344; for `left` too since #349, the maintainer's decision of
     * 2026-09-23). The single home of that rule: it skips every local
     * subscriber whose presence entry on `channel` has the frame's member id,
     * read from `presence` at emit time — so a connection that claimed the
     * member while the arrival was queued, or that claimed it here while
     * another instance announced it, is excluded too, and so are a lapsed
     * instance's own tabs when a peer's sweep announces them gone. Excluding
     * only the origin connection would send a second tab a frame about
     * itself. In a consistent roster a `left` excludes nobody: it is
     * announced only when no process holds the slot, and a live local
     * connection of that member means this process holds it. No caller can
     * opt out, and none has to remember it.
     *
     * **One unusable socket is skipped, never fatal** (FR-009). `send` returns
     * `void` and a closing `WebSocket.send` raises, so an uncaught throw here
     * aborted the fan-out mid-iteration: every subscriber the loop had not
     * reached was silently skipped, in an order nothing defines, and the throw
     * escaped over a join that had already committed. Not a silent catch — it
     * warns, and it deliberately carries **nothing derived from the member**:
     * `info` is arbitrary application PII, and log stores are read more widely
     * than the data they describe.
     *
     * @param channel - The channel whose local subscribers receive the frame.
     * @param frame - The `joined` / `left` frame to encode and send.
     * @throws Whatever `encode` throws, before any socket is written.
     */
    private emitPresence(
        channel: string,
        frame: PresenceTransitionFrame,
    ): void {
        const set = this.subscriptions.get(channel)
        if (!set) return
        const encoded = this.encode(frame)
        const members = this.presence.get(channel)
        const self = String(frame.member.id)
        for (const clientId of set) {
            const entry = members?.get(clientId)
            if (entry && sameMemberId(entry.id, self)) continue
            const connection = this.connections.get(clientId)
            if (!connection) continue
            try {
                connection.send(encoded)
            } catch (error) {
                try {
                    console.warn(
                        `realtime: a presence frame could not be delivered on ${
                            safeForLog(channel)
                        } — the socket is skipped and the fan-out continues: ${
                            renderError(error)
                        }`,
                    )
                } catch (sink) {
                    // #395 (security review LOW): a throwing sink must not
                    // abort this fan-out — the remaining local sockets must
                    // still receive the frame. One marked line instead, which
                    // never throws (#391).
                    writeMarkedFallback(EMIT_PRESENCE_LOG_FAILED, error, {
                        label: 'sink failure',
                        error: sink,
                    })
                }
            }
        }
    }

    /**
     * Act on a control message received off the bus (already authenticated and
     * name-validated by the driver, A2/FR-015/FR-016). Dispatched by kind — a
     * control frame drives a roster/eviction consequence, never event fan-out:
     *
     * - `presence-join` / `presence-leave`: emit the `joined` / `left` frame to
     *   THIS instance's local presence subscribers, so a member arriving in or
     *   departing from the roster on another instance is seen here (US2). The
     *   sender published it only on `arrived` / `gone` (#344), so this re-emit
     *   is already per member; both skip local connections of that member id
     *   ({@link emitPresence}, #349).
     * - `evict`: the owning instance revokes the target socket (hard-close +
     *   roster release, Q2); an instance that does not own it is a no-op here —
     *   the owning instance's teardown fans any `left` to it via
     *   `presence-leave` (FR-009). The durable marker (FR-014) is the backstop
     *   for a lost frame.
     * - `revoke-channel`: the owning instance removes the target from ONE
     *   channel and leaves the socket open (#332). Same ownership rule as
     *   `evict`; the durable record is the same backstop.
     *
     * **The switch has no `default`, and that is load-bearing.** An instance
     * running an older release meets a kind added after it here, matches
     * nothing, and returns — inert rather than wrong. (`revoke-channel` itself
     * no longer reaches a `0.3.0` peer's switch: its `revocationId` field is
     * MAC-covered, so that peer drops the frame at ingest with a WARN, #337.)
     * Adding a `default` that threw or warned would turn a forward-compatible
     * frame into noise on every peer during a rolling deploy.
     */
    private handleControl(control: ControlMessage): void {
        switch (control.kind) {
            case 'presence-join':
                if (control.channel && control.member) {
                    // A connection here that claimed this member id never
                    // receives a frame about itself: `emitPresence` excludes
                    // it, for this arm and the next (#344, #349).
                    this.emitPresence(control.channel, {
                        type: 'presence',
                        channel: control.channel,
                        action: 'joined',
                        member: control.member,
                    })
                }
                return
            case 'presence-leave':
                if (control.channel && control.member) {
                    this.emitPresence(control.channel, {
                        type: 'presence',
                        channel: control.channel,
                        action: 'left',
                        member: control.member,
                    })
                }
                return
            case 'evict':
                // Only the instance that owns the socket revokes it; every other
                // instance leaves it to the owner (which fans any `left` here
                // via a `presence-leave`). The revoke is async; its awaits
                // settle in microtasks, and it logs on failure — never a silent
                // catch. Dispatched through the one contained entry (#376).
                if (this.connections.has(control.target)) {
                    this.#dispatchRevocation({ target: control.target })
                }
                return
            case 'revoke-channel':
                // Same rule as `evict`: only the owner acts. Every other
                // instance hears any resulting `left` as a `presence-leave`.
                // A frame with no channel is not a channel revocation and is
                // dropped rather than widened into a socket kill.
                if (
                    control.channel === undefined ||
                    !this.connections.has(control.target)
                ) {
                    return
                }
                // A frame with no revocation id names no record to clear, so
                // it is dropped too (#337) — the durable record, if written, is
                // recovered by the reconcile, which has the id. Never silently
                // (#340): a driver that strips the field delays every
                // cross-instance revocation to the next tick, and this line is
                // the only place that can say so. Owner-only, so one frame is
                // one WARN rather than one per instance in the fleet.
                if (control.revocationId === undefined) {
                    console.warn(
                        `realtime: a revoke-channel frame for ${
                            safeForLog(control.channel)
                        } carries no revocationId and was ignored — ` +
                            'a stored revocation now waits for the reconcile ' +
                            'tick, and is not enforced at all by a driver ' +
                            'without onRevocationReconcile. The driver must ' +
                            'pass revocationId through unchanged.',
                    )
                    return
                }
                this.#dispatchRevocation({
                    target: control.target,
                    channel: control.channel,
                    ids: [control.revocationId],
                })
                return
        }
    }
}
