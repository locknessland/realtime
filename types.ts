/**
 * @fileoverview Core real-time types — the `Connection` a hook receives, the
 * lifecycle hooks, and the structural view of a Hono `WSContext`.
 *
 * `WSContext` is imported as a **type** from `@lockness/hono/network` (an
 * allowed edge — a real type, no mirror drift). Driver payloads are kept
 * structural + `unknown` at the wire boundary (hard rule #3).
 *
 * @module @lockness/realtime/types
 */

import type { WSContext, WSMessageReceive } from '@lockness/hono/network'

export type { WSContext, WSMessageReceive }

/**
 * Per-deployment configuration for the authenticity of control-plane and
 * presence-identity messages (FR-015, #268).
 *
 * The reserved control topic is isolation-by-**convention** only — Redis pub/sub
 * has no per-topic ACL, so anyone with bus `PUBLISH` could otherwise forge an
 * evict or spoof a presence member. Every control / presence-identity frame
 * therefore carries an HMAC over its payload, keyed by this **shared** secret;
 * a frame whose MAC is absent or fails to verify is dropped on ingest and never
 * obeyed. The secret must be identical on every instance of one deployment (so
 * the MAC is cross-instance-stable) and is redacted from every log line.
 *
 * A value object: it carries the secret, no behaviour.
 *
 * @example
 * ```ts
 * const control: RealtimeControlConfig = { secret: Deno.env.get('REALTIME_SECRET')! }
 * ```
 */
export interface RealtimeControlConfig {
    /**
     * The per-deployment shared secret keying the control/presence authenticity
     * MAC. Identical on every instance; never logged in cleartext.
     */
    readonly secret: string
    /**
     * How long after issue a control frame may still be obeyed, in
     * milliseconds — the anti-replay freshness window (#272). Also how long a
     * frame's nonce is remembered, which is the same number by construction.
     *
     * The default absorbs NTP-synchronised skew with a wide margin while
     * keeping both the replay window and the nonce store small. Widen it only
     * for a fleet whose clocks genuinely drift: a longer window is a longer
     * period during which a captured frame remains replayable against an
     * instance that restarted, and a proportionally larger store.
     *
     * A fleet whose clocks are far enough apart will see control frames dropped
     * as stale — the WARN names the observed delta, so the cause is legible
     * rather than looking like a dead bus.
     *
     * @default 30000
     */
    readonly windowMs?: number
    /**
     * The largest control payload, in bytes, that will be published or
     * accepted.
     *
     * It bounds cost at ingest: a receiving instance rejects an oversized frame
     * before parsing it and before hashing it, so an unauthenticated peer
     * cannot make every instance in the fleet do real work on demand.
     *
     * Raise it only if a legitimate frame is genuinely near the limit — a
     * presence member with an unusually large `info` payload is the realistic
     * case. **Raise it on every instance at once.** The publisher refuses to
     * send above its own limit and each receiver enforces its own, so a fleet
     * running mixed values silently loses the frames that fall between them.
     *
     * @default 8192
     */
    readonly maxPayloadBytes?: number
    /**
     * The most control-frame nonces one instance remembers at a time.
     *
     * The store is bounded because it is fed by the bus, and a frame rate that
     * outruns the freshness window would otherwise grow it without limit. The
     * cap is reached by ordinary load rather than by attack: a fleet whose
     * clients all reconnect at once — a rolling deploy, a load-balancer
     * failover — issues TWO presence control frames per client per channel, a
     * `leave` as the old socket drops and a `join` as the new one lands, so
     * roughly 5 000 clients across two presence channels is ~20 000 frames
     * inside one 30-second window.
     *
     * At the cap, every instance is guaranteed an equal share of it
     * (`maxEntries / instances`), and what is dropped is the oldest nonce
     * belonging to an instance ABOVE its share — so one noisy instance cannot
     * crowd the others out, an instance under its share is never evicted at
     * all, and the surplus above the shares still goes to whoever sent most
     * recently.
     * A frame older than the evicted entry but still inside the window becomes
     * replayable once — the deliberate alternative to refusing new entries,
     * which would fail the control plane closed.
     *
     * Raise it for a large fleet before widening `windowMs`: a bigger store
     * costs memory, a longer window costs replayability.
     *
     * @default 10000
     */
    readonly maxEntries?: number
}

/**
 * A live WebSocket connection handed to the lifecycle hooks.
 *
 * `identity` is the **server-derived** identity resolved at the upgrade (never
 * from a wire frame) and is immutable; `metadata` is free-form and is never
 * treated as identity (security S1).
 *
 * **One object per socket, for the socket's whole life** (#361). A transport
 * that wires its own hooks must present the very object it passed to
 * `ChannelManager.register` on every later call for that socket, and never
 * build a fresh `Connection` per frame. The manager enforces it (#363): while
 * the object it registered holds an id, `register` and `subscribe` refuse a
 * different object under that id with `ConnectionIdInUseError`, before any
 * authorizer runs; a retired object is refused with
 * `ConnectionDisconnectedError`; and an object `register` never bound is
 * refused by `subscribe` with `ConnectionNotRegisteredError` (#370).
 * `buildEvents` creates one object per socket, so `handlerHooks` meets this for
 * you. The other two lifecycle duties — register at open, disconnect at close
 * with the registered object — are `ChannelManager.register`'s and
 * `disconnect`'s to state.
 *
 * @typeParam Identity - The app's identity shape (e.g. a user id or record).
 */
export interface Connection<Identity = unknown> {
    /**
     * A stable per-connection transport id (not an identity).
     *
     * **It must be unguessable and never reused.** The framework's own upgrade
     * path generates `crypto.randomUUID()`, but an application wiring its own
     * transport supplies this itself, and "stable" has been read as an
     * invitation to pass a user id or a session id. It is not. **The server
     * mints it, per socket** — never from client input, and never from a user
     * or session key (#363). An id another socket can guess or share is one it
     * can register first, which locks its owner out; and since a live id is
     * refused, a refusal tells the caller that id is online.
     *
     * The reason is the control plane. `manager.evict(id)` travels between
     * instances as a signed frame naming this id, and an id that is guessable
     * or reused across connections turns a captured frame into a repeatable
     * weapon: it hard-closes whatever socket currently holds that id,
     * unsubscribes every channel, and removes the member from the authoritative
     * roster. With a fresh random id per connection the same frame targets
     * something that no longer exists and does nothing.
     *
     * **Charset: letters, digits and `:` `.` `_` `-`, at most 200 characters.**
     * `crypto.randomUUID()` satisfies it. This is not a new constraint — the
     * control plane has always dropped a frame naming an id outside it — but it
     * was undocumented and enforced in only one of three paths, so an id like
     * `user@example.com` evicted on its own instance and silently nowhere else.
     * `ChannelManager.register`, `subscribe` and `evict` all refuse such an id
     * outright, and everything downstream may assume it **of `Connection.id`**.
     * That scope is deliberate: `PresenceMember.id`, which an application's
     * authorizer returns and which rides the same control frame, is checked
     * only for its type and is not covered here (#306).
     */
    readonly id: string
    /**
     * The server-verified identity, or `null` for an unauthenticated socket.
     *
     * **This is the only charge target a rate meter can use** (#329), and the
     * rule has one home — here. Two properties decide whether a key bounds
     * anything, and clearing one is not enough:
     *
     * - **Rotation.** {@link Connection.id} is minted per socket and by
     *   contract never reused, so a counter keyed on it is reset by every
     *   reconnect. This value survives one.
     * - **Minting.** It survives a reconnect; it is not unforgeable. Under open
     *   self-registration an attacker mints identities at signup cost, so a
     *   per-identity bucket scales with account count and needs a second key
     *   above it.
     *
     * Two consequences for anyone metering on it. `null` here is **not** a
     * fallback key: every unauthenticated socket shares it, so one bucket keyed
     * on the null identity lets one attacker deny service to every other
     * anonymous client. And `Identity` is `unknown`, so an object identity keys
     * a `Map` **by reference** and a meter built on it misses every time and
     * fails **open**, silently. Key on a stable string you derive.
     *
     * The framework itself meters no verb rate — see
     * {@link ChannelManager.handlerHooks} for why, and `docs/realtime.md` for
     * the per-frame costs and a worked example.
     */
    readonly identity: Identity | null
    /** Free-form connection metadata; never an identity source. */
    readonly metadata: Readonly<Record<string, unknown>>
    /**
     * Send a frame to this connection.
     *
     * @param data - The payload (string or binary).
     */
    send(data: string | ArrayBuffer | Uint8Array): void
    /**
     * Close this connection.
     *
     * @param code - An optional close code.
     * @param reason - An optional close reason.
     */
    close(code?: number, reason?: string): void
}

/**
 * The lifecycle hooks a connection drives. Any hook may be async; a throw in
 * `onMessage` is routed to `onError`, never crashing the connection.
 *
 * @typeParam Identity - The app's identity shape.
 */
export interface WebSocketHooks<Identity = unknown> {
    /** Fired once the socket is open. */
    onOpen?(connection: Connection<Identity>): void | Promise<void>
    /** Fired for each inbound frame. */
    onMessage?(
        connection: Connection<Identity>,
        data: WSMessageReceive,
    ): void | Promise<void>
    /** Fired once the socket closes. */
    onClose?(
        connection: Connection<Identity>,
        code: number,
        reason: string,
    ): void | Promise<void>
    /** Fired on a transport error or a throwing hook. */
    onError?(
        connection: Connection<Identity>,
        error: unknown,
    ): void | Promise<void>
}

/**
 * The minimal structural view of a Hono `WSContext` the handler drives — the
 * two methods used. `WSContext` satisfies it.
 */
export interface Socket {
    /** Send a frame. */
    send(data: string | ArrayBuffer | Uint8Array): void
    /** Close the socket. */
    close(code?: number, reason?: string): void
}
