/**
 * @fileoverview The broadcast-driver seam — how a broadcast crosses (or does
 * not cross) process boundaries.
 *
 * A driver takes a published {@link BroadcastMessage} and delivers it back to
 * `onMessage` on **every** instance that subscribed — including the publisher.
 * The manager registers one `onMessage` handler and, on receive, fans the
 * message out to its **locally authorized** subscribers (security S6). Thus
 * local and cross-process delivery share one path.
 *
 * A cross-process driver MAY additionally expose two OPTIONAL capabilities the
 * manager feature-detects (never mandates), so the memory driver and any
 * third-party driver keep single-process behaviour unchanged:
 *
 * - the presence-state ops ({@link PresenceCapableDriver}) that own the
 *   authoritative `here` roster off the instance's in-process map (FR-005);
 * - the {@link ControlMessage} seam `onControl`, a **distinct** path from
 *   `onMessage` (A2/FR-016) — a control frame is not a channel event and must
 *   never route through `deliverLocal`.
 *
 * @module @lockness/realtime/driver
 */

import type { PresenceMember } from './channel.ts'

/**
 * The most self ids one {@link BroadcastDriver.readRoster} call may carry
 * (#341).
 *
 * A shared roster read serves every caller that queued behind it, and each
 * caller's own member is fetched by id so its reply can keep it. Uncapped, one
 * read would ingest one entry per queued caller and the per-read bound would
 * be a function of the burst, not a constant. At 1 000 the ingest of one read
 * stays below `(limit + 1 000) × maxPresenceMemberBytes`, and a Redis script's
 * `unpack` of the ids stays far below Lua's stack limit.
 *
 * **Every driver enforces it, not only the manager.** The seam is exported, so
 * a caller other than `ChannelManager` can reach a driver directly; a driver
 * refuses a list longer than this before issuing any command.
 *
 * @example
 * ```ts
 * import { MAX_ROSTER_READ_SELF_IDS } from '@lockness/realtime'
 *
 * if (selfIds.length > MAX_ROSTER_READ_SELF_IDS) {
 *     throw new Error('split the batch')
 * }
 * ```
 */
export const MAX_ROSTER_READ_SELF_IDS = 1000

/**
 * What one bounded roster read reports (#341): a window onto the room, the
 * room's size, and the members of the callers the read serves.
 *
 * All three describe **one instant**. A `total` counted by a second command
 * could disagree with `members` under a concurrent join or leave, and a reply
 * could then report fewer members than it lists.
 *
 * Invariants a driver guarantees:
 *
 * - `members.length === min(limit, total)`, less any stored entry the driver
 *   could not parse (skipped with a WARN, never returned).
 * - One entry per `String(member.id)`, in the driver's order. The order is the
 *   driver's choice: join order on the memory driver, a random sample on Redis
 *   when the room is larger than `limit`.
 * - `selves` holds, for each requested self id the roster holds at that
 *   instant, its {@link PresenceMember} — accepted only when the entry stored
 *   under that id's own slot carries that same id, so an entry naming anyone
 *   else is never returned as a self — **one per `String(id)`**, however many times
 *   and in whichever type (`7`, `'7'`) the id was requested. An absent id
 *   contributes nothing. Order is not significant.
 * - Only the client-visible {@link PresenceMember} is returned; driver-internal
 *   metadata (the Redis entry's `owner`) never appears in either list.
 *
 * @example
 * ```ts
 * const window: RosterWindow = {
 *     members: [{ id: 1 }, { id: 2 }],
 *     total: 10_000,
 *     selves: [{ id: 9_999, info: { name: 'joiner' } }],
 * }
 * ```
 */
export interface RosterWindow {
    /** At most `limit` members of the room, in the driver's order. */
    readonly members: PresenceMember[]
    /** The room's population when the window was read. */
    readonly total: number
    /** The requested self ids' members that the roster held at that instant. */
    readonly selves: PresenceMember[]
}

/** A message broadcast to a channel. */
export interface BroadcastMessage {
    /** The channel name. */
    readonly channel: string
    /** The event name. */
    readonly event: string
    /** The event payload. */
    readonly data: unknown
}

/**
 * Why a control frame was not published, and which frame it was (#318).
 *
 * Delivered to {@link BroadcastDriver.onControlRefused}. Carries the channel
 * and the reason so a refusal is actionable without correlating raw logs across
 * instances — the two refusals have different fixes (shrink the member, or
 * configure a secret) and a single "publish failed" signal would not separate
 * them.
 */
export interface ControlRefusal {
    /**
     * `oversize` — the serialized frame exceeds the configured ceiling, so
     * every peer would drop it on ingest. `no-secret` — no control secret is
     * configured, so the frame would be unauthenticated and dropped by every
     * peer's FR-015 check.
     */
    readonly reason: 'oversize' | 'no-secret'
    /**
     * The kind of frame that was refused.
     *
     * This widens with {@link ControlMessage}'s union — a new control kind is a
     * new value here too, on a type this package exports. A consumer switching
     * exhaustively over it sees the new member.
     */
    readonly kind: ControlMessage['kind']
    /** The presence channel, when the refused frame named one. */
    readonly channel?: string
    /** The serialized frame's size in bytes. Present only for `oversize`. */
    readonly bytes?: number
    /** The configured ceiling in bytes. Present only for `oversize`. */
    readonly limit?: number
}

/**
 * A whole connection is revoked; the owning instance hard-closes the socket.
 * This is `ChannelManager.evict`.
 *
 * **Never cleared.** The record becomes moot the instant the socket dies, so it
 * is left to expire — which is why {@link BroadcastDriver.clearRevocation} does
 * not accept one.
 *
 * @example
 * ```ts
 * const wholeConnection: ConnectionRevocation = { target: connectionId }
 * ```
 */
export interface ConnectionRevocation {
    /** The revoked connection id. */
    readonly target: string
    /** Absent: no channel is what makes this the whole connection. */
    readonly channel?: undefined
    /** Absent: a connection revocation is never cleared, so needs no identity. */
    readonly id?: undefined
}

/**
 * One connection revoked from one channel, socket left open. This is
 * `ChannelManager.revokeChannel`.
 *
 * **One call, one record** (#337). Two revocations of the same connection and
 * channel are two records, told apart by {@link id}, so a clear for the one an
 * instance applied can never remove one written after it. Keyed on the pair
 * alone, the clear for an older revocation erased a newer one in flight — and
 * when the newer one's control frame was lost, nothing ever enforced it.
 *
 * @example
 * ```ts
 * const oneRoom: ChannelRevocation = {
 *     target: connectionId,
 *     channel: 'private-orders',
 *     id: crypto.randomUUID(),
 * }
 * ```
 */
export interface ChannelRevocation {
    /** The revoked connection id. */
    readonly target: string
    /** The channel the revocation is scoped to. */
    readonly channel: string
    /**
     * Identity of THIS revocation: `crypto.randomUUID()`, minted by the manager
     * once per `revokeChannel` call. Two revocations of the same pair are two
     * records. A driver stores and returns it verbatim and compares it by
     * equality; it never parses, merges or mints it.
     */
    readonly id: string
}

/**
 * What a revocation revokes — the domain fact the driver seam carries.
 *
 * Two scopes, and they differ in whether the socket survives: a
 * {@link ConnectionRevocation} (`channel` absent) hard-closes the socket; a
 * {@link ChannelRevocation} (`channel` and `id` present) leaves one channel and
 * keeps the socket open.
 *
 * **The record is whole, and a decoder may never guess a missing part.** A
 * channel-scoped record returned without its channel reads as a connection
 * revocation and kills a session that should have lost one room — an escalation
 * with no error, no warning and no type failure. `listRevocations` drops what it
 * cannot fully decode for exactly this reason, and a channel record without its
 * id is as undecodable as one without its channel.
 *
 * @example
 * ```ts
 * const wholeConnection: Revocation = { target: connectionId }
 * const oneRoom: Revocation = {
 *     target: connectionId,
 *     channel: 'private-orders',
 *     id: crypto.randomUUID(),
 * }
 * ```
 */
export type Revocation = ConnectionRevocation | ChannelRevocation

/**
 * A control frame carried on the same bus as channel events but on a **distinct**
 * seam (A2/FR-016) — it is not a {@link BroadcastMessage} and never reaches
 * `deliverLocal`. It instructs the owning instance to act on a connection
 * (revoke a socket, apply a presence join/leave), never to deliver an event.
 *
 * The `mac` is the FR-015 authenticity tag (an HMAC over the payload, keyed by
 * a per-deployment shared secret). It is **optional on the type** because an
 * unauthenticated frame is representable on the wire — the ingest check is what
 * drops a control message whose `mac` is absent or fails to verify, before the
 * message is ever obeyed.
 */
export interface ControlMessage {
    /**
     * The control kind. `evict` revokes a whole connection; `revoke-channel`
     * revokes it from ONE channel and leaves the socket open; `presence-join` /
     * `presence-leave` announce a roster change across instances.
     *
     * **A new KIND is safe here; a new FIELD costs every older peer the frame**
     * — and the asymmetry is load-bearing rather than stylistic. The Redis
     * driver's MAC covers a fixed field list, so a kind added to this union
     * changes no canonical bytes and a peer running an older release verifies
     * the frame, admits it, and falls off the end of a `switch` that has no
     * `default`. A **field** added to the wire but not to that list would ship
     * unauthenticated, so it is always added to both — and then every older
     * peer drops that frame as an invalid MAC, with a WARN.
     *
     * That is acceptable only on a kind no published peer ACTS on. A field
     * that is `undefined` is left out of the canonical bytes, so a field
     * carried by one kind leaves every other kind's MAC byte-identical.
     * `revocationId` (#337) rides only on `revoke-channel`, which a `0.3.0`
     * peer never obeyed, so what it costs there is a WARN and not a lost
     * action. A field on `evict` or a presence kind would cost the action.
     * Decision-table home for that rule: `#canonical`'s field list in
     * `drivers/redis.ts`.
     */
    readonly kind:
        | 'evict'
        | 'presence-join'
        | 'presence-leave'
        | 'revoke-channel'
    /**
     * The connection id the control acts on — for `evict` and
     * `revoke-channel`, the only kinds whose receiver acts on it.
     *
     * **Informational on `presence-join` / `presence-leave`** (#348): the
     * announcing connection's id, or the channel name for a departure a
     * driver reported through {@link BroadcastDriver.onRosterDeparture} (no
     * connection announced it). No receiver may act on it for those kinds;
     * it stays on the wire, and inside the MAC, because a `0.3.0` peer
     * requires it.
     */
    readonly target: string
    /**
     * For a `presence-join` / `presence-leave`: the presence channel the roster
     * change is on. Absent for an `evict` (which spans every channel).
     */
    readonly channel?: string
    /**
     * For a `presence-join` / `presence-leave`: the **client-visible**
     * {@link PresenceMember} to fan out to local presence subscribers (FR-018 —
     * the owning-instance sweep metadata is internal to the driver and never
     * travels on this field). Absent for an `evict`.
     */
    readonly member?: PresenceMember
    /**
     * `revoke-channel` only: the {@link ChannelRevocation.id} this frame
     * announces, so the owner clears exactly that record once it has applied
     * it (#337). Absent on every other kind.
     */
    readonly revocationId?: string
    /** The FR-015 authenticity tag; absent on an unauthenticated frame. */
    readonly mac?: string
}

/**
 * The result of {@link BroadcastDriver.holdMember}: whether this hold filled an
 * empty roster slot.
 *
 * A hold means "this process holds the slot `String(member.id)` with this
 * entry". `arrived` is `true` **only if no process held the slot** before this
 * hold — cluster-wide on a shared roster, not per connection and not per
 * instance. The manager announces a `joined` frame from it, so a driver may not
 * fake it: reporting `true` for a slot another holder already fills sends a
 * duplicate `joined` to every subscriber.
 *
 * @example
 * ```ts
 * const { arrived } = await driver.holdMember('presence-room', { id: 7, info: {} })
 * if (arrived) console.log('member 7 is now present')
 * ```
 */
export interface RosterHold {
    /** `true` iff no process held the slot before this hold. */
    readonly arrived: boolean
}

/**
 * The result of {@link BroadcastDriver.releaseMember}: whether this release
 * emptied the roster slot.
 *
 * A release means "this process drops its hold on the slot". `gone` is `true`
 * **only if this process held the slot and no holder is left** after the
 * release. A release by a process that did not hold the slot is `false`, even
 * when the slot ends up empty. The manager announces a `left` frame from it, so
 * a driver may not fake it: reporting `true` while another holder remains
 * removes a present member from every client's list.
 *
 * @example
 * ```ts
 * const { gone } = await driver.releaseMember('presence-room', 7)
 * if (gone) console.log('member 7 has left')
 * ```
 */
export interface RosterRelease {
    /** `true` iff this process held the slot and no holder is left. */
    readonly gone: boolean
}

/**
 * A roster slot a driver emptied while releasing ANOTHER process's hold —
 * reported through {@link BroadcastDriver.onRosterDeparture} (#348).
 *
 * The Redis driver produces one when its ghost sweep releases a crashed
 * instance's hold and no holder is left: nobody else would ever announce that
 * member's `left`. `member` is the entry the released holder last stored, as
 * the roster read decodes it.
 *
 * @example
 * ```ts
 * driver.onRosterDeparture?.(({ channel, member }) => {
 *     console.log(`member ${member.id} left ${channel}`)
 * })
 * ```
 */
export interface RosterDeparture {
    /** The presence channel whose slot was emptied. */
    readonly channel: string
    /** The client-visible member the released holder last stored. */
    readonly member: PresenceMember
}

/**
 * What one revocation re-check did (#384): how many applies it attempted, and
 * how many of those failed. A handler registered through
 * {@link BroadcastDriver.onRevocationReconcile} may resolve to one, and a
 * driver may report it; the Redis driver puts it on its pass sample.
 *
 * **The one home of what the two counts mean.** Documentation elsewhere links
 * here rather than restating it.
 *
 * - **One unit is one apply**: one connection revocation, or **one channel
 *   pair** — every record of one `(target, channel)` pair is settled by ONE
 *   leave (#337), so a pair with three stored ids is one unit, not three. A
 *   count of stored ids would multiply one leave's failure by N, which no
 *   operator can act on.
 * - **Only a record for a socket this instance owns is attempted.** A record
 *   the ownership check drops names no socket here, so it is neither
 *   attempted nor failed.
 * - **A failure is an apply that threw**, the hard-close and the teardown of a
 *   connection revocation included. It does not mean the socket stayed
 *   subscribed: an apply that resolves without effect counts as done. A
 *   record that could not be CLEARED after its leave succeeded is not a
 *   failure either — the revocation was applied, and only the record outlives
 *   it.
 *
 * Both counts are safe integers with `0 ≤ failed ≤ attempted`. A value that
 * claims to be a tally and breaks that is refused by the Redis driver, which
 * warns once per pass and reports no counts.
 *
 * @example
 * ```ts
 * driver.onRevocationReconcile?.(async (): Promise<RevocationTally> => {
 *     let attempted = 0
 *     let failed = 0
 *     for (const revocation of await localRevocations()) {
 *         attempted++
 *         if (!await apply(revocation)) failed++
 *     }
 *     return { attempted, failed }
 * })
 * ```
 */
export interface RevocationTally {
    /** How many applies this re-check attempted. */
    readonly attempted: number
    /** How many of those threw. */
    readonly failed: number
}

/**
 * A broadcast transport. `publish` emits a message; every instance's
 * `onMessage` handler (registered once) receives it and re-resolves local
 * delivery. The memory driver loops back in-process; the Redis driver fans out
 * across processes.
 *
 * The presence-state ops and `onControl` are **optional**: a driver that omits
 * them keeps single-process behaviour, and the manager reaches them only behind
 * one feature-detect guard (A5). Use {@link PresenceCapableDriver} for the
 * narrowed shape once that guard has confirmed them present.
 *
 * **The notification hooks share one lifecycle** (#349, ADR 007) —
 * {@link onControlRefused}, {@link onRevocationReconcile},
 * {@link onRosterDeparture}, {@link onRosterLapse} and
 * {@link onRosterMaintenance}:
 *
 * - **one owner per driver**: a second registration replaces the first;
 * - **one handler** per hook, never a list;
 * - **the driver's own shutdown drops it**, so a shut-down driver calls
 *   nothing. This interface declares no `close()`; each driver's own shutdown
 *   is where the drop happens.
 *
 * {@link onControl} is the one exception: its lifetime is its subscription.
 *
 * **The rule for a sixth hook.** A new driver-to-owner notification becomes a
 * new hook only if its payload **and** its delivery contract differ from every
 * existing one; otherwise it extends one. The per-hook bookkeeping is
 * consolidated only once a **second** production driver implements three or
 * more of these hooks — not before.
 */
export interface BroadcastDriver {
    /**
     * Publish a message to all instances (including this one).
     *
     * @param message - The message to broadcast.
     */
    publish(message: BroadcastMessage): void | Promise<void>
    /**
     * Register the handler invoked for every received message.
     *
     * @param handler - Called with each delivered message.
     */
    onMessage(handler: (message: BroadcastMessage) => void): void
    /**
     * OPTIONAL (FR-005, #345). Hold the channel's roster slot `String(member.id)`
     * for this process, with `member` as this process's entry.
     *
     * Holding a slot this process already holds replaces its entry and reports
     * `arrived: false`. Several processes may hold one slot at once; the slot
     * stays in the roster while any holder remains. See {@link RosterHold} for
     * what `arrived` promises — a driver may not fake it.
     *
     * **Replaces the pre-`0.4.0` add method**, whose old name a `ChannelManager`
     * refuses at construction if a driver still offers it.
     *
     * `member` is deep-frozen (#354): to store extra fields beside it, build a
     * new object rather than writing to it.
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member this process holds the slot as.
     * @returns Whether this hold filled an empty slot.
     */
    holdMember?(
        channel: string,
        member: PresenceMember,
    ): RosterHold | Promise<RosterHold>
    /**
     * OPTIONAL (FR-005, #345). Drop this process's hold on the channel's roster
     * slot `String(memberId)`.
     *
     * The slot leaves the roster only when its last holder releases it; a
     * release by a process that holds nothing leaves every other holder's hold
     * untouched. See {@link RosterRelease} for what `gone` promises — a driver
     * may not fake it.
     *
     * **Replaces the pre-`0.4.0` remove method**, whose old name a
     * `ChannelManager` refuses at construction if a driver still offers it.
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member whose slot this process releases.
     * @returns Whether this release emptied a slot this process held.
     */
    releaseMember?(
        channel: string,
        memberId: string | number,
    ): RosterRelease | Promise<RosterRelease>
    /**
     * OPTIONAL (FR-005, #341). Read a bounded window of the channel's
     * authoritative roster, its population, and the members of `selfIds` — at
     * one instant. See {@link RosterWindow} for the invariants.
     *
     * **Replaces the pre-`0.4.0` whole-room read**, which a `ChannelManager`
     * refuses at construction if a driver still offers it.
     *
     * **Cost contract.** A driver SHOULD transfer and parse O(`limit` +
     * `selfIds.length`) entries per call, independently of the room's size.
     * Nothing can enforce this through the types: a driver that reads the whole
     * room and slices it satisfies the signature and reintroduces the
     * per-subscribe cost this method exists to bound.
     *
     * **The members are handed out uncopied** (#354): to the application, and
     * to every caller sharing one read. Return members that nothing mutates
     * afterwards — the bundled drivers return deep-frozen ones. A driver that
     * decodes its own members instead of storing the frozen object
     * `holdMember` received should freeze what it returns.
     *
     * @param channel - The presence channel.
     * @param limit - The most members to return; a positive integer.
     * @param selfIds - The member ids whose entries must be returned in
     *   `selves` when the roster holds them; at most
     *   {@link MAX_ROSTER_READ_SELF_IDS}. May be empty.
     * @returns The window, the population and the selves.
     * @throws {Error} Before any work, if `limit` is not a positive integer or
     *   `selfIds` is longer than {@link MAX_ROSTER_READ_SELF_IDS}.
     */
    readRoster?(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow | Promise<RosterWindow>
    /**
     * OPTIONAL (A2/FR-016). Register the handler for {@link ControlMessage}s —
     * a **distinct** seam from {@link onMessage}, never folded into the
     * channel-event union. A driver exposing this seam only ever delivers a
     * frame that already passed the FR-015 authenticity check; an unauthenticated
     * frame is dropped inside the driver and never reaches this handler.
     * Drivers pass `revocationId` through unchanged and include it in any MAC
     * they compute over the frame.
     *
     * @param handler - Called with each **authenticated** received control message.
     */
    onControl?(handler: (control: ControlMessage) => void): void
    /**
     * OPTIONAL (FR-015/FR-016). Publish a {@link ControlMessage} to every
     * instance's {@link onControl} seam, attaching the authenticity MAC. The
     * counterpart to {@link onControl}; a driver that omits one omits both. The
     * `mac` field of `control` is ignored — the driver computes and attaches it.
     * Drivers pass `revocationId` through unchanged and include it in any MAC
     * they compute over the frame.
     *
     * @param control - The control message to broadcast (its `mac` is set here).
     */
    publishControl?(control: ControlMessage): void | Promise<void>
    /**
     * OPTIONAL (S1/FR-014). Durably record a revocation, so it survives a lost
     * control frame. The record lives in the driver (decision-table home:
     * "whether a revoked connection stays revoked across a reconnect") and is
     * re-checked by the owning instance on each {@link onRevocationReconcile}
     * pass.
     *
     * **The seam carries the domain fact; the driver chooses the bytes.** How a
     * {@link Revocation} is encoded is the implementation's business and is
     * never normative on this interface — the same split this package makes
     * between the client-visible `PresenceMember` and the driver-internal sweep
     * metadata.
     *
     * @param revocation - What is revoked: a whole connection, or a connection
     *   in one channel.
     */
    markRevocation?(revocation: Revocation): void | Promise<void>
    /**
     * OPTIONAL (S1/FR-014). The revocations that are live, with expired
     * entries reaped. The owning instance re-checks this on each
     * {@link onRevocationReconcile} pass to recover a missed revoke.
     *
     * **What an implementation must return — the one home of this contract**
     * (#359, refining #276's "exactly the records live at call time"). A call
     * judges liveness against ONE `now` of its own choosing, and it returns
     * every record that was live at that `now` **and present for the whole
     * enumeration**. A record written, or removed, while the call enumerates
     * may or may not be returned — so a store that pages is conforming, and a
     * record it misses because it landed behind its cursor is returned by the
     * next call. Other documentation links here rather than restating it.
     *
     * **An implementation MUST fail closed.** A record it cannot fully decode
     * is dropped, never returned with a missing or partial scope: a record
     * returned without its channel is applied as a whole-connection revocation,
     * which hard-closes a socket that should only have left one room. This
     * index is the one cross-instance write channel with no authenticity tag,
     * so what a decoder refuses is the boundary. A read that fails, or a reply
     * it cannot read, **throws** — never `[]`, which the caller reads as
     * "nobody is revoked".
     *
     * @param owns - Which targets the caller keeps (#359). Implementations
     *   SHOULD apply it while enumerating, so a store holds only the caller's
     *   records rather than every instance's. The caller filters again, so an
     *   implementation that ignores it is correct — only unbounded in its own
     *   store. It is called synchronously, once per decoded record; a throw
     *   from it fails the call. Omitted, every live record is returned, which
     *   is what an existing zero-argument caller gets.
     * @returns The live revocations `owns` keeps, each record once.
     * @throws {Error} When the store cannot be read, or its reply cannot be.
     * @example
     * ```ts
     * // A custom store that applies `owns` as it enumerates.
     * import type { Revocation } from '@lockness/realtime'
     *
     * const stored: Revocation[] = [{ target: 'c1' }, { target: 'x9' }]
     * function listRevocations(owns?: (target: string) => boolean) {
     *     return stored.filter((r) => owns === undefined || owns(r.target))
     * }
     * listRevocations((id) => id === 'c1') // [{ target: 'c1' }]
     * ```
     */
    listRevocations?(
        owns?: (target: string) => boolean,
    ): Revocation[] | Promise<Revocation[]>
    /**
     * OPTIONAL (S1/FR-014). Forget a revocation the owning instance has now
     * applied.
     *
     * **Only a channel-scoped record needs this, and that is why it exists.** A
     * connection-scoped record becomes moot the instant the socket dies, so it
     * is left to expire. A channel-scoped one has a live socket to act on for
     * the whole TTL, so an uncleared record would re-apply the leave at every
     * reconcile tick — kicking a client that has legitimately re-subscribed,
     * once per tick, until the record expires. Clearing on apply makes a record
     * mean exactly one thing: *a revocation the owning instance has not applied
     * yet.*
     *
     * **Removes exactly the record with this id** (#337). A record for the same
     * target and channel with another id MUST survive: it is a revocation
     * written after the one being cleared, and nothing else will enforce it if
     * its control frame was lost. Clearing a record that is already gone is not
     * an error.
     *
     * @param revocation - The channel revocation that has been applied.
     */
    clearRevocation?(revocation: ChannelRevocation): void | Promise<void>
    /**
     * OPTIONAL (#318). Register the handler the driver invokes when it declines
     * to publish a control frame.
     *
     * {@link publishControl} warns and RETURNS on a refusal, so from the
     * manager's side a refused frame and a published one are the same
     * `void | Promise<void>`. `subscribe` answers `{ ok: true }`, the
     * authoritative roster is correct, and peers already in the channel hold a
     * stale roster until they resubscribe — with the only signal anywhere a
     * WARN on the single instance that refused. This seam is how that becomes
     * observable to something an operator can alert on.
     *
     * It reports; it does not decide. #312 settled that a refusal never rolls
     * back the roster write, and this seam does not reopen that.
     *
     * Its registration follows the hooks' shared lifecycle
     * ({@link BroadcastDriver}): one handler, replaced on re-registration,
     * dropped by the driver's own shutdown.
     *
     * @param handler - Called with each refusal, before `publishControl`
     *   returns. A throwing handler is contained and logged; it never becomes
     *   the caller's problem.
     */
    onControlRefused?(handler: (refusal: ControlRefusal) => void): void
    /**
     * OPTIONAL (S1/FR-014). Register the handler the driver invokes on its
     * periodic reconcile pass, so the owning instance re-checks
     * {@link listRevocations} and applies any that name a local socket —
     * recovering a revoke whose control frame was lost while the owning socket
     * was between reconnects. How far that bounds exposure to a lost revoke
     * is the implementation's to state; the Redis driver's bound is on its
     * own `onRevocationReconcile`.
     *
     * Its registration follows the hooks' shared lifecycle
     * ({@link BroadcastDriver}).
     *
     * **The handler may resolve to a {@link RevocationTally}** (#384): how
     * many applies the re-check attempted and how many failed. A driver may
     * report it — the Redis driver puts it on its pass sample and re-arms its
     * enforcement deadline only after a pass with no failure — and may ignore
     * it. Resolving to nothing is conforming: the handler an application
     * registered before #384 changes nothing. The driver still calls it with
     * no argument.
     *
     * @param handler - Called with no arguments on each reconcile tick;
     *   resolves to the re-check's tally, or to nothing.
     */
    onRevocationReconcile?(
        handler: () =>
            | RevocationTally
            | void
            | Promise<RevocationTally | void>,
    ): void
    /**
     * OPTIONAL (#348). Register the handler the driver calls for every roster
     * slot it empties while releasing **another process's** hold — on Redis,
     * the ghost sweep of a crashed instance. The manager announces each one as
     * a `left`, exactly as it announces a leave.
     *
     * **Never for {@link releaseMember}**: that release's `gone` is already
     * announced by its caller, and calling the handler too would send a
     * second `left`. A driver may not report a departure the roster did not
     * record — the manager announces it to every subscriber of the channel.
     *
     * Its registration follows the hooks' shared lifecycle
     * ({@link BroadcastDriver}): a closed driver reports nothing.
     *
     * The driver awaits the handler, one departure at a time, and contains a
     * throw as a WARN; the release that produced the departure stays
     * committed either way.
     *
     * @param handler - Called with each departure.
     *
     * @example
     * ```ts
     * driver.onRosterDeparture?.(({ channel, member }) =>
     *     console.log(`swept ${member.id} out of ${channel}`)
     * )
     * ```
     */
    onRosterDeparture?(
        handler: (departure: RosterDeparture) => void | Promise<void>,
    ): void
    /**
     * OPTIONAL (#349). Register the handler the driver calls when this
     * process's roster holds may have been released **on its behalf** — on
     * Redis, when the heartbeat finds that this instance's liveness key had
     * lapsed, so a peer's ghost sweep may have taken it for dead. The owner
     * should write its holds again through its normal write path; the manager
     * re-checks its durable revocations, then re-holds every local slot one
     * at a time, announcing only what a hold says arrived.
     *
     * **Delivery contract.** The driver never awaits the handler from the
     * path that detected the lapse (a heartbeat that waited behind every slot
     * write would cause the next lapse). At most one run is in flight, and
     * however many lapses arrive during a run, exactly one trailing run
     * follows it. A run that throws or rejects is one WARN on the driver's
     * side, and the next successful detection runs it again; the handler need
     * not retry. A driver may call it when no hold was actually released: the
     * re-write must be idempotent, and a hold that finds its slot held
     * announces nothing.
     *
     * **Why an `AbortSignal`.** A run writes K slots, and the driver's
     * shutdown must not wait for all of them nor let a write be issued after
     * it resolves. The driver aborts the signal when it shuts down, and the
     * handler checks it before each write; the shutdown then waits for the
     * one write in flight, or for whatever the handler awaits before its
     * first write (the manager's revocation re-check).
     *
     * Only an owner with a roster registers it. A driver without the notion
     * of liveness omits it. Its registration follows the hooks' shared
     * lifecycle ({@link BroadcastDriver}); the shutdown that drops it first
     * aborts the signal and waits for the run in flight.
     *
     * @param handler - Called with a signal the driver aborts on shutdown.
     *
     * @example
     * ```ts
     * driver.onRosterLapse?.(async (signal) => {
     *     for (const slot of localSlots()) {
     *         if (signal.aborted) return
     *         await rewrite(slot)
     *     }
     * })
     * ```
     */
    onRosterLapse?(
        handler: (signal: AbortSignal) => void | Promise<void>,
    ): void
    /**
     * OPTIONAL (#371). Register the handler the driver invokes on its own
     * periodic roster-maintenance pass, so a roster release this instance
     * could not commit — a presence leave's own release, or the #323/#373
     * join compensation's reclaim — is retried without waiting for the ghost
     * sweep to notice a dead instance, which a healthy one never is.
     *
     * **Fired unconditionally, never gated on a detected fault** — the
     * opposite delivery contract from {@link onRosterLapse} (edge-triggered
     * on a detected liveness lapse) and from {@link onRevocationReconcile}
     * (the #362/#384 deadline-measured pass this hook is deliberately kept
     * out of, so an unrelated retry never inflates that pass's measured
     * duration). Its payload is nothing and its cadence is every tick that
     * proved the connection healthy — a payload AND a delivery contract that
     * differ from every existing hook, which is why this is its own hook
     * rather than an addition to either (the "sixth hook" rule stated above).
     *
     * Its registration follows the hooks' shared lifecycle
     * ({@link BroadcastDriver}): one handler, replaced on re-registration,
     * dropped by the driver's own shutdown, which waits for a run in flight
     * before dropping it.
     *
     * A driver that omits this leaves the ghost sweep as the only backstop
     * for an owed release — exactly the behaviour before #371, and a
     * conforming, unchanged default for every driver that predates it.
     *
     * @param handler - Called with no arguments after each tick that proved
     *   this instance's connection healthy; a throw is contained by the
     *   driver's own scheduler and never reaches the caller that fired it.
     *
     * @example
     * ```ts
     * driver.onRosterMaintenance?.(() => {
     *     console.log('roster maintenance tick')
     * })
     * ```
     */
    onRosterMaintenance?(handler: () => void | Promise<void>): void
    /**
     * OPTIONAL (#295). Declare that this instance now hosts `channel`, so the
     * driver subscribes to its traffic and nothing else's.
     *
     * A driver that omits this — or that omits {@link unwatchChannel} — keeps
     * today's behaviour: one prefix-wide subscription, every channel's traffic
     * on every instance. The two are detected **as a set**, never
     * independently: watching without unwatching makes the subscribed set
     * monotonic over the process lifetime, which is strictly worse than the
     * behaviour it replaces and invisible, because delivery stays correct.
     *
     * **The awaited guarantee is the write leg only.** It resolves once the
     * subscribe frame has reached the broker socket — never that delivery has
     * started, which no driver can promise without waiting on the broker's own
     * acknowledgement. The residual is one round trip **plus the queue depth
     * ahead of the frame**, since a driver may serialize its writes.
     *
     * A rejection means the frame did not reach the socket. It does not mean
     * the channel is unhosted: the caller keeps the membership, because a
     * driver that self-heals will re-issue the subscription and delivery
     * resumes.
     *
     * @param channel - The channel this instance has begun hosting.
     * @returns Resolves once the subscribe frame is on the wire.
     * @throws If the frame could not be written.
     */
    watchChannel?(channel: string): void | Promise<void>
    /**
     * OPTIONAL (#295). Declare that this instance no longer hosts `channel`.
     *
     * Removes the subscription **and** the channel from whatever set the driver
     * re-issues after a reconnect. A driver that unsubscribes on the wire and
     * leaves the channel in its re-issue set resurrects it on the next fault —
     * a leak that only appears under fault, and one that decays the fan-out win
     * silently back toward the behaviour this replaces.
     *
     * @param channel - The channel this instance has stopped hosting.
     * @returns Resolves once the unsubscribe frame is on the wire.
     * @throws If the frame could not be written.
     */
    unwatchChannel?(channel: string): void | Promise<void>
}

/**
 * A {@link BroadcastDriver} narrowed to one that subscribes per channel — both
 * watch ops are guaranteed present. Obtain it from `channelWatcher`, never by
 * testing the members at a call site.
 */
export interface ChannelWatchCapableDriver extends BroadcastDriver {
    /** Declare that this instance hosts `channel`. */
    watchChannel(channel: string): void | Promise<void>
    /** Declare that this instance no longer hosts `channel`. */
    unwatchChannel(channel: string): void | Promise<void>
}

/**
 * A {@link BroadcastDriver} narrowed to one that can durably record revocations
 * — all three revocation ops are guaranteed present. Obtain it from
 * `revocationStore`, never by testing the members at a call site.
 */
export interface RevocationStoreDriver extends BroadcastDriver {
    /** Durably record a revocation. */
    markRevocation(revocation: Revocation): void | Promise<void>
    /**
     * The live revocations — the contract is
     * {@link BroadcastDriver.listRevocations}'s, not restated here.
     *
     * @param owns - Which targets the caller keeps; optional, and an
     *   implementation may ignore it (see the contract).
     * @returns The live revocations `owns` keeps.
     * @throws {Error} When the store or its reply cannot be read.
     */
    listRevocations(
        owns?: (target: string) => boolean,
    ): Revocation[] | Promise<Revocation[]>
    /** Forget exactly the channel revocation with this id, once applied. */
    clearRevocation(revocation: ChannelRevocation): void | Promise<void>
}

/**
 * A {@link BroadcastDriver} narrowed to one that owns the authoritative presence
 * roster — the presence-state ops are guaranteed present. Obtain it from the
 * manager's single feature-detect guard, never by asserting the shape ad hoc.
 */
export interface PresenceCapableDriver extends BroadcastDriver {
    /**
     * Hold the channel's roster slot for this process (#345). See
     * {@link BroadcastDriver.holdMember} for the contract.
     *
     * @param channel - The presence channel.
     * @param member - The client-visible member this process holds the slot as.
     * @returns Whether this hold filled an empty slot.
     */
    holdMember(
        channel: string,
        member: PresenceMember,
    ): RosterHold | Promise<RosterHold>
    /**
     * Drop this process's hold on the channel's roster slot (#345). See
     * {@link BroadcastDriver.releaseMember} for the contract.
     *
     * @param channel - The presence channel.
     * @param memberId - The id of the member whose slot this process releases.
     * @returns Whether this release emptied a slot this process held.
     */
    releaseMember(
        channel: string,
        memberId: string | number,
    ): RosterRelease | Promise<RosterRelease>
    /**
     * Read a bounded window of the channel's authoritative roster (#341). See
     * {@link BroadcastDriver.readRoster} for the contract.
     *
     * @param channel - The presence channel.
     * @param limit - The most members to return; a positive integer.
     * @param selfIds - The member ids to return in `selves` when held.
     * @returns The window, the population and the selves.
     */
    readRoster(
        channel: string,
        limit: number,
        selfIds: readonly (string | number)[],
    ): RosterWindow | Promise<RosterWindow>
}
