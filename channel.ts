/**
 * @fileoverview Channel kinds and the authorization contract.
 *
 * A channel's kind is derived from its name (Laravel-style prefixes):
 * `presence-*` → presence, `private-*` → private, anything else → public. The
 * app supplies an authorizer; the manager (not the channel) enforces it at
 * subscribe over the connection's **server-derived** identity (security S1).
 *
 * @module @lockness/realtime/channel
 */

/** The three channel kinds. */
export type ChannelKind = 'public' | 'private' | 'presence'

/**
 * Derive a channel's kind from its name.
 *
 * @param name - The channel name.
 * @returns Its kind.
 *
 * @example
 * ```ts
 * channelKind('presence-room.1') // 'presence'
 * channelKind('private-orders')  // 'private'
 * channelKind('news')            // 'public'
 * ```
 */
export function channelKind(name: string): ChannelKind {
    if (name.startsWith('presence-')) return 'presence'
    if (name.startsWith('private-')) return 'private'
    return 'public'
}

/**
 * A presence channel member's public identity, shared only with authorized
 * members of that channel.
 *
 * **An immutable value** (#354). Every `PresenceMember` the framework hands
 * out — in a `subscribe` snapshot's `here.members`, in the frames a custom
 * `encode` receives, and to a driver's `holdMember` — is deep-frozen where it
 * was minted, and the same object may be handed to several callers at once.
 * A write throws `TypeError` (ES modules are strict), and `id` / `info` are
 * `readonly`, so a direct write is also a compile error. Copy before
 * decorating; the copy is yours:
 *
 * ```ts
 * const view = here.members.map((m) => ({
 *     ...m,
 *     info: { ...m.info, isYou: m.id === me },
 * }))
 * ```
 *
 * Do not compare members by identity to detect a change: two snapshots may
 * hold the same object on one driver and fresh ones on another.
 */
export interface PresenceMember {
    /**
     * The member's stable id.
     *
     * **A string or a finite number (#346), length-bounded and charset-free
     * (#306).** The type is checked at runtime, not only by this annotation:
     * an authorizer in plain JS, behind a cast or reading a nullable column
     * can hand over `null`, `undefined`, a bigint or an object, and every
     * presence consumer keys a member by `String(id)` — so two users with a
     * `null` id would share one entry, and on Redis every peer would drop the
     * frame announcing it. Then the string form must be 1 to 200 characters.
     * `ChannelManager.subscribe` refuses anything else with a
     * `PresenceMemberIdError` before it writes the roster. Any finite number
     * is accepted, `1e21` included, and `-0` is the same member as `0`; send a
     * 64-bit key as a string, since a number above `2 ** 53` has already lost
     * precision.
     *
     * The charset is deliberately NOT constrained, unlike {@link Connection.id}
     * which #304 bound to `[A-Za-z0-9:._-]`. This is application identity —
     * deployments key presence on emails, usernames and external providers' ids
     * — and that charset rejects `a@b.com` on the `@`. Nothing is bought by it
     * either: RESP bulk strings are length-prefixed so no value can forge a
     * command, control frames carry a MAC, and the owned-set entry parses on
     * the FIRST space with a charset-bounded channel ahead of it, so a field
     * containing spaces is unambiguous.
     *
     * Length is the one thing no layer below bounds. The id becomes a Redis
     * hash field, and the caps upstream are a 10 MiB RESP frame and an 8 KiB
     * control payload — neither a bound on this value, and the roster write
     * happens before the control publish, so without this an oversized id lands
     * in the hash while the frame announcing it is dropped with a warning and
     * `subscribe` still answers `{ ok: true }`.
     */
    readonly id: string | number
    /**
     * Optional public info shown to other members. Deep-frozen, like the
     * member (#354): spread it into a new object to add a field.
     */
    readonly info?: Readonly<Record<string, unknown>>
}

/**
 * The presence roster one `subscribe` returns — bounded, and honest about it
 * (#339).
 *
 * A room's population has no ceiling, so the reply cannot be the room. It is at
 * most `maxPresenceSnapshotMembers` members (default
 * `MAX_PRESENCE_SNAPSHOT_MEMBERS`), and the joiner's own member is always among
 * them when the roster it was cut from holds it. `members.length < total` if
 * and only if the snapshot is partial.
 *
 * **A UI hint, not an access list.** Which members fill the window is the
 * driver's choice: join order on the memory driver; on Redis, the whole room in
 * hash order when it fits, and otherwise a new random sample of K members on
 * every subscribe, re-joins included. Authorization never reads this.
 *
 * @example
 * ```ts
 * const { here } = await manager.subscribe(connection, 'presence-room.1')
 * if (here) render(here.members, { others: here.total - here.members.length })
 * ```
 */
export interface PresenceSnapshot {
    /** Up to the bound's worth of members, in driver order, self included. */
    members: PresenceMember[]
    /**
     * How many entries the roster held when this snapshot was cut — taken from
     * the read already made, never an extra driver command. A snapshot-time
     * number: `joined`/`left` frames do not carry or update it. It counts
     * MEMBERS on every source — one member with two tabs counts once, on the
     * `'local'` fallback and on a roster-less driver too (#343).
     */
    total: number
    /**
     * Where the roster came from. `'authoritative'` is every instance's roster,
     * read from the driver. `'local'` is a **fragment**: this instance's own
     * members, returned when the authoritative read failed on a join that had
     * already committed everywhere. A driver with no roster capability is
     * single-process, so its local view IS the authority and it reports
     * `'authoritative'`.
     */
    source: 'authoritative' | 'local'
}

/**
 * The result of authorizing a connection for a channel.
 *
 * **`false` refuses THIS ATTEMPT. It never removes a standing membership**
 * (#331). A denial on a channel the connection already holds answers that
 * subscribe frame `{ ok: false }` and leaves the subscription, the roster entry
 * and delivery exactly as they were.
 *
 * That is deliberate, and it is what makes `false` safe to return. An
 * authorizer is arbitrary application code — {@link Authorizer} sanctions a DB
 * read, an audit write or a rate-limit increment on **admission**, which is not
 * a verb budget: it never runs for a public channel or for `unsubscribe` at
 * all, so it cannot bound how often the channel verbs are invoked. See
 * `ChannelManager.handlerHooks` for where that belongs (#329). So `false` today
 * already carries "not this fast" and "I could not check". Revoking on it would
 * give those the force of an eviction, silently, with no compile error and no
 * way to express the difference.
 *
 * **Revocation is an explicit server-side verb**, never a side effect of the
 * client asking again: `ChannelManager.unsubscribe` for one channel,
 * `ChannelManager.evict` for a real revoke — which is durable and crosses
 * processes, where a denial-driven removal would survive nothing.
 *
 * **The admission rule is enforced at runtime, not only by this type** (#347).
 * `true` or a `PresenceMember` admits; `false` denies; `undefined`, `null`,
 * `0`, `''`, `'yes'`, `1`, an array, a boxed primitive and an object that
 * throws when inspected (#353) make `subscribe` throw `AuthorizeResultError`.
 * Any other object is admitted **only as a member, on every channel kind**
 * (#357): anything but `{ id, info? }` with a valid `id` — a Deno KV entry, a
 * pg `QueryResult`, `{}`, a raw row — throws `PresenceMemberShapeError`,
 * `PresenceMemberIdError` or `PresenceMemberSizeError`, and a private channel
 * discards the member it admits. So one authorizer serves both kinds with the
 * same outcome. The type cannot
 * stop those on its own: `(await select())[0]` under the default
 * `noUncheckedIndexedAccess: false`, an `any`-typed query row, a cast or a
 * plain-JS app all reach the manager with a value this union does not name.
 * They throw rather than deny because a missing `return` is a bug, and read as
 * `false` it would be a deny-all nobody could diagnose. Lockness deliberately
 * does not treat a falsy value as a quiet deny.
 */
export type AuthorizeResult = boolean | PresenceMember

/**
 * What {@link classifyAuthorizeResult} decided about one authorizer result.
 *
 * - `deny` — the authorizer returned exactly `false`: a policy decision.
 * - `admit` — `true` (`member` is `undefined`) or an object (`member` is it,
 *   UNCHECKED: typed `object`, not `PresenceMember`, because only
 *   `admitPresenceMember` may turn it into one — #350). `subscribe` runs that
 *   admission on every channel kind (#357), so an object admits only if it is
 *   a member; this verdict is the classification, not the last word.
 * - `invalid` — anything else, a defect in the authorizer. `type` is the
 *   value's type label only; the value itself is never carried, because it is
 *   application data and the label ends up in a log.
 */
export type AuthorizeVerdict =
    | { verdict: 'deny' }
    | { verdict: 'admit'; member: object | undefined }
    | { verdict: 'invalid'; type: string }

/**
 * Classify an authorizer's result against the admission rule (#347).
 *
 * Pure and total: it runs on `unknown` because the value comes from
 * application code the type system does not reach (see {@link AuthorizeResult}).
 * `ChannelManager.subscribe` calls it straight after the awaited authorizer and
 * before the member id check, the size check, the caps and every write, so a
 * refusal is never a partial write.
 *
 * An admitting object's FIELDS are not checked here — `admitPresenceMember`
 * owns that (#306, #326, #346, #350), and `subscribe` runs it on EVERY channel
 * kind (#357): a private channel checks the member exactly as a presence one
 * does, then discards it. This function takes no `kind` on purpose — the rule
 * about the value has one home, and it is not here.
 * The rule is "an object", not "a plain object": a class-instance member is
 * legitimate. Arrays and boxed primitives are objects to `typeof` and are
 * refused all the same; `[]` is what an empty query result looks like.
 *
 * **It never throws** (#353). An object it cannot inspect — a revoked Proxy,
 * on which `Array.isArray` throws, or a Proxy whose `getPrototypeOf` trap
 * throws — is `invalid` with the type `'uninspectable object'`, so `subscribe`
 * raises `AuthorizeResultError` rather than an anonymous `TypeError`. A
 * transparent Proxy is an object like any other and admits.
 *
 * @param result - Whatever the authorizer resolved to.
 * @returns The verdict: `deny`, `admit` with the member (if any), or
 *   `invalid` with the value's type label.
 *
 * @example
 * ```ts
 * classifyAuthorizeResult(false)       // { verdict: 'deny' }
 * classifyAuthorizeResult(true)        // { verdict: 'admit', member: undefined }
 * classifyAuthorizeResult({ id: 7 })   // { verdict: 'admit', member: { id: 7 } }
 * classifyAuthorizeResult(undefined)   // { verdict: 'invalid', type: 'undefined' }
 * ```
 */
export function classifyAuthorizeResult(result: unknown): AuthorizeVerdict {
    if (result === false) return { verdict: 'deny' }
    if (result === true) return { verdict: 'admit', member: undefined }
    if (isAdmittingObject(result)) return { verdict: 'admit', member: result }
    return { verdict: 'invalid', type: typeLabel(result) }
}

/**
 * Whether a value is an object that may stand for a member.
 *
 * It claims `object` and nothing more: the member's fields are checked
 * downstream, by `admitPresenceMember` (see {@link classifyAuthorizeResult}).
 *
 * @param value - The authorizer's result.
 * @returns `true` for a non-null, non-array, non-boxed-primitive object.
 */
function isAdmittingObject(value: unknown): value is object {
    if (typeof value !== 'object' || value === null) return false
    return objectLabel(value) === 'object'
}

/**
 * The label {@link typeLabel} gives an object it cannot inspect (#353).
 *
 * A constant, because nothing about such a value can be learned without
 * running the trap that just threw.
 */
const UNINSPECTABLE_LABEL = 'uninspectable object'

/**
 * The type label of an object: `'array'`, `'boxed <primitive>'`, `'object'`,
 * or {@link UNINSPECTABLE_LABEL} when inspecting it throws (#353).
 *
 * The ONE place the realtime classifiers inspect an object beyond `typeof`, so
 * {@link classifyAuthorizeResult} and {@link typeLabel} cannot disagree about
 * one. Both inspections here can throw: `Array.isArray` on a revoked Proxy, and
 * `instanceof` — which asks the handler for a prototype — on a revoked Proxy
 * or one whose `getPrototypeOf` trap throws. The value comes from application
 * code, and a classifier over untrusted input must answer rather than throw,
 * or its caller loses the named error it builds from the answer.
 *
 * @param value - Any object, including a Proxy.
 * @returns The label. Never throws.
 */
function objectLabel(value: object): string {
    try {
        if (Array.isArray(value)) return 'array'
        return boxedPrimitiveLabel(value) ?? 'object'
    } catch {
        // Not silent: the label IS the report. Every caller throws a named
        // error carrying it (`AuthorizeResultError`, `PresenceMemberIdError`)
        // or logs it in a WARN. The caught error is not kept — a trap the
        // application wrote may quote application data in its message.
        return UNINSPECTABLE_LABEL
    }
}

/**
 * The label for a boxed primitive (`new Boolean(false)`, `Object(1n)`…), or
 * `undefined` when the value is not one.
 *
 * Boxed primitives are refused because they are objects only by accident:
 * `new Boolean(false)` is truthy and has no member fields.
 *
 * @param value - Any value.
 * @returns `'boxed <primitive>'`, or `undefined`.
 */
function boxedPrimitiveLabel(value: unknown): string | undefined {
    if (value instanceof Boolean) return 'boxed boolean'
    if (value instanceof Number) return 'boxed number'
    if (value instanceof String) return 'boxed string'
    if (value instanceof BigInt) return 'boxed bigint'
    if (value instanceof Symbol) return 'boxed symbol'
    return undefined
}

/**
 * A log-safe label for a value's TYPE, never its content.
 *
 * Package-internal (not exported from `mod.ts`): it names the offending type in
 * the messages of the realtime errors that refuse an application value —
 * `AuthorizeResultError` (#347) and the presence member id check (#346) — so an
 * operator learns what shape arrived without the value itself, which is
 * application data, reaching a log.
 *
 * `typeof` with the three cases it gets wrong for this purpose split out:
 * `null` (not `'object'`), arrays and boxed primitives.
 *
 * **Total** (#353): an object it cannot inspect — a revoked Proxy, or one whose
 * `getPrototypeOf` trap throws — is `'uninspectable object'`, never a thrown
 * `TypeError`. It labels the value inside an error's constructor and a WARN,
 * where a throw would replace the named error with an anonymous one.
 *
 * @param value - Any value.
 * @returns `'undefined'`, `'null'`, `'number'`, `'string'`, `'array'`,
 *   `'symbol'`, `'boxed boolean'`, `'object'`, `'uninspectable object'`…
 *
 * @example
 * ```ts
 * typeLabel(undefined)          // 'undefined'
 * typeLabel([])                 // 'array'
 * typeLabel(new Boolean(false)) // 'boxed boolean'
 * ```
 */
export function typeLabel(value: unknown): string {
    if (value === null) return 'null'
    if (typeof value === 'object') return objectLabel(value)
    return typeof value
}

/**
 * An app authorizer for private/presence channels. Receives the connection's
 * **server-derived** identity (never a wire field).
 *
 * Return exactly one of three things (#347):
 *
 * - `false` to deny — `subscribe` answers `{ ok: false }`.
 * - `true` to allow. On a presence channel the member is then
 *   `{ id: connection.id }`.
 * - A {@link PresenceMember} object to allow a presence channel as that member.
 *   A private channel accepts one too: it runs the same member check, then
 *   discards the member (#357). Any other object — a Deno KV entry, a pg
 *   `QueryResult`, `{}`, a row — throws a member error on EITHER kind; on a
 *   private channel, answer with a boolean (`return entry.value !== null`).
 *
 * Anything else — `undefined` from a missing `return`, `null` or `undefined`
 * from an empty query, `0`, `''`, `'yes'`, `1`, an array — makes `subscribe`
 * throw `AuthorizeResultError`. Write `return row ? { id: row.id } : false`,
 * never the raw row: it throws `PresenceMemberShapeError` on either kind
 * (#350, #357), and an absent one throws `AuthorizeResultError`.
 * `?? false` closes the gap where a value may be absent.
 *
 * **What reaches the room is exactly `{ id, info? }`** (#350): the pair is
 * read once from your object, serialized once and parsed back, and only that
 * copy is stored and announced — any other own key, or an `info` that is not
 * a JSON object, throws `PresenceMemberShapeError` before anything is written.
 * The CONTENTS of `info` remain your declaration: `info: row` ships the whole
 * row to everyone in the room. Lockness guarantees the envelope only.
 *
 * @typeParam Identity - The app's identity shape.
 */
export type Authorizer<Identity = unknown> = (
    identity: Identity | null,
    channel: string,
) => AuthorizeResult | Promise<AuthorizeResult>
