/**
 * @fileoverview What a presence member may be, and the one way an
 * authorizer's object becomes one: {@link admitPresenceMember} (#350).
 *
 * The rule "what a presence member may be" changed under #306 (id length),
 * #326 (size), #346 (id type) and #350 (shape), and each time it was an edit
 * to the orchestrator. It lives here, beside the three errors that refuse a
 * member, so the next change to it has one home.
 *
 * **Admission PARSES the member instead of checking it in place.** The
 * authorizer's object is untrusted application input: a getter or a Proxy can
 * answer a second read differently from the first, and a `toJSON` decides what
 * a serialization ships. So `id` and `info` are each read exactly once, the
 * pair `{ id, info }` is serialized once, and those bytes are parsed back. The
 * parsed copy is checked with `isPresenceMemberWire` — the same predicate
 * every peer's frame ingest runs, on the same representation — and it is the
 * only member the manager ever stores, holds, snapshots or announces.
 *
 * **That member is an immutable value** (#354): {@link freezePresenceMember}
 * deep-freezes it where it is minted, so it can be shared by reference —
 * with every snapshot, every encoder and every caller of one roster read —
 * without any of them being able to change it for the others.
 *
 * @module @lockness/realtime/presence_member
 */

import { safeForLog } from '@lockness/contract'
import { type PresenceMember, typeLabel } from './channel.ts'
import {
    isPresenceMemberIdValue,
    isPresenceMemberKey,
    isPresenceMemberWire,
    MAX_NAME_LENGTH,
} from './protocol.ts'

/**
 * The sentence every member error ends with (#357).
 *
 * Since #357 an object result is admitted as a member on EVERY channel kind, so
 * these errors now reach applications whose private-channel authorizer returned
 * a lookup (a Deno KV entry, a pg `QueryResult`, a row) where it meant "allow".
 * On a private channel the member is discarded, and the fix there is almost
 * never to shape one — it is to return a boolean. The hint says so without
 * teaching {@link admitPresenceMember} anything about channels: the errors stay
 * kind-agnostic, and the sentence has one home.
 */
const PRIVATE_CHANNEL_HINT =
    'On a `private-*` channel no member is used: return `true` to admit (#357).'

/**
 * A presence member whose serialized form is over the byte ceiling, or cannot
 * be serialized at all (#326).
 *
 * `ChannelManager.subscribe` raises it on a private or a presence channel
 * (#357) when the pair `{ id, info }` read from the object `authorize()`
 * returned serializes past `maxPresenceMemberBytes`, or when `JSON.stringify`
 * throws on it (a cycle, a bigint, a throwing `toJSON` in `info`) — then the
 * size is reported as `Infinity`. It is raised after the id check and before
 * the caps and every write, so a refusal leaves nothing behind.
 *
 * @example
 * ```ts
 * import { PresenceMemberSizeError } from '@lockness/realtime'
 *
 * try {
 *     await manager.subscribe(connection, 'presence-room.1')
 * } catch (error) {
 *     if (error instanceof PresenceMemberSizeError) {
 *         log.error('authorize() returned an oversized presence info')
 *     }
 * }
 * ```
 */
export class PresenceMemberSizeError extends Error {
    /** Always `'PresenceMemberSizeError'`, for logs and `onError`. */
    override readonly name = 'PresenceMemberSizeError'

    /**
     * Build the refusal. The member's content is never part of the message.
     *
     * @param bytes - The serialized size that was refused, or `Infinity` when
     *   the member could not be serialized.
     * @param limit - The ceiling it exceeded.
     */
    constructor(bytes: number, limit: number) {
        super(
            `realtime: this presence member serializes to ${bytes} bytes, ` +
                `over the ${limit}-byte ceiling (#326). Its content is NOT ` +
                'echoed here — it is application data and this message reaches ' +
                'logs. The member is refused at admission, before any local ' +
                'join, roster write or announcement exists, because the roster ' +
                'write happens before the control publish: admitted, it would ' +
                'sit in the authoritative roster while the frame announcing it ' +
                'is dropped for being oversized, leaving a member present in ' +
                'the room and invisible to every other instance. Shrink ' +
                '`member.info`, or raise maxPresenceMemberBytes AND the ' +
                "driver's control.maxPayloadBytes together. " +
                PRIVATE_CHANNEL_HINT,
        )
    }
}

/**
 * A presence member id the roster and the control plane cannot carry (#306,
 * #346).
 *
 * `ChannelManager.subscribe` raises it on a private or a presence channel
 * (#357) when the object `authorize()` returned has an id that is not a string
 * or a finite number (#346), or whose string form is empty or over
 * {@link MAX_NAME_LENGTH} characters (#306). It is raised after the authorizer
 * result is classified (#347) and before the size check, the caps, and every
 * write and publish, so a refusal leaves nothing behind.
 *
 * **A throw, not `{ ok: false }`.** `{ ok: false }` means "not authorized"
 * (#331); a malformed id is a defect in the application's authorizer, and read
 * as a denial it would be one nobody could diagnose.
 *
 * **The message never echoes a non-primitive id.** A string or number id is
 * encoded through `safeForLog`; any other value is named by its type only
 * (`null`, `undefined`, `object`, `array`, `bigint`…). An object id may be a
 * whole user record — application data, and this message reaches logs (#326)
 * — and a template literal on a Symbol, or `String()` on a null-prototype
 * object, would itself throw.
 *
 * @example
 * ```ts
 * // An authorizer that cannot produce a malformed id: deny when it is absent.
 * const authorize = (user: User | null) =>
 *     user?.id == null ? false : { id: String(user.id) }
 * ```
 */
export class PresenceMemberIdError extends Error {
    /** Always `'PresenceMemberIdError'`, for logs and `onError`. */
    override readonly name = 'PresenceMemberIdError'

    /**
     * Build the refusal, naming the id without echoing application data.
     *
     * @param id - The offending id. A string or number is encoded before it
     *   reaches the message; any other value is named by its type only.
     */
    constructor(id: unknown) {
        super(
            `realtime: presence member id ${describeMemberId(id)} is ` +
                'unusable — it must be a string or a finite number whose ' +
                `string form is 1 to ${MAX_NAME_LENGTH} characters (#306, ` +
                '#346). Every presence consumer keys a member by `String(id)`, ' +
                'so a null, undefined or object id would merge different ' +
                'people into one entry that no peer instance accepts. The id ' +
                'also becomes a Redis hash field on the authoritative roster, ' +
                'and an oversized one is written there BEFORE the control ' +
                'frame that announces it is refused for size. Deny in ' +
                'authorize() when the id is absent, and send a 64-bit key as ' +
                `a string. ${PRIVATE_CHANNEL_HINT}`,
        )
    }
}

/**
 * A presence member that is not exactly `{ id, info? }` with a JSON-object
 * `info` (#350).
 *
 * `ChannelManager.subscribe` raises it on a private or a presence channel
 * (#357), before anything is written, published or delivered, when the object
 * `authorize()` returned:
 *
 * - has an own enumerable key other than `id` and `info` — a raw database row
 *   is the usual cause, and before #350 every one of its columns reached the
 *   room. On a private channel it is usually a lookup wrapper returned where
 *   a boolean was meant: a Deno KV `{ key, value, versionstamp }` or a pg
 *   `QueryResult`, which admitted there before #357 even when it found
 *   nothing; or
 * - has an `info` whose JSON form is not an object — `null`, an array, a
 *   `Date` (which serializes to a string), or a `toJSON` returning one. Every
 *   peer's ingest drops such a member, so admitted it would be visible on this
 *   instance and nowhere else; or
 * - has an `info` that serializes to NOTHING — a function, a symbol, or a
 *   `toJSON` returning `undefined`. `JSON.stringify` drops such a key without
 *   a word, so admitted the member would join as a bare `{ id }` and the
 *   application would never learn its `info` was lost.
 *
 * **Refused, never corrected** — the product default recorded on #350, in
 * line with #346 and #347: silently stripping a key would hide the
 * application's mistake, and a top-level `name` would vanish from the UI with
 * no error.
 *
 * **The message never echoes a value.** It names up to three offending key
 * names (log-encoded) and their count, or the `info`'s type label only — the
 * parsed `info`'s, or the supplied one's when it serialized to nothing.
 *
 * @example
 * ```ts
 * // Never the raw row: pick the id, and declare what the room may see.
 * const authorize = async (user: User | null) => {
 *     const row = user ? await findProfile(user.id) : undefined
 *     return row ? { id: row.id, info: { name: row.displayName } } : false
 * }
 * ```
 */
export class PresenceMemberShapeError extends Error {
    /** Always `'PresenceMemberShapeError'`, for logs and `onError`. */
    override readonly name = 'PresenceMemberShapeError'

    /**
     * Build the refusal from what was wrong — key NAMES or a type LABEL,
     * never a value.
     *
     * @param offence - `extraKeys`: the own keys found beside `id` and
     *   `info`, log-encoded here; `infoType`: the type label of the parsed
     *   `info`; or `droppedInfoType`: the type label of a supplied `info`
     *   that serialized to nothing.
     */
    constructor(
        offence:
            | { extraKeys: readonly string[] }
            | { infoType: string }
            | { droppedInfoType: string },
    ) {
        super(
            'extraKeys' in offence
                ? describeExtraKeys(offence.extraKeys)
                : 'infoType' in offence
                ? describeInfoType(offence.infoType)
                : describeDroppedInfo(offence.droppedInfoType),
        )
    }
}

/** How many offending key names a {@link PresenceMemberShapeError} names. */
const NAMED_KEYS = 3

/**
 * The message for a member carrying keys beside `id` and `info`.
 *
 * @param keys - The offending own keys.
 * @returns The log-safe message.
 */
function describeExtraKeys(keys: readonly string[]): string {
    const named = keys.slice(0, NAMED_KEYS).map((key) => `"${safeForLog(key)}"`)
    const more = keys.length > NAMED_KEYS
        ? `, and ${keys.length - NAMED_KEYS} more`
        : ''
    return `realtime: a presence member may carry only \`id\` and \`info\`, ` +
        `and this one has ${keys.length} other own ` +
        `key${keys.length === 1 ? '' : 's'}: ${named.join(', ')}${more} ` +
        '(#350). Their values are NOT echoed here — they are application ' +
        'data and this message reaches logs. Everything the member carries ' +
        'reaches every subscriber of the room, so a raw database row would ' +
        'ship every column; it is refused before anything is written. ' +
        'Return `{ id, info }` and declare in `info` what the room may see, ' +
        'e.g. `{ id: row.id, info: { name: row.displayName } }`. ' +
        PRIVATE_CHANNEL_HINT
}

/**
 * The message for a member whose `info` does not serialize to a JSON object.
 *
 * @param label - The parsed `info`'s type label.
 * @returns The log-safe message.
 */
function describeInfoType(label: string): string {
    return "realtime: a presence member's `info` must serialize to a JSON " +
        `object, and this one serializes to a value of type ${label} ` +
        '(#350). Every other instance drops a member whose `info` is not an ' +
        'object, so it would join here and be invisible everywhere else. A ' +
        '`Date` serializes to a string; wrap it, e.g. ' +
        '`info: { since: date.toISOString() }`, or omit `info`. ' +
        PRIVATE_CHANNEL_HINT
}

/**
 * The message for a member whose `info` was supplied and serialized to
 * nothing, so `JSON.stringify` dropped the key.
 *
 * @param label - The supplied `info`'s type label.
 * @returns The log-safe message.
 */
function describeDroppedInfo(label: string): string {
    return "realtime: a presence member's `info` must serialize to a JSON " +
        `object, and this one, a value of type ${label}, serializes to ` +
        'nothing (#350). JSON drops a function, a symbol, and anything whose ' +
        '`toJSON` returns `undefined`, so the member would join as a bare ' +
        '`{ id }` and its `info` would be lost without an error; it is ' +
        'refused instead. Declare a JSON object, or omit `info`. ' +
        PRIVATE_CHANNEL_HINT
}

/**
 * How a refused member id appears in {@link PresenceMemberIdError}'s message:
 * a string or number encoded through `safeForLog`, anything else by type only.
 *
 * @param id - The refused id.
 * @returns The log-safe rendering.
 */
function describeMemberId(id: unknown): string {
    if (typeof id === 'string' || typeof id === 'number') {
        return safeForLog(String(id))
    }
    return `of type ${typeLabel(id)}`
}

/**
 * Assert a presence member's id can cross the control plane and land in
 * the roster (#306, #346).
 *
 * **TYPE first (#346).** `PresenceMember.id` is typed `string | number`,
 * but the value comes from the application's `authorize()` — plain JS, a
 * cast, a nullable column or a parsed payload — and the type does not
 * reach it. Every presence consumer keys a member by `String(id)`, and
 * `String(null)`, `String(undefined)` and `String({})` are all ordinary,
 * short keys: two different people with such an id collapsed into ONE
 * presence entry, and on Redis every peer's ingest guard dropped the frame
 * the local join had just published. The rule is
 * {@link isPresenceMemberIdValue} — a string or a finite number — the SAME
 * predicate the Redis frame ingest and roster read apply, so this instance
 * can no longer accept what its peers refuse. It runs before `String(id)`,
 * which itself throws on a null-prototype object. Nothing is coerced: an id
 * is refused, never replaced (say, by `connection.id`), because a
 * replacement would hide the application's bug and give one user a
 * different id per tab.
 *
 * **Then LENGTH (#306), and the charset is deliberately NOT constrained.**
 * This value is application identity, not a framework-minted name: real
 * deployments key presence on an email, a username or an external
 * provider's id, and `isValidName`'s charset rejects `a@b.com` on the `@`.
 * Borrowing `Connection.id`'s charset here would break those
 * applications to buy nothing, because the three ways a hostile id could
 * hurt are all closed elsewhere:
 *
 * - Not command injection — `encodeCommand` emits length-prefixed RESP
 *   bulk strings, so a CRLF or a space in the value cannot forge a command.
 * - Not owned-set parser confusion — the entry is `<channel> <field>` and
 *   the parse is `indexOf(' ')`, which takes the FIRST space, so a field
 *   may contain spaces freely. That rests on the channel containing none —
 *   which #306 asserted and **nothing enforced**: `isValidName` ran only on
 *   the WebSocket wire, never on `subscribe`'s public path. #314 added
 *   `ChannelManager`'s `#assertUsableChannel`, and this claim now cites an
 *   enforcement point instead of a convention. (#306 originally credited the
 *   MEMBER id's charset, which does not exist at all.)
 * - Not frame forgery — control frames carry a MAC.
 *
 * What is NOT closed elsewhere is length. The id becomes a Redis hash field
 * on the authoritative roster, and the only two caps upstream of it are a
 * 10 MiB RESP frame and an 8 KiB control payload — neither a bound on this
 * value. Worse, the roster write happens BEFORE the control publish, and
 * the oversize check there only warns and returns: an oversized id is
 * already in the hash while the frame announcing it is silently dropped and
 * `subscribe` still answers `{ ok: true }`. Refusing at the boundary is
 * what keeps that from being a partial write. Length stays a join-time
 * rule: the receive side applies the type rule only.
 *
 * A NUMERIC id is length-checked through its string form. `String(1e21)`
 * is `"1e+21"`, whose `+` is outside `isValidName` — which is why a charset
 * predicate could not be applied to this type without rejecting a
 * legitimate large integer. Length has no such problem. Non-finite numbers
 * fail the type rule: `String(NaN)` is `"NaN"`, a perfectly
 * ordinary-looking field name that every NaN-identified member would
 * silently share.
 *
 * @param id - The member id to check — the ONE read of the candidate's `id`
 *   (#350), never the candidate itself.
 * @throws {PresenceMemberIdError} If the id is not a string or a finite
 *   number, or its string form is empty or over {@link MAX_NAME_LENGTH}
 *   characters.
 */
function assertUsableMemberId(id: unknown): asserts id is string | number {
    if (!isPresenceMemberIdValue(id)) throw new PresenceMemberIdError(id)
    const text = String(id)
    if (text.length > 0 && text.length <= MAX_NAME_LENGTH) return
    throw new PresenceMemberIdError(text)
}

/**
 * Deep-freeze a freshly minted presence member in place, and return it (#354).
 *
 * **Why frozen, not copied.** A member is shared by reference after it is
 * minted: the manager's local map, the memory driver's roster, every `here`
 * snapshot, the frames the application's `encode` receives, and — through the
 * #333 barrier — every caller sharing one roster read. Before #354 any of them
 * could write through that sharing into presence state or into another
 * caller's reply. Copying per caller would close it at a per-caller cost —
 * K members per subscribe frame, re-joins included — which is exactly what
 * the barrier and `rosterSnapshot` exist to refuse. Freezing costs one walk per
 * mint, and makes the sharing safe.
 *
 * **Called at the package's three mint sites and nowhere else**: the return of
 * {@link admitPresenceMember}, and on the Redis driver the roster decode
 * (`#parseRosterValue`) and the control-frame ingest (`#verifyAndDecode`).
 * Freezing at an exit instead would run per caller, and would freeze objects a
 * third-party driver still owns.
 *
 * **Why the walk is iterative.** A recursive freeze overflows the stack near
 * 16 000 levels, while `JSON.parse` accepts far deeper input. The default
 * bounds keep a member well above that, but an operator can raise them and a
 * roster entry at rest is bounded only by RESP, so the walk keeps an explicit
 * stack and cannot throw a `RangeError`.
 *
 * **Precondition: `member` is a fresh `JSON.parse` tree** — no cycle, no node
 * shared with anything else. That is why there is no `Object.isFrozen`
 * short-circuit (it is sound only for a node already known to be DEEPLY
 * frozen) and no visited set. Package-internal — not exported from `mod.ts`.
 *
 * @param member - A member just parsed from JSON.
 * @returns The same reference, with every object and array reachable through
 *   own keys frozen.
 *
 * @example
 * ```ts
 * const member = freezePresenceMember(JSON.parse(text))
 * Object.isFrozen(member.info) // true
 * ```
 */
export function freezePresenceMember(member: PresenceMember): PresenceMember {
    const pending: object[] = [member]
    for (let node = pending.pop(); node !== undefined; node = pending.pop()) {
        // `Object.keys` covers an array's indices too, so arrays are walked.
        for (const key of Object.keys(node)) {
            const child: unknown = (node as Record<string, unknown>)[key]
            if (typeof child === 'object' && child !== null) pending.push(child)
        }
        Object.freeze(node)
    }
    return member
}

/**
 * Turn the object an authorizer returned into the presence member the room
 * receives: exactly the JSON round trip of `{ id, info? }` (#350).
 *
 * The ONLY way a candidate becomes a {@link PresenceMember}. In order:
 *
 * 1. Its own enumerable keys must be a subset of `id` and `info`, or it
 *    throws {@link PresenceMemberShapeError} naming the extras (the product
 *    default: refuse, never strip).
 * 2. `id` is read ONCE, and checked: #346's type rule, #306's length rule.
 * 3. `info` is read ONCE.
 * 4. `{ id }` or `{ id, info }` is serialized once. A serializer throw (a
 *    cycle, a bigint, a throwing `toJSON`) is a
 *    {@link PresenceMemberSizeError} of `Infinity`; more than `maxBytes`
 *    UTF-8 bytes is a {@link PresenceMemberSizeError} (#326).
 * 5. Those bytes are parsed back and checked with `isPresenceMemberWire`,
 *    the predicate every peer's frame ingest runs on the same bytes. Only an
 *    `info` whose JSON form is not an object fails here — `null`, an array,
 *    a `Date`, a `toJSON` returning one — and it throws
 *    {@link PresenceMemberShapeError} naming the parsed `info`'s type.
 * 6. An `info` that was supplied (not `undefined`) but is absent from the
 *    parsed copy serialized to nothing — a function, a symbol, a `toJSON`
 *    returning `undefined` — and throws {@link PresenceMemberShapeError}
 *    naming the supplied `info`'s type. Refused, never admitted as `{ id }`.
 * 7. The parsed copy is deep-frozen by {@link freezePresenceMember} and
 *    returned (#354): it is shared by reference from here on, so nothing
 *    may write to it — not the manager, not a driver, not the application.
 *
 * **Why parse, not check in place.** A getter, a Proxy trap or a nested
 * `toJSON` can answer every later read differently from the one a check saw,
 * and every consumer downstream reads the member again — the roster field, the
 * local map, the snapshot, the control frame, its MAC, the local encoder. A
 * value validated and then re-read is not validated. The parsed copy is plain
 * JSON: nothing downstream can re-read it into something else, and the bytes
 * measured here are exactly the bytes every later serialization produces.
 *
 * Pure and synchronous: no I/O, and `subscribe` calls it before the caps and
 * every write (#323/#327/#347). Package-internal — not exported from `mod.ts`.
 *
 * What it does NOT decide: what the application puts INSIDE `info`.
 * `info: row` still ships the row; Lockness guarantees the envelope only.
 *
 * @param candidate - The object `authorize()` returned, on a private channel
 *   as on a presence one (#357) — or the framework's `{ id: connection.id }`
 *   for `true` on a presence channel. Untrusted; each field is read once.
 *   The function knows nothing of channels: a private channel discards what
 *   it returns.
 * @param maxBytes - The serialized-size ceiling, `maxPresenceMemberBytes`.
 * @returns A fresh, JSON-shaped, deep-frozen member sharing no reference
 *   with `candidate` (#354).
 * @throws {PresenceMemberShapeError} On an own key beside `id`/`info`, an
 *   `info` whose JSON form is not an object, or an `info` that serializes to
 *   nothing (a function, a symbol, a `toJSON` returning `undefined`).
 * @throws {PresenceMemberIdError} On an id that is not a string or a finite
 *   number, or whose string form is empty or too long.
 * @throws {PresenceMemberSizeError} On a pair over `maxBytes`, or one that
 *   cannot be serialized.
 *
 * @example
 * ```ts
 * admitPresenceMember({ id: 7, info: { name: 'Ada' } }, 4096)
 * // → { id: 7, info: { name: 'Ada' } }, a new, deep-frozen object
 * admitPresenceMember({ id: 7, email: 'a@b.c' }, 4096)
 * // throws PresenceMemberShapeError naming "email"
 * ```
 */
export function admitPresenceMember(
    candidate: object,
    maxBytes: number,
): PresenceMember {
    const extraKeys = Object.keys(candidate).filter((key) =>
        !isPresenceMemberKey(key)
    )
    if (extraKeys.length > 0) throw new PresenceMemberShapeError({ extraKeys })
    const fields = candidate as { id?: unknown; info?: unknown }
    const id = fields.id
    assertUsableMemberId(id)
    const info = fields.info
    let text: string
    try {
        text = JSON.stringify(info === undefined ? { id } : { id, info })
    } catch {
        // A cycle, a bigint or a throwing `toJSON` in application-supplied
        // `info`. Refused HERE, as a named error, rather than allowed to throw
        // out of a later serialization that runs after a write. `Infinity`
        // names it as unbounded rather than inventing a byte count. No
        // `cause`: the serializer's error can quote the value (a throwing
        // `toJSON` writes its own message), and this error reaches logs.
        throw new PresenceMemberSizeError(Infinity, maxBytes)
    }
    // BYTES, not string length: a ceiling compared against a payload limit
    // must be in the payload's own unit, and one emoji is four of them.
    const bytes = new TextEncoder().encode(text).length
    if (bytes > maxBytes) throw new PresenceMemberSizeError(bytes, maxBytes)
    const admitted: unknown = JSON.parse(text)
    if (!isPresenceMemberWire(admitted)) {
        throw new PresenceMemberShapeError({
            infoType: typeLabel((admitted as { info?: unknown }).info),
        })
    }
    // Only a function, a symbol or an object whose `toJSON` answered
    // `undefined` reaches here, so `typeof` is the whole label — and unlike
    // `typeLabel`'s `instanceof` checks it runs no Proxy trap: `info` is still
    // read exactly once.
    if (info !== undefined && admitted.info === undefined) {
        throw new PresenceMemberShapeError({ droppedInfoType: typeof info })
    }
    return freezePresenceMember(admitted)
}
