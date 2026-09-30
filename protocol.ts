/**
 * @fileoverview The JSON wire protocol — envelope types, name validation, and
 * a fail-safe codec.
 *
 * Decoding is defensive: an oversized frame, malformed JSON, an unknown type,
 * or an out-of-charset / oversized channel name raises a {@link ProtocolError}
 * the caller turns into an `error` frame — never a thrown connection crash
 * (FR-006/FR-006a/FR-009, security S3).
 *
 * @module @lockness/realtime/protocol
 */

import type { PresenceMember } from './channel.ts'

/** Client → server frames. */
export type ClientMessage =
    | { type: 'subscribe'; channel: string }
    | { type: 'unsubscribe'; channel: string }
    | { type: 'ping' }

/** Server → client frames. */
export type ServerMessage =
    | {
        type: 'subscribed'
        channel: string
        members?: PresenceMember[]
        /**
         * The roster size `members` was cut from (#339). Present from 0.4.0;
         * `members.length < total` means the snapshot is partial. Absent from
         * an older server, which sent the whole room.
         */
        total?: number
    }
    | { type: 'unsubscribed'; channel: string }
    | { type: 'event'; channel?: string; event: string; data: unknown }
    | {
        type: 'presence'
        channel: string
        action: 'here' | 'joined' | 'left'
        members?: PresenceMember[]
        /**
         * On a `here` frame: the roster size `members` was cut from (#339). A
         * snapshot-time number — `joined`/`left` frames never carry it.
         */
        total?: number
        member?: PresenceMember
    }
    | { type: 'error'; message: string }
    | { type: 'pong' }

/** Raised for any invalid inbound frame. The caller sends an `error`, not a crash. */
export class ProtocolError extends Error {
    /**
     * Build the refusal. `name` is set to `'ProtocolError'` for logs.
     *
     * @param message - What was wrong with the frame.
     */
    constructor(message: string) {
        super(message)
        this.name = 'ProtocolError'
    }
}

/** Max channel/event name length. */
export const MAX_NAME_LENGTH = 200
/** Default max inbound frame size in bytes. */
export const MAX_FRAME_BYTES = 16 * 1024
const NAME_RE = /^[A-Za-z0-9:._-]+$/

/**
 * Whether a channel/event name is within the allow-list charset and length.
 *
 * @param name - The name to validate.
 * @returns `true` when the name is safe to route and relay.
 */
export function isValidName(name: string): boolean {
    return typeof name === 'string' &&
        name.length > 0 &&
        name.length <= MAX_NAME_LENGTH &&
        NAME_RE.test(name)
}

/**
 * Whether a value may be a presence member id: a string, or a finite number
 * (#346).
 *
 * **One rule, three callers, and they must change together.** The join
 * boundary (`admitPresenceMember`'s id check, `presence_member.ts`), the Redis
 * frame ingest
 * (`isPlainMember`) and the Redis roster read (`#parseRosterValue`) all decide
 * the id's TYPE through this predicate. Before #346 the two receive-side sites
 * each carried their own copy and the join carried none, so a `null`, an
 * `undefined` or an object id joined locally — `String()` turned each into an
 * ordinary-looking key that two different people then shared — while every
 * peer dropped the frame announcing it. A sender that accepts what its
 * receivers refuse is a silent partial failure; one predicate makes that
 * disagreement unrepresentable.
 *
 * TYPE ONLY. #306's length bound stays at the join, where it protects the
 * roster write; applying it on the receive side would skip entries that still
 * count in a snapshot's `total` (#339). The one receive-side difference from
 * the copies it replaced: an overflowing literal such as `1e999` parses to
 * `Infinity`, which they accepted and this refuses — no join can produce one,
 * since the join boundary refuses a non-finite id first.
 *
 * Package-internal: exported from this module for its callers, NOT from
 * `mod.ts`.
 *
 * @param value - A candidate member id, straight from `authorize()` or a wire.
 * @returns `true` for a string or a finite number.
 *
 * @example
 * ```ts
 * isPresenceMemberIdValue('ada@example.com') // true
 * isPresenceMemberIdValue(1e21)              // true
 * isPresenceMemberIdValue(Number.NaN)        // false
 * isPresenceMemberIdValue(null)              // false
 * ```
 */
export function isPresenceMemberIdValue(
    value: unknown,
): value is string | number {
    if (typeof value === 'string') return true
    return typeof value === 'number' && Number.isFinite(value)
}

/**
 * Whether an own key may sit on a presence member: `id` or `info`, and
 * nothing else (#350).
 *
 * The key half of {@link isPresenceMemberWire}, extracted so the join's
 * admission (`admitPresenceMember`, which must NAME the offending keys, not
 * just answer no) and every receiver ask the one allow-list. Two copies of it
 * would drift the day a third key is allowed.
 *
 * Package-internal: exported from this module for its callers, NOT from
 * `mod.ts`.
 *
 * @param key - An own enumerable key of a candidate member.
 * @returns `true` for `id` and `info`.
 *
 * @example
 * ```ts
 * isPresenceMemberKey('info')  // true
 * isPresenceMemberKey('email') // false
 * ```
 */
export function isPresenceMemberKey(key: string): boolean {
    return key === 'id' || key === 'info'
}

/**
 * Whether a value may be a presence member's `info`: absent (`undefined`), or
 * a non-null, non-array object (#350).
 *
 * One of the two value halves of {@link isPresenceMemberWire}, extracted so
 * the Redis roster read (`#parseRosterValue`) asks a stored entry the same
 * `info` question the join and the frame ingest ask a member.
 *
 * Package-internal: exported from this module for its callers, NOT from
 * `mod.ts`.
 *
 * @param value - A candidate `info`, as parsed from JSON.
 * @returns `true` for `undefined` or a non-array object.
 *
 * @example
 * ```ts
 * isPresenceMemberInfoValue(undefined)       // true
 * isPresenceMemberInfoValue({ name: 'Ada' }) // true
 * isPresenceMemberInfoValue(null)            // false
 * isPresenceMemberInfoValue('1970-01-01')    // false
 * ```
 */
export function isPresenceMemberInfoValue(
    value: unknown,
): value is Record<string, unknown> | undefined {
    return value === undefined || isNonArrayObject(value)
}

/**
 * Whether a value is a non-null, non-array object — answering `false`, never
 * throwing, for a revoked Proxy (#353).
 *
 * `Array.isArray` throws on a revoked Proxy ("Cannot perform 'IsArray' on a
 * proxy that has been revoked"), and both wire predicates ran it on a value a
 * driver or an application supplied. A predicate that throws hands its caller
 * an anonymous `TypeError` in place of the refusal it would have reported —
 * the manager's departure handler, documented never to throw, threw.
 *
 * @param value - Any value.
 * @returns `true` for an inspectable non-array object.
 */
function isNonArrayObject(value: unknown): value is Record<string, unknown> {
    if (typeof value !== 'object' || value === null) return false
    try {
        return !Array.isArray(value)
    } catch {
        // Not silent: `false` is the refusal, and every caller reports it —
        // the join throws a named error, the departure handler and the Redis
        // frame ingest WARN, the Redis roster read skips with a reason. A
        // revoked Proxy is no member and no `info`.
        return false
    }
}

/**
 * Whether a value is exactly a presence member as a receiver sees it: a
 * non-null, non-array object whose EVERY own key passes
 * {@link isPresenceMemberKey} (`id` or `info`), whose `id`
 * passes {@link isPresenceMemberIdValue} and whose `info` passes
 * {@link isPresenceMemberInfoValue} (#348, made strict by #350).
 *
 * **One rule, and every caller runs it on the PARSED WIRE FORM**:
 *
 * - the join, inside `admitPresenceMember` (`presence_member.ts`), on
 *   `JSON.parse` of the `{ id, info }` pair it just serialized — so a member
 *   is admitted exactly when every peer admits the frame announcing it;
 * - the Redis control-frame ingest (`isPlainMember`, the pre-MAC guard on the
 *   one field an attacker can make arbitrarily large), on `JSON.parse` of the
 *   frame;
 * - the manager's departure handler, on a member a driver reports through
 *   `onRosterDeparture` — this instance must not show its own subscribers a
 *   member every peer refuses.
 *
 * The Redis roster read (`#parseRosterValue`) asks the id and `info` halves
 * only, NOT the key rule: it REDUCES a stored entry to `{ id, info }`, so a
 * legacy entry carrying extra keys is read back without them rather than
 * skipped — skipping would hide a 0.3.0 member for its whole session during a
 * rolling deploy.
 *
 * **The key rule is an allow-list, not a count** (#350). #348 bounded the
 * count at two, so `{ id, smuggled }` passed and every peer re-emitted
 * `smuggled`. And running this on an in-memory object is not enough: a
 * `Date` `info` passes there as an object and arrives as a string — which is
 * why the join runs it on the parsed copy, never on the authorizer's object.
 *
 * **Total** (#353): it answers `false`, and never throws, for a value it
 * cannot read — a revoked Proxy, or a live one whose `ownKeys` or `get` trap
 * throws. The departure handler hands it whatever a driver reported, and is
 * documented never to throw.
 *
 * Package-internal: exported from this module for its callers, NOT from
 * `mod.ts`.
 *
 * @param value - A candidate member, parsed off the wire or from a driver.
 * @returns `true` when every peer's ingest would admit it; `false` for
 *   anything else, a value whose reads throw included.
 *
 * @example
 * ```ts
 * isPresenceMemberWire({ id: 7, info: { name: 'Ada' } }) // true
 * isPresenceMemberWire({ id: 7 })                        // true
 * isPresenceMemberWire({ id: 7, info: [] })              // false
 * isPresenceMemberWire({ id: 7, smuggled: 1 })           // false
 * isPresenceMemberWire({ id: null })                     // false
 * ```
 */
export function isPresenceMemberWire(value: unknown): value is PresenceMember {
    if (!isNonArrayObject(value)) return false
    // The reads below run a LIVE Proxy's `ownKeys` and `get` traps, which
    // `isNonArrayObject` never does (#353 review) — so they sit in a try of
    // their own. No read is added: the key list, `id` and `info` are still
    // each read once, and the join only ever hands this parsed JSON (#350).
    try {
        if (!Object.keys(value).every(isPresenceMemberKey)) return false
        const member = value as { id?: unknown; info?: unknown }
        // The join boundary's own predicate (#346): a member is refused here
        // only for an id the sending instance would itself have refused at
        // `subscribe`.
        const idOk = isPresenceMemberIdValue(member.id)
        return idOk && isPresenceMemberInfoValue(member.info)
    } catch {
        // Not silent, for the reason `isNonArrayObject`'s catch is not: every
        // caller reports `false`. A member whose own traps throw is no member
        // — and the departure handler, documented never to throw, reached
        // here with a driver's value.
        return false
    }
}

/**
 * Encode a server frame for the wire.
 *
 * @param message - The frame to send.
 * @returns The JSON string.
 */
export function encodeServerMessage(message: ServerMessage): string {
    return JSON.stringify(message)
}

/**
 * Decode and validate an inbound client frame.
 *
 * @param raw - The raw frame (string or bytes).
 * @param maxBytes - The max accepted frame size (default {@link MAX_FRAME_BYTES}).
 * @returns The parsed, validated client message.
 * @throws {ProtocolError} On oversize, malformed JSON, unknown type, or an
 *   invalid channel name.
 */
export function decodeClientMessage(
    raw: string | ArrayBufferLike | Uint8Array,
    maxBytes: number = MAX_FRAME_BYTES,
): ClientMessage {
    const text = typeof raw === 'string'
        ? raw
        : new TextDecoder().decode(raw as ArrayBuffer)
    // Measure real UTF-8 bytes, not UTF-16 code units — a multi-byte frame must
    // not slip past the byte budget.
    if (new TextEncoder().encode(text).byteLength > maxBytes) {
        throw new ProtocolError('frame exceeds the maximum size')
    }

    let parsed: unknown
    try {
        parsed = JSON.parse(text)
    } catch {
        throw new ProtocolError('malformed JSON frame')
    }
    if (typeof parsed !== 'object' || parsed === null) {
        throw new ProtocolError('frame is not an object')
    }

    const msg = parsed as Record<string, unknown>
    switch (msg.type) {
        case 'subscribe':
        case 'unsubscribe': {
            if (typeof msg.channel !== 'string' || !isValidName(msg.channel)) {
                throw new ProtocolError('invalid channel name')
            }
            return { type: msg.type, channel: msg.channel }
        }
        case 'ping':
            return { type: 'ping' }
        default:
            throw new ProtocolError(`unknown frame type: ${String(msg.type)}`)
    }
}
