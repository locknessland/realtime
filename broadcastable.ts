/**
 * @fileoverview The `Broadcastable` marker — a realtime-owned interface an
 * event implements to be forwarded to channels.
 *
 * Owned here (not in `@lockness/events`) and read via a structural guard, so
 * the events→broadcast bridge needs no static edge to `@lockness/events`.
 *
 * @module @lockness/realtime/broadcastable
 */

/**
 * An event that opts into broadcasting.
 *
 * `broadcastWith()` is the **only** data forwarded (security S2); when absent,
 * the bridge sends a minimal projection — never the whole event object.
 */
export interface Broadcastable {
    /** The channels this event broadcasts on. */
    broadcastOn(): string[]
    /** The wire event name (defaults to the dispatched event name). */
    broadcastAs?(): string
    /** The payload to forward — the ONLY data that leaves the server. */
    broadcastWith?(): Record<string, unknown>
}

/**
 * Whether a dispatched event value implements {@link Broadcastable}.
 *
 * @param value - The event instance (`onAny`'s `data`).
 * @returns `true` when it exposes a `broadcastOn()` method.
 */
export function isBroadcastable(value: unknown): value is Broadcastable {
    return typeof value === 'object' && value !== null &&
        typeof (value as Broadcastable).broadcastOn === 'function'
}
