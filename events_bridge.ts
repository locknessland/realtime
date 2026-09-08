/**
 * @fileoverview The events → broadcast bridge — forwards `Broadcastable` events
 * to channels (FR-007/FR-007a/FR-007b).
 *
 * `@lockness/events` is **soft-loaded through a variable specifier** (a literal
 * dynamic import would harden the edge and fail `deps:analyze`), and its
 * dispatcher is typed by a **local structural interface** — no `import`/`import
 * type` from `@lockness/events`. The bridge forwards only `broadcastWith()`
 * (never the whole event, S2), warns on a public-channel target, and registers
 * with an `AbortSignal` so it can be torn down (no leaked global listener).
 *
 * @module @lockness/realtime/events_bridge
 */

import type { ChannelManager } from './manager.ts'
import { safeForLog } from '@lockness/contract'
import { channelKind } from './channel.ts'
import { type Broadcastable, isBroadcastable } from './broadcastable.ts'

/** The wildcard payload `@lockness/events`' dispatcher emits (structural). */
export interface AnyEventPayload {
    /** The dispatched event name. */
    event: string
    /** The event instance (a class event emits itself as `data`). */
    data: unknown
}

/** The minimal dispatcher surface the bridge uses (structural — no edge). */
export interface DispatcherLike {
    onAny(
        listener: (payload: AnyEventPayload) => void,
        options?: { signal?: AbortSignal },
    ): void
}

/** Options for {@link startBroadcasting}. */
export interface BroadcastBridgeOptions {
    /** Abort to detach the wildcard listener (teardown; test isolation). */
    signal?: AbortSignal
    /** Warn sink for a public-channel target (defaults to `console.warn`). */
    warn?: (message: string) => void
    /** Inject a dispatcher (tests); defaults to the soft-loaded one. */
    dispatcher?: DispatcherLike
}

/**
 * Soft-load `@lockness/events`' global dispatcher. Returns `null` when the
 * package is not installed.
 *
 * @returns The dispatcher, or `null`.
 */
async function loadDispatcher(): Promise<DispatcherLike | null> {
    // A VARIABLE specifier keeps the edge soft — a literal `import('@lockness/
    // events')` would be a static edge `deps:analyze` counts against `allow`.
    const specifier = '@lockness/events'
    try {
        const mod = await import(specifier) as {
            dispatcher?: () => DispatcherLike
        }
        return mod.dispatcher?.() ?? null
    } catch (error) {
        // CONTROL FLOW, NOT A LOG LINE. Do not convert this to `renderError` or
        // `safeForLog`, however much it looks like the sites #277 and #291
        // converted -- it matches the same grep and it is the opposite case.
        //
        // This string is never logged. It is substring-matched to decide
        // whether the optional `@lockness/events` package is simply absent (in
        // which case the soft edge stays soft and this returns null) or
        // something else went wrong (rethrow). `renderError` prefixes the error
        // name and truncates at 200 code points, so a resolver message long
        // enough to push `Module not found` past the boundary would stop
        // matching, and an absent optional dependency would start THROWING
        // instead of degrading. `safeForLog` is wrong here for the same reason.
        const message = error instanceof Error ? error.message : String(error)
        if (
            message.includes('Cannot resolve') ||
            message.includes('not a dependency') ||
            message.includes('not in import map') ||
            message.includes('Module not found')
        ) {
            return null
        }
        throw error
    }
}

/**
 * Forward one dispatched event to its broadcast channels, if it is
 * {@link Broadcastable}. Exported for direct testing.
 *
 * @param manager - The channel manager to broadcast through.
 * @param payload - The `onAny` payload (`{ event, data }`).
 * @param warn - Warn sink for a public-channel target.
 */
export function forwardEvent(
    manager: Pick<ChannelManager, 'broadcast'>,
    payload: AnyEventPayload,
    warn: (message: string) => void,
): void {
    const event = payload.data
    if (!isBroadcastable(event)) return

    const name = (event as Broadcastable).broadcastAs?.() ?? payload.event
    // Only broadcastWith() leaves the server; absent → minimal projection,
    // NEVER the whole event object (security S2).
    const data = (event as Broadcastable).broadcastWith?.() ?? {}

    for (const channel of event.broadcastOn()) {
        if (channelKind(channel) === 'public') {
            // Encoded HERE rather than at the sink, because `warn` is a
            // `(message: string) => void` — by the time it runs these values
            // are already inside one string and nothing downstream can tell
            // them apart. This site is reached through that helper rather than
            // a direct `console.*`, which is why a grep of this file for
            // `console.` finds only the default sink and not this.
            warn(
                `realtime: event "${safeForLog(name)}" broadcasts on public ` +
                    `channel "${safeForLog(channel)}" — it has no authorizer ` +
                    'and reaches every subscriber',
            )
        }
        manager.broadcast(channel, name, data)
    }
}

/**
 * Start forwarding `Broadcastable` events to channels.
 *
 * @param manager - The channel manager to broadcast through.
 * @param options - Signal, warn sink, and/or an injected dispatcher.
 * @returns `true` if the bridge started; `false` if `@lockness/events` is absent.
 *
 * @example
 * ```ts
 * const controller = new AbortController()
 * await startBroadcasting(manager, { signal: controller.signal })
 * // on shutdown: controller.abort()
 * ```
 */
export async function startBroadcasting(
    manager: Pick<ChannelManager, 'broadcast'>,
    options: BroadcastBridgeOptions = {},
): Promise<boolean> {
    const dispatcher = options.dispatcher ?? await loadDispatcher()
    if (!dispatcher) return false
    const warn = options.warn ?? ((m: string) => console.warn(m))
    dispatcher.onAny(
        (payload) => forwardEvent(manager, payload, warn),
        { signal: options.signal },
    )
    return true
}
