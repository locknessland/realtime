/**
 * @fileoverview The WebSocket handler abstraction — `createWebSocketHandler`
 * over Hono's `upgradeWebSocket`, with the CSWSH origin guard and upgrade-time
 * identity resolution.
 *
 * `upgradeWebSocket` is obtained **once** here from `@lockness/hono/deno` (the
 * only sanctioned route — the main barrel exposes WS types only; hard rule #1).
 * The origin check and the event mapping are pure, testable pieces.
 *
 * @module @lockness/realtime/websocket
 */

import type { Context, MiddlewareHandler } from '@lockness/hono'
import { upgradeWebSocket } from '@lockness/hono/deno'
import type { WSEvents } from '@lockness/hono/network'
import { renderError } from '@lockness/contract'
import type { Connection, Socket, WebSocketHooks } from './types.ts'

/** Options for {@link createWebSocketHandler}. */
export interface WebSocketHandlerOptions<Identity = unknown> {
    /** The lifecycle hooks the connection drives. */
    hooks: WebSocketHooks<Identity>
    /** Extra allowed origins (exact origin triples, e.g. `https://app.example`). */
    origins?: readonly string[]
    /** The app origin for the same-origin default; falls back to `APP_URL`. */
    appUrl?: string
    /**
     * Resolve the connection's identity at the upgrade from a server-verified
     * credential (a session cookie, a verified token). Returns `null` for an
     * unauthenticated socket. **Never** read identity from a wire frame (S1).
     *
     * @param c - The upgrade request context.
     * @returns The verified identity, or `null`.
     */
    resolveIdentity?: (
        c: Context,
    ) => Identity | null | Promise<Identity | null>
    /**
     * The upgrade function (defaults to `@lockness/hono/deno`'s
     * `upgradeWebSocket`). Injected only by tests to drive the handler without
     * a live Deno socket.
     */
    upgrade?: typeof upgradeWebSocket
}

/**
 * The origin of a URL (scheme+host+port), or `null` if unparseable.
 *
 * @param url - A URL string.
 * @returns The origin triple, or `null`.
 */
export function originOf(url: string): string | null {
    try {
        return new URL(url).origin
    } catch {
        return null
    }
}

/** The resolved origin allow-list for a handler. */
export interface AllowedOrigins {
    /** Explicitly allowed exact origins. */
    allowed: Set<string>
    /** The same-origin default (from `appUrl`/`APP_URL`), or `null`. */
    sameOrigin: string | null
}

/**
 * Resolve the allow-list from options + `APP_URL`.
 *
 * @param options - The handler options.
 * @returns The allowed set and the same-origin default.
 */
export function resolveAllowedOrigins(
    options: Pick<WebSocketHandlerOptions, 'origins' | 'appUrl'>,
): AllowedOrigins {
    const allowed = new Set<string>()
    for (const entry of options.origins ?? []) {
        const origin = originOf(entry)
        if (origin) allowed.add(origin)
    }
    const appUrl = options.appUrl ?? Deno.env.get('APP_URL') ?? undefined
    const sameOrigin = appUrl ? originOf(appUrl) : null
    return { allowed, sameOrigin }
}

/**
 * Decide whether an upgrade's `Origin` is allowed (CSWSH control).
 *
 * Fail-closed: an absent header, an empty string, and a literal `null` origin
 * are rejected. Matching is on the exact origin triple — never substring, never
 * implicit wildcard.
 *
 * @param origin - The request `Origin` header value, or `null` when absent.
 * @param allow - The resolved allow-list.
 * @returns `true` when the origin may upgrade.
 */
export function checkOrigin(
    origin: string | null,
    allow: AllowedOrigins,
): boolean {
    if (origin === null || origin === '' || origin === 'null') return false
    if (allow.sameOrigin !== null && origin === allow.sameOrigin) return true
    return allow.allowed.has(origin)
}

/**
 * Wrap a live socket as a {@link Connection}.
 *
 * @param socket - The underlying socket (a Hono `WSContext` satisfies it).
 * @param id - The stable transport id. Must be unguessable and never reused
 *   across connections — see {@link Connection.id} for why that is a security
 *   property and not a style preference.
 * @param identity - The server-derived identity (or `null`).
 * @param metadata - Free-form connection metadata.
 * @returns The connection handed to hooks.
 */
export function makeConnection<Identity = unknown>(
    socket: Socket,
    id: string,
    identity: Identity | null,
    metadata: Record<string, unknown> = {},
): Connection<Identity> {
    return {
        id,
        identity,
        metadata: Object.freeze({ ...metadata }),
        send: (data) => socket.send(data),
        close: (code, reason) => socket.close(code, reason),
    }
}

/**
 * Build the Hono `WSEvents` object mapping the socket lifecycle to the hooks.
 *
 * A single {@link Connection} is created lazily on first event and reused. A
 * throw in `onOpen`/`onMessage` is routed to `onError`, never crashing the
 * connection.
 *
 * @param hooks - The lifecycle hooks.
 * @param identity - The upgrade-resolved identity.
 * @param metadata - Free-form connection metadata.
 * @returns A Hono `WSEvents` object.
 */
export function buildEvents<Identity = unknown>(
    hooks: WebSocketHooks<Identity>,
    identity: Identity | null,
    metadata: Record<string, unknown> = {},
): WSEvents {
    let connection: Connection<Identity> | undefined
    const connFor = (socket: Socket): Connection<Identity> => {
        connection ??= makeConnection(
            socket,
            crypto.randomUUID(),
            identity,
            metadata,
        )
        return connection
    }
    const reportError = async (
        conn: Connection<Identity>,
        error: unknown,
    ): Promise<void> => {
        if (hooks.onError) {
            await hooks.onError(conn, error)
        } else {
            // No app handler — never swallow silently (security logging).
            //
            // Rendered, not handed over as an object. This is the framework's
            // DEFAULT sink and the hottest of the four in this package:
            // `guard()` routes every throw from the application's `onOpen` and
            // `onMessage` here, which is exactly where DSN-bearing driver
            // failures are produced. The object form printed the message AND
            // the stack, neither passing through the DSN redaction.
            //
            // It also encodes, which matters because a CLIENT reaches this:
            // `decodeClientMessage` interpolates the frame's own `type` field
            // into `unknown frame type: ...`. Encoding at THIS sink rather
            // than at that throw site is deliberate — the thrown message also
            // goes to the app's `onError` hook, and escaping it there would
            // corrupt what the application sees in order to fix a problem
            // that exists only at the log. Measured: rendering here escapes an
            // injected newline and a U+202E override alike.
            console.error(
                `realtime: unhandled websocket error: ${renderError(error)}`,
            )
        }
    }
    const guard = async (
        conn: Connection<Identity>,
        fn: () => void | Promise<void>,
    ): Promise<void> => {
        try {
            await fn()
        } catch (error) {
            await reportError(conn, error)
        }
    }
    return {
        onOpen: (_evt, ws) => {
            const conn = connFor(ws)
            void guard(conn, () => hooks.onOpen?.(conn))
        },
        onMessage: (evt, ws) => {
            const conn = connFor(ws)
            void guard(conn, () => hooks.onMessage?.(conn, evt.data))
        },
        onClose: (evt, ws) => {
            const conn = connFor(ws)
            void hooks.onClose?.(conn, evt.code, evt.reason)
        },
        onError: (evt, ws) => {
            const conn = connFor(ws)
            // Preserve the real transport event as the cause rather than
            // fabricating a bare generic error — AND put its detail in the
            // message, because `renderError` renders `name: message` and
            // drops `cause` entirely. Without this the line above would read
            // `Error: websocket transport error` and carry no information at
            // all: a diagnostic regression the encoder would otherwise have
            // introduced right here. The cause stays for the app's `onError`
            // hook, which still receives the object untouched.
            const detail = 'message' in evt &&
                    typeof evt.message === 'string' && evt.message !== ''
                ? evt.message
                : evt.type
            void reportError(
                conn,
                new Error(`websocket transport error: ${detail}`, {
                    cause: evt,
                }),
            )
        },
    }
}

/**
 * Create a Hono handler that upgrades a request to a WebSocket, guarding the
 * origin and resolving the connection's identity first.
 *
 * @typeParam Identity - The app's identity shape.
 * @param options - The hooks, origin allow-list, and identity resolver.
 * @returns A Hono middleware handler returning the upgrade `Response` (or `403`).
 *
 * @example
 * ```ts
 * const handler = createWebSocketHandler({
 *     hooks: { onMessage: (c, data) => c.send(`echo: ${data}`) },
 *     resolveIdentity: (c) => c.get('user') ?? null,
 * })
 * app.get('/ws', handler)
 * ```
 */
export function createWebSocketHandler<Identity = unknown>(
    options: WebSocketHandlerOptions<Identity>,
): MiddlewareHandler {
    const allow = resolveAllowedOrigins(options)
    const upgradeFn = options.upgrade ?? upgradeWebSocket
    return async (c, next) => {
        if (!checkOrigin(c.req.header('origin') ?? null, allow)) {
            return c.text('Forbidden origin', 403)
        }
        const identity = options.resolveIdentity
            ? await options.resolveIdentity(c)
            : null
        const upgrade = upgradeFn(() => buildEvents(options.hooks, identity))
        return upgrade(c, next)
    }
}
