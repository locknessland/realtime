/**
 * @fileoverview Public surface of `@lockness/realtime` — WebSockets +
 * broadcasting over authorized channels.
 *
 * `createWebSocketHandler` upgrades a request (guarding the origin, resolving a
 * server-derived identity) and drives lifecycle hooks over a typed
 * `Connection`. Channels, drivers, the wire protocol and the events bridge are
 * added by the epic's later children.
 *
 * @module @lockness/realtime
 *
 * @example
 * ```ts
 * import { createWebSocketHandler } from '@lockness/realtime'
 *
 * app.get('/ws', createWebSocketHandler({
 *     hooks: { onMessage: (conn, data) => conn.send(`echo: ${data}`) },
 *     resolveIdentity: (c) => c.get('user') ?? null,
 * }))
 * ```
 */

export type {
    Connection,
    RealtimeControlConfig,
    Socket,
    WebSocketHooks,
    WSContext,
    WSMessageReceive,
} from './types.ts'
export {
    createWebSocketHandler,
    type WebSocketHandlerOptions,
} from './websocket.ts'
export {
    type Authorizer,
    type AuthorizeResult,
    type ChannelKind,
    channelKind,
    type PresenceMember,
    type PresenceSnapshot,
} from './channel.ts'
export type {
    BroadcastDriver,
    BroadcastMessage,
    ChannelRevocation,
    ConnectionRevocation,
    ControlMessage,
    ControlRefusal,
    PresenceCapableDriver,
    Revocation,
    RevocationStoreDriver,
    RevocationTally,
    RosterDeparture,
    RosterHold,
    RosterRelease,
    RosterWindow,
} from './driver.ts'
// A VALUE, so it cannot ride the type-only re-export above: a driver author
// needs the number to honour the seam's input assert (#341).
export { MAX_ROSTER_READ_SELF_IDS } from './driver.ts'
export { MemoryBroadcastDriver } from './drivers/memory.ts'
export {
    type PassSample,
    type RedisBroadcastConnectionConfig,
    RedisBroadcastDriver,
    type RedisBroadcastDriverOptions,
    type RedisCommandClient,
    type RedisSubscriber,
} from './drivers/redis.ts'
export {
    // Both id errors are exported because a NAMED error type an application
    // cannot name is just an `Error`. #304 made `ConnectionIdError` named
    // precisely so a caller could tell "a bug in my own code that no retry
    // fixes" from "a dead socket" on the shared `onError` hook — which needs
    // `instanceof`, which needs this line. #306 adds the presence-member
    // sibling and closes the same gap for it. #347's `AuthorizeResultError`
    // is exported for the same reason: an authorizer returning a value
    // outside its contract is a bug no retry fixes. A lifecycle refusal that
    // reaches the shared `onError` hook needs `instanceof` too (#361, #370):
    // the three lifecycle refusals below tell a socket that is gone
    // (`ConnectionDisconnectedError`), an id held by a different object
    // (`ConnectionIdInUseError`) and a socket never registered
    // (`ConnectionNotRegisteredError`) apart.
    AuthorizeResultError,
    CHANNEL_LIMIT_SCOPES,
    ChannelLimitError,
    type ChannelLimitScope,
    ChannelManager,
    type ChannelManagerOptions,
    ChannelNameError,
    ConnectionDisconnectedError,
    ConnectionIdError,
    ConnectionIdInUseError,
    ConnectionNotRegisteredError,
    type DisconnectOutcome,
    type LeaveOutcome,
    MAX_CHANNELS_PER_CONNECTION,
    MAX_PRESENCE_MEMBER_BYTES,
    MAX_PRESENCE_SNAPSHOT_MEMBERS,
    MAX_WATCHED_CHANNELS,
    type OutboundFrame,
    RevocationScopeError,
    type RevokeChannelOutcome,
    type SubscribeResult,
} from './manager.ts'
export {
    PresenceMemberIdError,
    PresenceMemberShapeError,
    PresenceMemberSizeError,
} from './presence_member.ts'
export {
    type ClientMessage,
    decodeClientMessage,
    encodeServerMessage,
    isValidName,
    MAX_FRAME_BYTES,
    MAX_NAME_LENGTH,
    ProtocolError,
    type ServerMessage,
} from './protocol.ts'
export { type Broadcastable, isBroadcastable } from './broadcastable.ts'
export {
    type AnyEventPayload,
    type BroadcastBridgeOptions,
    type DispatcherLike,
    forwardEvent,
    startBroadcasting,
} from './events_bridge.ts'
