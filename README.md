# @lockness/realtime

Bidirectional real-time for Lockness — WebSockets and broadcasting over
authorized channels, with presence, on top of Hono's `upgradeWebSocket`.

```ts
import { createWebSocketHandler } from '@lockness/realtime'

app.get(
    '/ws',
    createWebSocketHandler({
        // Identity is resolved AT the upgrade from a verified credential —
        // never from a wire frame.
        resolveIdentity: (c) => c.get('user') ?? null,
        // Same-origin by default (from APP_URL); widen with `origins`.
        hooks: {
            onOpen: (conn) => console.log('open', conn.id, conn.identity),
            onMessage: (conn, data) => conn.send(`echo: ${data}`),
            onClose: (conn, code) => console.log('close', conn.id, code),
        },
    }),
)
```

## What ships

- **A WebSocket handler** over `upgradeWebSocket` with `onOpen` / `onMessage` /
  `onClose` / `onError` hooks, each receiving a typed `Connection` (a
  per-connection transport id that must be **unguessable and never reused**,
  `send`, `close`, a server-derived `identity`, free-form `metadata`). The
  handler generates that id for you; supply your own only with a fresh
  `crypto.randomUUID()`, never a user or session id — `evict` names it in a
  frame that crosses the bus.
- **A CSWSH origin guard** — fail-closed, exact origin triple, same-origin by
  default from `APP_URL`.
- **Channels** — public / private / presence, with an app authorizer, backed by
  a memory or Redis driver. The Redis driver runs across instances: broadcasts
  fan out over pub/sub, the presence `here` roster is authoritative in Redis,
  and `manager.evict(id)` revokes a connection wherever its socket lives. Build
  it with `RedisBroadcastDriver.fromConfig(config, { control: { secret } })`,
  where `config` is a `RedisBroadcastConnectionConfig` — a Redis client config
  plus the subscribe socket's liveness and retry cadences (`keepaliveMs`,
  `livenessMs`, `retryBaseMs`, `retryMaxMs`; see
  [`@lockness/redis`'s README](../redis/README.md) for the defaults and the
  constraints between them). The subscribe socket keeps itself alive on an idle
  bus and retries a failed re-dial indefinitely rather than going deaf — the
  control plane and presence-identity frames are HMAC-authenticated **and
  replay-protected** (a timestamp and nonce inside the signed payload; stale or
  repeated frames are refused), with a configurable payload ceiling. The
  reserved `prefix` bounds **outbound** routing — no deployment receives
  another's frames, nested prefixes included
  ([#288](https://github.com/locknessland/lockness-monorepo/issues/288)) — and
  is **not** an inbound boundary: anything on the broker can publish into, and
  read from, these topics and keys, so use Redis ACLs for that. A prefix must
  match `[A-Za-z0-9:._-]{1,64}`, must not contain `__` — the lead-in every
  reserved separator begins with — and must not **end** with `_`, which would
  put it inside the `~<prefix>__*` ACL grant of the deployment one character
  shorter; the `RedisBroadcastDriverOptions.prefix` docstring is the single home
  for the full statement, and it lists all five refusals. **The Redis driver
  requires Redis 7.0+**; the memory driver has no such floor. Note the control
  wire format changed: during a rolling upgrade, control frames do not cross
  between old and new instances — see
  [docs/realtime.md](../../docs/realtime.md#control-plane-replay-protection).
- **Names that cross the control plane are bounded at the boundary** — three of
  them, and the bounds are deliberately not the same. A `Connection.id` must
  match `[A-Za-z0-9:._-]`, at most 200 characters; `register`, `subscribe` and
  `evict` throw `ConnectionIdError` otherwise, because a frame naming an id
  outside that charset is dropped by every _other_ instance and the failure was
  previously silent and partial. A `PresenceMember.id` — the value your
  `authorize()` returns — is bounded only by **length** (1–200 characters, and a
  numeric id must be finite), raising `PresenceMemberIdError`: it is your users'
  identity, so an email or a username has to keep working, and the charset would
  buy nothing that length-prefixed commands and MAC-signed frames do not
  already. A **channel name** must match the connection-id charset and raises
  `ChannelNameError`: it travels the control plane on a presence join, and it is
  the left half of the roster's `<channel> <member>` entries, which are parsed
  on the first space. All three error types are exported so an `onError` handler
  can separate a caller bug from a dead socket.
- **Durable revocation** — an evict outlives a lost pub/sub frame. A custom
  `BroadcastDriver` opts in by implementing `markRevoked(id)` and
  `listRevoked()`, plus `onRevocationReconcile(handler)` to say when the
  re-check runs; all three are optional, and a driver that omits them gets
  fire-and-forget eviction. See [realtime.md](../../docs/realtime.md).
- **Watched-channel limits** — `ChannelLimitError` is raised when a subscribe
  would take the instance past `maxWatchedChannels` (default `1_000`) or the
  connection past `maxChannelsPerConnection` (default `100`). Both are options
  on `ChannelManagerOptions`, validated at construction, and a per-connection
  cap above the instance cap is refused there — one connection would otherwise
  consume the whole instance. A breach **mutates nothing**: no registration, no
  presence member, no broker subscription. Only a join that GROWS a set is
  charged, so a second client on a hosted channel is always admitted.
- **Anonymous hosting reservation** — `subscribe` runs no authorizer for a
  public channel, so a connection with no identity may cause a 0 → 1 hosted
  channel transition only below `anonymousHostingShare` (default `0.8`) of the
  instance cap. Without it about ten anonymous sockets deny new channel hosting
  to every connection on the instance. Set it to `1` if you authenticate nobody.
  A breach of the reserved share carries `scope: 'instance-anonymous'`; treat
  `scope` as an open set — it is typed `ChannelLimitScope`, an alias for
  `string` rather than a union, so an exhaustive `switch` cannot be written
  against it, and `CHANNEL_LIMIT_SCOPES` names the values this version raises —
  and never forward the error's message to a client — the numbers are
  properties.
- **Per-channel subscription** — `watchChannel(channel)` and
  `unwatchChannel(channel)`, the fourth and fifth optional seams and the only
  pair detected **together**. A driver that implements both receives one exact
  topic per channel this instance hosts instead of every channel under its
  prefix; one that implements neither — or only one of them — keeps the
  prefix-wide subscription, because a subscribed set that grows and never
  shrinks is worse than the glob it would replace and invisible, since delivery
  stays correct. Both return `void | Promise<void>`, and an awaited
  `watchChannel` resolves when the subscribe frame is on the wire — never that
  delivery has started. See [realtime.md](../../docs/realtime.md).
- **Refusal reporting** — the sixth optional seam. `onControlRefused(handler)`
  hands you a `ControlRefusal` (`reason`, `kind`, `channel`, `bytes`, `limit`)
  whenever the driver declines to publish a control frame, so an oversized
  presence member is something you can alert on rather than a WARN on one
  instance. See [realtime.md](../../docs/realtime.md).
- **A broadcaster** that satisfies `@lockness/notification`'s `BroadcasterLike`
  — real-time is a drop-in notifications broadcast transport.
- **A JSON wire protocol** + an optional browser client helper.
- **An events bridge** — an event that implements `broadcastOn()` is forwarded
  to those channels (`@lockness/events` soft-loaded).

See [docs/realtime.md](../../docs/realtime.md) for the full guide.

## Testing against a real Redis

The unit suite is hermetic. The cross-process behaviours — presence, eviction
and durable revocation — are additionally covered against a **live broker**,
because an in-process fake can model Redis's `EXPIRE` and `ZADD` option flags
wrongly and stay green. That suite is skipped unless you ask for it:

```bash
docker run -d --rm --name lockness-it-redis -p 63790:6379 redis:7-alpine
LOCKNESS_REDIS_PORT=63790 deno task test:redis
docker stop lockness-it-redis
```

Each run owns its own key namespace and cleans up after itself, including after
a failure. See [docs/testing.md](../../docs/testing.md) for the full env-var
contract and what the suite refuses to do.

**Mutation batteries.** This package carries 11 of the repo's 16 — executables
that break a source file on purpose and check that the suites notice.
`deno
test` does not run them; `deno task mutate` does, one at a time:

```bash
deno task mutate realtime   # this package's 11
deno task mutate            # all 16, as nightly CI runs them
```

A single battery still runs directly while you are writing one:

```bash
deno run -A packages/realtime/tests/mutations/prefix_288.ts
```

Three of them mutate code whose suite needs the live broker and **refuse to
start** without one, because an `ignored` suite reads as green and would turn
every row into a false survival. The convention — `killedBy` attribution,
`expectSurvival` for a recorded equivalent mutant, and when a battery is worth
writing at all — is in
[docs/testing.md](../../docs/testing.md#mutation-batteries).
