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

- **A Redis revocation timing the driver can enforce, or no boot**
  ([#362](https://github.com/locknessland/lockness-monorepo/issues/362)): the
  constructor refuses a `presence.reconcileIntervalMs` / `revocationTtlSeconds`
  pair it could not keep the lost-revocation bound with — see
  [the revocation timing](../../docs/realtime.md#revocation-timing) — and one
  WARN per episode says so when no revocation pass completes in time. Across the
  fleet, a record outlives the longest live TTL
  ([#380](https://github.com/locknessland/lockness-monorepo/issues/380), same
  link).
- **A WebSocket handler** over `upgradeWebSocket` with `onOpen` / `onMessage` /
  `onClose` / `onError` hooks, each receiving a typed `Connection` (a
  per-connection transport id that must be **unguessable and never reused**,
  `send`, `close`, a server-derived `identity`, free-form `metadata`). The
  handler generates that id for you; supply your own only with a fresh
  `crypto.randomUUID()`, never a user or session id — `evict` names it in a
  frame that crosses the bus. A throw or rejection from `onOpen`, `onMessage` or
  `onClose` is **reported, not fatal**: it reaches `onError`, or one default
  `console.error` line. That includes a close-path teardown failure re-thrown by
  `disconnect`. An `onError` that fails falls back to one line marked
  `(the onError hook failed too)` that names both errors
  ([#369](https://github.com/locknessland/lockness-monorepo/issues/369)).
- **A CSWSH origin guard** — fail-closed, exact origin triple, same-origin by
  default from `APP_URL`.
- **Channels** — public / private / presence, with an app authorizer, backed by
  a memory or Redis driver. The authorizer returns exactly `true`, `false` or a
  `PresenceMember`; anything else — `undefined`, `null`, `0`, a raw query row
  that was not found — throws `AuthorizeResultError` rather than admitting or
  quietly denying
  ([#347](https://github.com/locknessland/lockness-monorepo/issues/347)). An
  object admits only as a `PresenceMember`, on **every** channel kind: on a
  private channel a Deno KV entry, a pg `QueryResult`, `{}` or a raw row throws
  a member error, and a well-formed member is checked and then discarded —
  answer a private channel with a boolean
  ([#357](https://github.com/locknessland/lockness-monorepo/issues/357)). On a
  presence channel the room receives **exactly `{ id, info }`**, copied once at
  admission — never your object: any other own key, or an `info` that is not a
  JSON object once serialized (a function or a symbol included), throws
  `PresenceMemberShapeError`
  ([#350](https://github.com/locknessland/lockness-monorepo/issues/350)). What
  you put inside `info` is still yours to choose — `info: row` ships the row.
  What a `joined` frame promises — and what it does not — is in
  [docs/realtime.md](../../docs/realtime.md#what-a-joined-frame-promises--and-what-it-does-not).
  The Redis driver runs across instances: broadcasts fan out over pub/sub, the
  presence `here` roster is authoritative in Redis, and `manager.evict(id)`
  revokes a connection wherever its socket lives. Build it with
  `RedisBroadcastDriver.fromConfig(config, { control: { secret } })`, where
  `config` is a `RedisBroadcastConnectionConfig` — a Redis client config plus
  the subscribe socket's liveness and retry cadences (`keepaliveMs`,
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
  `authorize()` returns — must be **a string or a finite number** whose string
  form is 1–200 characters, raising `PresenceMemberIdError` otherwise. The type
  is checked first: a `null`, `undefined` or object id used to merge different
  users into one presence entry, since every consumer keys a member by
  `String(id)`, so deny when your user's id is absent and send a 64-bit key as a
  string. Its charset is deliberately free: it is your users' identity, so an
  email or a username has to keep working, and the charset would buy nothing
  that length-prefixed commands and MAC-signed frames do not already. A
  **channel name** must match the connection-id charset and raises
  `ChannelNameError`: it travels the control plane on a presence join, and it is
  the left half of the roster's `<channel> <member>` entries, which are parsed
  on the first space. All three error types are exported so an `onError` handler
  can separate a caller bug from a dead socket.
- **`PresenceMember` is bounded by SIZE too** — the whole serialized member,
  `info` included, must fit `maxPresenceMemberBytes` (default `4096`), or
  `subscribe` throws `PresenceMemberSizeError` at admission, before any local
  join, roster write or announcement exists. `info` is the field an end user
  typically controls through a profile edit, and without the bound an oversized
  one was written to the authoritative roster while the frame announcing it was
  dropped with a warning and `subscribe` still answered `{ ok: true }` — a
  member present in the room and invisible to every peer instance, arranged by
  pasting a long enough bio. The default is half the driver's control-payload
  ceiling so an admitted member can always be announced; **raise the two
  together or not at all**. Measured in bytes, not characters. See
  [realtime.md](../../docs/realtime.md).
- **Two revocation scopes, and the difference is the socket.**
  `manager.evict(id)` hard-closes it and drops every room;
  `revokeChannel(id,
  channel)` removes one room and leaves the socket open.
  Reach for the first when the identity is unwelcome, the second when a room is
  — using `evict` for a per-room action drops every other room the connection
  holds, and the bundled client implements no reconnect at all.
- **A disconnected connection is refused at admission.** Once `disconnect` has
  begun for a connection object, `register` and `subscribe` throw
  `ConnectionDisconnectedError` for it — before the authorizer where they can,
  and always before anything is written — so a subscribe racing the socket's
  close no longer strands a membership. A different object presenting an id
  another object holds — live or still being torn down — gets
  `ConnectionIdInUseError`. `subscribe` on an object `register` never bound
  throws `ConnectionNotRegisteredError`, before the authorizer; `handlerHooks`
  registers for you. Pass the registered object to `disconnect(conn)` from your
  close hook: it acts only for the object that owns its id. See items
  [17](../../docs/realtime.md#17-a-disconnected-connection-is-refused-at-admission),
  [20](../../docs/realtime.md#20-subscribe-requires-register) and
  [21](../../docs/realtime.md#21-an-id-held-by-a-live-connection-is-refused) of
  Upgrading to v0.4.0.
- **The local verbs report.** `unsubscribe` takes a connection id. `disconnect`
  takes the registered `Connection` object — the form above — or, still accepted
  but deprecated for application callers (#392), a bare id. All three act only
  on what this instance owns, and say which: `'left'` / `'not-subscribed'` /
  `'not-owned'`. `'not-owned'` means one of two things depending on the caller:
  for `unsubscribe` and `disconnect`'s id form, no socket registered under that
  id lives on this instance at all; for `disconnect`'s object form, the id may
  well be owned here, but not by the object passed — it is not (or is no longer)
  the one that owns it, so nothing local was touched. Server-side values — never
  relay them to a client.
- **Durable revocation** — a revoke outlives a lost pub/sub frame. A custom
  `BroadcastDriver` opts in by implementing `markRevocation(revocation)`,
  `listRevocations()` and `clearRevocation(channelRevocation)` — detected as a
  **set**, all three or none — plus `onRevocationReconcile(handler)` to say when
  the re-check runs. All three are optional — but "optional" means different
  things to the two verbs: a driver that omits them leaves `evict`
  fire-and-forget, and makes `revokeChannel` **refuse** with
  `RevocationScopeError` whenever the driver has a control plane, rather than
  degrade into an undurable revoke. A single-process driver (no control plane)
  owes no durability and is unaffected. **Every `revokeChannel` call is its own
  record**, identified by a manager-minted `id`, and `clearRevocation` removes
  exactly that id and never another record for the same connection and channel.
  The Redis member is `"<target> <channel> <id>"`. See
  [realtime.md](../../docs/realtime.md).
- **Watched-channel limits** — `ChannelLimitError` is raised when a subscribe
  would take the instance past `maxWatchedChannels` (default `1_000`) or the
  connection past `maxChannelsPerConnection` (default `100`). Both are options
  on `ChannelManagerOptions`, validated at construction, and a per-connection
  cap above the instance cap is refused there — one connection would otherwise
  consume the whole instance. A breach **mutates nothing**: no registration, no
  presence member, no broker subscription. Only a join that GROWS a set is
  charged, so a second client on a hosted channel is always admitted.
- **A re-join is a read, not a join** — a re-subscribe to a presence channel the
  connection already holds writes nothing, announces nothing locally or to other
  instances, and returns the same bounded `here` snapshot a first join returns,
  so a client re-subscribing after a network blip is never refused and cannot
  tell the difference. Its `member` payload is **discarded**: there is no
  "member updated" event, and `joined` is not one. **Zero writes is not zero
  cost**: it still performs one authoritative roster read per inbound frame —
  bounded to K members plus the own entries of the callers a shared read serves
  (at most `MAX_ROSTER_READ_SELF_IDS`), whatever the room's size
  ([#341](https://github.com/locknessland/lockness-monorepo/issues/341)). See
  [realtime.md](../../docs/realtime.md).
- **A presence subscribe returns a bounded snapshot** — `here.members` holds at
  most `maxPresenceSnapshotMembers` members — `MAX_PRESENCE_SNAPSHOT_MEMBERS`
  (100) by default — the joiner's own always among them, and `here.total` counts
  the room, so `members.length < total` means partial. `here.source` says
  whether it came from every instance or only this one. The reply is bounded,
  and so is the driver read behind it. It is a UI hint, not an access list:
  return one member id per identity from `authorize`, because join order decides
  who fills the window on the memory driver (Redis returns a random sample of a
  room larger than K, different per subscribe) and an authorizer returning
  `true` lets one identity take many slots. `members` and `rosterSource` were
  removed in 0.4.0.
- **Presence members are read-only** — every `PresenceMember` the framework
  hands out (in `here`, in the frames a custom `encode` receives, to a driver)
  is deep-frozen where it is minted and may be shared with other callers, so a
  write throws `TypeError`; `id` and `info` are `readonly`. `here` and
  `here.members` are yours. Copy before decorating:
  `{ ...m, info: { ...m.info, isYou: m.id === me } }` or `structuredClone(m)`
  ([#354](https://github.com/locknessland/lockness-monorepo/issues/354)).
- **Presence is announced per member, and a slot is held per instance** — a
  member's first connection anywhere sends one `joined`, its last one `left` (a
  crashed instance's members leave silently until
  [#348](https://github.com/locknessland/lockness-monorepo/issues/348)); a
  second tab, or closing one of two, sends nothing. On Redis every instance with
  a connection for a member holds its slot, and the slot leaves the roster only
  with its last holder, so no instance leaving or dying removes a member another
  still holds. Roster keys carry no TTL: run that Redis with `noeviction` or a
  `volatile-*` policy. See
  [The authoritative presence roster](../../docs/realtime.md#the-authoritative-presence-roster).
- **A custom presence driver implements `readRoster`, `holdMember` and
  `releaseMember`** — a bounded window with the population counted in the same
  read, and a per-process hold / release that reports `arrived` / `gone`, which
  the manager announces from and a driver may not fake. A driver that still has
  any pre-0.4.0 roster method is refused at construction, once, naming them. See
  [Writing a presence driver](../../docs/realtime.md#writing-a-presence-driver);
  the Redis scripts need nothing beyond the
  [Redis 7.0 minimum](../../docs/realtime.md#redis-minimum-version).
- **The framework does not meter the verb rate, and `authorize` is not where a
  budget goes** — the caps bound how many channels are held, never how often
  they are asked for, and the authorizer never runs for a public channel or for
  `unsubscribe` at all. A verb budget belongs in your own `onMessage`, keyed on
  a stable string derived from `connection.identity` (never on `connection.id`,
  which a reconnect rotates, and never on the `null` identity, which every
  anonymous socket shares), with a burst clearing
  `manager.maxChannelsPerConnection` so a reconnecting client is never refused.
  The per-frame cost table and the reasoning are in
  [realtime.md](../../docs/realtime.md).
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
- **Pass measurements** — the Redis driver's `onPassComplete(handler)` hands you
  one frozen `PassSample` per completed ghost sweep and revocation pass: its
  trigger, outcome, duration and pages, and how many units it attempted and how
  many failed (`attempts`, `failures`). Forward it to your metrics with
  [the OpenTelemetry recipe](../../docs/observability-and-crypto.md#framework-instruments).
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

**Mutation batteries.** This package carries 13 of the repo's 18 — executables
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
