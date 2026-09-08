/**
 * @fileoverview The control-plane replay window (#272) — the single home for
 * whether a control frame is fresh, whether it has already been seen, and what
 * makes two frames "the same frame".
 *
 * Extracted from `drivers/redis.ts` deliberately. The MAC is a pure function of
 * the wire and the secret; this is **mutable, time-dependent, per-instance
 * state with its own lifecycle** — a second reason to change inside a file
 * already owning pub/sub, the roster, the heartbeat, the ghost sweep and the
 * revocation index. A concrete class and **not** an interface: one
 * implementation exists and the second is hypothetical, so a port here would be
 * speculative generality.
 *
 * **It names no drop.** Every "this frame was refused, and here is why" message
 * belongs to the verifier's guard chain, so that vocabulary has one home rather
 * than two. The single WARN this class does raise is a capacity notice — the
 * store hit its cap — which is an operational fact about the store itself and
 * not a verdict about any frame. Nothing else can report it, because nothing
 * else can see it.
 *
 * **What this does and does not guarantee.** The store is per-process, so the
 * honest statement is *a frame is obeyed at most once per receiving process,
 * per window* — not "once per instance", which a restart falsifies. The
 * freshness window is what protects an instance holding no nonce for a frame:
 * one that restarted, or one that started after the frame was issued. The nonce
 * store only tightens that for instances which were up for the original.
 *
 * @module @lockness/realtime/control_replay_window
 */

/** The verdict for one admitted frame. */
export type AdmitVerdict = 'ok' | 'stale' | 'duplicate'

/** How a {@link ControlReplayWindow} is built. */
export interface ControlReplayWindowOptions {
    /**
     * How long after issue a frame may still be obeyed, in milliseconds. Also
     * how long a nonce is remembered — the two are the same number by
     * construction, never configured separately.
     */
    windowMs: number
    /**
     * The clock, in epoch milliseconds. **Required, not optional.** An optional
     * clock defaulting to `Date.now` gives production and tests two different
     * paths through the one seam this class exists to collapse; the driver
     * supplies the default exactly once.
     */
    now: () => number
    /**
     * The most nonces to remember at once. Reached only under a frame rate that
     * outruns the window; past it, every origin is guaranteed an equal share
     * (`maxEntries / origins`) and the oldest entry belonging to an origin ABOVE
     * its share is evicted.
     * @default 10000
     */
    maxEntries?: number
}

const DEFAULT_MAX_ENTRIES = 10_000

/**
 * How long before the at-cap warning may be raised again, in milliseconds.
 *
 * The latch used to be a plain boolean, so an operator saw the line once per
 * process however long the condition lasted — and "we hit the cap for ten
 * seconds during a deploy" and "we have been at the cap for a week" produced
 * exactly the same single line. Re-arming makes the duration visible without
 * logging per admission, which at the cap is by definition the hot path.
 */
const WARN_INTERVAL_MS = 60_000

/** One remembered frame. The origin is stored, never re-parsed out of the key. */
interface SeenEntry {
    /** The publishing instance's id — the per-origin share's subject. */
    origin: string
    /** The frame's own timestamp, in epoch milliseconds. */
    issuedAt: number
    /**
     * A monotonic admission counter — the entry's position in arrival order.
     *
     * Needed because eviction compares candidates ACROSS origins, and each
     * origin's entries live in their own map, so there is no single iteration
     * order to read the answer off. Arrival order, not `issuedAt`: the
     * freshness gate admits any `ts` within `±windowMs`, so timestamps are not
     * monotonic and ordering by them would evict a different entry than the one
     * actually held longest.
     */
    seq: number
}

/**
 * Remembers recently-seen control frames so a replayed one can be refused.
 *
 * @example
 * ```typescript
 * const seen = new ControlReplayWindow({ windowMs: 30_000, now: () => Date.now() })
 * seen.admit('instance-a', 'f3a9', Date.now()) // 'ok'
 * seen.admit('instance-a', 'f3a9', Date.now()) // 'duplicate'
 * ```
 */
export class ControlReplayWindow {
    readonly #windowMs: number
    readonly #now: () => number
    readonly #maxEntries: number
    /**
     * Insertion-ordered `key -> issuedAt`. A `Map` preserves insertion order,
     * so the oldest entry is always the first — which is what makes both
     * pruning and cap eviction a walk from the front rather than a sort.
     */
    readonly #seen = new Map<string, SeenEntry>()
    /**
     * Each origin's own entries, `origin -> key -> seq`, in arrival order.
     *
     * An index, not a duplicate store: it holds keys and sequence numbers, and
     * `#seen` remains the only home for the entries themselves. It exists so
     * eviction can ask each origin for its OLDEST entry in O(1) and compare
     * across origins in O(origins) — replacing a walk of `#seen` that was
     * O(maxEntries) on a store held by many small origins, and whose bound had
     * to abandon the very guarantee it was bounding.
     *
     * `inner.size` is also the origin's live count, so there is one structure
     * rather than a count that can drift from the entries it counts.
     *
     * Maintained in lockstep with `#seen` through {@link #forget}, the only
     * place an entry is removed. Per-origin FIFO holds because every removal —
     * prune or eviction — takes an origin's oldest: prune walks global arrival
     * order, which within one origin IS its arrival order.
     */
    readonly #byOrigin = new Map<string, Map<string, number>>()
    /** Monotonic admission counter; the source of each entry's `seq`. */
    #nextSeq = 0
    /**
     * When the at-cap warning was last raised.
     *
     * `-Infinity` rather than `0`, because the clock is injected: a test (or a
     * deterministic runtime) whose `now()` returns 0 would make a `0` sentinel
     * both "never warned" and "warned just now", and the branch would warn on
     * every admission — the per-admission spam the interval exists to prevent,
     * on the hot path by definition.
     */
    #warnedAt = Number.NEGATIVE_INFINITY

    /**
     * @param options - The window, the clock, and the entry cap.
     */
    constructor(options: ControlReplayWindowOptions) {
        this.#windowMs = options.windowMs
        this.#now = options.now
        this.#maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES
    }

    /**
     * Decide whether a control frame may be obeyed, and remember it if so.
     *
     * Called **only after** the frame's MAC has verified. Admitting an
     * unauthenticated frame would let anyone with bus PUBLISH write into this
     * store, trading one weakness for a worse one.
     *
     * The key is the **`(origin, nonce)` pair**, never the nonce alone. Two
     * instances are independent sources: keyed on the nonce by itself, one
     * sender's legitimate frame is dropped as another's duplicate — and since
     * publishing is fire-and-forget with no retry, that loss is silent and
     * permanent. Failing closed on the control plane is worse than the replay
     * this class prevents.
     *
     * @param origin - The publishing instance's id, from the MAC'd payload.
     * @param nonce - The frame's nonce, from the MAC'd payload.
     * @param issuedAt - The frame's timestamp, in epoch milliseconds.
     * @returns `'ok'` to obey, `'stale'` if outside the window, `'duplicate'`
     *   if already seen.
     */
    admit(origin: string, nonce: string, issuedAt: number): AdmitVerdict {
        const now = this.#now()
        this.#prune(now)

        // Both directions. A one-sided check would let anyone who can push a
        // clock forward extend their own replay window indefinitely.
        if (Math.abs(now - issuedAt) > this.#windowMs) return 'stale'

        const key = `${origin} ${nonce}`
        if (this.#seen.has(key)) return 'duplicate'

        this.#evictIfAtCap(now, origin)
        const seq = this.#nextSeq++
        this.#seen.set(key, { origin, issuedAt, seq })
        let owned = this.#byOrigin.get(origin)
        if (owned === undefined) {
            owned = new Map<string, number>()
            this.#byOrigin.set(origin, owned)
        }
        owned.set(key, seq)
        return 'ok'
    }

    /** How many nonces are currently remembered — for tests and diagnostics. */
    get size(): number {
        return this.#seen.size
    }

    /**
     * Drop every entry older than the window.
     *
     * Called at the head of {@link admit}, never on a timer: a timer is one
     * more thing that can be stopped, and the driver already reasons about
     * three. The property this delivers is that **no entry older than the
     * window is ever consulted** — not that none ever exists, which would be
     * false for an instance that is ingesting nothing.
     */
    #prune(now: number): void {
        for (const [key, entry] of this.#seen) {
            // The `break` is a BOUND on work, not a claim that everything after
            // this point is fresh. Entries are arrival-ordered but carry the
            // FRAME's timestamp, and the freshness gate admits any `ts` within
            // `±windowMs`, so two entries up to two windows apart in issue time
            // can arrive in either order. An older one can therefore sit behind
            // a younger one and survive this walk.
            //
            // That is retention, never a wrong verdict. A surviving entry can
            // only be matched by an identical `(origin, nonce)`, and both are
            // inside the MAC along with `ts` — so the only frame that can reach
            // it is a verbatim replay carrying that same old `ts`, which the
            // freshness gate above returns 'stale' for before the duplicate
            // lookup ever runs. Residue is bounded at roughly two windows, and
            // absolutely by the entry cap.
            if (now - entry.issuedAt <= this.#windowMs) break
            this.#forget(key, entry)
        }
    }

    /**
     * Remove one entry and decrement its origin's count — the single removal
     * path.
     *
     * Both callers (the prune walk and the cap eviction) go through here for
     * one reason: the ledger and the entry map must never disagree, and two
     * places that delete are two places that can forget to decrement.
     *
     * @param key - The `(origin, nonce)` map key.
     * @param entry - The record being removed, whose `origin` is decremented.
     */
    #forget(key: string, entry: SeenEntry): void {
        this.#seen.delete(key)
        const owned = this.#byOrigin.get(entry.origin)
        if (owned === undefined) return
        owned.delete(key)
        if (owned.size === 0) this.#byOrigin.delete(entry.origin)
    }

    /**
     * Make room at the cap, without letting one origin crowd the rest out.
     *
     * Every origin is guaranteed an equal share of the cap,
     * `maxEntries / origins`; what goes is the oldest entry belonging to an
     * origin ABOVE its share, so no instance can be driven to zero remembered
     * nonces while another is over its share.
     *
     * **The guarantee has exactly one hole, and it is stated rather than
     * implied.** When the cap divides evenly and nobody is over their share,
     * there is no over-user to charge. The origin asking for the slot pays
     * instead — unless it is publishing its FIRST frame and holds nothing to
     * charge, in which case the oldest entry goes and that may belong to an
     * origin sitting exactly at its share. One entry, once, per newly-arriving
     * origin at a perfectly-divided cap. `with every origin equal, eviction
     * falls to the globally oldest` pins that case, and an earlier version of
     * this comment claimed it could not happen while that test proved it does.
     *
     * **Why not plain drop-oldest.** That was the rule before #283. It lets a
     * busy instance shorten everyone's effective retention to
     * `maxEntries / total rate`, so an origin that publishes once per window can
     * have its nonce forgotten before the window is over — silently disarming
     * its replay protection while the store looks healthy.
     *
     * **Why not "evict from whichever origin holds the most".** That was the
     * first attempt and it was worse. Equalising COUNTS is not equalising
     * protection: an instance publishing ten times as much needs ten times the
     * slots to cover the same wall-clock window, so an equal count leaves the
     * busiest instance with the least coverage — moving the exposure to where
     * the frames actually are.
     *
     * **Why the share is computed, not a constant.** A fixed floor was tried
     * and had no derivation: 64 entries over a 30s window covers 2.1 frames per
     * second, while this feature's own worked example (a 5 000-client fleet
     * reconnecting across two presence channels) implies 33 per second per
     * instance — sixteen times more. `maxEntries / origins` IS the max-min fair
     * share; a constant could only ever lower it, and would put an underived
     * knee at whatever fleet size made it bind.
     *
     * **Why candidates come from `#byOrigin` rather than a walk of `#seen`.**
     * The obvious implementation scans `#seen` from the oldest for the first
     * evictable entry. On a store held by many small origins that walk is
     * O(maxEntries) per admission — measured at 47us at a 10 000 cap rising
     * linearly to 340us at 80 000 — and bounding the walk was worse than the
     * cost: past the bound it fell back to dropping the globally-oldest entry,
     * which by construction belongs to an origin the walk had just found to be
     * AT or under its share. The bound voided the guarantee for exactly the
     * origins the guarantee exists for. Asking each origin for its own oldest
     * makes the selection exact and O(origins).
     *
     * **Drop-oldest is unchanged for whoever is chosen.** The deliberate trade —
     * one forgotten in-window nonce, rather than refusing new entries and
     * failing the control plane closed — is what #272's risk table rates as the
     * lesser harm, and this does not revisit it.
     *
     * @param now - The current time, for the re-arming warning.
     * @param incoming - The origin about to be admitted. It pays for the
     *   exact-division case rather than a bystander, when it has an entry to
     *   pay with.
     */
    #evictIfAtCap(now: number, incoming: string): void {
        if (this.#seen.size < this.#maxEntries) return

        if (now - this.#warnedAt >= WARN_INTERVAL_MS) {
            this.#warnedAt = now
            console.warn(
                `realtime: the control replay window is at its ${this.#maxEntries}-entry ` +
                    `cap across ${this.#byOrigin.size} origin(s) and is ` +
                    'evicting to make room. A frame older than the evicted ' +
                    'entry but still inside the window could be replayed once. ' +
                    'Lower the window or raise control.maxEntries.',
            )
        }

        const share = Math.floor(
            this.#maxEntries / Math.max(1, this.#byOrigin.size),
        )
        // One pass over ORIGINS — a fleet size — comparing each over-share
        // origin's oldest entry. `Map` preserves insertion order, so an
        // origin's oldest is its first key and needs no search.
        let victim: string | undefined
        let oldest = Number.POSITIVE_INFINITY
        for (const owned of this.#byOrigin.values()) {
            if (owned.size <= share) continue
            const head = owned.entries().next()
            if (!head.done && head.value[1] < oldest) {
                oldest = head.value[1]
                victim = head.value[0]
            }
        }
        if (victim !== undefined) {
            const entry = this.#seen.get(victim)
            if (entry !== undefined) {
                this.#forget(victim, entry)
                return
            }
        }
        // Every origin is exactly at its share — reachable only when the cap
        // divides evenly among them, since otherwise one must be above it.
        // Nobody is over-using anything, so the cost falls on the origin
        // ASKING for the slot: it is the one about to exceed its share, and
        // charging a bystander here is the one remaining way an origin under
        // its share loses an entry to someone else's traffic. That would make
        // the guarantee this class documents conditional, and a conditional
        // guarantee is the shape all three withdrawn rules failed in.
        const own = this.#byOrigin.get(incoming)
        const mine = own?.entries().next()
        if (mine !== undefined && !mine.done) {
            const entry = this.#seen.get(mine.value[0])
            if (entry !== undefined) {
                this.#forget(mine.value[0], entry)
                return
            }
        }
        // A first-ever frame from this origin: it holds nothing to charge, so
        // age decides.
        const first = this.#seen.entries().next()
        if (!first.done) this.#forget(first.value[0], first.value[1])
    }
}
