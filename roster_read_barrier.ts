/**
 * @fileoverview The per-channel authoritative-roster read barrier (#333) — the
 * single home for **when the driver is asked**, as opposed to what it answers.
 *
 * Every presence `subscribe` frame, re-joins included, ended in one
 * authoritative roster read whose reply is the whole room, cluster-wide, with
 * every member's `info`. #327 removed the frame's *write* cost and #329 decided
 * the framework ships no verb meter — but an application-side frame meter
 * bounds the frame **rate** and provably not the per-frame **byte** cost, since
 * a room can grow without the rate changing at all. That residue is this file.
 *
 * **Extracted rather than inlined**, and the partition is by responsibility.
 * `ChannelManager`'s one reason to change is the *membership* invariant;
 * "how often do we ask the driver" is a cost rule with its own lifecycle and
 * its own state. `ControlReplayWindow` is the precedent in this package: a
 * concrete class, not a port, because one implementation exists and a second is
 * hypothetical.
 *
 * ## The rule, and why it is the TRAILING edge
 *
 * At most one read per channel is in flight. A caller arriving while one is in
 * flight is answered by **a read issued after the current one settles** —
 * never by the one already running. That is a read barrier, not the
 * leading-edge single-flight the issue proposed, and the difference is
 * correctness rather than tuning.
 *
 * The invariant it preserves:
 *
 * > **Ask-time freshness.** The snapshot a caller receives was read from the
 * > driver at an instant no earlier than the moment that caller asked for it.
 *
 * That is exactly the property every caller had when each issued its own read,
 * which is what makes this unobservable through the public contract — no
 * option, no error type, no protocol change, and nothing exported from
 * `mod.ts`.
 *
 * **A leading-edge single-flight breaks it, visibly.** A first join commits its
 * roster write before reading, and the Redis client chains commands onto its
 * tail synchronously at call time (ADR 003 §2), so the write is enqueued ahead
 * of the read and a joiner always sees **itself** in its own `here`. Share a
 * read that was issued before a joiner's own write committed, and that joiner
 * subscribes to a room and is handed a roster it is not in. Clients render
 * `here` directly. The barrier buys that property back for one extra read per
 * burst — two rather than one for K concurrent callers — and that price is
 * correct.
 *
 * ## Self ids ride the shared read, in batches (#341)
 *
 * A bounded read returns a window, and a caller's own member may fall outside
 * it — so each read also fetches the members of the callers it serves. The
 * running read carries the id of the caller that issued it. Callers arriving
 * during it queue into **batches**, each a `Set` of `String(id)` and each
 * answered by one read:
 *
 * - a caller whose id is already in a queued batch **joins that batch without
 *   counting** — N frames from one member cost one id (S1), so a pipelined
 *   re-join storm from one socket still costs two reads, as under #333;
 * - a caller with **no id** (a superseded join) contributes nothing and joins
 *   the earliest queued batch;
 * - a new id joins the newest batch, and when that batch already holds the cap
 *   (`MAX_ROSTER_READ_SELF_IDS`) it opens the **next** batch instead — never
 *   rides a read without its id, which would silently drop its self.
 *
 * Batches are issued FIFO, each when its predecessor settles, so a storm of S
 * distinct members costs ⌈S / cap⌉ + 1 sequential reads, each bounded, with one
 * in flight — the accepted residue of the #341 plan. The cap bounds what one
 * read transfers; this file only decides how ids are grouped, never how many a
 * driver accepts.
 *
 * ## What it does not do
 *
 * It bounds reads per unit time. The bytes of one read are bounded by the
 * driver's `readRoster` (#341), never here. It refuses nothing, meters nothing,
 * and remembers no desired state — an in-flight promise is not a state to
 * reconcile, which is why ADR 003 §6's ban on coalescing roster **writes** does
 * not reach a read.
 *
 * @module @lockness/realtime/roster_read_barrier
 */

import { MAX_ROSTER_READ_SELF_IDS, type RosterWindow } from './driver.ts'

/**
 * The function a barrier calls when it decides a fresh read is owed.
 *
 * @param channel - The presence channel.
 * @param selfIds - The distinct member ids of the callers this read serves.
 */
export type RosterRead = (
    channel: string,
    selfIds: readonly string[],
) => RosterWindow | Promise<RosterWindow>

/**
 * One queued read: the ids it will fetch, and the promise every caller in it
 * shares. `ids` stays open until the read is issued, which happens only after
 * the read ahead of it settles.
 */
interface Batch {
    readonly ids: Set<string>
    readonly promise: Promise<RosterWindow>
}

/**
 * One channel's state. `queue` is FIFO; `pending` indexes every id in it, so a
 * repeated id finds its batch without a scan. Both exist only while a read is
 * in flight.
 */
interface Slot {
    running: Promise<RosterWindow>
    readonly queue: Batch[]
    readonly pending: Map<string, Batch>
}

/**
 * Collapses concurrent authoritative roster reads of one channel onto the
 * trailing edge, batching the callers' self ids.
 *
 * @example
 * ```ts
 * const barrier = new RosterReadBarrier((channel, selfIds) =>
 *     roster.readRoster(channel, 100, selfIds)
 * )
 * // Eight concurrent subscribes to one channel cost two reads, not eight.
 * const windows = await Promise.all(
 *     Array.from({ length: 8 }, (_, i) => barrier.snapshot('presence-room', i)),
 * )
 * ```
 */
export class RosterReadBarrier {
    readonly #read: RosterRead
    readonly #maxSelfIds: number
    /**
     * Keyed by channel, and **bounded by reads in flight, not by names ever
     * seen**. Every entry is deleted the moment its last read settles with
     * nothing queued behind it, so a client churning distinct presence names
     * cannot grow this map — which would make the remedy for #333 a second
     * instance of #334 in a different map.
     */
    readonly #slots = new Map<string, Slot>()

    /**
     * @param read - Issues one authoritative read. Takes a FUNCTION rather than
     *   the driver: the unit is then testable alone and cannot drift with
     *   `BroadcastDriver`'s eleven optional members, three of which are
     *   feature-detected by `typeof`.
     * @param maxSelfIds - The most distinct ids one read may carry. Defaults to
     *   the seam's `MAX_ROSTER_READ_SELF_IDS`; injectable so the overflow path
     *   is reachable in a unit test without a thousand callers.
     * @throws {RangeError} When `maxSelfIds` is not a positive integer — a
     *   batch that can hold no id would open a new read for every caller.
     */
    constructor(
        read: RosterRead,
        maxSelfIds: number = MAX_ROSTER_READ_SELF_IDS,
    ) {
        if (!Number.isInteger(maxSelfIds) || maxSelfIds < 1) {
            throw new RangeError(
                `RosterReadBarrier: maxSelfIds must be a positive integer, got ${maxSelfIds}`,
            )
        }
        this.#read = read
        this.#maxSelfIds = maxSelfIds
    }

    /**
     * How many channels currently have a read in flight.
     *
     * A test seam, on `ControlReplayWindow.size`'s precedent — and the one
     * assertion that distinguishes "shares reads" from "retains state". It
     * returns to zero once every burst settles.
     *
     * @returns The number of channels holding a slot.
     */
    get size(): number {
        return this.#slots.size
    }

    /**
     * The authoritative roster window for `channel`, sharing an in-flight read
     * where sharing cannot cost freshness.
     *
     * **The returned window is shared by every caller of one read**, and so
     * are its arrays and the `PresenceMember` objects in them. Callers that hand the list onward
     * must copy it; `ChannelManager.rosterSnapshot` spreads it for exactly that
     * reason, and that spread stopped being defensive the day this class
     * arrived. The members themselves are deliberately **not** cloned: a
     * per-caller deep copy restores the per-caller `O(room)` cost this class
     * exists to remove, in CPU instead of bytes. `PresenceMember` is a value
     * object and nothing can mutate one: it is deep-frozen where it is minted
     * (#354).
     *
     * @param channel - The presence channel to read the roster of.
     * @param selfId - The caller's member id, fetched by the read that answers
     *   it so the caller's own member survives a bounded window; `undefined`
     *   when the caller holds none.
     * @returns The window the driver reported.
     * @throws Whatever the read threw — propagated to **every** caller sharing
     *   it, never swallowed. The manager's own `#closingRead` stays the single
     *   decider of the local fallback.
     */
    snapshot(
        channel: string,
        selfId?: string | number,
    ): Promise<RosterWindow> {
        const slot = this.#slots.get(channel)
        if (!slot) {
            const running = this.#issue(
                channel,
                selfId === undefined ? [] : [String(selfId)],
            )
            this.#slots.set(channel, {
                running,
                queue: [],
                pending: new Map(),
            })
            this.#watch(channel, running)
            return running
        }
        return this.#batchFor(channel, slot, selfId).promise
    }

    /**
     * The queued batch that answers a caller arriving during a read.
     *
     * Everyone who arrives during one read shares the queued reads, not one
     * each — otherwise K callers queue K reads and the bound is a delay rather
     * than a bound. A new batch is opened only when there is none, or when a
     * NEW id would take the newest past the cap.
     */
    #batchFor(
        channel: string,
        slot: Slot,
        selfId: string | number | undefined,
    ): Batch {
        if (selfId === undefined) {
            return slot.queue[0] ?? this.#open(channel, slot)
        }
        const id = String(selfId)
        const queued = slot.pending.get(id)
        if (queued) return queued
        const newest = slot.queue.at(-1)
        const batch = newest && newest.ids.size < this.#maxSelfIds
            ? newest
            : this.#open(channel, slot)
        batch.ids.add(id)
        slot.pending.set(id, batch)
        return batch
    }

    /**
     * Open a batch at the tail, issued when the read ahead of it settles.
     *
     * THE TRAILING EDGE — and `#issue` on BOTH branches, because a
     * continuation that only runs on fulfilment strands every queued caller
     * forever the first time the driver rejects. That is the trap in this
     * shape.
     */
    #open(channel: string, slot: Slot): Batch {
        const ahead = slot.queue.at(-1)?.promise ?? slot.running
        const ids = new Set<string>()
        const promise = ahead.then(
            () => this.#issue(channel, [...ids]),
            () => this.#issue(channel, [...ids]),
        )
        const batch = { ids, promise }
        slot.queue.push(batch)
        return batch
    }

    /** Run the read, normalising a synchronous driver to a promise. */
    #issue(channel: string, selfIds: readonly string[]): Promise<RosterWindow> {
        try {
            return Promise.resolve(this.#read(channel, selfIds))
        } catch (error) {
            // A driver that throws synchronously must reject like one that
            // rejects, or the slot below is never installed and the map leaks.
            return Promise.reject(error)
        }
    }

    /**
     * Promote the head batch when `promise` settles, or give the channel's
     * entry back.
     *
     * A promoted batch's ids leave `pending` here: a caller arriving after its
     * read was issued must not join it, or it would be answered by a read
     * older than its ask.
     */
    #watch(channel: string, promise: Promise<RosterWindow>): void {
        const settled = () => {
            const slot = this.#slots.get(channel)
            // A slot replaced by a later burst is not ours to retire.
            if (!slot || slot.running !== promise) return
            const head = slot.queue.shift()
            if (!head) {
                this.#slots.delete(channel)
                return
            }
            for (const id of head.ids) slot.pending.delete(id)
            slot.running = head.promise
            this.#watch(channel, head.promise)
        }
        // Both arms: a rejected read must still release the channel, or one
        // driver fault makes that room unreadable for the life of the process.
        promise.then(settled, settled)
    }
}
