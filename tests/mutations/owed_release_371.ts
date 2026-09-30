/**
 * @fileoverview #371's mutation battery — the owed-release ledger is fed by
 * both catch sites that could otherwise only WARN, the drain re-issues each
 * queued slot exactly once through `#syncRosterMember`, sequentially, and the
 * ledger is bounded.
 *
 * Each row puts back one way the #371 remedy can be wrong:
 *
 * - M1 (**disables the remedy**): the driver never learns to call the drain
 *   at all — `onRosterMaintenance` is never registered. A queued release then
 *   sits forever with nothing to retry it.
 * - M2: `unsubscribe`'s release catch stops enqueueing — a release failure
 *   there is reported nowhere but the (removed) inline WARN it replaced.
 * - M3: the #323/#373 join compensation's reclaim catch stops enqueueing —
 *   the same residue #373 explicitly left for #371 to close.
 * - M4: a successful drain no longer clears its ledger entry — harmless to
 *   every OBSERVABLE frame, so a witness must count the release calls
 *   themselves, not merely the frames, to see it.
 * - M5: the cap is disabled — a slot past `MAX_PENDING_ROSTER_RELEASES` is
 *   queued and drained like any other, instead of refused and left to the
 *   ghost sweep.
 * - M6: the sequential walk becomes `Promise.all` — invisible to every
 *   fully-synchronous double, so the witness it dies on uses a driver whose
 *   `releaseMember` genuinely yields once before settling.
 *
 * Every row was proven LIVE before it was trusted: a marker was placed on the
 * row's mutated line and seen to execute under the killing witness.
 *
 * Runs under the shared harness: green baseline before anything is mutated, an
 * atomic per-file lock, anchors matched exactly once (a stale anchor reports
 * the row DEAD, never a silent survival), a non-compiling mutant reported DEAD,
 * and every kill attributed to the test that claims it.
 *
 * ```bash
 * deno run -A packages/realtime/tests/mutations/owed_release_371.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/owed_release_371
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const MANAGER = new URL('../../manager.ts', import.meta.url)
const SUITES = [
    new URL('../owed_release_371.test.ts', import.meta.url).pathname,
]

const W1 =
    '#371 W1 (AC) a release that rejects once — disconnect still rejects, ' +
    'and after the next drain the roster no longer holds the member, ' +
    'exactly one left, no extra joined'
const W2 =
    '#371 W2 (covers #373 residue) a join reclaim that itself fails is ' +
    'released by the drain, and the ORIGINAL roster error still wins'
const W3 = '#371 W3 the drain itself fails once — succeeds on the ' +
    'following tick, still exactly one left, no duplicate'
const W3C = '#371 W3c the drain issues releases sequentially, never ' +
    'concurrently'
const W5 = '#371 W5 the cap: enqueue past MAX_PENDING_ROSTER_RELEASES is ' +
    'refused and logged with the ghost-sweep wording, never silently dropped'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — disables the remedy: onRosterMaintenance never ' +
            'registered on the driver',
        file: MANAGER,
        edits: [[
            '            this.driver.onRosterMaintenance?.(() => this.#drainOwedReleases())\n',
            '            // Mutant: the hook is never registered — nothing ever drains\n' +
            '            // the owed-release ledger.\n',
        ]],
        // Witness: 'the drain released 7 after the (simulated) next
        // heartbeat' — the roster still holds 7 forever, with nothing to
        // invoke the (never-registered) handler `driver.drain()` calls.
        killedBy: W1,
    },
    {
        label: "M2 — unsubscribe's release catch stops enqueueing the " +
            'owed release',
        file: MANAGER,
        edits: [[
            '            } catch (error) {\n' +
            '                // QUEUED EITHER WAY (#371): a release this instance could not\n' +
            '                // commit is retried after every successful heartbeat, whether\n' +
            '                // or not the leave itself also failed.\n' +
            '                this.#recordOwedRelease(channel, { clientId, member }, error)\n' +
            "                // The leave's failure came first and is the one re-thrown\n" +
            "                // when it also failed; this release's own failure is only\n" +
            "                // ever reported through the ledger's WARN above.\n" +
            '                if (!outcome.failed) throw error\n' +
            '            }\n',
            '            } catch (error) {\n' +
            '                if (!outcome.failed) throw error\n' +
            '            }\n',
        ]],
        // Witness: 'the drain released 7 after the (simulated) next
        // heartbeat' — nothing was ever queued from this catch, so the drain
        // finds an empty ledger and the roster still holds 7.
        killedBy: W1,
    },
    {
        label: "M3 — the #323/#373 join compensation's reclaim catch " +
            'stops enqueueing the owed release',
        file: MANAGER,
        edits: [[
            '                try {\n' +
            '                    await this.#syncRosterMember(channel, origin)\n' +
            '                } catch (cleanupError) {\n' +
            '                    this.#recordOwedRelease(channel, origin, cleanupError)\n' +
            '                }\n',
            '                try {\n' +
            '                    await this.#syncRosterMember(channel, origin)\n' +
            '                } catch {\n' +
            '                    // Mutant: the reclaim failure is dropped, not queued.\n' +
            '                }\n',
        ]],
        // Witness: 'the drain finished the reclaim the compensation could
        // not' — with nothing queued, the drain leaves the committed-but-
        // lost hold in place.
        killedBy: W2,
    },
    {
        label: 'M4 — a successful drain does not clear its ledger entry',
        file: MANAGER,
        edits: [[
            '                await this.#syncRosterMember(channel, origin)\n' +
            "                // ONLY if nothing overwrote this slot's entry while the write\n" +
            '                // was in flight — the same guard `#syncRosterMember` uses for\n' +
            '                // its own tail, and for the same reason: a fresher failure\n' +
            '                // recorded during this await must survive the delete below.\n' +
            '                if (this.#owedReleases.get(key) === origin) {\n' +
            '                    this.#owedReleases.delete(key)\n' +
            '                }\n',
            '                await this.#syncRosterMember(channel, origin)\n' +
            '                // Mutant: the ledger entry is never cleared on success.\n',
        ]],
        // Witness: 'the ledger entry was actually cleared: a further drain
        // does not retry an already-settled slot' — the release-call count
        // keeps climbing on every further drain instead of staying flat.
        killedBy: W3,
    },
    {
        label: 'M5 — the pending-release cap is disabled',
        file: MANAGER,
        edits: [[
            '        const atCapacity = !this.#owedReleases.has(key) &&\n' +
            '            this.#owedReleases.size >= MAX_PENDING_ROSTER_RELEASES\n',
            '        const atCapacity = false // Mutant: the cap never refuses.\n',
        ]],
        // Witness: the cap test's own refusal-and-residue assertions — see
        // the battery's own W5 run for the exact wording; kept here as the
        // liveness attribution below.
        killedBy: W5,
    },
    {
        label: 'M6 — the sequential drain walk becomes Promise.all',
        file: MANAGER,
        // Re-anchored for #408: the channel decode moved behind
        // `#rosterSlotChannel` (one encode/decode pair for the roster slot
        // key), so the mutant's own body now calls the helper too rather
        // than re-inlining the NUL split.
        edits: [[
            '    async #drainOwedReleases(): Promise<void> {\n' +
            '        for (const [key, origin] of [...this.#owedReleases]) {\n' +
            '            const channel = this.#rosterSlotChannel(key)\n' +
            '            try {\n' +
            '                await this.#syncRosterMember(channel, origin)\n' +
            "                // ONLY if nothing overwrote this slot's entry while the write\n" +
            '                // was in flight — the same guard `#syncRosterMember` uses for\n' +
            '                // its own tail, and for the same reason: a fresher failure\n' +
            '                // recorded during this await must survive the delete below.\n' +
            '                if (this.#owedReleases.get(key) === origin) {\n' +
            '                    this.#owedReleases.delete(key)\n' +
            '                }\n' +
            '            } catch (error) {\n' +
            '                this.#recordOwedRelease(channel, origin, error)\n' +
            '            }\n' +
            '        }\n' +
            '    }\n',
            '    async #drainOwedReleases(): Promise<void> {\n' +
            '        await Promise.all([...this.#owedReleases].map(\n' +
            '            async ([key, origin]) => {\n' +
            '                const channel = this.#rosterSlotChannel(key)\n' +
            '                try {\n' +
            '                    await this.#syncRosterMember(channel, origin)\n' +
            '                    if (this.#owedReleases.get(key) === origin) {\n' +
            '                        this.#owedReleases.delete(key)\n' +
            '                    }\n' +
            '                } catch (error) {\n' +
            '                    this.#recordOwedRelease(channel, origin, error)\n' +
            '                }\n' +
            '            },\n' +
            '        ))\n' +
            '    }\n',
        ]],
        // Witness: 'never Promise.all: the drain issues one release at a
        // time' — the peak-concurrency gauge reads 2, not 1, once both
        // queued slots are drained at once.
        killedBy: W3C,
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#371 — the owed-release ledger is fed, drained once per ' +
                    'slot, sequentially, and bounded',
                SUITES,
                MUTATIONS,
            ) >
                0
            ? 1
            : 0,
    )
}
