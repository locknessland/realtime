/**
 * @fileoverview #358's mutation battery — the ghost sweep reads a dead
 * instance's owned set in bounded `SSCAN` pages, one full iteration per pass.
 *
 * The remedy's decisions live in four homes of `drivers/redis.ts`:
 * `OWNED_SCAN_COUNT` (the page size), `#sweepOwned`'s page loop (the one
 * owned-set read, the fourth `#closing` check, a renewal ending the scan),
 * `decodeScanReply` with `SCAN_REPLY_REFUSED` (what a SCAN-family envelope
 * means, and the one message it throws), and `#sweepInstance`'s one log site
 * (the *unfinished* suffix on a `kept` end). Each row below drops one clause a
 * refactor could drop while the rest of the suite stays green.
 *
 * - M1 `SMEMBERS` restored for the owned set: the whole set in one reply (on
 *   a live broker #358 W2 also kills it, past the reply cap).
 * - M2a no `COUNT` on the scan. M2b a `COUNT` other than the constant.
 * - M3 the page loop runs once (`while (false)`).
 * - M4 an empty page ends the scan (an inserted `break`: `page` is
 *   block-scoped inside `do {}`, so the `while` condition cannot test it).
 * - M5 the read never advances: every `SSCAN` sends cursor `'0'` while the
 *   loop still tests the returned cursor — killed through the fake's scan
 *   call ceiling, which turns a scan that never ends into a rejection.
 * - M6 no `#closing` check before a page read.
 * - M7 a refused release no longer stops the scan.
 * - M8 the decoder reads a missing cursor as `'0'`.
 * - M9 a `kept` end logged like `completed` (the suffix dropped); #358 W6 (ii)
 *   is its second killer.
 * - M10 (S2) the decoder's message interpolates the cursor.
 * - M11 (S1) the decoder accepts a leading-zero cursor.
 * - M12 no `#closing` check before each release — #355 M12b's mutant, which
 *   #358 W4 also kills since the review: the release in flight completes and
 *   none of its page follows it.
 *
 * **Anchors.** `if (this.#closing) return 'closed'` appears twice at 12
 * spaces since #358 — before each page read and before each release — so M6
 * anchors on the check PLUS the page-read line, and M12 (like #355 M12b) on
 * the check plus the release. `} while (cursor !== '0')` also ends the
 * revocation pass's page loop since #359, so M3 anchors on the two sweep-only
 * lines above it. `killedBy` strings end in a space where a shorter witness
 * id is a prefix of a longer one (`W1 ` vs `W10` / `W11`).
 *
 * Every row was proven LIVE by the harness run that recorded it: the mutant
 * ran and turned its named witness red (`KILLED`, attributed).
 *
 * ```bash
 * deno task mutate sweep_paging_358
 * ```
 *
 * @module @lockness/realtime/tests/mutations/sweep_paging_358
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'

const REDIS = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../sweep_paging_358.test.ts', import.meta.url).pathname,
]

/** `#sweepOwned`'s page loop, from the cursor to the loop's end. */
const PAGE_LOOP = "        let cursor = '0'\n" +
    '        do {\n' +
    "            if (this.#closing) return 'closed'\n" +
    '            const page = decodeScanReply(\n' +
    '                await this.command.command(\n' +
    "                    'SSCAN',\n" +
    '                    this.ownedKey(deadId),\n' +
    '                    cursor,\n' +
    "                    'COUNT',\n" +
    '                    String(OWNED_SCAN_COUNT),\n' +
    '                ),\n' +
    '            )\n' +
    // Re-anchored for #360: the page increment lands inside the loop. M1's
    // replacement drops it, so the sweep also reports zero pages.
    '            if (this.#sweepPass) this.#sweepPass.pages++\n' +
    '            const end = await this.#sweepPage(deadId, page.items, count)\n' +
    "            if (end !== 'swept') return end\n" +
    '            cursor = page.cursor\n' +
    "        } while (cursor !== '0')\n"

const CLOSING_BEFORE_PAGE_READ =
    "            if (this.#closing) return 'closed'\n" +
    '            const page = decodeScanReply(\n'

const SCAN_ARGS = "                    'SSCAN',\n" +
    '                    this.ownedKey(deadId),\n' +
    '                    cursor,\n' +
    "                    'COUNT',\n" +
    '                    String(OWNED_SCAN_COUNT),\n'

const SWEEP_PAGE_CALL =
    '            const end = await this.#sweepPage(deadId, page.items, count)\n'

const MUTATIONS: Mutation[] = [
    {
        label: 'M1 — the owned set read whole with SMEMBERS again',
        file: REDIS,
        edits: [[
            PAGE_LOOP,
            '        const end = await this.#sweepPage(\n' +
            '            deadId,\n' +
            '            asArray(\n' +
            "                await this.command.command('SMEMBERS', this.ownedKey(deadId)),\n" +
            '            ) ?? [],\n' +
            '            count,\n' +
            '        )\n' +
            "        if (end !== 'swept') return end\n",
        ]],
        killedBy: '#358 W1 ',
    },
    {
        label: 'M2a — the page read sends no COUNT',
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'SSCAN',\n" +
            '                    this.ownedKey(deadId),\n' +
            '                    cursor,\n',
        ]],
        killedBy: '#358 W1 ',
    },
    {
        label: 'M2b — the page read sends a COUNT other than OWNED_SCAN_COUNT',
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'SSCAN',\n" +
            '                    this.ownedKey(deadId),\n' +
            '                    cursor,\n' +
            "                    'COUNT',\n" +
            '                    String(OWNED_SCAN_COUNT * 10),\n',
        ]],
        killedBy: '#358 W1 ',
    },
    {
        label: 'M3 — the page loop runs only once',
        file: REDIS,
        // Anchored on the sweep-only context since #359 (A5): the revocation
        // pass's page loop ends on the same natural `while` line, so the bare
        // line matches twice.
        edits: [[
            "            if (end !== 'swept') return end\n" +
            '            cursor = page.cursor\n' +
            "        } while (cursor !== '0')\n",
            "            if (end !== 'swept') return end\n" +
            '            cursor = page.cursor\n' +
            '        } while (false)\n',
        ]],
        killedBy: '#358 W1 ',
    },
    {
        label: 'M4 — an empty page ends the scan instead of cursor 0',
        file: REDIS,
        edits: [[
            SWEEP_PAGE_CALL,
            '            if (page.items.length === 0) break\n' +
            SWEEP_PAGE_CALL,
        ]],
        killedBy: '#358 W8 ',
    },
    {
        label: "M5 — every page read sends cursor '0': the scan never advances",
        file: REDIS,
        edits: [[
            SCAN_ARGS,
            "                    'SSCAN',\n" +
            '                    this.ownedKey(deadId),\n' +
            "                    '0',\n" +
            "                    'COUNT',\n" +
            '                    String(OWNED_SCAN_COUNT),\n',
        ]],
        killedBy: '#358 W7 ',
    },
    {
        label: 'M6 — no closing check before a page read',
        file: REDIS,
        edits: [[
            CLOSING_BEFORE_PAGE_READ,
            '            const page = decodeScanReply(\n',
        ]],
        killedBy: '#358 W10 ',
    },
    {
        label: 'M7 — a refused release goes on to the next page',
        file: REDIS,
        edits: [[
            "            if (end !== 'swept') return end\n" +
            '            cursor = page.cursor\n',
            "            if (end === 'closed') return end\n" +
            '            cursor = page.cursor\n',
        ]],
        killedBy: '#358 W11 ',
    },
    {
        label: "M8 — the decoder reads a missing cursor as '0'",
        file: REDIS,
        edits: [[
            '        const cursor = asBulk(parts[0])\n' +
            '        const items = asArray(parts[1])\n',
            "        const cursor = asBulk(parts[0]) ?? '0'\n" +
            '        const items = asArray(parts[1])\n',
        ]],
        killedBy: '#358 W9 ',
    },
    {
        label: 'M9 — a kept end logged like completed: the suffix dropped',
        file: REDIS,
        edits: [[
            "            const unfinished = end === 'kept' || end === 'closed'\n",
            "            const unfinished = end === 'closed'\n",
        ]],
        killedBy: '#358 W7 ',
    },
    {
        label: "M10 — the decoder's message carries the cursor",
        file: REDIS,
        edits: [[
            '    throw new Error(SCAN_REPLY_REFUSED)\n',
            '    throw new Error(\n' +
            '        `${SCAN_REPLY_REFUSED}: ${String(asBulk(asArray(reply)?.[0]))}`,\n' +
            '    )\n',
        ]],
        killedBy: '#358 W9 ',
    },
    {
        label: 'M11 — the decoder accepts a leading-zero cursor',
        file: REDIS,
        edits: [[
            'const SCAN_CURSOR = /^(0|[1-9][0-9]{0,19})$/\n',
            'const SCAN_CURSOR = /^[0-9]+$/\n',
        ]],
        killedBy: '#358 W9 ',
    },
    {
        label: 'M12 — no closing check before each release',
        file: REDIS,
        edits: [[
            "            if (this.#closing) return 'closed'\n" +
            '            const outcome = await this.#release(\n',
            '            const outcome = await this.#release(\n',
        ]],
        killedBy: '#358 W4 ',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#358 — the ghost sweep reads the owned set in bounded pages',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
