/**
 * @fileoverview #316's mutation battery — the ghost sweep's owned-entry parse.
 *
 * `#sweepInstance` splits an owned-set entry on the FIRST space
 * (`entry.indexOf(OWNED_SEP)`), and that choice is load-bearing: the entry is
 * `` `${channel} ${memberId}` ``, a channel cannot contain a space (#314) and a
 * member id deliberately may (#306). The round-trip holds only under
 * first-space parsing.
 *
 * #281's live sweep scenario executed that line on every pass and could not
 * observe it, because its fixture id was `2` — with a single space in the
 * entry, `indexOf` and `lastIndexOf` return the same index. #314's boundary
 * test states the invariant as a string property computed IN the test, so a
 * driver-side mutation never reaches it either. The fixture now carries a
 * two-space id, and this battery is what proves the difference is noticed.
 *
 * **Requires a live broker.** Without one the suite it mutates is `ignored`,
 * which the harness reads as green — so every row would report SURVIVED and the
 * file would announce a catastrophe that is really a missing service. It
 * refuses to start instead.
 *
 * ```bash
 * LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \
 *   deno run -A packages/realtime/tests/mutations/sweep_parse_316.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/sweep_parse_316
 */

import { type Mutation, runBattery } from '@mutations/harness.ts'
import { LIVE_BROKER } from '../../../redis/tests/live_broker.ts'

const DRIVER = new URL('../../drivers/redis.ts', import.meta.url)
const SUITES = [
    new URL('../redis_broker_integration.test.ts', import.meta.url).pathname,
]

if (!LIVE_BROKER) {
    console.error(
        'This battery needs a live broker. Without one the suite it mutates is ' +
            '`ignored`, the harness reads that as green, and every row reports ' +
            'SURVIVED — a false catastrophe.\n\n' +
            '  LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \\\n' +
            '    deno run -A packages/realtime/tests/mutations/sweep_parse_316.ts',
    )
    Deno.exit(2)
}

const MUTATIONS: Mutation[] = [
    {
        label: 'the owned-entry parse splits on the LAST space, not the first',
        file: DRIVER,
        edits: [[
            'const sep = entry.indexOf(OWNED_SEP)',
            'const sep = entry.lastIndexOf(OWNED_SEP)',
        ]],
        killedBy:
            'a crashed instance’s members are swept, and a LIVE peer’s are not',
    },
    {
        label: 'the empty-channel guard widens: `sep < 0` becomes `sep <= 0`',
        file: DRIVER,
        edits: [['if (sep < 0) continue', 'if (sep <= 0) continue']],
        killedBy:
            'a crashed instance’s members are swept, and a LIVE peer’s are not',
        expectSurvival:
            'Equivalent. The two forms differ only at `sep === 0` — an entry ' +
            'that BEGINS with a space, i.e. an empty channel name. ' +
            '`ChannelManager.subscribe` refuses that at the boundary (#314’s ' +
            '`#assertUsableChannel`, via `isValidName`), so no `addMember` can ' +
            'write such an entry. Recorded rather than dropped, for the same ' +
            'reason as the self-skip row in the #281 table: a guard unreachable ' +
            'from a valid input is the desired state, not a redundancy.',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#316 — the ghost sweep’s owned-entry parse',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
