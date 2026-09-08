/**
 * @fileoverview #310's mutation battery — the presence self-skip, made
 * reachable.
 *
 * Row 7 of #281's table (`packages/realtime/tests/redis_broker_integration.ts`
 * header) recorded the `id === this.instanceId` self-skip as an EQUIVALENT
 * mutant, and it was one against every scenario in that suite. #293 was filed
 * expecting to change that and moved it the other way: the configuration that
 * would let an instance's own liveness key lapse — a heartbeat at or above half
 * the TTL — is now refused at construction, and `start()` awaits one
 * `#heartbeat()` before arming the interval, so there is no boot window either.
 *
 * The path that still diverges is TRANSIENT rather than configural, and cannot
 * be reached by choosing options: the liveness `SET` failing for a window while
 * the instance is otherwise healthy and still serving sockets. `#heartbeat`
 * catches and logs at WARN, so nothing else stops. Without the self-skip the
 * instance then reads `EXISTS 0` on ITSELF and evicts its own connected users
 * from every presence channel it holds.
 *
 * `withFaultyInstance` injects exactly that fault and nothing else — every
 * command but `SET {prefix}__alive:*` still reaches the broker, which is what
 * separates "alive with a lapsed key" from "stopped".
 *
 * **Requires a live broker.** Without one the suite it mutates is `ignored`,
 * which the harness reads as green — so the row would report SURVIVED and
 * re-record row 7 as equivalent for the second time, on no evidence. It refuses
 * to start instead.
 *
 * ```bash
 * LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \
 *   deno run -A packages/realtime/tests/mutations/self_skip_310.ts
 * ```
 *
 * @module @lockness/realtime/tests/mutations/self_skip_310
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
            '`ignored`, the harness reads that as green, and the row reports ' +
            'SURVIVED — which would re-record row 7 as equivalent on no ' +
            'evidence, for the second time.\n\n' +
            '  LOCKNESS_REDIS_INTEGRATION=1 LOCKNESS_REDIS_PORT=<port> \\\n' +
            '    deno run -A packages/realtime/tests/mutations/self_skip_310.ts',
    )
    Deno.exit(2)
}

const MUTATIONS: Mutation[] = [
    {
        label: 'the `id === this.instanceId` self-skip is removed',
        file: DRIVER,
        edits: [[
            'if (!id || id === this.instanceId) continue',
            'if (!id) continue',
        ]],
        killedBy: 'an instance whose OWN liveness key lapses does not sweep',
    },
]

if (import.meta.main) {
    Deno.exit(
        await runBattery(
                '#310 — the presence self-skip',
                SUITES,
                MUTATIONS,
            ) > 0
            ? 1
            : 0,
    )
}
