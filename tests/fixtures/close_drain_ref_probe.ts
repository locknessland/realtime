/**
 * @fileoverview A standalone probe for #368 W8, spawned as its own process by
 * `close_drain_368.test.ts` — never imported directly.
 *
 * `ref` and `unref` only diverge when nothing else keeps the event loop
 * alive AND nothing at the top level is still `await`-ing the call —
 * measured, not assumed (#368 grooming): a directly `await`ed call still
 * settles a ref'd OR an unref'd timer alike, because the top-level module
 * itself has not finished. The divergence needs the shape a caller that
 * does not hold the process open on `close()` actually has: `run()` below is
 * never awaited, so the script's own flow "finishes" (prints `main
 * returned`) while `run()` is still suspended on a stall this process holds
 * no other timer or I/O for. If the drain's timer is ref'd, Deno keeps
 * running until it fires and `run()` prints `settled`. If it is unref'd,
 * Deno finds nothing else keeping it alive once the top level returns and
 * exits there — `settled` never prints.
 *
 * @module @lockness/realtime/tests/fixtures/close_drain_ref_probe
 */
import { awaitCloseDrain } from '../../drivers/close_drain.ts'

async function run(): Promise<void> {
    const result = await awaitCloseDrain(
        300,
        new Promise<void>(() => {}),
        Promise.resolve(),
        Promise.resolve(),
    )
    console.log(`settled ${JSON.stringify(result)}`)
}

// Deliberately not awaited — see the fileoverview.
run()
console.log('main returned')
