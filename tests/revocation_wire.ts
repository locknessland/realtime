/**
 * @fileoverview #380 — the one test-side home of what the revocation reap and
 * the floor announce look like on the wire.
 *
 * The Redis driver sends three `EVAL`s that touch revocation keys, and a
 * suite that picks one of them out must not pick up another:
 *
 * | command  | numkeys | `args[3]` | `args[4]` | length |
 * | :------- | :------ | :-------- | :-------- | :----- |
 * | reap     | `'2'`   | index     | floor     | 7      |
 * | announce | `'1'`   | floor     | ttl       | 6      |
 * | mark     | `'1'`   | index     | ttl       | 7      |
 *
 * The announce never carries the index key (#380 A2). Before #380 the reap
 * was a one-key `EVAL` with no operand after its key, and three suites each
 * kept their own copy of that predicate; they import these instead, so the
 * shape moves in one place when the wire does.
 *
 * **Never by script text.** A match on the Lua source breaks on a reformat
 * the broker cannot see (`prefix_anchoring.test.ts` says why).
 *
 * @module @lockness/realtime/tests/revocation_wire
 */

/**
 * Whether `args` is the revocation pass's reap: `EVAL <script> 2 <index>
 * <floor> <ttl> <key ttl>`.
 *
 * @param args - The command as the driver issued it.
 * @param index - The revocation index key of the driver under test.
 * @returns `true` for the reap only — never the announce or the mark.
 * @example
 * ```ts
 * const isReapHere: CommandMatch = (args) => isReap(args, INDEX)
 * ```
 */
export function isReap(args: readonly string[], index: string): boolean {
    return args[0] === 'EVAL' && args[2] === '2' && args[3] === index &&
        args.length === 7
}

/**
 * Whether `args` is the floor announce: `EVAL <script> 1 <floor> <ttl>
 * <key ttl>`.
 *
 * @param args - The command as the driver issued it.
 * @param floor - The revocation floor key of the driver under test.
 * @returns `true` for the announce only — never the reap or the mark.
 * @example
 * ```ts
 * assertEquals(sent.filter((args) => isAnnounce(args, FLOOR)).length, 1)
 * ```
 */
export function isAnnounce(args: readonly string[], floor: string): boolean {
    return args[0] === 'EVAL' && args[2] === '1' && args[3] === floor &&
        args.length === 6
}
