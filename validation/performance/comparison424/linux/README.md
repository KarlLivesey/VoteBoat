# Equal-work Linux424 diagnostic

Original runner55518 is terminal0. All nine rotating cases complete1,048,576
originals and262,144 four-entry requests after1024 warmups. All three applied
frontiers reach1,049,601. Before/after checks cover1407 frozen41ee217 inputs,
actual library/executable/compiler artifacts and the measurement driver.
One asserted opt3/debug2/thin-LTO/codegen1 binary runs both engines with identical
client/server topology, timing boundary and finite budgets. Both API aggregate
original bounds are16384; hard/soft units are recorded. VoteBoat's histograms
preserve1,049,600 originals. All27 phases and four provider scopes validate;
every VoteBoat trial retains204 leader checkpoints.

| Variant | Median completed PUTS/s | Request p99 |
| --- | ---: | ---: |
| VoteBoat observation off |1,160,164|17.021ms|
| VoteBoat observation on |1,219,278|16.191ms|
| OpenRaft shared driver |3,217,590|6.393ms|

Every trial is retained. Off/on variability establishes no observer speedup or
absence of overhead. This short volatile, in-process no-op workload establishes
no disk-durable result, sustained curve, healthy margin or causal comparison with
an older build. The untouched/guard-extended reference is separate; snapshot
representations differ as disclosed in the comparison contract.

First instrumented leader compaction takes408.832ms inclusive: state cloning
11.678ms, store append4.868ms, and small snapshot pin/load/reconcile intervals.
Publication takes1.090ms. Intervals overlap and nest; they cannot be summed as
CPU time or a critical path. Next inspect work outside provider methods inside
the public compaction helper, Core transition/completion and returned broadcasts.
The evidence does not identify pure Core cost or justify removing proofs.
Eliminating only the clone would address about2.9% of this one compaction interval.

Driver qualification retains two mistakes: an older debug artifact rejected
--attribution; then successful smoke processes exposed an incorrect assumption
that direct follower application counters include snapshot restoration/replay.
Exact follower frontiers and leader receipts remain mandatory. Direct follower
phase counts equal all originals only when no snapshot installation occurred.
The corrected three-case smoke and release diagnostic pass. An additional debug
smoke rerun had already started before check-only steering; all its raw results
remain. No failed or slower actual timing was discarded or restarted.
