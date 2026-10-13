# Opt-in matched-host phase attribution

Read-only proposal for root review. Linux415's short gains leave a large gap; use
bounded measurements to select the next change. Perf is unavailable. No new
dependencies, production edits, altered payloads, peer scheduling, maintenance,
operation IDs or completion semantics.

## Data and scope

An explicit diagnostic flag selects an instrumented actor at startup. Ordinary
actors remain untimed; preferably specialize the same actor implementation with
a compile-time boolean so disabled clock calls/counters disappear. Output a
separate optional attribution object with per-replica call counts, total elapsed
nanoseconds and maximum interval for each phase, plus existing originals,
proposal widths, persistence units, retained entries and queue peaks. No per-entry
trace allocation or unbounded samples. Use checked counters and finite budgets.

These are monotonic **elapsed operation intervals**, not CPU time. Actors overlap
on shared runtime workers, and descheduling can occur inside any interval. Sums
across replicas may exceed wall time; they do not identify the end-to-end critical
path. Existing request p50/p99 and total measured completion time remain separate.
Bash process user/system totals can support interpretation, but not phase CPU
attribution. Do not label Instant timings as a CPU profile.

## Minimal phases

- Host admission: ready request collection, original correlation and input reservation, excluding the Core call.
- Core: direct step/propose/heartbeat calls, recording input/effect/original counts.
- Persistence helper: full public `persist_effect` interval, including Core continuation; retain this combined value.
- Store work: instrument existing MemoryStore append and barrier bodies separately. They are subintervals of persistence helper, not additive siblings. Helper minus store time is a labeled residual with callback/marshalling cost, not a claimed pure Core measurement.
- Application/results: split no-op `apply_batch` from result preflight/correlation/delivery so pending-result work is visible.
- Send: owned in-process enqueue calls and message/entry counts; this excludes the receiver's later processing and network transit.
- Maintenance: checkpoint/compact and snapshot stage/install helpers. Keep inclusive totals and any nested store/Core work explicitly marked, with no double counting.
- Host wait: select/await residence outside synchronous work, including peer/client/timer wakes. Queueing request latency is separate; busy ready loops are not idle wait.

Start with whole actor lifetime, explicitly including startup/warmup and final
drain, to avoid another synchronization protocol. Record that scope and warmup
count beside the timed workload interval. If this obscures the bottleneck, a
separate reviewed measurement-window protocol can follow; do not quietly reset
counters while actors are recording or infer exact epoch boundaries from a flag.

## Ownership and failure

Actor-local counters record coarse completed operations; report only after owned
shutdown/join. Public provider helpers and actual ticket barriers remain unchanged.
Every original result is still checked and every follower frontier verified.
Failures preserve stderr/exit and reject the measurement rather than emitting a
partial success. Time spent in failing operations may be recorded with a separate
failure count; it must not inflate completed original counts. Guard source, lock,
toolchain, artifact and workload exactly as in the ordinary comparison.

## Acceptance and experiment

The existing eight controls must pass with instrumentation both off and on,
including wide admission, exact requests/frontiers and snapshot maintenance.
New checks verify counters and nesting labels without testing hardware timing
thresholds. Default output and scheduling remain compatible; no clocks are read
in the off specialization. Formatter and strict Clippy stay zero.

Run one separate diagnostic campaign on the frozen415 candidate at the limiting
4096-client/batch4 cell, with the same default retention, original count and
runtime resources. Repeat small trials with instrumentation off/on to quantify
observer overhead; instrumented rates do not qualify the minimum. Use call counts
and elapsed distributions to distinguish host mapping, Core/store evaluation,
application/results, send and maintenance before implementing broad changes.
Counters transferable to native paths should attach to existing provider/hosting
seams; no claim of cross-provider equivalence without actual native evidence.
