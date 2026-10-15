#470 raw-validation gauge correction, before editing

Original campaign33443 is terminal1/ALL_TIMING_END: all18 workloads exit0,
four frozen raw checks fail only on correlation snapshot capacity monotonicity.
Original validators/raw outputs remain immutable. Revalidate existing data only.

Data/API: distinguish cumulative numeric counters from two per-snapshot gauges:
barrier_pending_tickets and barrier_correlation_capacity_bytes. The latter is
capacity of the cloned pending Vec in Trace::snapshot, not the live observer
owner's retained capacity or whole-process memory. Preserve35/21 CSV formats,
all per-row nonnegative integer/conservation/physical checks and cumulative
counter/histogram monotonicity. Return gauges as named stage/replica observations,
not accumulated deltas; no production/timed observer change.

Transitions: before/after/create-join snapshot gauges may decrease as pending
tickets drain (actual four144→0 with pending1→0). Recovery remains independent.
Counters retain their prior lifetime/reset rules. Independent fixture controls
must accept declining gauges and reject cumulative counter regressions and
invalid gauge values. No gauge proves persistent memory bounds.

Ownership/failure: root changes only current checker/tests, freezes a separately
named revalidation closure and runs it after original termination. Keep original
campaign status1/four failures, raw source/artifact/profile/source scope, exact
files and all tails. No workload, compiler, profile, maintenance, deadline or
engine behavior rerun/change. Revalidation receipts cannot rewrite original exits.

Acceptance: focusedJS controls and all18 existing raw histories with all12 exact
OFF/ON readbacks pass; unchangedoutcomes/recovery/joins/storage conservation and
physical activation checks remain. Complete enabled strict hook stayszero. Link
actual rates/refusals/physical counts and performance rejection/next action to
macro/current+nexttwo plans. Full P9 remains active and incomplete.
