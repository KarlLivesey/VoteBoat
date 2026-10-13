# Native proposal-batch attribution diagnostic

Benchmark-only source ownership: examples/native_benchmark.rs and new
examples/benchmark/batching.rs. Existing startup --journal-timings enables the
additive diagnostic. Original CLI/results/provider/timers/order unchanged;
ordinary benchmark workloads carry no new observer. No runtime policy edits.

Observe public ReplicaProgress.steps. Successful proposed original operations
sharing exact VisitTicket are the returned original members of one actual ready
proposal invocation. EffectOwner installs them contiguously with one effect then
finishes that visit before executing another original invocation. Count each
original admission once; repeated OperationId with distinct original admissions is
valid and remains counted. Reject repeated admission/position or noncontiguous
membership.
Exclude failures, nonproposal and commit-only visits. Histogram count-weighted
width must equal original success count; distinct visits sum to batch count.

Public owner.deadline/role/Core.has_pending_dependency and ReplicaDriverUsage
are snapshotted immediately before actual Node.poll. Record successful batch
widths conditioned on heartbeat deadline due and pending dependency at that
observation, and aggregate public persistence lease counts. These are pre-poll
correlations, not observed queued classes/refusal reasons. Runtime queue class
counts and exact collector refusal are private and unavailable at this seam.
Existing JournalTimings before/after snapshots supply actual per-replica journal
sync/publication calls for separate comparison; this diagnostic does not infer
barriers from commands or change storage instrumentation.

Data is bounded by three replicas, configured benchmark groups and width<=64;
no per-operation history retained in live diagnostic. Per-poll accounting uses
returned bounded owner steps. CSV emitted after workload; no diagnostic file I/O
in timed poll. Diagnostic CPU overhead is named and must not be treated as an
uninstrumented throughput comparison. Warmup/measurement are separate totals.

Acceptance: independent accounting fixtures for width1/4/32, distinct replica/
group, empty commit-only/failure steps, malformed duplicate admissions/original positions, repeated operations with
distinct admissions,
correlated deadline/dependency labels, and weighted sums. A real owner/native
fixture must assert histogram successful originals equal actual applied receipt
count with no loss/duplicate; batch4+ singleton paths use real runtime controls.
Existing native benchmark restart/receipt verification remains mandatory.
Strict all-target all-feature Clippy and formatting after edits; parent handles
publication/final qualification and serialized timed pilot after framework work.
