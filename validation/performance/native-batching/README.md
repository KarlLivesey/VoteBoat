# Native proposal-batch accounting diagnostic

Benchmark-only observer enabled by existing startup `--journal-timings`; no
production queue, heartbeat, durability or batching policy changes. The normal
benchmark CLI/primary artifacts remain and `proposal-batches.csv` is additive.
This profile includes diagnostic CPU overhead; it is not an uninstrumented
throughput baseline. No timed attribution pilot has run in this qualification.

Each successful positioned original OwnerStep is counted. The exact VisitTicket
identifies one invocation's expanded original proposal batch; commit-only,
unproposed and failed visits are excluded. Repeated OperationId values with
separate original admissions remain separate originals. The width-weighted
histogram MUST equal each phase's actual successful applied receipt count; the
benchmark checks that before accepting its measurement. Group/replica labels
remain explicit. `width=0` rows contain observation totals only, not a batch.

`pre_poll_heartbeat_due_visits` and `pre_poll_dependency_visits` are correlations
with the public state immediately before Node.poll. They do not reveal queued
Control/Background classes or why the collector chose a singleton. Every row
states `queue_selection_reason_observed=false`; those internal reasons are not
available through this public seam. Persistence lease observations are
**replica-level snapshots repeated per group**, not independent per-group
counts that can be summed across groups. Other observation totals repeat in each
width row; use width=0 rows for per-replica/group observation totals.

Actual WAL synchronization/manifest publication calls still come from the
existing JournalTimings before/after rows in `journal.csv`/`publication.csv`.
Do not infer barrier counts or storage durability from successful proposal
positions. Existing exact receipts, three-replica state, recovery/retry and worker
join checks remain mandatory for an attribution run.

Scope is the startup single-group reference profile (`--journal-timings` already
requires it); shared/offered/lane profiles remain uninstrumented by this observer.
The internal real-node control additionally uses window4 and window1, checking
actual returned originals against applied receipts and singleton widths.

See SCHEMA.md for the pre-edit ownership/failure plan and QUALIFICATION.md for
actual checks and original failures. Mac accounting4/0 and targeted strict lint
now pass against the same frozen source; both asserted-release artifacts and
compiler/source guards are prepared. The corrected Mac environment and initial
failures are retained. No timed throughput conclusion is claimed. Sources are
identified in OWNED_SOURCES.sha256; root owns serialized benchmark campaigns.
