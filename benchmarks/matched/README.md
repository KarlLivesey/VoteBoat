# Concurrent framework comparison

[Comparison conditions and capacity units](COMPARISON.md) are required before a
like-for-like capacity claim. Current primary API capacity is4096 requests:
VoteBoat selects4096 originals for batch1 and16384 for batch4, with explicit
bounded staging bytes. Earlier4096/64 original-versus-message labels remain
preserved in historical raw evidence. Linux and Mac combined qualification and short equivalent-envelope timings
pass; sustained throughput acceptance remains outstanding.

Standalone benchmark workspace; comparison dependencies stay outside VoteBoat
production. The frozen baseline passed Linux/macOS scoped checks and short
primary diagnostics. The startup batch-limits update passes eight Linux tests,
formatting, strict all-target Clippy and four actual client/server runtime controls.
Frozen414 macOS checks and short paired diagnostics now pass with the recorded
test-only fixture correction. Current415 macOS qualification remains separate.
These controls establish no sustained capacity or minimum-floor win.
[Baseline evidence](validation/REPORT.md),
[Linux pilot](validation/pilot-linux131072/REPORT.md),
[Mac pilot](validation/pilot-mac131072/REPORT.md),
[Startup limits checks](validation/performance/slice414-harness/REPORT.md).

`--server-workers N` selects the server runtime's worker thread count for either
engine (default 16; positive integer up to the harness budget of 32768). For
example, pass `--server-workers 1` for a one-worker scaling cell. The effective
value appears in both `settings.server_workers` and the existing top-level
`server_workers` JSON field; clients retain the original one-worker multithread
runtime. Keep worker counts equal when comparing engines. This flag belongs to
the volatile framework binary;
there is no durable binary sharing this workspace's entry point.

H1 retains only this flag. Both the fixed original-ID buffer and current-thread
client runtime were rejected and reverted because they changed realized merge
widths and persistence work per original. The quiet-Mac 30-pair buffer check
found mean width +5.858% (wider in 30/30 pairs); the workload source now equals
flag-only `297aec15` byte for byte. See [H1 evidence](validation/performance/h1/README.md).

One three-voter group, empty business payloads, 1024 singleton warmups and equal
closed-loop per-client requests. Every request awaits all its original applied
results. Requested totals round down like pinned OpenRaft. Primary defaults retain
OpenRaft payload1024/API4096/append4096. VoteBoat configures its public startup
limits to4096*entries-per-request original proposals and1024 replication entries. Reports
read back effective Core limits and record exact original-entry batch widths,
including warmup. Optional `--envelopes common64` selects64 API requests,64*entries-per-request
VoteBoat original proposals and independent replication64; it remains diagnostic.

Default-retention snapshots every5000 entries, keeps1000 and requires1024 newly
reclaimable entries. VoteBoat uses real public publication/pinning/compaction and
snapshot installation. Its image also defines its log base, so its no-op checkpoint
captures applied−1000; OpenRaft snapshots the latest frontier and retains older
logs. This model difference is disclosed, not hidden by resetting cores. VoteBoat
uses inline no-op application and host timers; OpenRaft owns separate application
workers and randomized elections. These engine/host scheduling differences remain
part of the comparison. Separate `retain-all` disables snapshots on both engines.
Both providers model volatile completion; real disk performance is separate.

[The pinned upstream manifest](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/Cargo.toml)
supplies shared dependency requirements. This workspace commits its independent
Cargo.lock. Fetch once, then run checks offline:

```sh
cargo +stable fetch --locked --manifest-path benchmarks/matched/Cargo.toml
cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
CARGO_TARGET_DIR=target/p9/matched-checks cargo +stable test --locked --offline --manifest-path benchmarks/matched/Cargo.toml
CARGO_TARGET_DIR=target/p9/matched-checks cargo +stable clippy --locked --offline --manifest-path benchmarks/matched/Cargo.toml --all-targets -- -D warnings
```

Coordinate separate release builds and quiet Linux/M2Max timing after qualification.
Freeze source, lock, toolchain, flags and artifact hashes. `run-curve.sh` runs the
seven required cells, interleaves engines and preserves every cell's output/error/
exit status. It never builds. `run-upstream-curve.sh` separately preserves the
untouched published-default driver. Internal OpenRaft queue/retention occupancy is
unavailable through its minimal provider; it is not reported as zero. Long curves
and policy review remain outstanding. Reports keep the comparison gate
pending until that evidence exists.

`--engine voteboat --attribution true` adds bounded per-replica elapsed phase
intervals after actor shutdown. The default path reads no attribution clocks and
omits that object. Records include campaign, warmup and follower drain. Persistence
and maintenance are inclusive; store append/barrier intervals are nested. Actors
overlap, so sums describe neither CPU time nor the request critical path. Upstream
phase attribution is unavailable and explicitly rejected. Instrumented rates are
diagnostic and cannot qualify the performance minimum.
[Attribution checks](validation/performance/slice417-attribution/REPORT.md) cover
the default and enabled paths, every original result, maintenance and snapshot
installation.

[The Linux elapsed diagnostic](validation/performance/slice417-attribution/DIAGNOSTIC-LINUX.md)
passes nine exact cases and guards. It points first to inclusive maintenance and
persistence-helper work; off/on rates overlap across trials and qualify no floor.

Checkpoint attribution also labels four inclusive actor boundaries (reservation,
publication, compaction and returned effects), plus the actual volatile log state
clone, exact durable-state comparison and snapshot provider calls. The comparison
uses full `GroupLog` equality without materializing a copy. Its item count is
retained entries in successfully equal states; unequal/error calls count zero,
not partial or physical traversal. The old clone phase remains separately named. Fixed before/after provider counter snapshots
report calls, failures, elapsed totals and item counts within each boundary.
Maxima remain whole provider lifetime; scoped maxima are deliberately absent.
Reconciliation includes its pinned-image load. Helper work outside these provider
methods stays explicitly unattributed; these intervals are elapsed time, not CPU.
Disabled mode reads no clocks or observer cells. Counter exhaustion preserves the
provider outcome and invalidates only the final diagnostic report.

The compact observer additionally reports three fixed gaps: `state_to_pin`,
`pin_to_append` and `barrier_to_reconcile`. Existing provider clock observations
and exact method ordinals delimit each successful helper call. These inclusive
wall intervals include nested work; they do not isolate CPU or a single function.
With the comparison seam, `state_to_pin` begins after exact comparison; it excludes
the prior full Eq and must not be compared as the old clone-to-pin scope. Reports
now contain 28 phases and 14 provider methods per checkpoint scope.
Failed helpers retain their original errors and certify no successful gaps.
[Schema and controls](validation/performance/slice425-gaps/SCHEMA.md).

The unified runner's finite campaign guard defaults to 3600 seconds; use
`--campaign-timeout-secs 1..7200` to select a recorded budget for either engine.
Owned cleanup has a separate 30-second guard. An incomplete or timed-out campaign
is a failure, with its original cause retained alongside any cleanup failure.
Throughput ends after all client joins, before latency-vector flattening/sorting
and follower verification; every original result and sample is still checked.
The untouched upstream driver keeps its own 400-second guard, so an outer watchdog
cannot make its slow 20M singleton cell finish. That limitation remains explicit.


Slice428 adds a separately named protected checkpoint verification phase (29
phases,15 scoped methods). It includes full fallback/dev reference work; logical
equal-entry counts do not measure skipped or compared entries. Compaction alone
uses the immutable core view and owner-preserved exact prefix. Raw equality and
completion/provider contracts remain unchanged. See [schema](SCHEMA.md).
