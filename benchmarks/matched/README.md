# Concurrent framework comparison

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

One three-voter group, empty business payloads, 1024 singleton warmups and equal
closed-loop per-client requests. Every request awaits all its original applied
results. Requested totals round down like pinned OpenRaft. Primary defaults retain
OpenRaft payload1024/API4096/append4096. VoteBoat configures its public startup
limits to 4096 original proposal entries and 1024 replication entries. Reports
read back effective Core limits and record exact original-entry batch widths,
including warmup. Optional `--envelopes common64` selects 64/64 for both engines
and remains attribution only.

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
