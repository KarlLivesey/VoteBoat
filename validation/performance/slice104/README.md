# Slice 104 — scheduled offered load with bounded admission

Base revision: 5c94569 plus benchmark-only scheduled-load/verification changes.
Production consensus, providers, formats, resources and timer defaults are unchanged;
RPL-1.5 is retained. The same public Node/LogStore/JournalIo contracts carry all
work and observations. There is no second WAL or hidden runtime.

## Reproduction

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
BINARY FRESH_ROOT tcp 120 8 8 --offered 4
BINARY FRESH_ROOT tcp 240 8 8 --offered 48
BINARY FRESH_ROOT quic 120 8 8 --offered 4
BINARY FRESH_ROOT quic 240 8 8 --offered 48
node validation/check-native-benchmark.mjs --storage validation/performance/slice104/{tcp,quic}-{low,high}
node validation/check-offered.test.mjs
```

All four performance runs are sequential, with fresh roots and the same copied
release executable. No compilation or bulk build/file work runs concurrently with
performance measurement. Each uses three replicas on this one host, eight groups,
node 1 leading every group initially, one native file WAL/worker, snapshot worker
and peer endpoint per replica (plus one dial worker per replica for TCP).
Global window 8 and per-group ceiling 1 bound retained accepted clients. No client
retry queue postpones refused offers. Payloads are 8-byte counter increments;
64 closed-loop warm-up commands and their verification precede measurement.
Benchmark-only heartbeats are 50 ms and elections 10000–19999 ms, as in the earlier
shared assembly; these are not the production service defaults.

The low-rate cases schedule 120 offers over 30 seconds; high-rate cases schedule
240 over 5 seconds. Processing is capped at 64 due offers per reactor turn.
Intended starts remain fixed despite actual dispatch lateness. Raw rows retain
every offer, including refused IDs; known Applied histories and expected values
account for holes. Runs publish a successful summary only after all-replica values,
quorum reads, shutdown/joins, full reopen, original first/last successful historical
retries, unchanged values and final joins. Recovery operations are not measured
useful work. All four pass, with zero extra recovery retries and zero unknown,
NotProposed, no-leader or provider-admission-refused outcomes. No failed new run
is omitted. Window refusals are real dropped offers, never useful operations.

## Results

| Transport / load | Offered ops/s | Offers | Admitted / applied | Window refused | Applied during ops/s | Applied total ops/s | Horizon backlog / drain applied | Drain ms | Intended-start p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP low | 4 | 120 | 120 | 0 | 4.000 | 4.000 | 0 / 0 | 0.072 | 1454.546 |
| TCP high | 48 | 240 | 40 | 200 | 6.400 | 7.428 | 8 / 8 | 384.976 | 1691.862 |
| QUIC low | 4 | 120 | 120 | 0 | 4.000 | 4.000 | 0 / 0 | 0.177 | 1091.054 |
| QUIC high | 48 | 240 | 121 | 119 | 23.600 | 23.474 | 3 / 3 | 154.721 | 506.619 |

Dispatch p99 is 1.045/0.918 ms for TCP low/high and 1.027/1.458 ms for QUIC.
Service p99 is 1454.446/1691.717 ms for TCP and 1091.031/506.590 ms for QUIC.
Maximum retained accepted clients are 6/8 for TCP and 5/8 for QUIC. No decisions
occur after the offering horizon in these samples. Recovered aggregate values
are 184/104 for TCP and 184/185 for QUIC (warm-up plus actual Applied commands).

`applied_during_ops_s` counts completions strictly before the scheduled horizon.
Total applied and admitted rates use offering-plus-drain elapsed time. The checker
also prints admitted-during and applied-drain rates separately in analysis.txt;
the latter are transient drain rates, not sustainable capacity. Horizon backlog
counts tickets admitted before the boundary but completed at/after it. Latency
from intended start includes delayed dispatch. `NA` percentiles represent no
successful Applied samples; they never certify zero latency. Every offer's status
is retained even though useful-write percentiles describe only Applied outcomes.

These are one sample/case, finite single-host illustrations. The high-rate cases
are short and differ substantially between transports; this is not evidence of
a transport speed advantage or stable sustainable capacity. Desktop/filesystem
activity is uncontrolled. No deliberate checkpoint/compaction or fault injection
is included during offering. There is no passed fixed-p99 acceptance claim.
The original 250 ms TCP serial target remains unmet from slice103; these offered
workloads do not replace that gate, and the one-operation compatibility results
below are not a target measurement. Longer maintenance/recovery/fault/platform
and separate-host evidence remains required for the full P0–P7 objective.

## Validation and artifacts

Each case has .txt summary, .offers.csv, .storage.csv and .run.txt configuration.
`analysis.txt` retains independent passing checks: all scheduled IDs/times and
classifications, actual retained load behind every window refusal, bounded global
and per-group windows, hole-aware values/indices, horizon/drain/rate/latency
arithmetic, and existing joined storage histogram/unit/ticket/primitive totals.
Storage deltas span offering plus drain and retain full completed calls crossing
boundaries. Primitive I/O can finish inside unfinished logical barriers; aggregate
worker durations sum concurrent work, not the elapsed critical path. Joined
snapshots reconcile exact totals. Initialization adds one primitive sync/publication
per session; recovery counters start anew. Observations confer no authority.

Six new Rust tests check schedule rounding/lateness, global/per-group limits,
exact replica/binding completion identity, unknown rejection/slot release,
refused-ID holes and applied history, no-sample percentiles, and preservation of
both work and cleanup failures. Together with the existing actual-file observer
test, all seven pass with all features and TLS-only. Five independent checker
tests pass (checker-tests.txt), including coordinated-omission, phantom refusal,
unknown, wrong receipt history and maximum-pending corruptions. All-target/
all-feature Clippy with warnings denied, formatting/diff checks and the unchanged
71-contract inventory pass. These finite checks are not a consensus or crash proof.

The original four-argument startup TCP and five-argument shared QUIC modes each
pass a one-measured-operation correctness run with complete recovery/retry/joins.
The latter has eight groups and window 3, retaining groups with only warm-up
history. Raw compatibility artifacts and compatibility-analysis.txt are retained;
their timing is not performance evidence. The updated checker also passes all
16 archived slice103 shared histories and both serial histories.

## Environment and hashes

Current kernel is Linux **7.2.9-2-cachyos**, differing from slice103's recorded
7.2.9-1. These results are not a before/after timing comparison with that slice.
Ryzen 7 9800X3D, eight cores/16 logical CPUs; no exclusive reservation or affinity.
MemTotal 63434604 kB. Btrfs on /dev/nvme1n1p2, Sabrent SB-ROCKET-NVMe4-2TB,
firmware RKT4B5.1; mount options rw,nosuid,nodev,noatime,compress=zstd:1,ssd,
discard=async,space_cache=v2,subvolid=257,subvol=/@home. Rust 1.98.1
(48a229cea 2026-09-01), x86_64-unknown-linux-gnu, LLVM22.1.8. Loopback TCP/TLS or
QUIC, three full replicas sharing this host/device; no separate network machines.

SHA-256:

- Cargo.lock: 532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8
- examples/native_benchmark.rs: 1bd5c75dc2139535c2c69aa4b09ded014be05deb826034641a7cdc7b145444e1
- examples/benchmark/offered.rs: 9592f2448c3f81197f6bbacc46fcff05f1850c1d504ec05787f5c2518f3ad558
- examples/benchmark/shared.rs: 398d6fc99bb979fa6e9e7ce1c27c41c6a92c62ff8e59c879686313b9c4123d81
- examples/benchmark/observe.rs: 8e94ef2798febe4a70d2f632b975c50d6964bd1e44d64b0f188f5a9af3e59c36
- release executable: 4372627c7dc70eda4fd2f95682bc69699d5fb58ff3787512c40222c368bf94de

Full P0–P7 remains active. P8/Windows stay deferred; CI remains background feedback.
