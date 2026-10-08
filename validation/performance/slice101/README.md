# Slice 101 shared native Multi-Raft baseline

Base revision `6b07231`, plus the benchmark assembly/verification changes committed
with this evidence. Production consensus/storage/runtime/transport and service
resource/timer defaults are unchanged. RPL-1.5 remains the package license.

Build:

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
```

Controlled runs use fresh roots, TCP then QUIC, each with arguments
`FRESH_ROOT PROTOCOL 256 8 GROUPS`, GROUPS=1 then 8. They run sequentially without
concurrent compilation. All replicas run on one polling host process/machine.
This is a finite closed-loop partitioned-counter workload, not sustainable or
open-loop performance, a cross-group transaction or a separate-host deployment.
Each group has three full voters, actual authenticated peer traffic, 8-byte +1
commands and native filesystem synchronization. Warm-up has 64 total operations;
measurement has 256 total operations, with window 8 and per-group ceiling
ceil(8/GROUPS). Dispatch waits for the next round-robin group rather than skipping
slow groups. Each application's lifetime capacity is 320 on both sides.

Assembly uses public Node::from_parts with one authoritative WAL/worker, one
snapshot worker and one peer endpoint per replica, regardless of group count.
TCP additionally has one dial worker per replica: nine worker threads total for
TCP, six for QUIC (plus the polling host thread). These counts follow the explicit
constructor/spawn sites; no process-wide thread census is claimed. Each group
has its own core/application and snapshot handles. Leaders at measurement start
are node 1 for every group in all four runs; leadership is not spread across nodes.

Both controlled sides explicitly use 50 ms heartbeats and 10000–19999 ms elections.
Longer failure detection is a declared experimental setting, not a latency fix.
A preliminary one-group TCP run at 1000–1999 ms elections passed (initial-tcp-g1
files), but p99 1559.711 ms overlapped that election range. The preliminary
8-group TCP run reached measurement then failed operation 92 with
`Unknown(LeadershipChanged)`. It is invalid: no successful summary or sample set
was published. Its retained local root is target/benchmark-runs/slice101-tcp-g8.
This is evidence of instability under those settings, not a proven cause of it.
The preliminary run predates the final source hashes/receipt-history assertion
below and is not part of the controlled comparison.

| Protocol | Groups | Applied ops/s | Elapsed s | p99 ms | Persistence batches | Worker events | Apply deliveries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 | 21.141 | 12.109438 | 950.520 | 554 | 1108 | 295 |
| TCP | 8 | 9.204 | 27.814103 | 1308.700 | 1524 | 3030 | 756 |
| QUIC | 1 | 15.807 | 16.194905 | 1769.466 | 551 | 1102 | 292 |
| QUIC | 8 | 7.786 | 32.879749 | 2920.492 | 1526 | 3036 | 757 |

Every controlled run verifies all groups on all three replicas, quorum reads,
full drain/join/reopen, original first/last retry outcomes, unchanged partitioned
values and final worker joins. Recovered aggregate is 320: one group has 320 or
each of eight groups has 40. No measured uncertainty/retry is accepted. All four
have zero additional recovery retries. CSV includes concrete group identity and
per-group applied index, not a global log position. Independent validation checks
unique complete operation sequences, round-robin routing/historical values,
per-group increasing positions, global/per-group in-flight bounds, timestamp
arithmetic, nearest-rank percentiles and rate summaries:

```sh
node validation/check-native-benchmark.mjs RUN_DIRECTORY...
node validation/check-native-benchmark.mjs validation/performance/slice101/tcp-g1 validation/performance/slice101/tcp-g8 validation/performance/slice101/quic-g1 validation/performance/slice101/quic-g8
```

More groups are slower here and submit more persistence batches. Shared ownership
alone has not amortized barriers sufficiently for this workload. Diagnostic counts
are interval totals across replicas, not contiguous watermarks or exact causal
traces. The worker currently executes one submitted batch/barrier at a time;
additional tracing is needed to distinguish driver batch fill, arrival timing,
queued worker submissions and shared-store history costs before selecting tuning.
There is no throughput-improvement claim. The prior 250 ms TCP serial p99 target
remains unmet/unproven; these window-8 results do not replace that target.

Environment rechecked: AMD Ryzen 7 9800X3D, eight cores/16 logical CPUs, Linux
7.2.9-1-cachyos; Btrfs rw,nosuid,nodev,noatime,compress=zstd:1,ssd,discard=async,
space_cache=v2 on the repository device. Same Sabrent NVMe/firmware and memory
configuration as slice 99; no exclusive CPU/device reservation, affinity or
independent replica machines. Desktop activity may contribute variation. Compiler
is stable Rust 1.98.1; dependency versions/features are locked and offline.

Final measured source/executable SHA-256:

- Cargo.lock: `532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8`
- examples/native_benchmark.rs: `9cb7e34b0d72a9134321488cd1c1af37f508115d7cbe664b7ee58ac60d53675f`
- examples/benchmark/shared.rs: `dadf5972bdf92bf675967d1ba3c63568103b7d11fbf0e8c4e362420796223f3e`
- release native_benchmark: `04cdc43f3f6e741dd0fa16d52875cd1049009ea07733d1c112cc3d0c75eef2ed`

Remaining P7 evidence includes fixed-budget tuning, sustained/offered-load,
maintenance/recovery load, broader faults, separate hosts and macOS execution.
P0–P7 remains active; P8 remains deferred and CI remains background feedback.

Extra correctness-only TCP runs (`tcp-edge` and `startup-edge` files) pass full
recovery/retry/joins: eight shared groups with one measured operation/window 3,
and the original four-argument startup mode with one operation/window 1. These
exercise groups with only warm-up history and retain CLI behavior. Compilation
was permitted during these extra checks; their timings are not performance
comparison evidence. All-target/all-feature Clippy with warnings denied, TLS-only
example compilation, formatting/diff checks and the 71-contract inventory pass.
