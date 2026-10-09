# Slice 102 shared WAL batch and stage attribution

Base revision `3b68add`, plus benchmark-only observer/generalization changes
committed with this evidence. Production providers, consensus/runtime/transport,
persistent format, resource limits and timers are unchanged. RPL-1.5 is retained.
The same public LogStore and JournalIo contracts carry the observations; no extra
WAL, storage worker or hidden runtime is introduced.

Build:

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
```

Four sequential measured runs use fresh roots with arguments
`FRESH_ROOT tcp|quic 256 8 GROUPS`, GROUPS=1 then 8 for each protocol. There is no
concurrent compilation during these four runs. Same one-host/three-replica topology,
8-byte counter commands, 64 total warm-up operations, global window 8, per-group
ceiling ceil(8/GROUPS), per-group lifetime capacity 320, 50 ms heartbeats and
10000–19999 ms elections as the controlled slice-101 baseline. All groups begin
measurement with node 1 leading. One WAL/worker, snapshot worker and peer endpoint
per replica; TCP additionally has one dial worker per replica. Snapshot handles
and application/core state remain per group.

All four pass every group's values on all replicas, quorum reads, full worker
joins/reopen, original first/last retry outcomes, unchanged final values and final
joins. Recovered aggregate is 320; extra recovery retries are zero. No measured
uncertainty/refusal/timeout/retry is accepted. These remain finite closed-loop
local runs, not sustained or separate-host/fault-isolated capacity evidence.

| Protocol | Groups | Applied ops/s | Elapsed s | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| TCP | 1 | 15.635 | 16.373335 | 779.263 |
| TCP | 8 | 7.276 | 35.185517 | 2791.030 |
| QUIC | 1 | 17.137 | 14.938738 | 1680.206 |
| QUIC | 8 | 7.326 | 34.943279 | 2873.822 |

## Actual storage observations

Each `.storage.csv` contains cumulative snapshots per replica before/after
measurement, after create-session join and after recovery-session join. Observers
forward exact original results/tickets/capabilities and lock only briefly to
update bounded counters. A real-file unit test rejects empty/stale barriers,
checks append alone is not durable, preserves exact barrier ticket scope and
reopens the actual store. No observation creates durability or read authority.

Measured deltas sum completed calls across three workers:

| Protocol | Groups | Append calls | Group units | Appended commands | Mean units/append | Append ms | Barrier calls | Barrier ms | Sync ms | Manifest publication ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 | 551 | 551 | 768 | 1.000 | 22.012 | 550 | 26133.482 | 9317.161 | 16813.155 |
| TCP | 8 | 1521 | 1528 | 768 | 1.005 | 76.022 | 1519 | 105131.796 | 36347.672 | 68749.712 |
| QUIC | 1 | 549 | 549 | 768 | 1.000 | 19.556 | 548 | 26055.870 | 8535.164 | 17527.908 |
| QUIC | 8 | 1361 | 1523 | 767 | 1.119 | 72.594 | 1359 | 94075.088 | 32172.096 | 61856.552 |

Measured batch histograms (`group units:append calls`):

- TCP one group: `1:551`.
- TCP eight groups: `1:1520|8:1`.
- QUIC one group: `1:549`.
- QUIC eight groups: `1:1329|6:31|8:1`.

The eight-group workload mostly submits one group transition per append. The
worker currently executes one append/barrier pair per submitted request; sharing
the worker has not combined most of these barriers. Native synchronization and
manifest publication dominate these observed storage costs; append processing
(including validation/encoding/state cloning) is small by comparison in this run.
Publication remains required by the unchanged recoverable-prefix contract and
must not simply be removed. This selects ready-queued-request shared barriers as
the next candidate, subject to bounded ownership/failure schema and crash tests;
it does not prove that candidate will improve performance or that queues always
contain coalescible requests.

These are aggregate worker durations, not one elapsed critical path. Primitive
completions can be visible inside an unfinished logical call; full call durations
can cross either interval boundary. Hence append/barrier counts can differ and
primitive sums need not match logical barrier sums exactly in measured deltas.
The QUIC-eight command count of 767 is a boundary observation, not evidence of a
lost command; full joined recovery/value checks pass. Physical command appends
can include replication/retry, so they are not useful-operation counts. Maxima
are cumulative maxima, not interval maxima. Native initialization/recovery each
adds one primitive sync/publication outside logical barriers; cumulative totals
include it. Recovery snapshots start counters anew. Snapshot/reclamation I/O is
not attributed by these fields.

After joins, exact append/barrier/unit/ticket totals, histograms, per-group units
and native primitive call counts reconcile. Independent validation checks those
relationships and all useful receipt routing/history/indices/windows/latencies:

```sh
node validation/check-native-benchmark.mjs --storage validation/performance/slice102/tcp-g1 validation/performance/slice102/tcp-g8 validation/performance/slice102/quic-g1 validation/performance/slice102/quic-g8
```

Output is retained in `analysis.txt`. Passing this arithmetic check is not a
protocol proof. Instrumentation adds overhead and filesystem timing varies; no
speedup/regression relative to unobserved slice-101 results is attributed to a
production change. The prior 250 ms TCP serial p99 target remains unmet/unproven;
window-8 partitioned results do not replace it.

Environment remains the slice-101 one: Ryzen 7 9800X3D (8 cores/16 logical CPUs),
Linux 7.2.9-1-cachyos, Btrfs on the repository's Sabrent NVMe, Rust 1.98.1/LLVM22.1.8.
Same memory/device/firmware/mount settings as slice99/101; no exclusive resource
reservation or affinity, and desktop activity can contribute variation.

Measured SHA-256:

- Cargo.lock: `532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8`
- examples/native_benchmark.rs: `e051a35e1851ecd1abfca972f17897ef6c3823b3c07fdc4fdb849573042360bb`
- examples/benchmark/shared.rs: `398d6fc99bb979fa6e9e7ce1c27c41c6a92c62ff8e59c879686313b9c4123d81`
- examples/benchmark/observe.rs: `8e94ef2798febe4a70d2f632b975c50d6964bd1e44d64b0f188f5a9af3e59c36`
- release native_benchmark: `c13311026d65a35db50b45760578cb97f624f9e0ea69d9dd4bd75c981a5fe3d9`

Full P0–P7 remains active. Measured tuning, sustained/offered-load, maintenance/
recovery load, macOS/separate-host and broader phase/fault evidence remain. P8
stays deferred and CI remains background feedback.

The additional `startup-edge` raw files record an original four-argument plain
NativeStartup correctness run (one measured TCP operation/window 1), including
full restart/retry/joins. It has no observer and its timing is not used for
performance comparison. All-target/all-feature Clippy with warnings denied,
TLS-only observer test/compilation, formatting/diff checks and the 71-contract
inventory pass. The observer real-file unit test also passes with all features.
