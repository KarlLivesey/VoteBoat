# Slice 103 — bounded shared native durability barriers

Base revision: 007dbde. The production change is confined to NativeLogWorker:
collect immediately ready independent requests within existing request/unit/byte
and store pending-ticket limits, append each original request independently,
validate one exact barrier ticket union, then deliver original scoped terminals.
No fill delay, reclamation overtaking, persistent format, protocol, timer default,
worker-count or public API change. Original credits and control reserves remain.
RPL-1.5 is retained.

## Reproduction and provenance

Build the base and candidate with:

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
```

Run copied release executables sequentially with fresh roots:

```sh
BINARY FRESH_ROOT tcp 256 8 1
BINARY FRESH_ROOT tcp 256 8 8
BINARY FRESH_ROOT quic 256 8 1
BINARY FRESH_ROOT quic 256 8 8
BINARY FRESH_ROOT tcp 256 1
```

Baseline A is copied verbatim from slice102 (same executable hash). Baseline B
was rebuilt from 007dbde in a managed worktree; its executable hash matches A.
Candidate A/B are fresh independent runs of the same candidate executable.
The four baseline-A, four baseline-B and eight candidate shared runs give two
samples per variant/case; the serial comparison has one sample per variant.
These are not randomized trials, confidence intervals or sustainable capacity
measurements. Compilation and large build preparation occurred between measured
runs, never concurrently with performance measurement. Desktop activity was not
controlled; no exclusive CPU/device reservation or affinity was used.

Every shared case uses three replicas on this one host, one authoritative native
file WAL/worker, snapshot worker and peer endpoint per replica; TCP has a dial
worker per replica. Node 1 leads all groups at measurement start. The unchanged
observer, workload and resources use 64 warm-up +256 measured 8-byte counter
commands, global window 8 and per-group ceiling ceil(8/groups), 50 ms heartbeats
and explicit benchmark elections 10000–19999 ms. These benchmark election settings
are not the production service defaults.

Every successful run gates publication on all-group/all-replica final values,
quorum reads, full joins/reopen, original first/last retry outcomes, unchanged
final values and final joins. All 18 archived runs pass with aggregate recovered
value 320 and zero extra recovery retries. No failed new measurement was omitted.

## Useful-operation results

| Protocol | Groups | Baseline A / B ops/s | Candidate A / B ops/s | Baseline A / B p99 ms | Candidate A / B p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 | 15.635 / 22.434 | 18.416 / 22.137 | 779.263 / 585.535 | 1112.344 / 649.176 |
| TCP | 8 | 7.276 / 10.612 | 20.108 / 30.614 | 2791.030 / 1242.371 | 1491.583 / 576.278 |
| QUIC | 1 | 17.137 / 23.955 | 18.249 / 16.243 | 1680.206 / 615.657 | 851.175 / 1852.930 |
| QUIC | 8 | 7.326 / 9.687 | 22.746 / 27.861 | 2873.822 / 1124.028 | 1594.928 / 400.469 |

Eight-group candidate throughput exceeds both baseline samples for each transport.
The measured storage deltas show approximately 3.0–3.1 units/barrier versus
1.0–1.1 before. This supports the selected barrier-amortization mechanism under
this finite workload. One-group controls show no reliable improvement; their
one outstanding persistence transition leaves no independent request to combine.
Tail latency varies substantially. In particular fresh baseline-B QUIC eight-group
p99 is lower than candidate-A p99. Do not claim universal latency improvement,
single-group acceleration, offered-load stability or meeting a latency budget.

Original four-argument startup mode has window 1, no storage observer and the
unchanged 1000–1999 ms benchmark elections. Baseline: 7.997 ops/s,
p99 478.658 ms; candidate: 8.517 ops/s,
p99 311.801 ms. Both pass complete recovery/retry/joins.
The predeclared 250 ms TCP serial p99 target remains **unmet**. The observed timing
difference from one serial pair does not establish a single-group improvement.

## Independent checks and safety evidence

```sh
node validation/check-native-benchmark.mjs --storage validation/performance/slice103/{baseline,candidate}-{tcp,quic}-g{1,8}-{a,b}
node validation/check-native-benchmark.mjs validation/performance/slice103/{baseline,candidate}-tcp-serial
```

Retained outputs: analysis.txt and serial-analysis.txt. Useful receipt IDs, routing,
per-group positions/values, bounded windows, nearest-rank percentiles and rate
arithmetic pass. Joined storage snapshots reconcile full append records, ticket
unions, histograms/group units and primitive I/O. Joined barrier calls may now be
less than append calls; barrier tickets still equal successful appended units.
Batch histograms describe original appends, not barrier-window sizes.

Mid-run snapshots count full completed calls crossing interval boundaries.
Primitive completions may occur within unfinished logical barriers. Aggregate
worker durations sum concurrent work, not elapsed critical-path time. Native
initialization contributes one extra sync/publication per replica/session;
recovery counters start anew. Diagnostic counts confer no durability authority.
The independent checker is an arithmetic/conformance check, not a protocol proof.

Eight deterministic/crash tests in tests/shared_barrier.rs pass with all features.
They cover reversed union order, mixed runtime-owner identities, exact original
credits/completions, independent append rejection, fatal append and partial/
duplicate/wrong-session/wrong-generation/failed barriers, bounded deferred FIFO,
close/reclaim ordering and native dependency limits. Native framing tests cover
436 failure schedules (106-byte records, every byte cut of either queued record,
loss/persistence of unsynced bytes, sync and manifest boundaries). Eight actual
FileLogIo fault/reopen cases check acknowledged promises and valid Raft recovery.
These finite tests do not constitute a full crash proof or process-kill campaign.
Existing worker (9), maintenance (5), log-store (9), effect-owner/runtime (127)
checks pass, including actual three-node/100-group restart paths. Final all-target/
all-feature Clippy with warnings denied passes.

Environment: Linux 7.2.9-1-cachyos, Ryzen 7 9800X3D (8 cores/16 logical CPUs), Btrfs
on the same Sabrent NVMe, Rust 1.98.1/LLVM22.1.8. Same environment and mount/device
settings as slice102; no macOS or separate-host performance evidence.

SHA-256:

- Cargo.lock: 532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8
- examples/native_benchmark.rs: e051a35e1851ecd1abfca972f17897ef6c3823b3c07fdc4fdb849573042360bb
- examples/benchmark/shared.rs: 398d6fc99bb979fa6e9e7ce1c27c41c6a92c62ff8e59c879686313b9c4123d81
- examples/benchmark/observe.rs: 8e94ef2798febe4a70d2f632b975c50d6964bd1e44d64b0f188f5a9af3e59c36
- candidate src/native/worker.rs: 1c07f30b978beca1c3e049f1ead65323f39edb37ee2f07af0be043f1935959ea
- baseline executable: c13311026d65a35db50b45760578cb97f624f9e0ea69d9dd4bd75c981a5fe3d9
- candidate executable: dcdca3c317edea006a280fa9ec7a59526defedccd601426c3b3d404928219707

This is incremental P7 tuning evidence. Full P0–P7 and earlier phase gaps remain
active; sustained/offered-load, maintenance/recovery, broader faults, macOS and
separate-host evidence remain. P8 stays deferred; CI remains background feedback.

Final native-only shared-barrier tests also pass (8/8), as do formatting/diff
checks and the unchanged 71-contract inventory.
