# Slice222 — original serial TCP measurement and durability diagnostics

No production change is made. The existing release benchmark runs on fresh
Btrfs workspace directories with three logical replicas on one NVMe host/device.
The reference remains TCP/TLS startup, one group,64 warmup plus256 useful8-byte
writes, one outstanding operation, heartbeat50ms and election1000ms+1000ms.
File::sync_all, atomic manifest rename and directory synchronization remain.

| Run | Applied ops/s | p99 | Outcome |
| --- | ---: | ---: | --- |
| Uninstrumented reference | 7.890 | 471.057ms | Complete; original250ms gate fails |
| Existing journal diagnostic | 4.945 | 872.815ms | Complete; instrumentation cannot replace reference |

Both runs recover320 on all replicas, check original retry receipts and fresh
reads, and join every selected worker before summary publication. The raw
checker validates all256 receipt IDs/positions/values, window1 and latency/rate
arithmetic. Source/binary/context hashes bind the reference summary and samples.
The reference process exits0; its actual serial gate exits1 with passed=false.
`reference-gate-sandbox.log` retains an initial child-spawn EPERM; the unchanged
checker then runs with permission to spawn its required raw validator. That
sandbox refusal is not a latency result or a bypass of validation.

Recorded diagnostic measurement deltas:

| Replica | Log sync calls | Log sync time | Manifest file sync | Directory sync |
| --- | ---: | ---: | ---: | ---: |
| 1 | 512 | 12.182s | 10.273s | 10.249s |
| 2 | 511 | 11.423s | 11.554s | 12.003s |
| 3 | 511 | 12.094s | 11.226s | 12.003s |

Each replica publishes511/512 manifests. Recorded mean log synchronization is
22.35–23.79ms per call. Open/write/rename totals are much smaller. These are
validated file-call counter deltas with possibly cut live boundaries and
overlapping replica durations, not summed client latency or per-operation
critical-path proof. Host poll maxima are0.855ms reference/0.542ms diagnostic.
The evidence selects durability publication for investigation; no candidate,
timer change, weaker synchronization or optimistic durable-voter rule is adopted.
Sequential shared-host differences also cannot establish a causal speedup over
the older873.493ms reference or blame instrumentation for the entire difference.

Commands:

```sh
cargo +stable build --locked --offline --release --example native_benchmark
target/release/examples/native_benchmark target/bench-slice222-reference tcp 256 1
target/release/examples/native_benchmark target/bench-slice222-diagnostic tcp 256 1 --journal-timings
node validation/check-native-benchmark.mjs validation/performance/slice222/reference validation/performance/slice222/diagnostic
node validation/check-serial-gate.mjs validation/performance/slice222/reference
node validation/check-journal-timings.mjs validation/performance/slice222/diagnostic.journal.csv
node validation/check-publication-timings.mjs validation/performance/slice222/diagnostic.publication.csv
```

The serial command deliberately exits1 for the observed failed criterion.
Native run roots must be fresh; original directories are retained in target/.
Archived prefixes contain unchanged summary/sample/journal/publication bytes.
`diagnostic-cost-summary.json` derives the table from the validated checker
deltas; it adds no timing measurements. No fabricated storage.csv is supplied.
`provenance.json`, host-before/toolchain/code-state logs and source-binary hashes
record the actual shared desktop/NVMe, release build and unchanged code revision.
No matching cargo/rustc/native VoteBoat work was active at inspection, and no
new builds or native suites overlap measurement. Other desktop activity remains;
there is no CPU/device isolation, sustainable-capacity or separate-host claim.
Formatting/four strict all-target profiles pass through the unchanged hook.

Background platform evidence is separate. Matching221
[run38054779549](https://github.com/KarlLivesey/VoteBoat/actions/runs/38054779549)
has terminal macOS counter170 pass/1 fail before directory/transfer. The failure
is Cluster::stop after restored19701 membership-drain plan cancellation and
resuming=false: node2 accepts quit but exits1 with Closed. This locates the
observed boundary at shutdown; it does not establish the cause. Matching Ubuntu
passes171/22/21. Earlier live observations remain distinct from terminal state.
Preceding220 Ubuntu166/22/21 passes;
its macOS162/4 failures are retained221. These are exact-source partial platform
results, not complete macOS acceptance.

Macro progress: original P7 evidence is current and its fixed criterion remains
unmet. Next finish the concrete usable-service shutdown boundary, then attribute
durability operations before a bounded crash-tested tuning candidate and review
replacement-provider obligations. Broad security stays with Daybreak. Full
P0–P7, broader faults/platform/deployment and performance acceptance remain open.
