# Slice138 — native startup journal attribution

Source base58c56ca plus recorded optional timing/startup/benchmark changes.
provenance.json contains source/binary hashes, Linux/Btrfs labels and workload.
Private hostname/device inventory is not published, limiting independent hardware
reproduction. Three replicas share one host/device; no CPU/device reservation.
Both runs were sequential after release compilation, without overlapping builds
or bulk workload. Timers, durability, warmup, payload and window are unchanged.

```
target/release/examples/native_benchmark target/bench-slice138-tcp-diagnostic tcp 256 1 --journal-timings
target/release/examples/native_benchmark target/bench-slice138-tcp-reference tcp 256 1
```

Diagnostic: exit0,46.337179s,5.525ops/s,p99756.731330ms,recovered320,original
retries verified and all workers joined. Three timing readers count actual
FileLogIo append/log-sync/full-manifest calls. Journal CSV has before/after
measurement and create/recover join snapshots. Logical LogStore counts are
unavailable and zero; no counters are substituted for durable receipts. Fixed
serial checker rejects journal_timings=true even though raw receipt validation
passes. Its empty gate JSON is a refusal output, not a numeric acceptance result.

Measured per-replica deltas:

| Replica | Log-sync calls | Mean log-sync ms | Manifest calls | Mean publication ms |
| --- | --- | --- | --- | --- |
| 1 | 512 | 19.64 | 512 | 37.53 |
| 2 | 511 | 21.37 | 511 | 39.96 |
| 3 | 511 | 22.04 | 511 | 39.31 |

Manifest publication includes temporary file write/sync, rename and directory
sync. It is the larger recorded file-call category. This identifies a concrete
cost to investigate; overlapping worker sums are not client critical-path time,
and mean file durations do not explain every p99 outlier or leadership failure.
No individual syscall attribution, broad remote/platform proof or tuning gain.

Uninstrumented reference: exit0,41.720369s,6.136ops/s,
p50=143.249880ms,p95=304.390220ms,p99=734.953838ms,max=862.347280ms;
recovered320,recovery retries0,original retries verified,workers joined. Raw
256-receipt validation passes; original250ms numeric gate correctly fails exit1.
The diagnostic/reference difference is not a controlled overhead estimate or
performance acceptance. Neither run lost leadership, so no new failure cause
for the earlier unknown outcome is established.

Actual validation: 2 downstream journal timing/real publication-failure tests and
9 log-store conformance/crash tests pass;5 selected native log-store library tests
pass including every file-replacement boundary and saturating timings. Example
16 tests pass, all-feature/all-target check and Clippy -D warnings pass; release
build14.31s,core-only check3.28s. Three journal-verifier tests pass12.85ms; six
fixed-gate tests pass11.05ms. Format/whitespace and80-contract inventory pass.
Final native-only timing checks are recorded in validation/REPORT.md.

Initial compile missed the untimed connector builder's None option; corrected
locally. Initial failure test wrongly expected complete synced bytes to disappear
on publication failure. Recovery legitimately retains them; failed barrier
returns no receipt and in-process durable state stays unchanged. Corrected test
checks that actual contract instead of weakening storage/recovery semantics.

Next: investigate bounded recoverable manifest publication to reduce measured
metadata cost. Preserve all required synchronization/selection dependencies and
prove interrupted publication before accepting an optimization. Full P0–P7 stays
active; P8/Windows deferred, RPL-1.5 retained, CI background.

Native-only timing tests2/2 also passed after8.45s build. TCP timing exercised;
QUIC timing compiles but was not newly executed. All run/check handles terminal.
