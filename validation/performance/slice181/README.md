# Slice181 — manifest publication attribution

Source base `1b460ef2b05110bd0dd38d18e8990c0f2b70c485` plus the recorded source
hashes and committed instrumentation changes. `provenance.json` records the
CPU, memory, kernel, NVMe model/firmware and Btrfs options. Three logical replicas
share one physical host/device. The older full-suite process remained live on
this host with its stores on tmpfs; no CPU/device reservation or isolated-host
claim. Builds finished before these sequential measurements.

The same native startup assembly, 64-operation warm-up, 256 eight-byte increments,
window1, 50ms heartbeat and 1000–1999ms election settings were used. No durability
primitive changed: WAL sync, staging-file sync, atomic rename and directory sync
still precede a successful barrier. Optional counters now distinguish five
publication steps. Disabled timing makes no clock reads or counter updates.

```
./target/release/examples/native_benchmark target/bench-slice181-reference tcp 256 1
./target/release/examples/native_benchmark target/bench-slice181-diagnostic tcp 256 1 --journal-timings
```

Both commands exited0, recovered value320, verified original operation retries
with zero recovery retries and joined all workers. Raw256-receipt checks pass.

| Run | Measured seconds | Applied ops/s | p99 ms | Maximum ms |
| --- | ---: | ---: | ---: | ---: |
| Uninstrumented reference | 49.559878 | 5.165 | 956.095274 | 1472.581905 |
| Instrumented diagnostic | 40.348833 | 6.345 | 733.740841 | 768.524383 |

The original uninstrumented250ms gate fails (exit1) with its checked provenance.
The faster instrumented result is not evidence of a tuning benefit or measured
instrumentation overhead: runs are finite, sequential and share uncontrolled
host load. Instrumented runs cannot replace the fixed reference gate.

Mean measured operation times in the instrumented run:

| Replica | Calls per step | Staging open ms | Write ms | File sync ms | Rename ms | Directory sync ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 512 | 0.020 | 0.007 | 15.357 | 0.233 | 16.445 |
| 2 | 511 | 0.132 | 0.006 | 18.348 | 0.233 | 19.158 |
| 3 | 511 | 0.131 | 0.006 | 18.378 | 0.258 | 18.968 |

WAL sync is separate (mean16.320/17.920/18.263ms). The directory step includes
opening its directory. These are sums of actual invoked call durations, not a
client critical-path reconstruction. File/directory synchronization dominate
publication means; simply caching file opens is not a supported explanation or
solution for the p99 failure. Some open/rename calls also had larger maxima.
Reducing synchronization count needs a recoverable design preserving the exact
authoritative boundary; no unsafe fast path was enabled.

`diagnostic.publication.csv` retains all60 phase/replica/step rows. The independent
checker validates complete stage sets, successful joined call counts and nested
durations, monotonic counters and measured deltas. Live snapshots can cut a call;
they are not atomic. Reopen starts independent counters. The original journal
CSV schema and its checker remain unchanged. Partial failed runs are retained
but cannot pass the complete-publication checker.

Validation: 3 public journal timing/error tests,9 log-store conformance tests,
8 selected native log-store tests (including every publication/replacement
interruption boundary),28 native benchmark tests,4 publication checker negative/
positive tests,3 existing journal checker tests and6 original serial-gate tests
pass. Formatting, both strict all-target Clippy profiles, warning-denied docs and
95-contract inventory pass. An initial rename-failure fixture assumed opening a
directory failed; corrected it to check that NativeLogStore recovery rejects the
invalid selected manifest. One added output line exceeded the function-size
lint; consolidated journal artifact writing without relaxing the lint.

The raw gate runner first met a sandbox child-process restriction; the same
unchanged checker was rerun with local subprocess permission and produced the
numeric failing gate result. No checker bypass or threshold change.

This advances P7 attribution only. It does not establish sustainable throughput,
full critical-path attribution, isolated/device-independent performance,
physical power-loss behavior, macOS execution or complete P0–P7 acceptance.
