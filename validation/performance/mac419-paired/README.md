M2 Max compares exact `32217ae` (418) with qualified `7511e5e` (419 ordinary incoming append ownership/normalization). All 51 original cases passed completion and source/library/executable guards. Matched standalone manifests, lock and source match; native benchmark/observer sources also match. Every trial, including regressions, is retained.

The like-for-like audit found that legacy API capacity labels compare upstream requests with VoteBoat originals. These same-engine 418→419 pairs retain identical limits and are valid within their scope; the cross-engine reference rates do not establish equal-work envelopes or a capacity win. See the [comparison contract](../../../benchmarks/matched/COMPARISON.md). Original raw records remain unchanged.

| Primary framework cell | Before → after originals/s | Median paired ratio | Before → after p99 ms |
| --- | ---: | ---: | ---: |
| 1 client, batch 1 | 64,542 → 63,588 | 0.972× | 0.02888 → 0.03013 |
| 4,096 clients, batch 4 | 2,556,825 → 2,780,973 | 1.086× | 7.485 → 6.922 |

Primary18 measured 131,072 originals after 1,024 warmup in three interleaved trials with untouched pinned OpenRaft. Untouched medians were 11,277 singleton and 2,208,612 bulk originals/s. Each singleton pair regressed (−2.8%, −12.9%, −0.4%); each bulk pair improved (+8.6%, +2.5%, +8.8%). Ratios are medians of paired trials; rates and p99 are medians of their respective values. They are short observations, not confidence intervals.

The separate phase9 measured 1,048,576 bulk originals after 1,024 warmup: TRACE-off 3,000k → 3,328k/s, median paired 1.128×, p99 6.171 → 5.599 ms. Candidate TRACE-on median was 3,380k/s, p99 5.588 ms. Phase counters cover whole actor lifetime including campaign, warmup, drain and shutdown; nested helpers and replicas overlap. Do not sum elapsed durations, treat them as exclusive CPU cost, or infer a throughput cause or instrumentation benefit from these short runs.

| Durable native cell | Before → after completed writes/s | Median paired ratio | Before → after p99 ms |
| --- | ---: | ---: | ---: |
| TCP/TLS, window 32 | 280.8 → 273.5 | 0.974× | 132.93 → 138.10 |
| QUIC, window 32 | 267.6 → 261.1 | 0.977× | 139.99 → 182.57 |
| TCP/TLS, window 1 | 12.90 → 12.04 | 0.933× | 91.20 → 97.76 |
| QUIC, window 1 | 12.52 → 12.57 | 1.012× | 92.96 → 266.30 |

Native24 measured 256 original commands after 64 warmup in three interleaved pairs per cell. Every Applied value/index/history, reopened value320, retry, worker join/drain, weighted width and physical journal counter guard passed. It directly exercises ordinary append/staging; it does not trigger the 5,000-entry checkpoint. Most width32 cases retained sixteen width16 proposals and32 leader sync/publications. One candidate QUIC trial used fifteen width16 plus width1/15 (17 visits,34 leader sync/publications). All singleton cases used256 width1 visits and512 leader sync/publications. Full per-replica physical counts and durations remain in native SUMMARY/RESULTS; raw samples, WAL and manifests support the recovery checks. Worse tails remain, including candidate QUIC singleton10.10/s with419.82ms p99 and candidate QUIC window32 trial234.8/s with182.57ms p99. There is no demonstrated durable-service gain from this slice.

Matched results are volatile, zero-payload, in-process quorum/application framework work. Native uses8-byte counter commands and three replicas sharing one M2 Max host/device, real TCP/TLS or QUIC, synchronous journals and the unchanged observer. No throughput cause is inferred from proposal width or I/O counts. Two short framework cells and bounded device runs do not qualify the published floor, a durable minimum or steady device capacity. No live421/422 source was used.

Original framework runner93903 / PID13259 completed0, followed by native67565 / PID18580 completed0. No rebuilds, transfers or other measurements overlapped each Mac timing window, and there were no failed attempts or reruns. Fresh matched opt3/assertions/overflow/thin-LTO/codegen1 and native opt3/assertions/overflow/default-LTO/default-codegen proofs are in [418 qualification](../mac418/README.md) and [419 qualification](../mac419/README.md). Sources1384/1390 and immutable original/copied library/executable identities passed before/after. Publication contains all158 framework and568 native original transferred files plus their hash manifests and derived summaries. `EVIDENCE.json` verifies every publication file; no executable, rlib, source checkout or build tree is included.
