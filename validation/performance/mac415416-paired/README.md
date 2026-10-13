# M2 Max incremental Core415 measurements

All 18 matched and 24 durable native cases passed, on their original sequential processes, using frozen 8a5840d against qualified 414 artifacts. Every source/library/executable and original-result guard passed; no case was discarded or rerun. Native median throughput improved about 8–12%, while the matched batch cell regressed about 13%. These measurements exercise Core415 with direct Counter or the minimal application; they do not measure the executable wrapper416. [Mac qualification](../mac415416/README.md) records the exact source/profile/test scope.

| Volatile matched cell | 414 originals/s | 8a originals/s | Untouched upstream originals/s | Median paired 8a/414 | 414 p99 μs | 8a p99 μs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One client, batch 1 | 52,536 | 56,238 | 10,109 | 0.966× | 34.8 | 31.3 |
| 4096 clients, batch 4 | 2,301,590 | 2,009,315 | 1,952,202 | 0.873× | 7,656 | 10,655 |

Absolute metrics are medians of three trials; ratios are computed within each trial before taking their median. These can move differently: the singleton's absolute median increases, but its paired ratios 0.966/0.923/1.070 give a median decline 3.4%. All batch paired ratios decline 0.865/0.873/0.873. Each case has 131,072 original completions and 1024 warmup originals; primary envelopes 4096/4096/1024, one client worker, 16 server workers, three replicas, zero-byte payload, default retention, and assertion-enabled thin-LTO/codegen1 release. Original frontiers and weighted proposal originals are checked for both VoteBoat variants; the untouched pinned minimal upstream exposes its request/original completion counts and latency samples, without the additional per-replica diagnostics. These short two-cell results do not qualify the published OpenRaft minimum.

| Durable native cell | 414 completed writes/s | 8a completed writes/s | Median paired 8a/414 | 414 p99 ms | 8a p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP/TLS, window32 | 257.6 | 275.4 | 1.087× | 133.90 | 133.91 |
| QUIC, window32 | 243.6 | 258.9 | 1.083× | 151.96 | 139.64 |
| TCP/TLS, singleton | 11.34 | 12.84 | 1.116× | 305.92 | 92.24 |
| QUIC, singleton | 11.56 | 13.05 | 1.118× | 92.20 | 94.59 |

Every native case uses 64 warmup plus 256 measured eight-byte Counter originals, three replicas sharing one M2 Max/device, startup assembly, actual TCP/TLS or QUIC and instrumented physical I/O. Completion requires every original Applied outcome/index/value; afterward value 320 is recovered from native files, an original retry is verified, and workers join. Matching native release keeps default LTO/codegen with assertions/overflow enabled. The baseline is matching-profile 414 e7e248b2, never the old observer 91d72 or untimed confounded 1db489. New native executable is 2c4fc972.

Leader physical work counts are unchanged: all window32 trials have 16 width 16 proposal visits and 32 WAL syncs/manifest publications; all singleton trials have 256 width 1 visits and 512 sync/publications. The per-replica durations and counts are retained individually. For example, TCP singleton median leader sync/publication totals shift 3.46/10.06 seconds to 2.79/8.29 seconds. This I/O variation prevents attributing the native rate changes to Core instructions alone.

Tail regressions remain: QUIC singleton median p99 increases 92.20→94.59ms, with one candidate trial at 320.27ms and 9.68 writes/s. The first candidate QUIC window32 trial achieves 100.62 writes/s and 1,136.22ms p99, while the other two achieve 258.94/273.64 writes/s and 139.64/137.66ms p99. TCP window32 median p99 is essentially unchanged. No slower trial was replaced.

The matched campaign is volatile and minimized; native is a focused same-device durable-service comparison, not a sustained remote-replica capacity sweep. Both use only three pairs. Raw commands, all original exits/latencies/widths/I/O and source/artifact identities remain in matched/raw.tgz (95 files) and native/raw.tgz (557 files), with verified member maps. Summaries retain every individual trial. Library/compiler proof is linked in the qualification archive and prior414 records; identity-and-drivers retains the exact selected source/artifact maps. Wrapper416 throughput and published-floor qualification remain separate.
