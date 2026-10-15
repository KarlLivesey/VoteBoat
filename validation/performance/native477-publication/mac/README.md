#477 Mac APFS paired publication results

Original SSH handle 94749 actually terminated with exit 0 after ALL_TIMING_END; all 48 case processes,48 frozen validators and48 readback checks passed. No case was retried or replaced. Both hosts terminated before collection. Source 7e9068296ce00dde5bf3e03bf4d852ed2833d185 and the same qualified native476 executable serve both explicit atomic and witness arms;476 qualification is archived separately. This is real durable three-voter Counter over TCP/TLS or QUIC, not the volatile framework.

| Cell | Protocol | Atomic completed/s | Witness completed/s | Atomic p99 ms | Witness p99 ms | Atomic window refusals | Witness window refusals |
|---|---|---:|---:|---:|---:|---:|---:|
| singleton | tcp | 7.250 | 7.406 | 217.860 | 159.952 | 24 | 19 |
| singleton | quic | 7.406 | 7.406 | 188.004 | 162.845 | 19 | 19 |
| moderate | tcp | 124.968 | 127.856 | 3153.844 | 766.032 | 178 | 0 |
| moderate | quic | 127.710 | 127.785 | 897.705 | 1891.831 | 0 | 3 |
| bulk | tcp | 357.263 | 481.317 | 6927.865 | 1275.512 | 0 | 0 |
| bulk | quic | 353.521 | 470.403 | 7178.674 | 1472.977 | 0 | 0 |
| overload | tcp | 313.715 | 488.097 | 12582.852 | 7914.134 | 1629 | 479 |
| overload | quic | 375.734 | 490.957 | 10682.127 | 7952.975 | 1238 | 347 |

Three-trial arm medians; ANALYSIS.json retains every trial, paired ratios/ranges, regressions, p50/p95/p99/max, intended/service/dispatch latency, during/drain rates, all refusal classes, backlog, physical deltas, maintenance and resources. Singleton offers 8/s with window 1; moderate 128/s with window 256; bulk 512/s and overload 1024/s with window 4096. Offered caps are not maximum-capacity measurements.

All 297984 offers reconcile to 285221 applied plus 12763 window refusals; other refusal/unknown/pending classes are zero. All cases recover 64+applied, preserve original-operation retries and join workers. Active maintenance completes 672 checkpoints and 2016 reclaims. Readbacks cover all three Create and three Recover providers/capacity profiles. Source/artifact, CPU-policy and device guards match before/after.

Bulk rate and p99 improve in all six pairs: TCP 357.263 to 481.317 completed originals/s, p99 6927.865 to 1275.512 ms; QUIC 353.521 to 470.403/s, p99 7178.674 to 1472.977 ms. Witness bulk still has 539–745 queued originals at horizon across trials: zero bulk refusals is buffering, not refusal-free headroom. Overload remains queue-limited with refusals and seconds of tail latency.

The moderate QUIC regression is retained: p99 median 897.705 to 1891.831 ms, worse in two trials; witness refusals 43/0/3 versus atomic 0/0/0. Singleton TCP still refuses 19–20 of 256 with witness; QUIC refuses 15–19. This slice does not establish uniformly better latency.

Mean leader bulk TCP barrier 22.174 to 14.007 ms and ordinary publication 16.310 to 7.243 ms; QUIC 22.379 to 14.292 ms and 16.543 to 7.470 ms. Command/unit means fall roughly 9.05 to 7.71 TCP and 9.10 to 7.69 QUIC. The benefit is not a larger observed batch. Metadata-inclusive command/unit means are not literal proposal widths; encoded codec bytes are not device I/O. Primitive wall scopes nest and are not additive CPU. Ordinary witness namespace counters exclude cold selector/reclaim/repair and do not imply zero whole-process namespace work.

Raw ledgers and full controller/child logs are preserved. The fixture exposes no explicit missed-timer counter; 15 late offer decisions and dispatch tails are retained without calling them timer misses. Whole-process RSS is in Mac bytes and includes warmup/recovery/cleanup, not a sustained memory/debt plateau. Three voters share one APFS internal SSD; this is not independent placement or filesystem-only causation.

Preparation negatives, superseded configurations and the shared initial Linux sandbox discovery failure remain available. No Mac case failed or was replaced. Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. Source trees, binaries, buildtrees and generated WAL remain on their original hosts. FileLogIo remains default; real ext4/XFS and full P9 acceptance remain open.
