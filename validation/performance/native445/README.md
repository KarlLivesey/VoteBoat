# Configured64-wide durable batches

All36 measured cases pass: three rotating trials of quantum16/32/64 on TCP/TLS
and QUIC, on Linux and M2 Max. Each completes15000 originals plus64 warmups,
recovers value15064, verifies original retry outcomes and joins workers. One
group/three real Counter voters,8-byte payload/window64. Compiled sourceab091ea1,
native opt3/debug0/assertions+overflow/default LTO/codegen; validators912cdf4.
Within each host all policies use the same artifact/compiler/settings.

| Host | Protocol | q16 PUTS/s / p99 ms | q32 PUTS/s / p99 ms | q64 PUTS/s / p99 ms |
|---|---|---:|---:|---:|
| Linux | TCP/TLS | 247.827 / 714.750 | 359.402 / 529.898 | 572.176 / 280.593 |
| Linux | QUIC | 249.519 / 917.798 | 386.172 / 369.475 | 505.190 / 424.194 |
| M2 Max | TCP/TLS | 351.765 / 272.278 | 544.076 / 314.788 | 690.381 / 279.569 |
| M2 Max | QUIC | 346.365 / 764.764 | 553.430 / 135.404 | 753.943 / 94.878 |

Actual measured widths in every case:16×937+8,32×468+24,64×234+24.
Leader observed sync/publication counts fall to938 for q32 and470 for q64;
q16 varies1464–1649 on Linux and1698–1707 on Mac. These are observed live
measurement deltas; final joined totals remain in the raw CSV. I/O intervals
overlap/nest and cannot be summed as CPU time or a client critical path.

Mac q32/q64 throughput improves in all three pairs on each protocol, with TCP
latency regressions: q32 trials1/3 and q64 trial3. Linux TCP trial1 regresses
from its unusually fast q16 baseline2828.653/s to701.279/547.911; later q16
trials are246.930/247.827. QUIC q32 trial3 reaches3277.813/s, versus386.172/
384.727 earlier. All variation, slow tails and failures remain; their cause is
unestablished. Default16 stays. These bounded runs do not qualify sustained20M,
maintenance, fairness, offered-load headroom or physical RSS/debt bounds.

[Linux complete evidence](linux/measurements/README.md),
[Mac complete evidence](mac/measure/README.md). Setup archives preserve the old
rounding-check failures, exact mathematical correction, Mac absolute-path
publication-check no-ops and canonical-path rechecks without workload/build
repeats. Mac443's earlier relative-path publication check actually executed;
its original archive is unchanged. Native results stay separate from the volatile
OpenRaft framework comparison and do not satisfy its full Linux/Mac floors.
