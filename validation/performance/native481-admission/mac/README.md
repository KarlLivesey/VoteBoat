# 481 Mac APFS admission comparison

Original SSH handle 81015 actually terminated with exit 0 after ALL_TIMING_END. All 48 native processes, frozen validators and readback checks passed; no case was retried or replaced. Collection began only after both original hosts terminated. Baseline is published 476 (7e9068296ce00dde5bf3e03bf4d852ed2833d185); candidate is published 480 (f7b0ef12709bb7e7aad4f0279851aa035b508e9e). Both explicitly select WitnessLogIo, with separately fresh native artifacts and identical within-host compiler/profile/tool settings.

| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline window refusals | Candidate window refusals |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| singleton | tcp | 7.406 | 7.406 | 7.406 | 7.406 | 158.366 | 160.100 | 19 | 19 |
| singleton | quic | 7.406 | 7.406 | 7.406 | 7.406 | 164.042 | 164.020 | 19 | 19 |
| moderate | tcp | 127.839 | 127.831 | 127.812 | 127.750 | 744.062 | 754.158 | 0 | 0 |
| moderate | quic | 127.829 | 127.852 | 127.781 | 127.797 | 779.149 | 1664.171 | 0 | 0 |
| loaded | tcp | 794.133 | 1065.505 | 459.250 | 784.125 | 6162.868 | 4870.050 | 8230 | 5631 |
| loaded | quic | 809.012 | 1177.929 | 483.625 | 995.625 | 6139.026 | 4376.000 | 8035 | 3989 |
| skew | tcp | 754.503 | 738.781 | 599.375 | 584.875 | 3350.969 | 3519.898 | 1394 | 1501 |
| skew | quic | 751.412 | 790.023 | 582.750 | 648.500 | 3436.286 | 3043.225 | 1508 | 1009 |

Three-trial arm medians, including refused offers rather than removing failed admissions. ANALYSIS.json retains every raw-recomputed trial and all 24 paired comparisons, intended/service/dispatch p50/p95/p99/max, completed/during/drain rates, all refusal classes, per-group backlog, resources, physical deltas and cold maintenance. Singleton is 256 offers at 8/s, window 1, ready 16; moderate 8192 at 128/s, window 256, ready 16; loaded 16000 at 2000/s, window 4096, ready 64; skew 8192 at 1024/s, window 4096, ready 64, two groups 31:1 with 2048 pending/group fixture bound. Every case has 64 warmups and active 2s maintenance. These finite offered caps are not maximum capacity, spare headroom or sustained 20M acceptance.

All 391680 offers reconcile to 296506 applied plus 95174 window refusals. Other no-leader/admission-refused/not-proposed/unknown/pending classes are zero. Every case recovers 64+applied, verifies original-operation retries and joins workers. Maintenance completes 624 checkpoints and 1872 reclaims. Every actual group has leader replica 1. All voters and both skew groups share the same physical APFS device.

All six loaded pairs improve completed throughput and p99. TCP medians rise 794.133 to 1065.505 completed originals/s and QUIC 809.012 to 1177.929/s. TCP refusals fall from 8230/7597/8383 to 6821/5279/5631; QUIC 8742/7653/8035 to 3989/3753/4579. Both still refuse many offers and drain after the horizon, so the 2000/s offered rate is not established useful capacity.

Regressions are retained. Moderate QUIC intended p99 worsens in all three pairs (ratios 2.541/1.753/2.136); its median rises 779.149 to 1664.171ms and candidate trial 1 adds 38 window refusals versus zero baseline. Moderate TCP p99 worsens in two pairs. Singleton TCP first pair loses 1.25% completed rate; median throughput stays 7.406/s while p99 rises 158.366 to 160.100ms. Skew TCP completed throughput loses in two pairs and its median falls 754.503 to 738.781/s.

| Skew group | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline median refused | Candidate median refused |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hot 7936 offers | tcp | 726.044 | 710.515 | 568.125 | 553.625 | 3351.944 | 3520.874 | 1394 | 1501 |
| Cold 256 offers | tcp | 28.459 | 28.266 | 31.250 | 31.250 | 454.874 | 835.434 | 0 | 0 |
| Hot 7936 offers | quic | 722.633 | 761.867 | 551.625 | 617.250 | 3437.262 | 3043.992 | 1508 | 1009 |
| Cold 256 offers | quic | 28.655 | 28.661 | 31.125 | 31.250 | 786.090 | 434.037 | 0 | 0 |

Every cold skew case applies all 256 offers with zero cold refusals. Cold TCP p99 nevertheless worsens in every pair: 454.874/454.299/923.457 to 761.284/835.434/997.624ms, median 454.874 to 835.434ms. Cold QUIC p99 improves in all three pairs, median 786.090 to 434.037ms. Per-group completed rates share the hot-drain elapsed denominator; they are not independent cold capacity. Hot candidate refusals remain TCP 1373/1501/1900 and QUIC 1200/806/1009, with hot backlog approaching the 2048/group ceiling.

Physical work remains visible. Loaded TCP leader barrier wall/native elapsed medians rise 0.347 to 0.502, with metadata-inclusive commands/unit 27.107 to 27.400 and barriers/completed original 0.03689 to 0.03650. Loaded QUIC fractions 0.352 to 0.546, commands/unit 28.345 to 26.394 and barriers/original 0.03528 to 0.03789. Skew barrier fractions remain about 0.89–0.90; whole-child CPU falls roughly 0.42 to 0.32 core-equivalent for TCP and 0.44 to 0.35 for QUIC. These overlapping elapsed fractions are not CPU attribution. Resources include warmup/recovery/cleanup. Ordinary namespace counters exclude cold selectors/reclaim/repair; append-unit counts include metadata and are not literal proposal widths, and codec bytes are not drive I/O.

185 late offer decisions and all dispatch distributions are retained. No explicit missed-timer count exists. Mac RSS is whole-process bytes, not a continuous memory/debt plateau. Shared preparation failures, superseded guard records, actual controller commands/streams and all 48 resource/validator/readback receipts remain available. The separate native480-admission/mac archive preserves qualification initial 101, resume 0 and missing runtime-scope append 0 distinctly.

Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. No source trees, binaries, build trees or generated WAL are included. FileLogIo and ready 16 remain defaults. No full P9, sustained acceptance, ext4/XFS acceptance or independent physical placement claim follows.
