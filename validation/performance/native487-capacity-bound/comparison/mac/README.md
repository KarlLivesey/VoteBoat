# 487 Mac APFS Counter capacity-bound comparison

Original67726 actually terminated0 (tool148477) after48 fixed processes, validators, readbacks and post-guards. No case was retried or replaced. Collection began after both original hosts terminated. Baseline is published480 f7b0ef12709bb7e7aad4f0279851aa035b508e9e; candidate is published486 dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69. Both use WitnessLogIo, identical within-host compiler/tools and separately fresh native opt3/debug0/assertions/overflow/default-LTO-codegen artifacts. Configured worker counts are unchanged. No CPU affinity restriction or single-core execution is claimed.

| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU us/original | Candidate CPU us/original | Baseline average cores | Candidate average cores |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| singleton | tcp | 7.406 | 7.375 | 161.366 | 167.152 | 14653.364 | 14727.823 | 0.0977 | 0.0984 |
| singleton | quic | 7.375 | 7.406 | 165.098 | 166.158 | 21282.589 | 21546.464 | 0.1395 | 0.1424 |
| moderate | tcp | 126.669 | 127.858 | 772.463 | 760.857 | 1035.263 | 1032.512 | 0.1289 | 0.1297 |
| moderate | quic | 127.844 | 127.677 | 833.333 | 806.001 | 1465.797 | 1474.808 | 0.1822 | 0.1829 |
| loaded | tcp | 1070.849 | 1297.300 | 4854.187 | 3455.130 | 586.680 | 282.446 | 0.5336 | 0.3080 |
| loaded | quic | 1021.043 | 1332.900 | 4753.135 | 3357.463 | 618.483 | 318.240 | 0.5105 | 0.3427 |
| skew | tcp | 773.221 | 776.634 | 3127.944 | 3247.519 | 487.169 | 328.100 | 0.3236 | 0.2231 |
| skew | quic | 785.173 | 740.803 | 3119.642 | 3201.881 | 540.828 | 397.371 | 0.3495 | 0.2426 |

These are three-trial arm medians. Every individual pair, intended/service/dispatch distribution, refusal, recovery, retry, join, physical counter and maintenance record remains in READOUT.json/raw ledgers. All391680 offers reconcile to323608 applied plus68072 window refusals; all other status classes are zero. All1036 original transferred payloads have exact SHA256/length/mode proof.

All six loaded pairs improve completed throughput, p99 and whole-child CPU per completed original. Loaded TCP CPU ratios are0.536/0.458/0.479; QUIC0.515/0.568/0.491. Average busy cores also decrease, using child user+system CPU seconds divided by that same resource-helper child wall boundary. This avoids attributing gains to additional configured workers or using native-timer elapsed as the denominator. Whole-child CPU includes startup,64warmups, drain, verification, recovery and cleanup; CPU/original divides that broader numerator by measured applied originals and is not timer-only attribution. RSS is raw process bytes, not a continuous plateau.

Regressions remain: singleton CPU/original worsens in all six pairs. Moderate TCP p99 worsens in one pair (ratio2.439); moderate QUIC in two (1.045/2.304), including15 candidate refusals in trial2. Skew TCP p99 worsens in all three pairs and rate loses two; skew QUIC p99 and rate each lose two. Skew cold TCP p99 trials456.493/460.326/887.317ms become863.912/809.464/425.099; cold QUIC387.448/877.607/480.639 become884.601/458.588/1079.287. All cold cases still apply256/256 with zero refusals. No universal gain or fairness claim follows.

Singleton256@8/window1/ready16; moderate8192@128/window256/ready16; loaded16000@2000/window4096/ready64; skew8192@1024/window4096/ready64/groups2/31:1 with2048 pending/group. All have64warmups, active2s maintenance and120s drain. Finite offered horizons do not establish maximum/sustained useful capacity, healthy-margin framework floors, continuous bounded debt or full P9 acceptance. Per-group rates share whole-case drain elapsed and cannot replace one-group floors.

All voters/groups share one APFS physical device on the M2 Max. Source/artifact/compiler/device/CPU guards before/after are retained. Snapshot installs are aggregate replica3 counters and cannot identify individual delayed originals. No missed-timer counter exists; late offer decisions are not timer misses. Inclusive/nested storage/publication/cold elapsed scopes overlap and cannot be summed as CPU. Ordinary provider counters exclude cold selectors/repair/reclaim; append units include metadata and are not literal proposal widths. Encoded bytes are not drive I/O. No ext4/XFS or independent-placement acceptance is claimed.

Original command/stdout/stderr/exit records and collection provenance remain. A read-only initial path query error is retained; it did not modify or rerun the frozen reader. PACKING/SELECTION bind deterministic lossless gzip, original bytes/hashes/modes and archived representations. SHA256SUMS seals every payload recursively except itself. No source/buildtree/binary/WAL was transferred or archived. This archive finishes current487 evidence only; user requested commit/push and pause afterward.
