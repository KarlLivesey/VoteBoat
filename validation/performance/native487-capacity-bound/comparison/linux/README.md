# Linux487 paired Counter capacity-bound results

Original controller 44616 actually terminated with exit 0/tool 8d63f2 after all 48 END records and ALL_TIMING_END/CAMPAIGN_EXIT0. Both hosts actually terminated before readout. All 48 native processes, frozen validators and physical readbacks pass; no case was replaced/retried. Baseline480 f7b0ef1 versus candidate486 dcdcafe3, fixed three durable voters, Witness provider, 8B payload and unchanged within-host compiler/profile/runtime/worker settings.

| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU us/original | Candidate CPU us/original | Baseline avg busy cores | Candidate avg busy cores |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| singleton | tcp | 5.562 | 5.531 | 779.728 | 754.867 | 19849.747 | 21573.335 | 0.0966 | 0.0993 |
| singleton | quic | 5.937 | 6.000 | 705.812 | 638.553 | 19490.068 | 18986.572 | 0.0954 | 0.0952 |
| moderate | tcp | 124.974 | 122.687 | 3046.693 | 3418.042 | 944.704 | 958.682 | 0.1136 | 0.1149 |
| moderate | quic | 125.590 | 127.476 | 2889.078 | 1759.514 | 970.304 | 954.887 | 0.1191 | 0.1180 |
| loaded | tcp | 629.570 | 753.827 | 7052.436 | 5949.939 | 479.211 | 280.340 | 0.2700 | 0.1778 |
| loaded | quic | 702.331 | 810.306 | 6297.598 | 5489.118 | 483.184 | 258.221 | 0.2662 | 0.1795 |
| skew | tcp | 398.857 | 402.549 | 6824.550 | 6567.673 | 529.754 | 442.116 | 0.1758 | 0.1474 |
| skew | quic | 443.638 | 371.598 | 6249.932 | 6392.711 | 521.031 | 481.811 | 0.1757 | 0.1475 |

Three-trial medians; READOUT preserves all individual trials/ranges, 24 pairs and every regression. All 391680 offers conserve to 253450 completed originals + 138230 window refusals. All other refusal classes, unknown and pending are zero. Completed rate uses the native original horizon+drain timer; during/drain rates and nearest-rank intended/service/dispatch p50/p95/p99/max remain in JSON. Intended/service cover applied originals, dispatch covers all offers. Every case recovers 64+applied, verifies original retries and joins workers; 624 completed checkpoints/1872 reclaims/zero maintenance skips are retained.

CPU_LINUX.json recomputes raw user+system CPU us/completed original and average busy cores=(user+system)/SAME whole-child elapsed seconds. The CPU and elapsed boundaries include startup/warmup/maintenance/drain/refused-offer work/verification/recovery/cleanup; they are wider than the native throughput timer. No CPU-wider/native-timer division, CPU-stage causality or continuous memory/debt plateau is claimed. Fixed worker/thread settings did not change.

Loaded TCP median CPU/original falls 479.211→280.340us and QUIC 483.184→258.221us, with same-boundary average busy cores falling 0.2700→0.1778 and 0.2662→0.1795. Loaded TCP paired throughput ratios 0.859/1.325/1.197 retain the first-trial loss; QUIC 1.081/1.603/1.154. Across all 24 pairs, 10 lose throughput, 8 worsen p99, 7 increase CPU/original and 6 increase average busy cores. Singleton TCP and moderate TCP medians regress on CPU/original; no universal improvement is asserted. Skew QUIC median throughput 443.638→371.598/s regresses despite lower CPU/original.

All 256 cold offers apply in every skew case with zero cold refusals. Median cold intended p99 TCP 1298.238→1287.555ms and QUIC 1256.863→1201.789ms; all trial cold/hot tails/backlogs/refusals remain. The fixture caps each group at 2048 pending and leads both groups on replica 1, so this is not unrestricted fairness or independent placement. Three voters share one Btrfs volume/device; aggregate skew cannot replace a one-group floor.

All per-replica barrier/WAL-sync/publication/cold-maintenance counters and physical readbacks are retained. Nested elapsed wall scopes overlap and cannot be summed as CPU; append units include metadata and are not literal proposal width. Encoded codec bytes and process block counters are not physical I/O bytes. Peak RSS has whole-child scope; raw Linux KiB is converted to bytes.

Fixed matrix: singleton q16/256@8/window1; moderate q16/8192@128/window256; loaded q64/16000@2000/window4096; skew q64/8192@1024/window4096/groups2/31:1. 64 warmups, active 2s maintenance, original 120s drain and retained-entry limit unchanged. This finite diagnostic is not maximum/sustained20M acceptance or a debt plateau. Before/after exact source/modes/artifacts/tool/validator/device/CPU-policy guards and all raw errors remain.

The initial supplementary CPU helper failed before writing output because its SHA helper expected Path while __file__ was str; only Path conversion changed. Initial helper/error/exit and successful same-raw corrected output are preserved. Frozen raw reader was unchanged. No workload/build/check suite/profile was rerun during collection.

Raw stdout/stderr/patches and whitespace-sensitive/large text are reversibly gzip-packed. SELECTION records exact original hashes/lengths/modes; PACKING records containers; SHA256SUMS seals all payloads. No binaries/full production source/buildtrees/generated WAL/snapshot/cache states are archived. The earlier fresh486 qualification archive remains separately sealed. This completes only current487 wrap-up before the requested pause. FullP9 and ext4/XFS/sustained/composition acceptance remain incomplete; no next optimization/profile/preparation was started.
