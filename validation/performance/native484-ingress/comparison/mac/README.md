# 484 Mac APFS scoped ingress comparison

Original Mac controller 2749 terminated with exit 0 after all 48 original cases, validators, readback checks and post-guards. No case was retried or replaced. Collection began after both original hosts terminated. Baseline is published 480 f7b0ef12709bb7e7aad4f0279851aa035b508e9e; candidate is published 483 efd383dc36b4b9edf8f66021b90115824440b468. Both use WitnessLogIo and separately fresh native opt3/debug0/assertions/overflow/default-LTO-codegen artifacts with identical within-host compiler/tool identities. The separate mac qualification archive records fresh builds and functional qualification.

| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| singleton | tcp | 7.406 | 7.406 | 7.406 | 7.406 | 165.053 | 160.915 |
| singleton | quic | 7.406 | 7.375 | 7.406 | 7.375 | 163.256 | 162.872 |
| moderate | tcp | 127.832 | 127.845 | 127.781 | 127.797 | 1306.882 | 830.698 |
| moderate | quic | 127.823 | 127.873 | 127.797 | 127.828 | 775.851 | 811.113 |
| loaded | tcp | 1078.570 | 1342.682 | 827.375 | 1216.125 | 4709.463 | 3067.395 |
| loaded | quic | 1110.755 | 1499.466 | 843.250 | 1331.750 | 4625.423 | 2676.933 |
| skew | tcp | 797.170 | 738.905 | 639.000 | 566.500 | 3109.447 | 3438.682 |
| skew | quic | 785.004 | 732.471 | 625.750 | 576.750 | 3099.940 | 3304.539 |

These are three-trial arm medians. READOUT.json retains every trial, all 24 adjacent paired comparisons, all refusal classes, per-group latencies, physical deltas and resource scopes. All 391680 offers reconcile to 330860 applied and 60820 window refusals; other refusal/outcome classes are zero. Every case checks recovery, original-operation retries, worker joins, active maintenance and physical provider diagnostics. All actual exits are zero. All 1036 original transferred payloads have exact SHA256, length and mode verification.

All six loaded pairs improve completed throughput and p99, but the candidate fails portable retention. Mac skew TCP loses completed throughput in all three pairs; skew QUIC loses two. All six cold-skew p99 pairs regress, while every cold case still applies all 256 originals with zero cold refusals. Cold TCP p99 median rises 383.173 to 854.009 ms; cold QUIC 411.649 to 809.077 ms. Moderate TCP candidate trial 2 has 67 window refusals and p99 ratio 2.770; moderate QUIC p99 worsens in two pairs. Singleton QUIC completed rate loses in all three pairs. Independent recomputation agrees with both hosts and records Linux 17/24 completed-rate losses. The candidate is rejected as a portable optimization; no universal gain claim follows.

Singleton is 256 offers at 8/s/window1/ready16; moderate8192 at128/s/window256/ready16; loaded16000 at2000/s/window4096/ready64; skew8192 at1024/s/window4096/ready64/groups2/weights31:1 with2048 pending per group. All cases retain64 warmups, active2s maintenance and120s drain. Finite offered caps and horizons do not establish maximum sustained capacity, healthy-margin floors, memory/debt plateau or full P9 acceptance. Per-group rates share the whole-case elapsed boundary and cannot replace a one-group floor.

All three voters and both skew groups share the M2 Max APFS physical device. Actual AC Power was captured before timing. Source, compiler, artifact, device and CPU guards are retained before/after. Snapshot-install evidence is aggregated on replica3 and cannot locate a particular delayed original. No missed-timer counter exists; late offer decisions are not timer misses. Whole-child CPU and raw RSS bytes include startup, warmup, drain, verification, recovery and cleanup. They are not timer-only attribution or a continuous memory plateau.

Nested elapsed scopes overlap and cannot be summed as CPU or per-original waiting. Append units include metadata transitions and are not literal proposal widths. Ordinary publication primitives exclude cold selectors, repair and reclaim; encoded bytes are not drive-I/O bytes. There is no independent physical placement or ext4/XFS acceptance here.

The first display-only compact helper field error is retained; it did not alter raw data or the frozen reader. Independent review retains its initial parser failure and focused correction. Original process streams, collection commands, errors, validators, source/tool guards and every slower trial remain available. No browser, images, profiling, policy changes or replacement workload was used for this readout.

PACKING.json records lossless gzip of large or whitespace-sensitive originals. SELECTION.json binds every selected original hash, byte length and mode to its archived representation. SHA256SUMS seals all payloads recursively. No source trees, executables, build trees or generated WAL are included.
