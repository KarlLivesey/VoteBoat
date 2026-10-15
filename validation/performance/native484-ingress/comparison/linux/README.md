# Linux484 paired native ingress results

Original controller 84364 actually terminated with exit 0 (tool chunk 52f183), with 48 END records, ALL_TIMING_END and CAMPAIGN_EXIT 0. Both host terminals preceded collection. All 48 native processes, frozen native validators and physical readback checks passed; no case was replaced or retried. Baseline480 f7b0ef1 and candidate483 efd383d use their original fresh native artifacts, explicit WitnessLogIo and three durable voters.

| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline window refusals, trials | Candidate window refusals, trials |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| singleton | tcp | 6.339 | 5.750 | 6.312 | 5.750 | 426.399 | 712.197 | 58,50,53 | 89,59,72 |
| singleton | quic | 6.375 | 5.844 | 6.375 | 5.844 | 480.930 | 618.535 | 52,48,73 | 77,69,58 |
| moderate | tcp | 127.001 | 123.067 | 126.203 | 122.938 | 1999.864 | 4182.107 | 129,31,47 | 308,353,118 |
| moderate | quic | 126.740 | 123.570 | 126.781 | 123.531 | 2112.988 | 3272.071 | 24,64,56 | 275,324,77 |
| loaded | tcp | 807.092 | 629.374 | 632.125 | 547.375 | 5763.106 | 8149.859 | 6847,6579,8408 | 8588,7167,7525 |
| loaded | quic | 765.733 | 764.098 | 603.500 | 571.625 | 6125.799 | 5747.768 | 5,7076,8282 | 2754,7368,7743 |
| skew | tcp | 359.715 | 404.957 | 251.625 | 269.500 | 7475.871 | 6741.002 | 3700,4127,4479 | 4256,3967,3551 |
| skew | quic | 422.705 | 429.684 | 279.125 | 286.750 | 6201.337 | 5980.634 | 3901,3584,3951 | 3579,3823,3952 |

Three-trial medians, not a pooled score. All 391680 scheduled offers reconcile to 263904 applied originals and 127776 window refusals. Every other refusal class, unknown outcome and final pending count is zero. All cases recover 64+applied, verify original retry outcomes and join workers. There are 624 completed checkpoints, 1872 reclaims and zero maintenance skips. READOUT retains all 48 individual records, 24 adjacent pairs, median/min/max/trials, completed/during/drain rates, intended/service/dispatch nearest-rank p50/p95/p99/max, raw-ledger conservation and per-group values/backlogs. Intended/service quantify successful originals; dispatch includes all offers.

Results are mixed with substantial regressions: 17/24 pairs lose completed throughput, 16/24 worsen intended p99, and 20/24 have either loss. Singleton TCP and both moderate protocols lose throughput in all three pairs. Loaded TCP paired ratios are 0.634/0.780/1.508; QUIC 1.029/0.998/0.925. These finite measurements do not establish a universal improvement. All prior481 regressions remain explicit reference context in REFERENCE481_CONTEXT.json; no prior score is spliced into this campaign.

| Loaded protocol | Baseline CPU us/original | Candidate CPU us/original | Baseline leader barriers/original | Candidate leader barriers/original | Baseline mean barrier ms | Candidate mean barrier ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tcp | 439.932 | 480.890 | 0.03605 | 0.03566 | 24.829 | 31.588 |
| quic | 421.666 | 458.409 | 0.03597 | 0.03649 | 27.207 | 26.118 |

Whole-child CPU includes warmup, maintenance, recovery, startup/cleanup and refused-offer work. CPU/original uses this wider boundary and is not stage attribution or measurement-only CPU. No allocator or causal inference is made. Whole-child RSS ranges from 15093760 to 36728832 bytes; Linux raw KiB is converted to bytes. This is not a continuous memory/debt plateau. Block counters are not byte counts. Host-poll maxima and per-original elapsed values describe fixture rounds, not individual visits.

Physical work retains all three replicas, barrier/WAL-sync/publication deltas and primitive counters, plus cold initialization/reclaim/recovery stages. Nested elapsed scopes overlap; do not sum them as CPU time. Ordinary Witness publication has one WAL sync plus one witness-file sync, with no per-barrier file open/rename/directory sync. Metadata-inclusive commands/append-unit are not literal proposal width; codec bytes are not physical device I/O.

| Skew cold group | Baseline p99 ms | Candidate p99 ms |
| --- | ---: | ---: |
| tcp | 1777.798 | 1512.052 |
| quic | 1064.944 | 1264.328 |

All 256 cold offers eventually apply in every skew case, with zero cold refusals; TCP cold tails improve in the median while QUIC worsens. Hot/cold backlog, all per-group refusals and trial tails are preserved. Both groups lead on replica1 and the fixture limits each group to 2048 pending. This does not prove unrestricted cold fairness. All replicas share the same Btrfs volume/device, so aggregate skew throughput cannot replace a one-group floor or independent placement.

Fixed cells remain singleton q16 256@8/window1, moderate q16 8192@128/window256, loaded q64 16000@2000/window4096, skew q64 8192@1024/window4096/groups2/31:1. There are 64 singleton warmups, active 2-second maintenance and the original 120-second drain bound. The existing retained-entry guard remains unchanged. This is a finite offered-load diagnostic, not sustained 20M-operation acceptance or maximum capacity.

Before/after guards bind actual source bytes/modes, artifact original/copy identities, compiler/protoc/profile/runtime tools, fixture permissions and the complete validator closure. Linux stable1.98.1/LLVM22.1.8/protoc3.9.0 is unchanged within host; native opt3/debug0/assertions/overflow/default LTO and codegen remain unchanged. Source inventories contain 1495 baseline and 1503 candidate exported inputs, including supplemental schemas; candidate publication qualification covers 1502 inputs plus its exported schema. Fresh root/executable/buildscript freshness is established separately in the published480 and484 qualification archives. No build, profile, policy or filesystem changes occurred during timing.

The first Linux preparation failed under sandbox-only device visibility before READY, without a workload. Its raw error and original prepared files are retained beside revision2 host-visible setup and the exact final config. Unpublished sentinel refusal, focused qualification guard attempts and every raw stderr/output/exit remain. Raw logs are losslessly gzip-packed, including whitespace-sensitive small stdout/stderr. SELECTION records original hashes, lengths and modes; PACKING records reversible containers; SHA256SUMS seals every payload. No binaries, source trees, build trees, generated WAL/snapshot states or caches are included.

P9 remains active and incomplete: full sustained framework margins, real ext4/XFS acceptance, bounded debt/headroom, fairness, independent placement/shared physical barriers and P8 composition remain open. This diagnostic does not redefine the full goal.
