#475 Linux native offered-load sweep

Original31476 completed once with actual exit0/ALL_TIMING_END; all54 native processes and all54 independent validators pass. Published source dfc195cd39196fc6f16e69ea74de34dabd359ef2; same native executable a3a062ff2a8ab9cb36f22a66737e5c0c52088b5a67f61b87897a5a7527072457 in every cell. Fresh artifact/compiler/profile proof is separately sealed in ../fresh-linux. This sweep changes the private admission fixture only; no production code, storage format or filesystem policy changed.

One group, three voters,8B Counter,64 warmups,8192 scheduled originals/case, ready16, active maintenance every2s, no pause. All cells use the common selected4096 admission profile. Rates32/128/512 originals/s have256/64/16s offered horizons; windows32/256/4096 rotate over three trials. Completed throughput includes drain; during throughput ends at the offered horizon. Latency below is intended-to-completed p99 among successful originals.

| Protocol | Offered/s | Window | Completed originals/s median | During/s median | p99 ms median | Window refusals, trials1/2/3 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| tcp | 32 | 32 | 30.143 | 30.125 | 1569.400 | 375, 606, 468 |
| tcp | 32 | 256 | 31.970 | 31.949 | 3525.289 | 0, 0, 0 |
| tcp | 32 | 4096 | 31.975 | 31.969 | 3159.952 | 0, 0, 0 |
| tcp | 128 | 32 | 68.798 | 68.469 | 1571.855 | 3987, 3267, 3778 |
| tcp | 128 | 256 | 119.963 | 119.859 | 5030.248 | 424, 400, 501 |
| tcp | 128 | 4096 | 127.724 | 127.562 | 5709.110 | 0, 0, 0 |
| tcp | 512 | 32 | 83.426 | 82.188 | 1343.261 | 6845, 6823, 7223 |
| tcp | 512 | 256 | 160.908 | 151.500 | 4649.846 | 6035, 4820, 5522 |
| tcp | 512 | 4096 | 200.436 | 183.875 | 18964.727 | 1095, 1576, 1181 |
| quic | 32 | 32 | 29.935 | 29.938 | 1946.394 | 518, 462, 965 |
| quic | 32 | 256 | 31.966 | 31.953 | 3720.212 | 0, 0, 0 |
| quic | 32 | 4096 | 31.971 | 31.957 | 3862.248 | 0, 0, 0 |
| quic | 128 | 32 | 71.920 | 71.734 | 1218.970 | 3167, 3611, 3569 |
| quic | 128 | 256 | 121.426 | 121.375 | 4885.058 | 403, 702, 112 |
| quic | 128 | 4096 | 125.550 | 126.203 | 5968.627 | 0, 0, 0 |
| quic | 512 | 32 | 92.010 | 91.062 | 1152.054 | 6459, 7199, 6703 |
| quic | 512 | 256 | 138.036 | 129.812 | 5869.476 | 5859, 6070, 4909 |
| quic | 512 | 4096 | 205.054 | 187.312 | 20051.894 | 999, 1099, 1483 |

All442368 offers reconcile to333153 applied+109215 explicit window refusals. Other refusal classes, unknown and pending are zero. Every case recovers64+applied, verifies retry outcomes and joins workers;2948 completed checkpoints,8844 completed reclaims and22 explicit maintenance skips are retained. All measurement storage error deltas are zero. Raw maintenance, bases, original offers and storage stage snapshots are retained; no failed/slow case replaced.

At512/s, widening32→4096 raises completed medians TCP83.426→200.436 originals/s and QUIC92.010→205.054, while p99 grows1.343→18.965s and1.152→20.052s. Every wide512 cell reaches4096 inflight with roughly4000 outstanding at the horizon; this is queueing, not refusal-free headroom. At128/s/window4096 all offers are admitted, but p99 medians5.709s TCP/5.969s QUIC are still poor. No latency SLO or healthy-headroom claim follows from eventual completion.

At512/window4096 leader mean barrier/WALsync/manifestpublication medians are TCP42.594/14.552/27.835ms and QUIC42.136/14.345/27.916ms. Physical commands/appendunit medians9.286/9.403 do not demonstrate ready64 batching or a4096-wide proposal. Leader barrier wall91–92% and manifestpublication60% remain nested elapsed contexts; never sum as CPU or infer per-original service latency. Per-trial/min/max counts, latency, width, barriers and all three replica deltas are in INDEPENDENT_ANALYSIS. Lifetime maximum counters are not subtracted.

All voters share the same Sabrent NVMe and Btrfs compress=zstd:1 volume. Before/after configured CPU policies match:16 CPUs amd-pstate-epp/powersave/balance_performance. The existing host-after snapshot was observed12.768s after the final workload ended, before this collection; it is preserved without replacement. There were no setting changes. Wholeprocess RSS peaks range13.3–27.4MB and include warmup, drain, recovery and cleanup; this is not a continuous-memory plateau or bounded-debt proof.

Original commands/stdout/stderr/exits, validator receipts, controller/configuration, resource collectors/readings, host snapshots, all raw result tables and derived readouts are retained. READOUT/SUMMARY pre-existed collection and were independently checked against every original ledger/storage/resource. PACKING preserves large originals losslessly; SHA256SUMS seals the exact recursive payload set. Source/build trees, binaries and generated WAL states remain local under target and are excluded. No workload was rerun.

The initial independently checked analysis reference usedfloor(n*99/100); it was corrected to the fixture conventionceil(n*99/100)-1. All54 original summary-percentile assertions and all numerical records/cells remain exactly equal. The initial script/readout/seal receipt and original manifest are preserved under percentile-correction; no workload reran.

This finite Btrfs baseline does not close sustained20M framework acceptance, APFS or pending ext4/XFS acceptance, any engine gain, independent physical placement, hot/cold fairness, skew or P8 composition requirements. A publication redesign must retain independent acknowledged-damage detection; eventual WALmarker validity cannot silently substitute for that contract.
