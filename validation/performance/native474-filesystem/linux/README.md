#474 same-device native filesystem comparison

Original27143 ends0/ALL_TIMING_END after391.355s; all12 workloads and12 independent validators pass. Same immutable455 native ELF13c6f446, no build/source/default change. All3 voters share one NVMe Btrfs compress=zstd:1 filesystem; freshdefault versus NoCoW changes CoW/compression/filesystemdata-checksum policy together. Engine WALCRC/durability/recovery unchanged. No ext4/XFS or isolated-CoW/three-machine claim.

| Protocol | Policy | Completed originals/s median | During horizon/s | p99 ms median |
| --- | --- | ---: | ---: | ---: |
| tcp | default | 151.528 | 150.333 | 1572.131548 |
| tcp | nocow | 174.645 | 174.667 | 996.205798 |
| quic | default | 157.988 | 157.667 | 1327.084479 |
| quic | nocow | 150.844 | 150.067 | 2094.807896 |

tcp NoCoW/default paired rate ratios 1.208805, 0.972365, 1.351998; p99 ratios 0.629608, 1.998262, 0.480549.

quic NoCoW/default paired rate ratios 1.105744, 0.810443, 0.899618; p99 ratios 0.707026, 1.898602, 1.745374.

No universal deployment recommendation or engine change is retained: TCPtrial2 loses rate and nearly doubles p99; QUICtrials2/3 lose both. Preserve everycase, including2.500493s TCPNoCoW and2.519605s QUICNoCoW tails. No case replacement/retuning or cause beyond combined policy is inferred. Larger-window offered-load evidence is next.

Everycase onegroup/3voters/8B Counter/64warmup/15000scheduledoffers500/s/window64/ready16/active2smaintenance/noPause. Alloffers reconcile to applied+windowrefused, unknown/pending/noleader/admissionrefused/notproposed0, recovery64+applied/retryverified/workersjoinedtrue. All hit64inflight, so this is a saturated fixed-point diagnostic, not refusal-free capacity or load curve. Both total/drain and during-horizon throughput, intended/service/dispatch latency and wholeprocess resources remain raw.

Replica1 configured-leader medians default→NoCoW: TCP meanbarrier43.916→37.735ms, WALsync15.035→12.677ms, manifestpublish28.869→25.048ms; QUIC40.876→41.520ms,14.085→13.903ms,26.778→27.601ms. Nested barrier wall86–88% and publication57–58% remain elapsed contexts, not additive CPU or per-original latency; commands/unit and barriers/original remain in READOUT. Lifetime maxima stay stage snapshots; no max counter subtraction.

Post-run attributes certify370regularfiles,18WALfiles in eacharm,0mismatches. CPU governor/EPP/driver/CPU sets remain exactly unchanged; powersave/balance_performance is disclosed, not assigned as cause. Full original/copy source1420/modes/root-and-example fresh:false/compiler/nativeopt3debug0assertionsoverflow/defaultLTO+CGU/validator guards pass beforeafter; published9559 restoration/fullhook and empty-source-worktree bindquietgrant. No builds/checks/copies/archives/profiles/competingbenchmarks duringtiming.

Original commands/stdout/stderr/exits, all12offer/storage/maintenance/bases/config/summary raws, validators, resources, beforeafter guards/attrs/host/config/controller/setupreview and derived readout are retained. Synthetic WAL states/binaries/fullsource/buildtrees remain under their originaltarget paths and are notduplicated. PACKING losslessly preserves originals; SHA256SUMS seals exactrecursive payloads. Wholecommand RSS is not continuous physicalmemory/debt; no full P9seven-cell/both-host healthy margin, headroom/fairness/placement/P8 scope closes.
