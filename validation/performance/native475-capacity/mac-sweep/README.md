#475 Mac APFS finite offered-load sweep

Original campaign29711 persisted status0/ALL_TIMING_END. All54 processes and54 independent validators end0; no cases restarted or replaced. Same immutable native475 executable/common4096 capacity profile: onegroup/threevoters,8192 offers+64warmups,ready16,maintenance2s,TCP/QUIC,rates32/128/512,windows32/256/4096,three rotating trials. All voters share APFS disk3s1/internalSSD disk0s2. This is a finite capacity diagnostic, not a candidate/baseline speed acceptance or20M sustained proof.

| Protocol | Offered/s | Window | Completed originals/s | During/s | p99 ms | Window refusals | Backlog | Commands/append unit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| tcp | 32 | 32 | 31.807 | 31.797 | 951.157 | 47 | 5 | 0.866 |
| tcp | 32 | 256 | 31.988 | 31.984 | 1099.909 | 0 | 4 | 0.873 |
| tcp | 32 | 4096 | 31.985 | 31.980 | 1023.708 | 0 | 5 | 0.872 |
| tcp | 128 | 32 | 112.119 | 111.938 | 910.986 | 1005 | 23 | 3.016 |
| tcp | 128 | 256 | 127.687 | 127.656 | 916.234 | 0 | 19 | 3.392 |
| tcp | 128 | 4096 | 127.709 | 127.656 | 849.589 | 0 | 22 | 3.389 |
| tcp | 512 | 32 | 160.609 | 159.125 | 792.431 | 5614 | 32 | 4.516 |
| tcp | 512 | 256 | 336.854 | 333.750 | 1631.833 | 2619 | 241 | 8.910 |
| tcp | 512 | 4096 | 364.009 | 349.438 | 6499.404 | 0 | 2601 | 9.204 |
| quic | 32 | 32 | 31.800 | 31.797 | 953.012 | 48 | 4 | 0.863 |
| quic | 32 | 256 | 31.985 | 31.977 | 1098.444 | 0 | 6 | 0.871 |
| quic | 32 | 4096 | 31.988 | 31.984 | 921.072 | 0 | 4 | 0.867 |
| quic | 128 | 32 | 112.278 | 112.078 | 899.151 | 992 | 27 | 2.985 |
| quic | 128 | 256 | 127.557 | 126.734 | 863.547 | 0 | 21 | 3.362 |
| quic | 128 | 4096 | 127.776 | 127.672 | 865.192 | 0 | 21 | 3.362 |
| quic | 512 | 32 | 160.915 | 160.938 | 641.862 | 5592 | 25 | 4.475 |
| quic | 512 | 256 | 282.734 | 287.750 | 3299.178 | 3332 | 239 | 8.834 |
| quic | 512 | 4096 | 363.740 | 347.125 | 6535.222 | 0 | 2638 | 9.163 |

Three-trial medians; all individual trials, intended/service/dispatch latency, all refusal classes, backlog/drain, resource records and exact storage deltas are retained in INDEPENDENT_ANALYSIS.json. Successful-original p99 uses the fixture nearest-rank convention ceil(n*99/100)-1. All442368 offers reconcile to384624 applied and57744 window refusals; no unknown/pending/other refusal classes. All cases recover64+applied, verify original-operation retries and join workers. Active maintenance completes2970 checkpoints and8910 reclaims.

At offered512/s and window4096, TCP/QUIC complete364.009/363.740 originals/s with p996499.404/6535.222ms and median backlog2601/2638. Zero refusals there means buffering, not refusal-free headroom: achieved rate remains below512/s. Mean leader barrier22.226/21.857ms consists of nested WALsync5.872/5.628ms and publication16.351/16.227ms; publication is about73–74% of barrier elapsed. Do not sum overlapping attribution as CPU time. Physical commands/appendunit9.204/9.163 is distinct from a4096-wide append or ready16/Core64 bounds.

Mac RSS is raw bytes, covering the entire process including warmup/recovery/cleanup; it is not a continuous plateau/debt proof. Before/after host power/thermal/process/filesystem observations are retained without changing settings. All timing completed before collection/analysis. No universal filesystem gain, independent placement, publication-format change, or safety relaxation is claimed. Real ext4/XFS scratch and all full P9 acceptance remain pending. Large originals retain lossless PACKING provenance; raw WAL/binaries/buildtrees stay on the Mac.
