#477 Linux paired native publication

Original58029 terminates0; all48 processes,48 independent validators and48 six-plus-six readback checks pass. No case retries/replacements. Root authorized collection only after both Linux58029 and Mac94749 actually terminated. Source476 `7e9068296ce00dde5bf3e03bf4d852ed2833d185`; schema/evidence published `c3aeed5c800fcb47e70a8b0df1cbd5f655d9dfe2`. Same fresh native executable `0bf40f6b4547c446f9db2a86a4bffe6d9ab223640d470154ec266a0d7cc2d064` serves both explicit atomic and witness arms, with identical native diagnostics. Fresh root/example/build-script proofs and compiler/source identities are sealed in476/linux and bound here by exact hashes.

Linux stable1.98.1/LLVM22.1.8/protoc3.9.0; native opt3/debug0/assertions+overflow/defaultLTO/defaultcodegen. One group, three voters sharing one Sabrent NVMe Btrfs compress=zstd:1 volume;8B Counter/64 warmups/ready16/capacity4096/maintenance2s/120s drain guard. Device and CPU policies match before/after; powersave/balance_performance remains disclosed and unchanged. Three trials rotate workloads/protocols/adjacent arm order;12 pairs start each arm. Singleton256offers/8s-1/window1; moderate8192/128s-1/window256; bulk8192/512s-1/window4096; overload8192/1024s-1/window4096.

Completed rates below include drain. During rates end at each offered horizon. P99 uses intended-to-completed latency of successful originals. All per-arm medians, ranges and trial values, plus nearest-rank p50/p95/p99/max intended/service/dispatch, are in final/READOUT.json.gz. Dispatch quantiles cover all scheduled offers.

| Cell | Protocol | Arm | Completed originals/s median | During/s median | p99 ms median | Window refusals trials1/2/3 |
| --- | --- | --- | ---: | ---: | ---: | --- |
| singleton | tcp | atomic | 5.094 | 5.094 | 692.156 | 88,93,105 |
| singleton | tcp | witness | 5.969 | 5.969 | 586.735 | 59,81,65 |
| singleton | quic | atomic | 4.818 | 4.844 | 803.990 | 96,100,108 |
| singleton | quic | witness | 6.062 | 6.062 | 535.246 | 68,62,55 |
| moderate | tcp | atomic | 114.243 | 113.484 | 5323.052 | 746,805,336 |
| moderate | tcp | witness | 121.546 | 121.281 | 4647.846 | 399,44,433 |
| moderate | quic | atomic | 120.452 | 120.391 | 5329.522 | 401,586,468 |
| moderate | quic | witness | 123.075 | 122.922 | 4181.977 | 401,83,308 |
| bulk | tcp | atomic | 198.032 | 177.250 | 19704.453 | 1474,1260,1194 |
| bulk | tcp | witness | 266.437 | 214.188 | 12856.661 | 688,71,1088 |
| bulk | quic | atomic | 163.538 | 120.375 | 23693.973 | 1983,2386,2170 |
| bulk | quic | witness | 303.161 | 270.750 | 10756.221 | 0,0,0 |
| overload | tcp | atomic | 221.167 | 190.625 | 18947.478 | 2582,2437,2824 |
| overload | tcp | witness | 242.424 | 257.375 | 16276.293 | 1953,3267,2037 |
| overload | quic | atomic | 191.985 | 178.500 | 22172.507 | 3100,2668,2609 |
| overload | quic | witness | 279.225 | 249.625 | 14347.258 | 2606,2089,2125 |

All24 paired total-completion rates improve, but intended p99 regresses in singletonTCPt2 (1.028132), singletonQUICt1 (1.086547) and moderateTCPt3 (1.028301). During-rate regressions are moderateTCPt3 (0.993445) and overloadTCPt2 (0.501507); the latter admits fewer originals and refuses3267 versus2437 despite its slightly higher total rate. Keep these original tails and refusals. Full service/dispatch ratios and every quantile/maximum/physical-duration regression flag remain in REGRESSION_INVENTORY.

| Cell | Protocol | Trial | Witness/atomic completed | During | Intended p50 | p95 | p99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| singleton | tcp | 1 | 1.175070 | 1.179641 | 0.558688 | 0.848585 | 0.581375 | 0.872950 |
| singleton | tcp | 2 | 1.073616 | 1.073620 | 0.834100 | 0.820562 | 1.028132 | 1.044954 |
| singleton | tcp | 3 | 1.266423 | 1.273333 | 0.623737 | 0.668123 | 0.847692 | 0.480179 |
| singleton | quic | 1 | 1.170602 | 1.168750 | 0.627808 | 0.861892 | 1.086547 | 1.215393 |
| singleton | quic | 2 | 1.258400 | 1.251613 | 0.499549 | 0.815991 | 0.962012 | 1.051448 |
| singleton | quic | 3 | 1.363461 | 1.367347 | 0.506913 | 0.609279 | 0.509423 | 0.574836 |
| moderate | tcp | 1 | 1.063923 | 1.068704 | 0.460963 | 0.648722 | 0.873154 | 0.858242 |
| moderate | tcp | 2 | 1.139173 | 1.134764 | 0.448279 | 0.285877 | 0.297617 | 0.333217 |
| moderate | tcp | 3 | 1.001743 | 0.993445 | 0.577238 | 0.853062 | 1.028301 | 1.019994 |
| moderate | quic | 1 | 1.003535 | 1.005301 | 0.486869 | 0.906408 | 0.954960 | 0.999136 |
| moderate | quic | 2 | 1.073550 | 1.086992 | 0.495791 | 0.662856 | 0.424379 | 0.453352 |
| moderate | quic | 3 | 1.021782 | 1.021025 | 0.739298 | 1.411484 | 0.784682 | 0.776117 |
| bulk | tcp | 1 | 1.345420 | 1.296142 | 0.780042 | 0.656168 | 0.652475 | 0.648519 |
| bulk | tcp | 2 | 1.517953 | 1.419252 | 0.574093 | 0.556331 | 0.540376 | 0.540902 |
| bulk | tcp | 3 | 1.073077 | 1.036527 | 0.994465 | 0.915902 | 0.916002 | 0.915344 |
| bulk | quic | 1 | 1.873754 | 2.072409 | 0.393935 | 0.443747 | 0.453964 | 0.464704 |
| bulk | quic | 2 | 1.931579 | 2.518605 | 0.419676 | 0.412651 | 0.414509 | 0.417530 |
| bulk | quic | 3 | 1.603225 | 2.232606 | 0.481781 | 0.653118 | 0.632903 | 0.635579 |
| overload | tcp | 1 | 1.224175 | 1.405246 | 1.003482 | 0.856924 | 0.853652 | 0.849795 |
| overload | tcp | 2 | 1.040840 | 0.501507 | 0.920362 | 0.854900 | 0.859022 | 0.856750 |
| overload | tcp | 3 | 1.180753 | 1.618711 | 0.738573 | 0.914744 | 0.912550 | 0.914293 |
| overload | quic | 1 | 1.759806 | 1.494483 | 0.539998 | 0.541297 | 0.542559 | 0.541188 |
| overload | quic | 2 | 1.496285 | 1.405462 | 0.799470 | 0.660891 | 0.675176 | 0.672245 |
| overload | quic | 3 | 1.454414 | 1.342972 | 0.744737 | 0.646218 | 0.647074 | 0.651395 |

297984 offers reconcile to249383 applied+48601 window refusals; other refusal/unknown/pending classes are zero. Exact unique tickets, contiguous application values, timing/window conservation, recovery64+applied, retries and worker joins pass.668 checkpoints/2004 reclaims/4 explicit skips are retained. No scalar storage error deltas; every physical publication step and manifest error count is zero. Raw checkpoints/reclaims/bases and all original per-operation outcomes remain available.

Bulk leader mean barrier/WALsync/publication medians atomic→witness: TCP41.605/14.203/27.395→28.152/14.115/14.026ms; QUIC48.612/16.108/32.496→24.963/12.717/12.286ms. These are nested elapsed wall contexts, not additive CPU. Counter deltas and each replica/step are retained; lifetime maxima are never subtracted and recovery-handle counters remain absolute. Cold checkpoint/reclaim latency sums include queuing/overlap and do not establish critical-path CPU or all file primitives. Ordinary witness namespace counters are zero; initialization/reclamation/repair namespace work is outside that scope.

Bulk TCP still reaches4096 inflight and roughly4096 pending at horizon in both arms; witnessQUIC accepts all8192 but retains3813–3892 pending at16s and drains afterward. Overload witnesses refuse1953–3267 TCP and2089–2606 QUIC originals. These cells are not maximum capacity or healthy/refusal-free headroom. Mean commands/appendunit is a physical suffix-command ratio including metadata transitions/retries, not literal proposal width; encoded bytes are not device I/O. Wholeprocess RSS/context-switch/CPU/block counts include warmup, drain, recovery and cleanup; block counts are not bytes and RSS peaks are not continuous memory/debt plateaus.

The native completion timer is unchanged: before measure loop to immediately after it, before ledger persistence/history validation/follower verification/worker joins/recovery. This differs from the framework all-client-join boundary. All successful cases still verify joins afterward. Independent validators run only after ALL_TIMING_END; source/tool/artifact and host identity guards also pass. No same-host builds/checks/copies/archiving/profiling competed with timing.

Preparation versions and failures remain distinct: initial Linux setup cannot see the real device inside the sandbox (lsblk32, no READY/workload); Linux2 is the preserved pre-alias freeze with no timing; Linux3 alone ran. Harmless Python ownership probes preserve success, signal and intentional metadata-write failure, not VoteBoat benchmark replacements. All actual scripts/configuration/commands/start/end/exits/readbacks, guard and host snapshots, raw histories and derived data are retained. Large files are lossless gzip with original/container hashes and lengths; SELECTION retains source paths/bytes/modes. Full source/build trees, executables/libraries, Python bytecode and generated voter WALs are excluded.

FileLogIo remains default. Finite Btrfs improvement does not close APFS/real ext4-XFS gain acceptance, sustained20M framework floors, bounded memory/debt, fair/skewed traffic, independent placement/shared physical barriers or P8 composition. No filesystem-only causation, universal speed claim or fullP9 completion follows.
