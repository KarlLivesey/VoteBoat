# Mac 430 versus 431 paired diagnostic

Original remote PID56466 completed0/DONE, all24 cases exit0 and stderr empty.
Before/after artifact guards and exact completion/frontier/envelope/observer checks
passed. Three rotating trials per cell,1048576 completed originals+1024 warmups;
one group/three voters, empty payload, volatile stores/in-process RPC, runtime1/16.
Timer ends at all client joins before latency sorting/follower verification/shutdown.
Every frontier[1049601;3]; every original outcome retained. No competing Mac
build/check/copy/archive/calibration guard during timing. Archive SHA256:
1848fcabe9c4f7ca95f5814497acb64ba4c95998fec6754a6aa8100991fa928a.

| Cell | Variant | Median completed originals/s | Median p50 ms | Median p99 ms |
| --- | --- | ---: | ---: | ---: |
| clients4096/batch4 |430 off |3930556 |4.123750 |5.112458 |
| clients4096/batch4 |431 off |4070259 |4.077166 |4.642917 |
| clients4096/batch4 |431 on |4104017 |4.148167 |4.664542 |
| clients4096/batch4 |OpenRaft |2085180 |7.698292 |9.721500 |
| clients1/batch1 |430 off |56083 |0.017416 |0.031291 |
| clients1/batch1 |431 off |56042 |0.017500 |0.031125 |
| clients1/batch1 |431 on |51420 |0.019042 |0.032500 |
| clients1/batch1 |OpenRaft |10121 |0.098292 |0.133917 |

Candidate ratios by trial: bulk0.976727/1.037013/1.032665, singleton1.014007/
0.990635/1.015607. Bulk trial1 throughput regresses2.327% and p50+1.445%, while
p99 improves3.198%; other bulk trials improve throughput and both latencies.
Singleton trial2 throughput regresses0.936%, p50+0.718%, p99+1.747%; trials1/3
improve those measures. Observer throughput regresses versus candidate in bulk1/3
by5.516%/2.654% and all singleton trials by7.785%/10.822%/8.247%. Observer p50
regresses in allsix; p99 regresses in five (bulk1/2 and all singleton trials).
Every exact rate/p50/p99/exit and all12 paired comparisons remain in
results/COMPACT.json, SUMMARY.json and raw outputs. No failure, tail or regression
removed. These mixed trials do not establish a consistent gain in every cell.

Immutable published4309d2b785 versus431ebcacbbb. Same actual Mac1.98.0 compiler
binary/version, locks/Cargo/clippy/build.rs and asserted framework opt3/debug2/
assertions/overflow/thinLTO/codegen1/packed profile. Four actual fresh:false
root/executable/pinned upstream/bench_minimal records and verbose invocations,
both dev/release-oracle gates and complete actual dSYM original/copy inventories
verified. Source1429→1430 adds owned_append_tests and changes receive/membership
fixture only, removes nothing; all essential committed inputs exact. Full root
and matched43 checks EXECUTED on431; no prior root scope reuse. Actual16384 CLI
fixtures plus eight rejection controls passed. Full qualification remains
../mac430 and ../mac431; baseline release receipt copied here. ON29 phases/four
scopes/15 methods unchanged; logical items are not physical comparison counts.

Attribution is inclusive elapsed wall time, nested/overlapping; do not sum as CPU
time or infer allocator cause. Snapshot models differ and upstream append capacity
is a soft merge threshold per comparison contract. Offered-load refusal counts
are not exposed. These short volatile cases do not qualify sustained20M floors,
real quorum durability, headroom/fairness/skew, placement/barriers or P8 composition.
8422500 originals/s is a batch4 minimum, not a target or stopping point; user wants
substantially more throughput. P9 remains active and incomplete. Regular files with
SHA256SUMS; no new data publication dependency changed the frozen source/artifacts.
