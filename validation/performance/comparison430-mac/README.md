# Mac429 versus430 paired diagnostic

Original PID97258 completed0/DONE, all24 case exits0 and stderr empty. Exact
original/frontier/envelope/observer checker and before/after artifact guards passed.
Three rotating trials per cell,1048576 completed originals plus1024 warmups; one
group/three voters, empty payload, volatile stores/in-process RPC, runtime1/16.
Throughput ends at all client joins, before latency sorting/follower verification
and shutdown. Every frontier[1049601;3]; per-original outcome channels remain.
No same-host builds/checks/copies/archives during timing. Raw archive SHA256:
81d874bc8f7c43821918045725a36d543c919cb2b8f9d75e020bdfa87822088b.

| Cell | Variant | Median completed originals/s | Median p50 ms | Median p99 ms |
| --- | --- | ---: | ---: | ---: |
| clients4096/batch4 |429 off |2838807 |5.673459 |6.490833 |
| clients4096/batch4 |430 off |4302495 |3.832417 |4.679459 |
| clients4096/batch4 |430 on |4429382 |3.803417 |4.734083 |
| clients4096/batch4 |OpenRaft |2183750 |7.522708 |9.137250 |
| clients1/batch1 |429 off |55494 |0.017666 |0.031459 |
| clients1/batch1 |430 off |55021 |0.017833 |0.033167 |
| clients1/batch1 |430 on |51032 |0.019125 |0.033125 |
| clients1/batch1 |OpenRaft |10037 |0.099333 |0.134958 |

Bulk candidate/baseline ratios1.408068/1.515600/1.598636, with all three p50/p99
improvements. Singleton ratios1.009506/0.974642/0.994265: trials2/3 throughput
regress2.536%/0.574%, p50 regress1.891%/0.700%, p99 regress10.328%/1.600%.
Observer throughput regresses against candidate in bulk1/2 by0.394%/0.946%, and
all singleton trials by7.281%/5.648%/8.041%; bulk3 improves3.050%. Observer p99
regresses in all bulk trials and singleton1/3, while singleton2 improves. All24
exact rates/p50/p99/exits and all12 paired comparisons remain in results/COMPACT.json,
SUMMARY.json and raw outputs; no failure, slow tail or regression removed.

Published immutable429832479d versus4309d2b785; same actual Mac compiler1.98.0,
locks/Cargo/clippy and opt3/debug2/assertions/overflow/thinLTO/codegen1/packed
profile. Four actual fresh:false library/executable records and verbose compiler
invocations, both build.rs/release cfg gates and complete Cargo-correlated dSYM
inventories verified. Source1428→1429 adds actor/tests/replies.rs, changes actor
and test loader only, removes nothing; essential source/fixtures/build inputs exact.
Candidate root scopes reuse passed429 on1401 exact source inputs plus3 contextual
files; standalone matched43 and fresh builds/CLI controls executed on430. Full
qualification remains ../mac429 and ../mac430; baseline release receipt copied here.
Three actual16384 CLI fixtures and eight rejection controls passed. ON29 phases,
four scopes,15 methods unchanged. Logical equal items are not physical comparisons.

Attribution is inclusive elapsed wall time, nested/overlapping; do not sum as CPU
time or infer allocator cause. Snapshot models differ and upstream append capacity
is a soft merge threshold per comparison contract. Offered-load refusal counts
are not exposed. These short volatile cases do not qualify sustained20M floors,
durable application, headroom/fairness/skew, placement/barriers or P8 composition.
Batch4 still misses8422500 completed originals/s minimum. Regular files with SHA256SUMS.
