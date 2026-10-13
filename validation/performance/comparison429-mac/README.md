# Mac428 versus429 paired diagnostic

Original remote PID 70865 completed0/DONE, all 24 cases exit0, all stderr empty and
exact completion/frontier/envelope/observer checker exit 0. Three rotating trials per
cell,1048576 completed originals plus1024 warmups; one group/three voters, empty
payload, volatile stores/in-process RPC, clientruntime 1/server 16. Throughput timer
ends after all client joins and before latency sorting/follower verification and
shutdown. Each frontier[1049601;3]; peroriginal outcomes preserved. No competing
same-host builds/checks/copies/archive during timing. Source/profile/driver/artifact
before/after guards passed. Archive SHA256 7cd6f730e2d900d86799e0816b42ffcb3502eac6eb26dcdfd5857d6ec099ed2f.

| Cell | Variant | Median completed originals/s | Median p50 ms | Median p99 ms |
| --- | --- | ---: | ---: | ---: |
| clients4096/batch4 |428 off |2856831 |5.720875 |5.975625 |
| clients4096/batch4 |429 off |2826141 |5.804917 |6.076291 |
| clients4096/batch4 |429 on |2710231 |6.023833 |6.341000 |
| clients4096/batch4 |OpenRaft |2139994 |7.620917 |9.428834 |
| clients1/batch1 |428 off |54463 |0.017834 |0.033208 |
| clients1/batch1 |429 off |54696 |0.017958 |0.031750 |
| clients1/batch1 |429 on |50133 |0.019459 |0.033333 |
| clients1/batch1 |OpenRaft |9956 |0.100083 |0.136084 |

Candidate/baseline paired throughput ratios by trial: bulk0.947830/1.009661/0.989257;
singleton1.031061/0.962379/1.013088. Bulk trial 1 candidate throughput regresses5.217%
and p50/p99 regress5.485%/2.212%; trial3 throughput regresses1.074% and latency
1.469%/1.685%. Singleton trial 2 throughput regresses3.762% and p50/p99
2.817%/13.780%. Observer-on throughput regresses against candidate-off in allsix
pairs: bulk5.682%/8.403%/4.101%; singleton8.309%/5.907%/8.343%. Observerp50
regresses in allsix; p99 regresses in five (allbulk, singleton trials 1/3). All 24 exact
rates/p50/p99/exits and all 12 paired comparisons, including every improvement and
regression, remain in results/COMPACT.json and raw outputs. This does not establish
a repeatable Mac capacity gain; no failure, slow tail or regression was omitted.

CONFIG_PAIR.mac.json guards published immutable a46da2b (428) versus 832479d (429), same
actual Mac rustc1.98.0 binary/version identity, locks/Cargo/clippy and framework
opt3/debug2/assertions/overflow/thin-LTO/codegen1/packed profile. Four fresh:false
artifacts/root and pinned upstream compiled sources/manifests exact; every actual
Cargo-correlated dSYM file hash and original/copy bundle inventory exact. Both
build.rs/release oracle proofs retain ordinary assertions/overflow and exclude
extra dev full equality oracle. Baseline release receipt is copied here; full baseline
qualification remains ../mac428, candidate ../mac429. Inventory 1427→1428 adds only
entry_equality test module, modifies LogEntry equality and module loader, removes nothing;
all essential committed source/fixture/build inputs included. Guard/checker
refusal controls use three actual16384 CLI fixtures plus four CLI and four artifact
mutations, all correctly rejected. ON has 29 phases/four scopes/15 methods unchanged.

Attribution is inclusive elapsed wall time, often nested/overlapping; do not sum as
CPU time or infer allocator cause. Logical retained equal items are not physical
comparisons. Snapshot models differ and upstream append capacity is a soft merge
threshold per comparison contract. Offered-load refusal counts are not exposed.
These short volatile cases do not qualify sustained 20M floors, durable application,
headroom/fairness/skew, independent placement/barriers or P8 composition. Batch4
still misses 8422500 originals/s minimum. Regular files with SHA256SUMS.
