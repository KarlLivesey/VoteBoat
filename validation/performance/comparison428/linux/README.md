# Linux426/428 paired diagnostic

Original60660 terminated0/DONE: all24 rotating cases pass exact original,
request, frontier, maintenance, envelope, source/compiler/artifact and observer
guards. Each trial measures1,048,576 originals plus1024 warmups, singleton1/batch1
or bulk4096/batch4. No case was restarted. Mac timing remains separate.

| Cell | 426 OFF PUTS/s | 428 OFF PUTS/s | 428 ON PUTS/s | OpenRaft PUTS/s |
| --- | ---: | ---: | ---: | ---: |
| Bulk | 1,268,667 | 1,976,497 | 1,926,464 | 3,191,203 |
| Singleton | 100,026 | 101,833 | 85,264 | 20,181 |

Request p99 medians for426 OFF/428 OFF/428 ON/OpenRaft are bulk
15.521/11.978/10.764/7.065ms and singleton0.018876/0.018715/0.021330/0.125306ms.
Every OFF pair improves: bulk1.6826/1.5579/1.4777 and singleton
1.0518/1.0012/1.0181. ON singleton regresses in all three pairs against both OFF
variants. ON bulk is slower than candidate OFF in two trials and faster in one;
this does not establish observer speedup. All rates, p99 values, ratios and
regressions remain in linux-results/PAIRED.json, SUMMARY.json and raw reports.

Leader bulk protected verification takes89.204/93.168/92.552ms inclusive across
204 checkpoint calls. Whole compaction is101.416/108.033/106.798ms; these nested
intervals cannot be summed as CPU. Compact raw matches_state and cloned-state
calls are zero. Reconcile is0.149/0.174/0.175ms; state_to_pin gaps are
0.0186/0.0211/0.0216ms, pin_to_append0.777/0.957/0.942ms and
barrier_to_reconcile6.324/7.354/7.201ms. Logical equal-state retained entries are
4,354,605/4,338,173/4,344,877, not actual compared/skipped work. Full fallback is
inclusive in the protected phase. Suffix correlation has no separate phase;
its share and any allocator/CPU cause are not established here. Host admission
86.408–96.913ms and result handling86.253–94.412ms remain observed inclusive
costs. Candidate CPU profiling is the next diagnostic before selecting a fix.

CONFIG_PAIR uses published799f2cf baseline and publisheda46da2b candidate,
identical within-host compiler/locks/opt3/debug2/assertions/overflow/thinLTO/
codegen1/packed symbols. Candidate build.rs/reference-oracle profile proof and
DWP hashes are guarded separately; source inventories1418/1424 differ exactly
by six new428 files. OFF has no observer. ON29 phases/four scopes/15methods
retains old raw-Eq/clone phases and labels the new protected verification scope.
Historical426 raw-Eq totals are not relabeled as this protected phase.

This regular-file archive retains all raw reports, stderr/exits/resource timers,
unchanged drivers/config, preparation failures/controls, source/artifact/compiler
identities and original terminal status. It excludes binaries/source/build trees.
EVIDENCE.sha256 covers archived files. No failed timed case was omitted; all
slower tails remain. No GUI/browser/SVG is needed to read the exported data.

The bulk rate remains below matched OpenRaft and the8,422,500 minimum healthy
batch4 floor. These short volatile trials do not establish sustained20M/full
seven-cell acceptance, real quorum-durable application capacity, offered-load/
headroom/fairness/skew/placement/shared barriers or P8 composition performance.
P9 remains active and incomplete; P10 is excluded.
