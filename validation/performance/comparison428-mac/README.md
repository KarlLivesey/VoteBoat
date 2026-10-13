# Mac426 versus Mac428 paired diagnostic

Original remote PID26380 completed24 cases with status0/DONE. Three rotating
trials per cell,1048576 completed originals plus1024 warmups. One group/three
voters, empty payload, volatile stores/in-process RPC, client runtime1/server16.
Timing ends at all client joins before latency sorting/follower verification and
shutdown. Actual frontiers[1049601;3] and all original outcomes verified. No failed
case and all24 stderr files are empty. No competing same-host setup/build/copy
activity during timing. Remote results archive SHA256
b8e03003981d9216bff80952063b8adbc6c5a01dbc1e96d7e786b2f46bb83c5f.

| Cell | Variant | Median completed originals/s | Median p50 ms | Median p99 ms |
| --- | --- | ---: | ---: | ---: |
| clients4096/batch4 |426 off |2899216 |5.608125 |6.180417 |
| clients4096/batch4 |428 off |2950267 |5.498500 |6.164542 |
| clients4096/batch4 |428 on |2889514 |5.627875 |6.283792 |
| clients4096/batch4 |OpenRaft |2121622 |7.618000 |10.000417 |
| clients1/batch1 |426 off |54559 |0.017917 |0.033042 |
| clients1/batch1 |428 off |53868 |0.018000 |0.034750 |
| clients1/batch1 |428 on |50308 |0.019375 |0.033667 |
| clients1/batch1 |OpenRaft |10004 |0.099416 |0.136375 |

Candidate/off versus baseline/off trial throughput changes: bulk -9.102%,+0.277%,
+1.761%; singleton +3.186%,-0.744%,-1.827%. Bulk trial1 candidate p50/p99 regress
11.570%/8.439%; bulk trial2 p50 regresses2.306%. Singleton trial2 p50/p99 regress
0.234%/5.169%; trial3 regresses0.698%/10.221%. Observer-on throughput regresses
versus candidate-off in bulk trials1/3 by3.411%/2.059% and singleton trials1/2/3
by8.560%/6.608%/7.880%; bulk trial2 improves3.144%. Every rate, latency and
regression (including observer tails) is retained in results/COMPACT.json and raw
outputs; medians do not establish a repeatable improvement on this host.

CONFIG_PAIR.mac.json proves immutable426799f2cf baseline versus published428a46,
same actual Mac compiler1.98.0 and source-lock/config profile. Source inventory
1422→1427 is explained in INVENTORY_SCOPE.json: all essential committed inputs
match, new build.rs/five source modules/.gitignore included; only documentation
plans omitted. Four fresh:false artifacts and complete actual dSYM bundles,
release oracle absence, driver hashes, upstream pin and pre/post source/artifact
identities are guarded. Actual16384 CLI fixtures and eight refusal controls are
in CHECKER_CONTROLS.json. Snapshot representation differences and upstream soft
append threshold retain the comparison contract. ON has29 phases/four scopes/15
methods; protected comparison phase elapsed includes possible full fallback,
logical equal items are not physical traversal counts. Attribution is inclusive,
nested/overlapping elapsed wall time; do not sum as CPU time or infer an allocator
cause. Offered-load refusal counts are not exposed by these diagnostic cases.

These short volatile cases do not qualify sustained20M floors, durable application,
headroom/fairness/skew, placement/barriers or P8 composition. Batch4 still misses
8422500 completed originals/s required minimum. All files are regular with
SHA256SUMS; no raw failure, slow tail or regression was removed.
