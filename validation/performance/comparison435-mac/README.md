# Mac435 sustained bulk paired diagnostic

Retained434 04049e2b3f00ccfa7390cf92b0ace448b35ff62b versus published435
5c2390cb98d11901163e143815f5fa17199dc0f2 on the same M2 Max12core/96GiB Mac,
stable1.98.0/LLVM22.1.8 and asserted opt3/debug2/overflow/thinLTO/codegen1 profile.
Pinned OpenRaft e88bb40707f2e511d8ecc44d31edcb1d814a11a8; Linux1.98.1 differs and
is compared separately. Root library/executable/two upstream artifacts all fresh:false;
original/copy executable and complete dSYM identities, UUID and release build-script
oracle absence guarded. Source inventories1433→1436: three completion modules added,
four files changed, no removals. Full hashes and exact Mac archive permissions guarded.

Original remote PID95176, launchSSH32473, actual exit0/DONE. Twelve serial cases in three
rotating trials, clients4096/batch4,20M requested normalized identically to19,988,480
completed originals/4,997,120requests plus1024 singleton warmups; every frontier
[19,989,505;3]. All12 case exits0, stderr empty, no failures; before/after artifact guards
and exact observer/completion checker0. This archive has bulk only, no singleton run.

| Variant | Median completed originals/s | Median request p99 ms |
| --- | ---: | ---: |
| 434 OFF | 4,461,961 | 4.475917 |
| 435 OFF | 4,719,331 | 4.043792 |
| 435 ON | 4,540,429 | 4.339500 |
| OpenRaft OFF | 1,991,441 | 10.478542 |

435 OFF/434 OFF throughput ratios1.028181/1.057681/1.044275; corresponding p99 ratios
0.937651/0.895643/0.907077. Every paired trial improves both. Observer ON/OFF rates
0.977906/0.945511/0.974522 and p99 ratios1.017090/1.150643/1.068842; all observer
regressions kept, including trial2 p99 4.652959ms. Raw values, times and commands remain.

Candidate OFF checkpoints3887–3895 per replica, follower snapshot installs1150–1565;
final retained entries1860–5980, logical leader peaks22096–22172/follower peaks≤8044,
peer queue≤6 and client queue4096. Every replica catches up to its exact frontier.
Logical counts are not RSS bytes or physical traversal. No dedicated refused/rejected
counter, offered-load/headroom or fairness evidence in this successful closed-loop cell.
Observer29 phases/four scopes/15methods; inclusive elapsed wall scopes overlap and are
not CPU time. See COMPACT.json for every trial's maintenance and queue history.

Framework one group/three voters, empty payload, volatile stores, in-process RPC,
client runtime1/server16, default maintenance, equivalent batch4 API envelopes,
ordinary replication1024 and all per-original completion channels unchanged.
Throughput ends after all client joins and before latency flatten/sort, verification and
shutdown. VoteBoat historical checkpoint image and OpenRaft current-state snapshot
representations differ; upstream append capacity is a soft merge threshold. This is
framework evidence, separate from native disk-backed TCP/TLS/QUIC or quorum-durable
application measurements. Required8,422,500 minimum, healthy matched margins, full
curve/both hosts, offered load/fairness/skew/placement/P8 remain unachieved.

Preparation originalSSH63472 failed before timing on Linux-specific git_modes lookup.
Actual Mac SOURCE.modes preserved; focused guard-only SSH90377 retry0 verifies the full
0o664/0o775 permission map and same executable bits. Raw failure, original scripts and
minimal correction retained in preparation/attempt1 and MODE_GUARD_FIX.diff. Neither
compiled source nor qualification/build was rerun. Qualified before/after source
permissions and current read-only comparison freeze are explicitly distinguished in
PREPARATION_HISTORY.json. Actual CLI3, output refusal4 and artifact/dSYM/module refusal9
controls pass. No browser/new tool/policy/dependency/source optimization or rerun.

Bulk archive SHA34962ddc819cf41ce23f1b532eb2850226c29b3cfc738f0d02c829a1136f0c3a;
setup archive SHA10a5ee1a9b03f57573a7ef9c5f529480fbf7426e54f00af1d841bb02b2ee358b.
Regular raw/provenance files only; no executable/source/build duplication. SHA256SUMS
covers each file other than itself. README rates rounded; raw reports are authoritative.
