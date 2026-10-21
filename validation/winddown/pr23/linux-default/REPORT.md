# PR23 exact1c — default FAILED; parent CLOSED / PARKED

Tested frozen head: 1c2595364cbe405922695ce9c8728316ecf05741. **Actual outer exit1.**
All129/129 Cargo-discovered test harnesses executed exactly once: **2639 passed /1 failed /1 ignored**.
Buildrc0 plus exactly one successful build-finished record. Three ordinary non-harness service artifacts excluded from direct execution.
One eligible library doctest target ran separately: rc0,0 cases,one result record.
Full matrix **INCOMPLETE,aggregate1**: all-features/no-default/native-only/matched HELD,never launched.

Parent CLOSED/unmerged at2026-10-21T06:20:52Z: https://github.com/KarlLivesey/VoteBoat/pull/23#issuecomment-6309504497
Fair Mac performance INCOMPLETE,0/70 attempted: quiet load<1 unmet in bounded900s,min1.305176. No performance loss or product defect inferred.
Source approvals A5595438315/B5595449962 remain history,not full qualification. No Linux performance comparison or later configuration.
No diagnostic,repair,rebuild,isolated rerun,source/head commit or main tracked source/docs/memory write.
Parent alone owns main/closure/disposition; parent main independently advanced to 576c7cfb25820d7d3e34b218b66016bd4ac5159b.

## Actual failure

routed:109passed/1failed/0ignored,rc101,1803.61s test time/1803.840s wrapper.
Case native::creation::tcp_namespace_race_recorded_owner_loss_and_recovery_schedules.
Thread819010 panicked at tests/routed/namespace_race.rs:108:9: native routed progress timed out.
Last printed Schedule:first=Publish,committed=true,checkpoint=true. This is emitted ordering,not inferred causal phase.
Emitted nodes1Leader/2Follower term2 commit=last=applied5,pendingfalse; replica/outbound usage0,owner_drained=true,peers_drained=false.
Node1 retained1 client request/1220bytes; peer links Open/Ready,receive_frame_bytes24. No node3 local dump or backtrace emitted.
Full raw log and routed-emitted-failure.txt preserved; first-observed snapshot is separate and unchanged.
No cause,host-noise,WAL-fork,port,owner,product-defect or relationship to old PR15 failures inferred.

## Audit evidence

FINAL-AUDIT.json/default-completeness.json/binary-exits.json:129 distinct binaries,each raw rc/result/named outcome once,all executable SHA256 unchanged.
Cargo JSONL:187 compiler artifacts,129 selected harnesses,3 excluded ordinary service artifacts,1 eligible library doctest target.
Six zero-test cfg harnesses explicitly recorded:replicated_counter,quic,embedded_counter,quic_connect,wal_benchmark,durable_vote.
Required default suite nonempty. Every original current executable and docs completed despite first failure.
Exhaustive normal-debug representative_relay_model9/0/0,rc0,3241.539s wrapper,alonecore10,fully joined before ordinary runs.
First ordinary starts5.061s after recorded relay end. Ordinary concurrency normal on10-11,RUST_TEST_THREADS unset.
All63399 tracked file hashes unchanged before/after. Protected2607 present hashes/blobs match pinned source.
Full tracked-manifest SHA256:fd9aa07146a1dbf979d3896db1fe9831fdff56b80305028f6951ff9e9df2db19
Protected compressed-manifest SHA256:b710ddfab01114cbe97ee7f927fce5a744bd7ac9fd86a623fbd3b3fe324664a1
Protected2621 union=original44/mainb77/fixture66fc/desired,2607present/14absent; includingb959=2628/2607/21,additional7ST2-only absent.
Parent folded source-audit P3; no source-head change here.
Pinned exact-source strict7/actual enabled nonforce push-hook7 receipts verified/reused. Prior sourceprep failures and historical PR15 failures remain failed/unclassified.
Raw argv/env/logs/exits/preexec/resources,full build inventory and exact running controller text retained here.
Prelaunch schema/ownership/failure plan retained in PRELAUNCH-AND-PROGRESS.md.

## Resources and limitations

Native gate:absolute nice19,cores10-11,max2Cargo jobs,incremental0,dev/testopt0/assertions true/symbols0.
Pre-product Rust probe available_parallelism2 ordinary/1 relay; RUST_TEST_THREADS unset. No forced serialization/filter/skip.
Native descendant receipts:38659 observed task records,0 priority/affinity violations.
Own guarded target maximum1379401728bytes (1.285GiB),below60GiB; stock global caps unchanged.
**All-task resource compliance is NOT PASS/NOT CLAIMED.** Pre-sweep audit found3 idle automatic app tool hosts:
753109nice16,753147/753275nice-4,affinity0-15,outside native controller/product descendant receipts.
Full identities/starttimes retained in pre-sweep-owned-processes.json. Surviving helper threads restored19/cores10-11 for cleanup.
Late restoration does not erase observations or create a retroactive PASS. Native test failure remains failed independently.
Earlier read-only helpers preceded explicit resource setup. A status-reader syntax error and report-preparation expression error reached no product workload; corrected without touching/restarting suite.
Resource sampling every0.2s is not continuous observation of every short-lived descendant; explicit exec resource setup/inheritance retained.
Adapter exercised default only; later configurations unqualified. No known product process live in completed audit (4299 known PID/starttime identities checked).
Mandatory immediate own target/worktree/registration/scratch/helper sweep and independent final stock guard follow.

Task thread:thread:delegated-task:command%3Amcp%3A949e93bd-7d0b-478d-b727-5f1bd57eb166%3Adelegate-task%3Awinddown-n1c-independent-exclusive-gate-sol-20261021

## Actual final cleanup

Cleanup VERIFIED: own target/worktree/registration/seed branch/scratch removed by stock sweep;3 positively-owned idle helpers closed;0 owned product jobs/deleted-cwd/target processes; independent final stock guardrc0/PASS. Shared ports,foreign worktrees/processes and main tracked files untouched. No later configuration or Linux performance run.
See cleanup-final.json,sweep.log,diskguard-final.log. All-task helper-resource limitation remains NOT PASS; cleanup does not erase it.
