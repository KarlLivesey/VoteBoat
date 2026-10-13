# Mac425 versus426 paired diagnostic

Original remotePID62425 terminates0/DONE. All24case exits and the original checker
pass exact originals/frontiers, observer scopes, retention/queue bounds and both
before/after frozen source/compiler/artifact/driver guards. Three rotating trials
per cell use1048576 originals+1024warmups, one group/three voters/empty payload,
volatile stores/in-process RPC, client runtime1/server runtime16 and unchanged
maintenance/default envelopes. Mac is M2 Max12cores/96GiB/macOS27.0.

| Cell | Variant | Median completed PUTS/s | Median request p99 ms |
| --- | --- | ---: | ---: |
| clients4096/batch4 |425 off |2,607,177 |6.795 |
| clients4096/batch4 |426 off |3,021,055 |5.680 |
| clients4096/batch4 |426 on |2,982,296 |6.121 |
| clients4096/batch4 |OpenRaft |2,107,743 |10.006 |
| clients1/batch1 |425 off |56,415 |0.031 |
| clients1/batch1 |426 off |55,253 |0.032 |
| clients1/batch1 |426 on |51,906 |0.032 |
| clients1/batch1 |OpenRaft |10,166 |0.134 |

Bulk trial1 regresses3,442,049→3,283,107; trial2 is2,579,100→2,599,090;
trial3 is2,607,177→3,021,055. The paired median ratio is about1.008, distinct
from the median-rate ratio1.159. Every singleton pair regresses. Do not infer
uniform improvement or discard the first pair. SUMMARY.json retains all actual
rates/pairs/p50/p99; raw commands/errors/exits/timers/JSON remain in raw/.
Every run completes1048576 originals and frontiers1049601; no refusal or partial
completion is success. Bulk requests262144; singleton1048576.

426 leader exact-comparison intervals are7.905/16.424/8.014ms, within compaction
19.339/35.699/20.278ms. Admission99.510/118.404/110.305ms and results
139.347/162.203/155.929ms are larger on this host. These are inclusive elapsed
wall-time scopes, often nested/overlapping; do not sum as CPU time or infer an
allocator cause. New state_to_pin begins after exact comparison, unlike425.
Observer28phases/4scopes/14methods and exact-success/logical-entry units pass.
No compaction clone calls occur.

[Mac qualification](../../mac426/README.md) records fresh asserted root/executable/
upstream identities, actual CargoJSON/rustc and1422 candidate hashes/modes.
Immutable425 executable9656bf5f... remains unchanged. Both Mac sources compile
with identical Rust1.98.0/LLVM22.1.8/profile/locks; Linux uses1.98.1 and is separate.
Both engines retain per-original channels.4096 upstream request messages atbatch4
match16384 VoteBoat original entries; ordinary replication1024, upstream append
setting is a soft merge threshold. OpenRaft pin e88bb40707f2e511d8ecc44d31edcb1d814a11a8.
VoteBoat historical snapshot base differs from upstream current application
snapshot/retained older logs; declared cadence does not mean identical work.
Timer ends after all client joins before sorting/flattening/follower check/shutdown.

No builds/copies/checks/archive or other benchmark ran on the Mac during timing;
quiet preflight, original PID and terminal markers remain. BSD tar's first
collection fails due option position, preserved in COMPARISON_COLLECTION_ERROR;
collection alone is corrected after termination. No benchmark is restarted.
A root summary initially referenced nonexistent results/ instead of raw/ and was
corrected without changing outputs or rerunning measurements.

These short closed-loop diagnostics establish no sustained minimum, offered-load
headroom, bounded RSS/debt or real device durability. Bulk remains below8,422,500.
P9 stays active; full both-host curves, durable/fairness/placement/P8 families remain.
