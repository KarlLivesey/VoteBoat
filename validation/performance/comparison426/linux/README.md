# Linux425 versus426 paired diagnostic

Original campaign session87563 terminated0. All24 cases pass every-original,
three-frontier, runtime/payload/timer/capacity-unit, bounded queue/retention,
observer-scope and before/after artifact/source/compiler guards. Three rotating
trials per cell use1048576 measured originals +1024 singleton warmups, one group,
three voters, empty payload, volatile stores, in-process RPC, client runtime1/
server runtime16, default maintenance and upstream-default envelopes.

| Cell | Variant | Median completed PUTS/s | Median request p99 ms |
| --- | --- | ---: | ---: |
| clients4096/batch4 |425 off |1,241,106 |15.708 |
| clients4096/batch4 |426 off |1,282,790 |15.191 |
| clients4096/batch4 |426 on |1,281,352 |14.873 |
| clients4096/batch4 |OpenRaft |3,501,421 |5.904 |
| clients1/batch1 |425 off |110,076 |0.018 |
| clients1/batch1 |426 off |94,903 |0.022 |
| clients1/batch1 |426 on |82,206 |0.021 |
| clients1/batch1 |OpenRaft |19,955 |0.126 |

All three bulk pairs improve; all three singleton pairs regress. `SUMMARY.json`
retains every rate and paired ratio. All raw commands, stderr, errors, process
exits, wall/user/system timers and JSON remain; no case is replaced. Throughput
ends after all client joins before latency aggregation/sorting, follower checks
and shutdown. Every case completes1048576 originals and frontiers1049601; bulk
has262144 requests and singleton1048576. No refusal/partial result is success.
These short closed-loop trials establish no offered-load headroom or sustained
healthy-margin floor.426 bulk is below the8,422,500 minimum.

Fresh426 root/executable/upstream artifact proof is linked in
[Linux build](../../matched426-linux/README.md); immutable425 executable SHA84ed0561...
remains unchanged. Both use Rust1.98.1/LLVM22.1.8 and identical locks/profile.
Guarded source inventories are1409/1418 selected inputs with scopes explicit.
Pinned OpenRaft is e88bb40707f2e511d8ecc44d31edcb1d814a11a8. Its4096-request
batch4 API envelope and VoteBoat16384-original envelope are equal original bounds;
ordinary replication is1024. Upstream append4096 is a soft merge threshold.
Both engines retain every per-original completion channel.

426 exact-comparison leader elapsed totals are377.022/371.845/379.593ms, about97%
of compaction389.192/383.616/391.908ms. This is inclusive elapsed wall time, not
CPU or allocator attribution. Materialization removal gave only a modest bulk
improvement; full comparison remains the measured dominant interval. The first
run counts4341777 logical retained entries over204 successful comparisons; those
are not physical traversal reads. New state_to_pin totals0.030/0.032/0.028ms start
AFTER equality;425 state_to_pin included equality/temporary destruction and is a
different scope. The old clone phase stays separately named. Observer28 phases,
four scopes and14 methods each pass, with no compaction state clone calls.

VoteBoat snapshots define their historical compacted base; OpenRaft snapshots
current application state while retaining older logs. Declared cadence is matched,
actual snapshot work differs. Every queue/retained-entry bound passes, but these
short logical controls are not a sustained RSS/debt or real disk durability proof.
Mac pairing is separate and pending. P9 remains active and all macro acceptance
families remain open.
