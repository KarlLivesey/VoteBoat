# Slice 106 — failed QUIC catch-up diagnosis and buffered compaction schedules

Base revision 33194d1. No production or benchmark source changes. RPL-1.5 retained.

One diagnostic repeat uses the slice105 final executable:

```sh
BINARY FRESH_ROOT quic 480 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
```

It fails the unchanged catch-up gate. The source remains leader in term 1 with
the same store binding/session. Replica 3 records one new install across its
groups, but selected group 1 retains base 0 below required boundary 11. Actual
pause/resume are retained, as are all 480 offer rows (416 Applied, 64 window
refusals). Cancellation, reclaim cleanup and close all succeed. No successful
summary, full value/read/recovery verification or throughput result is published.
Observed-outcomes.json is a classification count from raw CSV, not recovery proof.

This identifies the rejected predicate, not the exact packet history or a
production replication bug. The native experiment does not establish whether
selected group 1 recovered entirely through buffered logs. It does demonstrate
why another group's install cannot certify selected-group snapshot catch-up.
The long QUIC acceptance remains unpassed; the earlier short success is retained
in slice105 rather than repeatedly rerunning to obtain a preferred result.

No compilation/bulk build work overlaps this diagnostic run. Same single-host,
three-replica/eight-group/window8 setup, 64 warm-up operations, 8-byte counter
payload and benchmark-only 50 ms heartbeat / 10000–19999 ms elections as slice105.
Linux7.2.9-2-cachyos, Ryzen9800X3D, shared Btrfs/NVMe, no exclusive reservation;
see slice105/README.md for full recorded environment. This is failed diagnostic
evidence, not sustainable capacity or a transport comparison.

Executable SHA-256:
7b8380531225c96df37b1adaa7fea4ddd92fe2ec645023ef9b6aa3b481c3721b.
Maintenance source SHA-256:
3ace37062856d869e6257de6f413d1c89f83620f9e53c0debb233f9f2292dd3d.
Cargo.lock SHA-256:
532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8.

## Deterministic conformance schedules

New tests use the existing public SnapshotCluster fixture. After election, retain
a follower-bound Append and its same-context commit-bearing retry. Deliver other
messages to commit with the remaining quorum; compact the leader. Then compare:

1. Deliver retained messages before repair: follower commits the matching log,
   retains base 0 and needs no snapshot install.
2. Drop retained messages: repair requires a real snapshot install at base 2.

Both schedules verify exact committed values/boundaries, delayed old Append
requests after a newer committed suffix, original operation retry outcomes,
quorum reads and reconstruction of each application/core from persisted state.
Native file variants additionally close all stores, reopen WAL/snapshot files,
check recovered bases/values, elect a leader and retry the original operation.
This is an explicit finite schedule, not a reconstruction of live QUIC packets,
process power loss or a full consensus proof.

```sh
cargo +stable test --test snapshot --all-features --locked --offline
cargo +stable test --test snapshot --no-default-features --locked --offline
cargo +stable clippy --all-targets --all-features --locked --offline -- -D warnings
node validation/check-maintenance.test.mjs
```

Results: snapshot suite 21/21 all-features and 9/9 core/contracts-only; Clippy
passes. Two new Rust tests each cover both schedules; native uses actual files.
Five independent maintenance checker tests pass, including the new two-group
case where another group's installed base and positive aggregate count cannot
certify the selected group's base. Existing success gates remain unchanged.
Full P0–P7 remains active. Fixed-p99, platform and earlier phase gaps remain;
P8/Windows deferred, CI background.
