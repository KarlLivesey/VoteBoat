# Slice158 — QUIC pause diagnostics and forced native recovery

Base revision a803a8b with this slice's diagnostic/test changes. RPL-1.5 retained.
No production library, wire, persistence or benchmark scheduling change.

## One original long workload

```sh
target/release/examples/native_benchmark target/slice158-quic-pause quic 480 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
node validation/check-native-benchmark.mjs --storage validation/performance/slice158/quic-pause
```

This single run passes the unchanged selected-group catch-up gate, all-replica
values/quorum reads, recovery retries, recovered bases and worker joins. Group1's
forced boundary is11, beyond its pre-pause accepted prefix10; its source remains
leader1 in term1 with the original store binding. The follower records two new
snapshot installs after resume and its group1 base is11 before close and after
reopen. A different group's install alone still cannot satisfy this gate.

480 offers over60 seconds yield416 admissions/applied results and64 window
refusals. 409 apply during the horizon and7 during drain; there are no unknown,
pending or not-proposed outcomes. The recovered total is480 including64 warm-up
operations. 26 checkpoint waves and78 exact reclamations complete, with3 explicit
skips. Applied p99 is2918.173043ms, not a fixed-p99 capacity success. The independent
raw-result checker passes; its output and all successful raw files are retained.

This does not erase the failed slice105/106 runs or reconstruct their packets.
A reactor pause retains transport data and does not guarantee a snapshot is
necessary. Added diagnostics record selected-group base/accepted/committed/
applied/value before pause, before resume and at failed validation; success
continues through the original checks. No rerun was used to select a preferred
outcome within this slice.

Environment: Linux7.2.9-2-cachyos, x86_64 Ryzen9800X3D, same shared Btrfs/NVMe
workspace as slice105. Actual run root is on Btrfs, not tmpfs. Three local replicas,
eight groups, window8, one WAL worker/snapshot worker/peer endpoint per replica,
8-byte counter payload, benchmark heartbeat50ms and elections10000–19999ms.
No compiling or bulk verification overlapped measurement; this is not an
exclusive hardware reservation, separate-host or macOS result. Rust/cargo1.98.1.

SHA-256:

| Input | Digest |
| --- | --- |
| release native_benchmark | 945b3e58ac46ea65f349208adc10b35b561b1f52ee1c68a7e9eedf8beeeb4725 |
| examples/benchmark/maintenance.rs | 16b76428132c84001c208a1035c55ab40354eff69790519f0903f31d6ff4a4b6 |
| examples/benchmark/shared_recovery.rs | c2955ab3b530f81942a4a7a79eafdb7d287174a03d38b505bdc19f644705037a |
| Cargo.lock | 532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8 |

## Controlled recovery acceptance

Two native TCP/TLS and QUIC tests use the same shared assembly with eight groups.
After baseline replication, abort/join follower3, commit on the remaining quorum
and checkpoint both survivors beyond every old follower prefix. Close every
transport before reopening all original files. Every stale group must install
its own snapshot: old last2, required base3, recovered base3. Check restored
application indices/values, all replicas' values/quorum reads, original receipts,
another complete file restart and nonregressing bases. This is an explicit
transport-restart schedule, not the same-leader polling-pause experiment.

```sh
cargo +stable test --locked --offline --all-features --example native_benchmark -- --nocapture
node validation/check-maintenance.test.mjs
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
```

The first two focused cases failed because the fixture checked loaded-event
counts as soon as WAL bases advanced, before application restore observations.
The retained initial log records that failure. Waiting for each restored
application index/value fixes the fixture without weakening the base or count
assertion. Final full example suite18/18 passes in6.20s, independent maintenance
checker5/5 passes, and both complete strict Clippy profiles pass with zero
diagnostics. These selected Linux histories do not prove arbitrary-fault safety,
sustainable capacity or completion of P0–P7.
