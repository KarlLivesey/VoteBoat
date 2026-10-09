# Slice 105 — offered load during maintenance and follower catch-up

Base revision b2a07ca plus benchmark-only maintenance and validation changes.
Production consensus, providers, formats, resources and timer defaults are unchanged.
RPL-1.5 remains the public license.

## Reproduction and acceptance

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
BINARY FRESH_ROOT tcp 480 8 8 --offered 8
BINARY FRESH_ROOT tcp 480 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
BINARY FRESH_ROOT quic 480 8 8 --offered 8
BINARY FRESH_ROOT quic 480 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
BINARY FRESH_ROOT quic 480 8 8 --offered 8 --maintenance 2
node validation/check-native-benchmark.mjs --storage validation/performance/slice105/{tcp,quic}-{control,maintenance}
node validation/check-maintenance.test.mjs
node validation/check-offered.test.mjs
```

The four matched cases use one copied release executable and fresh roots, run
sequentially without concurrent compilation or bulk build/file work. Each uses
three replicas sharing one host/device, eight groups, global window 8 and per-group
ceiling 1. All groups initially have replica 1 as leader. Payloads are 8-byte
counter increments, after 64 closed-loop warm-up commands and verification.
One native WAL worker, snapshot worker and peer endpoint serve each replica;
TCP also uses one dial worker. Benchmark heartbeats are 50 ms and elections
10000–19999 ms, unchanged from earlier shared runs, not production defaults.

Each case schedules 480 offers over 60 seconds at 8/s. Every offer retains its
original intended start and known outcome, including dropped window refusals.
Maintenance opportunities occur every 2 seconds, with at most one active wave:
checkpoint one leader/group, verify a strictly advanced durable snapshot boundary
on the same leader/term/store with drained snapshot work, then reclaim each
replica's WAL using exact original full tickets. Busy or missed opportunities are
explicit bounded skips, not a deferred work queue.

The maintenance cases pause replica 3's host reactor starting at intended second 1
for a full five seconds from the observed start. Workers/sockets/files remain
owned. This is a host-poll stall, not a process crash or separate-host outage.
A forced checkpoint during the pause must exceed that group's pre-pause highest
accepted leader index; after resume the follower must install a new snapshot and
reach that durable boundary while the original source leader/term/store remains
unchanged. Thus resumed polling alone cannot pass the catch-up gate.

Successful publication additionally requires all-replica expected values, quorum
reads, complete worker joins, full reopen, nonregressing recovered bases and
original historical retries with unchanged values. Recovery is not useful
measured work. `maintenance-config.txt` is initial configuration, not final counts.
Actual outcomes are in the summary and raw CSVs. Independent arithmetic checks
are retained in analysis.txt; they confer no protocol authority.

## Limits and preliminary evidence

These finite one-sample cases do not establish sustainable capacity, a transport
speed advantage, arbitrary-fault correctness, separate-host behavior or macOS
execution. Desktop/device load is uncontrolled. The original 250 ms TCP serial
p99 target remains unmet; these workloads do not replace that gate. Full P0–P7
remains active, including earlier membership/lifecycle/platform/fault gaps.
P8/Windows remain deferred and CI remains background feedback.

Maintenance latencies include queue and completion-observation delay and sum
concurrent work; they are not pure I/O costs or an additive critical path.
The storage observer excludes internal snapshot/replacement I/O. Reclamation
rows report actual before/after bytes independently.

`preliminary-smoke-tcp` is a correctness smoke run with concurrent compilation;
its timing is not performance evidence. `preliminary-tcp-control` is the first
control under the original executable. Review then corrected pause timing so a
late observed start cannot shorten the requested duration. Both preliminary runs
are retained and independently checked, but excluded from the matched set.
No runtime failure is omitted.

The matched QUIC pause run failed the unchanged final snapshot catch-up gate.
Its `failed-quic-pause.*` artifacts retain 411 Applied and 69 window-refused
offers, maintenance rows and successful cleanup, but no successful summary or
recovery claim. The original combined predicate does not identify whether the
follower install/base or source identity caused refusal. Buffered delivery during
a host-poll stall is a possible explanation, not a verified diagnosis. A separate
no-pause QUIC case checks maintenance alone; it does not satisfy the failed pause
case. A subsequent diagnostic-only source change retains each predicate's values.

## Environment and provenance

Linux 7.2.9-2-cachyos, Ryzen 7 9800X3D (8 cores/16 logical CPUs), MemTotal
63434604 kB; no exclusive CPU/device reservation or affinity. Btrfs on
/dev/nvme1n1p2, Sabrent SB-ROCKET-NVMe4-2TB firmware RKT4B5.1; mount options
rw,nosuid,nodev,noatime,compress=zstd:1,ssd,discard=async,space_cache=v2,
subvolid=257,subvol=/@home. Rust 1.98.1 (48a229cea 2026-09-01), LLVM22.1.8,
x86_64-unknown-linux-gnu. Loopback TCP/TLS or QUIC. Kernel differs from slice103's
7.2.9-1; no cross-kernel before/after comparison is made.

SHA-256:

- Matched release executable: 9f89a26e2ff45bf38c6771d66bc1f66084a515f5b83ac000c8ba670d89d6d772
- Preliminary executable: 10005891535535d38ef0af8e68fd7f037f61c31b1629e9ca056fa8e81bc1c3f4
- examples/native_benchmark.rs: e924f8b48a13bb95c344473ecfd4a4372dc308bfce6b876b63076756a3ce2a5b
- examples/benchmark/offered.rs: 7c62fd5e8b3777d792b0cf767a0431c91bee77579e0a18c39ba64669f6e027e9
- examples/benchmark/maintenance.rs (matched): 112c5bc3e869bc2d52941d33a283cf25617321c44591df0978121125d48aad32
- examples/benchmark/maintenance.rs (preliminary): f23fec96bdc4eb79b33b96120aa0be3df9e682f527443f562d5bcece38a46f06
- examples/benchmark/shared.rs: 398d6fc99bb979fa6e9e7ce1c27c41c6a92c62ff8e59c879686313b9c4123d81
- examples/benchmark/observe.rs: 8e94ef2798febe4a70d2f632b975c50d6964bd1e44d64b0f188f5a9af3e59c36
- Cargo.lock: 532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8

## Successful 60-second cases

| Case | Applied / 480 | Window refused | During / total applied ops/s | Drain applied | Intended-start p99 ms | Checkpoints / reclaims / skips | Reclaimed bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP control | 462 | 18 | 7.700 / 7.700 | 0 | 1171.573 | — | — |
| TCP maintenance + pause | 443 | 37 | 7.383 / 7.383 | 0 | 2051.818 | 27 / 81 / 2 | 352388 |
| QUIC control | 456 | 24 | 7.483 / 7.317 | 7 | 1495.611 | — | — |
| QUIC maintenance, no pause | 424 | 56 | 7.050 / 7.058 | 1 | 1956.002 | 26 / 78 / 3 | 344004 |

All four successful cases have zero unknown, NotProposed, no-leader or provider
admission-refused outcomes, zero extra recovery retries, and passing value/read/
recovery/historical retry/join gates. TCP's pause lasts 5.000158025 seconds;
the forced group 1 snapshot boundary 11 exceeds pre-pause leader last index 10.
Replica 3 records two new installs and recovers at/after the forced boundary.
QUIC maintenance without pause makes no forced catch-up claim. These differing
experiments and single samples do not support a transport speed comparison.

Six maintenance Rust tests cover durable checkpoint observation, leader/term/store
scope, exact reclaim tickets, bounded missed opportunities, invalid/missed pause
and full actual duration after a late start. Together with the seven existing
ledger/observer tests, all 13 pass with all features and TLS-only. All-target/
all-feature Clippy with warnings denied passes after the diagnostic correction.
Four independent maintenance checker tests and five offered checker tests pass;
negative cases reject false durability, missing reclamation, pending work,
regressed recovery and fabricated/shortened catch-up. The updated checker also
passes all four slice104 offered histories and 18 slice103 closed-loop histories.
The unchanged 71-contract inventory checks shape and paths, not protocol proof.

Diagnostic-only final executable SHA-256:
7b8380531225c96df37b1adaa7fea4ddd92fe2ec645023ef9b6aa3b481c3721b.
Final maintenance source SHA-256:
3ace37062856d869e6257de6f413d1c89f83620f9e53c0debb233f9f2292dd3d.
The matched long runs above precede this failure-message-only change; they retain
their original executable/source hashes rather than being relabeled as reruns.

The final diagnostic executable's short QUIC pause case uses 120 offers over
15 seconds with all other settings unchanged. It passes the original gate:
96 Applied, 24 window refusals, 5 checkpoints, 15 reclaims, 2 skips and 108348
reclaimed bytes. Actual pause duration is 5.000185054 seconds; forced boundary 11
exceeds prior last index 10 and replica 3 records one new snapshot install.
Full values/reads/reopen/base/retry/join checks and independent arithmetic pass,
with zero extra retries. The failed long experiment remains unexplained and
retained; this passing short case does not diagnose it or establish repeatable
60-second QUIC catch-up. No further timing or acceptance change was made.

Reproduce the short diagnostic case with:

```sh
BINARY FRESH_ROOT quic 120 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
node validation/check-native-benchmark.mjs --storage validation/performance/slice105/diagnostic-quic-pause
```
