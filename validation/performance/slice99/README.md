# Slice 99 measurement environment

Runs start from revision `f514eb8`. The declared-timer matrix additionally uses
the startup/timing changes and benchmark committed alongside this evidence.
SHA-256 identities of the measured sources:

- examples/native_benchmark.rs: `8b952d3f3eabcaa2848ab424f061d462d60ae3e1e959bad521f5ff4a6e7c7078`
- src/native/startup.rs: `93e6784d2b3e928fe50e2532de4c5cf368fad39e29c20dfaab61f59071879eed`
- src/runtime/timed.rs: `8613a1e7d77e3454e256566b0de323e57a1387a25326cf8e43eb363edb309bac`

Final matrix timing: heartbeat 50 ms, election 1000–1999 ms, 32 expirations/poll.
Cargo.lock SHA-256:
`532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8`.
Build: `cargo +stable build --locked --offline --release --all-features --example native_benchmark`;
Cargo default release settings, no target CPU/affinity override. Compiler Rust
1.98.1 (`48a229cea`, 2026-09-01), LLVM 22.1.8, x86_64-unknown-linux-gnu.
Enabled features: native, tls, quic; exact dependencies retained in Cargo.lock.

Machine: AMD Ryzen 7 9800X3D, 8 cores/16 logical CPUs (allowed CPUs 0–15),
60 GiB RAM, Linux 7.2.9-1-cachyos x86_64. No CPU pinning, frequency lock or
exclusive machine reservation. Other desktop workloads remain possible.
All three replicas and polling host share this machine and loopback interfaces.
Storage is the repository's target/benchmark-runs on Btrfs, Sabrent
SB-ROCKET-NVMe4-2TB `/dev/nvme1n1p2`, firmware RKT4B5.1. Mount options:
rw,nosuid,nodev,noatime,compress=zstd:1,ssd,discard=async,space_cache=v2,
subvol=/@home. All replicas share that device and filesystem; three replica
WALs on one host do not establish independent machine/power-loss resilience.

See docs/PERFORMANCE.md for workload, durability, polling and acceptance semantics.
Raw CSV and successful summaries accompany each retained result below. Values are
observations from finite runs, not sustainable throughput or release thresholds.

Failed attempts are part of the record: the first QUIC/window-32 run returned
an unspecified non-applied client outcome (the initial harness diagnostic was
insufficient). The second diagnostic run completed its measured writes but failed
post-reopen retry verification with `Unknown(LeadershipChanged)`. Neither produced
a valid summary or contributes performance results. Recovery-only identical retry
handling was then made explicit and counted; measured uncertainty remains fatal.

Final release executable SHA-256:
`00f5adb9cb6faf5da115b215129a3dbdeb4cff7893dce3b6be54d0196d26b96e`.

The longer default-timer TCP serial attempt failed at measured operation 123
with `Unknown(LeadershipChanged)` and contributes no performance result. The
retained `default-*` CSV/TXT files are successful preliminary window-32 runs with
50 ms heartbeat and 150–299 ms elections, before the startup API change. TCP used
harness hash `094bf13fa900e4ad56b7c42a404f0bf8cd989b45d9fbe20ae001379df6efd83d`;
QUIC used `01b73e0e0cfd0aa68e6a11cacd869e94fd8e9ce5a41c27d8a523565e41716cb0`.
Their diagnostics/recovery handling differ, so they are recorded for context,
not a controlled before/after improvement claim.

## Equal-count declared-timer observations

Each run has 64 warm-up and 256 measured +1 operations. All four recovered value
320 on every replica, verified original first/last retry outcomes, joined all
workers and needed zero additional recovery retry attempts.

| Transport | Window | Applied ops/s | p50 ms | p99 ms | Measured seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP/TLS | 1 | 8.902 | 103.470 | 227.608 | 28.758217 |
| TCP/TLS | 32 | 36.242 | 824.037 | 1558.050 | 7.063670 |
| QUIC | 1 | 7.156 | 125.318 | 546.170 | 35.773888 |
| QUIC | 32 | 25.136 | 1304.719 | 1942.021 | 10.184409 |

The larger window increases observed throughput while worsening latency. These
single finite samples do not demonstrate improvement at a fixed p99 budget, or
prove that longer elections resolved the default-timer instability. No errors
occurred in the four measured intervals; the failed preceding attempts above
remain part of the evidence. Attribution of barrier, scheduling and network
costs, repeated steady-state runs, maintenance, offered load, independent hosts
and multiple shared groups remain outstanding.

An independent CSV check verified 256 unique operation IDs (65–320), unique
applied indices, corresponding counter results (65–320), nonnegative matching
dispatch/completion latency, and agreement with nearest-rank percentile and rate
summaries for all six retained matrices/preliminary runs.
