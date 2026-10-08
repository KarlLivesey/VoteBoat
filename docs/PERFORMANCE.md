# Native performance baseline

Build and run the release harness through the same public Node, native WAL,
TLS/QUIC and Counter contracts as embeddings:

```sh
cargo +stable build --locked --offline --release --all-features --example native_benchmark
mkdir -p target/benchmark-runs
./target/release/examples/native_benchmark target/benchmark-runs/tcp-serial tcp 256 1
./target/release/examples/native_benchmark target/benchmark-runs/tcp-pipelined tcp 256 32
./target/release/examples/native_benchmark target/benchmark-runs/quic-serial quic 256 1
./target/release/examples/native_benchmark target/benchmark-runs/quic-pipelined quic 256 32
```

Run sequentially on otherwise idle hardware. Each root must be absent and its
parent must exist. Existing files are never reused or deleted. Roots retain all
three replicas' WALs/snapshot metadata, `samples.csv` and `summary.txt`. Embedded
public repository test keys are restricted to loopback use; this harness is not
a deployment service. TCP and UDP endpoint reservations are released immediately
before native startup, so a competing bind can invalidate startup.

The workload is one ordinary-majority three-full-voter group, with all replicas
and one polling host process on one machine. Each useful operation is an 8-byte
signed +1 counter command with a unique operation ID. Counter retry capacity is
exactly warm-up plus measured operations. The native worker/store uses real
synchronization (`FileLogIo::sync` calls `File::sync_all`); no durability bypass or
mock network is selected. Each replica has its own native store and workers.

A 64-operation warm-up and quorum read complete before measurement. The bounded
client window is 1–32; measured count is 1–50,000. Default Node queue and poll
budgets are unchanged. The harness explicitly
selects heartbeat 50 ms and election 1000–1999 ms through
`NativeStartup::open_with_protocol_and_timers`, with 32 timer expirations per
poll; the same choice is used on every replica and reopen. The service/default
startup wrappers retain their existing 50 ms heartbeat and 150–299 ms elections.
One host drives all three nodes, parking for up to
100 microseconds between incomplete polling rounds. The OS and worker wakeups
may change that actual delay. One phase has a 120-second progress deadline;
choose a workload that fits it.

Throughput counts only successful committed/applied leader receipts, from start
of the measured workload until the last such completion. It excludes startup,
warm-up, subsequent follower drain, verification, shutdown and reopen. Latency
starts immediately before dispatch and ends when the matching applied receipt is
observed; it includes bounded queueing and host polling. CSV times are nanoseconds
relative to the measured interval, with operation ID, applied log index and
historical counter result. Summary percentiles use nearest rank and microseconds.
All measured operations must succeed exactly once, with no admission refusal,
unknown outcome, timeout or hidden retry. An invalid run has no successful summary.

After measurement, every replica must apply through the last receipt index and
hold the expected value; a quorum-backed read must agree. All workers drain/join,
then all replicas reopen from their actual retained storage. First and last
operation retries must return their original historical outcomes and leave the
value unchanged after every replica applies. Recovery-only leadership uncertainty
can trigger up to four identical retry attempts per operation; extra attempts
are explicitly reported as `recovery_retries` and logged outside the measured
interval. Other verification failures invalidate the run. A final join completes
before raw samples and the successful summary are published.

This is a closed-loop local baseline. It does not estimate open-loop offered-load
latency, client-network latency, separate-host durability/fault isolation or
sustainable throughput. Small finite runs are sensitive to cache state, fsync
latency, scheduler load and timer changes. They perform no checkpoint/compaction
traffic during measurement and do not validate performance under maintenance.
The Counter retains lifetime deduplication state, so operation count changes both
history size and workload cost. Compare equal counts/configuration with repeated
runs, fixed p99 budgets and full failure/error reporting before accepting tuning.

Keep raw samples, git revision plus harness hash, compiler/build flags and lockfile,
CPU/core allocation, memory, kernel, filesystem/mount options, device/firmware and
network topology with results. Linux results are in
[the slice-99 evidence](../validation/performance/slice99/README.md). macOS execution,
open-loop load, shared multi-group scaling, maintenance/recovery load and attributable
batching/lane improvements remain required P7 work. No consensus protocol, default timer value or release performance threshold
changed. The native startup API now exposes the existing TimerConfig capability
so embeddings can declare timing appropriate to their deployment.

## WAL barrier and host progress attribution

The local WAL harness times the unchanged FileLogIo through the public JournalIo
interface, without sockets, Raft workers or a consensus quorum:

```sh
cargo +stable build --locked --offline --release --example wal_benchmark
./target/release/examples/wal_benchmark target/benchmark-runs/wal-single 64 1
./target/release/examples/wal_benchmark target/benchmark-runs/wal-batched 64 32
```

Arguments are a fresh root, 1–512 measured batches and 1–32 entries per batch;
warm-up adds eight batches and total retained entries must fit the native limit.
A record is an 8-byte +1 command. Each batch appends one complete group transition
and completes its actual native durability barrier. No commit index from this
storage workload is evidence of a distributed Raft decision. After timing, the
entire acknowledged GroupLog must reopen identically; a fresh Counter replays all
records and first/last operation retries must preserve historical results/value.

`samples.csv` separates complete append/barrier time from primitive file append,
WAL sync and manifest publication time, plus encoded WAL bytes. The observer
forwards each actual operation/result; publication includes the manifest file
sync, rename and directory sync required by the existing format. `summary.txt`
reports only local durable-record rate and stage totals. Batch size changes both
records per barrier and retained history, so this is attribution, not a controlled
replicated optimization result. See [slice 100](../validation/performance/slice100/README.md).

The replicated harness additionally reports measured-phase totals from existing
NodeProgress: persistence batches, worker events and application deliveries,
alongside host poll rounds, total poll wall time and its maximum. Deliveries are
batches, not useful operations; counts span three replicas and may include
background work crossing interval boundaries. Host poll time excludes worker
execution and parked/waiting time; it is not CPU time. These observations cannot
be summed with parallel replica storage timings to reconstruct a critical path.
They are diagnostics, never durable or committed-prefix watermarks.
