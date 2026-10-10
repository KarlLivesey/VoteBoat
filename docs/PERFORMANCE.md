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
interval. Other verification failures invalidate the run. The measured sample CSV is retained before subsequent verification/recovery;
a final join completes before the successful summary is published. Failed
warmup/measurement runs retain partial diagnostic histories and attempt explicit
worker shutdown without becoming successful measurements.

This is a closed-loop local baseline. It does not estimate open-loop offered-load
latency, client-network latency, separate-host durability/fault isolation or
sustainable throughput. Small finite runs are sensitive to cache state, fsync
latency, scheduler load and timer changes. They perform no checkpoint/compaction
traffic during measurement and do not validate performance under maintenance.
The Counter retains lifetime deduplication state, so operation count changes both
history size and workload cost. Compare equal counts/configuration with repeated
runs, fixed p99 budgets and full failure/error reporting before accepting tuning.

## Shared Multi-Raft comparison

An optional fifth argument selects a benchmark assembly using the public
`NativeNode::from_parts` contracts with 1–32 groups. For example:

```sh
./target/release/examples/native_benchmark target/benchmark-runs/tcp-shared-one tcp 256 8 1
./target/release/examples/native_benchmark target/benchmark-runs/tcp-shared-eight tcp 256 8 8
```

Use the optional argument for both sides of a comparison: omitting it selects
the existing one-group NativeStartup convenience API. Each shared replica has
one authoritative WAL/worker, one snapshot worker and one peer endpoint, plus one
TCP dial worker when using TCP. Snapshot handles and application/core state are
per group. Group count does not multiply endpoints or workers. Default resource
limits stay unchanged. The shared comparison explicitly selects 50 ms heartbeats
and 10000–19999 ms elections on both sides: initial trials at 1000–1999 ms lost
leadership under the measured storage latency. The four-argument startup mode
retains 1000–1999 ms elections. These settings trade failure detection speed for
headroom in this throughput experiment; they do not change service defaults.

Global operation IDs route round-robin across groups. The global window remains
bounded by WINDOW; each group is additionally bounded by ceil(WINDOW/GROUPS).
The dispatcher waits when the next round-robin group has reached that bound;
it does not skip slow groups. Thus WINDOW=8 gives one group eight outstanding
requests, or eight groups at most one each. Each group uses the same lifetime
Counter capacity (total measured count plus 64 warm-up operations). Workloads
whose per-group history plus a 64-entry control reserve exceed the default log
capacity are rejected before creating the run directory.

Requests route to the observed leader of their concrete group; the core still
checks proposal/read authority. Summary `leader_placement` records group:node
pairs at the start of measurement, not a guarantee against later elections.
CSV adds a `group` column. Applied positions and recovery boundaries are checked
separately per group. `recovered_value` is the sum across groups; every replica
and each group's quorum read must match its round-robin partition of that sum.
Original first/last retries return their own group's historical outcomes.
All successful runs retain the full shutdown/reopen/retry/final-join gates.

This measures partitioned useful operations sharing local resources. It does not
measure a single group's acceleration, cross-group transactions or independent
machine failure domains. Record actual leader placement and compare equal total
operation counts, windows and resources when interpreting results.
The [slice-101 baseline](../validation/performance/slice101/README.md) records the
controlled TCP/QUIC comparison and its limitations: eight groups are slower in
this workload, so it supplies no scaling-improvement claim.

Keep raw samples, git revision plus harness hash, compiler/build flags and lockfile,
CPU/core allocation, memory, kernel, filesystem/mount options, device/firmware and
network topology with results. Linux results are in
[the slice-99 evidence](../validation/performance/slice99/README.md). macOS execution,
open-loop load, broader shared multi-group scaling, maintenance/recovery load and attributable
batching/lane improvements remain required P7 work. No consensus protocol, default timer value or release performance threshold
changed. The native startup API now exposes the existing TimerConfig capability
so embeddings can declare timing appropriate to their deployment.

## Static local lanes

The same public Node/native assembly can partition 1–32 groups across 1–4
independent local lanes. Use a fresh run directory:

```sh
./target/release/examples/native_benchmark --lanes target/benchmark-runs/lanes-one tcp 64 8 4 1
./target/release/examples/native_benchmark --lanes target/benchmark-runs/lanes-two tcp 64 8 4 2
node validation/check-native-benchmark.mjs --storage target/benchmark-runs/lanes-two
```

Arguments after the directory/protocol are total operations, total window, total
groups and lanes. Operations, groups and window must each be at least the lane
count. The planner partitions all three totals and the total64-operation warm-up;
it does not multiply offered work or concurrency. Each lane owns contiguous,
disjoint concrete group IDs and three distinct store identities/directories.
Operation IDs are local to their concrete group. Changing the lane assignment
is not a supported file migration.

One host thread drives each lane's three loopback replica owners. Each replica
lane has one shared native WAL/worker, snapshot worker and peer endpoint, plus
one dial worker for TCP. Groups within a lane share those resources. Thus two
lanes mean two owner threads, six WAL workers, six snapshot workers and six peer
endpoints; TCP adds six dial workers. This is a single-machine experiment with
three logical replica hosts, not independent machines or failure domains.

Every lane completes warm-up and quorum-read verification before the shared
measurement start. Dispatch/completion timestamps use that common origin;
latency begins immediately before each proposal, while elapsed throughput also
includes any delay in waking a lane after the start. No lane starts post-run
verification, output-file writes or recovery until all measured workloads finish.
A lane awaiting a phase boundary continues polling its owners. The reported sum
of lane maximum concurrency is an upper bound, not an observed simultaneous peak.

Each lane verifies all replica values and quorum reads, drains and joins workers,
reopens its actual files, checks historical retry receipts and verifies unchanged
values before final joins. The coordinator joins every host thread. A failed
lane invalidates the whole run, with retained files and a failure record instead
of a success summary. Selected pre-start and partial-replica failures are tested;
this is not exhaustive OS worker-creation failure coverage.

Raw output includes the checked static plan, per-lane samples/storage traces and
aggregate samples/summary. The independent checker validates assignment, total
counts, per-group history, per-lane/global windows, timing arithmetic and native
storage counters. A successful finite run proves those checks, not sustainable
capacity. Shared physical storage can limit every lane; controlled comparisons,
maintenance/load tests and the original fixed-p99 gate remain required.

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

## Shared WAL attribution

The shared assembly wraps its selected native store and journal through the same
public `LogStore`/`JournalIo` contracts. Production providers and defaults remain
unchanged. Its helper functions are generic over the selected store, so the
original four-argument NativeStartup mode still runs without an observer.

Successful shared runs additionally publish `storage.csv`, with per-replica
cumulative snapshots before/after measurement, after create-session joins and
after recovery-session joins. Recovery counters start anew. Fields report logical
append calls, group transition units, physically appended command entries, batch
size histogram and per-group units; append/barrier wall durations and maxima;
encoded bytes; primitive append, synchronization and manifest-publication calls
and durations. Physically appended commands can repeat during replication/retry
and are not useful-operation counts. `errors` counts append/barrier failures.

Observers forward exact original tickets/results and all range/reclamation
capabilities. Each WAL has one bounded aggregate; no aggregate lock spans I/O.
Counts are diagnostics, not commit/durable watermarks. Snapshot I/O is separate.
Native create/recover each performs one initialization sync/publication outside
a logical barrier, so a joined snapshot has one more primitive sync/publication
than logical barriers. Initialization is included in cumulative totals.

A mid-run snapshot can include a completed primitive inside an unfinished logical
call. Whole calls crossing a measurement boundary contribute their full duration
when they complete; maximum fields are cumulative maxima, not interval maxima.
Sums across three concurrent workers are aggregate work, not elapsed critical-path
time. Joined snapshots remove in-flight ambiguity and reconcile exact logical
append/barrier/ticket/unit totals, histograms and physical call counts.

Validate both workload and storage arithmetic, requiring the extra file:

```sh
node validation/check-native-benchmark.mjs --storage RUN_DIRECTORY...
```

The validator also accepts an archived prefix with `.txt`, `.csv` and
`.storage.csv` files. Instrumentation adds measurement overhead; observed timings
alone are not evidence of a tuning improvement over earlier unobserved runs.

## Ready queued requests share a native barrier

NativeLogWorker now gathers immediately available FIFO requests within its existing
request/unit/retained-byte limits and the store's pending-ticket limit. It does
not wait for more work. A queued reclamation or an oversized next request closes
the window; the deferred item executes next. Each original append still validates
independently and retains its own on-disk record and Written/Failed result.
Successful appends share one physical barrier over their exact ticket union.
Only a fully validated union can be projected back to per-request DurableLog
subsets. Failed or malformed durability evidence grants no subset success.
Original request credits and control reserves release only on original terminals.
No API, protocol, persistent format or service timer/resource default changes.

Joined storage metrics can therefore have fewer barrier calls than append calls,
while total barrier tickets must equal successfully appended transition units.
The checker validates this relationship and reports mean appends/units per barrier.
Batch histograms describe original append calls; they do not alone describe the
number of requests sharing a barrier. Compare physical barriers as well as useful
applied receipts and latency at the same workload/resources/timers.

The scheduling change does not create single-group parallel ordering or a
cross-group transaction. With one group and one outstanding persistence transition,
there is no second independent request to combine; its durability requirements
remain unchanged. Single-group latency and the predeclared TCP serial p99 target
must still be checked separately from multi-group aggregate throughput.

Repeated slice-103 comparisons and raw evidence are recorded in
[validation/performance/slice103](../validation/performance/slice103/README.md).
Eight-group throughput improves in both transports with fewer barriers; single-
group controls vary without a reliable gain. The original startup TCP serial
candidate p99 is 311.801 ms versus baseline 478.658 ms (one sample each), still
above the predeclared 250 ms target. These finite closed-loop results do not prove
sustainable offered-load capacity or a general latency improvement.

## Scheduled offered load

The shared native benchmark also accepts an optional offered rate:

```sh
cargo +stable build --release --example native_benchmark --all-features --locked --offline
./target/release/examples/native_benchmark FRESH_ROOT tcp 120 8 8 --offered 4
./target/release/examples/native_benchmark OTHER_FRESH_ROOT quic 240 8 8 --offered 48
```

These commands schedule 120 offers over 30 seconds and 240 over 5 seconds.
RATE is 1–100000 offers/second, count/window/groups retain existing bounds, and
the intended offering horizon is capped at 300 seconds. Warm-up is separate.
A reactor turn handles at most 64 due offers. Lateness never resets the schedule:
`offers.csv` records every ID's intended start, actual dispatch/decision and
terminal time. Its fixed count bounds retained history. A full global client
window or per-group ceiling ceil(window/groups) produces `window_refused`, with
no deferred client retry queue. Other rows distinguish no ready leader, admission
refusal, NotProposed, Applied and Unknown. Reasons are diagnostics with comma and
newline characters normalized to `|`.

The summary counts offered, admitted and useful Applied outcomes separately.
`applied_during_ops_s` uses completions strictly before the intended offering
horizon; `applied_total_ops_s` includes completed drain work over the full measured
elapsed time. `admitted_ops_s` also uses that full elapsed denominator. Separate
admitted-during, applied-drain, late-decision and backlog-at-horizon counts expose
catch-up/drain rather than hiding it in one rate. Backlog at the horizon counts
admitted tickets dispatched before the boundary but completed at or after it.
Dispatch p99 includes every offer; useful-operation p99 is from intended time,
while service p99 starts at actual dispatch. No successful Applied outcomes means
those two latency percentiles are `NA`, never a zero-latency result.

Drain has a separate 120-second limit. Unknown/pending/invalid outcomes invalidate
successful summary publication; raw rows and `failure.txt` retain diagnostic state
and cleanup results. Cancellation only stops observation and does not roll back a
possibly committed operation. `run.txt` retains configuration even for failed runs.
Successful summaries still require every group's exact known values, all-replica
and quorum reads, explicit joins/reopen and original first/last successful
historical retry results. Refused offer IDs leave holes; expected values come from
actual Applied receipts, not from the last offered ID. Recovery/retry operations
are excluded from useful measurement counts.

The same independent checker accepts offered runs and archived prefixes ending
in `.txt`, `.offers.csv` and `.storage.csv`:

```sh
node validation/check-native-benchmark.mjs --storage RUN_DIRECTORY...
node validation/check-offered.test.mjs
```

It reconciles every offer's classification and intended time, retained-window
history (including actual load behind window refusals), per-group applied values,
horizon/drain counts, rate/latency arithmetic and the existing storage counters.
The original four/five-argument closed-loop modes remain available. Offered mode
adds no production API/provider/format/timer change. Finite low/high offered-rate
illustrations do not establish sustainable capacity, maintenance performance or a
passed p99 budget; those require longer, representative, repeatable measurements.

[Slice-104 raw evidence](../validation/performance/slice104/README.md) records
four complete TCP/QUIC low/high offered-rate runs plus the original CLI correctness
checks. Independent analysis prints admitted-during and transient applied-drain
rates separately. Both 30-second low-rate samples admit every offer; higher-rate
samples expose actual window refusals. They establish the mode and its accounting,
not sustainable maintenance capacity or a passed p99 budget.

## Checkpoint, reclamation and follower catch-up load

Scheduled load can enable one bounded maintenance wave at a time:

```sh
BINARY FRESH_ROOT tcp 480 8 8 --offered 8 --maintenance 2
BINARY OTHER_FRESH_ROOT quic 480 8 8 --offered 8 --maintenance 2 --pause-follower 1:5
```

`--maintenance SECONDS` (1–60) selects periodic opportunities strictly before the
scheduled offering horizon. A wave asks the current leader of one round-robin
group for a checkpoint. Admission is not completion: the benchmark waits for the
same leader/term/store binding, a strictly advanced durable snapshot base at or
beyond the requested applied boundary, and drained snapshot routing/worker work.
It then submits one WAL reclamation per replica and matches each exact full
ReclaimTicket before accepting its result. Later opportunities while a wave is
active are recorded as skipped; there is no deferred maintenance queue. Missed
opportunities after a host stall are also explicit skips. Bounded raw history
follows the 300-second offering cap and minimum maintenance period.

The optional `--pause-follower START:DURATION` requires maintenance mode. Integer
seconds select a pause of replica 3's host reactor, lasting 1–5 seconds and ending
before the offering horizon, with at least one maintenance opportunity in the
window. It must be a follower in every group. Workers, sockets and files remain
owned; this is a polling stall, not a process crash or worker restart. Original
intended client times continue. Resume waits the full requested duration from the
observed pause start; intended and actual timestamps are both retained. A late
start that cannot fit the full duration before the horizon fails the run.
During the pause, maintenance prioritizes a group
whose leader applied beyond the leader's highest accepted index captured at pause
start. The successful catch-up gate requires that checkpoint to complete while
the follower remains paused, a new snapshot install on replica 3 after resume,
its durable base at/after that boundary, and unchanged source leader/term/binding.
That evidence distinguishes real snapshot catch-up from merely resuming polling.

A host-poll stall retains socket and transport data, including possible log
messages sent after the pause began. It therefore cannot guarantee that the
selected group will need a snapshot. Slice106's explicit buffered/dropped
delivery tests show why the selected durable base must be checked independently
of aggregate install counts. A failed snapshot gate is retained as a failed
experiment; it does not by itself establish lost acknowledged data. The live
QUIC packet history is not reconstructed by those deterministic tests.

`maintenance.csv` retains each opportunity, checkpoint boundary, exact reclamation
sequence, before/after bytes and admission-to-observed-completion times, plus pause
and resume observations. `bases.csv` records every replica/group's base before
close and immediately after reopen; bases may advance but cannot regress. The
ordinary useful-write/refusal/raw-history, all-replica values, quorum reads,
recovery/retry and full join gates still apply. Client drain also waits for active
maintenance and all opportunity decisions; unknown or failed work invalidates a
successful summary and preserves diagnostic artifacts/cleanup attempts.

Maintenance times include queue and terminal-observation delay. A paused reactor
can delay observing a completed worker operation. They are not pure physical I/O
costs or an additive elapsed critical path. The existing storage observer's
append/barrier primitive counters omit internal snapshot and replacement I/O;
explicit reclamation rows provide actual rewritten before/after bytes separately.
No new durable token, protocol, provider, resource count or timer default is added.
The independent checker validates maintenance schedules, bounded wave ordering,
bytes, observed boundaries, recovered bases and selected catch-up arithmetic;
it does not prove protocol correctness or sustainable fixed-p99 capacity.

Slice 105 archives 60-second control and maintenance cases in
[validation/performance/slice105](../validation/performance/slice105/README.md).
TCP maintenance includes verified paused-follower snapshot catch-up; QUIC's
paused case fails that gate and is retained. A separate QUIC run verifies
maintenance without the pause. These are selected finite observations, with
all failed/preliminary cases kept separate and the fixed-p99 target still unmet.

## Startup journal diagnostic mode

Append `--journal-timings` to the four-argument startup command to retain
`journal.csv` phase snapshots of actual native file append, log-sync and
manifest-publication calls. It uses the same startup assembly and timers, with
optional fixed-size atomic timing handles; no observer callback or per-operation
artifact I/O. Logical LogStore counts are unavailable on this assembly (zero).
The summary includes `journal_timings=true`, and the fixed serial acceptance
checker rejects that diagnostic in place of the uninstrumented reference.
Parallel worker time sums overlap; these timings attribute file work, not every
client critical-path dependency or separate-host capacity.

Instrumented startup runs also retain `publication.csv`, with separate counters
for staging-file open/write, file synchronization, rename and directory
synchronization. Each row records calls, errors, total/max duration and enclosing
manifest totals. No synchronization or publication ordering is skipped. The
directory step includes opening the directory. Physical WAL replacement has its
own path and is not included in these publication-step counters.

```sh
node validation/check-publication-timings.mjs RUN_DIRECTORY/publication.csv
```

Live phase snapshots can fall inside a call; they are not atomic observations.
Joined snapshots require all five successful step counts to agree with the
enclosing call count. Failed runs retain partial diagnostics, which the complete
result checker refuses. Per-replica durations overlap, and adding them does not
reconstruct client latency. Keep `--journal-timings` off for the original fixed
serial acceptance gate.

Slice158 adds one successful60-second QUIC pause run with the original gate:
group1's forced boundary11 is installed and retained across reopen, two new
follower installs occur, and the source leader/term/binding is unchanged. All
value/read/retry/join and independent raw-result checks pass. Earlier failed
runs remain in slices105/106; this success does not reconstruct their packet
histories or make host polling stalls guarantee snapshot necessity. Applied
p99=2918.173043ms remains outside the fixed-p99 target. Raw artifacts and exact
scope are in [slice158](../validation/performance/slice158/README.md).

Separate TCP/TLS and QUIC tests close every transport after both surviving
replicas compact beyond all eight stale follower prefixes. Reopening the actual
native files requires a snapshot for each group; restored applications, exact
retries and another full restart are checked. These are controlled recovery
histories, not throughput measurements or substitutes for the polling-stall run.
