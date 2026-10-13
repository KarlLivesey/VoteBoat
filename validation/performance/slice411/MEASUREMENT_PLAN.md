# Paired411 durability and412 provider measurement plan

Status: no candidate snapshot/build or timed run has been started. Root later
authorized isolated c35baseline builds on both hosts after this plan was ready;
those build-only artifacts are recorded separately below the baseline directory.
Reference is immutable committed c35e1f1. Candidate must be explicitly frozen
after411Core/physical-owner integration and412OwnedLogState controls finish.
Run the same paired campaigns on Karl's Linux host and Apple M2 Max separately.
Compare variants within a host; hardware/storage differences are not speedups.

## What the existing native harness already supports

`native_benchmark FRESH_DIRECTORY tcp|quic OPERATIONS WINDOW` runs three
real native replicas, real FileLogIo WAL synchronization + atomic manifest
publication, TCP/TLS or QUIC loopback connections and the actual Node/worker/
EffectOwner/Counter path. WINDOW1..32 admits that many simultaneous original
client requests; it is not restricted to serial writes. Payload is8bytes.
Its64-operation warmup is separate from measured timing.

Completed rows count only original ClientOutcome::Applied receipts with exact
ticket/operation identity, non-duplicate Value outcomes and the expected ordered
Counter value. Before successful summary, all3 applications catch up, quorum
reads agree, original workers/transport drain, the same stores reopen, earliest
and latest original operations retry with historical outcomes, then all workers
join again. Startup/recovery/close time is outside measured elapsed_s.

Both native startup and shared assemblies use physical owners. Current411
integration selects eligible ordinary append Written events automatically; no
new CLI enable switch is required for the first campaign. We must still inspect
the final frozen implementation to confirm that selection did not change.

## Minimum useful durable campaign

One untimed functional pilot per variant/host/protocol may qualify the existing
harness before measuring; a failed run remains a failed artifact. Do not include
pilot results in the three-trial medians. Suggested pilot:32 measured operations,
window4, journal timings. This is a pilot execution plan, not permission to run
while candidate source remains unfrozen.

Main campaign: protocols tcp/quic × windows1/32 ×3 alternating reference/
candidate pairs =24 measured runs per host.256 original operations per run,
64 warmup, one hot group. Candidate/reference order alternates each trial.
Every run gets a distinct fresh directory on the same local filesystem/device.

```sh
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY tcp 256 1 --journal-timings
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY tcp 256 32 --journal-timings
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY quic 256 1 --journal-timings
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY quic 256 32 --journal-timings
```

Startup mode avoids changing group count/timer settings between variants and
exposes actual append/sync/manifest publication timings. It uses heartbeat50ms,
election minimum/spread1000ms. Record these rather than silently substituting
shared-mode's10000ms timers. If functional pilot shows these timers unsuitable
on a host, choose shared mode consistently for all paired runs, not per variant.

Required raw outputs: argv, stdout/stderr/exit;256 unique samples.csv rows with
operations65..320, correct monotonically ordered group values, no duplicate or
missing original IDs; summary operations256/warmup64/recovered_value320 and
retry_verified/workers_joined true; all retained journal.csv/publication.csv
and original journal directories. Require successful recovery before accepting
throughput. Any refusal/Unknown/partial completion invalidates this closed-loop
case; preserve failure and cleanup files rather than treating it as lower TPS.

Report per-variant trial data and paired medians: useful applied_ops_s, p50/p95/
p99/max operation latency; max_inflight, poll_rounds, host_poll_ms,
max_host_poll_ms, persistence_batches, worker_events, application_deliveries.
Subtract cumulative before_measurement from after_measurement for append/sync/
manifest calls, elapsed totals and encoded bytes; report syncs and barrier calls
per useful applied operation. Do not add concurrently occurring replica timing
totals into client latency. Journal maxima/histograms are cumulative snapshots;
maxima cannot be subtracted into an exact measured-window maximum.

This is a real synchronized-write + three-voter-quorum/application benchmark,
on one physical machine and one filesystem. It does not establish independent
failure domains or remote-machine WAN throughput. Clients invoke the embedding
Node::propose API locally; peer replication uses real sockets. No remote client
connection, HTTP/JSON service framing or service command parser is included.

## Optional overload stage after the minimum campaign

Existing shared-mode offered load already records refusals, intended/dispatched/
completed timestamps and drain/backlog. No new harness is needed to inspect
overload once native throughput is measured. Fixed512 offers, window32, groups1,
no maintenance. Choose2 integer rates per host only after the preceding paired
closed-loop results: roughly50% and150% of the lower variant's measured window32
rate; use the same rates for both variants and record the arithmetic.

```sh
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY tcp 512 32 1 --offered RATE
ARTIFACT/native_benchmark FRESH_CASE_DIRECTORY quic 512 32 1 --offered RATE
```

Retain all512 offers.csv rows/statuses, including window_refused, no_leader,
admission_refused and not_proposed. Unknown/pending/invalid outcomes prevent a
successful result. Report admitted/applied during offering separately from
applied during drain, dispatch_p99, service_p99/end-to-end p99, max_inflight,
backlog_at_horizon and drain_ns. Do not count refusals as useful writes or hide
them by computing latency only over accepted rows without the refusal counts.
Shared assembly has one WAL/snapshot worker/endpoint per replica and10000ms
election timers; keep that assembly distinct from the startup closed-loop data.

Do not add maintenance, follower pauses or static execution lanes to this first
411attribution campaign. Existing --maintenance, --pause-follower and --lanes
support later sustainability/scaling work, but introduce additional variables.

## Separate412 framework/provider campaign

Reference c35e1f1 and final combined candidate use the public-core scaffold.
The workload driver/settings must match. MemoryStore changes from borrowed
apply_batch into the public OwnedLogState provider are the deliberate412
experimental variable; that adapter cannot simply be copied into reference
because the helper does not exist there. Record exactly that source difference.

The scaffold uses synchronous public persist_effect completion; it does not
execute411EffectOwner's asynchronous Written continuation. Its results support
412provider/staging cost claims, not411native replication-overlap claims.

Full concurrency grid including the requested512point:
clients1/64/256/512/1024/4096 ×batch1/4 ×3
alternating pairs =72 runs per host. Use16384 measured operations,1024 singleton
warmup and32768 retained-entry budget, with no hidden reset/checkpoint. This is
the smallest uniform operation count that admits4096 clients with4entry batches.
All3 applied/retained frontiers must be17409; completed count16384 and completed
batches16384/BATCH. Persist full JSON and exact original completion validation.

```sh
ARTIFACT/framework_benchmark --clients CLIENTS --operations 16384 --warmup 1024 --batch BATCH --retained-entries 32768
```

Report operations_per_second, request_p50_ns/request_p99_ns, peak pending
operations and message queue, delivered_messages, persistence_units and complete
comparison mismatch labels. Request latency is completion of every original
operation in one client batch, not a guessed per-operation batch latency.
These remain volatile/in-process/single-cooperative-driver diagnostics and
unmatched to OpenRaft; do not claim the minimum floor from this comparison alone.

## Build and identity gates before either campaign

Freeze each complete source +fixture tree; preserve source manifests and exact
commit/working-tree distinction. Separate CARGO_TARGET_DIR per variant and host.
Never reuse one target directory across older simultaneously prepared snapshots.
All-feature release opt3, debug assertions=true, overflow checks=true; no
target-cpu=native or other new flags for just one variant. Capture Cargo JSON:

```sh
CARGO_TARGET_DIR=ISOLATED_VARIANT_BUILD CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true cargo +stable build --locked --offline --release --all-features --example native_benchmark --example framework_benchmark --message-format=json
```

Require actual non-fresh library/example compilation from the frozen source,
matching profile/features; copy/hash original executable and library artifacts
immediately and refuse accidentally identical changed variant artifacts. Check
every source and immutable copy before and after timing. Keep builds/tests off
the measured host during the campaign. The invalid shared-cache409attempt stays
separate evidence of why source hashes alone are not enough. Hardware metadata
includes CPU/core counts/memory/OS/compiler and filesystem/mount/device type
without serials. Do not silently run durable journals on tmpfs; use the chosen
host's normal local filesystem. Remote control artifacts may live in/tmp, but
verify the actual measurement filesystem before accepting disk results.

## Missing knobs / practical limits

No additional capability or CLI knob is required for the minimum durable
campaign. Window32 is enough to exercise queued batching and possible overlap;
it is not a claim to cover4096 durable clients. It cannot fill the64-command
ready-batch ceiling; larger window support is later saturation coverage.

Existing outputs do not count actual pre-barrier replication sends or correlate
Written/send/Durable timestamps. Therefore end-to-end speedup alone cannot prove
that overlap was exercised or identify its fraction. Use411controlled ordering
tests for the causal contract. A small transparent overlap counter/event trace
would be needed for direct runtime attribution, if the first durable results
leave that mechanism uncertain; it is not a prerequisite to first measurement.

Existing startup --journal-timings cannot combine with shared GROUPS/offered/
lanes. Offered/shared storage.csv already counts provider append/barrier/sync
work but does not publish the detailed native manifest-substage journal report.
No per-group fairness latency or queue-byte/wakeup-lag sample is emitted by the
first closed-loop summary; do not infer those guarantees from max_inflight.
