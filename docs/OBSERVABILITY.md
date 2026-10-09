# Bounded node observations

`observability::Observer` is a synchronous diagnostic contract. The host calls
`NodeObservation::from_poll(owner, time, node.state(), &result)` after the owning
Node's poll returns, then `record_bounded`. Keep the original poll result even if
the observer rejects the sample. Node/core do not invoke, own or await an observer.
The counter service explicitly selects `native::observability::NativeCounterObserver`
and uses the same public contract as a host replacement.

Observations contain RuntimeOwner, host-supplied monotonic sample time, post-call
NodeState and 13 fixed u64 counters. There are no group labels, request payloads,
strings, event queue or persistent format. Capture counts returned owner steps,
step errors, worker/snapshot events, snapshot installs, persistence batches,
application deliveries and peer sends/receives/blocked ingress/connection failures.
It walks the already bounded returned step list. Failed polls count once as failed
and carry no invented partial-progress counts. Application deliveries are not
successful client commands; peer sends are not remote durable acknowledgements.

Native counters occupy one fixed-size value, allocate no resources and use no
I/O, clocks, locks or threads. Counters saturate at u64::MAX. Exact owner binding
(including store session, lane and runtime generation), nonregressing time and
open scope are checked before mutation. Equal times are allowed; repeated samples
count repeatedly and are not deduplicated. No counter is a watermark or authority.

`snapshot_counters` exports a Copy CounterSnapshot; it does not reset counters or
perform external I/O. The host can pass this value to its own exporter outside
consensus execution. `close` permanently stops this observer view and remains
idempotent; the final snapshot stays readable. Dropping a view does not close a
host's shared exporter. Hosts implementing Observer must keep record/snapshot/close
bounded and nonblocking and scope closure to their own view. Rejections return
WrongOwner, ClockRegressed or Closed without changing the native snapshot.
There is no accepted asynchronous diagnostics work, cancellation ticket or hidden
export thread. Construction is explicit and contract compatibility is the Rust
version1 interface; no persistent/wire negotiation is involved.
Host sinks can return `Overloaded` when their bounded capacity is unavailable;
the sample was not retained and consensus must continue with its original result.

Restart constructs a new collector for the new runtime/store session. Counts
are volatile and do not include startup replay performed before Node polling.
They do not establish leadership, quorum availability, a safe read, successful
application results or durable recovery. Observer refusal never changes the
original Node result. Misbehaving host implementations that block or panic violate
the provider contract; no arbitrary callback is sandboxed by this library.

The local trusted command port supports:

```sh
voteboat-counter client BASE_PORT NODE metrics
```

The reply begins `OK evidence=local_volatile store_session=...` followed by the
fixed numeric counters. It performs no consensus operation and can be queried on
a follower. It is bounded below 1KiB even with saturated counters. The automatic
client accepts only reads/writes; choose a concrete node for its local metrics.
The command uses the existing trusted loopback controls, not a remote metrics or
authorization service. Richer timer/queue/I/O latency, budgeted per-group events
and external exporter integrations remain future work.

Validation: tests/observability.rs implements an independent host observer and
checks fixed counts, shared views, owner/session/generation rejection, clock
regression, saturation and export after close. tests/support/node.rs injects a
refusing observer beside real Node writes/reads/shutdown. tests/counter_service.rs
runs three-process TCP/TLS and QUIC metrics/write/read/retry histories, closes/
joins workers and reopens stores; new counters start without replay deliveries
while original retries and values survive. These are finite conformance histories,
not a performance or full protocol proof.

## Optional native journal timing

`native::log_store::JournalTimings` is a fixed-size, volatile native file
measurement handle, separate from the post-poll Observer contract. Explicitly
attach it with `FileLogIo::with_timings`, or pass `NativeStartupTimings` to static
`NativeStartup::open_with_journal_timings`. The host retains a clone and reads
`snapshot()`; dropping that clone never closes files or workers. The default
file path retains no timing handle and performs no timing clock reads.

Append, log sync and complete manifest publication each expose completed call
count, error count, elapsed nanoseconds and maximum duration. Updates saturate;
there are no callbacks, queues, exporters or per-group labels. Current-state
snapshots may interleave with updates; read after worker join for stable totals.
These counters do not acknowledge an operation, identify a durable prefix or
bind a provider completion. Parallel worker durations overlap and cannot simply
be added to calculate client critical-path latency. Creation/replacement and
snapshot-file costs are outside these three call categories.

`native_benchmark ... --journal-timings` selects the startup-only diagnostic
assembly with the original timers and durability. Its summary is explicitly
marked; `journal.csv` contains phase snapshots, with logical LogStore counters
unavailable (zero), rather than fabricated. The fixed serial acceptance checker
rejects marked diagnostic runs. A separate uninstrumented reference is required
for performance acceptance.
