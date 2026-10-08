# Live WAL-worker maintenance

`PersistenceWorker` now offers optional `submit_reclaim(max_bytes)` and bounded
`poll_reclaims(limit)` operations. Defaults reject Unsupported and return no
maintenance events, so existing host workers retain their write-only contract.
`LogStore::supports_reclaim` advertises the selected store's capability; its
default is false. NativeLogStore advertises its selected journal/codec support.

NativeLogWorker captures this capability and the store's WAL ceiling at explicit
construction. It admits at most one maintenance request, using the same checked
worker request sequence and bounded queue as persistence. ReclaimTicket is a
separate type scoped to the exact store session and worker generation. It is an
admission identity, never a durability ticket or contiguous log prefix. Restart
uses the existing fresh store session and explicitly selected worker generation;
maintenance adds no persistent counter or format.

The scalar request and fixed correlation/result metadata consume ordinary data
request/byte credits. Control reserves remain available. The image's encoding
ceiling uses the selected store's separate WAL budget, not the persistence batch
ceiling. Unsupported capability and invalid byte limits reject before transfer or
sequence consumption. Accepted requests, active work and unpolled terminals retain
worker credits; polling a terminal releases them. Polling zero does nothing.
Generic worker/facade budgets require a positive ceiling; they impose no native
frame-size minimum on host stores. Native encoding may safely reject a ceiling
too small for its own checkpoint format when the operation executes.

## Execution and completion

Maintenance executes on the already selected blocking WAL thread, between complete
append/barrier operations in queue order. Earlier writes have reached their store
barrier before cleanup starts. Their owner-side Written/Durable receipts can
remain unpolled because cleaning preserves their exact logical state and ticket
IDs. Later accepted writes remain bounded in the queue and execute against the
same binding and revisions after replacement. No additional worker/runtime/store
is created, and the serialized node owner performs no cleanup I/O.

ReclaimEvent contains the exact ticket and either LogReclaimed byte statistics or
a storage error. It is delivered separately from WorkerEvent and cannot certify
Raft persistence, application success or snapshot installation. Native execution
checks that the report shrinks/preserves byte usage within its supplied ceiling
and that maintenance did not change the authoritative store binding.

A safe Rejected result leaves the lane usable. Uncertain, Corrupt and Fenced
results fence the worker; queued persistence then fails without fabricated
Written/Durable stages. A worker panic/disconnection produces one uncertain
terminal for each still-retained request in its own event class. Both event
classes must be polled to drain. Close stops new intake and drains accepted work;
explicit try_reclaim joins the worker and returns its store, or reports the panic.
Abandoning a maintenance wait cannot cancel physical replacement or decide which
pair recovery will select. Physical crash safety still follows
[the WAL replacement protocol](WAL_RECLAMATION.md).

## Driver and node API

ReplicaDriver.request_reclaim submits through the selected PersistenceWorker and
retains one original ticket/budget. Each driver poll processes at most one
maintenance event, checks exact scope and report bounds, then retains one matched
result. poll_reclaim transfers that result to the caller; another request is
refused until the prior result has been taken. Result statistics have no owned WAL
payload or Raft effect lease. Fixed metadata is included in driver construction
limits and usage; it is separate from effect-lease counts.

Node.reclaim is available only while Running; Node.poll_reclaim remains available
through shutdown or failed recovery. Pending cleanup and unconsumed results keep
healthy shutdown from reaching Drained. Fatal results fence the owner and enter
RecoveryRequired while preserving the matched failure result, earlier client
replies and other accepted provider work. Foreign or contradictory receipts fence
coordination and retain the original ticket plus observed bad receipt for explicit
recovery inspection. Failed-domain cleanup does not pretend to cancel accepted
storage work; drain/recover the selected worker explicitly.

This baseline pauses further I/O on that WAL lane during the bounded full-image
rewrite. It keeps the node owner and other selected lanes independent, but makes
no latency, foreground fairness or throughput claim. Incremental cleaning and
automatic scheduling/throttling remain future work.

## Evidence

Four host-node tests cover held-result shutdown, unsupported/safe rejection and
continued proposals, wrong receipt fencing and original ticket retention, and
uncertain failure preserving earlier replies and unknown pending writes. Five
native-worker tests use an independently supplied host LogStore with gated
maintenance to cover complete-barrier ordering, retained credits/close/join,
control reserves, capability/budget rejection without sequence consumption, safe
refusal, uncertain/invalid/binding-changing providers and panic recovery of both
request classes. The actual three-node/100-group WAL/TLS facade history now cleans
inside live selected workers, continues new writes, consumes exact maintenance
results, then shuts down, reopens files, restores snapshots and retries operations
before further writes. These finite Linux histories do not establish macOS
execution, arbitrary schedules, incremental cleaning or performance.
