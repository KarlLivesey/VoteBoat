# Asynchronous snapshot work

`SnapshotWorker` is an object-safe, construction-selected snapshot I/O contract.
`native::snapshot_worker::NativeSnapshotWorker<S>` implements it with one explicit
blocking thread owning a bounded map of selected `SnapshotRetention` handles.
The worker owns no WAL, core, application, listener or hidden executor. Different
groups can share this worker; a slow snapshot I/O operation can delay its other
snapshot work, while the separate WAL/runtime can continue. No latency or
throughput claim is made.

## Bindings and accepted work

A `SnapshotWorkerBinding` includes the authoritative WAL's recovered
`StoreBinding` and a distinct `SnapshotWorkerGeneration`. The host supplies a
fresh generation within that WAL session; restart changes the persisted WAL
session before work resumes. Snapshot stores have their own persisted sessions.
Selected handles must be quiescent before transfer. Construction checks their
store identities and group keys rather than requiring
their sessions to equal the WAL's. No new durable watermark is introduced.

A work item owns one scoped runtime `VisitTicket` and one Publish or Load job.
Admission returns a checked worker-local sequence or returns the original work
on rejection. One request per group can remain accepted, including an unpolled
terminal event. Request/byte credits are retained until terminal polling transfers
its output to the owner. Close rejects new work and drains accepted work.
`try_reclaim` returns the selected handles after close and terminal delivery.
Dropping observation does not cancel a publication, pin or retention change.

The native provider validates every selected store and output allowance before
creating its thread. A failed operation conservatively fences further work on
this shared worker; accepted requests receive failure rather than success. A
panicked/disconnected worker reports unknown outcomes for retained requests.
These stores must be recovered before reuse. Its shutdown does not close a
host-owned executor or another worker.

## Resource accounting

Defaults bound 4,096 selected group handles, 16 requests and 256 MiB of retained
work/completion reservation. Two requests and 64 MiB are reserved against bulk
snapshot sends. Publish and installation loads use control capacity; send loads
cannot consume that reserve. These are configurable limits, not progress promises
when control consumers stall. Provider handles and their resident metadata have
separate host budgets.

Admission charges original Publish image capacities, fixed envelope slack and a
finite maximum loaded-image allowance for every operation. The allowance is the
selected store's application ceiling plus eight metadata ceilings and 4 KiB.
`snapshot_image_bytes` checks actual payload/vector capacities and bounded policy
structure. Native reads validate file sizes before decoding; returned images
must also fit their allowance. An excessive original vector capacity rejects
before transfer. Provider file/codec scratch, core/application clones and other
runtime memory remain separately budgeted; this is not a process RSS guarantee.

The caller must reserve space for a returned image before submitting/polling a
Load. With `EffectOwner`, keep the original lease and use `extend_reservation`
before work that needs additional output space. A rejected prepared Publish
still owns its cloned image; keep it under a host request budget or drop it before
retrying. Worker credits cannot account for host-owned rejected or polled data.

## Installation dependencies

Use `prepare_snapshot_work` on the serialized owner. It checks the authoritative
binding and visit group. For StageSnapshot it validates the exact staged message
and restores its application checkpoint on a clone before preparing a Publish.
The original effect remains leased; the worker never calls application code.

1. Publish reconciles retention against the owner's current durable log anchor,
   stages ordered chunks, seals/synchronizes, publishes, pins and loads/verifies
   the exact image. Only then does it return Published(reference).
2. `complete_snapshot_work` checks the exact recorded admission ticket and visit
   before core access. For that original StageSnapshot, it calls the existing
   core transition and returns Persist. Publication alone releases no remote
   acknowledgement or application success.
3. Submit Persist through the selected WAL worker and deliver its exact Written
   and Durable events on the owner. Written releases nothing. Only the actual
   durable log reference produces SnapshotInstalled.
4. Prepare an installation Load for that reference. The snapshot worker verifies
   the pinned image and reconciles retention to this already-durable reference.
5. Completion validates the reference/bootstrap and application boundary, restores
   a clone, replays the committed tail and calls the core's snapshot-applied
   transition. It replaces application state only after validation succeeds.
   Only this stage produces SnapshotAck.

An absent reconciliation attestation, invalid application data or failed storage
completion fences the core and releases no acknowledgement. Existing native
publication and WAL formats, durability tickets and pin rules are unchanged.
If the process stops before the WAL reference is durable, recovery discards an
orphan publication/pin using the authoritative log. If it stops after WAL
durability but before application completion, recovery restores the pinned image
and retry state. Receipt loss does not justify pretending the operation failed
before external progress.

For SnapshotRequired, prepare a send Load. It preserves retention and completion
calls `snapshot_send` against the original leader request context. The resulting
owned Send enters the separately bounded outbound path. Snapshot load success
is neither network delivery nor a remote durable acknowledgement.

`EffectOwner::complete_effect_with` passes the original leased effect into a
serialized callback, avoiding a snapshot clone just to inspect it. Hosts record
successful snapshot admissions in a bounded map tied to original lease tickets,
reject unknown/repeated/obsolete events, and remove each mapping once. The
completion helper validates the supplied expected envelope; it does not own that
map or authorize a caller to reuse an old expected ticket. Map errors into the
owner's failure path and drain/recover accepted worker requests. Provider
attestations assume the selected public storage contract, not Byzantine proof.

## Evidence and remaining assembly

Downstream tests select a host `SnapshotWorker` through a trait object and native
threads over host `SnapshotRetention` handles. They cover rejection ownership,
zero-limit polls, stale generations, failure, excess capacity, control reserves,
exact leader send contexts, unpolled-credit retention and explicit close/reclaim.

The real-file history uses native scheduler/timer/entropy providers, an
`EffectOwner`, the native snapshot thread and native WAL thread. It covers
publication before WAL submission, durable installation with lost application
completion, normal acknowledgement, restart and operation deduplication. It
injects a protocol message directly; it is not a network
or automatic-election history.

A thread-safe crash model runs native snapshot storage through short writes,
failed/lost syncs, failed/lost publication and pin receipts, plus provider panic.
No failed request advances the core/application; recovery discards orphan data
without a durable WAL anchor. Earlier snapshot histories retain their broader
synchronous corruption and durability coverage.

Native checkpoint creation/compaction still require quiescent synchronous store
access. Full node admission/lease routing, network snapshot catch-up through the
new worker, peer reconnect handling and broader automatic-timer histories remain
pending. macOS execution and hardware power-cut testing remain unobserved. The
full P0–P7 goal remains active.
