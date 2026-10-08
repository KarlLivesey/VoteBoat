# Snapshot admissions tied to effect leases

`runtime::SnapshotRouter` is fixed bounded coordination over the public
`SnapshotWorker` seam. It owns the admission-to-lease map for one exact
`RuntimeOwner` and one construction-selected `SnapshotWorkerBinding`. It creates
no threads, stores, network connections or applications. The host supplies the
matching `EffectOwner`, selected worker and group-bound application for each call.

## Admission and reservation

Defaults allow 16 accepted requests and images up to 128 MiB. These limits are
independent of worker, effect-owner and transport limits; the tighter admission
limit wins. `SnapshotWorker::load_reservation(group)` declares a stable finite
capacity allowance for a selected group, or rejects an unassigned group. The
native worker derives it from that group's selected snapshot store limits.

`submit` validates the exact runtime owner, selected worker generation and live
effect lease before preparing work. StageSnapshot, SnapshotRequired,
SnapshotInstalled, CheckpointRequired and CheckpointCompacted enter this path.
It rejects another accepted snapshot for
the same group, an exhausted request map, excessive declared image size or an
oversized original Publish image before transferring ownership.

Before preparing a Publish clone or admitting a Load, the router ensures the
live visit reserves its queued/original effects plus a loaded-image allowance
and envelope slack. Retry reserves a minimum rather than repeatedly increasing
the same lease's charge. A requirement above the owner's entire applicable
ceiling returns a size error; temporary shared saturation returns overload. It
preserves effect-owner control reserves. Application
clone and provider scratch memory remain separate budgets.

For local checkpoints, the worker also declares a stable `checkpoint_bytes(group)`
serialization ceiling. The router reserves image space before calling application
serialization, then checks the returned capacity-costed image. Invalid boundaries
or excessive images return the original lease without submitting I/O.

Preflight validates the application image on the serialized owner. Worker
rejection drops the returned prepared request and returns the original lease
under the same ticket; no accepted mapping is created. The caller can retain
that lease for bounded retry. Prepared clones and rejected host staging never
escape into an unbounded internal queue.

Successful admission must return the selected binding and a strictly increasing,
nonzero worker sequence. The router records that ticket and the original lease.
The worker may also serve other owners; gaps are allowed. An invalid accepted
ticket fences the effect owner and returns the original lease for explicit failed
owner discard. The provider may have performed external work despite its broken
envelope, so the host still drains/recovers it. That error is not rejection-before-
external-progress and does not cancel the accepted prepared request.

## Exact terminal delivery

`deliver` first validates its matching runtime owner, worker binding, known
request and original runtime visit. Unknown, repeated, old-generation and
wrong-visit events return StaleCompletion without touching the core or removing
the accepted mapping. A request's one terminal event removes that mapping once.
Loaded image capacities must fit the allowance saved at admission; an oversized
provider result fences service before core delivery.

The router passes the original effect to `complete_snapshot_work` through
`EffectOwner::complete_effect_with`. That existing helper validates the snapshot
stage and calls the existing core transitions. Publication returns only Persist;
the WAL worker's exact durability produces SnapshotInstalled; verified retention
and application restore produce SnapshotAck. Snapshot reads for Send use the
original leader request context. No new quorum rule, durable watermark, Raft
acknowledgement type or on-disk format is introduced.

Local maintenance uses the existing checked RequestContext sequence, scoped to
the WAL session and group. CheckpointRequired asks the application owner to
serialize its contiguous applied prefix (strictly beyond the old snapshot base
and at most the committed prefix). Publication/pinning releases only Persist.
Exact WAL durability releases CheckpointCompacted. Its Reconcile request verifies
the pinned image before releasing older pins; only its exact Reconciled completion
finishes maintenance and allows a leader's refreshed replication requests out.
The visit remains suspended throughout. It grants no vote, quorum evidence or
client result. Restart reconstructs the snapshot from the authoritative WAL and
existing pin manifest; no maintenance generation or new file format is persisted.

A storage/install error fences the effect owner. The router explicitly discards
the current failed lease after dropping its payload; other accepted leases remain
charged. `discard_failed` drops those router-owned payloads only after the exact
owner is fenced. Accepted provider work remains unresolved until provider drain
or recovery. Dropping the router abandons observation and cannot make the live
effect owner forget its outstanding reservations or dependencies.

The application argument must be the host's application for the original group.
A provider contract and truthful application boundary remain trust assumptions;
this routing map is not independent proof of storage or application correctness.

## Shutdown and composition

Close effect-owner ingress and keep polling/delivering all existing dependencies.
A publication can produce WAL work and then require an installation Load during
this drain. The router therefore allows these continuations after ingress closes.
Only after the healthy effect owner and router drain should the host close and
reclaim the snapshot/WAL workers and drain outbound transport. Local checkpoint
drain similarly requires publication, WAL anchoring and retention reconciliation.
A failed owner
uses explicit discard plus provider drain/recovery instead of healthy shutdown.

The router's identity is the existing runtime owner, including WAL session,
execution lane and runtime generation. Recovery or owner reassignment uses a
fresh owner; an old router cannot deliver into it. The selected snapshot-worker
generation stays scoped to its WAL session. No additional persistent generation
is required.

## Evidence

Core-only tests use injected scheduler/timer/entropy, persistence and snapshot
workers. They cover original rejection ownership, stable reservation on retry,
request-map limits, excessive declared images, wrong worker binding, stale worker
generation and runtime visit, duplicate terminal delivery, invalid accepted
tickets, oversized results, failure discard, and shutdown through all three
installation dependencies.

A three-node/100-group history combines native ready/timer/entropy providers,
EffectOwner, SnapshotRouter, native WAL and snapshot threads, outbound queues
and native peer transports over actual TCP/TLS with the default feature set.
Two replicas begin from explicitly pre-seeded committed counter checkpoints and
pinned logical compaction. The initially empty third replica installs all 100
images through the network and the router, then catches up through ordinary
Raft replication. It checks fresh reads, operation retries and actual-file restart
with fresh WAL/store sessions before further replicated writes. The native timer
provider triggers network heartbeat traffic without explicit Heartbeat events.
Elections in this history are explicit campaigns. It now also creates repeated
local checkpoints for all replicas while both workers retain their stores, checks
advancing snapshot generations/bases, and repeats maintenance after actual-file
recovery. Checkpoint admissions are batched within background ingress ceilings.

The driver retains rejected outbound sends under their original effect tickets,
reserves decoded ingress, bounds rejected snapshot staging by active leases and
checks that every live external lease has a bounded holder. Accepted/received
frame accounting prevents false network quiescence. Native-without-TLS uses
bounded simulated delivery; no encryption claim applies to that path. The
existing 100-group replacement/partition/recovery history also now selects native
runtime providers. Earlier host conformance and snapshot I/O fault histories
remain separate evidence.

A separate native 100-group history uses timer-driven elections from empty
bootstrap, isolates the node holding the most leaders, replaces those leaders
through the surviving quorum and heals. It verifies that isolated uncommitted
writes never change the final applied history, then checks retries and fresh
reads. Its default build uses actual TCP/TLS; isolation occurs after decoding.
This bounded seeded history does not cover the full network/storage fault matrix.

Full node facade/admission, peer roster/reconnect handling, application result
routing, asynchronous checkpoint creation/compaction and broader network fault
histories remain pending. macOS execution and hardware power cuts
remain unobserved. The full P0–P7 implementation goal stays active.
