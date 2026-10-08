# Reserved effect ownership

`runtime::EffectOwner<Q, T, E>` owns a quiescent `TimedShard` and coordinates its
output lifetimes. Scheduler, timer and entropy providers remain replaceable;
consensus and durability validation remain fixed. The selected persistence
worker stays host-owned and is identified by its exact `WorkerBinding`. The
owner creates no thread, store, listener or application runtime. A construction
check rejects the wrong store binding and insufficient reservation limits.

## Reserve before execution

Before stepping one ingress event, the owner reserves output space for that
visit's event and subsequent durable completions. `Raft::effect_reservation`
conservatively includes the current retained log, four input budgets, at most
64 entries per replication append, append/read fan-out to the configured voter
set, and metadata slack. It uses checked arithmetic and the selected log batch
limit. The bound is tied to the current native core's effect paths, not an
arbitrary provider's promise. Exactly one event runs in a visit. A larger log
can require a larger reservation on a later visit; an estimate above the entire
configured owner budget fences service before executing that event. Recovery
with adequate budgets or future compaction/admission integration is required.

Defaults allow 256 active visits and 256 MiB of conservative output reservation,
with 16 visits and 32 MiB reserved against non-control input. `next_class` inspects
the same bounded priority selection used by the runtime step. Bulk work cannot
consume these control reserves. Control classification does not guarantee
progress when every control consumer itself stalls; committed data can also be
released by a control acknowledgement. Capacity saturation requeues an unstepped
visit without consuming its event. A held group can coexist with other runnable
groups. Timer expirations retain their existing bounded overload behavior.

These reservations cover retained effect objects and payload capacities,
including leased work. Effect-array accounting includes metadata growth slack.
Core/log state, worker input/completions, outbound buffers, application state,
host task results and socket/TLS/codec memory have separate budgets. This is
not a process-wide RSS or CPU-duration guarantee. The reservation calculation
walks the current log; no throughput or zero-overhead claim is made.

## Exact leases and persistence

`advance(now, limit)` returns bounded invocation metadata and errors. It does
not report client success. `take_effect` fairly hands out one owned effect per
active group, tagged with its visit and a checked owner-local sequence. One
external lease blocks further outputs from that group. Its reservation and
ingress visit stay held until the lease and core dependencies are resolved.
Dropping an observer is not cancellation or a credit release.

Persist leases are submitted together through `submit_persists`, preserving
shared multi-group WAL batching. Preflight validates every exact core effect,
visit, store and worker generation before ownership transfer. Rejection returns
all leases for retry under their original tickets. The worker's request sequence
is an admission identity, not a durable prefix. Accepted requests retain their
visit set; Written and terminal stages must match it exactly. Unknown, repeated
or old-generation completions are rejected before core access. Durable before
Written, malformed visit sets or invalid core completions fence the owner.

`deliver_worker` applies the existing core admission/completion checks on the
serialized timed owner. Written releases no dependent effects. Only the exact
durable ticket can release the core's responses or committed entries, which
return to the reserved output stage. No new persistence token, quorum rule or
commit watermark is introduced here. The owner never passes network Sent as a
Raft acknowledgement.

## Application, reads and transferred sends

For a Send, transfer its owned message into the selected bounded outbound queue.
After acceptance, `release_transferred_send(ticket)` releases the effect lease
without cloning the message. If admission rejects it, retain the returned
message and its live effect ticket for retry. Outbound ownership and credits
then follow the queue and peer-transport contracts through local completion.

Apply Committed entries in order through the selected application. `release`
requires an applied index covering their original last index. The host supplies
the application's truthful boundary; an application provider remains obliged
to implement its state-machine contract. It must not acknowledge a client from
Committed alone. Application errors and receipts have separately bounded host
result retention.

For ReadReady, `complete_effect` consumes the original core barrier after
checking application progress, then invokes the serialized completion callback.
The callback can read immutable application state at that exact boundary and
return its result. Insufficient application progress retains the lease and
barrier for retry, without running the callback. A consumed lease cannot
provide authority for another read. The original invocation/query mapping is
host-owned. Return application query errors as part of the callback's result
rather than pretending they are successful reads.

Callbacks may return follow-up core effects, which are capacity-checked and
restaged before escaping. They must use the existing validated helpers and
perform no blocking I/O. Arbitrary host assertions are not independent proof of
security, storage durability or application correctness. `extend_reservation`
reserves additional finite space before host work that can produce a larger
result; bulk extensions cannot consume the control reserve. An oversized result
or a callback consensus failure fences service. The explicit
[snapshot worker](SNAPSHOT_WORKER.md) now drives publication and
installation through these callbacks. `complete_effect_with` exposes the
original leased effect without cloning its image. The host retains a bounded
admission-to-lease map and reserves loaded-image space before polling. Full node
routing and network snapshot catch-up through this worker remain pending.

## Failure and drain

A provider/core failure fences all groups in this owner. Accepted external
storage can have an unknown outcome and must be drained/recovered through its
host-owned worker. Unsent internal effects are dropped. Outstanding external
leases keep their reservation until consumed by `discard_failed`; failure does
not make their memory or progress disappear. Request metadata remains bounded
and records unresolved accepted persistence. A failed owner is recovered under
a fresh runtime generation, not resumed by clearing a flag.

`close_admission` stops new ingress and disarms automatic timers. Existing
visits, queued ingress, persistence and output leases continue to drain. Only
a quiescent healthy owner reports `is_drained`. The host then closes/drains its
selected worker, outbound queues and connections. Closing this owner never
shuts down an application-owned executor or another provider user.

## Evidence and remaining assembly

Core-only tests inject host ready/timer/entropy/worker providers. They cover
automatic elections, rejection ownership, written/durable ordering, duplicate
and stale generations, held-group isolation, application/read boundaries,
control reserves, oversized callback capacity, failure discard and close/drain.

A three-node/100-group history combines this production effect owner with real
native WAL workers, outbound queues and, with the default TLS feature, actual
framed TCP/TLS connections. It preserves operation retries, fresh reads,
leader replacement, healing and actual-file restart with new store sessions.
That history uses injected host scheduler/timer providers and explicit campaigns;
automatic election behavior is tested separately. The native scheduler/timer
providers retain their earlier conformance histories. Native-without-TLS uses
bounded simulated delivery. Network isolation is injected after decode.

A full node facade, bounded peer roster/reconnect handling, asynchronous
checkpoint creation/compaction, administrative/application result admission and
broader automatic-timer network histories remain. No complete deployable
consensus release or macOS execution is claimed. The full P0–P7 goal remains active.
