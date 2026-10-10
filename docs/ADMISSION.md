# Constrained outbound admission, contract version 1

`AdmissionPolicy::reserve` receives fixed Copy metadata: exact outbound owner
binding, peer, most-restrictive traffic class, batch cost and current node/peer
usage. Usage includes queued and dispatched work but excludes the prospective
batch. A success returns an opaque owned `AdmissionLease`; refusal must return
all provider credits and accept no messages. The provider sees no borrowed payload
and grants no membership, quorum, ownership, durability or delivery authority.

`NativeOutbound<P=NativeAdmissionPolicy>::with_policy` selects the public policy.
The original new constructor selects an independent native finite budget matching
the node ceiling. Public Node parts can use this queue like any OutboundQueue.
Native startup/service defaults exercise the same reserve path. There is no hidden
process-wide budget, executor, clock, network operation or provider loading.

Mandatory batch binding/class/capacity, node/peer ceilings, control reserve,
background ceiling, peer count and sequence exhaustion are checked first. Policy
can further restrict data/background; it cannot waive any hard check. Control
bypasses optional policy, so a closed/refusing policy cannot consume or veto the
queue's reserved control capacity. Mixed batches retain their most restrictive
class. Policy refusal becomes the existing OutboundError::Overloaded and returns
the exact original message Vec without consuming a ticket. Existing owners stage
and retry rather than interpreting this as a fatal consensus failure.

# Ownership, completion and sharing

The lease shares one reservation through an Arc-owned opaque Send+Sync provider
token. Clone extends that reservation's lifetime without adding credits. Queue
bookkeeping and OutboundBatch each retain an owner. Polling, short transport I/O,
channel flush, transport rejection and rejected queue completion preserve the batch's
lease. Valid queue completion releases the retained owner and consumes the batch;
the provider token drops only when the final lease owner is released.

If a caller abandons a dispatched batch, the live queue continues holding its
reservation. If the queue is destroyed first, a downstream batch keeps its lease
through actual buffer/message ownership. Closing a queue rejects new work and
drains accepted work without closing the policy, another queue or another host
view. Extra caller-held lease clones intentionally extend the reservation. Never
strip admission from an accepted batch or replace it during downstream forwarding.

Outbound Rust contract version 2 adds `OutboundBatch::admission`. A host queue
without additional policy credits sets None. A queue with such credits preserves
them through batches and rejected completions. This is a source contract change;
wire and durable formats are unchanged. Queue ticket identities/generations and
Raft acknowledgements retain their original meanings.

`NativeAdmissionPolicy` shares finite bulk batch/message/capacity-byte ceilings
across cloned views. Reserve uses bounded atomic attempts and rolls back earlier
dimensions if a later reservation fails. Contention may return Overloaded without
waiting. Closing one view permanently rejects its new reservations; other views
and existing leases remain valid. Usage fields are bounded diagnostic samples,
not a coherent concurrent snapshot. The native token returns credits atomically
on final Drop. Credits start empty after restart: persisted Raft/application
recovery and operation-ID retries remain authoritative.

One fixed opaque reservation allocation is retained per accepted bulk batch;
Arc/counter/map/queue metadata is bounded by batch ceilings, separately from the
message-capacity byte accounting. No allocation/throughput/zero-overhead performance
claim is made. Arbitrary host policy/token code must honor bounded nonblocking
reserve/Drop and rollback obligations; an interface cannot sandbox a callback.

# Evidence and limits

The shared provider checks in `tests/provider_conformance/admission.rs` run32
seeded256-action ownership histories against each host/native policy. An
independent model accounts unique reservations across lease clones, dropped
views, closes and capacity refusal. Separate checks transfer the final owner
between threads and count one opaque-token destructor. An intentionally broken
early-release provider verifies that the model detects lost reservation
ownership. These are finite histories; they do not certify arbitrary concurrent
callbacks or general client/disk admission.

`tests/admission.rs` implements an independent downstream fixed-sample policy and
injects it into native queues. It checks exact rejection allocation, metadata
scope, permissive/refusing policy versus mandatory limits/control, invalid/delayed
completion, close/drop in both ownership orders, shared native budgets, rollback
at each dimension and concurrent native views. `tests/transport.rs` retains a
policy lease through short I/O and delayed channel flush, then destroys the queue
while the terminal batch still owns credits. Existing outbound conformance and
real native shared-runtime/TCP/QUIC histories exercise the default provider.

This slice accounts outbound bulk messages. Client/read/router, disk/storage,
encoded frames and decoded ingress remain separately bounded mechanisms. C14 frame
leases release at local flush/decode while admission remains until the original
batch is released. Shared-pool connection admission/fairness/control storage,
general host client/disk policy and tenant/service authorization remain separate
work. A control admission reserve does not promise quorum availability, preemption
of a frame already on a stream, new peer capacity or unrestricted snapshot repair.
No live quorum-weight change is a policy operation.
