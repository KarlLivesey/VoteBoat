# Owned transport buffers, contract version 3

`buffer::BufferPool` provisions owned `FrameBuffer` leases. A request names its
maximum reserved bytes and initial visible length. Rejection transfers nothing;
success holds the maximum reservation until Drop, independently of visible
length. Growing preserves the prefix and zeroes new bytes. Shrinking does not
return reserved credits or necessarily free capacity. The immutable and mutable
slices expose the same length/storage. Capacity must stay within the reservation.
This is a Rust contract; wire and persistent formats are unchanged.

`NativeBufferPool` uses shared atomic byte/lease counters and owned Vec storage.
Cloning shares counters. Closing a view permanently refuses its new acquisitions;
other views and accepted leases remain valid. Allocation is lazy: reserving a
1 MiB frame with a 24-byte header initially allocates 24 bytes. Leases free their
storage before returning credits. There is no cache, thread, singleton or
process allocator replacement. Reservation contention has a finite retry bound
and may refuse with Overloaded. Diagnostic usage fields are individually sampled,
not a coherent concurrent snapshot. The byte budget counts Vec capacity; allocator
metadata, Arc counters, session buffers and decoded messages have separate bounds.

Errors distinguish invalid limits, overload, closed view, excessive size,
allocation failure and provider violation. An in-budget resize may fail allocation;
it leaves the existing bytes intact. The provider cannot revoke accepted leases.
These volatile credits are reconstructed empty on process startup and convey no
storage durability or consensus evidence.

Version2 adds `BufferClass`, `acquire_class` and an optional `control_reserve`.
Ordinary `acquire` is bulk. A selected reserve protects both bytes and lease slots
from bulk; control can use any free total capacity but cannot exceed total limits.
Unclassified input is bulk. Existing implementations default to no reserve and
class acquisition forwards to their existing acquire method. Declaring a reserve
without implementing class acquisition fails with ProviderViolation; it does not
silently weaken that declaration. Providers must keep limits/reserve stable across
shared views and acquisitions.

`NativeBufferPool::new_with_control_reserve(total, reserve)` explicitly selects
protected capacity. Both dimensions must leave positive bulk capacity. Total and
restricted-bulk counters reserve independently with precise rollback on partial
admission/allocation failure. `bulk_usage` reports restricted reservations, zero
when no reserve is selected; ordinary usage includes both classes. The unselected
native path uses only total counter operations. Counters are volatile, not a
retention registry or durability receipt.

# Native integration and ownership

`NativePeerTransport<S,C,P>` accepts a host pool through `with_buffers`.
`NativeTransportFactory::with_buffers` produces a `NativeSharedTransportFactory`
that clones explicitly shared pool views into authenticated connections. The
original constructors remain available and select a native two-lease,
`send_frame_bytes + receive_frame_bytes` budget independently per connection.
Both paths use the public contract. Construction requires capacity for at least
one full-duplex connection, creates no lease and takes no pool-wide shutdown
ownership. A host sharing a pool across N active connections should budget both
frame reservations for each connection, or explicitly manage admission/pressure.
If a reserve is declared, it must fit one maximum send frame/lease and leave a
full-duplex bulk connection (send plus receive and two leases). Malformed or
insufficient declarations refuse before any frame acquisition or plaintext I/O.

Send checks outbound ownership, acquires its maximum frame reservation, sizes and
encodes into the lease, then accepts the original OutboundBatch. Any refusal returns
the original batch and releases the temporary lease. A short write or pending
channel flush retains the lease. Local flush completion releases encoded storage;
the original batch still holds its independent queue credits until completion is
consumed and returned to the queue. Pool exhaustion becomes the existing
TransportError::Overloaded, so the owning peer driver stages and retries the batch.
The class comes from validated actual message contents through batch_cost.
Control-only protocol traffic uses Control; mixed data or snapshot work uses Bulk.

Receive acquires its maximum Bulk reservation before reading, but allocates only the
codec header. A valid advertised length permits growth within that reservation.
Invalid headers never allocate full-frame storage. Exhaustion reads no plaintext
and stops that direction for the current poll, allowing later retry. Completed
decode releases encoded storage before the decoded batch is consumed; decoded
retention remains separately bounded. Idle partial receives retain their maximum
reservations. Resize failure is terminal, avoiding a retry at an already-consumed
header. Failure/abort drops the session first, releases partial frame/send leases
and preserves the existing failed-send/original-batch semantics. Graceful close
releases partial receive after draining the accepted send. Dropping a transport
releases its leases but abandons completion observation; it cannot return credits
owned by an unrelated outbound queue.

The driver checks returned reservation, capacity and both slice lengths before
use. Host implementations still must obey allocation and stable-storage contracts;
post-validation cannot sandbox arbitrary provider code or undo its allocations.
The unselected pool supplies no traffic reserve. The selected reserve protects
control sends from shared bulk saturation, not receive/connection admission or
per-owner fairness. Control competition and bounded atomic contention can still
refuse; no deadline is promised. It does not preempt an accepted send on the same
ordered connection.
An undersized shared pool can stall connections holding idle receive reservations.
Hosts must provision/admit connections accordingly; constrained policy integration
is the next slice. Existing per-connection defaults preserve previous full-duplex
frame capacity.

Version3 adds `BufferOwner`, `BufferOwnerLimits`, `owner_limits` and `bind_owner`.
`NativeBufferPool::new_with_owner_limits(total, reserve, owners)` explicitly
selects at most1024 owner registrations with fixed per-owner Bulk ceilings.
Checked quota sums must fit the global bulk remainder; overbooking is rejected.
Transport requires each quota to fit full-duplex maximum frames and two leases.
Native transport binds its view to the authenticated local/peer node and store
identities before accepting frames. Connection and store-session generations do
not change this accounting identity: reconnecting to the same store incarnation
shares old held credits. Different store incarnations use different slots.
These identities grant no authorization or voter permission.

Unbound selected views refuse all frame acquisitions with InvalidOwner. Bound
views cannot change owner; clones retain that owner. Factory roots should remain
unbound so each accepted connection can select its actual authenticated peer.
Closing one view leaves other views/frames valid. Every accepted frame, including
Control, retains its owner registration until Drop. Bulk frames alone charge the
owner byte/lease counters. Cold binding uses try_lock over a bounded weak registry;
contention returns Overloaded and poisoning returns ProviderViolation. Expired
registrations are pruned on binding. Frame acquisition/drop do not lock this map.
Owner, restricted-bulk and total acquisition roll back partial failure precisely.
Allocation failure releases all accepted credits. The default unselected path
creates no registry, and compatibility binding defaults refuse unsupported owner
declarations. Volatile quotas restart empty and carry no durability evidence.

This provides bulk headroom against another configured peer's bulk pressure.
Control can still compete for total capacity, and simultaneous connections for
one owner compete for its quota. It promises no receive deadline, per-group
fairness, connection preemption or automatic service selection. Slice144 public
native/downstream tests cover reconnection with held leases, retained registration,
other-peer data progress, control reserve, delayed flush, abort, wrong-owner
constructor refusal, allocation rollback and concurrent same-owner holders.
Those selected transport sessions are host attestations; real encrypted Node
pressure is the next deliverable, not evidence supplied by these fixtures.

# Codec provisioning and evidence

`WireCodec::encoded_length` and `encode_into` allow direct bounded provisioning.
The native codec uses a nonallocating validation/count pass, then writes into the
caller slice and produces identical wire bytes. Exact destination length is
required; an error may modify the destination but no such frame is sent.
Existing host codecs can use compatibility defaults backed by their bounded
`encode_batch` Vec. Those defaults allocate temporary encoding storage in addition
to the pool budget; they do not promise pooled-only allocation. Override both
methods to avoid that scratch allocation. Native transport uses the direct path.

`tests/buffer.rs` exercises independent downstream/native shared-view lifetimes,
maximum reservations, prefix preservation, overload, close and concurrent owners.
`tests/transport.rs` injects a downstream pool into native factory/connections and
checks delayed flush, short I/O, exhaustion/retry before reads, original queue
credits, encoding rejection, receive growth failure, abort/drop and malformed
headers. `tests/wire.rs` checks direct encoding against canonical frames and rejects
short/long destination slices. Real TCP/TLS and QUIC histories exercise default
native provisioning. This does not integrate WAL/snapshot/application buffers,
provide a universal memory budget, or establish performance gains.

Outbound bulk policy leases cover original message ownership independently of
these frame buffers; see [constrained admission](ADMISSION.md). Optional selected
encoded control capacity does not close shared receive/connection/fairness gaps.

```rust
use voteboat::{buffer::BufferLimits, native::buffer::NativeBufferPool,
    transport::TransportLimits};
let frames = TransportLimits::default();
let pool = NativeBufferPool::new_with_control_reserve(
    BufferLimits { reserved_bytes: 2 * frames.send_frame_bytes + frames.receive_frame_bytes,
        leases: 3 },
    BufferLimits { reserved_bytes: frames.send_frame_bytes, leases: 1 },
).expect("valid frame budget");
// Pass pool to NativeTransportFactory::with_buffers, sharing its views explicitly.
```

Reserve conformance includes independent downstream/native byte/slot saturation,
allocation-overflow rollback, concurrent bulk holders, close and exact credit
return. Native transport with host-attested sessions verifies control progress,
mixed-batch rejection, delayed flush/abort and unclassified input refusal. These
host sessions are not cryptographic evidence. Default real TCP/TLS and QUIC paths
also have regression checks; no real encrypted shared-reserve load experiment or
performance gain is claimed.
