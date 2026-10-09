# Owned transport buffers, contract version 1

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
storage before returning credits. There is no cache, thread, lock, singleton or
process allocator replacement. Reservation contention has a finite retry bound
and may refuse with Overloaded. Diagnostic usage fields are individually sampled,
not a coherent concurrent snapshot. The byte budget counts Vec capacity; allocator
metadata, Arc counters, session buffers and decoded messages have separate bounds.

Errors distinguish invalid limits, overload, closed view, excessive size,
allocation failure and provider violation. An in-budget resize may fail allocation;
it leaves the existing bytes intact. The provider cannot revoke accepted leases.
These volatile credits are reconstructed empty on process startup and convey no
storage durability or consensus evidence.

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

Send checks outbound ownership, acquires its maximum frame reservation, sizes and
encodes into the lease, then accepts the original OutboundBatch. Any refusal returns
the original batch and releases the temporary lease. A short write or pending
channel flush retains the lease. Local flush completion releases encoded storage;
the original batch still holds its independent queue credits until completion is
consumed and returned to the queue. Pool exhaustion becomes the existing
TransportError::Overloaded, so the owning peer driver stages and retries the batch.

Receive acquires its maximum reservation before reading, but allocates only the
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
The pool itself supplies no fairness, traffic-class control reserve or deadline.
An undersized shared pool can stall connections holding idle receive reservations.
Hosts must provision/admit connections accordingly; constrained policy integration
is the next slice. Existing per-connection defaults preserve previous full-duplex
frame capacity.

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

Outbound bulk policy leases now cover original message ownership independently
of these frame buffers; see [constrained admission](ADMISSION.md). Their reserved
control accounting does not close shared encoded-pool connection/fairness gaps.
