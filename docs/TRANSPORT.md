# Peer transport, contract version 4

`transport::PeerTransport` owns one authenticated peer connection and multiplexes
Raft groups in bounded frames. `NativePeerTransport<S, C, P>` uses the same public
`SecureSession`, `WireCodec` and `BufferPool` contracts offered to host replacements.
The pool type defaults to NativeBufferPool; see [owned buffers](BUFFERS.md).
`PeerTransport::security` exposes the authenticated/simulator capability to
assembly; boxed providers implement the same contract. Its
constructor takes the selected outbound queue by shared reference only to copy
its binding and limits; it does not own the queue or create another one. The
host supplies a ready session. An insecure or unready provider, wrong local
node/store, incompatible codec version or incompatible resource limits fails
construction before transport I/O.

## Ownership and local completion

The owner polls `OutboundQueue`, then submits a dispatched `OutboundBatch` to
the connection for that ticket's peer. Submission validates the exact outbound
binding, peer, nonzero sequence and original message/vector capacity against the
queue's limits before encoding. Rejection returns the complete original batch.
A successful submission retains its messages and an independently budgeted
encoded frame. One accepted batch, including an unobserved terminal completion,
occupies the send slot. Many groups may share that frame; this is not a
one-message interface. Further admission returns Overloaded until the owner
consumes the terminal result.

Short plaintext writes retain their exact offset. When all frame bytes have
been accepted and the session reports no pending local ciphertext, the encoded
frame is released and `take_send` returns the original batch tagged with the
connection binding and Sent. The owner passes that batch/result to the original
outbound queue's `complete`. Its credits remain held until then. Local Sent says
nothing about remote receipt, durable prefix, committed writes or read authority.
Only actual Raft responses passing the existing core checks can provide those
facts. There are no transport-generated acknowledgements.

Channel or frame failure first drops the owned session and partial buffers,
preventing further external progress and releasing its channel-owned ciphertext.
An accepted send then returns once as Failed, with unknown remote delivery. An
already completed send remains its original result. Abort follows the same
conservative rule even if no plaintext progress was observed. Retrying a client
operation still uses its original operation ID and application deduplication.
Dropping the transport abandons its completions; it cannot release credits in
an unrelated owner queue or undo bytes already sent.

## Prefix validation, ingress and budgets

Only the codec's fixed bounded prefix is read initially. `frame_length` must
validate it before the driver allocates storage for the full declared frame.
The maximum frame reservation is acquired before consuming the header. The declared
length must fit both the selected codec and transport receive limits. Exactly
one complete frame is decoded against the authenticated incoming `WireScope`.
Decode errors release the partial frame and emit no partial message batch.
The driver also checks the returned batch's scope, message count and conservative
retained capacity cost before publication. Trusted host codecs remain obliged
to enforce their own allocation limits while decoding; post-validation cannot
undo an allocation inside a malicious provider.

One decoded receive remains owned until `take_received`. While it is occupied,
no further frame plaintext is consumed, allowing session/socket backpressure to
propagate. The owner must reserve ingress capacity before taking the batch and
retain rejection ownership if a shard cannot admit its messages. A previously
validated batch remains available after a later failure or abort. Every batch
carries its exact connection binding; the node owner rejects obsolete store
sessions/connections before ingress. The driver itself rejects any in-place
change of authenticated identity/generation.

Contract 2 adds `received_info`: exact immutable connection/class/count/capacity
metadata for that retained receive, without ownership transfer. Native and host
providers can use `ReceivedBatch::info` for bounded accounting. `PeerRoster`
validates inspection and compares the eventual owned batch against it. The new
`IngressRouter` reserves class/count/byte credits before extraction, retains
frames across overloaded groups, and rejects still-held obsolete input. See
[decoded ingress](INGRESS.md). Wire frames and negotiated wire version remain 1.

Default ceilings are 1 MiB encoded output, 1 MiB partial receive and 4 MiB
conservative decoded retention per connection. Original outbound messages are
charged to the selected queue. Decode can temporarily coexist with its encoded
receive frame. `usage` reports retained frame capacities and decoded cost; it is
not exact allocator metadata or RSS. TLS buffers, host streams, peer roster,
rejected sends, decoded ingress and effects have separate budgets. The driver
creates no executor, listener or per-group thread. The original constructor
selects a bounded per-connection native pool; with_buffers selects a host pool.

Poll defaults to 16 plaintext calls and 64 KiB per direction, plus the independent
session I/O budget. Zero budgets are valid. Alternating read/write preference
preserves progress even with a one-call visit. Invalid poll budgets reject
without channel progress or discarding accepted work. WouldBlock stops that
direction for the visit. Bounded frame encoding/decoding is indivisible work
within these finite codec limits; plaintext budgets do not promise CPU
nanoseconds. The host
fairly visits connections and uses the outbound queue's reserved control space
and traffic-class scheduling. An active TCP frame cannot be preempted by another
frame on the same stream. Global concurrency, connection counts and deadlines
for incomplete application frames are host policy, not hidden unbounded work.

## Shutdown and integration evidence

Close rejects new sends, finishes the accepted send, then sends channel close
and drains that close output. Completed send/receive slots remain observable;
partial receive work is discarded on explicit local close. A clean peer EOF at
a frame boundary closes the driver. EOF within a frame fails as Truncated;
unclean channel EOF retains the channel's explicit failure. Abort drops only
this logical channel. Shared listeners, configurations and unrelated connections
remain host-owned.

Conformance uses an injected trusted-host test channel with short writes and
finite buffers, explicitly without a cryptographic claim. Actual TCP/TLS tests
transfer simultaneous 100-group batches and a recursive nine-voter snapshot.
An injected host queue and wire version 37 single-fixture codec demonstrate
composition through public interfaces, not full alternative codec conformance.
The native three-node/100-group history uses this public transport with actual
TLS sockets and WAL workers, including delayed durability, overload, fresh
reads, old-leader isolation, replacement, healing and recovered store sessions.
Its partition/duplicate faults are injected after receive decoding; they are
not kernel-level network fault simulation. The SnapshotRouter history also installs snapshots for 100 compacted groups
through native snapshot/WAL workers and these authenticated connections, then
recovers actual files and continues replicated writes. Native timers generate
heartbeat traffic; campaigns are explicit. See [snapshot routing](SNAPSHOT_ROUTER.md).

A separate 100-group history uses native timers for initial elections,
partition-driven replacement and healing over these sockets, checking retries
and fresh reads. This seeded post-decode partition schedule is bounded evidence;
a complete node facade, listener/dial execution, application result admission and
the broader network fault matrix remain in progress. Bounded effect ownership and asynchronous snapshot routing are
implemented as separate composable runtime components. This connection driver does not by itself constitute
a complete deployable Raft service. macOS execution remains unobserved.

`PeerRoster` now supplies the bounded authorized peer map, connection reservations,
fair visits and reconnect scheduling over this public transport. It validates
fresh connection generations and exact completion/input scopes. The native
effect-owner histories use it, including a quiescent real TLS reconnection before
further replicated work. Socket/TLS establishment and ingress/result admission
remain explicit host assembly. See [the roster contract](PEER_ROSTER.md).

## Configuration envelope admission

PeerTransportFactory::configuration_capacity is a synchronous borrowed check
against the selected codec and transport. Its default returns
UnsupportedConfigurationAdmission; static send/receive behavior is unchanged.
WireCodec supplies the same optional check using ConfigurationWireRequirements
and returns ConfigurationWireCapacity with an exact selected wire version and
append/command/checkpoint footprints. Providers must bound work and temporary
memory without allocating application-sized payloads or accepting asynchronous
requests. No durability, completion ticket, ownership transfer or cleanup follows.

NativeTransportFactory asks its actual selected codec, checks the named version
and transport frame/decoded budgets, and rejects malformed footprints. The peer
roster independently checks its selected version and upper transport budgets,
even before a connection exists. Node invokes this at configuration execution,
after authorization/binding checks and before persistence; host approval cannot
waive it. Host providers must explicitly implement the capability to administer
networked membership. No fallback codec or transport is constructed.

Admission covers one configuration append, one declared maximum command and
one checkpoint with the prospective membership and retained operation IDs. It
is not a promise about arbitrary multi-message batches, future configuration
history growth or application state beyond the declared envelope. Existing queue,
worker and output reservations still apply; service integration must enforce
application command/checkpoint bounds and revalidate every later configuration.
No frame format, store format, generation or durability token changes.
