# Bounded decoded ingress

`runtime::IngressRouter` owns the transfer from authenticated decoded peer frames
into an existing serialized `EffectOwner`. It is a fixed identity/ownership guard,
composed over the same public `PeerTransport`, scheduler and timer contracts used
by native and host providers. It does not create a second Raft owner, execute the
core, run application code, perform I/O or read a clock. The native TLS cluster
histories now select this router instead of staging decoded messages in a shared
fixture deque. Committed application results now use the separate
[application-result router](APPLICATION_RESULTS.md), followed by the
[client router](CLIENTS.md). Read-result admission and the complete node
facade/reactor remain unfinished.

## Inspect before ownership transfer

Peer transport contract **2** adds `received_info()`. It returns immutable exact
connection, traffic class, message count and retained capacity bytes for the one
decoded frame still owned by the provider. Inspection transfers nothing, and
polling cannot replace that receive before it is taken or explicitly discarded.
`ReceivedBatch::info(limit)` supplies bounded scope/class/capacity validation for
native and host implementations. This is a Rust contract change; wire frames,
TLS ALPN, routing prefaces, WAL and snapshot formats retain their existing IDs.

`PeerRoster::received_info(peer)` checks current authentication/binding, declared
limits, count and reported retention before publishing this metadata. Its
`take_received` then checks the actual owned frame against the same metadata.
An incorrect class, count, scope or byte total fences the roster and returns the
original extracted batch. Preallocated provider buffers are allowed; they must
not underreport the actual decoded receive's retained bytes. Host mechanisms
remain trusted code, not a sandbox against malicious in-process allocation.

`IngressRouter::receive(roster, peer)` checks its local node/store scope and
reserves batch/count/byte/class credits **before** calling `take_received`.
Overload or an oversized frame leaves ownership in the transport and returns no
payload. The transport then stops decoding further frames on that connection.
Once accepted, the router returns a scoped `IngressTicket` and retains the
original vector and every nested payload. Capacity accounting includes vector
spare capacity, command/entry/snapshot/policy retention and pending metadata,
rather than just serialized lengths. Extracted provider violations return the
original batch for explicit bounded failure handling and stop that intake path.
The caller must dispose of it or retain it under a separate failure-output budget.

Default ceilings are 64 frames, 8192 messages and 64 MiB. Non-control traffic
cannot consume the last 8 frame slots, 512 messages or 8 MiB. Background/snapshot
traffic additionally has an 8-frame, 1024-message and 16 MiB ceiling. Per-frame
ceilings are 128 messages and 4 MiB decoded retention. Limits are checked at
construction, including space for one maximum frame plus its metadata within
control and background reservations. Hard caps are 4096 frames, 1048576 messages
and 1 GiB, with at most 4096 messages in one frame. Choose compatible codec and
transport limits; a frame larger than the selected ingress per-frame ceiling
requires rejecting/reconfiguring that connection, not repeated overload retries.

Frames with mixed RPC classes are charged to their highest-cost class (snapshot,
then data, then control), matching native outbound batching. Prefer homogeneous
frames when providing an alternative transport. Reservations permit control from
another available peer when bulk is saturated. They do not bypass a blocked
bulk frame at the head of the same TCP connection; existing sender priorities
and bounded frames still matter. No new denial-of-service or liveness guarantee
for arbitrary network/host scheduling is claimed.

## Dispatch and credits

`dispatch(roster, effect_owner, visits)` first checks the exact runtime owner and
local identity. Each visit handles at most one message or one stale frame.
Visits rotate across retained frames; a scan cursor rotates past blocked messages
inside each frame. A full group leaves its message held while another group can
progress. Only `RuntimeError::Overloaded` is retried. Other runtime admission
rejections explicitly discard that RPC and report its group/reason in the returned
progress. Incoming peer traffic cannot create an unknown group or incarnation.

Before each transfer, the router compares the frame's exact connection binding
with the roster's live binding. Retirement or replacement discards still-held old
input and produces a `StaleConnection` completion. Events already accepted by the
runtime remain runtime-owned: losing their old connection cannot roll them back.
A fenced roster prevents further dispatch until the host explicitly handles its
failed domains. The Raft core continues validating identities, terms, contexts,
configuration and ordered log prefixes when it executes an admitted event.

Accepted frames retain their **full original** frame/count/byte charge until all
messages have been admitted/rejected or the remaining frame is discarded. Partial
admission never prematurely releases a still-retained vector or payload. This
conservatively overlaps the runtime's own admission budget for already transferred
messages; those are distinct ownership boundaries. Completing a frame drops its
remaining buffers and transfers one exact terminal `IngressCompletion`, including
admitted/rejected/discarded counts. Their sum is the original frame count.
`Drained` only means no messages remain in this ingress frame. It is not execution,
commitment, application success, remote receipt or durable quorum evidence. No
new durability token exists here; all Raft effects retain their existing exact
Written/Durable and application/snapshot dependencies.

The router may reorder messages when scanning/removing them. Network delivery
order is not a Raft safety assumption; the existing core rejects gaps and stale
terms and the sender retries. It does not reorder application log application.
Callers budget progress/completion metadata too: one dispatch accepts at most
65536 visits, and output vectors have at most that many corresponding records.
No output RPC payload queue is hidden in the router. Scheduler and core admission
still own their own limits. Kernel/TLS/frame buffers, outbound work, application
results and caller-retained metadata require separate budgets. The router's fixed
queue storage is bounded by its frame ceiling; costs are conservative retention
accounting, not an exact allocator/RSS promise.

## Lifetime and recovery

An `IngressBinding` contains the recovered local identity, exact `RuntimeOwner`
and a fresh host-reserved `IngressGeneration`. Multiple/replacement routers in
one runtime lifetime need different ingress generations; replacing the runtime
needs a fresh runtime generation, and restarting storage needs a fresh persisted
store session. The checked ticket sequence never wraps and is an allocation
sequence, not a durable maximum or contiguous replicated prefix. Ingress persists
nothing; sender Raft retries recover dropped volatile RPCs after restart.

Close stops new receive transfers and continues dispatching accepted frames.
Abort closes intake, drops held RPCs and returns one cancellation completion per
accepted frame, preserving counts for messages already admitted. Drop abandons
these observations and drops only still-owned frames. None of these operations
closes a peer transport, application, worker or host scheduler. Shared owners and
rosters remain usable by other routers with independent binding generations.

## Evidence boundary

The core/host-only ingress tests in `tests/effect_owner.rs` use independent host
peer transports, schedulers/timers and the public effect owner. They cover full
charge retention across a blocked group and partial progress, successful retry,
control and background reservations, stale connection replacement, false receive
metadata returning its original batch, spare-capacity rejection before extraction,
wrong runtime generation, unknown groups, explicit abort, and independent router
shutdown over shared owners. Native three-node/100-group TLS histories exercise
the same receive/dispatch path through real connections, WAL and snapshot workers,
partitions, reconnection, application retries/reads and actual-file recovery.
These are finite Linux checks, not macOS execution, a full proof or performance
measurements. Physical WAL cleanup, reconfiguration, recursive responsibilities
and safe split/merge remain unfinished alongside node result/reactor assembly.
