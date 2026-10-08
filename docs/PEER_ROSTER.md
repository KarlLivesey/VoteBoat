# Bounded peer connection coordination

`transport::PeerRoster<P: PeerTransport>` owns a finite, construction-authorized
peer map and coordinates its selected connection providers. It creates no
listener, socket, TLS instance, queue, thread or executor. The native path uses
`NativePeerTransport` over native TLS and wire providers; downstream hosts can
select their own `PeerTransport`, including boxed instances. This fixed
identity/lifetime guard does not replace quorum validation or grant membership.

## Construction and identities

`PeerRosterConfig` binds the recovered local node/store session, exact outbound
queue binding, negotiated wire version, transport ceilings and roster limits.
The authorization map binds each allowed remote node to a store identity and
incarnation. Discovery hints cannot add or change these identities. Local peers,
incompatible limits and empty generation ranges are rejected before progress.
An empty authorized map is valid for an instance with no current remote peers.

The host reserves a **disjoint inclusive range** of SecureSessionGeneration
values within this local store session. The roster issues checked increasing
generations only inside that range. Recreating a roster in the same session must
use unused generations: after complete shutdown, `reclaim_generation` returns
the next unused value within its range. Concurrent rosters require separate
host-reserved ranges. Merely constructing another roster with the original range
is not permitted. Restart persists a fresh WAL session before service starts,
which invalidates the previous local connection domain. No new durable generation
or remote progress watermark is introduced.

`due_connections(now, limit)` examines at most `limit` peers with a rotating
cursor. It reserves connection capacity before returning a ConnectTicket. The
host creates/dials and authenticates the connection under separate socket/TLS
budgets, then constructs its selected transport. `attach` accepts it only for
the exact pending, unexpired ticket, authenticated security attestation, local
identity, authorized peer store, generation and wire version. The transport must
be Open, have no accepted send/completion or decoded input, and fit the selected
ceilings. Bounded preallocated frame buffers are allowed. Rejection
returns the original provider without polling it or consuming the pending
attempt. The host can correct the assembly, or call `connect_failed`.

An authenticated remote store session may stay equal across reconnection or
increase after remote recovery; it cannot regress below this roster's observed
session. That floor is volatile channel history, not a durable replica prefix.
Raft still checks its own term, configuration, group and request contexts.

## Bounds and progress

Defaults authorize at most 1,024 peers, allow 16 simultaneous connection attempts
and reserve at most 256 MiB across connecting/live/retiring transport slots.
Each reserved slot charges the sum of its selected send-frame, receive-frame and
decoded-image ceilings. This is conservative capacity reservation; outbound
messages, sockets, TLS/handshake memory, host staging and ingress are separate.
Providers are checked against their declared ceilings before and after polling.
Post-validation cannot undo an allocation inside a broken host provider.

Connect timeout defaults to 10 seconds. Failed/expired connections retry after
100 milliseconds, doubling up to 10 seconds. A valid local Sent completion
resets the failure count; it supplies no remote acknowledgement. Deadlines use
caller-supplied local monotonic milliseconds. Time reversal rejects progress.
Arithmetic cannot wrap a deadline or generation; sequence exhaustion returns
every already-issued ticket and rejects subsequent issuance.

`poll(now, visits, budget)` fairly visits at most the requested number of peers.
Each actual connection receives the existing independent session/plaintext
budget. It returns bounded progress, expiry and disconnect events; one failed
peer does not prevent another visit. `next_deadline` supplies the earliest
reconnect/attempt-expiry scheduling hint, excluding waiting work when connection
capacity is full. The host combines it with reactor readiness and runtime timers.
There is no hidden busy poll or timing thread.

Expiry invalidates the logical ticket and returns ConnectExpired. The host must
cancel/close its obsolete dial or handshake before replacing those host-owned
resources. A late authenticated result is returned as stale. Host-held stale
or rejected providers remain under host budgets, outside the roster reservation.

## Send and receive ownership

Only already-dispatched OutboundBatch values enter `submit`. Disconnected,
busy or incompatible admissions return the exact original batch. The host keeps
such batches under a bounded staging budget. Each peer accepts one send through
its terminal completion, correlated by the exact outbound ticket and connection.
**Outbound sequence values are not dispatch order:** reserved control scheduling
may send a newer ticket before older bulk work. The roster does not impose a
sequence watermark on those admissions.

`take_send(peer)` returns the original TransportSend once. The host passes its
batch/result to the original OutboundQueue's `complete`; only that operation
releases the queue's credits. Neither dispatch, local Sent, reconnect nor roster
shutdown provides a Raft acknowledgement, durable voter evidence or client
result. The selected provider remains responsible for preserving the original
batch through partial writes and uncertain delivery.

`take_received(peer)` checks the exact current connection, authenticated sender
node/store session, destination and capacity-costed batch before transferring it.
The host reserves ingress first and retains ownership if group admission rejects.
These credits are independent of the transport reservation. A retired connection
discards its decoded receive and partial buffers before replacement; uncertain
delivery is handled by Raft retry, never by fabricated acknowledgements.

On failure or explicit `disconnect`, abort releases channel/frame buffers, while
the roster retains the old transport and its full slot reservation until the
accepted send's exact terminal event is taken. A replacement cannot overlap that
unresolved send. Other peers remain usable. A malformed send/receive completion
fences this roster and returns the offending owned payload. Further submission
and connection issuance stop. The host resolves its queue ownership, aborts/drains
other work and can explicitly `discard_failed_send` for an unresolved correlation.
That discard releases no queue credits and never permits this roster to resume.

## Shutdown and evidence

`close` stops admission and invalidates pending connection attempts. The host
cancels those external attempts, then keeps polling and taking accepted send
completions. Providers drain their accepted output before channel close.
`abort` stops the owned channels immediately, with accepted sends reported as
uncertain Failed. `is_drained` requires explicit close/abort and no retained
connection or attempt. Completed input can be discarded on retirement. These
operations do not shut down shared host infrastructure. Dropping observation
does not undo already-sent bytes or release a separate outbound queue's credits.

`tests/peers.rs` injects a trusted-host transport and independent outbound queue
through public interfaces. It covers authorization/security rejection, declared
limits, fair visits, aggregate reservation, timeout/backoff, stale attempts,
session rollback, exact failed-send ownership, invalid completions, oversized or
mis-scoped input, priority reordering, shutdown, generation handoff and exhaustion.
Its security attestation is a test assumption, not cryptographic evidence.

The native three-node/100-group effect-owner histories now select this roster
over actual TCP/TLS connections and WAL workers. The snapshot/checkpoint history
replaces a quiescent peer pair with fresh generations after the retry deadline,
then continues timer heartbeats, replicated writes, reads, repeated checkpoints
and actual-file recovery. This is bounded reconnect evidence, not a complete
kernel-network fault matrix or proof. Native-only histories use bounded simulated
delivery and test roster coordination separately. macOS execution remains
unobserved. Listener/dial execution, complete ingress/result admission and the
full node facade remain in progress.

The native histories also explicitly close every roster, poll TLS shutdown and
check that all connection reservations are released before dropping the nodes.
