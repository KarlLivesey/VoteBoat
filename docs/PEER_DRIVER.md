# Bounded peer reactor

`runtime::PeerDriver<C, F>` owns construction-selected `PeerParts`: a
`PeerConnector`, `PeerTransportFactory`, `PeerRoster`, `IngressRouter`, and a
fixed route map. Dial/accept directions and addresses are hints for exactly the
authorized roster peers. They grant no membership, store identity or service
authority. The driver borrows the existing EffectOwner and OutboundQueue when
polling. It creates no listener, clock, thread, executor or fallback provider.

Construction validates exact local/runtime/outbound/ingress scopes, complete
route coverage, limits and quiescent connector/roster/ingress/outbound work.
Roster attempt timeouts must fit the connector's supplied deadline capability.
Rejection returns all selected parts without closing host resources. Components
remain owned by the driver until drained reclamation or explicit failed recovery;
they cannot be swapped while accepted work exists.

`PeerTransportFactory<S>` consumes one authenticated Ready session and constructs
one transport bound to the selected outbound queue. Failure closes/drops the
owned session; it creates no send completion. `NativeTransportFactory<C>` uses
an explicitly selected Clone wire codec and transport limits with
`NativePeerTransport`. Host factories use the same contract. Authentication,
session scope, negotiated wire version and roster attachment are revalidated;
neither a factory nor a routing address can replace those guards.

## Progress and ownership

Call `poll(owner, outbound, now, budget)` on the serialized owner. It fairly
polls transports and roster expiry, cancels exact expired connector tickets,
collects connector outcomes, validates live authenticated scopes before factory
attachment, and starts bounded eligible attempts. It consumes exact local send
terminals through OutboundQueue completion, reserves ingress before extraction,
rotates rejected outbound batches, and dispatches bounded ingress into the owner.
After receiving frames, the next peer scan starts after the last successful
recipient of shared ingress credit. Later capacity-denied peers do not reset the
cursor to the same first recipient. A zero-visit poll preserves the cursor;
scans with no receive retain their ordinary bounded rotation. This preserves
receive opportunities when dispatch repeatedly frees only one frame's capacity.
Use ReplicaDriver separately to advance owner inputs, deliver WAL/snapshot
completions, apply commands and execute original reads. Poll both fairly; socket
readiness/wakeups and supplied monotonic time remain the host's responsibility.

Accepted connector attempts retain driver slots until their actual terminal
receipt, including canceled/expired sockets or dials. A replacement for that peer
cannot spend a generation or start while its old provider work remains retained.
Late returned sessions close without attaching. Existing roster backoff and
host-reserved generation ranges remain authoritative; restart persists a fresh
store session. These IDs are allocation/lifetime scopes, not durability tokens
or contiguous log prefixes. `next_deadline` combines connector/eligible roster
hints and omits retries blocked by retained canceled work; separately schedule
the core's timer deadlines and provider readiness.

`PeerDriverLimits` bounds staged batch slots and dynamic coordination metadata.
Preallocated staging must cover the selected outbound queue's entire node batch
ceiling. Thus rejected batches from a stalled peer cannot fill a smaller staging
queue and prevent extraction of other peer/control work. Payloads retain the
outbound queue's original node/peer/class count and capacity-byte charges until
exact local completion. The driver does not create a second payload budget or
release credits on submit, timeout or disconnection.

Budgets bound connection scans, connector I/O/receipts, transport visits/I/O,
send extraction/retries and ingress dispatch. Returned diagnostic/completion
vectors have separate per-poll bounds. Retained route values and provider/socket/
TLS resources belong to the explicitly selected configuration/provider budgets;
the metadata gauge does not claim an exact allocator or RSS ceiling.
The explicit drain cancellation vector is bounded by the selected ingress batch
ceiling, independently of the ordinary ingress dispatch visit budget.

Ordinary failed connections back off and can reconnect. Wrong borrowed bindings,
regressed time and invalid budgets reject before polling. Provider violations and
fatal assembly errors fence the serialized owner, close connection admission and
abort transports while preserving observed rejected payloads for recovery. A
provider violating a poll limit may already return an oversized allocation: it
is quarantined whole after fencing, rather than silently discarded or treated as
normally bounded work. `into_recovery` hands off parts, unresolved attempt scopes,
staged/quarantined batches and exceptional returned send/receive payloads. Host
code must resolve original queue credits and provider lifetimes explicitly; no
recovery handoff reports delivery or repairs an untrusted provider automatically.

Connection establishment, ingress admission and local Sent/Failed results never
prove remote receipt, quorum participation, commitment or durability. Only the
existing checked Raft messages and exact storage barriers can establish protocol
progress. No WAL/wire/checkpoint format or quorum rule changes in this assembly.

## Shutdown

Stop service intake through ReplicaDriver before node shutdown. `close` stops
new connections and receive transfer, closes owned connector/roster lifetimes,
and permits accepted transport work to drain. Continued polling resolves unsent
staged or queued batches as Failed, with unknown remote delivery. Healthy owner
polling can still dispatch previously accepted ingress. Externally owned queues,
workers, stores and schedulers remain open.

If the local core has already failed, call `close`, then `drain(outbound, now,
budget)`. This explicit path never accesses the core: it returns network buffers
and cancels held ingress with typed cancellation completions. A failed peer
provider requires `into_recovery` instead; drain cannot make a violated provider
trustworthy. `is_drained` covers this driver's owned components and observed
payloads. Whole-node shutdown must also check the external outbound queue,
local/service owners and workers, and finish consumer-held outputs. Drive at
least once after close for externally queued batches. `into_parts` returns owned
providers only after local drain, enabling explicit native dial-worker join and
generation-range handoff.

## Evidence boundary

Sixteen independent host tests in `tests/support/peer_driver.rs` cover factory
attachment, stalled-peer isolation, exact local completions, ingress admission,
late/canceled sessions and fresh generations, wrong scopes/budgets/time, failed
construction ownership, provider/factory violations, original payload recovery,
oversized-poll quarantine, outstanding-work shutdown and failed-core drain.
The real-file three-node/100-group native TLS histories use PeerDriver and
ReplicaDriver through elections, quorum loss, replacement, healing, snapshot/
checkpoint catch-up, fresh connections and recovery. Partition faults live only
in a test transport wrapper. These finite Linux histories do not prove arbitrary
schedules, macOS execution or performance. Native node assembly, physical WAL
reclamation, membership transitions, responsibilities and selected split/merge
paths now exist. Broader fault and platform acceptance remains open.

The shared downstream tests in `tests/support/peer_receive_fairness.rs` cover
recurring contention for one background-frame slot or one data-message allowance.
Both peers retain their original frames until admission and make equal progress
through eight full scans with interleaved zero-visit polls. Another case holds
recovery input, retains its next frame in the transport, admits a different
peer's control messages and drains an exact outbound completion. These tests
exercise admission/ownership; they do not claim applied snapshot recovery,
commitment or bounded latency under native network load.
