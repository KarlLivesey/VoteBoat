# Client proposals and outcomes

`runtime::ClientRouter<R>` supplies bounded single-group proposal ownership over
the existing effect owner, application-result router and public application
providers. It creates no I/O, executor, clock, application store or consensus
owner. Native 100-group histories now use this path for client writes/retries and
consume their applied/unknown completions. The separate
[read invocation owner](READ_REQUESTS.md) now handles reads through the same
shared steps and effect owner. Responsibility routing/authorization and the
complete production facade/reactor remain unfinished.

## Application admission contract 1

`application::ProposalAdmission` is an optional extension of the existing bounded
application capability. Its deterministic `validate_proposal` sees the candidate
operation/content plus every unapplied durable-log command and retained client
command for the group. It changes no application state, performs no I/O and
returns an upper bound for one receipt's inline and nested retention.

The native Counter rejects malformed commands and counts unique operation IDs
absent from applied dedup state. Queued/in-flight commands reserve remaining
dedup capacity; duplicate IDs share a reservation, including conflicting content
that will deterministically produce OperationConflict on application. Nothing is
evicted. A new operation at capacity is rejected before proposing it; an existing
operation can still retry and obtain its original outcome.

All service proposals for one group must use the selected router/application.
Lower-level direct Propose APIs remain available to embedding hosts that implement
their own complete admission policy. Mixing unaccounted direct proposals or
multiple independent routers for the same live group defeats capacity admission.
Hosts select the correct serialized application and consistent schema/capacity on
all replicas. This is trusted in-process code, not isolation from malicious
providers or false reported state.

## Admission and exact execution identity

Submit an owned `ClientRequest` with group incarnation, operation ID and command.
The router checks its exact RuntimeOwner, current local leader, application/log
boundary, count/byte/per-group budgets and application admission. Rejection
returns the original request and its original vector, without spending a client
ticket. A successful ticket means **volatile admission only**.

The runtime owns an exact-length command copy. The router retains the original
content for pending capacity checks and eventual result correlation, charging both
copies conservatively alongside result/metadata reservations. Runtime ingress has
its own independent charge. All ticket allocation is checked and never wraps.

`admit_tracked` on Shard, TimedShard and EffectOwner supplies an exact volatile
AdmissionTicket. The queue carries it to Stepped/OwnerStep, independent of priority
ordering and the visit that eventually executes it. Proposed position is extracted
from the actual successful Persist effect; it proves no durability or commitment.
Stopping a runtime returns the exact still-unprocessed tracked tickets separately
from active work; a focused stop test checks duplicate-operation input identity.

Drive service steps using `ClientRouter::advance(owner, now, limit, application)`.
The application lookup borrows the host's current selected group state. Immediately
before a Propose enters the core, the runtime repeats syntax/capacity/boundary
validation against current application, log and retained commands, and checks that
the current receipt bound still fits the original reservation. A snapshot,
application catch-up, leadership change or changed output bound cannot make the
old preflight check authoritative. Failure creates no proposal and produces the
exact invocation's NotProposed outcome, including Admission/ReceiptBudget errors.
The internal checked hook only rejects Propose; it cannot replace consensus
processing, manufacture persistence or mutate a quorum policy.

The router matches steps by AdmissionTicket, not merely operation ID. Concurrent
same-operation retries and conflicting commands get distinct client tickets and
actual positions. Host calls to lower-level owner.advance would bypass this
service execution check and lose its tracking; use the client driver while these
requests are live.

## Applied results and resource lifetime

Pass each opaque ApplicationResults envelope to `ClientRouter::deliver` with its
original application-result router and effect owner. Before consuming that output,
the router checks owner bindings and exact group/index/term/operation against the
original verified applied positions and tracked proposal. While the log entry is
retained, it cross-checks exact content too. It validates the
per-request receipt bound too. The application-result router already validated
ordered application and its own batch capacities. Only their combined evidence
produces Applied { position, receipt }. Local send success, Written, a proposed
position or a cached leader never does.

Unmatched follower, recovery or already-abandoned receipts are discarded. The
opaque application envelope retains budgeted verified positions, so later log
compaction cannot erase the original applied correlation. Previously generated Applied
completions remain valid if leadership changes while the consumer holds them.
Provider violations fence the owner and return the original application output
for explicit failed-domain cleanup. Binding/stale-output rejections preserve its
original application-result credits.

`poll` exports one opaque ClientCompletion. Credits remain through polling;
`complete` consumes the original envelope once and returns the outcome to
separately budgeted host storage. Another router/runtime returns it intact as
StaleTicket. Dropped observations do not roll back commands or release credits.
Queue/index metadata is bounded by outstanding request count; retained command
spare capacity and declared receipt capacity stay charged even when actual output
is smaller. This is conservative accounting, not an allocator/RSS measurement.

Defaults allow 4096 requests, 64 per group, 32 MiB, 64 KiB command length and
1 MiB per receipt. Hard ceilings are 65536 requests, 1 GiB total, 1 MiB commands
and 16 MiB receipts. Per-group caps prevent a stalled group consuming every
request slot when other groups use the remaining node budget. Application state,
validation scratch space, WAL, network, read results and external output retention
have separate bounds.

## Unknown outcomes, cancellation and shutdown

`cancel_wait(ticket)` emits Unknown(CancelledWait) once. It does not remove the
queued proposal. Completing that observation keeps the command's reservation
until execution/application or other exact evidence transfers it. A timed-out
queued command cannot disappear from the next capacity check.

Call bounded fair `reconcile(owner, visits)` while driving the node. Leadership
loss/change emits Unknown; failed owner observation also emits Unknown. For a
stepped abandoned request, no pending core dependency plus its durable log entry,
or a newer durable term, permits release of the pending reservation after the
consumer completes its output. Durable unapplied log contents then carry capacity
until application/dedup state takes over. An unstepped command stays reserved.
Neither timeout nor a changed leader alone proves non-commitment.

Close stops submissions while accepted work/completions drain. Abort explicitly
fences the owner, emits Unknown for unresolved waits and releases reservations
only through their consumed completions. Accepted external WAL work still drains;
abort is not storage cancellation. Recovery uses fresh runtime/store identities
and reconstructs capacity from checkpointed dedup state plus unapplied log entries.
Router replacement within a runtime needs a fresh ClientRouterGeneration.
Ticket sequences are allocation IDs, never durable or node-wide prefixes.
Do not replace a live same-group router while its requests remain queued.

## Evidence boundary

Nine core/host tests cover queued same-operation retries/content conflicts,
dedup saturation, cancellation before stepping, exact tracked positions, no reply
at Written, original runtime rejection ownership, per-group isolation, foreign
completion, failed-owner abort, close over shared providers, recovered unapplied
log capacity, host validation, wrong runtime, execution-time rejection and growing
receipt bounds, including a delayed applied reply across actual pinned logical
compaction. A separate tracked-input stop test checks unresolved identity.
Native three-node/100-group WAL/TLS histories check applied replies,
no applied replies to an isolated leader, Unknown on replacement, healing,
checkpoint/snapshot/retry/read behavior and actual-file restart.

These finite Linux checks do not establish a complete proof, macOS execution,
device power-cut behavior or performance. The production reactor/facade,
physical WAL cleanup, reconfiguration, responsibilities and split/merge remain
unfinished. Full P0–P7 remains active and CI remains background feedback.
