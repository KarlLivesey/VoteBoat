# Original read invocation ownership

`runtime::ReadRequests<Q, R>` owns a bounded original query from admission until
its exact terminal consumer completion. It composes an explicitly selected,
empty, open `ReadRouter<R>` with the public `BoundedReadableStateMachine` and
`EffectOwner`. There is no hidden thread, clock, store, application or executor.
Native Counter histories and downstream host applications use the same path.
The complete production reactor/facade and responsibility authorization remain
unfinished.

## Construction and admission

Select one `ReadInvocationBinding` with the exact RuntimeOwner and a fresh
host-reserved `ReadInvocationGeneration`. Supply the selected ReadRouter by
value. Construction rejects a different owner, held results, a closed router or
incompatible per-query/per-result capacities; rejection returns that same
execution router, including its outstanding resources. Its byte ceiling must
fit one maximum read execution. The service's retained request/reply budget and
the execution router's transient budget are separate conservative reservations.
No second application or backend is constructed.

Defaults permit 4,096 retained requests/results, 64 per group, 32 MiB retained
bytes, 64 KiB nested query capacity and 1 MiB per result. Hard ceilings are
65,536 retained requests, 1 GiB bytes, 1 MiB nested query capacity and 16 MiB per
result. Pending records include inline query/outcome and identity/cancellation
metadata. They additionally reserve nested query allocation capacity and the
full declared result bound. Count ceilings bound maps/ready slots; byte charges
are payload/record bounds, not allocator or process RSS guarantees. The selected
execution router reserves its own output space before the callback.

`submit(owner, application, group, query)` checks owner health, group/leadership,
applied boundary, request/per-group/count/byte limits and the application query
and output contract. Rejection returns the original query allocation without
spending an invocation ticket. Success transfers ownership and admits an exact
tracked runtime Read input. It establishes no quorum, persistence or read result.
One unresolved protocol read per group is admitted. Completed outputs may remain
consumer-held while later reads proceed, within the group's retention ceiling.

Request IDs are allocated above both the live core's `read_request_floor()` and
the service's checked allocation sequence. The floor is a maximum accepted ID,
not a contiguous progress prefix, durable watermark or authority. Queued service
requests cannot collide because only one unresolved read exists per group.
All service Read/CancelRead events for a group use its selected invocation owner.
Lower-level host embeddings can instead own that whole policy; mixing independent
owners or untracked pending reads is unsupported. Defensive stale-step rejection
still reports NotRead rather than manufacturing a result.

## Shared progress and execution

Drive the shared effect owner, including through `ClientRouter::advance` when
client proposals are live. Pass each returned step batch to
`ReadRequests::observe_steps` before dispatching its leases. Exact AdmissionTicket,
owner, group and read identity associate initial execution and cancellation with
the original invocation. Feed those original steps, without editing or dropping
them. This is trusted in-process assembly; a host can already mutate core state
through lower-level callbacks. A step error produces that invocation's NotRead;
a successful step alone never produces a read result.

Pass ReadReady leases to `execute(owner, lease, application, now)` with the
selected serialized group application. The router supplies its retained original
query. It rechecks current query/result bounds against the original reservation;
a grown/invalid bound cancels authority and returns NotRead without executing the
query. The selected ReadRouter then validates exact one-use quorum authority and
application catch-up immediately before immutable execution. Application query
errors become `Read { result: Err(..) }`; an invalid provider result fences the
owner and produces no successful read. No idle-connection or clock lease shortcut
is introduced. The returned barrier is the consumed committed-prefix boundary
for this invocation and cannot authorize another read.

Before callback execution, `ReadExecutionError::Rejected` returns the original
lease and leaves the query inside this invocation owner. In particular,
insufficient catch-up can retry that same lease after apply. A fenced runtime
requires recovery; an untouched query does not mean old authority survived.
`Failed` means execution authority/query may have been consumed and the owner is
fenced. Retained query/reply credits survive both paths until terminal cleanup
and consumer completion.

`poll` transfers an opaque, non-cloneable ReadCompletion with its original ticket.
`complete` accepts only that exact envelope once. Foreign completion returns it
intact. Credits stay reserved through polling and release only after the
invocation's protocol work is terminal and the consumer has completed it. The
receiving host then budgets returned result storage separately. A successfully
completed read remains a valid result if a later leadership/owner failure happens
while its envelope is held. Nothing here establishes delivery or a new durability
token.

## Cancellation, failures and shutdown

`cancel_wait(ticket)` publishes Unavailable(Cancelled) once. It stops waiting;
it does not release the unresolved protocol slot, query or reserved reply bytes.
If ReadReady already exists, `execute` consumes it through the checked
`EffectOwner::cancel_read` path, with no query callback or application catch-up
requirement. This prevents an unapplied ready read from pinning a group forever
after its caller abandons the wait.

Otherwise, bounded fair `reconcile(owner, visits)` queues a tracked CancelRead
only after the original Read step has been observed. Read is data traffic and
cancellation is control traffic: submitting cancellation before that step could
let it overtake the original and leave an orphan read. Control admission under
backpressure retries without dropping ownership. A queued cancellation retains
its reservation until its exact step returns, including when an intervening ready
lease was already canceled. StaleRead then means that original request is already
absent; a newer request has a different ID and is unaffected.

Reconciliation detects role/accepted-term changes and publishes
Unavailable(LeadershipChanged), retaining resources until exact cleanup. Owner
failure makes remaining unexecuted reads terminal with Unavailable(OwnerFailed).
`abort(owner)` explicitly fences the runtime and publishes Unavailable(Aborted)
for unresolved waits, preserving already completed read outcomes. Accepted
external WAL still drains and recovers under its original worker contract; read
cancellation does not roll back unrelated writes. Host-held failed effect leases
must still return through `discard_failed`.

`close` stops new reads while accepted work continues through execute, reconcile,
poll and complete. Drain reads before closing runtime admission, which otherwise
prevents needed cancellation inputs. A quorum-lost read needs an explicit caller
cancellation or later leadership/failure observation; no hidden deadline is
assumed. A host timer can implement a request deadline by calling cancel_wait.
Consumer-held outputs and queued cancellation steps must both drain. Dropping an
envelope cannot silently release credits.

Replacing a live service requires draining it or fencing/recovering its runtime.
Reserve fresh invocation and execution-router generations; persisted store-session
and runtime generations reject old completions after restart. Volatile queries,
allocation sequences and read completions are not replayed as durable writes.
Restarted cores use the new store session for their fresh request contexts. No
quorum, WAL format, snapshot schema or application checkpoint format changes.

## Evidence and remaining scope

Ten downstream host tests exercise original query correlation, retained credits,
foreign completions, runtime rejection ownership, per-group isolation, close,
pre-step and ready/lagging cancellation, queued cancellation against a subsequent
read, result-bound growth, application/provider failures, read-ID floor/exhaustion,
stale step outcomes, explicit selected-owner construction failure and failed-owner
abort preserving a valid completed result.

The real three-node/100-group native WAL/TLS history now admits and consumes all
service reads through these contracts. It verifies 100 reads stay pending with
no values under isolated-leader quorum loss, resolve as unavailable after
replacement, remain canceled after healing, and permit fresh replacement-leader
reads. Snapshot/checkpoint/reconnect/recovery and native-only network histories
also select this path. These are finite Linux histories, not a macOS run, a full
protocol proof or performance evidence. Production reactor assembly, physical
WAL cleaning, online membership/policy changes, responsibility routing and safe
split/merge remain required by the active P0–P7 goal.
