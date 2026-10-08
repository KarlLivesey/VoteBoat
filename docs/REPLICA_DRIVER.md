# Local replica assembly

`runtime::ReplicaDriver<R>` coordinates bounded local work over borrowed
`ReplicaParts`. The embedding host explicitly selects one EffectOwner and
PersistenceWorker, one OutboundQueue, a fixed group/application map,
ApplicationRouter, ClientRouter, ReadRequests and optional snapshot router/worker.
There is no provider fallback or hidden executor, thread, socket or clock.

Construct on quiescent components. Applications must already be replayed or
restored through each core's recovered commit index. Missing, extra or lagging
applications reject construction. Runtime, store, worker and router generations
must match, and the outbound local node must match every core. These are existing
scoped identities, not new durable watermarks. Restart requires fresh runtime
lifetimes and the existing authoritative recovery path.

Call `poll(parts, now, budget)` from the serialized owner. Polling first delivers
snapshot and WAL events, then advances tracked proposal/read inputs, dispatches
effects and retries rejected work, submits control/data WAL batches separately,
and reconciles client/read cancellation. `ReplicaProgress.steps` exposes input
outcomes for diagnostics; no successful step implies commitment. Only exact
Durable completion releases persistence dependencies. Application/read outputs
use the existing checked, opaque completion paths.

`ReplicaDriverLimits` bounds total retained leases and preallocated metadata.
Payloads keep their EffectOwner byte reservation while rejected. Each poll has
finite event/step/effect/retry/reconciliation budgets. WAL batches obey both the
driver and provider unit ceilings and use the same exact capacity-byte cost as
worker admission. Control and data batches are separate so data cannot spend
the worker's control reserve. Outbound credits move to the selected queue on
accepted submission; local send completion remains an outer transport concern.
Returned step vectors and provider completion vectors are separately bounded by
the poll budgets; their transient memory is not the retained-metadata gauge.

The driver exclusively dispatches local effects and application results for its
selected components. Hosts may admit client/read/control requests through those
owners and poll/complete client/read outputs. Keep selected identities and the
application group set fixed. Drive connector/roster/transport/IngressRouter in
the outer loop; this driver neither opens connections nor consumes incoming
frames. [PeerDriver](PEER_DRIVER.md) now supplies this outer coordination over
selected connector, factory, roster and ingress instances. Snapshot effects require selected snapshot components; omission returns
MissingSnapshots, retains the lease and fences the owner for recovery.

Wrong bindings, backwards time and invalid budgets reject before provider
polling and allow correction. Fatal provider/owner failures fence further local
driving; no rejected effect is silently retried with another provider.
`discard_failed` explicitly releases local retained effects, exceptional held
application output and snapshot-router leases, and reconciles pending requests.
Accepted work owned by external providers must still be drained/recovered by the
host. A previous successful client/read output remains valid across later failure.

`close_intake` stops new client/read requests while accepted/control work can
continue. It does not close externally owned workers or transports.
`is_drained` reports only local retained leases/results; whole-node shutdown must
also inspect all owner, service, worker, snapshot and transport queues, complete
consumer-held outputs, and join explicitly owned native workers.

Independent host providers exercise this composition in
`tests/support/replica_driver.rs`. Real-file three-node/100-group native histories
in `tests/effect_owner.rs` use the same driver, with network faults and delivery
controlled by the outer harness. These finite histories do not claim a complete
node facade, exhaustive proof, macOS execution or benchmark results.
