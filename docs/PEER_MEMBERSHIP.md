# Membership-derived connection assignments

`Node::reconcile_membership(routes, now)` derives the exact required peer/store
union from every group in its serialized effect owner. `PeerDriver` offers the
same operation to embedding hosts. Routes are owned endpoint/direction hints;
they cannot grant credentials, voting rights or membership activation.

`Raft::connection_replicas(limit)` includes the committed membership, every
configuration reachable by rolling back the accepted uncommitted suffix, and
both sides of a pending storage transition. A pending final commit therefore
cannot withdraw predecessor connections before its exact durability completion.
The commit index is a contiguous prefix, not a maximum observed acknowledgement.
This inspection creates no new durability effect or persistent watermark.

Lower-level hosts can construct `PeerAssignments::from_cores` and call
`PeerRoster::reconcile`. They must supply all groups sharing the roster and
serialize reconstruction/reconciliation with core transitions. Assignments are
ephemeral local snapshots, not durable authorization certificates. Reconcile
again after membership or pending storage state changes; the API is explicit,
not an automatic callback from the consensus core.

Before mutation, reconciliation checks the local node/store/session binding,
conflicting stores for a shared node ID, complete routes, metadata and peer
ceilings, monotonic time, and `PeerConnector::supports_peer` for every exact
peer/store. The default trait method returns false; host connectors opt in by
checking their provisioned credentials. Native TCP/TLS and QUIC connectors check
their construction-time pin maps. Rejection returns owned route hints and leaves
the roster unchanged. Hosts can provision a future-peer credential superset
through the lower-level connector seam; this operation does not install keys.

Unchanged peers retain their live bindings. Removing a peer cancels its roster
attempt and removes receive authority immediately, including revalidation of
input already held by the ingress router. The connector's accepted request slot
remains owned until a terminal provider receipt; a late Ready result is dropped.
An accepted send retains its original connection, batch and queue credits until
its exact terminal completion. Staged output for a removed peer completes locally
as Failed. None of these local outcomes is a Raft acknowledgement or proof that
remote delivery did not occur.

Inactive peer records remain within the configured peer ceiling to preserve
remote session floors on same-store re-enrollment. Capacity rejection can thus
require a full drain and replacement roster with the next reserved connection
generation range. Reusing a node ID with a different store also requires that
handoff; rejecting it prevents an identity switch from erasing a prior session
floor. Restart still requires the existing fresh persisted local StoreSession.
There is no unbounded identity history or live credential rotation.

The tests cover actual core rollback and durable final-commit boundaries,
shared-group unions, stale Ready work, retained input, accepted/staged sends,
capacity and identity rejection, and unchanged-roster reconciliation followed by
committed writes and recovery over native TCP/TLS and QUIC formats 2/3.
Prospective resource admission, distributed readiness and faulted activation
remain separate work. Public online configuration ingress remains gated.

## Prospective event inspection

`Node::preflight_peer_event(group, event, &routes, now)` and
`PeerDriver::preflight_event` inspect resources before an incoming event runs.
`PeerAssignments::for_event` and `Raft::event_connection_replicas` provide the
lower-level calculation. An incoming append contributes every intermediate
learner/joint assignment, including peers a later final entry would remove.
A membership snapshot contributes its full voter/learner union. Existing
committed, rollback and pending views, plus other hosted groups, remain included.
Unknown groups, wrong message group/destination, conflicting exact stores,
missing routes/pins and exhausted retained-peer/metadata limits reject.

These operations borrow input and leave clocks, providers, roster and cores
unchanged. They are conservative resource previews: malformed histories can
pass resource inspection and still fail protocol validation. Success retains no
capacity and cannot authorize an event or a configuration. The eventual online
admission path must recheck at serialized execution and retain its reservation
through the accepted/pending state. Independent successful previews do not
reserve capacity for a combination of queued events. The capacity ledger below now provides retained owner-side accounting. Retained
credential/route-plan admission and readiness gates are still unfinished;
configuration ingress stays closed.

Node construction also checks the complete rollback-reachable connection set.
Recovery with an accepted learner removal must still provision that learner when
the removal is uncommitted. A missing route/store assignment rejects construction
before provider polling and returns the original parts.

## Retained owner capacity

`ConnectionBudget` fixes one local identity and a peer ceiling, seeded with every
tracked roster store, including inactive history. `Shard`, `TimedShard` and
`EffectOwner` expose `set_connection_budget`, `connection_budget` and
`reserved_connection_peers`. A networked `Node::from_parts` prepares this budget
from its roster and installs it only after successful assembly. Host-only owners
opt in and must seed all shared roster records and use the actual selected
provider ceiling. Standalone budgets validate capacity and exact stores.
Networked Node construction additionally installs a closed provisioned peer set
from retained, pin-checked routes; the budget itself never creates credentials.

The existing owned event queue is the reservation ledger. Before transferring
configuration append, snapshot or granted witness-reply input, admission unions
its prospective stores with all queued prospective stores, live core views and
retained history. Shared peers count once across groups; different stores under
the same node ID conflict. Rejection returns the original event without spending
an admission ticket. Ordinary ingress uses this same path through EffectOwner.

Execution rechecks the union. A rejected protocol event releases its queue-only
peers. Stopping a group returns its queued events and tickets, releasing those
reservations. Closing admission retains queued reservations until they drain.
Accepted/pending peers move into bounded identity history. Staged snapshots and
verified promoted-peer replication permits participate even before a log storage
transition exists. Accepted identity history survives rollback and budget
replacement, matching the roster's inactive-history ceiling. A full drained owner
replacement is needed to discard it. No remote session floor is inferred from
this store-only map; the roster retains its existing session checks.

Budget installation/tightening validates all owned queue/core requirements before
mutation and preserves prior history. Group registration also checks the shared
union. A serialized `with_core` callback exceeding its reservation stops/fences
the group before its result/effects can escape. The ledger creates no new
persistence effect, durable receipt, watermark or generation. Restart rebuilds
from recovered core views plus explicitly supplied roster history under the
existing fresh local StoreSession contract.

This implementation recomputes a bounded union rather than maintaining a second
asynchronous ticket table. No throughput or allocation claim is made. Learner
readiness and full distributed transition histories remain unfinished. The
protocol ingress gate remains closed while those requirements are unfinished.

## Retained route admission

`PeerRoute<E>` owns an endpoint direction and exact store identity.
`PeerParts::admission_routes` optionally supplies future peers as well as current
ones. Node construction defaults it from current routes, checks every exact
store against `PeerConnector::supports_peer`, and installs the owner policy only
after assembly succeeds. Failed construction and drain/recovery return the hints
with the selected providers.

`Node::set_admission_routes` and `PeerDriver::set_admission_routes` replace the
owned plan atomically. Rejection returns the entire proposed map through
`PeerRoutesRejected`; the old plan, policy and clock remain intact. Replacement
must cover authorized roster peers and current/queued core requirements.
Admission rejects unavailable peers before taking event ownership. Public
capacity-budget replacement preserves the current closed set, so a stale cloned
budget cannot restore withdrawn routes.

`reconcile_planned_membership(owner, outbound, now)` derives actual connection
requirements from hosted cores. A queue-only future peer reserves admission but
does not authorize a connection. Node runs reconciliation before network work
and after replica execution so newly required peers become visible to deadlines.
Unchanged assignments preserve bindings, attempts and fairness. Retirement waits
for that peer's owner-held Send effects/leases and original outbound credits;
another peer's traffic does not impose a global drain barrier. Explicit low-level
`reconcile_membership` retains its caller-controlled withdrawal semantics.

Plans have at most 65536 entries and charge hint/identity metadata against the
driver limit. Host endpoint heap payloads and cloning must satisfy the connector's
bounded endpoint contract; native endpoints use fixed-size socket addresses.
Credential provisioning is still construction-time, and live credential rotation
is unsupported. This introduces no persistence effect, durable receipt,
watermark or generation, and grants no readiness, voting or activation authority.
