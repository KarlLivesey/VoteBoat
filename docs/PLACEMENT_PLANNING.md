# Placement plans

`PlacementPlanner` is the public synchronous recommendation seam. Native and
host-supplied planners use `PlacementRequest` and the common `plan_learner`
consumer. The result is a proposed `ConfigurationRecord`, not committed membership.

A request borrows current Membership and a finite PlacementSnapshot bound to an
exact group incarnation, configuration and nonzero sample generation. Candidates
carry node/store incarnation, declared failure domain, enabled status, free bytes,
free replica slots and load permille. The snapshot supplies observed/expiry
monotonic times; the caller supplies now and minimum free bytes. The common gate
rejects expired/backwards time, stale configuration, an active joint transition,
duplicate nodes/stores, missing current exact stores, invalid load and bounds.
There are at most 4096 candidates and 64 declared domains.

NativePlacementPlanner ranks eligible unused candidates by fewer current replicas
in their declared domain, lower load, more free bytes and then NodeId. Input order
does not break ties. Disabled, full or insufficient-space candidates are excluded.
Host samples are advisory: this is neither a resource reservation nor evidence
that physical failure domains are independent. Load never changes voting weights.

`plan_learner` validates provider scope/sample and exact candidate eligibility,
operation-history and replica limits, and configuration-ID exhaustion. It retains
all existing learners, voters and quorum policy, adds one learner, and calls the
selected PlacementAuthorizer. Authorization failure returns no accepted work.
Node admission remains responsible for current configuration, credentials/routes,
application capacity, durability and learner readiness. Promotion still uses the
ordinary joint/final protocol. A proposal cannot activate an owner.

There are no workers, I/O, asynchronous receipts or durability effects in planning.
Refusal leaves borrowed state untouched. Restart reconstructs current membership
from durable state and supplies a fresh sample. Sample generations are caller-local
bindings, not a persisted freshness oracle; callers must not reuse them for changed
samples. Keep the original record for operation-ID retries after admission rather
than rebuilding it against a later configuration.

Conformance is in tests/placement_planning.rs and tests/placement.rs. Real TCP/TLS
and QUIC executable histories in tests/counter_service.rs derive a learner record
from recovered membership, then enroll, catch up, promote, retire, checkpoint and
restart through the existing trusted administration path. Candidate capacities in
those tests are explicit fixtures, not measured device capacity.

`plan_replacement` prepares an unused learner and a target policy that replaces
one specified voter. Every recursive branch and edge weight is preserved. Admit
the learner record, catch up the learner, then use a fresh sample and current
membership with `plan_voter_change`. The target policy is an intention, not a
guarantee that future placement authorization or readiness will pass.

`plan_voter_change` accepts an explicitly chosen validated policy and returns
joint/final records sharing one operation ID. Target voters must already have
exact stores in the current voter or learner set. Promotion requires an enabled
candidate, but no additional free slot or second copy's worth of free bytes.
Unrelated learners remain. `RemovedVoters::RetainAsLearners` retains demoted
voters; `RemovedVoters::Retire` excludes them from the final configuration.
Removal does not permit early decommissioning. The joint record must commit
before finalization, and normal retirement rules still apply afterward.

The common gate checks history capacity and both new configuration IDs, then
authorizes the joint record. Execution rechecks authorization and actual learner
readiness; possession of a plan supplies neither. Persist accepted records and
retry their original operation IDs/payloads. Plan again only for a new operation.
The TCP/QUIC leader-demotion histories consume these native-authorized plans,
including unread joint receipts, WAL/checkpoint recovery and finalization.

These APIs do not implement automatic relocation orchestration, global
rebalancing, capacity reservations, a live sample collector or ownership
movement. They add no executable planning flag or public configuration endpoint.
