# Owned configuration requests

`Node::configure(ConfigurationRequest)` admits a typed administrative proposal
and returns a `ConfigurationTicket`. It preserves the original request on
rejection. Admission is volatile; it does not certify persistence or commitment.
The existing configuration journal remains authoritative for learners, joint
consensus and finalization, including expected configuration and operation IDs.

Drive accepted requests through `Node::poll_with_configuration_authorization`.
Its host callback receives the current core and proposal immediately before
execution. It must be bounded and nonblocking, and authorize service scope,
placement/failure domains, application/storage capacities and the required size
envelope. A callback
cannot waive journal grammar, quorum validation, exact readiness or durability.
The Node additionally rejects remote expansion without peer providers and static
wire format 1, and rechecks every promotion
proof against its live authenticated format-4 peer store/session. Queued work
retains no cached authorization. Ordinary `Node::poll` denies configuration
execution. Application commands retain their application admission checks.

Poll `Node::poll_configuration` for one of these outcomes:

| Outcome | Meaning |
| --- | --- |
| `Committed(position)` | The exact configuration record and term/index are in the selected core's locally durable committed prefix. A joint receipt does not certify a later final record. |
| `NotProposed(error)` | This exact invocation failed before producing configuration persistence. |
| `Unknown(reason)` | Observation ended; accepted work can still commit. Query/recover durable history before deciding what to retry. |

Finalization uses the joint operation ID and expected joint configuration and
requires joint commitment. The ticket identifies an invocation separately from
that durable operation. Runtime steps now expose configuration operation and
proposed position, alongside application proposal steps; those fields alone are
not committed receipts.

`Node::cancel_configuration` cancels the wait, preserving queued or already
submitted work. `begin_shutdown` closes administration intake, preserves observed
committed outcomes and marks unresolved waits unknown. Consume retained results
to let graceful shutdown finish. Abort/failure retains unknown results in
`NodeRecovery::configuration` with the original worker/provider ownership.

The public `ConfigurationRequests` observer provides the same contract to hosts
assembling their own owner loop. Submit to the selected owner, pass its original
steps and current durable state to `observe` after each advancement, and poll
results. Default limits are 1,024 retained requests/outcomes and 16 MiB of retained
record/observer accounting, with one slot per group. The owner separately charges
queued proposals and readiness allocations. A result occupies its slot until
polled; exported results are the host's responsibility. Admission tickets retain
the existing store session/runtime owner and monotonic owner sequence. Replacing
an observer does not reset that sequence; reconstructing an owner requires the
existing fresh runtime identity contract. No new durability token or generation.

Host tests cover durability-dependent receipts, joint/final sequencing, admission
limits, authorization revocation, cancellation without rollback, queued election
work, storage failure and retained results during shutdown/recovery. A native
startup test commits a same-electorate single-voter joint/final transition through
the selected worker, reopens the actual WAL and reconstructs membership through
explicit member recovery. This does not enable dynamic recovery through the
static startup convenience constructor.

Remote configuration-bearing Append and membership Snapshot remain gated by
default. Explicit member assemblies can select receive-side configuration
replication; native member startup does so after its exact-store/codec/route checks.
This is distinct from releasing public service mutation endpoints.
Generic application-envelope integration, authorized service mutation endpoints
and faulted multi-node add/promote/remove remain required. Counter bounds and
offline executable enrollment are implemented as described below. The supplied authorization
callback is also available to host policy implementations. Existing TCP/QUIC
readiness and static-service tests do not establish this complete path.

## Durable status and safe resumption

Use Node::configuration_status(group, operation), or Raft::configuration_status,
to inspect the selected authoritative durable log. The result separates committed
progress from durably accepted progress, using existing commit/log boundaries.
Pending proposals and Written completions cannot advance it. This is historical
local evidence, without a fresh read barrier or remote certificate.

| Observation | Resumption action |
| --- | --- |
| Committed learners, final, or compacted completed identity | Completed; this does not compare a new request payload. |
| Committed joint with the same accepted joint and target | Propose its exact final record through ordinary admission. |
| Accepted but uncommitted joint or final | WaitForCommit; do not append another final. |
| NotFoundLocally | Inconclusive; do not infer cluster-wide absence or invent replacement intent. |

Node::resume_configuration rederives this action from its current core. A final
uses the original operation, joint configuration and target, and receives a fresh
admission ticket. Drive it through poll_with_configuration_authorization;
leadership, current-term commitment, journal grammar and authorization are checked
again at execution. Ordinary polling still denies it. A retained earlier result
occupies its group slot until consumed. Cancellation stops observation, so queued
work may still commit before a later status query. No status API restores unknown
learner intent or bypasses provider binding, authorization or durability.

Existing snapshot operation identities preserve completion after compaction.
An identity outside the snapshot's active joint proves that operation finished
under the validated journal grammar; its discarded phase, payload and exact
position are unavailable. An active joint retains its original index and target;
its term is None if discarded below the snapshot boundary. No journal/checkpoint
format, generation, durability token or watermark is added. See
[configuration snapshots](CONFIGURATION_SNAPSHOTS.md).

Shared host/native tests cover pending acceptance, rollback, checkpoint/reclaim,
actual file reopen and uncertain commit publication. Node tests cover lost
observation and fresh authorization; native startup uses resumption for local
finalization before real WAL reopen. These establish local recovery behavior,
not remote enrollment or a complete online membership lifecycle.

## Native placement policy

PlacementAuthorizer is the public execution-time contract (version 1). Pass a selected
implementation to Node::poll_with_placement_authorizer, or combine its authorize
method with other checks in poll_with_configuration_authorization. Ordinary Node
polling still denies administration. Permission cannot waive journal grammar,
learner readiness, exact authenticated bindings or durability.

NativePlacementAuthorizer owns an immutable plan for one exact group incarnation.
ReplicaPlacement binds each node to its exact store identity/incarnation and a
typed FailureDomainId. Domain labels are trusted deployment assertions; the
provider cannot discover or certify physical independence. PlacementRequirements
sets minimum voting domains and optionally requires continued quorum after loss
of any one voting domain. Learners count toward neither rule. The provider uses
the validated recursive policy to evaluate each surviving voter set, including
weights and nested branches; counting domain labels alone is insufficient.

The current accepted stable configuration, active joint target and proposed
configuration must all satisfy the plan. Retain assignments for retiring replicas
until they are absent from those sets. An operator cannot apply stricter rules
that the old side violates and use finalization to bypass them. Placement approval
of a proposed new voter still does not establish committed learner preparation.

Plans retain at most 4,096 replicas and 64 domains, with unique exact store
identities and nonzero feasible minimum-domain requirements. Rejected construction
returns the original assignment map. Checks allocate only bounded temporary sets,
accept no asynchronous work and perform no I/O; no close/drain operation exists.
The caller owns the plan and reconstructs it from trusted deployment input after
restart. Queued requests consult the selected plan at execution, so replacing
caller policy does not preserve earlier approval. No durable generation or receipt
is introduced. Provisioned routes, credentials, readiness and selected transport
envelope admission remain separate checks. Native placement is usable from Rust;
service placement-policy inputs, authorized service endpoints and generic application
envelope enforcement remain work. Counter bounds and offline enrollment exist.

This is the C18 placement authorization subset. Eligible-host planning, scoring
and automatic move proposals remain pending under the broader placement roadmap.

## Selected codec and transport capacity

For a networked Node, all authorized configuration execution now asks the actual
selected factory for configuration_capacity. Default unsupported providers deny;
reported format must match the roster, and append/command/checkpoint footprints
must fit both factory and roster transport budgets. Native sizing validates the
journal preview and counts the exact record and prospective membership checkpoint,
including retained operation IDs. Command/checkpoint bytes use the proposal's
ReadinessRequirements envelope; no application-sized counting buffers are allocated.
This creates no persistence effect, durability receipt or cached approval.

The native worker test denies an oversized envelope without changing the durable
log, then commits/resumes/reopens the valid local joint/final operation. Host Node
injection checks unsupported capability, wrong format, restrictive roster budget,
invalid footprints and successful explicit capacity. See the
[transport contract](TRANSPORT.md) and [wire format](WIRE_FORMAT.md).

The caller/service must keep the declared envelope consistent with its application
schema, accepted command sizes, checkpoint/deduplication growth and storage limits.
This query alone does not enforce future application growth or arbitrary batching.
NativeMemberStartup now supplies explicit verified dynamic member restart for Rust
hosts. Service placement-policy inputs, generic enforced application envelopes
and complete faulted remote transitions remain pending; public service mutation
endpoints stay gated. Counter bounds and offline learner enrollment are implemented.
Explicit member assemblies now select validated receive-side configuration
replication, which supplies the native integration path for those transitions.

Explicit native member restart is documented in the [service/embedding guide](COUNTER_SERVICE.md).
The counter executable now exposes `serve recover-member` for existing dynamic
histories among explicitly provisioned identities, selecting wire format 6
on all participants. This reuses verified member recovery and enforced counter
bounds; ordinary service polling continues to deny configuration mutation.
Prepared TCP/QUIC service histories cover joint/final reopening, real checkpoint
drain and retry deduplication after restart. Service placement-policy inputs and
authorized mutation endpoints remain integration work; these fixtures do not
prove online proposal delivery.
The original static NativeStartup entry points retain their rejection of dynamic journals.

`voteboat-counter enroll create|recover` now performs the offline trusted learner
handoff from a stopped native source's authoritative pinned checkpoint. It verifies
source recovery and current committed membership before destination import, then
uses NativeMemberStartup::enroll_snapshot with the same enforced counter envelope.
Exact retries preserve WAL state; missing/corrupt/incompatible inputs do not imply
permission to reset stores. `--deployment FILE` declares additional exact stores
and routes within the original three-voter bootstrap; it grants no membership or
configuration execution authority. Versioned/size-bounded input, exact-identity
refusal, fourth-learner enrollment/catch-up and TCP/QUIC restart are exercised.
See the service guide for command syntax and partial-initialization limitations.

## Exact trusted administration plans

`native::administration::NativeAdministrationPlan<P>` supplies a native
implementation of the existing Node execution-time authorization callback. Supply
one exact group, the selected `PlacementAuthorizer`, the enforced application
envelope and a finite list of trusted `ConfigurationRecord` intents:

```rust
let plan = NativeAdministrationPlan::new(group, placement, requirements, intents)?;
boat.poll_with_configuration_authorization(now, budget, |core, proposal| {
    plan.authorize(core.state().bootstrap.group, core.membership(), proposal)
})?;
```

The constructor accepts at most 64 retained intent slots and 1 MiB of record
footprints, apart from the selected placement provider's own bounds. Empty lists,
zero schema/command/checkpoint requirements, excess vector capacity/record bytes
and duplicate `(operation, expected configuration)` keys refuse with every input
returned. Joint and final records can share their operation ID with different
expected heads. Bind the envelope to the application's actual admission/restore
limits; nonzero requirements alone do not prove application capacity. Derive
intents from trusted operator policy, never from the request being authorized.

Execution requires exact group, full record equality and exact envelope, then
calls the selected native or host placement provider against current membership.
Operation ID alone cannot approve a changed target. Node/core still enforce
journal grammar, current-term commitment, fresh authenticated readiness and the
actual selected wire/transport envelope. Approval itself emits no effect or
receipt. The plan is immutable and owns no work or session; it adds no generation,
watermark or durable authority. Reconstruct it from trusted input after restart.

Select the current plan on each poll. A queued request retains no earlier scope
approval; switching to a narrower plan can deny it before persistence. Ordinary
polling denies all configuration execution. Cancellation of the observation
ticket still does not revoke an executed intent or roll back configuration history.

Owning native TCP/TLS and QUIC histories start from a prepared committed learner,
authenticate readiness, deny a queued joint under a withdrawn scope with unchanged
durable state, then cancel observation and commit the actual joint on the old/new
voter sets. Durable status drives a fresh authorized final proposal. Both paths
commit an application write, checkpoint/drain and reopen all native files with
final membership, counter value and no readiness token. Unadmitted WrongIdentity
ingress after scope transition is permitted in these histories; administrative,
application, storage and other errors remain failures. These are finite available-
quorum histories, not the complete faulted remote lifecycle. Service plan parsing
and authorized mutation commands remain current integration work; default service
polling stays gated.
