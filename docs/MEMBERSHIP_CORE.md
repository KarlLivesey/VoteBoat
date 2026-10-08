# Accepted-log membership in the core

Raft now derives its voting predicate and replication identities from one
`Membership` value rather than consulting the immutable bootstrap at each
quorum site. This is integration groundwork for the full online protocol.
**Voter recovery and public online configuration ingress remain gated.**
Explicit [learner recovery](LEARNER_RECOVERY.md) supports committed exact-store
assignments with an unchanged bootstrap electorate. A host-authorized local
configuration proposal path now exists (below); the native online administrator
and network ingress remain gated. The internal transition tests described below
exercise these core paths without removing the release gate.

## State and dependency ordering

`Raft::membership()` borrows the latest accepted-log view. An accepted pending
WAL transition has its own validated view; the durable `GroupLog` remains
unchanged until its exact admitted `LogTicket` appears in a `DurableLog`.
Reading this view proves neither durability nor commitment. While that transition
is pending, other events return `Busy`; no ballot, replication acknowledgement
or committed/application effect escapes early. Failed storage drops the pending
view and fences the core, retaining its last durable configuration.

Each successful logical update derives the view from its surviving suffix plus
snapshot base. A final-entry rollback therefore restores joint rules, and
compaction preserves the base's full configuration/operation history. The cache
is reconstructed, never separately persisted or selected by a local policy file.
It adds bounded resident derived state alongside the log and pending transition;
these are core-state allocations, separate from output reservations. Existing
configuration replica/tree/operation limits remain authoritative. No new
persistence generation or watermark is introduced.

A configuration ID change clears outstanding reads immediately on acceptance.
After exact durability it clears volatile ballots, election context, replication
requests and recorded peer progress. The leader initializes the new replication
set and conservatively recollects matching durable prefixes under fresh contexts.
This applies to learner-only changes, same-voter policy changes and rollback.
Ordinary appends and commit updates keep existing request contexts. The commit
frontier remains a contiguous matching prefix, bounded by local durability and
Raft's current-term rule; it is never the largest observed acknowledgement.

## Quorum and identity audit

| Site | Membership rule now used |
| --- | --- |
| Campaign admission | Exact local node/store assignment must be a voter; learners/removed nodes return `NotVoter` before term or timer changes. |
| Durable self-vote completion and received ballots | Both policies must be satisfied in joint state; election messages target voting peers only. |
| Incoming sender identity | Exact active union store identity; learners can return replication replies but cannot supply Vote/Voted/ReadProbe/ReadAck or lead replication. |
| Vote grant | Exact local replica and candidate must both be voters. Same-term repeat grants retain the candidate store identity from the durable ballot origin, alongside single-vote and log-freshness rules. |
| Leader replication | Voters and learners from both sides of a joint configuration, each replica exactly once. |
| Append/snapshot/compacted acknowledgements | Existing term, request, sender and matching-prefix checks; learner progress may be recorded but is absent from quorum predicates. |
| Commit frontier | `Membership::frontier`, requiring old and new policies jointly, then local-prefix/current-term restrictions. |
| Read admission | Voting leader and current-term committed entry required. |
| Read probes and readiness | Probes target voters; readiness requires the joint predicate and exact barrier configuration. |
| Read consumption | One-use barrier checks the current membership ID, term, group, original context and applied prefix. |
| Message envelope | Requests use effective accepted membership ID; replies echo the request scope, independently of the responder head and snapshot-base IDs. Outstanding replication pins its admitted scope. |
| Configuration change | Clear old read authority and recollect volatile ballots/progress under fresh contexts. |
| Local removal/demotion | No new proposals/reads/campaigns after accepted final; finish final commitment, then step down and clear leadership state once the final record is durable and committed. |
| Node construction | Roster compatibility checks every effective replication assignment, including learners. This check grants no new network authorization. |
| Output reservation | Includes voters/learners at the current head, rollback-reachable uncommitted prefixes and prospective incoming append/snapshot configurations. Owner admission and exact next-event checks precede execution; counting grants no configuration authority. |

The implementation has no check-quorum, pre-vote, leader-transfer or separate
"quorum available" subsystem. Those future sites must use the same configuration
rules when introduced. The standalone `Voter` example and its separate ballot
store retain their documented static-configuration contract; they are not a
second election path inside `Raft`.

## Remaining gates

The remaining bootstrap checks in `Raft::recover_verified` deliberately authorize
only the existing static protocol: local voter identity, initial policy/map and
snapshot scope. That voter path still refuses dynamic entries/bases. Explicit
[dynamic member recovery](MEMBER_RECOVERY.md) instead verifies committed/accepted
exact-store assignments and reconstructs stable/joint/final predicates, with
verified checkpoint/application replay before exposing compacted members. The
original learner path accepts assignment-only state. Durable ballot validation
now uses historical origin rather than the current electorate; see
[ballot recovery](BALLOT_RECOVERY.md). A removed candidate's retained promise
cannot authorize another candidate or a replacement physical store in that term.

Live ingress still rejects configuration entries and membership snapshots.
Append/Snapshot from an exact locally authorized voter can now bridge differing
accepted heads. Replies echo request scope and match the original outstanding
request; elections/reads still require equal configurations. See
[replication scopes](REPLICATION_SCOPES.md) for catch-up, rollback and snapshot
evidence. A newly promoted sender that is only a learner or absent in the older
receiver's view remains rejected pending its catch-up authorization protocol.
A retiring leader now sends a restricted final commitment announcement before
stepping down; an already prepared receiver persists it without granting ongoing
authority. See [retiring leaders](RETIRING_LEADERS.md). Lost notices, lagging
configuration catch-up and roster retirement still need end-to-end histories.

Initial learner-only enrollment/restart now uses committed exact-store
assignments and verified application recovery. Remaining prerequisites include
application/storage compatibility and caught-up
evidence, route/roster admission, the distributed
activation/ballot state-machine model and faulted native/host network histories.
The bounded local ballot model is one prerequisite, not that complete model.
The internal tests do not justify removing any of these gates.

Output reservation now covers prospective fanout before an expanding event and
rollback through an uncommitted shrink, including snapshot membership bases.
Committed shrink stops budgeting obsolete configurations. Incoming journal
counts conservatively bound intermediate joint unions without cloning or
validating untrusted history; actual core membership validation remains mandatory.
Any future administrative event must also describe its prospective fanout before
its ingress gate can open.

## Host-authorized local proposals

`Event::Configure(Box<ConfigurationProposal>)` is explicit administrative input,
separate from an application command. The host authorizes placement/failure
domains and supplies the group's ReadinessRequirements. A voting leader must
have established a committed prefix in its current term before proposing.
The existing journal grammar validates the expected configuration, operation
identity, learner assignments, joint/final ordering and exact stores.

Every voter newly introduced by a joint proposal needs one `PromotionReadiness`:
a core-issued ReadyLearner and its current authenticated peer StoreBinding.
The token must cover the current committed prefix and exact required capabilities.
Missing, duplicate, extraneous, stale, wrong-store or mismatched-capability proofs
reject before mutation. Existing voters need no new readiness; same-voter quorum
policy changes still go through joint consensus. Finalization needs the exact
joint operation/target and committed joint index, and takes no learner proofs.

Success produces only the existing Persist effect. Accepted pending membership
is visible, but durable state and dependent sends wait for the exact LogTicket
in DurableLog. Written is insufficient. The record uses the existing WAL codec,
snapshot journal and recovery path; volatile readiness is not persisted.
Configuration records advance application replay but emit no command receipts
or ordinary client proposal position. Query the durable/committed journal to
resolve an uncertain administration outcome; retrying an operation ID is not
authority to append another transition.

Runtime admission charges owned record metadata and the proof vector's retained
capacity, reserves all prospective exact peers and output fanout, and checks
retained routes/pins through the existing owner policy. Queue-only requirements
remain reserved until execution/rejection/stop. Default Shard, TimedShard and
EffectOwner stepping refuses promotion proofs with AuthenticationRequired.
ClientRouter/Node advancement also retains that default. Hosts explicitly use
`EffectOwner::advance_with_configuration_bindings` to check each current peer
binding immediately before execution. A disconnect/restart cannot be hidden by
the binding captured in a queued proposal. Direct Raft::step hosts perform the
same authentication check themselves under the trusted host contract.

This path enables tested local proposal admission, not public online configuration
delivery. Native readiness RPC/worker integration, placement authorization,
distributed activation modeling and faulted network transition histories remain
required. Public configuration-bearing Append and membership Snapshot ingress
stay closed. Local proposal tests use host-asserted peer-2 acknowledgement messages;
they do not demonstrate that remote voters received configuration records.

## Evidence boundary

Eighteen internal tests in `src/raft/membership_tests.rs` drive the actual persistence,
completion, election, append-acknowledgement, read, compaction and rollback
helpers. Their deliberately prepared committed boundaries and host-asserted
completion tokens isolate the core rules being checked. They do not exercise
an online administrator or prove that their asserted durability occurred on disk.
They cover accepted-but-not-durable visibility/storage failure, learner identity
and exclusion, promoted voters in joint commit, same-voter weighted election/read
changes, stale configuration responses, final rollback into a compacted joint
base and local demotion/removal service fencing. The seventh retains an old-store
ballot through removal, learner-store replacement and promotion; it denies a
same-term new-store request and permits a fresh-term request only after durability.

A downstream public-interface test checks exact replica/voter store views through
learner/joint/final activation, rollback and checkpoint replay, including removed
and demoted identities. Existing host/native/TCP/TLS histories continue exercising
the static configuration supported by public recovery and ingress. The seven additional request-scope tests cover retained-leader catch-up, partial
chunks, snapshot installation and rollback; their separate evidence boundary is
documented in the scope audit. Finite helper checks are not a complete joint Raft
proof, macOS run,
power-failure certification or benchmark.
