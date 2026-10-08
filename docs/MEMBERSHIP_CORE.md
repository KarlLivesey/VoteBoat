# Accepted-log membership in the core

Raft now derives its voting predicate and replication identities from one
`Membership` value rather than consulting the immutable bootstrap at each
quorum site. This is integration groundwork for the full online protocol.
**Public recovery and ingress still refuse configuration-bearing state.** There
is no online reconfiguration API. The internal transition tests described below
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
| Campaign admission | Local node must be a voter; learners/removed nodes return `NotVoter` before term or timer changes. |
| Durable self-vote completion and received ballots | Both policies must be satisfied in joint state; election messages target voting peers only. |
| Incoming sender identity | Exact active union store identity; learners can return replication replies but cannot supply Vote/Voted/ReadProbe/ReadAck or lead replication. |
| Vote grant | Local replica and candidate must both be voters, in addition to durable single-vote and log-freshness rules. |
| Leader replication | Voters and learners from both sides of a joint configuration, each replica exactly once. |
| Append/snapshot/compacted acknowledgements | Existing term, request, sender and matching-prefix checks; learner progress may be recorded but is absent from quorum predicates. |
| Commit frontier | `Membership::frontier`, requiring old and new policies jointly, then local-prefix/current-term restrictions. |
| Read admission | Voting leader and current-term committed entry required. |
| Read probes and readiness | Probes target voters; readiness requires the joint predicate and exact barrier configuration. |
| Read consumption | One-use barrier checks the current membership ID, term, group, original context and applied prefix. |
| Message envelope | Effective accepted membership ID, independently of immutable bootstrap or snapshot-base IDs. |
| Configuration change | Clear old read authority and recollect volatile ballots/progress under fresh contexts. |
| Local removal/demotion | No new proposals/reads/campaigns after accepted final; finish final commitment, then step down and clear leadership state once the final record is durable and committed. |
| Node construction | Roster compatibility checks every effective replication assignment, including learners. This check grants no new network authorization. |
| Output reservation | Counts all current replication identities, including learners. Prospective growth from an online input remains an explicit unfinished prerequisite. |

The implementation has no check-quorum, pre-vote, leader-transfer or separate
"quorum available" subsystem. Those future sites must use the same configuration
rules when introduced. The standalone `Voter` example and its separate ballot
store retain their documented static-configuration contract; they are not a
second election path inside `Raft`.

## Remaining gates

The remaining bootstrap checks in `Raft::recover_verified` deliberately authorize
only the existing static protocol: local voter identity, initial policy/map,
snapshot scope and recovered ballot. Dynamic entries/bases are refused before
membership reconstruction. `log::apply_batch` also still validates ballots against
the bootstrap electorate. Enabling a newly promoted candidate requires a durable
ballot-history/recovery design that preserves earlier ballots when their candidate
is subsequently removed or the surrounding suffix is rolled back. Accepting only
the current voter set at recovery would be incorrect.

Live ingress still rejects configuration entries and membership snapshots. It
also currently requires the sender's configuration to equal the local accepted
configuration. That check needs protocol-aware handling for a lagging follower
receiving a newer joint/final entry, without allowing old responses to count for
new authority. Request/response configuration selection, a retiring leader's
final propagation and snapshot catch-up need actual end-to-end histories.

Other unfinished prerequisites are explicit learner assignment/recovery,
application/storage compatibility and catch-up evidence, prospective fanout
reservation before an expanding event, route/roster admission, the formal
activation/ballot state-machine model and faulted native/host network histories.
The internal tests do not justify removing any of these gates.

## Evidence boundary

Six internal tests in `src/raft/membership_tests.rs` drive the actual persistence,
completion, election, append-acknowledgement, read, compaction and rollback
helpers. Their deliberately prepared committed boundaries and host-asserted
completion tokens isolate the core rules being checked. They do not exercise
an online administrator or prove that their asserted durability occurred on disk.
They cover accepted-but-not-durable visibility/storage failure, learner identity
and exclusion, promoted voters in joint commit, same-voter weighted election/read
changes, stale configuration responses, final rollback into a compacted joint
base and local demotion/removal service fencing.

A downstream public-interface test checks exact replica/voter store views through
learner/joint/final activation, rollback and checkpoint replay, including removed
and demoted identities. Existing host/native/TCP/TLS histories continue exercising
the static configuration supported by public recovery and ingress. Finite helper
checks are not a formal membership model, complete joint Raft proof, macOS run,
power-failure certification or benchmark.
