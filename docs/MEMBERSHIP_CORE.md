# Accepted-log membership in the core

Raft now derives its voting predicate and replication identities from one
`Membership` value rather than consulting the immutable bootstrap at each
quorum site. This is integration groundwork for the full online protocol.
**Public service mutation endpoints remain gated.**
Explicit [learner recovery](LEARNER_RECOVERY.md) supports committed exact-store
assignments with an unchanged bootstrap electorate. A host-authorized local
configuration proposal path now exists (below). Explicit dynamic member recovery
and receive-side configuration replication are available; static/default cores
retain their ingress gate. NativeMemberStartup selects the receiving mode, with
authenticated routes and matching codecs. The native online administrator and
complete remote lifecycle release remain unfinished. Historical internal tests
below do not by themselves establish that release.

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

Default/static ingress still rejects configuration entries and membership snapshots.
Append/Snapshot from an exact locally authorized voter can now bridge differing
accepted heads. Replies echo request scope and match the original outstanding
request. Vote requests also bridge heads using the receiver's local electorate,
log freshness and durable single-vote promise; Voted responses and reads retain
exact current-scope checks. See
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
The internal tests alone do not justify releasing online service mutations.

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

## Append-only joint repair during election

An old-view voter campaigning with a durably accepted, uncommitted joint entry
sends one existing-format Append to each exact learner promoted by that entry,
then sends its ordinary Vote requests. The transfer ends at the joint entry and
includes a bounded retained tail since the stable configuration (at most 64
entries, also limited by the replication byte budget). leader_commit is zero.
Sends wait for the
campaign's exact persisted term/self-ballot completion. Repair acknowledgements
are ordinary Appended messages and cannot satisfy the candidate's vote set.

The receiver's public configuration gate allows this one shape only when its
committed and accepted membership agree on a stable exact local learner, the
sender is an exact voter of that view, the incoming joint promotes this learner,
the journal grammar is valid, and the preceding boundary matches a retained local
index/term. If that boundary precedes the local checkpoint, the batch must contain
the checkpoint's exact index/term; already-compacted entries are trimmed before
normal persistence. Retained overlapping entries must be identical, including payloads; the
final joint must extend beyond the local log end. Earlier entries in the batch
must be commands or noops. Contiguous indices, nondecreasing terms, per-entry and
aggregate byte limits are checked before normal receive can mutate role or term.
Term, group/store/context checks must also pass. It is a pure extension: no
existing entry can be overwritten and commitment cannot advance.
An existing voter or joint-view replica refuses this exception. Duplicate repair
after durable acceptance is refused; normal voting can proceed with the retained
joint after restart. General configuration Append and membership Snapshot remain
gated. Normal storage uncertainty fences the replica.

This is a deliberate protocol addition needed for partial joint delivery. Under
the authenticated non-Byzantine model, it distributes a configuration already in
a candidate's durable history, without permitting a candidate to rewrite another
voter's log or assert commitment. That restriction matters: letting unelected
candidates replace arbitrary voting histories could erase evidence needed by
log-freshness election checks. Accepted activation follows the existing journal
rules, but the common pending-dependency guard blocks voting/campaigning until
exact durability completion. Receiving the transfer does not make either side a
leader or authorize application writes or reads; a subsequent ordinary election
must collect the actual old-and-new predicate's durable votes.

The weighted actual-core regression drops repair traffic through four election/
restart rounds, then delivers the production transfer and Vote through public
entry points. Native provider checks cover wire versions 2–4, torn append/sync/
manifest failures and actual file reopen after a lost acknowledgement. These are
bounded evidence, not a proof over arbitrary forks or a complete remote release.
Native TCP/QUIC nodes also recover with 32 missing retained entries, elect and
commit an application write before file reopen. This path deliberately refuses
learners behind the bounded retained tail, compacted/missing preceding boundaries,
forked overlap, transferred snapshots,
uncommitted learner assignments, existing joint heads and promoted senders that
are not trusted voters in the learner's current view. Candidate suffixes beyond
the joint may still require additional catch-up before log freshness permits a
vote. Those cases remain release work. General retained-range repair spanning
multiple batches needs explicit candidate repair state and authenticated replies
that advance only a repair cursor. Ordinary Append backoff cannot be reused
unrestrictedly: a later or competing accepted joint could otherwise have its
voting history replaced by delayed pre-election traffic. Any new RPC requires
explicit codec/session capability negotiation and matching native integration.

## Multi-batch retained learner repair

`Raft::with_batched_joint_repair` explicitly enables the format-5 protocol during
assembly. Campaign's exact durable term/self-ballot completion starts at the
retained stable configuration boundary and emits one LearnerRepair batch per
promoted learner. Each carries the durable joint assignment as context and at
most 64 entries within the byte budget. This context alone cannot activate it.
Only the range containing the exact joint entry activates accepted membership.
The source joint may be committed or uncommitted; neither case exports its
commit boundary or changes the receiver's commitment rules.
If the source has compacted away the joint entry, it skips retained repair while
still emitting ordinary Vote requests; explicit format 6 supplies the committed
checkpoint path described below.

Every range checks the committed/accepted stable exact learner, trusted old
voter/store, promotion journal, contiguous indices/terms, byte bounds and identical
retained overlap before normal persistence. It cannot replace any suffix, assert
commit or grant serving authority. Late repair after joint activation is refused.
Dependent acknowledgements wait for the ordinary exact LogTicket/DurableLog
completion. Existing durable matching ranges may acknowledge immediately; a
compacted range returns a retained checkpoint hint. Hints require the candidate's
matching index/term and can advance only the repair cursor.

Candidate-local request contexts/cursors are separate from ballot and replication
progress. Replies check group, exact store, origin session, term, configuration,
context and expected end. They cannot count toward a quorum. Final acknowledgement
resends an ordinary Vote; only real durable ballots can elect. Campaign, term/view
changes, leader activation, compaction and fencing clear cursor state. Restart
reselects the volatile capability and reconstructs from durable retained history;
identical overlap makes a lost cursor reply retryable. No persisted cursor or new
durability token/generation/watermark is introduced.

Native TCP/QUIC format-5 nodes catch up a 160-entry tail in three batches, elect,
commit/apply and reopen a write. Core histories cover lost reply/restart, delayed
traffic after promotion, malformed authority/ranges, stale contexts, higher-term
persistence and a matching compacted receiver hint. Native WAL faults fence with
no reply and recover an old or complete batch. These are bounded histories, not
a full fork/term/liveness proof. Conflicting learner tails, promoted senders and broader recursive
policy histories remain release work. Service mutation ingress remains gated.

### Historical committed snapshot repair (native format 6)

`Raft::with_snapshot_joint_repair` selects format 6 and includes retained
format-5 repair. An old-view candidate can supply its own already committed,
pinned checkpoint to an exact learner being promoted by its accepted joint.
The image's stable configuration must equal the candidate's old configuration;
any included joint must equal its accepted joint. `SnapshotRequired` uses the
existing owned snapshot load and checks exact context, current reference,
metadata and membership before sending `LearnerRepairSnapshot`. A current
candidate cannot assert an arbitrary commit boundary through this path.

The receiver must still be an exact committed stable learner. It authenticates
the sender as an old-view voter/store, checks bootstrap, stable configuration,
retained operation IDs, terms, scope and exact promotion assignment. New-only
senders, existing voters, receivers already in a joint, and final/new-only images
are refused. This uses the authenticated, non-Byzantine Raft trust model: native
senders export their locally verified committed checkpoint. It adds no portable
commit certificate for untrusted images or Byzantine senders.

The image passes application validation, publication, pinning and an atomic
snapshot/membership/commit WAL binding before installation. The exact ordinary
durability completion permits `SnapshotInstalled`, not a repair reply. Application
restoration releases `LearnerRepaired`; pending storage or restoration blocks
votes and service work. A committed joint checkpoint may activate the joint
through that binding. A stable checkpoint is followed by retained repair through
the joint. The reply advances only the candidate's separate recovery cursor;
completion triggers an ordinary Vote request, never fabricated ballot evidence.

Restart reconstructs membership and application from the verified pinned image
and WAL without needing a repair receipt. Cursors and the assembly option remain
volatile. Existing snapshot generations/tickets and store sessions fence stale
work; no new durability token or quorum watermark is introduced. Queue budgeting
and connection reservation treat these images as ordinary bounded snapshots.
Native format 6 is explicit and exact across codec, roster and TLS/QUIC sessions;
older formats refuse it. General configuration ingress remains closed.

### Explicit receive-side configuration replication

`Raft::with_configuration_replication` selects ordinary configuration-bearing
Append and membership Snapshot reception at construction. It changes the default
release gate, not sender authentication, journal grammar, quorum predicates,
log matching, committed-prefix protection, size/fanout limits or storage/application
dependencies. Static cores keep their strict repair-only gate. Restart must repeat
the volatile selection. NativeMemberStartup selects it with explicit provisioned
stores and membership-capable wire versions; generic Node rejects missing peers
or a roster older than format 2 before any work. Direct core hosts supply the same
compatible authenticated transport and admission obligations.

A recursive recovery regression exposed why this is necessary: after repairing
and electing a retained old voter, another required old voter still lacks the
joint entry. Refusing that leader's normal catch-up prevents current-term commit.
The explicit path allows that old voter to catch up. Accepted membership, pending
WAL completion and commitment remain distinct; Written alone releases no reply.
New-only/promoted senders still require the existing witness permit when the
receiver's view cannot authorize them. Declaring a newer scope grants no authority.

Actual-core coverage uses nontrivial weighted/nested old and new policies whose
quorums require different two-leaf subtrees, then commits/applies a write and
obtains a joint read barrier. Native TCP/QUIC coverage uses three active stores,
a lagging old voter, an 80-entry missing learner prefix and an uncommitted
candidate command beyond the joint. Election, current-term commitment, application
and file reopen preserve that command plus the new write. The native policy's
nested single-leaf branch is structurally recursive but Boolean-equivalent to a
two-voter majority; the distinct nested predicate is exercised by the actual-core
history. Initial assignments are seeded, not distributed enrollment evidence.

The native history permits intentional ingress refusals from competing campaigns
(late learner repair and an unauthorized promoted candidate); it still requires
end-to-end progress and recovery and permits no storage/fencing errors. Distinct
per-node election entropy avoids the fixture's previous lockstep campaigns.
Finite histories are not a general fork/term/liveness proof. Service mutations,
promoted-leader/witness lifecycle integration and broader fault histories remain
release work.
