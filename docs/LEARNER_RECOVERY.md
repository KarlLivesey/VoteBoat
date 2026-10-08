# Explicit learner enrollment and restart

`Raft::recover_learner` is an explicit host-authorized entry point for a local
non-voting replica. The host supplies its selected store binding and durable log
state. An incoming message cannot create a group, write an assignment into an
empty store, or invoke this entry point. As with initial Bootstrap, the host is
responsible for administrative authorization and verified import of the committed
assignment; constructible Rust data is not a remote commitment certificate.

Both the committed prefix and the latest accepted membership must assign the
exact local NodeId and StoreIdentity as a learner. An uncommitted initial
assignment is insufficient. An unrelated uncommitted learner change may survive
restart if it retains that same committed local store assignment. Removal or
replacement in either view refuses enrollment. Store sessions remain supplied
by the selected recovered store; copying an identity onto another live machine
is not a supported replacement procedure.

This entry point currently permits an unchanged bootstrap voter policy/store map,
no joint snapshot base and no joint/final record in the retained suffix. Snapshot
bases must have that same electorate. It refuses a recovered vote and never
promotes a node. The original voter recovery entry point retains its online
configuration gate. Hosts restoring general dynamic-electorate state can now
use explicit [dynamic member recovery](MEMBER_RECOVERY.md), which reconstructs
voter or learner eligibility from its accepted journal and exact assignments.

Learners can durably accept ordinary Append traffic from an exact authorized
voter, including command/commit updates, and return the checked matching prefix.
The exact admitted LogTicket in DurableLog releases the acknowledgement; Written
and a completion without that ticket do not. Committed application replay uses
the same public StateMachine contract and treats configuration entries as protocol
state. No successful client proposal, campaign, read barrier, vote grant or read
probe acknowledgement is available from the learner. A higher-term vote request
can update its durable term before a negative reply, retaining no ballot.

`Raft::local_voter` exposes the exact accepted node/store assignment for runtime
coordination; it is not durability evidence. TimedShard creates no election timer
for a non-voting follower and cancels any former managed timer. A retiring leader
still retains its heartbeat timer until it finishes final commitment and steps
down, preserving the existing retirement dependency.

## Compaction and data verification

`Raft::recover_learner` refuses compacted state. Use
`snapshot::recover_learner_replica` with the selected LogStore, SnapshotRetention
and fresh application. The helper validates assignment, loads the exact pinned
snapshot, validates its group/bootstrap/membership/index/term/schema against the
log, restores application state and replays the retained committed tail before
returning the core. Missing referenced data prevents recovery. Normal checkpoint,
verified pin, logical compaction and physical WAL reclaim contracts are reused.
No new persistent record, format, token, effect, generation or watermark is added.
Membership-base retention remains bounded by the selected log byte budget.

## Evidence and remaining work

Eight downstream tests in `tests/learners.rs` cover host/native enrollment,
replication, exact completion ordering, voter/read/service exclusion, uncommitted
and wrong-store assignments, removed/replaced learners, promotion/policy gate
refusal, missing pinned data, checkpoint/application replay, every cut of a native
assignment frame and failed synchronization/manifest publication. A real native
log plus snapshot-file history compacts and reclaims the learner, closes both
stores, reopens them and verifies the exact assignment and application state.
These tests explicitly import a host-authorized assignment; they do not prove
that an online administration operation committed it on the remote voters.

A timed-runtime test processes a durable learner append with no election deadline,
including after a large virtual-time advance. Existing static three-node TCP/TLS
histories and the local ballot model remain separate evidence. This Linux run
is not macOS execution, hardware power-failure certification or a full joint
protocol proof.

A promoted leader that an older receiver still sees as a learner remains rejected.
Transport authentication is separate from group authority. Promotion/catch-up
provenance, readiness evidence (storage/application capability
and a durable caught-up prefix), retiring-leader
final propagation, distributed activation modeling and faulted actual network
membership histories remain release gates. Public configuration-bearing Append
and membership Snapshot ingress remain disabled.
