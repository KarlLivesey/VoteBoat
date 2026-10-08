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
provenance and retiring-leader final propagation now have separate core checks.
Readiness has the host-driven and native contracts below. Placement
authorization, distributed activation modeling and faulted actual
network membership histories remain release gates. Public configuration-bearing
Append and membership Snapshot ingress remain disabled.

## Native readiness exchange

`Event::CheckLearnerReadiness` starts the same fresh core request and emits a
format-4 RPC. `Node::request_learner_readiness` captures the current authenticated
roster binding and refuses incompatible protocol selection. An assigned learner
checks request scope without changing its term or election timer, then emits
`Effect::VerifyLearnerReadiness` and suspends that group's owner visit.

The existing SnapshotRouter submits `SnapshotJob::Readiness` to the selected
SnapshotWorker. Native work inspects validated provider limits and loads/verifies
the existing pin without publishing, reconciling retention or changing the WAL.
It consumes bulk capacity, preserving the worker's control/recovery reserve.
Original visit, effect lease and exact worker ticket remain live until completion;
another group can run while the result is withheld. Stale completions cannot
release those credits. Closing admission/provider drains accepted work normally.

After exact completion, the owner checks the captured prefix, applied boundary,
schema and a bounded live checkpoint/restore using the shared readiness checks.
Its LogLimits and durable state come from the authoritative log binding established
at recovery and exact WAL completions; custom hosts must bind the core to the
selected provider's actual limits/state. The direct synchronous helper additionally
reads and compares LogStore state/limits. Snapshot limits returned by work must
fit the allowance reserved before submission. The owner reserves space for both
the loaded pin and live checkpoint; application cloning/scratch obeys the host
application contract.

Capability/application mismatch or applied lag returns a negative reply without
fencing. Storage failure or malformed accepted output fences without readiness.
No new durable token, watermark or generation is introduced: the request refers
to an existing contiguous durable committed prefix, and the worker only verifies
its existing pin. A positive reply is a non-Byzantine authenticated assertion,
not a ballot, read acknowledgement, activation or new durability certificate.

The leader retains at most one checked `ReadyLearner`, exposed by
`Raft::ready_learner`. A new round/cancellation clears that retained result;
term/configuration/commit advancement invalidates it. Promotion still checks
the current authenticated session at execution, including caller-held tokens.
Lost replies require host cancellation/retry; no automatic readiness retry loop
or service CLI administrator is provided here. NativeStartup still bootstraps
static groups; enrolled learners use the public member/learner assembly contracts.

Real loopback TCP/TLS and QUIC histories exercise compacted learners through
native framing, the native worker over a host-selected snapshot provider and the
original EffectOwner/SnapshotRouter. They cover denial, success, delayed/stale
completion, other-group progress, original send credits and shutdown drain.
Separate existing native-file tests cover compacted checkpoint/reopen verification.
These are not yet a remote online enrollment/promotion history. Readiness checks
application/log/snapshot capabilities; the online administrator must also validate
selected wire/transport capacities and placement policy before promotion.

## Fresh readiness before promotion

`Raft::begin_learner_readiness` captures a committed configuration and a
current-term committed prefix for an assigned exact-store learner. The host
supplies its current authenticated StoreSession and explicit application schema,
command-size and snapshot-size requirements. At most one request is pending per
group; `cancel_learner_readiness` releases it without provider work. Freshness
uses the existing RequestContext, including the leader StoreBinding. An
uncommitted assignment or active joint transition cannot start a round.

`verify_learner_readiness` runs on the serialized learner owner with the selected
LogStore, SnapshotRetention and bound CheckpointStateMachine. It checks the
authenticated leader, exact local identity/session, group/configuration/term,
committed matching prefix and applied boundary. Pending persistence or snapshot
work refuses verification, so Written cannot establish readiness. Selected log
state must equal the core's durable state. Provider limits must cover the requested
capacity and the application must expose the requested checkpoint schema.

For compacted state, the helper loads the exact pinned image and checks
application restoration at its boundary. It also creates a bounded in-memory
application checkpoint and validates restoration on a clone, without publishing
a new image or changing the live application. Provider/application allocation
and Clone behavior retain their existing host contracts. These potentially
blocking checks belong on host-controlled maintenance work with core/application
serialization. Provider faults follow existing owner fencing/recovery policy;
an error never produces a successful readiness assertion.

`accept_learner_readiness` checks the complete pending request and authenticated
learner binding before creating a `ReadyLearner`. Invalid replies leave the
request pending; success consumes it, rejecting duplicate or old replies.
`check_learner_readiness` revalidates immediately before promotion. Commitment
advancing beyond the captured prefix, term/configuration changes, fencing or a
changed learner session requires a new round. Pending requests are conservatively
canceled when a valid staged update changes term/configuration or advances
commitment; restart retains none.

The receipt is constructible for host wire adapters, like durability completions.
It is trusted under the authenticated non-Byzantine provider/peer contract, not
as a cryptographic certificate. This implements the host-driven exchange, not
native RPC encoding, asynchronous Node maintenance integration or an online
administrator endpoint. A token is not a vote, replication acknowledgement,
membership commitment or service activation. Placement/failure-domain policy
remains a separate administration obligation. No new durable effect, watermark
or generation is added: the prefix is an existing contiguous committed boundary.

Five downstream histories exercise actual core election/replication, application
lag, pending/Written state, capability rejection, replay, changed bindings,
commit/term/configuration invalidation, missing compacted data and native file
checkpoint/reopen under a fresh session. Initial assignments are explicitly
host-imported; these tests do not prove online assignment.
