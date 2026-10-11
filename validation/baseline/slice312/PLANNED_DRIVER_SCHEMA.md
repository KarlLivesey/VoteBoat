# Proposed313: bounded Rust transfer driver and native embedding

Read-only plan. No production/shared-doc edits, builds or baseline302 changes.
312 application code is in flight, not yet qualified. The next implementation
must use its completed checked contracts; this file does not claim a native
transaction engine already exists.

## Smallest usable surface

Add one concrete transfer-protocol driver as a child of `transaction` (proposed
`src/transaction/driver.rs` and public re-export), using generic existing public
Node providers rather than a native-only production bound. A child uses the existing
private `Command`/codec without publishing an internal phase encoder. It is an
explicit Rust host composition, not a generic application/storage wrapper:

```rust
// Proposed shape: application semantics are concrete; providers stay composed.
struct TransferDriver<'a, S, T, E, W, O, H, C, F> {
    nodes: &'a mut [Node<S, T, E, TransferApplication, W, O, H, C, F>],
    // Bounded phase/invocation/output ownership; no storage or application wrapper.
}

// Proposed names; no implementation exists yet.
TransferDriver::new(profile, limits, nodes); // provider types inferred from Nodes
driver.submit(spec) -> Result<DriverTicket, SubmitRejected>; // returns spec on refusal
driver.resume(transaction_id) -> Result<DriverTicket, ResumeRejected>;
driver.advance(now: MonoTime, budget: DriverBudget) -> Result<DriverProgress, DriverError>;
driver.poll_completion() -> Option<DriverCompletion>;
driver.complete(completion) -> Result<DriverOutcome, CompletionRejected>;
driver.close();        // closes business intake, continues accepted protocol work
driver.stop_waiting(); // abandons observation without deciding/compensating
driver.is_drained();
```

The driver holds an exclusive lifetime borrow of a bounded slice of actual
generic Nodes. Its phase/ticket/result table is owned and bounded; Nodes, workers,
files and sockets remain owned by the embedding host. No business method accepts
an Applied enum, PhaseFact, ready flag, arbitrary output or coordinator decision.
Business input is checked immutable TransferSpec or a transaction ID to resume.
Account/status reads after dropping the drained driver use ordinary Node.read;
no new account protocol is necessary for the first example. DriverTicket and
DriverCompletion carry private invocation/generation identity and bounded output
credit, independently of the replicated transaction ID.

Use the actual public Node parameter bounds already required at
`src/runtime/node.rs:177–216`: S:ReadyScheduler, T:TimerService,
E:ElectionEntropy, W:PersistenceWorker, O:OutboundQueue, H:SnapshotWorker,
C:PeerConnector, F:PeerTransportFactory<C::Session>, and C::Endpoint:Clone for
the methods used. TransferApplication supplies Node's existing
ProposalAdmission/BoundedReadableStateMachine/CheckpointStateMachine and
ApplicationReceipt requirements. Neither provider associated errors nor a new
transaction storage API must leak into the surface: the used Node methods already
return their fixed client/read/Node errors.

This preserves scheduler, timers, persistence worker/log backend, snapshot worker,
outbound queue, connector and secure transport factory composition. Each selected
cohort has one homogeneous provider type tuple, just as an ordinary Node slice;
construction does not freeze the crate to that tuple or require native features.
Replacing a backend means assembling Node::from_parts with that provider and
instantiating the same inferred driver. Do not introduce a new application/log
wrapper, observation callback trait, trait object or provider conversion merely
to abbreviate the type parameters. No new seam is needed: propose/read/poll/
complete/cancel and metadata access are already on that generic Node impl.
Provider semantics remain trusted under those existing interfaces; business
callers still cannot supply observations through the driver.

The provider bounds above belong on the driver type/impl wherever Node's type
and methods require them; the abbreviated pseudocode omits that repetition.
This plan does not add a new lint exemption for the generic shape.

For the first native demo only, instantiate the driver on the existing alias
NativeNode<TransferApplication,NativeServiceConnector>. NativeNode is a public
type alias for the same generic Node, not a separate implementation or new
adapter. The same production matching/consumption code therefore consumes actual
native outputs, without another result-injection seam or native driver wrapper.
The first profile can bind exactly three live Node handles,
each hosting the same coordinator plus two participant groups. Leaders may differ
per group. A future general driver can widen this constructor deliberately; it
is not needed to complete this native embedding. Exclusive borrowing makes this
driver the sole client/read completion consumer for those handles while attached.
Construction requires empty client/read routers, Running Nodes and compatible
application metadata. Do not silently steal another subsystem's outstanding
completion. No opaque Node output is kept exported across an advance return.

## Construction and intake contracts

Actual312 checked types: TransferProfile binds one coordinator and two distinct
participants in canonical GroupIdentity order; TransferSpec binds transaction,
positive amount, debit/credit AccountRefs and ten per-destination original IDs.
See `src/transaction/types.rs` and `status.rs`. Applications expose
group/profile/role/limits/account_ids/contains_account metadata. Use those getters
for assembly and immutable account membership only, never phase authority.

Validate every bound application copy: exact group/incarnation/profile/role;
same coordinator lifetime N; participant provision at least N; consistent fixed
account ID sets per participant; compatible command/receipt/query/checkpoint
envelopes. No participant account creation/migration occurs in this profile.
submit rejects a missing debit/credit account or incompatible profile before
Start. Balance/funds/overflow/lock decisions still come from replicated Prepare,
not local metadata or cached balances. Safe membership may change voters normally.

Coordinator Start must reserve the entire original PhaseIds namespace by each
destination group against retained AND pending Start specs, including its six
participant IDs, not merely its four own IDs. Cross-group equal IDs are legal;
cross-transaction aliases within one destination are not. Otherwise an accepted
Start could prepare a peer and later be stranded by an Apply/Release alias. This
fits the existing single-coordinator retained-N scan; participant admission also
validates exact operation/content. Agent312 confirmed all-three-destination alias
checks and immutable account introspection; qualification remains its work.

Driver limits bound active transactions K<=N, node handles, stored specs, encoded
command capacity, pending attempts and retained business outputs. Start admissions
count distinct pending specs, not only committed local records. Persisted records
and all phase IDs are never evicted by driver slot release. Application capacity
at N still permits terminal work and exact retries; N+1 refuses before participant
work. One outstanding command OR query per active transaction is enough for the
first complete implementation; fair round-robin advancement avoids one stalled
transaction monopolizing the bounded driver. ReadInFlight/Overloaded is backpressure.

## Exact original ownership and content correlation

For each attempt retain handle slot, actual RuntimeOwner/store/session, full
ClientTicket or ReadInvocationTicket, transaction, expected step/destination,
original OperationId and exact encoded command/query. Poll completions directly
from the same owned Node; match the complete ticket before interpreting it, then
call that Node's complete_client/complete_read exactly once. On a mismatch or
completion rejection retain returned ownership, stop phase production and expose
an explicit driver-contract failure; no allegedly matching group/index supplies
readiness. Any valid unexpected result must eventually be consumed for cleanup,
without becoming a transaction fact.

Applied requires the actual consumed ClientOutcome::Applied. Independently check
position.index==receipt.index, original operation, expected semantic phase result,
and exact pending command identity. ProposalPosition or NodeProgress.proposed by
itself is insufficient. The actual ClientRouter already ties operation/content
and the opaque original application output to this local committed position.

312 now provides TransferReceipt.first_index: the canonical retained phase
position, distinct from this invocation's receipt.index/ProposalPosition when a
duplicate is applied. Use a nonzero semantically exact first_index to build a
private PhaseFact. A conflict with first_index zero supplies no fact. Preserve
canonical participant slots and full spec when encoding dependency assertions.
An Applied duplicate therefore can reconstruct identical bytes without a forced
extra quorum read. Without first_index, a fresh retained status query would be
required before encoding any dependent phase command; substituting the duplicate
invocation's current position changes same-ID bytes and breaks restart retries.

Fresh Status observations must come from the actual completed
ReadOutcome::Read { barrier, result: Ok(TransferRead::Status(...)) }. Match the
full read ticket/query and barrier.group/request; reject malformed or incompatible
spec, phase, decision or original operation. ReadBarrier is already core-created
and consumed through the Node's original read execution. Its configuration/term
describe that original read, not a cached lease or a new configuration freeze.
A status can include entries applied after the barrier's captured index; don't
invent a false rule requiring all returned historical positions <= barrier.index.
The actual application/read contracts validate the executed committed state.

TransferStatus retains full spec and canonical PhaseStatus index/op/outcome/facts/
refusal. Resume uses those original assertions for exact dependent-command bytes;
it never regenerates phase IDs or substitutes new observation positions. Local
TransferApplication.status, role==Leader and cached coordinator IDs are diagnostic
or routing hints only. The driver has no public observe_prepared/result injection.
Trusted embedding hosts can still deliberately bypass Node's byte intake; the
profile does not claim cryptographic foreign certificates or untrusted-host safety.

## Protocol progression and restart

1. submit proposes original Start to the configured coordinator. Only its exact
   Applied result or fresh retained Started-or-later status permits participant
   work. resume first quorum-reads the coordinator to recover the complete spec,
   original IDs and phase/decision. An absent status says no retained Start at that
   observation; it cannot prove an unknown old invocation will never commit. Ask
   the caller to resubmit its original immutable spec, without inventing Abort.
2. Started prepares participant0, then participant1 only after actual Prepared0,
   in canonical group/account order independent of debit direction. A real
   durable Refused result permits Abort and skips any unneeded later Prepare.
   Unknown/NotProposed/admission rejection/overload never substitutes Refused.
   Both actual Prepared facts permit Commit. Decide uses original coordinator ID,
   canonical prepare facts or the exact durable refusal and immutable decision.
3. After actual coordinator Decided, apply that exact decision to both participants
   using original apply IDs and canonical coordinator decision fact. Commit changes
   each balance once and retains locks. Abort changes no balance; an uncontacted
   participant receives a retained abort tombstone, so a late Prepare cannot revive
   it. If quorum status contradicts the durable immutable prerequisites, report
   inconsistency rather than manufacture missing Prepare/decision authority.
4. Actual both-participant DecisionApplied facts permit original AppliedAll at the
   coordinator. Only its real Applied completion/fresh status permits Release.
   Both actual Released facts permit original Finish. Business completion follows
   actual coordinator Finished with its original immutable decision. Sending two
   commands or observing two Written completions is never equivalent to AppliedAll.

Recovery resumes at the actual monotone phase. Decided does not need fresh Prepare
commands; AppliedAll continues Releases; Finished returns the retained terminal
outcome. Later retained phases also preserve the earlier canonical phase facts.
Drop volatile observation caches/tickets after host/Node restart, attach to newly
validated recovered Nodes and query current quorum status. Reuse original IDs and
exact original proof assertions from retained status. Different native store
sessions/router generations reject old tickets; participant group incarnations
never redirect an old transaction. Ordinary P4 voter changes retain these records,
locks and dedup through snapshots/suffix replay. New status reads use effective
quorum configuration; missing ConfigurationId in Applied is not a membership ban.

ClientRejected owns an unadmitted request. NotProposed applies only to its exact
invocation before proposal. Unknown can have committed/applied: resolve via fresh
status or exact retry. Application admission/structural/provider failure is an
error, not a replicated business Abort. Each stalled transaction remains retained
or reports an explicitly unresolved observation; timeouts never choose its decision.
Only actual committed business refusal drives protocol Abort.

Locked account queries remain Locked from prepare until their own Release after
AppliedAll. No early compensation/unlock follows a lost reply, canceled wait or
driver disappearance. Across-group fresh reads are not a global snapshot; this
composition relies on monotone facts and retained locks, without claiming a new
atomic multi-account read API.

## Cleanup and narrow native acceptance

close stops new business intake but advances already owned transfers. stop_waiting
ceases new phase production, cancels exact Node client/read waits, consumes every
terminal output and preserves unresolved transaction identity. Canceled client
credit can remain until actual owner/log reconciliation; don't equate Unknown
delivery with router drain. Driver is drained only when its tickets/outputs and
exclusive Nodes' client/read ownership have really drained. No blocking Drop or
implicit unlock/rollback. Then drop the borrow, begin_shutdown actual Nodes, poll
real selected-provider drain and reclaim/release its actual parts. The embedding
host retains the selected worker/executor join contract; native demo cleanup uses
the existing native helpers to join WAL/snapshot/socket owners. On unavailable
quorum preserve files/locks for resume. Reattaching
to still-live handles requires the old routers drained; cold Node recovery starts
new empty invocation generations.

Concrete first embedding: `examples/embedded_transfer.rs`, using three
NativeMultiStartup replicas with actual TCP/TLS or QUIC, one coordinator and two
participant bootstraps per replica, role-specific TransferApplication map, real
shared native WAL per physical replica and per-group snapshots. Driver lifetime
borrows those actual Nodes. Existing TLS fixtures/configuration and timing profiles
are enough. Explicit create/recover CLI selects an immutable transaction/spec;
recover resumes from retained coordinator spec. No fake local message engine,
global coordinator singleton, new remote admin protocol or certificate service.

Put required native histories in a focused transaction driver target/module and
reuse startup/multi construction/drain patterns. Exercise: complete commit with
distinct group leaders, reversed debit direction/canonical prepare order, Locked
reads, durable business Abort plus absent-participant tombstone, same-ID retries,
capacity-at-N finish/N+1 refusal, and driver credit/backpressure cleanup. One
controlled phase-loss/recovery history should cover lost Prepare observation,
leader replacement, lost Decided observation, one participant applied, actual
checkpoint plus committed suffix cold reopen, AppliedAll/one Release loss and final
Finished retry. Assert exact original bytes/first indices after each resume.
Required provenance controls use actual wrong-group/operation/original ticket
outputs, stale restarted owner tickets and malformed/conflicting spec; no fact
enters through a public caller claim. Tests retain prepared locks when quorum is
unavailable and cancellation abandons observation. Campaign/checkpoint or failure
injection occurs between drained driver borrow scopes (or private test hooks),
using actual public Node control/recovery; no synthetic authority.

Run affected controls on Linux and Mac with TCP and QUIC, then formatting and four
strict feature profiles in separate/sequential executable caches. This qualifies
the usable fixed-ownership native transfer path; it does not claim all broader
transaction variants, logical voters or global liveness. No performance/security
project or baseline302 rerun is a prerequisite for this driver slice.

## Inspected source seams

- `src/runtime/node.rs:483,508,540,543,549,552,558,561,799,831,1094`:
  propose/read, opaque completion consumption, cancellation, poll/drain/reclaim.
- `src/runtime/node.rs:177–216`, `src/runtime.rs:67,142,176`,
  `src/connect.rs:146`, `src/transport.rs:27`, `src/worker.rs:161`,
  `src/outbound.rs:273`, `src/snapshot_worker.rs:146`: existing generic Node
  and provider bounds; no additional native-only bound or observation seam.
- `src/runtime/clients.rs:19,25,92,107,283,505,599,680`: complete original
  owner/group/op/content correlation; cancel loses observation and credit persists.
- `src/runtime/read_requests.rs:22,100,110,485,510,692,701` and
  `src/raft.rs:196,204`: scoped original fresh read invocation/barrier execution.
- `src/runtime.rs:114`: ProposalPosition is not independently applied evidence.
- `src/native/node.rs:29`, `src/native/startup/multi.rs:10,14,25,94`:
  actual concrete Node alias and checked shared native group inventory/open.
- `tests/startup/multi.rs:8,38,64,104,132,157,190,204`:
  real three-replica/three-group create, independent leaders, WAL/checkpoints,
  recovery and owned shutdown. `examples/embedded_counter.rs` demonstrates
  downstream exact poll/complete/read/create/recover/drain contracts.
- `src/transaction/{types,status,command}.rs` and `src/transaction.rs:44`:
  in-flight312 checked immutable profile, canonical first positions/phase facts,
  private protocol commands and public immutable application metadata.
