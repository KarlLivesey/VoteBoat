# Proposed315 deterministic backing application and bridge contract

Plan only. No compilation input, production/test/shared-document source or Git
changed; no build/test executed for this review. P8 functional work, following
314's independent model gate. Models do not qualify this runtime. Fixed one-level
native315 does not complete supported recursion/layer activation316.

## Selected smallest complete path

Three logical parent voters, each stored by an ordinary three-physical-voter child
Node cohort. One deterministic `LogicalApplication` owns one mapping and one parent
replica image. Ordinary child WAL/snapshot workers remain unchanged and authoritative.
The parent uses generic public PersistenceWorker/SnapshotWorker implementations
connected to one explicit bounded host-owned bridge. No parent WAL, synchronous
network barrier, hidden executor, timeout promise or mandatory ancestor write.

First finish the application plus pure selected formats; then the actual child
pump/parent workers plus verified startup view; then construction-bound protocol
publication/ingress and native/public-host acceptance. Application-only completion
must not be called usable logical-voter315. These are linked implementation parts
of the already planned native deliverable, not optional new research prerequisites.

## Public types and constructor

Proposed module `logical_voter` (names settle before dependent tests):

- `LogicalStoreMapping { parent: GroupIdentity, voter: NodeId, store:
  StoreIdentity, child: GroupIdentity }`, checked constructor. Logical voter/store
  and physical child identities are distinct; whole-cohort construction checks
  the parent Bootstrap voter-store map and distinct mappings. A single application
  rejects another mapping/incarnation; it cannot certify the whole DAG itself.
- `RepresentativeEpoch` checked nonzero u64; `RepresentativeBinding { epoch,
  physical: PeerIdentity, physical_session: StoreSession, logical_session:
  StoreSession }`. The physical store must be the
  child's exact voter assignment. Epoch must advance exactly once; overflow refuses.
  Parent StoreBinding uses the logical store and logical_session, advanced
  monotonically in the committed Claim even when a new physical store has a lower
  physical_session; physical authenticated binding remains separately recorded/enveloped.
- `LogicalApplicationLimits { parent_log: LogLimits, parent_snapshots:
  SnapshotLimits, max_command_bytes, max_actions, max_retained_action_bytes,
  control_actions, control_action_bytes, max_capsule_bytes,
  max_parent_checkpoint_bytes, max_child_checkpoint_bytes }`, validated and encoded
  as immutable configuration. Parent LogLimits.max_groups is1. Aggregate retained
  action bytes, not max_actions times every maximal request, enforce lifetime
  payload retention; both count and bytes reserve a named control/recovery portion.
  Retry records are never silently evicted. Full capacity means explicit refusal,
  including after terminal work; pure quorum reads allocate no new action record.
- `LogicalApplication<C,S,W>` selected pure `LogCodec + Clone`, `SnapshotCodec +
  Clone`, `WireCodec + Clone`; native alias selects existing native formats. Format
  versions/limits are fixed constructor metadata, not live mutable adapters. Public
  checked `new(mapping, child_bootstrap, parent_bootstrap, limits, codecs)` returns
  all original owned inputs on refusal. It validates both bootstraps through
  log::apply_batch(Create), voter/store mappings, nonaliasing group incarnations,
  scalar overflow and complete nested command/checkpoint envelopes. The initial
  authoritative parent image is absent until the first original Claim applies;
  binding a bootstrap in construction is authorization/configuration, not a
  fabricated child durability receipt.
- `LogicalReceipt { index, operation, first_index, outcome, parent_revision:
  Option<LogRevision>, parent_generation: Option<LogGeneration>, epoch:
  Option<RepresentativeEpoch> }`: fixed-size receipt, current child invocation
  position distinct from retained original child first_index. No full image in
  every receipt. `LogicalOutcome` distinguishes Claimed/Mutated/SnapshotPublished/
  SnapshotReconciled/CapsuleAuthorized and expected semantic refusal (stale epoch,
  stale image, incompatible mapping/content, missing pin/bytes). Malformed codec,
  index gaps and incompatible checkpoint remain ApplicationError, atomically.
- `LogicalQuery::{Image, Outcome(OperationId), Snapshot(SnapshotRef), Metadata}`;
  `LogicalRead::{Image(Box<LogicalImage>), Outcome(Option<LogicalActionStatus>),
  Snapshot(Box<Snapshot>), Metadata(..), Uninitialized}`. Actual vector/boxed
  capacities are charged; query/result bounds are stable maxima from configuration.
  `LogicalImage` includes exact mapping, representative, GroupLog, child applied
  membership/boundary and parent published/pinned snapshot metadata. A read value
  has no opaque verified authority until the bridge consumes its original child
  ReadCompletion/ReadBarrier. A local getter may expose metadata for assembly only.

All six application/admission/read/checkpoint traits are implemented. Scalar
position/term fields and exact bindings are exported for correlation, never as
caller-set Ready flags or replacement quorum certificates.

## Commands and durable state transitions

Outer `LogicalCommand` binds schema, mapping, original OperationId, representative
binding and immutable origin child ConfigurationId. The Node ClientRequest
operation must match. Command construction that asserts parent effect/result
provenance stays crate-private to the bridge; raw protocol bytes are trusted-host
intake, not business requests or cryptographic certificates. Public application
configuration/read types do not permit construction of a logical release permit.

Retain canonical original bytes, first child index/term and scalar semantic outcome
for each action. Same ID/same bytes returns the original outcome and first_index
without mutation. Same ID/different bytes is OperationConflict, including queued
and accepted-but-unapplied commands. A valid retained historical duplicate is an
observation retry, not new representative work: later membership/fences do not
retroactively alter its outcome or authorize a different context.

1. **Claim { expected_epoch: Option<Epoch>, next: RepresentativeBinding }**:
   initial None->epoch1 initializes the configured parent GroupLog through the
   existing Create validator; later exact epochN->N+1 only replaces representative
   ownership. Preserve parent term/vote/ballot origin/log/configuration/generation/
   snapshot pins and retries. No empty fallback, guessed highest revision or erased
   ballot. The bridge must consume Applied and then a new child quorum Image read
   before enabling a successor parent owner; claim receipt alone is not reconstruction.
2. **Mutate { expected_generation, update: LogUpdate, origin: PersistOrigin }**:
   exact current epoch/session, group/revision/generation and immutable original
   WorkerTicket/VisitTicket content. Apply `log::apply_batch` with one Update to a
   temporary one-group map. It supplies the only parent transition validator for
   term/vote, contiguous suffix, ballot origin, snapshot/configuration base, accepted
   membership, revision and suffix generation. Snapshot updates additionally require
   this application's retained exact full pinned image. Child index is provenance;
   never substitute it for a parent commit/application/snapshot index.
3. **Publish { expected_parent_revision, expected_durable_anchor,
   original_snapshot_work, snapshot: encoded full Snapshot }**:
   decode using selected SnapshotCodec, validate SnapshotMetadata and exact logical
   store/group/schema/context. Derive immutable SnapshotRef from full encoded bytes,
   named native checksum/file length and monotonic checked snapshot generation.
   First retain/pin full bytes, then authorize Published after actual child Applied.
   At most two durable pins cover an anchor switch; retain the old pin and new image
   until the exact parent anchor Update applies. Reject a third unresolved switch
   or oversized content before ownership transfer. SnapshotStore publication is not
   itself log compaction; no hash/reference-only publication.
4. **Reconcile { reference, expected_parent_revision, original_snapshot_work }**:
   exact current authoritative GroupLog.snapshot must match a retained full image.
   Release obsolete pins only after that parent mutation's original child Applied
   authority. A stale Reconcile cannot discard a still-referenced old pin. Loads
   return the requested retained full image and original opaque work; missing bytes
   are unavailable/corrupt, never reconstructed from another logical leaf.
5. **Authorize { expected_parent_revision, expected_generation, origin,
   capsule }**: compare current representative, parent hard term, effective accepted
   parent membership/request context and exact referenced prefix/payload/checkpoint.
   Capsule carries immutable parent/logical/child scope, original EffectTicket or
   service invocation ticket, requester StoreBinding/session/sequence, response
   kind, separate canonical local parent ConfigurationId and complete bounded content.
   Message.configuration is the requester's echoed scope (raft.rs:125), so it is
   checked against the retained original request, not blindly equated to the
   responder's current accepted local configuration. Old-epoch new capsule refuses. Historical
   already-applied capsules survive claim/restart for their SAME original request.
   Mutation original identity/outcome prerequisites bind exact first indices; an
   arbitrary largest child index is insufficient.

`CapsuleBody::Protocol(Message)` uses the existing selected WireCodec under the
logical inner WireScope. Successful Voted requires the canonical ballot; Vote
request candidate last-term/index must match its own canonical parent prefix;
Append payload/previous term/index/commit and Appended matching index must match
canonical accepted state; ReadAck binds the actual current term/configuration and
fresh original request context; Snapshot/acks require retained full matching image.
Other supported Rpc variants get explicit checks/refusals before release; they
must not silently fall through to unconditional authorization. Static315 uses the
ordinary election/append/read/snapshot path; membership/control codecs remain
preserved and316 must qualify their actual logical runtime use.

`CapsuleBody::ClientApplied` binds the original ClientTicket, exact committed
parent operation/content/position and bounded serialized original receipt;
`ReadResult` binds original ReadInvocationTicket, original barrier fields and
bounded original query/result bytes. The driver consumes actual opaque parent
application/read outputs; a scalar historical status cannot manufacture either.
The deterministic backing application checks retained log/barrier/context/content
bindings; generic application-result semantics remain a trusted provider contract,
not re-proved by comparing bytes. The capsule encoder never substitutes a newly
constructed value for the original owned receipt/result. No foreign-success flag
or public untrusted foreign receipt intake is proposed.

## Child eligibility versus parent accepted policy

At intake and immediately before execution, `validate_proposal_context` compares
core's exact child group/bootstrap and `core.membership()` latest accepted voter
assignment with the command's exact physical representative node/store. Immutable
origin ConfigurationId records where the original bytes were first assembled;
it is NOT required to equal today's accepted ConfigurationId. Configuration
changes or rollback do not refresh the command's bytes or operation ID.
Learners/replaced stores refuse even if the application has only applied an older
configuration. It grants no quorum/authentication authority. The bridge obtains
this scalar construction context from the actual selected core, never from callers.

The application separately retains the child's committed ordered Membership.
For each child EntryPayload::Configuration, use existing crate-private
Membership::preview_next(entry.index, record, entry.index-1), or equivalent existing
replay_from with exact prior base, then advance the child applied boundary. These
entries are CHILD protocol metadata, never parent mutations/application receipts.
First semantic command replay validates the representative's eligibility under
the membership preceding its own ordered child position; it does not require that
position's ConfigurationId to equal the immutable origin ConfigurationId. Retained
exact duplicates resolve the original recorded outcome before current eligibility
checks, and never execute a second mutation.
Do not consult a later current core configuration to erase historical committed
promises. Checkpoint includes child bootstrap/Membership/applied boundary. Accepted
child rollback is handled by actual core admission; it does not retroactively
rewrite committed application metadata.

Parent accepted-policy timing is supplied only by GroupLog.membership() and
apply_batch: accepted Joint/Final entry changes the parent local predicate, joint
commit still needs both sides, final follows committed joint, rollback recomputes
from surviving suffix/snapshot base. Do not replace that with parent application
execution, child configuration, diagnostic liveness or one parent's new-only votes.

## Actual reusable formats and core-only exposure

Source APIs already exist:

- src/log.rs:318 `log::apply_batch`; GroupLog:76, LogUpdate:182,
  GroupLog.membership:112 and membership_at:123. No substitute parent validator.
- src/native/log_store.rs:60 public `LogCodec`, NativeLogCodec:103;
  encode/decode_batch:1106ff; public checkpoint methods:1116ff preserve full
  GroupLog including historical BallotOrigin. Normal format2 (`VBLOG002`),
  checkpoints `VBLCPT01`/`VBLCPT02`; decode must consume the exact returned size.
  Membership encode/decode exists crate-private:1082/1093.
- src/native/snapshot_store.rs:167 public SnapshotCodec; NativeSnapshotCodec:253
  prefix/finish/decode retains complete Snapshot metadata/application and membership.
  Compose prefix + exact application bytes + finish; reuse its decode and exact
  checksum/reference rules. No new snapshot format or copied membership codec.
- src/wire.rs:130 public WireCodec; src/native/wire.rs:40 NativeWireCodec provides
  pure Message encode/decode under explicit WireScope/limits/version.

Concrete core-only obstacle: LogCodec/SnapshotCodec and selected pure native formats
are currently reachable only through cfg(native), although their encoding depends
only on core identities/log/membership/policy, std containers and the existing pure
CRC32C functions. LogicalApplication must not acquire a native WAL/IO bound just to
encode. Recommended focused relocation: expose these same pure traits/formats in
unconditionally available shared format modules and re-export original native paths
for compatibility. Move the exact bounded Encoder/Decoder, checkpoint helper and
pure checksum helper with them; retain byte-identical formats and existing tests.
Native JournalIo/FileLogStore/SnapshotIo/file locks/workers remain native-only.
WireCodec is already core-only; NativeWireCodec's pure format implementation can
follow the same move if needed for a default core-only selected alias. Do not invent
another codec/framework, depend on files in the application, or define a useless
core-only alias without a working selected encoding. Exact file ownership for this
focused extraction must be assigned by root before source editing.

Outer application commands/checkpoints need a NEW named bounded framing schema
(`VBLOGAPP01` command, `VBLOGACP01` checkpoint proposed), version1 and checked tagged
scalars/count/lengths. Embed existing codec frames, not duplicate their fields.
Checkpoint encodes exact immutable constructor configuration/formats/mapping,
child Membership/applied boundary, optional parent GroupLog via existing checkpoint
codec, fence, all retry bytes/outcomes/first positions, publication metadata/full
snapshot bytes and pins. Restore into temporary state: decode exact framing,
validate all count/byte/configuration limits, first-position uniqueness/in-boundary,
command/outcome bindings, child metadata history, full parent image via codec and
apply_batch validation, snapshot match/pin closure and fence monotonicity; compare
reconstructed retained transitions where needed. Swap only after complete success.
Malformed command/batch/checkpoint leaves the original state intact. Every declared
command/child checkpoint ceiling uses checked lifetime bounds, includes complete
retry/action-byte pools plus both maximal pins, and must fit child log, snapshot,
wire, router and reserved-recovery envelopes before workers open.

## Bridge/startup and concrete runtime API obstacles

- One explicit `LogicalChildBridge` owns borrowed homogeneous generic child Nodes,
  immutable mapping/owner-generation and bounded input/output/held-value slots.
  Independent queues expose LogicalPersistenceWorker and LogicalSnapshotWorker
  implementing the existing public contracts. submit transfers original values
  only on success; pump(now, typed budget) performs actual Node propose/read/poll/
  complete. Retain node slot, ticket, op, full bytes/query, parent original scope and
  first index. Duplicate Applied receipt's current index never changes dependent
  command content: use retained first_index/status. Unknown observation retains
  uncertainty until an original quorum Outcome query resolves it.
- Written then Durable may both conservatively wait for exact original child
  Applied or fresh quorum resolution of the same retained action. Preserve Worker-
  /Visit-/LogTicket bindings and revision/generation/last index; no admission-only
  success. Snapshot Published requires exact retained full bytes/pin; Reconciled
  requires exact durable parent anchor. Close stops intake, pumps accepted original
  work and returns/consumes exact ownership; is_drained includes actual child
  clients/reads and held outputs, not just an empty bridge attempt table. Joining
  child workers remains host-owned. No timeout abort/replacement completion.
- Startup needs an opaque `VerifiedLogicalImage` constructor accessible only after
  consuming an exact child quorum Image result and validating bindings/boundary.
  Its read-only LogStore/SnapshotRetention view supplies GroupLog/fetch_range and
  exact pinned images for existing parent recover/member/compacted APIs.
  append_batch/barrier/reclaim/mutation methods refuse. Pure decode or a public
  LogicalImage does not construct this authority. Recover own child checkpoint+
  synchronized suffix and establish ordinary child current-term quorum first;
  unavailable/missing bytes cannot bootstrap an empty parent.
- src/runtime/replica.rs::retry_send and retry_read currently publish directly;
  persistence substitution alone is known unsafe. src/runtime/effects.rs:797
  release_transferred_send, src/runtime/clients.rs:494 deliver and
  src/runtime/read_requests.rs:512 execute are public bypass points. Logical binding
  must be checked BELOW a convenience driver, with a privately constructed exact
  consumed-capsule `LogicalReleasePermit`. Plain physical calls reject logical
  success without it, while retaining existing physical-mode behavior. Cancellation
  and failures may consume ownership without publication. Separate read capture
  from publication: execute currently consumes and publishes the original read in
  one call; putting a gate after Node::poll_read is already too late.
- Opaque ApplicationResults/ReadResults cannot be recreated. Add only the bounded
  `LogicalResponseCodec<A>` required to encode the borrowed original Query/Receipt/
  ReadResult with a fixed nonzero schema and enforce configured byte maxima. Encode
  queries while still borrowed before consuming them; hold original result/receipt
  and router charges through pending capsule. Native Counter and a downstream
  application implement the SAME contract; generic callers cannot fake permits by
  providing arbitrary encoded bytes or constructing a physical owner.
- WireScope matches physical Message.from/sender today. Logical envelope must retain
  actual TLS/QUIC physical session scope plus separately checked logical inner
  sender/store/epoch/capsule/original request. Explicit negotiated new capability;
  preserve existing normal codecs. Do not rewrite Message.from under unchanged
  physical scope checks or count representatives as extra voters.

## Acceptance and ownership for implementation

Application tests: initial claim/new claim, preserved vote/ballot origin/suffix;
actual existing apply_batch legal/illegal mutation behavior; exact operation retry
and conflicting pending content; capacity/control reserve at the configured bound;
current vs first child indices; old-epoch capsule refusal and historical duplicate;
full-payload publication before anchor, two pins/stale Reconcile, missing bytes;
actual child Configuration replay and latest-accepted core context before commit;
whole-batch atomic failure; all truncations/unknown format/incompatible constructor
restore; old child application checkpoint + genuine committed command suffix;
capacity-costed immutable reads and truthful lifetime deployment envelopes.

Bridge/runtime/native acceptance remains the existing315 list: real child tickets
and quorum reads, parent election/write/read/retry, child quorum loss/repair,
representative restart and cached-response fencing, old parent AND child checkpoint
with real suffix, simultaneous whole-child recovery, exact stale/wrong scope
completion, direct public publication bypass refusal, downstream selected-provider
composition, original output cleanup and real worker joins, TCP/TLS + QUIC on
Linux/macOS. Format and all four strict all-target Clippy profiles remain zero.
No same-target/debug executable races, weaker deadlines/durability, performance
measurements or security scope additions are proposed.


## Focused configuration/retry refinement (before API freeze)

The actual application hook receives `(&Raft, operation, bytes)` only; it does not
receive a one-use admission certificate. ClientRouter calls it both in check_submit
(clients.rs:249) and again immediately before the original Propose executes
(clients.rs:429). The application cannot infer whether some unknown byte string
was queued under a prior configuration from that hook, and must not invent such
an authority. The owning bridge retains the first real command/operation/Node
intake context; serialized origin ConfigurationId is immutable provenance, not an
eligibility lease. Current eligibility always comes from the actual core, and
ordered committed eligibility always comes from replay, not from this field.

The exact chosen rule for `validate_proposal_context` is:

1. Decode/bound/check exact mapping/group/bootstrap and outer OperationId. If an
   already-applied retained operation has identical original bytes, admit only an
   observation retry for its original semantic outcome/first_index; no new state,
   request, permit or slot. Conflicting retained reuse refuses.
2. For a nonretained action, require its physical node/store to remain a VOTER in
   core.membership(), the latest accepted predicate, and exact current expected
   epoch/session/revision/generation prerequisites as applicable. Initial/next
   Claim checks the proposed new representative instead. An unchanged voter
   remains eligible through stable/joint/final changes and suffix rollback;
   learner/replaced/removed assignments refuse new semantic work. Never use
   `core.local_voter()` as a substitute for the command's possibly remote actor.
3. Do not compare origin ConfigurationId for equality, monotonic ordering or
   current authorization. Even a lower accepted configuration after valid suffix
   rollback must not require modifying original bytes. The field was captured
   once by the trusted bridge from the actual core, is checked nonzero and part
   of immutable retry content, and grants no authority. A stale origin field
   cannot make an ineligible actor eligible. Actual preceding configuration at
   first committed application is recorded with the retained action outcome.
4. Pending-aware validate_proposal checks identical bytes/ID and count/byte
   reservations across retained, accepted-but-unapplied log and router commands.
   Seeing a pending duplicate cannot waive latest accepted eligibility for a new
   attempt. Existing accepted log work is allowed to reach committed application
   in original order; replay does not rerun today's intake hook.

`apply_batch` walks actual child committed positions atomically. A Configuration
entry first advances the canonical committed child Membership via the existing
validator. A FIRST original action then checks actor eligibility against that
preceding ordered Membership and current fence/image, regardless of its old
origin ConfigurationId. A command which really preceded a later removal remains
valid history and may apply before the later Configuration entry. A newly applied
old-epoch/ineligible action after a fence/removal returns a retained semantic
refusal, not a fabricated success or global application failure. An exact retained
historical duplicate after a removal returns its historical outcome; it cannot
reuse those bytes for a new parent request. Later current accepted state is never
used retroactively to erase a committed vote/mutation/capsule.

Four concrete ownership/recovery cases:

| Original work | Configuration/rollback event | Selected continuation |
| --- | --- | --- |
| Queued Propose, unchanged exact voter | Valid child change activates before execution | Pre-execution context check still allows the original bytes/ID; ordered application validates actual preceding policy. |
| Accepted original in surviving child suffix, reply lost | New accepted policy or child leader change | Poll/consume the ORIGINAL opaque Node output; if Unknown, resolve exact retained original outcome through fresh quorum query. Existing log order commits/applies the action or safely rolls it back; no bytes refresh. |
| Unknown original rolled out of suffix, unchanged voter | Rollback/current policy changes origin ConfigurationId | Retry SAME op/bytes in a fresh child invocation; retain old child attempt ownership through real router takeover/cleanup. Both copies, if retained, deduplicate to one original semantic result. No origin-ID equality blocks retry. |
| Queued/nonretained original, actor becomes removed/learner or epoch replaced | Pre-execution hook refuses or later exact retry refuses | No new old-actor semantic work. Consume NotProposed/Unknown and begin explicit fenced parent ownership recovery; do not strand a live parent Persist lease awaiting impossible old-epoch success. |

Unknown/cancelled/leadership-changed is NOT an abort, rollback or durability fact.
A fresh Outcome query may observe an applied original and return its exact retained
first_index/outcome. Its absence does NOT prove an owned/accepted action can never
commit: continue driving actual original child work, or retry exact bytes if still
eligible under the bounded attempt rule. Never fabricate a Refused/Applied outcome
from an empty diagnostic read. Actual NotProposed is terminal for that particular
Node invocation only; an earlier invocation of the same original action may still
be uncertain and requires exact resolution. Child quorum loss leaves the bridge
unavailable with original scopes/credits retained, not a timed-out successful parent.

If removal/fencing prevents resubmission and no original outcome is yet available,
close/fence the OLD parent phase producer and emit an original-scope worker Failed
with Uncertain reason (or consume/cancel a gated response without publication).
This is explicitly uncertain storage failure, not a certificate that the parent
mutation did not happen. Existing EffectOwner::deliver_worker only requires prior
Written for Durable, not for Failed (effects.rs:936); do not manufacture a Written
LogTicket just to fence a child action which never supplied parent evidence. Drain all accepted child actions/outputs/queries through
the actual Node routers; ClientRouter complete(Unknown) may release the observation
while its reservation survives until reconcile/takeover (clients.rs:616–665).
Bridge is_drained must include those real owned streams. All old parent worker/
visit/effect tickets remain exact; no receipt can release a fresh owner's credits.

Explicit replacement commits a fresh eligible Claim with fresh persisted logical
session, then consumes a fresh child quorum Image read at/after that Claim's applied
boundary. Any original child entry BEFORE Claim is necessarily included in its
applied prefix and retained outcomes; a late old-epoch action AFTER Claim refuses.
The successor reconstructs canonical parent promises/prefix/pins, creates a NEW
parent RuntimeOwner/worker generations and lets ordinary parent consensus/recovery
finish. It does not complete an old volatile lease under a new ticket or fabricate
parent/client success. Historical already-applied capsule packets may be sent only
for their exact original existing request and receiver live context; a resolved old
mutation never authorizes new old-epoch service outputs.

Static315 tests can cover immutable-origin-ID retry in bounded fixtures with
actual accepted child configuration/rollback through core/app contracts, or leave actual
layer lifecycle histories to316; the command semantics above are mandatory now.
316 must qualify surviving original before removal, rollback then same-ID retry,
queued pre-execution eligibility loss, removed actor Unknown->owned recovery,
and retained historical duplicate without a new request permit. No broad fault
Cartesian expansion or fresh-byte retry workaround is proposed.
