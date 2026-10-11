# Schema312 proposal — bounded account-transfer application

Read-only proposal; no production edit/build. Source contracts:
`src/application.rs:23–51,86–156,210–250` (admission/application/checkpoint/read),
`src/runtime/clients.rs:19–36,505–598` (original command/result correlation),
`src/runtime/node.rs:30–44,483–565` (one application type and public Node intake),
`src/maintenance.rs` and `src/maintenance/{state,codec}.rs` (nearby concrete
role-aware records, clone-then-publish application, bounded framing/restore).
`tests/transaction_model/protocol.rs:256–301,303–355` supplies the proposed
prepare/abort/application/release semantics; it is not native implementation.

## Concrete API shape

Use one concrete `TransferApplication` with private Coordinator/Participant state
variants and checked role-specific constructors, not a generic wrapper. Node's
applications map uses one A type, so this allows a coordinator and two participant
groups in the same existing public assembly. Common fixed-size Receipt, Query and
ReadResult types avoid a second result router or application abstraction.

- `coordinator(group, profile, limits)` and
  `participant(group, profile, initial_accounts, limits)` return checked results.
  Profile binds one coordinator and two distinct participant GroupIdentity values;
  every identity includes its incarnation. Each participant has a fixed account
  set with checked nonzero AccountId and bounded nonnegative balances. No account
  migration/split/creation API is needed for this first static-ownership slice.
- `TransactionId` is a checked distinct semantic wrapper over a nonzero ID.
  `TransferSpec` is immutable: ID, coordinator, two account/group references,
  positive amount, debit/credit role and original PhaseIds. PhaseIds contain Start,
  Decide, AppliedAll, Finish and each participant's Prepare, ApplyDecision, Release
  OperationId (ten total). Validate distinctness within each destination group,
  cross-transaction aliasing in that group's retained/pending records, and equality
  of an outer ClientRequest OperationId with the exact phase's original ID.
  Canonical prepare order is by (GroupIdentity, AccountId), independent of debit
  direction; driver behavior must match the qualified protocol/model order.
- Public caller intake is immutable start/resume/status and account queries.
  Internal phase commands: Start(spec), Prepare(spec), Decide(spec,decision),
  ApplyDecision(spec,decision), AppliedAll(spec,decision), Release(spec,decision),
  Finish(spec,decision). Their phase IDs come from spec, never a new retry allocation.
  Trusted driver construction follows real exact Node Applied completions/fresh
  quorum queries, as reviewed in TRANSACTION_PROVENANCE.md. No public ready flags
  or foreign-certificate upload API. Stored provenance references describe those
  observations; decoding them is not independently proving another group's state.

Coordinator record: full spec, Started/Decided(Commit|Abort)/AppliedAll/Finished,
immutable original decision, retained semantic phase outcomes and first applied
positions. Participant record: full spec, prepare result (none/prepared/business
refused), Absent/Prepared/DecisionApplied/Released, prepared before/after balance,
immutable applied decision and retained outcomes/positions. Query transaction
status returns this complete fixed-size bound; Account query returns Balance or
Locked(transaction), never an intermediate balance while that account is locked.
Receipt has this entry's index and phase OperationId, semantic outcome and duplicate
flag. Duplicate receipt index follows the current committed replayed command;
retained first positions/results remain separate historical state.

## Transitions and refusal semantics

Prepare first binds/retains the participant transaction. It either locks its own
account and records a checked proposed balance, or durably records a business
refusal (locked, insufficient funds or credit overflow). It changes no balance.
A committed business refusal is a receipt outcome, not ApplicationError: the
latter fences generic application ownership. Unknown/overload/NotProposed is not
an observed business refusal and cannot substitute for it in coordinator Abort.

Only the real trusted driver's both-Prepared observations permit Decide Commit;
a real refusal permits Abort under the selected model. First decision is immutable.
ApplyDecision Commit requires the matching Prepared record and owned lock, changes
the balance once, and retains the lock. Abort is valid from Prepared, Refused or
Absent: create/retain a terminal abort tombstone even if Prepare never arrived,
change no balance, and retain any lock actually owned by this transaction until
release. An absent/refused abort never acquires or releases another transaction's
lock. A late Prepare cannot resurrect an aborted or released record.

AppliedAll follows actual both-participant DecisionApplied observations. Release
follows actual coordinator AppliedAll observation and removes only this transaction's
lock. Finish follows both real Released observations. Timeout/cancellation cannot
invent Abort or remove a prepare. Lost replies retry original IDs/content or query
actual replicated status; no compensation or volatile rollback substitutes.

## Capacity: reserve terminal progress, not only prepare

Smallest complete initial capacity profile: one bound coordinator retains at most
N distinct Started transactions over its configured lifetime. Both participants
have capacity for at least N complete transaction records, including all three
local phase IDs/outcomes and abort tombstones; no independent phase-level dedup
limit may strand an already accepted transaction. Driver assembly checks this
compatible envelope using concrete application's public limits/requirements.
Only this coordinator's actually Started transactions reach internal participant
intake. Start N+1 rejects before any participant work. Business refusal retains
its record; abort of an uncontacted participant has its reserved lifetime slot.
If multiple coordinators are later supported, reserve their aggregate capacity
explicitly; do not silently reuse this single-coordinator bound.

Do not evict finished records/old phase IDs in this slice. Capacity exhaustion is
an explicit lifetime refusal; retries and terminal phases remain available. For
admission, count distinct new transaction bindings in retained plus unapplied-
durable/queued/in-flight pending commands, reserving the whole record/phase
footprint. Validate operation/content conflicts across all pending commands.
`ProposalAdmission` receives that pending stream; no fresh capacity calculation
may ignore it. Limits bound account count, transactions, command bytes, receipt/
query results and worst-case checkpoint bytes with checked arithmetic/capacity
accounting. Driver's active/ticket/output budget remains separately bounded.

## Trait and encoding requirements

Implement StateMachine, BoundedStateMachine, ProposalAdmission,
ReadableStateMachine, BoundedReadableStateMachine and CheckpointStateMachine
on this concrete application. validate_group checks exact bound identity at
construction/execution/restore; validate_proposal_context checks relevant own-group
context but adds no frozen-membership gate. Safe P4 voter changes remain available:
committed phase truth/locks/dedup survive them, and fresh reads use current quorum
configuration. Supply truthful whole-lifetime deployment_requirements so existing
membership/readiness admission can validate the new command/snapshot envelope.

apply_batch uses a checked temporary next state and publishes only after the
entire contiguous batch succeeds. Advance over Noop/Configuration entries without
application receipts; emit exactly one receipt per Command in input order. Repeat
exact phase content returns its retained original semantic outcome; conflicting
reuse must not mutate original state. Structural/encoding failures preserve the
whole application and return ApplicationError; deterministic business/protocol
conflicts are explicitly bounded outcomes. Receipt/read accounting includes inline
storage plus any allocation capacity, not logical byte length.

Use a named versioned fixed-width little-endian command/checkpoint format with
explicit role tags, exact length, checked counts/IDs and unknown-tag/version refusal.
No serializer dependency, unbounded embedded proof vector or live external lookup
is necessary. Existing snapshot provider supplies its data integrity/publication
contract; inner decode still validates semantic state. Avoid exposing an encoder
for untrusted arbitrary internal phase claims as a business API.

## Checkpoint and restore acceptance

Checkpoint includes magic/schema, exact role/group/profile, enforced capacities,
applied boundary, full account/lock table, every retained complete spec, phase IDs,
immutable decisions, prepared before/after values, original phase outcomes and
positions. Prepared records and unprepared abort tombstones are equally retained.
Restore checks schema/role/group/incarnation/profile/capacity equality and external
applied_index, total/remaining lengths before allocation, sorted unique accounts/
transactions/IDs, known account membership, positive amounts, role/participant
binding, distinct phase IDs and phase-operation aliases. Reject trailing data.

Validate phase/outcome/decision/position consistency; nonzero existing phase
positions lie within the applied boundary, absent phases have canonical empty
positions, and decisions never conflict. Locks correspond exactly to that account's
Prepared or prepared DecisionApplied record; Released/unprepared abort records
own no lock. Prepared arithmetic must match amount/direction; a currently locked
account equals its before balance until Commit application and its after balance
through committed DecisionApplied. Do not compare released historical balance
values to today's account balance after subsequent legitimate transfers.

Restore into temporary state and publish only after every relationship validates.
Fresh driver recovery reads actual coordinator/participants via Node quorum queries,
then resumes original phases; it never restores volatile observation masks or
assumes absence means noncommitment. Native acceptance must cover refusal+absent
Abort, delayed late Prepare after tombstone, capacity-at-N terminal completion,
queued capacity conflicts, duplicate/content conflict, membership snapshot/suffix
recovery and malformed restore leaving the original application intact.
