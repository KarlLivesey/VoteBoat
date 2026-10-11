# P8 transaction provenance — existing Node embedding first

Read-only review. No source/docs edits, builds, tests or Git operations.

## Concrete unsafe point

A replicated `Commit`, `DecisionApplied`, `AppliedAll` or `Release` command is
unsafe if admission treats caller-supplied readiness booleans, bare indices,
application receipts or arbitrary structs as proof of another group's state.
Replication makes the assertion durable; it does not make it true. In particular
an ApplicationRouter receipt applies to its original local committed effect,
not to another participant. Existing mechanisms contain no cross-group signed
certificate or arbitrary remote transaction observation contract.

The smallest complete fix is a trusted, non-Byzantine host transaction driver
that itself consumes original Node outputs/queries and constructs internal phase
commands only after the necessary scoped observations. Expose start/resume/status
and ordinary account operations to callers; do not expose a public
`observe_prepared(claim)` / `ready=true` / certificate-upload transition API.
Keep phase production out of business/untrusted command intake. No new network
wrapper, signatures or credentials are needed for this first Rust embedding.

## What actual outputs prove

`src/runtime/clients.rs:19–36,92–114` and `src/runtime.rs:114–118`:
ClientTicket binds group incarnation, immutable OperationId, router generation
and RuntimeOwner (including StoreBinding/session). ClientOutcome::Applied returns
ProposalPosition { index, term } and the application's receipt. There is no
standalone AppliedReceipt struct that also authenticates group/store/configuration.
The position by itself explicitly is not application/durability evidence.

`src/runtime/clients.rs:505–598,680–699`: ClientRouter correlates the original
committed position, exact operation and command content (while retained), verifies
receipt index/operation/bounds, consumes the opaque original application output,
and publishes Applied. Node::complete_client consumes the original private-field
completion from that router once. These checks establish one selected trusted
application's local applied command, not its foreign-dependency assertions.
ApplicationReceipt itself exposes only index, operation and allocation accounting
(`src/application.rs:138–142`); its application-specific phase/content result is
part of the new transaction application contract.

`src/runtime/read_requests.rs:22–29,110–134,510–594,701–721`:
ReadInvocationTicket binds owner/store session, generation, group and original
request. ReadOutcome::Read contains the core-created ReadBarrier and application
result. Barrier accessors expose group/configuration/term/request/index, not a
transferable lease. `src/raft.rs:795–818` checks the original live ready barrier,
current leader/term/configuration/origin, committed boundary and application
catch-up, then consumes it. Node has already executed that one authorized query
when it returns the completion. A canceled/unavailable/rejected read, arbitrary
local status, cached leader ID or copied barrier is not a new observation.

Node's public propose/read/poll/complete methods exist at
`src/runtime/node.rs:483–565`. They are sufficient for a host-owned driver. The
lower-level ApplicationRouter need not become the transaction-driver intake.

## Minimal trusted capability and collection rules

Construction binds one coordinator GroupIdentity and exactly two participant
GroupIdentity values, supported application schemas, static ownership profile,
allowed native/public Node handles and fixed record/byte limits. Each
attempt pins its actual Node/runtime/store binding and original client/read ticket.
Different participant leaders/stores are allowed only through that configured
handle set, never by relabeling a result from another handle/group.

The driver owns a bounded table from an exact outstanding ticket to transaction,
phase, expected group, original OperationId and immutable command/query content.
It obtains completions directly from Node polling and calls that same Node's
complete_client/complete_read. Match the full original ticket before consuming or
interpreting the outcome. Do not accept a public ClientOutcome passed in by a
caller just because it has the Applied enum variant. A shared host completion
multiplexer must retain the same exact-owner/ticket routing, not deliver by group
or largest index alone. After a returned outcome, store only bounded semantic
facts; the original output/credit ownership still completes normally.

Static ownership is the initial supported transaction scope: do not redirect an
old transaction to a replacement group incarnation or change its participant
ownership mapping. This is separate from mutable voter configuration. Exact
committed/applied immutable phase truth survives the already-safe P4 membership
protocol, including its checkpoint/replay and deduplication requirements. Client
Applied omits ConfigurationId, but that omission does not justify a new frozen-
membership gate: it proves the original scoped command, rather than configuration
authority for later work. Fresh Node quorum reads establish current authority
under the effective configuration; existing membership/transport validation stays
authoritative. Use validate_group, application admission and original driver
ticket/content correlation. An additional validate_proposal_context restriction
is needed only if a particular transaction deliberately depends on a configuration
or physical assignment, not merely because its completion lacks that metadata.

For Prepared: accept only an Applied receipt for the exact participant Prepare
operation/content whose application-specific outcome is Prepared, or a fresh
quorum query returning that exact retained prepared record. Validate transaction
ID/digest, coordinator and both participants, account/amount, phase IDs and lock
ownership. Prepare must remain a durable monotone fact with its account lock
until an exact coordinator decision is applied; an unrelated abort or timeout
cannot erase it. Only after both observations may the driver propose Commit.
A business refusal or unresolved observation never supplies missing readiness.

For DecisionApplied: first observe the immutable coordinator decision via its
original Applied completion or fresh quorum query; then propose the participant's
original apply-decision operation with exactly that decision/content. Accept only
that participant's real Applied phase result or fresh quorum status retaining it.
Only after both participants are actually observed DecisionApplied may the driver
propose coordinator AppliedAll. Keep both account locks until this point.

For release: each release proposal must follow observation of the original
coordinator's real applied AppliedAll state for this exact transaction/decision.
Observe that state through a matching completion or fresh quorum read, then send
the immutable release phase to each participant. A local driver counter showing
it sent two apply commands is insufficient. Report Finished/success only through
the declared coordinator terminal state and actual participant release rules.

Fresh reads across groups do not create a global snapshot. Collection is safe
only because these transaction facts are immutable/monotone and the locks prevent
intervening conflicting account activity. Locked-read/transfer isolation and
business abort rules must be specified by the separate transaction protocol/model.

This is an explicit trust boundary: phase-producing hosts and selected application
providers are trusted to obey these rules, just as current in-process providers
are trusted. Raw Node::propose accepts bytes and does not verify foreign truth.
Opaque observations/private constructors prevent accidental API substitution;
they do not sandbox a host that deliberately bypasses the driver or its command
encoder. Such bypass is outside this profile, and untrusted request dispatch must
not route internal phase commands. Do not advertise cryptographic certificates
or Byzantine resilience to conceal that assumption.

## Immutable identity and restart/checkpoint contract

Persist the complete transaction specification and every distinct phase OperationId
at Started: transaction/content binding, group incarnations, coordinator,
participant order, accounts/amount, decision and phase namespace. Reuse exactly
those IDs and bytes after unknown outcomes, driver replacement or restart. Reusing
an ID with other content must conflict, and changing the participant group
incarnation must not redirect an old transaction. Router/worker tickets are
volatile per-attempt identities, never persistent transaction IDs.

Coordinator checkpoints retain specification/phase IDs, original decision,
AppliedAll/Finished state, original terminal result and deduplication. Participant
checkpoints retain specification, Prepared/DecisionApplied/Released status,
account locks, balance effects, exact applied decision and original phase results
with deduplication. Restore validates these fields and their relationships before
replaying the committed suffix. A bare balances image loses prepared ownership
and is insufficient. The existing CheckpointStateMachine requirement already
includes all replay state/deduplication (`src/application.rs:210–220`); native
SnapshotMetadata binds group/bootstrap/membership/index/term/schema
(`src/snapshot.rs:30–94`). No foreign snapshot receipt replaces that dependency.

After driver restart, discard old pending tickets/observation cache. Read actual
coordinator and participant records with new Node quorum invocations. Retry
original immutable phase commands where appropriate; a duplicate Applied result
must return the retained original semantic outcome. Timeout/Unknown means resolve
or retry; it never decides Abort or releases a prepared account. Never restore
from an oracle's account values or regenerate phase IDs from a fresh allocation.

## Existing authenticated remote support and honest first exit

The sample command channel can select existing TLS pins and group/incarnation
reader/writer/admin grants (`src/bin/support/service_access.rs`). Its command
client returns bounded strings, and counter_service output intentionally formats
application values while dropping the rich original ticket/position/barrier
(`src/bin/counter_service.rs:333–373`). An authenticated `OK` string still is not
a transaction Prepared/AppliedAll observation contract. Current peer RPC codecs
carry Raft group/store/term/context; they are not remote application certificates.
Do not reuse discovery/manifest channels to imply transaction phase provenance.

First usable exit: a native replicated coordinator plus two native replicated
participants, driven through owned existing public Node handles in one Rust
embedding. Exercise the original phase IDs/content, real completions and quorum
reads, withheld replies, leader replacement, cold checkpoint+suffix recovery,
locks/abort/refusal and cleanup; include wrong-group/operation/content and
caller-readiness negative controls. This needs no transaction network wrapper.
Separate-process transport can later define a bounded authenticated observation
schema using the existing security contracts if required by its own deliverable.
The first embedding's trusted scope must be stated, not silently broadened.
