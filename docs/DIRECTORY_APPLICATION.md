# Replicated directory application

`directory::Directory` implements the existing public StateMachine,
BoundedStateMachine/ApplicationReceipt, ProposalAdmission, ReadableStateMachine,
BoundedReadableStateMachine and CheckpointStateMachine contracts. It can run over
ordinary Raft Command entries with the same log and snapshot providers as other
applications. It creates no runtime, file, socket, worker or consensus group.

This slice publishes and recovers routing metadata for **fixed, explicitly trusted
initial ownership**. It does not perform source fencing, ownership transfer,
reparenting or target activation. [Routed application execution](ROUTED_APPLICATION.md)
now supplies data-command admission/apply and native child writes during parent loss.

## Bootstrap, ownership and publication

Construct a `DirectoryPlan` for one exact metadata authority group using already
checked responsibility manifests. Its StateMachine group check binds that authority
to the actual Raft group at startup, assembly, execution and recovery. Lower-level
embedding hosts must perform the same check. The plan is an explicit trusted bootstrap
input for already established group assignments, not a discovery hint or a way
to replace an unreachable owner. External parent grants require caller verification
against an authenticated authorized source; this application cannot independently
verify committed ancestry held by another metadata group.

The plan permits at most 256 manifests, including retained input vector capacity.
It checks exact authority, distinct logical IDs, generation 1, Active initial
state, local parent/child scope/scheme/epoch bindings, unique child delegation
across the local forest and local ancestry cycles/depth. A foreign authority cannot
alias a locally planned responsibility. External child locators need not be mirrored
locally. There is no cross-parent move API. Constructor failure returns the original
owned manifest vector, including its allocation, before retaining work.

The application starts empty and uninitialized. First commit the bytes returned by
`Directory::bootstrap_command(max_bytes)` under an ordinary operation ID. The
Initialized receipt binds the exact full plan and configured capacities through
the Raft log; it does not activate a transferred owner. Publication is refused
until this command is committed and applied. WAL-only replay compares that binding
with the selected configuration and fails closed on any mismatch, including an
unused added grant. Checkpoints preserve the same binding and history.

A `DirectoryCommand { expected: None, manifest }`
installs exactly one manifest from that plan only after ordinary committed ordered
application. A locally authored child waits until its local parent is applied.
An external-parent initial grant is trusted bootstrap input. Publishing a parent
locator alone neither creates nor activates an external child group.

For an existing manifest, publication requires the exact current expected route
generation and the next consecutive generation. Explicit effective placement
requirements may change. Identity, parent, authority, adapter, partition scheme,
scope, ownership epoch, lifecycle state and execution mapping must remain identical.
Thus ordinary publication cannot change owner groups, revive/fence a responsibility
or silently change a child's voting rules. Placement metadata is a declaration;
actual replica/quorum changes still require the P4 protocol. The [transfer intent journal](TRANSFER_INTENTS.md) now records a proposed top-level
split/merge and locks conflicting publications without changing ownership.
The optional source wrapper supplies local committed fencing and exact-boundary
export. The optional target guard supplies committed staging/inline imports. [Verified transfer publication](TRANSFER_PUBLICATION.md) now checks complete cross-group
evidence and commits the exact ownership decision. Target activation remains pending.

There is one DirectoryReceipt per Command, including the initialization command. Published includes the resulting route
generation. Valid commands that lose their compare-and-set race or violate the
fixed grant return durable application outcomes: GenerationMismatch,
UnknownResponsibility or OwnershipChange. They consume bounded operation history.
A repeated operation with identical bytes returns its original outcome, even when
the current directory would produce a different outcome. Different bytes under
that operation return OperationConflict and retain the original history.

Syntax, index gaps and exhausted lifetime capacity return ApplicationError and
leave the entire batch unchanged. The surrounding application owner uses its
existing failure/fencing contract. Authorization must run before proposal; a
well-formed command or operation ID is not credentials. Accepted-but-uncommitted
commands do not appear in applied manifests or produce application receipts.

## Reads and composition

`Directory::manifest` is a borrowed local applied-state diagnostic.
`ReadableStateMachine::read_at` returns an owned optional manifest, refusing an
unapplied boundary. Distributed linearizable reads use the existing one-use
Raft read barrier and `read_at_barrier`; a leader cache or local applied diagnostic
is insufficient. Read/result bounds conservatively charge the manifest value
plus route capacity. The directory's applied index is a contiguous log prefix,
including noops and duplicate/rejected application commands.

After establishing source authorization and the required read semantics, admit
returned views into the selected ManifestCache. Cache/provider replacement cannot
waive immutable manifest validation or the directory's ownership restrictions.
The native three-replica history feeds applied directory views into the native
cache and resolves an existing child partition. That is not yet a routed data write.

## Lifetime bounds and formats

DirectoryLimits fixes 1..4096 ordinary retained unique operations and 1..64 MiB of retained
original command bytes. These capacities are schema parameters identical across
replicas and checked on restore. The constructor rejects a history budget too small for the initialization command.
Initialization consumes one operation slot and its full request bytes. Successful transfer publications use an additional bounded control pool reserved
when intents are accepted; see TRANSFER_PUBLICATION.md. There is
no retry eviction. ProposalAdmission
reserves both operation count and bytes against all pending unique operations;
conflicting pending payloads reserve their largest size. Applied duplicates need
no additional history slot. It validates syntax without predicting CAS outcomes,
which depend on committed application order.

Manifests, initial plan and current views have their own fixed count/route ceilings.
Map/allocator bookkeeping is count-bounded, separate from charged history bytes.
Apply clones bounded state before mutation; restore constructs a bounded replacement
before publication. Temporary old/new copies need embedding headroom. These limits
are not exact RSS guarantees. Host-owned pending queues use their existing budgets.

Directory application schema is 1. Fixed little-endian format tags are:

| Tag | Content |
| --- | --- |
| `VBMAN001` | Checked manifest fields; half-open u16 bucket boundaries, strict boolean/mode tags and bounded route count. IDs/incarnations are checked nonzero values. |
| `VBDINIT1` | Exact authority, operation/history capacities and the complete canonical initial plan. Its bounded maximum is 8 MiB; actual required size depends on the plan. |
| `VBDCMD01` | Expected generation (u64, zero means initial publication), u32 manifest length and one manifest. Maximum complete command is 32768 bytes. |
| `VBTINT01` | Exact checked before/after split or merge manifests; bounded by the same 32768-byte ceiling. |
| `VBTPUB01` | Complete checked source/target evidence and exact intent; at most 65536 bytes. |
| `VBDIR001` | Applied boundary, exact configured capacities/authority/bootstrap plan, then every unique original command in first-application order with its original index and operation ID. |

`DirectoryCommand::encode(max_bytes)` and `Directory::bootstrap_command(max_bytes)`
check their complete sizes before allocating;
publication decode rejects trailing bytes, unsupported tags, noncanonical booleans, invalid
IDs/ranges/modes and unchecked lengths/counts. Initialization admission compares
the complete canonical binding with the selected checked plan rather than decoding
untrusted counts into allocations. No arbitrary recursive wire tree or
runtime hash is introduced. The manifest codec is application format, not a new
peer protocol or WAL format. Existing snapshot integrity checks protect published
images; these application encodings do not introduce a separate checksum.

Checkpoints reconstruct current views and original outcomes by replaying retained
unique commands against the exact initial plan. Duplicate/conflicting retries and
noops have no additional state effect; the checkpoint's applied boundary preserves
their progress. Failed CAS outcomes are reproduced in original application order.
This keeps all retry content instead of using a digest with possible collisions.
History is deliberately bounded for the application's configured lifetime.

Restore rejects changed plans/capacities/authority/schema, nonincreasing or out-of-
bound history indices, duplicate operation IDs, malformed commands, oversized
history and trailing/truncated bytes. Failure leaves the existing application
unchanged. The conservative whole-lifetime snapshot envelope is:

`58 + encoded_plan_bytes + 56 * operation_capacity + history_byte_capacity + control_history_capacity`.

`readiness_requirements()` returns that bound, application schema and maximum
command size: the larger of the 65536-byte transfer-publication ceiling and actual
initialization size. Large forests can exceed default log command limits, so the
embedding must explicitly select compatible log/snapshot/transport envelopes;
this application does not resize providers or hot-reload its plan. Persisted
route generations and retry outcomes are reconstructed by replay, never chosen
from wall-clock time or a volatile cache maximum.

## Durability and evidence

No new durability token, core effect, membership rule or completion domain is
introduced. A publication result uses the existing locally durable matching
quorum prefix, commitment and ordered application. Checkpoint publication/pinning,
WAL compaction and snapshot installation use the existing public helpers and
exact store/group bindings. Merely encoding, appending or caching a command does
not make it committed. Drop releases only application memory; selected providers
retain their own shutdown/recovery responsibilities.

`tests/directory.rs` includes deterministic codec, bounded admission, atomic
application/restore, local topology, original retry outcomes and transfer-refusal
checks. The native test drives actual three-node Raft with three filesystem WALs:
committed full-plan initialization, publication, reply loss followed by retained-log
reopen (including changed-plan/capacity refusal before checkpoint), isolated former-leader
uncommitted publication, survivor election/update, native checkpoints/compaction,
older-replica snapshot installation, quorum read and a second reopen. It checks
that the abandoned command never enters retry history and the original publication
retry still returns generation 1 while the current manifest is generation 2.
Delivery is bounded and host-driven in process; this is not a TCP/QUIC directory
endpoint, an OS power-loss test or a proof of distributed ownership transfer.
