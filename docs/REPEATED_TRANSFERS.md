# Repeated ownership transfers

An activated `TransferTarget<A, P>` can become a source for a later checked
transfer, using its existing application and authoritative group log. The new
intent's before manifest must exactly match the original after manifest, including
identity, epoch, generation, adapter and scope. The existing top-level
Single/Partitioned restrictions still apply. No worker, store or ordinary-write
ancestor dependency is added.

The trusted authenticated host first commits the new directory intent and stages
all new targets. It checks their capacities and every source's freeze admission
before committing any irreversible fence. The source's `freeze_command(intent,
export_bytes, max_bytes)` produces VBTFRZ01 bound to its original bootstrap digest,
aggregate lifetime export budget and new intent. Apply also checks that the new
operation ID differs from the original lifecycle ID and all retained provider data
IDs. Scope contract version 3 exposes this exact operation-presence query. An
import likewise rejects a provider history containing the target's reserved
lifecycle ID, atomically preserving the non-serving staging state.

| State | Data permission | Durable retained facts |
| --- | --- | --- |
| Staged/imported | NotActive | Original bootstrap/import |
| Activated | Existing after-manifest scope | Original activation and imported data |
| Later frozen | Fenced | Original facts plus new intent, operation, F and budget |

The original `TargetStatus.activated` is historical lineage and remains unchanged
when a source freezes. It is not a current permission to serve. Query
`TargetQuery::Freeze` through a quorum read to discover the later source status;
ordinary data reads and writes always pass the guard and refuse after freezing.

At the new freeze entry F, ordered apply advances the provider with a no-op,
validates actual immutable exports and then fixes it at F. The wrapper's contiguous
applied prefix can advance through subsequent no-ops and rejected queued commands;
these do not change exported data. The exact original freeze command and operation
return the original fence on retry. Changed commands/IDs refuse and no timeout or
old activation retry can thaw the source. Freeze consumes no provider data-history
slot, so a full history still permits it.

`export_target` returns the provider's exact-F scope image, checked for schema,
scheme, range, boundary and payload capacity. Quorum-readable `SourceFreezeStatus`
uses the existing public commitment/fence shape. The next target imports actual
images with these commitments and source configurations, then the metadata group
publishes and the next target commits local activation. Foreign quorum provenance
and configuration remain authenticated trusted host input, not a certificate
created by serialization. Source F is lineage, distinct from the next target's I.
Original data requests/results and pending outbox instructions survive each move.
Original ownership records remain in their original target guard/directory;
there is no claim that opaque provider images contain an entire ancestry proof.

The export budget is at most 64 MiB aggregate configured lifetime payload capacity.
Check every target intersection's provider bound before fencing; insufficient
budgets refuse. The native counter conservatively charges its whole checkpoint
bound for each intersection. The next target's inline import has its separate
8 MiB hard ceiling and configured limit, so the host must preflight that capacity
as well. Bounded queries, receipts, commands and snapshots include the new records;
cloned temporary state still requires host headroom.

VBTRGT03 under target application schema 2 retains wrapper/provider boundaries,
new freeze command, operation and F alongside original stage/import/activation.
Restore checks phase ordering, bootstrap binding, reserved IDs, provider boundary,
export limits and actual exports before publishing candidate state. Schema-1
VBTRGT01/VBTRGT02 remain readable; schema 1 cannot label VBTRGT03. Readiness
advertises schema 2. This reader compatibility does not establish mixed-version
peer support or a live provider migration. All effects still depend on the
existing durable matching quorum, commitment and ordered apply.

Five downstream tests in `tests/transfer_repeat.rs` cover actual split -> merge ->
split, original retries/outbox, repeated activated-source fences, fixed provider F
while wrapper progress advances, exact retry, pending/same-batch transitions,
full provider history, lifecycle/data ID collisions, all freeze/checkpoint
truncations and changed boundary/ID/bootstrap refusal. Legacy active/inactive
reader checks and injected host-provider accounting remain in the activation and
scope suites.

Four native tests in `tests/routed/repeat.rs` build a real split and write to both
activated children. The same child guards/logs then become merge sources. Each
later intent, staging, individual source freeze, import, publication and activation
closes/reopens all five three-replica groups from WAL or checkpoints over TCP/TLS
or QUIC. Fresh quorum status reconstructs the next action with discarded action
receipts; original target stage/import/activation and new source fences retain
identity. After completion, metadata and all old groups stop before merged retries
and new writes; another reopen retains values/outbox and all old sources refuse.

These are selected Linux ordinary-majority graceful committed-boundary histories,
not power cuts, arbitrary partitions or a full liveness proof. Native evidence is
split -> merge; the further split is downstream deterministic composition. Data
and tombstones remain retained in unwrapped guards. The optional
[retirement guard](RETIREMENT.md) now releases old payloads after verified handoff
and explicit host retention release, retaining fences and lineage. Delegated-parent
coordination and P7 measured tuning remain. macOS/separate-host
execution remains unverified. Full P0–P7 stays active; P8 remains deferred.

## Imported partial sources — slice155a

Select `TransferTarget::with_partial_delegation(maximum, export_bytes)` before
bootstrap, optionally after `with_parent_adoption`. This selects application
schema6, immutable binding `VBTSOWN5` and checkpoint `VBTRGT07`. It keeps one
application owner and one ordered log. Existing profiles retain their formats.

An activated imported owner can accept a checked retained-insertion intent and
freeze only the delegated range. `TargetOutcome::ScopeFenced`,
`TargetQuery::ScopedFreeze` and `export_scoped` expose its original status and
immutable image. The caller supplies original quorum provenance, imports that
image into the new child, publishes the handoff and then submits
`RetainedGrantAdoption`. `TargetQuery::RetainedGrant` returns that original result.
Neither an image nor a constructed status authenticates another group.

Remaining data continues through the existing routed target API. Frozen ranges
refuse before publication; the updated grant refuses them after publication.
Original import, activation, data retries and outbox survive successive partial
transfers and checkpoint/replay. Parent and parent-slot observations share the
ordered control history. One unpublished transfer is allowed at a time, with
explicit lifetime transfer count and aggregate immutable export budget.

Current evidence includes actual same-authority nested creation/reservation,
source fence, child import, metadata publication/parent completion and activation,
twice; mixed parent history, full data-history control reserve, corrupt checkpoint
refusal and native journal interruption are also covered. Parent observations
in that mixed-history owner test are supplied by the host fixture. Selected new-profile TCP/QUIC service composition and later full-transfer
retirement/reclamation are covered by155b1–b3; broader provider/failure/platform
evidence remains open.
Slice155b2 now supports partial-profile retirement lineage after complete
remaining-data transfer and explicit retention release; see RETIREMENT.md.


## Moving the remaining range — slice155b1

A delegated manifest with one contiguous concrete source range can move that
entire remaining range while keeping every existing child/vacant selector exact.
Use `TransferIntent::move_remaining` for a root or `DelegationPlan::move_remaining`
for an owner with a parent. The latter requires the committed parent reservation
and its `child_intent`, followed by original parent completion after publication.
Select `Directory::with_remaining_transfer` before bootstrap (schema14); existing
Directory profiles do not accept these operations, including nested observations.

The explicit VBTINT07/VBDPLAN5 formats support a fresh single destination or
multiple fresh destination groups covering the same remaining range. Source IDs,
metadata identities, modified child selectors, gaps and reused destination IDs
are rejected. Existing source fence, target import, publication and activation
contracts still apply. Destinations automatically select target schema7 with
VBTSOWN6/VBTRGT08; they receive the current complete responsibility grant but
import only their exact concrete range. No owner activates from a routing change
alone. The original source stays fully fenced after this move.

Tests compose imported data -> two partial delegations -> remaining relocation
or split -> successor activation/new writes/retries and checkpoint recovery.
Native journal cuts cover the final source fence. Slice155b2 now retires the
original partial owner after successor activation and explicit retention release,
with selected native-file reclamation evidence.
Slice155b3 adds four native TCP/TLS/QUIC × WAL/checkpoint relocation histories
with two prior partial delegations, lost-phase results, explicit retirement,
checkpoint-selected physical reclamation and ancestor-offline successor service.
Both delegated children preserve imported data and retries. The native remaining
move uses one successor; multi-target behavior has the separate embedding tests.
