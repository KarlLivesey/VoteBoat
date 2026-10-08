# Source fencing and exact-boundary export

`transfer_source::TransferSource<A,P>` wraps the existing `RoutedApplication`
where A supplies public scope/checkpoint/bounded application contracts and P is a
partition policy. It implements the same public application, proposal, read and
checkpoint seams used by native Node and host assemblies. No core, transport,
worker, file or storage binding is added. This is the source half of P6; the [target guard](TARGET_IMPORTS.md) separately supplies committed staging/import.
Cross-group publication and activation remain unfinished.

## Inputs and state transitions

Construction consumes an unused routed application and an aggregate export-payload
budget (1..64 MiB). Rejection returns the original application. Its provider scope
must equal the exact local concrete-group route and its scheme must match the
grant. The routed application retains identity/owner checks and original semantic
request history. The source wrapper has its own contiguous applied prefix.

First commit `bootstrap_command` under a stable operation ID. `VBSROWN1` binds the
export budget and the existing exact routed bootstrap; it projects to that routed
bootstrap during apply. Raw routed bootstrap/fence commands are refused so they
cannot bypass this guard. Every routed data command must have an envelope key
identical to `A::command_key(payload)`; syntax/key mismatch fails atomically.
The original P5 RoutedApplication remains available for fixed-owner applications.

A trusted authenticated host verifies the directory's committed TransferIntent,
source identity and proposed target staging/capacities, then proposes
`freeze_command(intent)` under that intent's stable operation ID. Encoding alone
is not permission or foreign commitment evidence. This source cannot independently
validate a foreign directory's quorum. The exact before manifest must equal its
configured grant. The checked whole-responsibility intent may describe one source
split or compatible multi-source merge; this wrapper covers its own local scope.
Delegated-parent coordination is still outside the first intent format.

Before freezing, sum the provider's **configured lifetime** payload-capacity bounds
for all intersecting target ranges. Refuse a command exceeding the bootstrap-bound
budget. This is deliberately conservative: BucketCounter uses its full configured
checkpoint bound for every subrange. A large layout may need smaller configured
application limits or a later chunked-transfer protocol; the guard never silently
raises provider envelopes. The trusted host must check target persistence/import
capacity and establish non-serving staging before proposing this irreversible cut.
Those orchestration steps are not yet implemented.

At committed ordered apply index F, the command projects to the existing irreversible
routed fence. Check actual exported metadata/capacity against the provider contract
before publishing the candidate application state. Successful application retains
the exact intent and emits the existing OwnershipFence receipt. The routed state
now stays immutable at F. Wrapper progress continues through later noops, rejected
data and exact freeze retries. Exact retry returns the original fence/index;
a competing intent or other later command cannot replace the fence or mutate data.
There is no timeout rollback, cancellation, unfreeze or source retirement.

## Export, observation and durability

`export_target(group, max_bytes)` derives only the intersection assigned to that
concrete target and exports from immutable routed state at F. Schema, scheme,
range, source boundary and retained payload capacity are checked. Source F is
lineage metadata, never the target's applied index. Provider images retain data,
original requests/results and outbox; they are not quorum receipts or ownership
certificates. The source checkpoint also retains the full routed history/control
identities and the exact intent. Target import must retain appropriate lifecycle
lineage alongside its provider data rather than treating an image as authorization.

`SourceQuery::Data` uses the existing routed query and returns Fenced after the cut,
including when a read barrier requires a later wrapper prefix. `SourceQuery::Freeze`
returns optional `SourceFreezeStatus { fence, intent, exports }`. Each bounded
export commitment names target/range and SHA-256 of canonical schema/scheme/F/
length/content. The [target import](TARGET_IMPORTS.md) checks its images against
these source-observed commitments. Both refuse an unapplied
boundary. Use the existing one-use quorum read barrier for authoritative distributed
observation after a lost receipt. `fence`, `frozen_intent`, `routed` and exports are
local diagnostics. An authenticated non-Byzantine host must establish their provenance
before proposing foreign lifecycle facts; these structs provide no cryptography.

Only the existing durable quorum prefix, commitment and ordered application permit
a fence receipt to escape. A proposal, local write, image or status value does not.
Source snapshot/WAL recovery uses existing durability dependencies and store/group
bindings. All later data serving is fenced before native recovery exposes the app.
No new persistence token, consensus effect, route generation or external side effect
is introduced. Outbox instructions are retained, never delivered by replay/export.

## Bounds, recovery and failure

Scope application contract version **2** adds `export_scope_bound(range)`, a lifetime
upper bound on provider payload capacity, stable under future writes and noops.
Host providers must implement it. Existing provider image formats stay unchanged.
Metadata/image values and at most 256 target ranges have additional count-bounded
memory; payload budget is not an exact RSS limit. Source/result bounds charge inline
layout and nested route capacity. Command and snapshot envelopes are declared by
`readiness_requirements`; selected providers must admit them before deployment.

Proposal admission simulates at most 8192 pending commands on bounded cloned state
before checking a candidate; it changes no live state and creates no I/O. Per-command
bytes are checked before copying. Apply and restore clone bounded state, publish
only on success, and preserve the existing instance on failure. Simulation and
exports can be expensive; no performance claim is made. Caller queues retain their
own budgets and ordering. Provider contract violations/application errors follow
the existing owner-fencing path; they cannot produce a successful freeze receipt.

`VBSFREE1` contains the canonical VBTINT01 intent. Application checkpoint schema 1,
`VBSRC001`, stores outer applied prefix, exact export budget, frozen inner boundary,
optional intent and the existing routed checkpoint. Restore verifies configuration,
lengths, exact intent/grant, and fence/intent/boundary agreement before publication.
Before freeze, inner and outer prefixes agree. After freeze, inner prefix equals F
and outer prefix is at least F. Thus restore/export does not relabel later data as
having existed at F. Bootstrap WAL replay also rejects changed export budgets.
This wrapper has a distinct checkpoint tag; it is not a live migration of an existing
P5 application's stored schema. Host upgrades need an explicit migration path.
Drop releases memory; native owners drain/join their existing providers normally.

## Evidence and remaining lifecycle

Seven downstream source tests cover same-batch bootstrap/data/freeze/later data,
exact retry/competing intent, immutable exports, provider-key matching, original
outbox/retry results after adapter import, all command/checkpoint truncations,
atomic rejection, changed bindings, bounds, pending admission and status reads.
Four native three-node histories use real TCP/TLS or QUIC, actual WALs, elections,
WAL-only or published-checkpoint recovery, discarded observations, repeated reopen,
original fence retries, exact exports and quorum-backed data/status reads.
These are selected source-side schedules, not modeled power loss or proof of the
whole distributed transfer. The target import in those tests is local adapter
validation, not a committed target-ready receipt or activation.

The target guard now implements committed staging/inline imports with source
identity, F, epoch, operation, scope and observed content commitments. Next require all
fence/import evidence before metadata publication and durable target activation,
with interrupted-stage and no-dual-owner histories. Compatible merge reuses that
path. P6, recursive lifecycle and the full P0–P7 objective remain unfinished.

Checked metadata ownership decisions are now available through
[verified transfer publication](TRANSFER_PUBLICATION.md), including reserved control
history and original-decision recovery. Targets remain non-serving; durable activation,
complete interrupted split recovery and distributed merge are still pending.

[Durable target activation](TARGET_ACTIVATION.md) now verifies and retains the
publication decision before serving imported data. Selected TCP/QUIC WAL/checkpoint
histories serve with metadata offline and reopen the old source fenced. Complete
interrupted split schedules, distributed merge and recursive lifecycle remain work.

The [selected complete split recovery ledger](SPLIT_RECOVERY.md) now checks
all nine committed phase boundaries through whole-topology TCP/QUIC WAL/checkpoint
reopen, both-target activation and status-driven trusted host resumption. These
are graceful committed-boundary histories; merge and recursive lifecycle remain.
