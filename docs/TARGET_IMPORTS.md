# Non-serving targets and durable inline import

`transfer_target::TransferTarget<A,P>` implements existing public application,
bounded receipt, proposal, read and checkpoint contracts using a ScopeStateMachine
provider and PartitionPolicy. Native Node uses the same contracts as host assemblies.
It adds no store, worker, thread, transport or progress driver. This slice stages
and imports data; it has **no activation command and serves no client data**.

## Identity, staging and privileged phases

Construct with the exact checked TransferIntent, lifecycle operation ID, concrete
target group/incarnation, unused provider, policy and TargetLimits. The provider
scope must equal that target's route; schemes and hosting-group constraints must
match. Failure returns original intent, application and policy. A committed
`bootstrap_command` binds these inputs, provider schema/initial checkpoint and
budgets. Election or construction alone does not establish application staging.
Actual group creation/initial configuration and foreign authorization remain
explicit trusted-host responsibilities through existing native bootstrap/placement
paths. A routing manifest is not permission to create an arbitrary group.

Staging and import are privileged phases of one lifecycle operation: both commands
use that operation ID, and their exact command kinds distinguish phase retries.
Staging retries return the original staged index even after import. Import retries
return the original import index/digest. This phase-specific behavior is explicit;
ordinary application data still uses its original request IDs. The lifecycle ID
is reserved from data commands. Authorized source fences bind the same lifecycle
ID and must not conflict with original source data operations. Multi-source data-ID
collisions are refused by scope import rather than dropping an original result.

Before and after import, ordinary proposal admission rejects data commands without
taking ownership of the request. Direct committed well-formed data commands return
NotActive and project to a provider noop. Data reads return NotActive even with a
valid quorum barrier. Only status reads are available. There is no client hint,
configuration flag or timeout that activates the target.

## Source content and complete imports

A trusted host obtains the committed directory intent and authenticated,
quorum-backed source freeze observations. SourceFreezeStatus now includes bounded
per-target `SourceExportCommitment { target, scope, digest }`, derived from immutable
F-state. `transfer::ContentDigest::scope_image` uses SHA-256 over domain VBSIMAGE,
provider schema, scheme ID/version, range, source F, payload length and exact bytes,
all numeric fields little-endian. Hashing uses pinned ring 0.17.14, already present
for TLS and now a direct dependency for core import integrity. A digest supplies
content identity, not source authentication or a cryptographic quorum certificate.
Source checkpoint format is unchanged; these commitments are derived on observation.

`TargetImport::new` consumes operation, intent, target and ordered SourceImport
records. Each record contains the exact source OwnershipFence, authenticated
configuration context, image and expected source commitment. Required source
intersections follow the before manifest. Their complete disjoint ordered coverage,
group/incarnation, responsibility, epoch, operation, F, image metadata and digest
must match. Missing/reordered/repeated/wrong sources are refused. The target checks
schema against its selected provider. The host must verify the provenance of
source status/configuration and the expected digest; constructing a matching
self-supplied digest grants no authority.

`import_command` binds the canonical import to SHA-256 of this target's exact
bootstrap binding. The ordinary Raft Command contains **all actual image bytes**.
Ordered apply calls the provider's atomic import at the next target index I and
checks the configured checkpoint envelope before publishing application state.
Source F stays provenance; I is the target's own log boundary. Existing WAL quorum
synchronization, commitment and ordered apply permit Imported to escape. A majority
of digests without recoverable data cannot acknowledge this inline format.

The retained ImportStatus names original I, SHA-256 of the full load command, and
per-source fence/configuration/scope/image commitment. Different content under an
already imported lifecycle returns OperationConflict without replacing data.
Original source request results and pending outbox survive provider import;
replay/import performs no external outbox delivery. Source data/control lineage is
retained through the original intent/import record. Further transfers and retirement
must preserve all relevant lineage; they are not implemented by this slice.

## Bounds, recovery and formats

TargetLimits fixes import-body bytes (1..8 MiB) and provider checkpoint bytes
(1..64 MiB). The canonical body includes metadata as well as payload; a near-limit
image may not fit once framing is included. At most 256 source records and 8 MiB
aggregate retained image payload are accepted. These are first inline-format
ceilings, not arbitrary large/chunked transfer support. Native default log limits
may be smaller; compare `readiness_requirements` with selected WAL/snapshot/codec/
transport envelopes before staging or source fencing. Preflight target capacity
and non-serving staging for every target before an irreversible source cut.
The full orchestration/preflight protocol remains future work.

Proposal admission simulates at most 8192 pending entries on bounded cloned state,
refusing byte ceilings before copies and actual import/provider-capacity failures
before a local proposal. Apply and restore publish only a successful cloned state.
Malformed commands, gaps or provider errors leave live state unchanged and follow
the existing application's owner-fencing path if already committed by a bypassing
host. Constructor and admission rejection return original resources. Limits bound
retained records; cloning, hashing, temporary decoded images and provider state
need additional bounded headroom. No CPU, RSS or performance claim is made.

Read/query bounds charge inline results, source-vector spare capacity and provider
query buffers. `TargetQuery::Status` uses the existing quorum read barrier for
authoritative observation; `status`/`application` are local diagnostics. Status
alone has no foreign provenance. The trusted host must associate observations with
actual current target configuration before using them for metadata publication.

| Format | Meaning |
| --- | --- |
| `VBTSOWN1` | Exact group, lifecycle ID, budgets, intent and provider schema/initial image. |
| `VBTIMP01` | Canonical operation/target/intent and complete ordered source images with fence/configuration and expected commitments. |
| `VBTLOAD1` | Bootstrap-binding digest plus complete canonical import body. |
| `VBTRGT01` | Schema-1 target checkpoint: applied prefix, exact binding, first stage/import indices, original load command and current provider checkpoint. |

All formats reject trailing/truncated data and unchecked identities/counts. Restore
verifies exact binding, source coverage/content, phase/index consistency and provider
checkpoint boundary. Original import bytes remain retained for exact retries and
provenance; later noops may advance provider/target applied progress without changing
first import status. WAL-only replay compares the same staging binding. This is a
new application format, not automatic migration of another provider's checkpoint.
Drop releases memory; selected native owners still drain/join their own resources.

## Evidence and next steps

Nine downstream tests cover canonical formats, standard SHA-256 vector, changed
source content/metadata, returned allocations, incomplete/reordered source coverage,
multi-source data-ID collisions, provider capacity, atomic truncation/failure,
phase retries/conflicts, WAL replay/checkpoint restore and query/result bounds.
Multi-source import is data-path evidence, not a completed distributed merge.

Four native histories use TCP/TLS or QUIC with one three-replica source and two
three-replica staging targets. Both targets stage before source fencing. One target
imports data bound to a quorum-readable source commitment while the other remains
staged. WAL-only or checkpoint/compaction recovery, discarded observation, repeated
reopen, original digest/index status and synchronous inactive-write refusal are
checked. Imported values/outbox survive; all targets remain non-serving. This is
selected phase/recovery evidence, not power-loss modeling, complete publication/
activation, whole-transfer no-dual-owner proof or separate-host/macOS validation.

Next verify every source-fenced/target-ready observation before committed metadata
publication, then require a matching durable activation before serving. Add
interrupted-stage/no-dual-owner histories and operator resumption. Compatible merge
reuses that handoff; recursive parent coordination and later transfers/retirement
remain required P6 work. The full P0–P7 goal stays active and P8 stays deferred.

Checked metadata ownership decisions are now available through
[verified transfer publication](TRANSFER_PUBLICATION.md), including reserved control
history and original-decision recovery. Publication alone leaves targets non-serving;
local durable activation is required.

[Durable target activation](TARGET_ACTIVATION.md) now verifies and retains the
publication decision before serving imported data. Selected TCP/QUIC WAL/checkpoint
histories serve with metadata offline and reopen the old source fenced.

The [selected complete split recovery ledger](SPLIT_RECOVERY.md) now checks
all nine committed phase boundaries through whole-topology TCP/QUIC WAL/checkpoint
reopen, both-target activation and status-driven trusted host resumption. These
are graceful committed-boundary histories. Selected [merge](MERGE_RECOVERY.md) and
[repeated transfers](REPEATED_TRANSFERS.md) are also covered; recursive lifecycle and
retirement remain.
