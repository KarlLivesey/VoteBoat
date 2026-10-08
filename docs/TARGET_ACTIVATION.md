# Durable target activation

`TransferTarget<A,P>` now serves its imported scope after a matching activation
record commits and applies. This supplies P6's activate step after source fencing,
final import and [verified metadata publication](TRANSFER_PUBLICATION.md). It uses
the same public scope/application/read/checkpoint seams and native persistence
owner; there is no hidden runtime, extra store or parent write in ordinary data.

The trusted authenticated host obtains the original `TransferPublicationStatus`
through a directory quorum-read barrier and supplies its configuration identity
in `TargetActivation`. Before proposing, the host must verify foreign authority,
configuration and quorum provenance. Public serializable evidence is structural
content binding, not a cryptographic certificate. A route hint, leader cache,
import receipt or uncommitted activation cannot grant serving authority.

`activation_command` checks the exact lifecycle ID and intent, the local original
stage index and complete `ImportStatus` against that publication's target entry.
That includes import index, original load digest and every source fence,
configuration, scope and image commitment. It validates the publication command
ID/index separately from the lifecycle ID and target log positions. Metadata
publication and target activation indices are positions in distinct group logs;
neither becomes a cross-group applied watermark.

The canonical `VBTACT01` command binds the target's exact bootstrap digest,
metadata configuration and complete original decision. Ordered apply rechecks
local phase/evidence before retaining activation. It uses the reserved lifecycle
operation ID, as staging/import do; exact command bytes distinguish the phases.
`ActivationStatus` retains first activation index/digest, metadata configuration
and original publication ID/index. Exact retries return that first status while
the wrapper applied prefix advances. Different activation bytes or an ordinary
data request under the reserved lifecycle ID cannot replace it. Authorization of
privileged commands remains the host's responsibility.

## Data and public provider behavior

After activation, routed writes check responsibility/incarnation, adapter/scheme,
owner group, epoch, selected scope/bucket and exact envelope/payload key agreement.
The unchanged operation ID and application payload go to the selected scope
provider, which owns semantic retry/results/outbox state. Imported operations
therefore retry through the new owner without repeating effects. Location-only
route generations do not revoke an unchanged owner/epoch/scope. Wrong routes
produce `TargetOutcome::Rejected`; inactive data produces `NotActive`. Admission
refuses both, and committed rejected requests advance only the provider prefix.
Malformed payload/key bindings fail atomically. The provider determines semantic
conflicts and capacity refusals through its existing contracts.

`TargetReceipt<R>` and `TargetOutcome<R>` now carry `Applied(R)` as well as fixed
control outcomes. Receipt nested accounting delegates to the actual provider.
Batch bounds project ordered import/activation before querying each active data
receipt bound, so a provider's post-import bound is respected. Proposal admission
simulates at most 8192 pending entries and delegates active data admission to the
provider. Apply and restore build a bounded replacement before publishing it.

`TargetQuery::Data` serves only after local activation and owner checks. Distributed
linearizable users still need the existing quorum-read barrier. Generic read
results subtract provider inline size and charge retained nested capacity; no
provider receipt or result is assumed Copy or fixed-size. Status queries retain
original import lineage and fixed activation metadata. Local diagnostics alone
are insufficient for distributed authority.

## Resource, recovery and compatibility contract

One retained activation command has a 64 KiB ceiling, independently reserved from
ordinary provider history. It carries no application image. Maximum supported
split publication plus activation framing is 63,712 bytes. Import remains bounded
by its configured body limit (at most 8 MiB); provider checkpoint remains bounded
by its configured limit (at most 64 MiB). Command readiness is the maximum of the
bootstrap, configured load envelope and activation ceiling. Routed data must also
fit that complete command ceiling and the existing routed payload/key ceilings.
The snapshot envelope includes binding, original load, original activation and
provider checkpoint. Temporary cloned state/decoding copies require host headroom;
these byte/count limits are not exact RSS guarantees.

Activation consumes no ordinary provider operation slot. A full imported retry
history therefore permits activation and existing retries while refusing new
operations. There is no retry eviction, authority reclamation or unfreeze API.
Drop releases application memory; native owners retain their existing drain/join
and authoritative-store recovery duties.

`VBTRGT03` under target application schema 2 retains original stage/import/activation
plus a later source fence and separate provider/wrapper boundaries. Restore verifies
phase ordering, exact binding, checked publication/local import and provider state.
The reader also accepts schema-1 inactive `VBTRGT01` and active `VBTRGT02` records;
schema 1 cannot label the new format. This explicit reader compatibility is not a
mixed-version deployment or alternate-provider migration claim. WAL replay uses
the same checked commands. See [repeated transfers](REPEATED_TRANSFERS.md).
No new durability token or consensus effect is introduced: only the existing
durable matching quorum prefix, commitment and ordered apply permit service.

## Evidence and next deliverables

`tests/transfer_activation.rs` covers mismatched/missing imports, new-epoch refusal
before activation, original activation retry/conflicts, imported data retries,
routes/key/operation conflicts, pending phases, WAL replay, all activation/checkpoint
truncations, old inactive format, full provider retry history and a downstream
provider with variable nested receipts/read results whose bound changes on import.

Four native TCP/TLS/QUIC histories activate one of two imported three-replica
targets using the actual recovered directory decision. Metadata shuts down before
ordinary target writes; WAL-only or checkpoint reopen preserves original activation,
values, retries and outbox. The other target remains inactive. The old source
reopens fenced and refuses old-owner writes/reads while the activated target serves.
These selected graceful-reopen Linux histories are not power-loss, every interrupted
phase, distributed merge, recursive ownership or macOS/separate-host evidence.

Selected split, merge and repeated-transfer recovery now exercise that handoff.
Recursive coordination/retirement remain, then P7 measured tuning. Full P0–P7 remains active and P8
is deferred; static service availability remains independent of this work.

The [selected complete split recovery ledger](SPLIT_RECOVERY.md) now checks
all nine committed phase boundaries through whole-topology TCP/QUIC WAL/checkpoint
reopen, both-target activation and status-driven trusted host resumption. These
are graceful committed-boundary histories. Selected [merge](MERGE_RECOVERY.md) and
[repeated transfers](REPEATED_TRANSFERS.md) are also covered; recursive lifecycle and
retirement remain.
