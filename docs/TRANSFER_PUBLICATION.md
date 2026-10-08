# Verified transfer publication

`TransferPublication` connects the source fence and target imports to a committed
metadata ownership decision. It implements the publication step of P6; targets
remain non-serving until [durable target activation](TARGET_ACTIVATION.md) commits.
The existing static service is usable independently of this protocol.

The authenticated trusted host obtains quorum-backed source `SourceFreezeStatus`
and target `TargetStatus` observations with their exact configuration identities.
`SourceFenceEvidence` binds the canonical intent digest and every immutable export
commitment. `TargetReadyEvidence` binds staging/import indices and the imported
source fences, configuration identities, scopes and content digests. These public
values check structure and content; serializing them does not prove foreign
commitment or authentication. Hosts must verify foreign provenance before proposal.

`TransferPublication::new` requires the exact ordered source and target sets from
one checked top-level whole-responsibility split/merge intent. Every intersecting
source/target edge must appear exactly once with the same scope, source fence,
configuration and image digest on both sides. Missing, extra, reordered or changed
facts fail before ownership changes. No application images enter this record.
Constructor rejection returns the original owned evidence and intent.

The directory accepts publication only for its retained successful intent, the
currently locked responsibility and the exact current before manifest. Committed
ordered application replaces that manifest with the intent's exact after manifest,
releases the responsibility lock and retains target/incarnation reservations.
The publication command uses an operation ID distinct from the lifecycle ID.
Its original outcome and decision survive retries, subsequent metadata updates
and checkpoint/WAL recovery. A competing publication gets a retained rejection
if ordinary history has room; exhaustion cannot replace the successful decision.

`transfer_publication_at` is a local applied-state diagnostic. Distributed users
must obtain `LifecycleDirectory`'s `DirectoryQuery::Publication(lifecycle_id)`
through the existing quorum-read barrier. The returned `TransferPublicationStatus`
contains the original publication command ID, applied index and checked record.
The index is a log position within the directory's contiguous applied prefix,
not a cross-group watermark. Target activation must verify this committed status
against its exact retained intent and import; a routing hint is insufficient.

## Capacity and recovery

Accepting an intent reserves one 64 KiB publication allowance. Ordinary traffic
cannot consume it. The extra lifetime control pool is
`min(ordinary_operation_capacity, 256) * 65536` bytes, at most 16 MiB.
Successful publications charge actual canonical bytes to this pool and use one
extra journal record per accepted intent, without consuming an ordinary operation
slot. No pool reclamation or retry eviction exists. Intents fail durably with
`TransferControlBusy` when a further reserve cannot fit. Failed publications use
ordinary history. Pending admission credits at most one publication per open
lifecycle; additional competing requests reserve ordinary losing outcomes.

`VBTPUB01` is canonical little-endian bounded metadata, at most 64 KiB. The largest
supported 256-route split is 63,624 bytes; the corresponding merge is 61,584 bytes.
`VBTDEC01` adds 36 bytes for the original decision ID/index. Decoders reject invalid
identities, coverage, count bounds, truncation and trailing data. Retained read
results charge nested vector capacities. The directory snapshot envelope includes
both history pools and up to twice the ordinary operation count.

`VBDIR001` schema 1 retains its record layout: replay of the original commands
reconstructs manifests, locks, reservations, publication links and resource usage.
Old command histories remain readable; older code refuses the new command tag.
This is not a mixed-version deployment claim. Apply/restore stage bounded state
before replacement, so embedding hosts need temporary-copy headroom. No new store,
worker, durability token, consensus effect or mandatory ancestor write is added.
The existing durable quorum prefix and ordered application authorize the result.

## Evidence and remaining work

`tests/transfer_publication.rs` checks complete/mismatched observations, ordinary
history exhaustion, pending competition, original retries, decision/read bounds,
atomic restore, exhaustive truncations and the maximum split envelope.
`tests/routed/native.rs` drives actual three-replica source, two targets and
metadata groups over TCP/TLS and QUIC. It obtains quorum observations, stops the
fenced source, publishes despite exhausted ordinary operation slots and recovers
the original decision through WAL or checkpoint reopen and discarded observation.
Targets still refuse data after publication. These selected graceful-reopen
histories do not establish power-loss safety, activation, complete no-dual-owner
split recovery, distributed merge, recursive coordination or later retirement.

Durable target activation now consumes the verified decision and serves without
metadata access. Selected split, merge and repeated-transfer recovery now exercise
that handoff; recursive lifecycle/retirement remain before P7 measured tuning. Full P0–P7 remains active.

The [selected complete split recovery ledger](SPLIT_RECOVERY.md) now checks
all nine committed phase boundaries through whole-topology TCP/QUIC WAL/checkpoint
reopen, both-target activation and status-driven trusted host resumption. These
are graceful committed-boundary histories. Selected [merge](MERGE_RECOVERY.md) and
[repeated transfers](REPEATED_TRANSFERS.md) are also covered; recursive lifecycle and
retirement remain.
