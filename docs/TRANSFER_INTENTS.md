# Durable ownership-transfer intent

`transfer::TransferIntent` is the first P6 lifecycle record. Committing it in
`directory::Directory` records an exact proposed split or merge and reserves its
local responsibility and target groups. It does **not** change the published
manifest, fence sources, create groups, import data or activate targets.

## Contract and transitions

Contract version 1 accepts a whole top-level responsibility, with unchanged
identity/incarnation, authority, application adapter/schema, partition scheme and
scope. Both manifests are Active; epoch and route generation each increase by
exactly one. Execution changes Single to Partitioned or Partitioned to Single,
using distinct concrete groups on each side with no source/target overlap. A
one-to-one relocation, delegated route or parent binding is refused. Delegated
parent epoch coordination remains required future work in the full P6 goal.

`TransferIntent::new` returns both original owned manifests on rejection.
`encode`/`decode` use canonical `VBTINT01`: magic, two little-endian u32 manifest
lengths and the checked manifests. The complete command is bounded by the existing
32768-byte directory publication ceiling. Unknown tags, trailing bytes, truncated
lengths and invalid manifests are refused before retention.

Initialize the directory and publish the exact source manifest first. Submit the
encoded intent under a stable operation ID through existing proposal admission
and ordinary Raft commands. Authorization is the embedding host's responsibility;
syntactic validity grants no administrative permission. Accepted proposal or local
append does not establish a committed intent. Ordered application after the
existing durable quorum prefix returns `TransferIntentRecorded` and reserves the
responsibility and target groups. Ordinary publication for that responsibility
then returns `LifecycleBusy`. The published owner remains the source.

A stale/unpublished source returns `GenerationMismatch`, an unplanned responsibility
returns `UnknownResponsibility`, and a locked responsibility returns `LifecycleBusy`.
Targets already reserved by another intent or referenced by the local bootstrap
plan/current manifests return `TransferGroupBusy`. This is a local reservation,
not a global allocator or permission to create a foreign group. Outcomes consume
the existing bounded history. Exact retries return their original outcome;
changed bytes under the same operation return `OperationConflict`. Failed batch,
syntax, index or capacity validation leaves application state unchanged.

## Observation, recovery and resources

`Directory::transfer_intent_at` returns a local applied diagnostic containing the
original operation, first application index and checked intent. That index is a
record position, not a commitment watermark or a foreign durability certificate.
`LifecycleDirectory` wraps the same directory with `DirectoryQuery`/`DirectoryRead`
for manifest or transfer queries. Use the existing one-use quorum read barrier
for distributed linearizable observation. The original Directory manifest read
API remains available. Unsupported/unrecorded operations return None; reads
beyond the contiguous applied prefix fail.

The wrapper delegates application, admission, checkpoint and hosting-group checks
to its owned directory. It adds no worker, store, runtime or progress driver.
Read/result bounds charge the fixed enum plus retained nested route capacities.
The existing operation/history limits bound records and reservations; admission
also refuses more than 8192 pending entries, including repeated operation IDs.
Temporary cloned application state needs host headroom. Drop releases memory;
selected application/log/snapshot owners retain shutdown responsibility.

Schema-1 `VBDIR001` layout is unchanged: replay of retained original commands
reconstructs intent locks and target reservations alongside publications and retry
outcomes. Existing commands retain their behavior. Older implementations do not
understand `VBTINT01` and fail closed; this is no mixed-version or rolling-upgrade
claim. All selected application implementations must support the new command.
Checkpoint publication and WAL recovery use existing store bindings and durability
tokens. No new consensus effect, watermark, generation source or storage format
is introduced.

The [source fence wrapper](SOURCE_FENCING.md) now commits an intent-bound fence
and preserves immutable exports at its exact boundary. The
[target guard](TARGET_IMPORTS.md) commits staging and inline data imports while
remaining non-serving. Directory orchestration
still has no cancellation/unlock, foreign fence/import receipt, ownership
publication or activation command. Do not begin an operational transfer
expecting this intent-only slice to finish it. Forward recovery, retained lineage,
recursive coordination and no-dual-owner histories remain the P6 completion gate.

## Evidence and next two steps

`tests/directory/transfer.rs` checks shape, exact identity/epoch, collisions, retries,
locked publications, atomic truncation/restore, bounded pending admission and read
results. `tests/directory.rs` drives real three-replica Raft and native WAL/snapshot
files: lost observation, lagging replica, checkpoint/compaction, reopen, survivor
election, snapshot catch-up, original retry and quorum read. Owners remain unchanged.
Delivery is an in-process message pump; this new history is neither TCP/QUIC
transport evidence nor modeled power loss nor a distributed transfer proof.

Source fencing and committed non-serving target imports now implement these
local phases with exact identity/boundary/operation/content binding. Next verify all
fence/import evidence before directory publication and durable target activation.
Those steps turn this journal into a usable split; compatible merge reuses the same
handoff machinery before P7 measured tuning.

Checked metadata ownership decisions are now available through
[verified transfer publication](TRANSFER_PUBLICATION.md), including reserved control
history and original-decision recovery. Targets remain non-serving; durable activation,
complete interrupted split recovery and distributed merge are still pending.

[Durable target activation](TARGET_ACTIVATION.md) now verifies and retains the
publication decision before serving imported data. Selected TCP/QUIC WAL/checkpoint
histories serve with metadata offline and reopen the old source fenced. Complete
interrupted split schedules, distributed merge and recursive lifecycle remain work.
