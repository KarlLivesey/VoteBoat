# Slice 419 proposal: one owned incoming update per internal transition

Status: schema and read-only implementation map only. Slice 418 production stays
frozen. Implementation requires the parent task's explicit source release.

Purpose: remove redundant incoming suffix clones on the ordinary command path,
while keeping original effects, exact provider behavior and all maintenance. No Arc,
public payload layout, provider API, WAL format or batching-policy change is proposed.

## Current ownership and the unavoidable copy

Core currently plans by cloning update.suffix, separately clones the complete update
into Pending, and moves the ORIGINAL update into Effect::Persist. OwnedLogState and
native accepted planning clone that received suffix again and drop their consumed
original update after installation. Existing canonical payloads are already retained
by the append delta; slice 418 handles eligible prefix compaction independently.

Core Pending and a public owned Persist effect have independent lifetimes. Keep ONE
normalized Pending copy and move the original raw update into Effect exactly as
before. Do not normalize the original before crossing that provider boundary: the
runtime charges original effect Vec capacities and host-selected providers may
inspect those capacities. Current shared suffix validation checks payload lengths,
indices and terms, not a new general retained-capacity admission limit. Core proposal
admission separately checks original Proposal capacities before constructing entries.
Do not invent a new limit or change error precedence while removing a copy.

## Private shape and ordering

A private owned transition holds exactly one owned normalized LogUpdate plus a
private validated plan: append metadata, existing Compact descriptor, or a validated
Full replacement. An append plan stores no second suffix. It supplies metadata,
full-state access when applicable, exact term/entry access through its owned update,
exact predecessor/original correlation, and consuming prepared installation.

The borrowed validator produces metadata before cloning or normalization. Core's
adapter validates the original input, makes its single normalized owned Pending
copy, and keeps the original for Persist. A consumed-input adapter validates the
exact owned input under the provider's limits, then normalizes only new suffix entry
containers/command buffers. Buffers already at canonical capacity can move unchanged;
slack buffers may require allocation to reproduce existing clone normalization.
Both adapters use the same validation and error order. Incoming bytes/operation IDs
cannot be substituted by an unchecked caller-provided metadata descriptor.

Exact update correlation remains full field/content equality, not a digest or just
header equality. Exhaustive LogUpdate destructuring prevents future fields escaping
checks. Pure append metadata checks must still include full suffix content; sealing
binds the admitted plan to the owned original input. Full fallback retains the old
validator and error order; retained-history/configuration/snapshot materialization
on that rare path is outside the ordinary-append gain claimed here. Compact retains
slice 418 eligibility, capacity flag, deferred trim and exact SnapshotRef checking.

Retained summaries are computed from the ACTUAL normalized new allocations; original
capacities are inspected before normalization where existing limits require them.
Unchanged canonical buffers and summary invalidation rules stay intact. Conservative
runtime/effect reservations remain until separately measured; no smaller bound is
assumed merely because one clone disappears.

## Whole batch, native I/O and cleanup

OwnedLogState consumes original mutation units, validates in original input order,
and prepares all units before any canonical installation. Duplicate groups, group
creation/identity bounds and invalid later units preserve existing error order and
atomicity. Dropping an unsuccessful preparation only drops newly owned plans/inputs.

Native ordering remains: fence/identity/outstanding-budget/sequence checks; codec
encoding and encoded-size checks on the ORIGINAL borrowed mutation batch; logical
whole-batch preparation against accepted views; control WAL reserve check; tickets;
append I/O; accepted installation; pending-ticket/sequence/length bookkeeping.
Encoding failure must precede later logical-validation errors just as today. Existing
control and ticket generation metadata can be read before consuming mutations; do
not normalize input before codec encoding or make a hidden second whole batch copy.

Prepared units own received new payloads through actual append I/O. Append uncertainty
drops those prepared payloads and fences without installing accepted/canonical state.
Successful append moves normalized entries into accepted tail or owned Full state.
Barrier publication moves the retained tail into durable canonical state. Same-group
planning uses slice 418's exact accepted predecessor, retained durable offset and
remaining tail after compaction. Multiple unpublished revisions, repeated compaction,
replacement generations and subset barrier receipts remain unchanged. Invariant
failure after I/O still fences; no result escapes before its exact durability token.

## Acceptance

1. Differential full-oracle values/errors across singleton/batch, ballot, commit,
   malformed indices/terms/bytes, entry/batch/group limits and Full/Compact fallback.
2. Pointer controls: canonical prefix unchanged; provider-received new payloads with
   capacity==length move into accepted/canonical state; slack payloads retain exact
   old normalized charges. Core exposes original raw effect capacity and owns only
   one normalized Pending payload allocation.
3. Header-identical but payload/operation/snapshot-different completion fails intact;
   exact term/entry access works for early Written replication and retry aliases.
4. Malformed later multi-group unit causes no canonical mutation/native I/O; duplicate
   group and stale summary/predecessor refusal preserves old errors.
5. Native refusing codec plus malformed logical unit returns the codec error first;
   control reserve/refusal leaves state and receipts intact; uncertain append/sync/
   before-after publication with actual ModelIO.power_loss preserves exact states.
6. Accepted append/commit/compact/append, trim into accepted tail, Full fallback after
   trim, subset publication, unaffected groups, stale generations/sessions and restart.
7. Existing exact-original application results, checkpoint/pin/load/restore/reconcile,
   real TCP/QUIC retry/recovery and owner reservation controls remain required.

Root owns final all-profile hook, affected integrations, publication and matched
both-host comparison. This proposal makes no throughput claim and leaves application,
replication-message and committed-entry copies for later measured decisions.
