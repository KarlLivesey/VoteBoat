# Scope application data adapter

`scope::ScopeStateMachine` is an optional deterministic export/import capability
over the public application/checkpoint seam. `bucket_counter::BucketCounter<P>`
is the first native provider: one independent signed counter per logical bucket,
with a host-selected deterministic partition policy. The original global Counter
remains indivisible. No threads, sockets, file owners or dependencies are added.

The contract is version 2. Providers declare `export_scope_bound(range)`, a
configured lifetime upper bound on exported payload capacity, stable under future
writes/noops. Source fencing uses it to refuse oversized layouts before the cut.
The native counter conservatively uses its whole configured checkpoint bound
for each subrange. Existing image formats are unchanged. Scope images carry provider schema, partition scheme,
half-open bucket range, source applied boundary and immutable bounded bytes. They
are data containers, not ownership credentials, source-fence receipts or target
readiness. Constructor rejection returns the original image buffer; counter
construction rejection returns its original policy and configuration.

## Commands, retries and outbox

`encode_add(key, delta, outbox, max_bytes)` produces a bounded `VBBCMD01` command.
Keys are at most 4096 bytes and must map inside the configured scope. A nonempty
outbox payload is at most 65536 bytes. Successful addition records the resulting
bucket value and a pending instruction with the command's stable operation ID.
Overflow records its original result and queues no instruction. Replay does no
external work. An external recipient must deduplicate the stable IDs.

Each retained operation binds its exact canonical command bytes, including key,
delta and outbox payload. Exact retries return the original outcome without
changing the value or creating another instruction. Reused IDs with different
content return OperationConflict. A retry after later changes still returns its
original value. Full request bytes serve as exact content identity; this is not
a cryptographic digest or transfer certificate. History and pending instructions
are retained for the configured lifetime capacity, without implicit eviction or
acknowledgment-based reclamation in this first provider.

Batch apply is contiguous and atomic on error. Proposal admission simulates all
bounded pending commands before reserving a new operation. Receipt/read methods
implement the existing bounded contracts; `value` and `outbox` are local access,
not linearizable distributed reads or delivery receipts. Use the existing read
barrier path when serving through a group.

## Export, import and checkpoints

Export selects the exact counters, original command/results and pending outbox
items whose keys belong to the requested subrange. Export does not fence the
source. The lifecycle caller must export exactly its committed/applied fence and
bind source identity, epoch, operation and configuration in durable protocol
records. The application itself contains no serving-authority state.

Import accepts only empty staging application data, at the next target index.
Images must be sorted, disjoint and provide complete coverage of the configured
target scope; schema, scheme, range and source boundary must match their encoded
contents. Combined imports reject operation-ID collisions instead of discarding
history. Target operation/semantic capacities are checked before replacement;
failures leave the original state untouched. The target may have a shorter log
than its imported retry history: source index 100 does not become target index
100. Scope combination here is a data operation, not the P6 merge protocol.

`VBBCP001` checkpoints bind schema, scheme, scope, capacities, target applied
boundary, counter values and original request/results. Imported outbox items are
reconstructed from successful retained requests. Changed scope/capacities, gaps,
truncation, trailing bytes, malformed records and mismatched boundaries reject.
The surrounding lifecycle application must persist source lineage/import records
alongside this adapter's checkpoint; the counter's current applied boundary alone
cannot preserve a chain of ownership transfers.

At most 4096 operations and 256 imported ranges are accepted. Encoded images and
aggregate import bytes are capped at 64 MiB. Configured semantic storage plus
record/fixed overhead must fit that image envelope. Values occupy a fixed
256-element array; history has bounded collection overhead in addition to its
accounted command allocations. Clone-based atomic staging and admission use
bounded additional working memory; no zero-copy or performance claim is made.
Calls run synchronously in the owner's serialized application context, perform
no I/O and create no asynchronous work to drain. Native snapshot publication and
ordinary Raft replay supply persistence, not these in-memory return values.

## Lifecycle integration still required

The next P6 slice must commit metadata intent, keep targets non-serving, commit
and apply source fencing, bind actual recoverable target import data, collect
exact committed receipts, publish and commit target activation. It must preserve
lineage in its records/checkpoints and check routed envelope keys against
`command_key` before executing scoped payloads. The existing fixed-grant wrapper
relies on host application semantics and is not a transfer activation mechanism.
After source fencing, recovery must move forward; there is no unfreeze fallback.
None of these lifecycle steps is implemented by importing a ScopeImage directly.

Downstream tests supply a host scope adapter with a distinct schema/format through
the same public trait. Native snapshot tests seal without publishing, reopen with
no published state, then publish/reopen and restore imported retries/outbox at the
target index. They establish provider/checkpoint behavior, not a committed Import,
quorum-ready receipt, split/merge completion, power-loss model or no-dual-owner
protocol. Linux evidence does not establish macOS execution.

The optional [source](SOURCE_FENCING.md) and [target](TARGET_IMPORTS.md) guards now
retain intent/fence/import provenance through existing durable application paths.
The source publishes quorum-readable image commitments; the target remains
non-serving after committed import. Metadata publication and activation still
require the remaining cross-group lifecycle protocol.
