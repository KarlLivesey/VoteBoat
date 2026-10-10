# Moving metadata authority

Source fencing, import, publication and destination activation are implemented
for Rust embedding, including original/retained/imported owner metadata adoption
and foreign directory/cache locator refresh, owner adoption and selected repeated
moves. Selected native TCP/TLS and QUIC recovery histories exercise these paths;
an imported image alone does not authorize a new metadata group.

`MetadataAuthoritySource` wraps a `LifecycleDirectory` before its first bootstrap
and shares its existing Raft log, storage and runtime contracts. Construction
selects a fixed export budget, large enough for the directory's declared snapshot
bound, up to64 MiB. No second store or worker is created.

`MetadataMovePlan` binds the source/destination groups and every current local
manifest. It checks local parent/child closure, generations and destination
identity conflicts. Its derived manifests change metadata authority, local
parent/child references and route generations. Responsibility identities,
concrete data groups and ownership epochs stay unchanged. This structural plan
does not provision or activate the destination.

| Source state | Directory service | Export |
| --- | --- | --- |
| Before selected bootstrap | Uninitialized | Unavailable |
| Live, settled directory | Normal proposals and quorum reads | Plan can be prepared |
| Open lifecycle reservation | Existing lifecycle can progress | Freeze refused |
| Committed freeze at F | Mutations and authoritative reads fenced | Exact original directory checkpoint at F |

Use the wrapper's `bootstrap_command` to bind its profile, then normal directory
commands. The live-only `directory()` view supplies existing command builders
and local diagnostics, which still require matching quorum observations. Prepare `plan(destination)` and submit `freeze_command` with a fresh
operation ID. Pending admission and apply recheck the complete source view.
There is reserved control capacity for this fence even when ordinary operation
history is full. A stale plan cannot freeze a changed directory. Rejected
control IDs remain reserved in a separate bounded ledger, carried with their
original source indices and digest in the export. Filling that failure ledger
does not consume the successful-fence reserve.

After F, query `MetadataSourceQuery::Status` through the normal quorum-read path.
`export` returns immutable checkpoint bytes, plan and source status. The checkpoint and rejection-ledger digests,
schema, source group and F identify the original authority/index domain. Local
method results alone are not authenticated foreign quorum evidence. The host
must carry verified source observations into the later target protocol.

The wrapper's applied prefix can advance while its inner directory remains at F.
Exact fence retries return the original status. Later directory commands cannot
revive service. Recovery binds the initial profile, export budget, original
bootstrap, plan, F and image digest; it preserves all original directory command
history. There is no timeout-based unfreeze.

The selected downstream tests cover source/plan bounds, closed subtrees, stale
and pending views, exhausted history, exact retries, malformed checkpoints and
native journal interruption at every fence-frame byte. They establish the source
half, not end-to-end authority movement or native socket deployment.

`MetadataAuthorityTarget` binds a pristine source profile, complete plan, transfer
operation and source configuration before bootstrap. Commit its selected
`bootstrap_command`, then `import_command` with the source image and verified
configuration. Quorum status reports the destination's own stage/import indices
separately from source F. Exact retries and checkpoints preserve both domains.

`historical_directory()` exposes the original source history for inspection,
including original operation results. It is not a serving destination directory.
The target validates image/plan/rejection digests, bootstrap, complete manifests
and original indices; no directory command may occupy the source fence or a
rejected-control index. Changed imports and wrong construction bindings refuse.

This basic target profile accepts no activation or directory service. Select
`MetadataPublishingSource` and `MetadataServingTarget` before bootstrap for the
complete publication/activation path. After import, propose the source wrapper's
`publication_command` with the verified import and destination configuration.
Obtain its committed publication, then propose the destination wrapper's
`activation_command`. Both commands retain exact original results on retry.

Activation creates a private writable base with updated authority, local
references and generations. It preserves allocation reservations and original
operation outcomes. Checkpoints keep the original image and a bounded tail of
new destination commands; recovery reconstructs both domains. `Directory` queries
serve current manifests after activation. Inherited operation queries/receipts
are explicitly marked historical with the source identity; `Creation` queries
likewise name the original authority and return bounded encoded intents.

The creation-read budget includes the fixed result envelope plus the smaller of
the directory's immutable history capacity and the global creation-command
maximum. Reserve that full bound in the host read runtime. The bound is stable
across new reservations and metadata moves; it does not depend on current
history occupancy or the size of the currently selected record.

The source remains fenced after publication. New metadata writes and namespace
creation after activation are covered, as are original retries, checkpoint
truncation and native journal interruption of publication, activation and a later
write. These are selected embedding/modelled-journal tests.

For an original full data owner, select
`RoutedApplication::with_metadata_authority_adoption` before bootstrap. Build
`OwnerMetadataAdoption` from the complete plan, selected responsibility and
verified destination activation, then propose its encoding to the owner.
The existing bounded grant ledger preserves data, retries, outbox and earlier
parent changes. `MetadataAdoption` queries return the original owner receipt and
activation together, keeping the source and destination index domains explicit.
The same profile composes with `TransferSource` for later data splits.

For a retained owner, select `ScopedTransferSource::with_retained_grants`, then
`with_metadata_authority_adoption` before bootstrap. Schema6 uses the same ordered
grant ledger for retained, parent/slot and metadata changes, reserving bounded
space for complete observations. Its `MetadataAdoption` read includes original
activation provenance. Earlier scoped exports stay immutable; later retained
transfers can proceed through the new metadata authority after adoption.

For an imported owner, select `TransferTarget::with_metadata_authority_adoption`
before bootstrap. Select `with_partial_delegation` afterward if it must retain
scopes while delegating others. Profiles8/9 preserve the original import and
activation while recording metadata changes in the existing grant history.
The same `RetirementGuard` validates that history against the final source grant
after a later handoff, then retains it without the retired application payload.
`TargetQuery::MetadataAdoption` returns the original adoption provenance.

The native cache's opt-in `with_metadata_authority_moves` accepts only the exact
metadata/reference/generation transformation through `ManifestCache`. Verify
the move before supplying these hints. Partial parent/child refresh fails route
resolution; cache contents do not authorize writes.

For a foreign parent or child authority, select
`Directory::with_metadata_locator_updates` before bootstrap (schema15). Build a
`MetadataLocatorUpdate` from its exact current manifest, the complete move plan
and authenticated original activation. Propose its encoding to that authority.
It checks reciprocal responsibility/scope/epoch links, updates all matching
parent/child references with generation +1, and preserves data ownership. A
stale manifest or an open lifecycle/guard refuses the update. Each authority
commits independently; no cross-authority atomic transaction is implied.

The original operation/index, command digest and activation are available through
`DirectoryQuery::MetadataLocator`. Obtain them through the original authority's
quorum. Successful updates share the bounded control pool, with a lifetime count
no larger than the configured ordinary operation count. Complete commands above
`MAX_METADATA_LOCATOR_BYTES` are refused; no plan is truncated. Checkpoints replay
original commands and exact retries return the original outcome.

Select the cache's separate `with_metadata_locator_updates` option before admitting
hints. It accepts only the complete same-authority reference transformation for
one foreign source/destination pair. Partial authority refresh still refuses
incompatible routes. This is a hint check, not data-owner grant adoption.
A second metadata export and native TCP/QUIC authority-move composition remain work.

To adopt a foreign locator result into a data owner, select
`with_metadata_locator_adoption(maximum)` before bootstrap. It selects a distinct
immutable profile and includes the earlier metadata/parent command families:

| Owner | Schema | Bootstrap / checkpoint |
| --- | --- | --- |
| RoutedApplication | 6 | VBROWN06 / VBROUT06 |
| ScopedTransferSource, after retained grants | 7 | VBSCOWN7 / VBSCCHK7 |
| TransferTarget | 10 | VBTSOWN8 / VBTRGT11 |
| TransferTarget, then partial delegation | 11 | VBTSOWN9 / VBTRGT12 |

Construct `OwnerMetadataLocatorAdoption` from the complete update, its original
`MetadataLocatorStatus` and directory configuration. Authenticate that original
authority's quorum observation before submitting the command to the owner. The
owner must hold the exact before grant; metadata references and generation change
without changing data ownership, retries or exports. These commands share the
existing bounded metadata/parent history reserve. Old profiles reject them.

The `MetadataLocatorAdoption` query returns both the local owner receipt and the
original directory observation. Original operation IDs and index domains survive
retries, checkpoints and later fencing. Scoped retained changes interleave in the
same history. Imported full/partial retirement uses new lineage tags VBTPLRL1 and
VBTPRTL3, reconstructs the entire grant history and checks the exact final grant.
A missing adoption cannot be replaced by simply normalizing metadata fields.

For another move, select `MetadataAuthoritySource::from_serving(target, budget)`
with a **pristine** `MetadataServingTarget`, then wrap it in
`MetadataPublishingSource` before the target's bootstrap. This is a construction
choice, not a live profile upgrade. The repeated source/publication schemas are
3/4; the next import/serving schemas are 3/4. The original schemas remain unchanged.
The complete previous serving checkpoint becomes the next source image, including
older histories. Follow the same freeze, import, publish and activate sequence.

Use `source().active_directory()` for current local command builders. The repeat
source's `MetadataSourceQuery::Serving` forwards provenance-aware reads, including
creation observations; all directory service is fenced after the next freeze.
On the next destination, inherited reads/receipts name the actual original
authority and index, including histories older than its immediate predecessor.
`historical_directory()` is only the oldest original directory diagnostic;
`MetadataServingQuery` supplies the provenance-aware history view.

Construction refuses previously used numeric authority identities, even with a
new incarnation. Retained lineage permits at most `MAX_METADATA_MOVES` successive
moves and keeps the separate 64 MiB image ceiling; declared snapshot budgets may
refuse another move earlier. There is no implicit history pruning or source thaw.
Selected embedding tests cover three consecutive moves and native journal cuts.
Use `source().serving_target()` to borrow the repeated profile's existing import
and activation command builders before its next freeze. Plain source profiles
and frozen sources return `None`. This local accessor supplies no foreign quorum
authority; authenticate the observations supplied to those builders as before.

Selected native TCP/TLS and QUIC histories move A->B->C, refresh a foreign parent
locator and adopt both moves in an original full data owner. Every selected phase
survives an unread completion, joined worker abort, WAL/checkpoint reopen and
exact retry. The owner serves and recovers with all metadata stopped; stopped
metadata files and recovered logs remain unchanged.

Selected retained/imported histories also cover an initial retained split,
followed by a root/child metadata move from A to B. TCP/TLS and QUIC, WAL and
checkpoint recovery, and ordinary/partial-delegation-capable imported profiles
preserve initial activation, transfer publication provenance, frozen exports,
data retries and outbox counts. Both data owners recover and write with A and B
offline; their metadata files and logs remain unchanged. Later native handoffs
and retirement are covered below. Repeated moves for these families, wider
faults and macOS validation remain separate work.

Selected native histories additionally complete a second retained-range handoff
under the moved authority. They preserve the original and new exports, adopt
the remaining source grant, activate the new child, and recover independent
writes/retries at all three owners with both metadata services offline. This
path is covered over TCP/TLS and QUIC with WAL and checkpoint recovery.

Selected imported-owner histories complete a later full handoff, or first
delegate a strict subrange and then transfer the remainder, under the moved
metadata authority. The imported owner uses a RetirementGuard selected before
bootstrap. Incomplete retirement evidence refuses; all destination activations
and an explicit retention release precede retirement. WAL replay preserves the
tombstone and lineage, and checkpoint/reclamation removes the old command payload
without restoring a serving owner. Successors retain retries and accept new
writes with both metadata services offline. TCP/TLS and QUIC each cover WAL and
checkpoint recovery for both ordinary and partial-delegation histories.
