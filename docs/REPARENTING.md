# Reparenting within one metadata authority

Select `Directory::with_local_reparenting()` before bootstrap. Schema11 binds
`VBDINI11` initialization and `VBDIR011` checkpoints. There is no live profile
upgrade. This path applies when both parents, the child, the moved subtree and
the affected ancestry belong to that metadata authority.

Read the current old parent, destination parent and child through that authority's
quorum. Construct `ReparentPlan::new(old_parent, new_parent, child)`, encode it and
propose it with a stable operation ID. The destination must contain an exact
same-scope `Vacant` selector. A live destination is not overwritten. Application
and partition schemes must match; the child must have its original parent and
exact existing locator.

One committed command changes all three manifests:

- The old parent's child selector becomes vacant.
- The destination's vacancy points to the same child identity and authority.
- The child's parent binding points to the destination.

Each generation advances once. Ownership epochs, concrete data groups, other
selectors, explicit placement requirements and descendant manifests stay unchanged.
The metadata authority checks current ancestry, rejects cycles and excessive
route depth, and refuses pending lifecycle work or unconsumed creation in affected
ancestors/subtrees. An unavailable or foreign ancestry segment is not treated as
an empty subtree. This branch refuses it.

`DirectoryQuery::Reparent(operation)` through `LifecycleDirectory` recovers the
original plan and commit index. Exact retries and checkpoint/journal replay retain
that result. Ordinary history capacity must be available before admission; there
is no separate irreversible effect requiring a reserved completion phase.

Select `NativeManifestCache::with_local_reparenting()` while the cache is empty to
accept these trusted views. The default cache retains its older refusal rules.
Refresh the affected parent and child entries from the original authority;
updating the destination before a stale child entry yields `WrongParent` until
the child is refreshed. After refresh, the old parent resolves the slot as vacant
and the destination resolves the same physical owner. The cache does not certify
commitment or authorize a child to serve.

Already-active data owners continue at their original epoch, including retained
parent ranges and nested descendants. Data, semantic retry history and outbox
entries do not move. Direct child hints still identify the same owned resource;
there is no new competing data owner or need to copy its state.

For original full owners, select `RoutedApplication::with_parent_adoption(maximum)`
before bootstrap. This binds schema3, the original manifest and a lifetime limit
of up to64 parent changes. Obtain the original `ReparentStatus` through the metadata
authority's quorum and authenticate that authority/configuration. Encode an
`OwnerParentAdoption` containing that exact decision and configuration, then
propose it with a stable operation ID to the original data owner. A constructible
status is not a commitment certificate; the embedding must verify its provenance.

The owner accepts only its exact current child manifest and derives the checked
new parent/generation from that plan. Data, ownership epoch, retry history, outbox
and concrete owner remain unchanged. Parent changes have a separate bounded
control reserve, so ordinary data-history exhaustion cannot consume their slots.
Exact retries return the original `ParentGrantStatus`, even after another change
or a full fence. `RoutedControlQuery::ParentAdoption(operation)` supplies the fixed
original result through the existing quorum-read path. Conflicting IDs, stale
grants and new changes after fencing refuse. Once the configured lifetime reserve
is exhausted, a fresh change refuses; no old evidence is evicted.

`TransferSource` supports this selected full-owner profile and exposes the same
status as `SourceQuery::ParentAdoption`. Its freeze/export path now uses the adopted
grant. Recovery starts with the original immutable bootstrap binding, reconstructs
the complete parent-change chain, and then validates a frozen transfer against
that recovered grant. The original freeze boundary and export digests remain
unchanged when an old adoption command is retried after fencing. The original
schema1 profile remains separate; this is not an automatic on-disk upgrade.

Scoped retained sources and activated/imported owner families are not enabled by
this selection. They have separate active-grant/lineage state and need their own
checked composition. Cross-authority reparenting still needs guarded ancestry and
a durable multi-authority protocol. These remain part of the reparenting roadmap.

`tests/reparenting.rs` covers live/nested data continuity, retained service,
repeated moves, exact retries/status, cycle/depth/foreign-path refusal, lifecycle
and capacity conflicts, cache selection and partial refresh, atomic checkpoint
failure and every-byte native journal cuts. This is finite application/storage
evidence, not native network reparenting or macOS execution evidence.
`tests/reparenting/owner.rs` adds actual metadata reparent -> original owner
adoption -> two-target split/publication/activation, retained original retries and
outbox, repeated moves, full-history control reserve, strict recovery and every-byte
native adoption/freeze frame failures. It uses application histories and the native
journal failure model; it does not add a new network or macOS execution claim.

Cross-authority preparation and cancellation are now available behind
`Directory::with_reparent_guards()` selected before bootstrap (schema12).
They are a protocol phase; they cannot publish a cross-authority move yet.

Build a `CrossReparentPlan` from the exact old parent, destination parent and child
identities and the complete manifest set: old parent, destination ancestry through
its root, and every manifest in the moved subtree. The canonical set is bounded
at128 manifests. Every edge, authority, scope, active state and application/scheme
must agree. Missing paths, extraneous nodes, cycles and excessive resulting depth
refuse. The planner may read these views separately, but their validity must then
be protected by the committed guards below.

The smallest participating authority is the coordinator. Propose
`PrepareReparent { plan, coordinator: None }` there with one global operation ID.
Read its original `ReparentGuardStatus` through that authority's quorum, authenticate
its configuration/provenance, and derive `ReparentGuardEvidence::from_status`.
Propose the same plan/global operation to the remaining authorities, with that
coordinator evidence. Acquire them in sorted order to avoid contention loops.
Each checks its exact local manifests and reserves completion capacity before
returning `ReparentGuarded`. The digest and constructible values do not authenticate
foreign state themselves.

While a guard is held, conflicting metadata publication, local reparenting,
creation and lifecycle reservations refuse. Disjoint metadata and ordinary data
writes can proceed. No owner epoch or parent binding changes at this phase.
Guards are reconstructed from the original log/checkpoint and do not expire.
A timeout is neither successful completion nor permission to unlock.

To abandon preparation, propose `CancelReparent { guard }` to its coordinator.
`DirectoryQuery::ReparentCancellation(guard)` returns the original cancellation
operation/index, plan digest and coordinator guard index. Authenticate that quorum
observation, then propose `ReleaseReparentGuard` at each participant. Its tombstone
may arrive before a delayed prepare; the later prepare stays cancelled. The
coordinator itself accepts only its actual local cancellation record. Original
prepare/cancel results remain available after release, so a retried original
prepare receipt is historical evidence, not a claim that the lock is still held.
`DirectoryQuery::ReparentGuard` and `ReparentCancellation` use existing Node reads.

Prepared participants have reserved cancellation capacity even if ordinary
history fills. An unprepared participant needs ordinary capacity to record an
early cancellation; if it has none, preparation also cannot acquire a guard.
Resume incomplete cancellation by reading original statuses and retrying it.
The follow-on publication phase must bind all original guards and serialize its
commit/cancel decision at the coordinator before any participant changes a route.
That phase, owner adoption across authority boundaries and native network
complete-move recovery remain required work.
