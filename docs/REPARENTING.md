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

This completes the local metadata move, not the full reparenting roadmap. Existing
data-owner wrappers still retain their original full manifest grants; explicit
grant adoption is required before a later ownership transfer can use the new
parent binding. Cross-authority reparenting still needs guarded ancestry and a
durable multi-authority protocol. Neither is implemented by this command.

`tests/reparenting.rs` covers live/nested data continuity, retained service,
repeated moves, exact retries/status, cycle/depth/foreign-path refusal, lifecycle
and capacity conflicts, cache selection and partial refresh, atomic checkpoint
failure and every-byte native journal cuts. This is finite application/storage
evidence, not native network reparenting or macOS execution evidence.
