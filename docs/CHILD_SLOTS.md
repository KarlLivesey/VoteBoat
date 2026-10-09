# Retiring a deleted child slot

Select `Directory::with_child_slot_retirement()` before bootstrap. This selects
schema10, `VBDINI10` initialization and `VBDIR010` checkpoints. There is no live
upgrade from another directory profile.

After [recursive deletion](DELETION.md) has published a child's tombstone, its
parent can replace that exact child selector with `RouteTarget::Vacant`:

1. Read the current parent manifest and the child's original published
   `DeletionStatus` through their authenticated metadata quorums.
2. Derive `ChildDeletionEvidence::from_status` using the observed child
   configuration. Propose `RetireChildSlot { before, child }` with a stable
   operation ID. The parent checks the child's identity, authority, parent,
   ownership epoch and exact scope. Same-authority evidence must match the actual
   local deletion record. Foreign evidence requires the host's original quorum
   verification; a constructible value or digest is not a certificate.
3. Recover the original result through
   `DirectoryQuery::RetiredChildSlot(operation)`. The lifecycle read view shares
   the original directory log, checkpoint and applied boundary.

Retirement atomically advances the parent's route generation once. Its ownership
epoch and every other selector stay unchanged. Pending parent deletion,
transfer, delegation or unconsumed creation refuses the change. The operation
uses ordinary bounded history capacity and has no external second phase to
reserve. If admission fails, the existing child tombstone remains safe and the
parent route stays unchanged. Reopening and retrying the same command preserves
the original outcome and index.

`resolve` returns `RoutingError::Vacant` immediately for an empty selector, without
looking up a child. Retained concrete-owner routes still work at the original
ownership epoch. A stale child hint reaches an already fenced child owner.
`NativeManifestCache` accepts an authenticated newer generation that retires child
selectors; it cannot use this rule to remove concrete owners, restore vacant
selectors, regress a child epoch or change identities/ranges.

Vacancies preserve complete selector coverage. They are legal in `Delegated`
manifests, including an entirely vacant namespace, and forbidden in `Partitioned`
manifests. Manifests containing vacancies use `VBMAN002`; manifests without them
keep the original `VBMAN001` bytes. Original directory profiles reject vacancy
bootstrap plans and commands, and downgraded manifest tags fail to decode.

The existing metadata owner retains retired child identities, tombstones and
original command history; retirement does not authorize identity reuse or physical
reclamation. Deleting a vacant namespace needs no invented child/owner facts for
its empty selectors. Existing transfer/insertion protocols do not refill them.
Moving a live child and adopting it under another parent remains separate work.

`tests/child_slots.rs` checks local/foreign tombstones, retained service and stale
child fencing, successive retirements, exact replay, capacity/atomicity, lifecycle
conflicts, profile/codec refusal, cache transitions and every-byte native journal
cuts. These are finite application/storage checks; no network reparenting or
macOS execution is claimed.
