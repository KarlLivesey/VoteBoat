# Recoverable responsibility deletion

Select `Directory::with_namespace_deletion()` before bootstrap. Schema9 binds
`VBDINIT9`/`VBDIR009`; there is no live upgrade from earlier directory profiles.
The same directory log/checkpoint owns deletion locks and tombstones.

A trusted coordinator performs these steps:

1. Read the original active responsibility manifest through its metadata quorum.
   Propose `DeletionIntent { before }` with a stable operation ID. Recover its
   original `DeletionIntentStatus` using `DirectoryQuery::DeletionIntent(id)`.
2. Delete each delegated child bottom-up. Authenticate each child's original
   metadata quorum and configuration, read its published `DeletionStatus`, then
   derive `ChildDeletionEvidence::from_status`. Do not infer deletion from a
   missing child, unavailable process or locally manufactured status.
3. Use `RoutedControlReads` to expose data and fixed full-fence queries over an
   existing `RoutedApplication`. The original commands/schema/checkpoint stay the
   same. After receipt loss, read `RoutedControlQuery::Fence` through the original
   Node quorum; local `read_at` and `routed().fence()` alone establish no quorum.
   A scoped fence still returns `None` from this full-fence query.
   Commit a full fence at every distinct concrete owner, using the original
   deletion intent operation and ownership epoch. Authenticate the original owner
   quorum/configuration and retain its exact immutable `OwnershipFence`. No scope
   fence, stale epoch, unknown index or different operation substitutes for it.
4. Propose `DeletionCompletion` containing the original intent status, owner facts
   in group order and child facts in responsibility order. Read
   `DirectoryQuery::Deletion(intent_id)` to recover the original publication result.

Completion checks exact source/child coverage and original intent operation/index.
Same-authority children require matching actual local tombstone records; foreign
observations require host authentication and commitment verification before proposal.
These values and hashes check content/shape and are not cryptographic certificates.
The coordinator must reauthorize effects; encoding a command authorizes nothing.

Successful publication retains the original manifest with `Fenced` state and the
next route generation. The original ownership epoch and all identities/locators
remain. Routing refuses the tombstone. Already fenced owners refuse old-context
reads/writes independently of metadata availability. Original intents, tombstones,
commands and retry outcomes survive checkpoint/replay; new lifecycle or creation
requests cannot revive a deleted identity.

Deletion is forward-only after reservation. Control capacity is reserved before
owner fencing, so completion can proceed after ordinary history fills. Unfinished
transfer/delegation or unconsumed group creation refuses deletion. Resolve those
existing operations first; unresolved-creation cancellation is not implemented here.
Retained physical data, retries, outbox and fences are preserved. Deletion provides
no physical reclamation permission or automatic expiry of tombstone records.

Schema10 additionally supports [retiring an already-deleted child selector](CHILD_SLOTS.md)
to an explicit vacancy. Its parent retains every other route and its ownership
epoch; later deletion requires no owner or child fact for vacant selectors.

`tests/deletion.rs` covers original routed-owner fences, leaf and same/foreign
recursive deletion, a three-authority chain, capacity/pending/profile refusal,
checkpoint/codec truncations, exact retries and every-byte native journal faults
at intent/tombstone publication. These are finite application/storage histories.
Selected native TCP/QUIC WAL/checkpoint histories now pass unread phase-result
recovery across two metadata authorities and two original routed owners. Original
quorum facts/retries survive every phase; both metadata authorities stop during
independent owner recovery, retained values/retry/outbox remain, old service stays
fenced and late metadata recovery cannot thaw it. These are finite loopback
histories; wider owner families and retention/cancellation remain open.
