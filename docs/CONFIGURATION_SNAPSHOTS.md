# Configuration-aware snapshot bases

Snapshots now preserve the configuration at their included log boundary. This
completes the storage foundation needed to compact membership history; online
Raft reconfiguration remains disabled pending all-quorum, learner, context and
service-removal integration and the required protocol model/network histories.

`SnapshotMetadata::membership`, `GroupLog::snapshot_membership`, and
`LogUpdate::snapshot_membership` carry the same validated, owned membership base.
None means the original bootstrap configuration. A base includes the stable
configuration, any joint old/new transition and its operation/index, the last
configuration-record index, and retained operation identities. Bootstrap remains
the immutable creation identity and original voter/store map. It does not become
a mutable substitute for accepted-log membership.

`GroupLog::membership_at` derives the configuration at a particular retained
boundary; `checkpoint_membership` supplies the canonical optional base.
`membership` replays the surviving suffix from that base. Consequently a joint
snapshot followed by an uncommitted final entry recovers new-only rules, and
rolling back that final restores the joint predicate from the snapshot. A later
final checkpoint retains new membership and operation identities even when no
configuration entries remain in the WAL suffix. Commands and deduplication remain
in the application's checkpoint/replay contract.

The shared logical validator binds the base and snapshot reference in one atomic
update. The reference's configuration ID is the included effective configuration,
which can differ from both original bootstrap and the current accepted suffix.
For a matching index/term, the supplied base must exactly equal the configuration
derived from the local prefix, including operation history. A nonmatching snapshot
cannot regress committed configuration identity/index or discard its operation
identities. Membership metadata without a snapshot is rejected. All existing
commit-prefix, hard-state, snapshot-pin and suffix-generation rules still apply.

A later published snapshot cannot regress its configuration or rewrite a policy
under the same configuration ID. New configuration records must lie beyond the
previous published boundary and preserve its operation identities.
`SnapshotMetadata::follows` supplies this mandatory check for native and host
providers. A snapshot's validation establishes structure, not quorum provenance;
the core still controls install authorization and persistence dependencies.

Operation identities are retained across every compaction. The journal now has a
hard ceiling of 16,384 configuration operations; exhaustion rejects subsequent
transitions with `HistoryFull`. Nothing is silently evicted or treated as a fresh
operation. Snapshot/WAL metadata byte budgets can impose an earlier limit. A
future durable operation-retirement policy must preserve retry semantics before
relaxing this conservative ceiling.

Native snapshots with a membership base use `VBSNAP02`. The fixed header and CRC
still bind application data and boundary metadata. Its bounded metadata contains
a length-delimited original bootstrap batch followed by membership-checkpoint
subformat 1: stable configuration, last configuration index, optional joint state,
and the bounded operation-ID set. Static snapshots still use `VBSNAP01`; the native
codec reads both. Codec capability version 2 declares membership preservation and
legacy readability. Version-1 host codecs remain usable for static snapshots but
are refused for configuration-bearing publication/recovery before mutation.
Unknown versions, lengths/counts, duplicate identities, invalid checkpoint states
and trailing bytes fail closed. Seal checks exact metadata round-trip, so a
provider cannot declare support and then silently discard the base.

Format-2 native WAL snapshot mutation tag 3 adds the same membership base after
the existing snapshot reference. Static tag 2 remains unchanged. Full-image WAL
reclamation encodes and revalidates the base and suffix together, restoring exact
revision, suffix generation and physical batch sequence. Older binaries reject
new mandatory data; downgrade requires explicit migration. There is no fallback
that forgets membership to reopen a store.

No new durability tokens or escaping effects are introduced. Publish/seal do not
permit log deletion. The selected snapshot must first be durably pinned; the
matching WAL snapshot/base update must then complete its exact barrier. Only then
can the previous pin be reconciled away. Existing snapshot generations identify
immutable roots, and existing log generations fence suffix/snapshot replacement.
Recovery selects the authoritative WAL reference and its matching pinned data.
A lost receipt cannot promote a written transition to durable evidence.

Membership tree/maps and operation-set allocations are charged in snapshot-image,
transport/ingress, persistence-worker and effect accounting. Configuration-bearing
persistence uses data credits rather than consuming reserved control capacity.
Native metadata encoders/decoders enforce both encoded and retained-size budgets;
worker output allowances continue to fence oversized host results.

Ten downstream snapshot tests extend the journal conformance tests. Shared
host/native histories compact a joint base, roll back final, finalize later,
restore original application retry outcomes, compact all entries, and reject
operation reuse after compaction. Native tests reopen actual files, reclaim and
recover full-image WALs, cut every snapshot-prefix and application/footer write,
fail synchronization/publication, and cut every byte of the WAL reference/base
switch with both roots pinned. Codec capability and no-mutation rejection are
checked. Restore/compaction/install/send helpers also require the exact membership
base, and a regression rejects altered metadata under the same configuration ID
without advancing the application. A native codec unit test covers every checkpoint truncation, versions,
flags, oversized counts, changed operation identity, boundary and trailing bytes.

These tests exercise storage, codecs, application replay and dependency ordering.
They do not enable or prove joint Raft elections/commit/read behavior. The static
core explicitly refuses configuration-bearing Append, Snapshot, and recovery
state, including compacted membership bases. Default wire format 1 also refuses
this state; explicitly selected [wire format 2](WIRE_FORMAT.md) can carry it.
Live learner catch-up, all-quorum activation, stale-context handling, removed-node
service fencing, the formal protocol model and faulted network histories remain
next. Linux tests do not establish macOS execution or power-failure certification.
