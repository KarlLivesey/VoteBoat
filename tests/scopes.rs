// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use voteboat::{application::*, bucket_counter::*, identity::*, log::*, routing::*, scope::*};
#[path = "scopes/preview.rs"]
mod preview;

#[derive(Clone, Debug)]
struct HostPolicy;
impl PartitionPolicy for HostPolicy {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(101).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [value] => Ok(u16::from(*value)),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}
fn range(start: u16, end: u16) -> BucketRange {
    BucketRange::new(start, end).unwrap()
}
fn limits() -> BucketCounterLimits {
    BucketCounterLimits {
        operations: 32,
        semantic_bytes: 8192,
    }
}
fn fresh(scope: BucketRange) -> BucketCounter<HostPolicy> {
    BucketCounter::new(scope, HostPolicy, limits()).unwrap()
}
fn op(id: u128) -> OperationId {
    OperationId::new(id).unwrap()
}
fn bytes(key: u8, delta: i64, outbox: &[u8]) -> Vec<u8> {
    encode_add(&[key], delta, outbox, MAX_BUCKET_COMMAND_BYTES).unwrap()
}
fn entry(index: u64, id: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: op(id),
            bytes,
        },
    }
}
fn add(
    app: &mut BucketCounter<HostPolicy>,
    id: u128,
    key: u8,
    delta: i64,
    outbox: &[u8],
) -> BucketReceipt {
    app.apply_batch(&[entry(
        app.applied_index() + 1,
        id,
        bytes(key, delta, outbox),
    )])
    .unwrap()
    .remove(0)
}
fn source() -> BucketCounter<HostPolicy> {
    let mut app = fresh(range(0, 256));
    add(&mut app, 10, 1, 7, b"left-effect");
    add(&mut app, 20, 200, 11, b"right-effect");
    add(&mut app, 30, 2, 3, b"");
    let noops: Vec<_> = (4..=100)
        .map(|index| LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Noop,
        })
        .collect();
    app.apply_batch(&noops).unwrap();
    app
}

#[test]
fn split_import_rebases_target_prefix_and_preserves_retry_results_and_outbox() {
    let source = source();
    let left = source.export_scope(range(0, 128), 10000).unwrap();
    let right = source.export_scope(range(128, 256), 10000).unwrap();
    assert_eq!(left.source_applied(), 100);
    let mut a = fresh(left.scope());
    let mut b = fresh(right.scope());
    a.import_scopes(&[left], 1).unwrap();
    b.import_scopes(&[right], 1).unwrap();
    assert_eq!(a.applied_index(), 1);
    assert_eq!(a.value(&[1]), Ok(7));
    assert_eq!(b.applied_index(), 1);
    assert_eq!(b.value(&[200]), Ok(11));
    assert!(a.value(&[200]).is_err());
    assert!(b.value(&[1]).is_err());
    assert_eq!(
        add(&mut a, 10, 1, 7, b"left-effect"),
        BucketReceipt {
            index: 2,
            operation: op(10),
            outcome: BucketOutcome::Value(7),
            duplicate: true
        }
    );
    assert_eq!(
        a.outbox().collect::<Vec<_>>(),
        vec![(op(10), &[1][..], &b"left-effect"[..])]
    );
    assert_eq!(
        b.outbox().collect::<Vec<_>>(),
        vec![(op(20), &[200][..], &b"right-effect"[..])]
    );
    assert_eq!(add(&mut a, 40, 1, 1, b"").outcome, BucketOutcome::Value(8));
    assert_eq!(
        add(&mut a, 10, 1, 7, b"left-effect").outcome,
        BucketOutcome::Value(7)
    );
    assert_eq!(a.value(&[1]), Ok(8));
    assert_eq!(
        add(&mut a, 10, 2, 7, b"left-effect").outcome,
        BucketOutcome::OperationConflict
    );
    assert_eq!(a.value(&[2]), Ok(3));
    let checkpoint = a.checkpoint(10000).unwrap();
    let mut recovered = fresh(a.scope());
    recovered
        .restore_checkpoint(1, a.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(
        recovered.outbox().collect::<Vec<_>>(),
        a.outbox().collect::<Vec<_>>()
    );
    assert!(add(&mut recovered, 30, 2, 3, b"").duplicate);
    assert_eq!(recovered.value(&[2]), Ok(3));
}

#[test]
fn compatible_scope_combination_preserves_data_history_and_original_outcomes() {
    let source = source();
    let images = [
        source.export_scope(range(0, 128), 10000).unwrap(),
        source.export_scope(range(128, 256), 10000).unwrap(),
    ];
    let mut merged = fresh(range(0, 256));
    merged.import_scopes(&images, 1).unwrap();
    assert_eq!(merged.outbox().count(), 2);
    assert_eq!(
        add(&mut merged, 20, 200, 11, b"right-effect").outcome,
        BucketOutcome::Value(11)
    );
    assert_eq!(
        add(&mut merged, 10, 1, 7, b"left-effect").outcome,
        BucketOutcome::Value(7)
    );
    assert_eq!(merged.value(&[1]), Ok(7));
    assert_eq!(merged.value(&[200]), Ok(11));
    assert_eq!(source.applied_index(), 100);
}

#[test]
fn import_refuses_gaps_overlap_order_collision_and_capacity_atomically() {
    let source = source();
    let left = source.export_scope(range(0, 128), 10000).unwrap();
    let right = source.export_scope(range(128, 256), 10000).unwrap();
    let mut target = fresh(range(0, 256));
    let before = target.checkpoint(10000).unwrap();
    for images in [
        vec![left.clone()],
        vec![right.clone(), left.clone()],
        vec![left.clone(), left.clone()],
        vec![],
    ] {
        assert!(target.import_scopes(&images, 1).is_err());
        assert_eq!(target.checkpoint(10000).unwrap(), before);
    }
    let mut colliding = fresh(range(128, 256));
    add(&mut colliding, 10, 200, 9, b"different");
    let collision = colliding.export_scope(range(128, 256), 10000).unwrap();
    assert_eq!(
        target.import_scopes(&[left.clone(), collision], 1),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(target.checkpoint(10000).unwrap(), before);
    let mut small = BucketCounter::new(
        range(0, 256),
        HostPolicy,
        BucketCounterLimits {
            operations: 2,
            semantic_bytes: 8192,
        },
    )
    .unwrap();
    let before = small.checkpoint(10000).unwrap();
    assert!(small
        .import_scopes(&[left.clone(), right.clone()], 1)
        .is_err());
    assert_eq!(small.checkpoint(10000).unwrap(), before);
    assert!(target
        .import_scopes(&[left.clone(), right.clone()], 100)
        .is_err());
    target
        .import_scopes(&[left.clone(), right.clone()], 1)
        .unwrap();
    assert!(target.import_scopes(&[left, right], 2).is_err());
}

#[test]
fn every_checkpoint_and_command_truncation_is_atomic_and_metadata_is_bound() {
    let source = source();
    let checkpoint = source.checkpoint(10000).unwrap();
    let mut app = fresh(range(0, 256));
    let before = app.checkpoint(10000).unwrap();
    for end in 0..checkpoint.len() {
        assert!(app.restore_checkpoint(1, 100, &checkpoint[..end]).is_err());
        assert_eq!(app.checkpoint(10000).unwrap(), before);
    }
    let command = bytes(1, 7, b"effect");
    for end in 0..command.len() {
        assert!(app
            .apply_batch(&[entry(1, 10, command[..end].to_vec())])
            .is_err());
        assert_eq!(app.checkpoint(10000).unwrap(), before);
    }
    let image = source.export_scope(range(0, 256), 10000).unwrap();
    for forged in [
        ScopeImage::new(
            2,
            image.scheme(),
            image.scope(),
            100,
            image.bytes().to_vec(),
        )
        .unwrap(),
        ScopeImage::new(1, image.scheme(), image.scope(), 99, image.bytes().to_vec()).unwrap(),
        ScopeImage::new(
            1,
            image.scheme(),
            range(0, 128),
            100,
            image.bytes().to_vec(),
        )
        .unwrap(),
    ] {
        assert!(app.import_scopes(&[forged], 1).is_err());
        assert_eq!(app.checkpoint(10000).unwrap(), before);
    }
    let mut wrong = fresh(range(0, 128));
    assert!(wrong.restore_checkpoint(1, 100, &checkpoint).is_err());
    assert!(app.restore_checkpoint(1, 99, &checkpoint).is_err());
    assert!(app.restore_checkpoint(2, 100, &checkpoint).is_err());
    let mut trailing = checkpoint;
    trailing.push(0);
    assert!(app.restore_checkpoint(1, 100, &trailing).is_err());
}

#[test]
fn pending_admission_capacity_overflow_and_failed_batches_preserve_state() {
    let mut app = BucketCounter::new(
        range(0, 256),
        HostPolicy,
        BucketCounterLimits {
            operations: 1,
            semantic_bytes: 100,
        },
    )
    .unwrap();
    let command = bytes(1, i64::MAX, b"effect");
    assert!(app
        .validate_proposal(op(1), &command, [(op(1), command.as_slice())].into_iter())
        .is_ok());
    assert_eq!(
        app.validate_proposal(op(2), &command, [(op(1), command.as_slice())].into_iter()),
        Err(ApplicationError::DedupCapacity)
    );
    let before = app.checkpoint(10000).unwrap();
    assert_eq!(
        app.apply_batch(&[entry(1, 1, command.clone()), entry(2, 2, command)]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(app.checkpoint(10000).unwrap(), before);
    let mut app = fresh(range(0, 256));
    add(&mut app, 1, 1, i64::MAX, b"effect");
    assert_eq!(
        add(&mut app, 2, 1, 1, b"never-deliver").outcome,
        BucketOutcome::Overflow
    );
    assert_eq!(app.outbox().count(), 1);
    assert_eq!(app.value(&[1]), Ok(i64::MAX));
    let image = app.export_scope(range(0, 128), 10000).unwrap();
    let mut target = fresh(range(0, 128));
    target.import_scopes(&[image], 1).unwrap();
    assert_eq!(
        add(&mut target, 2, 1, 1, b"never-deliver").outcome,
        BucketOutcome::Overflow
    );
    assert_eq!(target.outbox().count(), 1);
    assert!(BucketCounterLimits {
        operations: usize::MAX,
        semantic_bytes: usize::MAX
    }
    .checkpoint_bound()
    .is_err());
}

#[test]
fn rejection_returns_owned_policy_and_image_buffers_and_caps_query_capacity() {
    #[derive(Clone, Debug)]
    struct InvalidPolicy(Vec<u8>);
    impl PartitionPolicy for InvalidPolicy {
        fn scheme(&self) -> PartitionScheme {
            PartitionScheme {
                version: 0,
                ..HostPolicy.scheme()
            }
        }
        fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
            HostPolicy.bucket(key)
        }
    }
    let policy = InvalidPolicy(vec![9; 32]);
    let allocation = policy.0.as_ptr();
    let rejected = match BucketCounter::new(range(0, 256), policy, limits()) {
        Ok(_) => panic!("invalid policy"),
        Err(r) => r,
    };
    assert_eq!(rejected.policy.0.as_ptr(), allocation);
    let bytes = Vec::with_capacity(64);
    let allocation = bytes.as_ptr();
    let (_, bytes) = ScopeImage::new(1, HostPolicy.scheme(), range(0, 256), 0, bytes).unwrap_err();
    assert_eq!(bytes.as_ptr(), allocation);
    assert_eq!(bytes.capacity(), 64);
    let mut key = Vec::with_capacity(MAX_ROUTING_KEY_BYTES + 1);
    key.push(1);
    assert_eq!(
        fresh(range(0, 256)).query_bytes(&key, usize::MAX),
        Err(ApplicationError::ReceiptBudget)
    );
}

/// A downstream provider supplies its own scope/checkpoint format while using
/// the native deterministic counter as its application primitive.
#[derive(Clone)]
struct HostScope(BucketCounter<HostPolicy>);
impl StateMachine for HostScope {
    type Receipt = BucketReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<BucketReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl CheckpointStateMachine for HostScope {
    fn schema_version(&self) -> u64 {
        77
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut bytes = b"HOST".to_vec();
        bytes.extend(
            self.0.checkpoint(
                max_bytes
                    .checked_sub(4)
                    .ok_or(ApplicationError::InvalidCheckpoint)?,
            )?,
        );
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != 77 || !bytes.starts_with(b"HOST") {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.0.restore_checkpoint(1, applied, &bytes[4..])
    }
}
impl ScopeStateMachine for HostScope {
    fn contains_operation(&self, operation: OperationId) -> bool {
        self.0.contains_operation(operation)
    }
    fn scope(&self) -> BucketRange {
        self.0.scope()
    }
    fn scheme(&self) -> PartitionScheme {
        self.0.scheme()
    }
    fn command_key<'a>(&self, command: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        self.0.command_key(command)
    }
    fn export_scope_bound(&self, scope: BucketRange) -> Result<usize, ApplicationError> {
        self.0
            .export_scope_bound(scope)?
            .checked_add(4)
            .ok_or(ApplicationError::InvalidCheckpoint)
    }
    fn export_scope(
        &self,
        scope: BucketRange,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        let inner = self.0.export_scope(
            scope,
            max_bytes
                .checked_sub(4)
                .ok_or(ApplicationError::InvalidCheckpoint)?,
        )?;
        let mut bytes = b"HOST".to_vec();
        bytes.extend(inner.bytes());
        ScopeImage::new(77, self.scheme(), scope, inner.source_applied(), bytes).map_err(|e| e.0)
    }
    fn import_scopes(&mut self, images: &[ScopeImage], index: u64) -> Result<(), ApplicationError> {
        if images.len() > MAX_SCOPE_IMPORTS
            || images
                .iter()
                .try_fold(0usize, |n, image| n.checked_add(image.bytes().len()))
                .is_none_or(|n| n > MAX_SCOPE_IMAGE_BYTES)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let decoded = images
            .iter()
            .map(|image| {
                if image.schema() != 77 || !image.bytes().starts_with(b"HOST") {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                ScopeImage::new(
                    1,
                    image.scheme(),
                    image.scope(),
                    image.source_applied(),
                    image.bytes()[4..].to_vec(),
                )
                .map_err(|e| e.0)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.0.import_scopes(&decoded, index)
    }
}
#[test]
fn downstream_scope_provider_uses_public_contract_with_its_own_format() {
    fn transfer<A: ScopeStateMachine>(source: &A, target: &mut A) {
        assert!(source.contains_operation(op(10)));
        assert!(!target.contains_operation(op(10)));
        let image = source.export_scope(target.scope(), 10000).unwrap();
        assert_eq!(
            source.command_key(&bytes(1, 7, b"left-effect")).unwrap(),
            &[1]
        );
        target.import_scopes(&[image], 1).unwrap();
        assert!(target.contains_operation(op(10)));
        assert_eq!(target.applied_index(), 1);
        let checkpoint = target.checkpoint(10000).unwrap();
        target
            .restore_checkpoint(target.schema_version(), 1, &checkpoint)
            .unwrap();
        assert!(target.contains_operation(op(10)));
        assert!(!target.contains_operation(op(999)));
    }
    let source = HostScope(source());
    let mut target = HostScope(fresh(range(0, 128)));
    transfer(&source, &mut target);
    assert!(target.checkpoint(10000).unwrap().starts_with(b"HOST"));
    assert_eq!(
        add(&mut target.0, 10, 1, 7, b"left-effect").outcome,
        BucketOutcome::Value(7)
    );
    assert_eq!(target.0.value(&[1]), Ok(7));
    assert_eq!(target.0.outbox().count(), 1);
}

#[cfg(feature = "native")]
#[test]
fn native_snapshot_publication_recovers_imported_retry_history_at_target_boundary() {
    use voteboat::{native::snapshot_store::*, quorum::*, snapshot::*};
    let source = source();
    let image = source.export_scope(range(0, 128), 10000).unwrap();
    let mut target = fresh(range(0, 128));
    target.import_scopes(&[image], 1).unwrap();
    let bytes = target.checkpoint(10000).unwrap();
    let root = std::env::temp_dir().join(format!(
        "voteboat-scope-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let group = GroupIdentity {
        id: GroupId::new(91).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let store = StoreIdentity {
        id: StoreId::new(91).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let node = NodeId::new(1).unwrap();
    let identity = SnapshotIdentity { group, store };
    let metadata = SnapshotMetadata {
        bootstrap: Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: [(node, store)].into(),
        },
        membership: None,
        index: 1,
        term: 1,
        application_schema: 1,
    };
    // This is provider/checkpoint evidence, not an Import or activation receipt.
    let mut snapshots = NativeSnapshotStore::create(
        FileSnapshotIo::create(&root).unwrap(),
        identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    let ticket = snapshots.begin(metadata.clone(), bytes.len()).unwrap();
    snapshots.write_chunk(ticket, 0, &bytes).unwrap();
    snapshots.seal(ticket).unwrap();
    drop(snapshots);
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(&root).unwrap(),
        identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(snapshots.load().unwrap(), None);
    let ticket = snapshots.begin(metadata, bytes.len()).unwrap();
    snapshots.write_chunk(ticket, 0, &bytes).unwrap();
    let sealed = snapshots.seal(ticket).unwrap();
    snapshots.publish(sealed).unwrap();
    drop(snapshots);
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(&root).unwrap(),
        identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    let snapshot = snapshots.load().unwrap().unwrap();
    let mut app = fresh(range(0, 128));
    app.restore_checkpoint(
        snapshot.metadata.application_schema,
        snapshot.metadata.index,
        &snapshot.application,
    )
    .unwrap();
    assert_eq!(app.applied_index(), 1);
    assert_eq!(app.value(&[1]), Ok(7));
    assert_eq!(app.outbox().count(), 1);
    assert!(add(&mut app, 10, 1, 7, b"left-effect").duplicate);
    std::fs::remove_dir_all(root).unwrap();
}
