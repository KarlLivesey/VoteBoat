// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host codec/application injection for a legitimate zero-byte checkpoint.
use super::*;
use crate::contracts::StorageError;
#[derive(Clone)]
struct EmptyCodec;
impl SnapshotCodec for EmptyCodec {
    fn format_version(&self) -> u32 {
        77
    }
    fn prefix(
        &self,
        metadata: &SnapshotMetadata,
        application_bytes: usize,
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        if application_bytes != 0 {
            return Err(StorageError::Rejected("empty-only test format"));
        }
        let mut prefix = NativeSnapshotCodec.prefix(metadata, 1, limits)?;
        prefix.push(0); // Format-owned sentinel; the application's encoding is empty.
        Ok(prefix)
    }
    fn finish(&self, body: &[u8], limits: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        NativeSnapshotCodec.finish(body, limits)
    }
    fn decode(&self, bytes: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError> {
        let mut snapshot = NativeSnapshotCodec.decode(bytes, limits)?;
        if snapshot.application != [0] {
            return Err(StorageError::Corrupt("empty-only sentinel"));
        }
        snapshot.application.clear();
        Ok(snapshot)
    }
}
#[derive(Clone)]
struct EmptyApplication {
    applied: u64,
}
impl StateMachine for EmptyApplication {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        for entry in entries {
            if entry.index != self.applied + 1
                || matches!(entry.payload, EntryPayload::Command { .. })
            {
                return Err(ApplicationError::InvalidCommand);
            }
            self.applied = entry.index;
        }
        Ok(Vec::new())
    }
}
impl CheckpointStateMachine for EmptyApplication {
    fn schema_version(&self) -> u64 {
        // The fixture plan's source schema (the Counter's).
        Counter::new(16).unwrap().schema_version()
    }
    fn checkpoint(&self, _: usize) -> Result<Vec<u8>, ApplicationError> {
        Ok(Vec::new())
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != Counter::new(16).unwrap().schema_version() || !bytes.is_empty() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.applied = index;
        Ok(())
    }
}
#[test]
fn host_empty_checkpoint_codec_imports_and_restores_the_actual_zero_byte_application() {
    let template = candidate();
    let mut input = import(&template);
    let mut source = EmptyApplication { applied: 0 };
    source
        .apply_batch(std::slice::from_ref(&input.learners))
        .unwrap();
    input.snapshot.application = source.checkpoint(0).unwrap();
    let mut a = LogicalApplication::new_enrollment_candidate(
        template.mapping,
        template.child_bootstrap.clone(),
        template.parent_bootstrap.clone(),
        template.limits,
        (NativeLogCodec, EmptyCodec, template.wire_codec),
        fixture::plan(),
    )
    .unwrap();
    input.origin.export.reference = SnapshotRef {
        store: store(1),
        ..a.snapshot_reference(&input.snapshot, SnapshotGeneration::new(9).unwrap())
            .unwrap()
    };
    let c = enrollment(input, 11);
    let e = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: c.operation,
            bytes: a
                .encode_command(&c, a.limits.max_command_bytes)
                .unwrap()
                .into(),
        },
    };
    assert_eq!(
        a.apply_batch(&[e]).unwrap()[0].outcome,
        LogicalOutcome::Claimed
    );
    assert!(a.state.snapshots[0].1.application.is_empty());
    let binding = StoreBinding {
        identity: store(3),
        session: StoreSession::new(1).unwrap(),
    };
    let core = crate::raft::Raft::recover_member_verified(
        NodeId::new(3).unwrap(),
        binding,
        a.state.parent.clone().unwrap(),
        a.limits.parent_log,
    )
    .unwrap();
    let mut target = EmptyApplication { applied: 0 };
    crate::snapshot::restore_application_image(
        &core,
        &mut target,
        Some(a.state.snapshots[0].1.clone()),
        a.limits.parent_snapshots,
    )
    .unwrap();
    assert_eq!(target.applied_index(), source.applied_index());
    assert!(target.checkpoint(0).unwrap().is_empty());
}
