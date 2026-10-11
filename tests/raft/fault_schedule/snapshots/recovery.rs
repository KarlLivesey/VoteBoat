// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn lost(id: u64, acknowledged: &[LogEntry], anchored: bool) -> LostPrefix {
    LostPrefix {
        node: id,
        operation: acknowledged
            .iter()
            .rev()
            .find_map(|entry| match entry.payload {
                EntryPayload::Command { operation, .. } => Some(operation),
                _ => None,
            }),
        stage: if anchored {
            FailureStage::SnapshotRecoveryLost
        } else {
            FailureStage::SnapshotPublicationLost
        },
    }
}
impl Replay {
    pub(in crate::fault_schedule) fn discard_snapshot(&mut self, id: u64, mode: crash::Mode) {
        self.snapshots.stages.remove(&id);
        self.snapshots.providers.remove(&id);
        self.snapshots.incoming.remove(&id);
        if let Some(image) = self.snapshots.images.get(&id) {
            image.crash(mode);
        }
    }
    pub(in crate::fault_schedule) fn recover_snapshot(
        &mut self,
        id: u64,
        store: NativeLogStore<TraceIo>,
        acknowledged: &[LogEntry],
    ) -> Result<(), LostPrefix> {
        let failure = || {
            lost(
                id,
                acknowledged,
                store.state(group(1)).unwrap().snapshot.is_some(),
            )
        };
        let mut provider = NativeSnapshotStore::recover(
            self.snapshots.images[&id].clone(),
            snapshot_identity(id),
            limits(),
        )
        .map_err(|_| failure())?;
        self.snapshots
            .recovered_roots
            .insert(id, provider.latest_reference().unwrap());
        let mut application = fresh();
        // These are the only recovery inputs: reopened native WAL, native
        // snapshot device and a fresh application. The oracle is compared later.
        let (core, _) =
            recover_replica(node(id), group(1), &store, &mut provider, &mut application)
                .map_err(|_| failure())?;
        if core.state().commit_index < acknowledged.len() as u64 {
            return Err(failure());
        }
        let mut expected = acknowledged.to_vec();
        expected.extend(
            core.replay_committed()
                .iter()
                .filter(|e| e.index > acknowledged.len() as u64)
                .cloned(),
        );
        assert_application(&application, &expected).map_err(|_| failure())?;
        self.cluster.applied.insert(id, expected);
        self.cluster.replicas.insert(id, Replica { core, store });
        self.snapshots.providers.insert(id, provider);
        self.snapshots.counts.recoveries += 1;
        Ok(())
    }
}
pub(super) fn assert_application(
    application: &Counter,
    expected: &[LogEntry],
) -> Result<(), CheckpointError> {
    let mut oracle = fresh();
    oracle.apply_batch(expected)?;
    if application.checkpoint(MAX_FAULT_BYTES)? != oracle.checkpoint(MAX_FAULT_BYTES)? {
        return Err(CheckpointError::InvalidBoundary);
    }
    Ok(())
}
