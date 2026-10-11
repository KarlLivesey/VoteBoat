// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl Replay {
    pub(in crate::fault_schedule) fn snapshot_effect(
        &mut self,
        id: u64,
        effect: &Effect,
        queue: &mut VecDeque<Effect>,
    ) -> bool {
        let result = match effect {
            Effect::SnapshotRequired {
                to,
                context,
                reference,
            } => {
                let core = &self.cluster.replicas[&id].core;
                supply_snapshot(
                    core,
                    self.snapshots.providers.get_mut(&id).unwrap(),
                    *to,
                    *context,
                    *reference,
                )
            }
            Effect::StageSnapshot(message) => self.stage_incoming_snapshot(id, message),
            Effect::SnapshotInstalled(reference) => self.finish_incoming_snapshot(id, *reference),
            _ => return false,
        };
        match result {
            Ok(effects) => queue.extend(effects),
            Err(_) => {
                self.snapshots.counts.failures += 1;
                if matches!(
                    effect,
                    Effect::StageSnapshot(_) | Effect::SnapshotInstalled(_)
                ) {
                    self.cluster
                        .replicas
                        .get_mut(&id)
                        .unwrap()
                        .core
                        .storage_failed();
                }
            }
        }
        true
    }
    fn stage_incoming_snapshot(
        &mut self,
        id: u64,
        message: &Message,
    ) -> Result<Vec<Effect>, CheckpointError> {
        let application = self.snapshot_counter(id)?;
        self.snapshots.ensure(id);
        // Retain source history only as an independent comparison oracle. The
        // real image travels in the real Raft message and enters native storage.
        let Rpc::Snapshot { snapshot } = &message.rpc else {
            unreachable!()
        };
        let index = usize::try_from(snapshot.metadata.index).unwrap();
        let expected = self.cluster.applied[&message.from.get()][..index].to_vec();
        let replica = self.cluster.replicas.get_mut(&id).unwrap();
        let effects = stage_snapshot_effect(
            &mut replica.core,
            &mut replica.store,
            self.snapshots.providers.get_mut(&id).unwrap(),
            &application,
            message.clone(),
        )?;
        self.snapshots.incoming.insert(id, expected);
        Ok(effects)
    }
    fn finish_incoming_snapshot(
        &mut self,
        id: u64,
        reference: SnapshotRef,
    ) -> Result<Vec<Effect>, CheckpointError> {
        let replica = self.cluster.replicas.get_mut(&id).unwrap();
        let mut application = fresh();
        let effects = finish_snapshot_install(
            &mut replica.core,
            &replica.store,
            self.snapshots.providers.get_mut(&id).unwrap(),
            &mut application,
            reference,
        )?;
        let mut expected = self.snapshots.incoming.remove(&id).unwrap();
        assert!(expected.starts_with(&self.cluster.applied[&id]));
        expected.extend(replica.core.replay_committed().iter().cloned());
        recovery::assert_application(&application, &expected)?;
        self.cluster.applied.insert(id, expected);
        self.snapshots.counts.installs += 1;
        Ok(effects)
    }
}
