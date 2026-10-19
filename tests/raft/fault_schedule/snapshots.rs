// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    application::{CheckpointStateMachine, Counter, StateMachine},
    contracts::StorageError,
    native::snapshot_store::{NativeSnapshotStore, SnapshotIo},
    snapshot::*,
};
#[path = "snapshots/device.rs"]
mod device;
#[path = "snapshots/effects.rs"]
mod effects;
#[path = "snapshots/histories.rs"]
mod histories;
#[path = "snapshots/recovery.rs"]
mod recovery;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Begin,
    Write,
    Seal,
    Publish,
    Compact,
    Abort,
    Stale,
    FailSync,
    BadSync,
    PublishBefore,
    PublishAfter,
}
impl Operation {
    pub(super) const ALL: [Self; 11] = [
        Self::Begin,
        Self::Write,
        Self::Seal,
        Self::Publish,
        Self::Compact,
        Self::Abort,
        Self::Stale,
        Self::FailSync,
        Self::BadSync,
        Self::PublishBefore,
        Self::PublishAfter,
    ];
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Begin => "snapshot_begin",
            Self::Write => "snapshot_write",
            Self::Seal => "snapshot_seal",
            Self::Publish => "snapshot_publish",
            Self::Compact => "snapshot_compact",
            Self::Abort => "snapshot_abort",
            Self::Stale => "snapshot_stale",
            Self::FailSync => "snapshot_fail_sync",
            Self::BadSync => "snapshot_bad_sync",
            Self::PublishBefore => "snapshot_publish_before",
            Self::PublishAfter => "snapshot_publish_after",
        }
    }
    pub(super) fn parse(name: &str) -> Result<Self, ()> {
        Self::ALL.into_iter().find(|op| op.name() == name).ok_or(())
    }
}
fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_application_bytes: MAX_FAULT_BYTES,
        max_metadata_bytes: 4096,
        // One chunk for a one-receipt Counter image (105 bytes), two for the
        // two-receipt replacement (130 bytes), as the histories expect.
        max_chunk_bytes: 128,
    }
}
fn fresh() -> Counter {
    Counter::new(MAX_ACTIONS).unwrap()
}
fn snapshot_identity(id: u64) -> SnapshotIdentity {
    SnapshotIdentity {
        store: identity(u128::from(id)),
        group: group(1),
    }
}
struct Stage {
    ticket: SnapshotTicket,
    bytes: Vec<u8>,
    offset: usize,
    sealed: Option<SealedSnapshot>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Phase {
    ticket: SnapshotTicket,
    offset: usize,
    bytes: usize,
    sealed: Option<SealedSnapshot>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub(super) begins: usize,
    pub(super) writes: usize,
    pub(super) seals: usize,
    pub(super) publishes: usize,
    pub(super) compactions: usize,
    pub(super) recoveries: usize,
    pub(super) installs: usize,
    pub(super) stale: usize,
    pub(super) failures: usize,
    pub(super) refused: usize,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Summary {
    phases: BTreeMap<u64, Phase>,
    bindings: BTreeMap<u64, StoreBinding>,
    roots: BTreeMap<u64, Result<Option<SnapshotRef>, StorageError>>,
    recovered_roots: BTreeMap<u64, Option<SnapshotRef>>,
    pub(super) counts: Counts,
}
#[derive(Default)]
pub(super) struct Dependencies {
    images: BTreeMap<u64, device::Image>,
    providers: BTreeMap<u64, NativeSnapshotStore<device::Image>>,
    stages: BTreeMap<u64, Stage>,
    previous: BTreeMap<u64, SnapshotTicket>,
    incoming: BTreeMap<u64, Vec<LogEntry>>,
    recovered_roots: BTreeMap<u64, Option<SnapshotRef>>,
    counts: Counts,
}
impl Dependencies {
    pub(super) fn has_image(&self, id: u64) -> bool {
        self.images.contains_key(&id)
    }
    pub(super) fn summary(&self) -> Summary {
        Summary {
            phases: self
                .stages
                .iter()
                .map(|(id, s)| {
                    (
                        *id,
                        Phase {
                            ticket: s.ticket,
                            offset: s.offset,
                            bytes: s.bytes.len(),
                            sealed: s.sealed,
                        },
                    )
                })
                .collect(),
            bindings: self
                .providers
                .iter()
                .map(|(id, p)| (*id, p.binding()))
                .collect(),
            roots: self
                .providers
                .iter()
                .map(|(id, p)| (*id, p.latest_reference()))
                .collect(),
            counts: self.counts.clone(),
            recovered_roots: self.recovered_roots.clone(),
        }
    }
    fn ensure(&mut self, id: u64) {
        if let std::collections::btree_map::Entry::Vacant(entry) = self.images.entry(id) {
            let image = device::Image::default();
            let provider =
                NativeSnapshotStore::create(image.clone(), snapshot_identity(id), limits())
                    .unwrap();
            entry.insert(image);
            self.providers.insert(id, provider);
        }
    }
}
impl Replay {
    pub(super) fn snapshot_counter(&mut self, id: u64) -> Result<Counter, CheckpointError> {
        let mut counter = fresh();
        let core = &self.cluster.replicas[&id].core;
        if let Some(provider) = self.snapshots.providers.get_mut(&id) {
            restore_application(core, &mut counter, provider)?;
        } else {
            counter.apply_batch(core.replay_committed())?;
        }
        Ok(counter)
    }
    pub(super) fn snapshot_action(&mut self, id: u64, operation: Operation) {
        match operation {
            Operation::Begin => self.snapshot_begin(id),
            Operation::Write => self.snapshot_write(id),
            Operation::Seal => self.snapshot_seal(id, device::Fault::None),
            Operation::BadSync => self.snapshot_seal(id, device::Fault::FalseSync),
            Operation::FailSync => self.snapshot_seal(id, device::Fault::Sync),
            Operation::Publish => self.snapshot_publish(id, device::Fault::None),
            Operation::PublishBefore => self.snapshot_publish(id, device::Fault::PublishBefore),
            Operation::PublishAfter => self.snapshot_publish(id, device::Fault::PublishAfter),
            Operation::Compact => self.snapshot_compact(id),
            Operation::Abort => self.snapshot_abort(id),
            Operation::Stale => self.snapshot_stale(id),
        }
    }
    fn snapshot_begin(&mut self, id: u64) {
        let core = &self.cluster.replicas[&id].core;
        if core.has_pending_dependency() || core.is_fenced() || core.state().commit_index == 0 {
            self.snapshots.counts.refused += 1;
            return;
        }
        let Ok(application) = self.snapshot_counter(id) else {
            self.snapshots.counts.refused += 1;
            return;
        };
        self.snapshots.ensure(id);
        let core = &self.cluster.replicas[&id].core;
        let index = application.applied_index();
        let metadata = SnapshotMetadata {
            bootstrap: core.state().bootstrap.clone(),
            membership: core.state().checkpoint_membership(index).unwrap(),
            index,
            term: core.state().term_at(index).unwrap(),
            application_schema: application.schema_version(),
        };
        let bytes = application
            .checkpoint(limits().max_application_bytes)
            .unwrap();
        match self
            .snapshots
            .providers
            .get_mut(&id)
            .unwrap()
            .begin(metadata, bytes.len())
        {
            Ok(ticket) => {
                assert!(self
                    .snapshots
                    .stages
                    .insert(
                        id,
                        Stage {
                            ticket,
                            bytes,
                            offset: 0,
                            sealed: None,
                        }
                    )
                    .is_none());
                self.snapshots.previous.insert(id, ticket);
                self.snapshots.counts.begins += 1;
            }
            Err(_) => self.snapshots.counts.refused += 1,
        }
    }
    fn snapshot_write(&mut self, id: u64) {
        let Some(stage) = self.snapshots.stages.get_mut(&id) else {
            return;
        };
        if stage.sealed.is_some() || stage.offset == stage.bytes.len() {
            return;
        }
        let end = (stage.offset + limits().max_chunk_bytes).min(stage.bytes.len());
        match self.snapshots.providers.get_mut(&id).unwrap().write_chunk(
            stage.ticket,
            stage.offset,
            &stage.bytes[stage.offset..end],
        ) {
            Ok(()) => {
                stage.offset = end;
                self.snapshots.counts.writes += 1;
            }
            Err(_) => self.snapshots.counts.failures += 1,
        }
    }
    fn snapshot_seal(&mut self, id: u64, fault: device::Fault) {
        let Some(stage) = self.snapshots.stages.get_mut(&id) else {
            return;
        };
        if stage.sealed.is_some() || stage.offset != stage.bytes.len() {
            return;
        }
        self.snapshots.images[&id].fault(fault);
        match self
            .snapshots
            .providers
            .get_mut(&id)
            .unwrap()
            .seal(stage.ticket)
        {
            Ok(sealed) => {
                stage.sealed = Some(sealed);
                self.snapshots.counts.seals += 1;
            }
            Err(_) => self.snapshots.counts.failures += 1,
        }
    }
    fn snapshot_publish(&mut self, id: u64, fault: device::Fault) {
        let Some(sealed) = self.snapshots.stages.get(&id).and_then(|s| s.sealed) else {
            return;
        };
        self.snapshots.images[&id].fault(fault);
        match self
            .snapshots
            .providers
            .get_mut(&id)
            .unwrap()
            .publish(sealed)
        {
            Ok(receipt) => {
                assert_eq!(receipt.sealed, sealed);
                self.snapshots.stages.remove(&id);
                self.snapshots.counts.publishes += 1;
            }
            Err(_) => self.snapshots.counts.failures += 1,
        }
    }
    fn snapshot_compact(&mut self, id: u64) {
        let Some(reference) = self
            .snapshots
            .providers
            .get(&id)
            .and_then(|p| p.latest_reference().ok())
            .flatten()
        else {
            return;
        };
        let core = &self.cluster.replicas[&id].core;
        if core.has_pending_dependency()
            || core.is_fenced()
            || reference.index <= core.state().base_index()
        {
            return;
        }
        let Ok(application) = self.snapshot_counter(id) else {
            return;
        };
        let replica = self.cluster.replicas.get_mut(&id).unwrap();
        match compact_replica(
            &mut replica.core,
            &mut replica.store,
            self.snapshots.providers.get_mut(&id).unwrap(),
            &application,
            reference,
        ) {
            Ok(effects) => {
                self.snapshots.counts.compactions += 1;
                self.effects(id, effects);
            }
            Err(_) => self.snapshots.counts.failures += 1,
        }
    }
    fn snapshot_abort(&mut self, id: u64) {
        if let Some(stage) = self.snapshots.stages.get(&id) {
            if self
                .snapshots
                .providers
                .get_mut(&id)
                .unwrap()
                .abort(stage.ticket)
                .is_ok()
            {
                self.snapshots.stages.remove(&id);
            }
        }
    }
    fn snapshot_stale(&mut self, id: u64) {
        let Some(ticket) = self.snapshots.previous.get(&id).copied() else {
            return;
        };
        let provider = self.snapshots.providers.get_mut(&id).unwrap();
        if ticket.binding != provider.binding() {
            assert_eq!(provider.abort(ticket), Err(StorageError::StaleTicket));
            self.snapshots.counts.stale += 1;
        }
    }
}
