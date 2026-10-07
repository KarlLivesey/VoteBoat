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
mod support;
use support::*;
use voteboat::{application::*, contracts::*, identity::*, log::*, raft::Raft, snapshot::*};

fn snapshot_identity() -> SnapshotIdentity {
    SnapshotIdentity {
        store: identity(1),
        group: group(1),
    }
}
fn metadata(index: u64) -> SnapshotMetadata {
    SnapshotMetadata {
        bootstrap: bootstrap(1, 1),
        index,
        term: 1,
        application_schema: 1,
    }
}
fn command(index: u64, operation: u128, delta: i64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        },
    }
}
fn committed_core() -> Raft {
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let state = store.state(group(1)).unwrap();
    let mut mutation = update(
        &state,
        1,
        4,
        Some(Suffix {
            from: 1,
            entries: vec![
                command(1, 1, 7),
                command(2, 2, 3),
                command(3, 1, 7),
                command(4, 3, -2),
            ],
        }),
    );
    if let LogMutation::Update(u) = &mut mutation {
        u.hard_state.voted_for = Some(node(1));
    }
    append(&mut store, vec![mutation]);
    Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap()
}

/// Downstream logical provider; no native framing or files are required.
type HostStage = (
    SnapshotTicket,
    SnapshotMetadata,
    usize,
    Vec<u8>,
    Option<SealedSnapshot>,
);
#[derive(Clone)]
struct HostSnapshots {
    identity: SnapshotIdentity,
    binding: StoreBinding,
    limits: SnapshotLimits,
    generation: u64,
    current: Option<Snapshot>,
    pending: Option<HostStage>,
}
impl HostSnapshots {
    fn new() -> Self {
        Self {
            identity: snapshot_identity(),
            binding: StoreBinding {
                identity: identity(1),
                session: StoreSession::new(1).unwrap(),
            },
            limits: SnapshotLimits {
                max_chunk_bytes: 4,
                ..SnapshotLimits::default()
            },
            generation: 0,
            current: None,
            pending: None,
        }
    }
}
impl SnapshotStore for HostSnapshots {
    fn identity(&self) -> SnapshotIdentity {
        self.identity
    }
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotLimits {
        self.limits
    }
    fn begin(&mut self, m: SnapshotMetadata, n: usize) -> Result<SnapshotTicket, StorageError> {
        m.validate()?;
        if m.bootstrap.group != self.identity.group {
            return Err(StorageError::WrongIdentity);
        }
        if self.pending.is_some()
            || n == 0
            || n > self.limits.max_application_bytes
            || self.current.as_ref().is_some_and(|s| {
                m.index <= s.metadata.index
                    || m.term < s.metadata.term
                    || m.bootstrap != s.metadata.bootstrap
                    || m.application_schema != s.metadata.application_schema
            })
        {
            return Err(StorageError::Rejected("stage limits or regression"));
        }
        self.generation += 1;
        let ticket = SnapshotTicket {
            binding: self.binding,
            group: self.identity.group,
            generation: SnapshotGeneration::new(self.generation).unwrap(),
        };
        self.pending = Some((ticket, m, n, Vec::new(), None));
        Ok(ticket)
    }
    fn write_chunk(
        &mut self,
        ticket: SnapshotTicket,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let p = self
            .pending
            .as_mut()
            .filter(|p| p.0 == ticket)
            .ok_or(StorageError::StaleTicket)?;
        if p.4.is_some()
            || offset != p.3.len()
            || bytes.is_empty()
            || bytes.len() > self.limits.max_chunk_bytes
            || bytes.len() > p.2.saturating_sub(p.3.len())
        {
            return Err(StorageError::Rejected("chunk limits"));
        }
        p.3.extend(bytes);
        Ok(())
    }
    fn seal(&mut self, ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        let p = self
            .pending
            .as_mut()
            .filter(|p| p.0 == ticket)
            .ok_or(StorageError::StaleTicket)?;
        if p.4.is_some() || p.3.len() != p.2 {
            return Err(StorageError::Rejected("incomplete stage"));
        }
        let sealed = SealedSnapshot {
            ticket,
            file_bytes: p.2 as u64,
            checksum: ticket.generation.get() as u32,
        };
        p.4 = Some(sealed);
        Ok(sealed)
    }
    fn publish(&mut self, sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        if self.pending.as_ref().is_none_or(|p| p.4 != Some(sealed)) {
            return Err(StorageError::StaleTicket);
        }
        let (_, metadata, _, application, _) = self.pending.take().unwrap();
        self.current = Some(Snapshot {
            metadata: metadata.clone(),
            application,
        });
        Ok(SnapshotReceipt { sealed, metadata })
    }
    fn abort(&mut self, ticket: SnapshotTicket) -> Result<(), StorageError> {
        if self.pending.as_ref().is_none_or(|p| p.0 != ticket) {
            return Err(StorageError::StaleTicket);
        }
        self.pending = None;
        Ok(())
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        Ok(self.current.clone())
    }
}
fn publish<S: SnapshotStore>(store: &mut S, m: SnapshotMetadata, bytes: &[u8]) -> SnapshotReceipt {
    let ticket = store.begin(m, bytes.len()).unwrap();
    let size = store.limits().max_chunk_bytes;
    for (i, chunk) in bytes.chunks(size).enumerate() {
        store.write_chunk(ticket, i * size, chunk).unwrap();
    }
    let sealed = store.seal(ticket).unwrap();
    store.publish(sealed).unwrap()
}
fn conformance<S: SnapshotStore>(store: &mut S) {
    assert!(store.load().unwrap().is_none());
    let ticket = store.begin(metadata(1), 7).unwrap();
    assert!(store.begin(metadata(2), 7).is_err());
    assert!(store.seal(ticket).is_err());
    assert!(store.write_chunk(ticket, 1, &[1]).is_err());
    assert!(store.write_chunk(ticket, 0, &[]).is_err());
    let mut foreign = ticket;
    foreign.binding.session = StoreSession::new(99).unwrap();
    assert!(store.write_chunk(foreign, 0, &[1]).is_err());
    store.abort(ticket).unwrap();
    assert!(store.seal(ticket).is_err());
    let bytes = [7; 7];
    let receipt = publish(store, metadata(1), &bytes);
    assert_ne!(ticket, receipt.sealed.ticket);
    assert!(store.publish(receipt.sealed).is_err());
    assert!(store.begin(metadata(1), 7).is_err());
    let staged = store.begin(metadata(2), 4).unwrap();
    assert_eq!(store.load().unwrap().unwrap().application, bytes);
    store.write_chunk(staged, 0, &[3; 4]).unwrap();
    assert!(store.write_chunk(staged, 4, &[3]).is_err());
    let sealed = store.seal(staged).unwrap();
    assert_eq!(store.load().unwrap().unwrap().metadata.index, 1);
    let mut wrong = sealed;
    wrong.checksum ^= 1;
    assert!(store.publish(wrong).is_err());
    assert!(store.write_chunk(staged, 0, &[3]).is_err());
    store.publish(sealed).unwrap();
    assert_eq!(store.load().unwrap().unwrap().metadata.index, 2);
    assert_eq!(store.load().unwrap().unwrap().application, [3; 4]);
    let mut wrong = metadata(3);
    wrong.bootstrap.configuration = ConfigurationId::new(2).unwrap();
    assert!(store.begin(wrong, 1).is_err());
}
#[test]
fn public_snapshot_conformance_with_host_provider() {
    conformance(&mut HostSnapshots::new());
}
#[test]
fn checkpoint_plus_tail_preserves_dedup_and_host_application_schema() {
    #[derive(Clone)]
    struct HostApplication(Counter);
    impl StateMachine for HostApplication {
        type Receipt = CounterReceipt;
        fn applied_index(&self) -> u64 {
            self.0.applied_index()
        }
        fn apply_batch(
            &mut self,
            entries: &[LogEntry],
        ) -> Result<Vec<Self::Receipt>, ApplicationError> {
            self.0.apply_batch(entries)
        }
    }
    impl CheckpointStateMachine for HostApplication {
        fn schema_version(&self) -> u64 {
            22
        }
        fn checkpoint(&self, budget: usize) -> Result<Vec<u8>, ApplicationError> {
            self.0.checkpoint(budget)
        }
        fn restore_checkpoint(
            &mut self,
            schema: u64,
            index: u64,
            bytes: &[u8],
        ) -> Result<(), ApplicationError> {
            if schema != 22 {
                return Err(ApplicationError::UnsupportedSchema);
            }
            self.0.restore_checkpoint(1, index, bytes)
        }
    }
    let core = committed_core();
    let mut application = HostApplication(Counter::new(20).unwrap());
    application
        .apply_batch(&core.replay_committed()[..2])
        .unwrap();
    let mut snapshots = HostSnapshots::new();
    let receipt = checkpoint_application(&core, &application, &mut snapshots).unwrap();
    assert_eq!(receipt.metadata.index, 2);
    let mut restored = HostApplication(Counter::new(20).unwrap());
    let result = restore_application(&core, &mut restored, &mut snapshots).unwrap();
    assert_eq!(result.checkpoint_index, 2);
    assert_eq!(result.replay_receipts.len(), 2);
    assert_eq!(result.replay_receipts[0].outcome, CounterOutcome::Value(7));
    assert!(result.replay_receipts[0].duplicate);
    assert_eq!(restored.0.read_applied(4), Ok(8));
    assert_eq!(restored.0.remaining_operations(), 17);
    assert_eq!(
        restore_application(&core, &mut restored, &mut snapshots).err(),
        Some(CheckpointError::InvalidBoundary)
    );
}
#[test]
fn invalid_snapshot_binding_boundary_schema_or_tail_never_partly_restores() {
    let core = committed_core();
    let mut app = Counter::new(20).unwrap();
    app.apply_batch(&core.replay_committed()[..2]).unwrap();
    let mut snapshots = HostSnapshots::new();
    checkpoint_application(&core, &app, &mut snapshots).unwrap();
    for mutation in 0..6 {
        let mut bad = snapshots.clone();
        let s = bad.current.as_mut().unwrap();
        match mutation {
            0 => s.metadata.index = 5,
            1 => s.metadata.term = 2,
            2 => s.metadata.bootstrap.configuration = ConfigurationId::new(2).unwrap(),
            3 => s.metadata.application_schema = 2,
            4 => {
                s.application[8..16].copy_from_slice(&1u64.to_le_bytes());
            }
            5 => bad.identity.store = identity(99),
            _ => unreachable!(),
        }
        let mut fresh = Counter::new(20).unwrap();
        assert!(restore_application(&core, &mut fresh, &mut bad).is_err());
        assert_eq!(fresh.applied_index(), 0);
        assert_eq!(fresh.read_applied(0), Ok(0));
    }
    let mut capacity = Counter::new(1).unwrap();
    assert!(restore_application(&core, &mut capacity, &mut snapshots).is_err());
    assert_eq!(capacity.applied_index(), 0);
    let mut invalid_state = core.state().clone();
    invalid_state.entries[3].payload = EntryPayload::Command {
        operation: OperationId::new(3).unwrap(),
        bytes: vec![0],
    };
    let bad_tail = Raft::recover(
        node(1),
        core.storage_binding(),
        invalid_state,
        LogLimits::default(),
    )
    .unwrap();
    let mut fresh = Counter::new(20).unwrap();
    assert_eq!(
        restore_application(&bad_tail, &mut fresh, &mut snapshots).err(),
        Some(CheckpointError::Application(
            ApplicationError::InvalidCommand
        ))
    );
    assert_eq!(fresh.applied_index(), 0);
    let mut too_far = Counter::new(20).unwrap();
    too_far
        .apply_batch(&[
            command(1, 1, 1),
            command(2, 2, 1),
            command(3, 3, 1),
            command(4, 4, 1),
            command(5, 5, 1),
        ])
        .unwrap();
    assert_eq!(
        checkpoint_application(&core, &too_far, &mut HostSnapshots::new()).err(),
        Some(CheckpointError::InvalidBoundary)
    );
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{cell::RefCell, io, rc::Rc};
    use voteboat::native::snapshot_store::*;
    #[derive(Clone, Copy, Debug, Default)]
    enum Failure {
        #[default]
        None,
        Begin(usize),
        Append(usize),
        SyncBefore,
        SyncAfter,
        PublishBefore,
        PublishAfter,
    }
    #[derive(Clone, Default)]
    struct Device {
        slots: [Option<Vec<u8>>; 2],
        synced: [Option<Vec<u8>>; 2],
        manifest: Option<Vec<u8>>,
        failure: Failure,
        trace: Vec<&'static str>,
    }
    impl Device {
        fn power_loss(&mut self) {
            self.slots = self.synced.clone();
            self.failure = Failure::None;
        }
    }
    #[derive(Clone, Default)]
    struct MemoryIo(Rc<RefCell<Device>>);
    impl SnapshotIo for MemoryIo {
        fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
            self.0
                .borrow()
                .manifest
                .clone()
                .ok_or(io::ErrorKind::NotFound.into())
        }
        fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
            let d = self.0.borrow();
            let bytes = d.slots[slot as usize]
                .as_ref()
                .ok_or(io::ErrorKind::NotFound)?;
            if bytes.len() > limit {
                return Err(io::Error::other("oversized slot"));
            }
            Ok(bytes.clone())
        }
        fn begin_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.borrow_mut();
            d.trace.push("begin");
            if let Failure::Begin(cut) = d.failure {
                d.slots[slot as usize] = Some(bytes[..cut.min(bytes.len())].to_vec());
                return Err(io::Error::other("interrupted begin"));
            }
            d.slots[slot as usize] = Some(bytes.to_vec());
            Ok(())
        }
        fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.borrow_mut();
            d.trace.push("append");
            if let Failure::Append(cut) = d.failure {
                d.slots[slot as usize]
                    .as_mut()
                    .unwrap()
                    .extend(&bytes[..cut.min(bytes.len())]);
                return Err(io::Error::other("interrupted append"));
            }
            d.slots[slot as usize].as_mut().unwrap().extend(bytes);
            Ok(())
        }
        fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
            let mut d = self.0.borrow_mut();
            d.trace.push("sync-data-and-name");
            if matches!(d.failure, Failure::SyncBefore) {
                return Err(io::Error::other("sync failed"));
            }
            d.synced[slot as usize] = d.slots[slot as usize].clone();
            if matches!(d.failure, Failure::SyncAfter) {
                return Err(io::Error::other("sync completion lost"));
            }
            Ok(())
        }
        fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.borrow_mut();
            d.trace.push("publish-root");
            if matches!(d.failure, Failure::PublishBefore) {
                return Err(io::Error::other("before root rename"));
            }
            d.manifest = Some(bytes.to_vec());
            if matches!(d.failure, Failure::PublishAfter) {
                return Err(io::Error::other("after root rename"));
            }
            Ok(())
        }
    }
    fn create(io: MemoryIo) -> NativeSnapshotStore<MemoryIo> {
        NativeSnapshotStore::create(
            io,
            snapshot_identity(),
            SnapshotLimits {
                max_chunk_bytes: 4,
                ..SnapshotLimits::default()
            },
        )
        .unwrap()
    }
    fn recover(io: MemoryIo) -> NativeSnapshotStore<MemoryIo> {
        NativeSnapshotStore::recover(
            io,
            snapshot_identity(),
            SnapshotLimits {
                max_chunk_bytes: 4,
                ..SnapshotLimits::default()
            },
        )
        .unwrap()
    }
    #[test]
    fn native_snapshot_conformance_and_restart_session_fencing() {
        let io = MemoryIo::default();
        let mut store = create(io.clone());
        conformance(&mut store);
        let ticket = store.begin(metadata(3), 4).unwrap();
        store.write_chunk(ticket, 0, &[9; 4]).unwrap();
        let sealed = store.seal(ticket).unwrap();
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut store = recover(io.clone());
        assert_eq!(store.load().unwrap().unwrap().application, [3; 4]);
        assert_eq!(store.publish(sealed), Err(StorageError::StaleTicket));
        assert_eq!(
            store.write_chunk(ticket, 0, &[7]),
            Err(StorageError::StaleTicket)
        );
        assert!(store.binding().session > ticket.binding.session);
        let receipt = publish(&mut store, metadata(3), &[9; 4]);
        assert!(
            receipt.sealed.ticket.generation > ticket.generation
                || receipt.sealed.ticket.binding != ticket.binding
        );
        let trace = &io.0.borrow().trace;
        assert_eq!(
            &trace[trace.len() - 2..],
            &["sync-data-and-name", "publish-root"]
        );
    }
    #[test]
    fn interrupted_chunks_seals_and_publication_recover_old_or_complete_new() {
        let original = MemoryIo::default();
        let mut store = create(original.clone());
        publish(&mut store, metadata(1), &[7; 4]);
        drop(store);
        let baseline = original.0.borrow().clone();
        let prefix_bytes = NativeSnapshotCodec
            .prefix(&metadata(2), 4, SnapshotLimits::default())
            .unwrap()
            .len();
        let failures: Vec<_> = (0..=prefix_bytes)
            .map(Failure::Begin)
            .chain((0..=4).map(Failure::Append))
            .chain([
                Failure::SyncBefore,
                Failure::SyncAfter,
                Failure::PublishBefore,
                Failure::PublishAfter,
            ])
            .collect();
        for failure in failures {
            let io = MemoryIo(Rc::new(RefCell::new(baseline.clone())));
            let mut store = recover(io.clone());
            io.0.borrow_mut().failure = failure;
            let result = (|| {
                let t = store.begin(metadata(2), 4)?;
                store.write_chunk(t, 0, &[9; 4])?;
                let s = store.seal(t)?;
                store.publish(s)
            })();
            assert!(result.is_err(), "fault={failure:?}");
            assert_eq!(store.load(), Err(StorageError::Fenced));
            drop(store);
            io.0.borrow_mut().power_loss();
            let recovered = recover(io).load().unwrap().unwrap();
            if matches!(failure, Failure::PublishAfter) {
                assert_eq!(recovered.application, [9; 4]);
                assert_eq!(recovered.metadata.index, 2);
            } else {
                assert_eq!(recovered.application, [7; 4]);
                assert_eq!(recovered.metadata.index, 1);
            }
        }
        // Interrupt the CRC footer itself after the complete image was written.
        for cut in 0..=4 {
            let io = MemoryIo(Rc::new(RefCell::new(baseline.clone())));
            let mut store = recover(io.clone());
            let t = store.begin(metadata(2), 4).unwrap();
            store.write_chunk(t, 0, &[9; 4]).unwrap();
            io.0.borrow_mut().failure = Failure::Append(cut);
            assert!(store.seal(t).is_err());
            drop(store);
            io.0.borrow_mut().power_loss();
            assert_eq!(recover(io).load().unwrap().unwrap().application, [7; 4]);
        }
    }
    #[test]
    fn corruption_truncation_missing_data_and_identity_mismatch_fail_closed() {
        let io = MemoryIo::default();
        let mut store = create(io.clone());
        publish(&mut store, metadata(1), &[7; 4]);
        drop(store);
        let baseline = io.0.borrow().clone();
        let length = baseline.synced[0].as_ref().unwrap().len();
        for bit in 0..length * 8 {
            let mut d = baseline.clone();
            let b = d.synced[0].as_mut().unwrap();
            b[bit / 8] ^= 1 << (bit % 8);
            d.power_loss();
            assert!(NativeSnapshotStore::recover(
                MemoryIo(Rc::new(RefCell::new(d))),
                snapshot_identity(),
                SnapshotLimits::default()
            )
            .is_err());
        }
        for cut in 0..length {
            let mut d = baseline.clone();
            d.synced[0].as_mut().unwrap().truncate(cut);
            d.power_loss();
            assert!(NativeSnapshotStore::recover(
                MemoryIo(Rc::new(RefCell::new(d))),
                snapshot_identity(),
                SnapshotLimits::default()
            )
            .is_err());
        }
        for bit in 0..baseline.manifest.as_ref().unwrap().len() * 8 {
            let mut d = baseline.clone();
            d.manifest.as_mut().unwrap()[bit / 8] ^= 1 << (bit % 8);
            assert!(NativeSnapshotStore::recover(
                MemoryIo(Rc::new(RefCell::new(d))),
                snapshot_identity(),
                SnapshotLimits::default()
            )
            .is_err());
        }
        let mut wrong = snapshot_identity();
        wrong.store = identity(99);
        assert!(
            NativeSnapshotStore::recover(io.clone(), wrong, SnapshotLimits::default()).is_err()
        );
        let mut wrong = snapshot_identity();
        wrong.group.incarnation = GroupIncarnation::new(2).unwrap();
        assert!(
            NativeSnapshotStore::recover(io.clone(), wrong, SnapshotLimits::default()).is_err()
        );
        io.0.borrow_mut().manifest = None;
        assert!(NativeSnapshotStore::create(
            io.clone(),
            snapshot_identity(),
            SnapshotLimits::default()
        )
        .is_err());
        assert!(
            NativeSnapshotStore::recover(io, snapshot_identity(), SnapshotLimits::default())
                .is_err()
        );
    }
    #[test]
    fn native_files_restore_checkpoint_plus_tail_and_exclude_concurrent_writers() {
        let directory =
            std::env::temp_dir().join(format!("voteboat-snapshot-{}", std::process::id()));
        let core = committed_core();
        let mut app = Counter::new(20).unwrap();
        app.apply_batch(&core.replay_committed()[..2]).unwrap();
        let mut store = NativeSnapshotStore::create(
            FileSnapshotIo::create(&directory).unwrap(),
            snapshot_identity(),
            SnapshotLimits::default(),
        )
        .unwrap();
        checkpoint_application(&core, &app, &mut store).unwrap();
        assert!(FileSnapshotIo::open(&directory).is_err());
        drop(store);
        assert!(FileSnapshotIo::create(&directory).is_err());
        let mut store = NativeSnapshotStore::recover(
            FileSnapshotIo::open(&directory).unwrap(),
            snapshot_identity(),
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut restored = Counter::new(20).unwrap();
        let result = restore_application(&core, &mut restored, &mut store).unwrap();
        assert_eq!(result.checkpoint_index, 2);
        assert_eq!(restored.read_applied(4), Ok(8));
        assert!(result.replay_receipts[0].duplicate);
        checkpoint_application(&core, &restored, &mut store).unwrap();
        drop(store);
        let mut store = NativeSnapshotStore::recover(
            FileSnapshotIo::open(&directory).unwrap(),
            snapshot_identity(),
            SnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(store.load().unwrap().unwrap().metadata.index, 4);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn staged_corruption_is_detected_against_submitted_bytes_before_sealing() {
        let io = MemoryIo::default();
        let mut store = create(io.clone());
        publish(&mut store, metadata(1), &[7; 4]);
        let ticket = store.begin(metadata(2), 4).unwrap();
        store.write_chunk(ticket, 0, &[9; 4]).unwrap();
        let end = io.0.borrow().slots[1].as_ref().unwrap().len() - 1;
        io.0.borrow_mut().slots[1].as_mut().unwrap()[end] ^= 1;
        assert!(matches!(store.seal(ticket), Err(StorageError::Corrupt(_))));
        assert_eq!(store.load(), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        assert_eq!(recover(io).load().unwrap().unwrap().application, [7; 4]);
    }

    fn crc(bytes: &[u8]) -> u32 {
        let mut value = !0u32;
        for byte in bytes {
            value ^= *byte as u32;
            for _ in 0..8 {
                value = (value >> 1) ^ if value & 1 == 1 { 0x82f63b78 } else { 0 };
            }
        }
        !value
    }
    #[test]
    fn bounded_codec_rejects_oversize_lengths_even_with_valid_checksums() {
        let limits = SnapshotLimits::default();
        let mut bytes = NativeSnapshotCodec.prefix(&metadata(1), 4, limits).unwrap();
        bytes.extend([7; 4]);
        bytes.extend(NativeSnapshotCodec.finish(&bytes, limits).unwrap());
        for mutate in 0..3 {
            let mut bad = bytes.clone();
            match mutate {
                0 => bad[32..36].copy_from_slice(&u32::MAX.to_le_bytes()),
                1 => bad[36..44].copy_from_slice(&u64::MAX.to_le_bytes()),
                2 => bad[7] = b'2',
                _ => unreachable!(),
            }
            let end = bad.len() - 4;
            let checksum = crc(&bad[..end]);
            bad[end..].copy_from_slice(&checksum.to_le_bytes());
            assert!(NativeSnapshotCodec.decode(&bad, limits).is_err());
        }
        let invalid = SnapshotLimits {
            max_application_bytes: usize::MAX,
            max_metadata_bytes: usize::MAX,
            max_chunk_bytes: usize::MAX,
        };
        assert!(NativeSnapshotCodec.decode(&bytes, invalid).is_err());
        assert!(NativeSnapshotCodec
            .prefix(&metadata(1), usize::MAX, limits)
            .is_err());
        let mut store = create(MemoryIo::default());
        assert!(store
            .begin(metadata(1), limits.max_application_bytes + 1)
            .is_err());
        use voteboat::quorum::{Limits, Policy, Tree};
        let mut recursive = metadata(1);
        recursive.bootstrap = bootstrap(1, 9);
        recursive.bootstrap.policy = Policy::new(
            Tree::Majority(
                (0..3)
                    .map(|site| {
                        Tree::Majority((1..=3).map(|n| Tree::Voter(node(site * 3 + n))).collect())
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap();
        let mut bytes = NativeSnapshotCodec.prefix(&recursive, 4, limits).unwrap();
        bytes.extend([7; 4]);
        bytes.extend(NativeSnapshotCodec.finish(&bytes, limits).unwrap());
        let decoded = NativeSnapshotCodec.decode(&bytes, limits).unwrap();
        assert_eq!(decoded.metadata, recursive);
        assert!(decoded
            .metadata
            .bootstrap
            .policy
            .is_satisfied(&[1, 2, 4, 5].into_iter().map(node).collect()));
        assert!(!decoded
            .metadata
            .bootstrap
            .policy
            .is_satisfied(&[1, 2, 3, 4, 7].into_iter().map(node).collect()));
    }

    #[test]
    fn downstream_snapshot_codec_is_used_and_incompatible_version_is_rejected() {
        struct HostCodec {
            calls: Rc<RefCell<usize>>,
            version: u32,
        }
        impl SnapshotCodec for HostCodec {
            fn format_version(&self) -> u32 {
                self.version
            }
            fn prefix(
                &self,
                m: &SnapshotMetadata,
                n: usize,
                l: SnapshotLimits,
            ) -> Result<Vec<u8>, StorageError> {
                *self.calls.borrow_mut() += 1;
                NativeSnapshotCodec.prefix(m, n, l)
            }
            fn finish(&self, b: &[u8], l: SnapshotLimits) -> Result<[u8; 4], StorageError> {
                *self.calls.borrow_mut() += 1;
                NativeSnapshotCodec.finish(b, l)
            }
            fn decode(&self, b: &[u8], l: SnapshotLimits) -> Result<Snapshot, StorageError> {
                *self.calls.borrow_mut() += 1;
                NativeSnapshotCodec.decode(b, l)
            }
        }
        let calls = Rc::new(RefCell::new(0));
        let io = MemoryIo::default();
        let mut store = NativeSnapshotStore::create_with_codec(
            io.clone(),
            snapshot_identity(),
            SnapshotLimits::default(),
            HostCodec {
                calls: calls.clone(),
                version: 1,
            },
        )
        .unwrap();
        publish(&mut store, metadata(1), &[7; 4]);
        store.load().unwrap();
        drop(store);
        assert_eq!(*calls.borrow(), 4);
        let mut store = NativeSnapshotStore::recover_with_codec(
            io.clone(),
            snapshot_identity(),
            SnapshotLimits::default(),
            HostCodec {
                calls: calls.clone(),
                version: 1,
            },
        )
        .unwrap();
        assert_eq!(store.load().unwrap().unwrap().application, [7; 4]);
        drop(store);
        let before = io.0.borrow().manifest.clone();
        assert!(NativeSnapshotStore::recover_with_codec(
            io.clone(),
            snapshot_identity(),
            SnapshotLimits::default(),
            HostCodec { calls, version: 2 }
        )
        .is_err());
        assert_eq!(io.0.borrow().manifest, before);
    }

    #[test]
    fn native_wal_and_checkpoint_crashes_recover_acknowledged_state_and_retries() {
        use voteboat::native::log_store::NativeLogStore;
        for failure in [
            Failure::SyncBefore,
            Failure::SyncAfter,
            Failure::PublishBefore,
            Failure::PublishAfter,
        ] {
            let log_io = ModelIo::default();
            let mut log =
                NativeLogStore::create(log_io.clone(), identity(1), LogLimits::default()).unwrap();
            append(&mut log, vec![LogMutation::Create(bootstrap(1, 1))]);
            let state = log.state(group(1)).unwrap();
            append(
                &mut log,
                vec![update(
                    &state,
                    1,
                    2,
                    Some(Suffix {
                        from: 1,
                        entries: vec![command(1, 1, 7), command(2, 2, 3)],
                    }),
                )],
            );
            let core = Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut app = Counter::new(20).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            let io = MemoryIo::default();
            let mut snapshots = create(io.clone());
            checkpoint_application(&core, &app, &mut snapshots).unwrap();
            let state = log.state(group(1)).unwrap();
            append(
                &mut log,
                vec![update(
                    &state,
                    1,
                    4,
                    Some(Suffix {
                        from: 3,
                        entries: vec![command(3, 1, 7), command(4, 3, -2)],
                    }),
                )],
            );
            let core = Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let receipts = app.apply_batch(&core.replay_committed()[2..]).unwrap();
            assert!(receipts[0].duplicate);
            assert_eq!(app.read_applied(4), Ok(8));
            io.0.borrow_mut().failure = failure;
            assert!(checkpoint_application(&core, &app, &mut snapshots).is_err());
            drop(snapshots);
            drop(log);
            io.0.borrow_mut().power_loss();
            log_io.0.borrow_mut().power_loss();
            let mut log =
                NativeLogStore::recover(log_io, identity(1), LogLimits::default()).unwrap();
            let core = Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut snapshots = recover(io);
            let mut app = Counter::new(20).unwrap();
            let restored = restore_application(&core, &mut app, &mut snapshots).unwrap();
            assert_eq!(
                restored.checkpoint_index,
                if matches!(failure, Failure::PublishAfter) {
                    4
                } else {
                    2
                }
            );
            assert_eq!(app.read_applied(4), Ok(8));
            assert_eq!(core.state().commit_index, 4);
            let state = log.state(group(1)).unwrap();
            append(
                &mut log,
                vec![update(
                    &state,
                    1,
                    5,
                    Some(Suffix {
                        from: 5,
                        entries: vec![command(5, 1, 7)],
                    }),
                )],
            );
            let core = Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let retry = app.apply_batch(&core.replay_committed()[4..]).unwrap();
            assert!(retry[0].duplicate);
            assert_eq!(retry[0].outcome, CounterOutcome::Value(7));
            assert_eq!(app.read_applied(5), Ok(8));
            assert_eq!(core.state().commit_index, 5);
            assert_eq!(core.state().hard_state.term, 1);
        }
    }
}
