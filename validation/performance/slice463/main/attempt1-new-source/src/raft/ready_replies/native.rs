// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Native framing/recovery with the synced-image model from tests/support.
use super::*;
use crate::native::log_store::{JournalIo, NativeLogStore};
use std::{cell::RefCell, io, rc::Rc};

#[derive(Default)]
struct Device {
    log: Vec<u8>,
    synced: Vec<u8>,
    manifest: Option<Vec<u8>>,
    publication_fault: u8,
}
#[derive(Clone, Default)]
struct ModelIo(Rc<RefCell<Device>>);
impl ModelIo {
    fn power_loss(&self) {
        let mut device = self.0.borrow_mut();
        device.log = device.synced.clone();
        device.publication_fault = 0;
    }
}
impl JournalIo for ModelIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .manifest
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let device = self.0.borrow();
        if device.log.len() > limit {
            return Err(io::Error::other("oversized WAL"));
        }
        Ok(device.log.clone())
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().log.extend(bytes);
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        device.synced = device.log.clone();
        Ok(())
    }
    fn truncate_log(&mut self, len: u64) -> io::Result<()> {
        self.0.borrow_mut().log.truncate(len as usize);
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        if device.publication_fault == 1 {
            return Err(io::Error::other("before publication"));
        }
        device.manifest = Some(bytes.to_vec());
        if device.publication_fault == 2 {
            return Err(io::Error::other("after publication"));
        }
        Ok(())
    }
}
fn seed(core: &Raft) -> (ModelIo, NativeLogStore<ModelIo>) {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), core.binding.identity, core.limits).unwrap();
    let group = core.state().bootstrap.group;
    let tickets = store
        .append_batch(vec![LogMutation::Create(core.state().bootstrap.clone())])
        .unwrap();
    store.barrier(&tickets).unwrap();
    for (from, entries) in [
        (1, core.state().entries[..1].to_vec()),
        (2, core.state().entries[1..].to_vec()),
    ] {
        let current = store.state(group).unwrap();
        let update = LogUpdate {
            group,
            expected_revision: current.revision,
            hard_state: core.state().hard_state,
            commit_index: 1,
            suffix: Some(Suffix { from, entries }),
            snapshot: None,
            snapshot_membership: None,
        };
        let tickets = store
            .append_batch(vec![LogMutation::Update(update)])
            .unwrap();
        store.barrier(&tickets).unwrap();
    }
    assert_eq!(store.state(group).unwrap(), *core.state());
    (io, store)
}

#[test]
fn native_power_loss_recovers_combined_transition_atomically_and_failed_publication_fences() {
    for phase in 0..4 {
        let (mut core, replies) = ready(0);
        let (io, mut store) = seed(&core);
        let before = core.state().clone();
        let combined = update(
            core.propose_after_ready_replies(&replies, vec![proposal(12), proposal(13)])
                .unwrap(),
        );
        let tickets = store
            .append_batch(vec![LogMutation::Update(combined)])
            .unwrap();
        core.replicate_admitted(tickets[0]).unwrap();
        if phase == 1 {
            let effects = core.complete(&store.barrier(&tickets).unwrap()).unwrap();
            assert!(
                matches!(effects.first(), Some(Effect::Committed(entries)) if entries.len() == 2)
            );
        } else if phase >= 2 {
            io.0.borrow_mut().publication_fault = phase - 1;
            assert!(store.barrier(&tickets).is_err());
            assert_eq!(
                store.state(before.bootstrap.group),
                Err(StorageError::Fenced)
            );
            core.storage_failed();
            assert_eq!(core.state(), &before);
            assert_eq!(
                core.complete(&DurableLog {
                    tickets: tickets.clone()
                }),
                Err(RaftError::Fenced)
            );
        } else {
            assert_eq!(core.state(), &before);
        }
        drop(store);
        io.power_loss();
        let recovered = NativeLogStore::recover(io, core.binding.identity, core.limits).unwrap();
        let state = recovered.state(before.bootstrap.group).unwrap();
        assert_eq!(state.commit_index, if phase == 0 { 1 } else { 3 });
        assert_eq!(state.last_index(), if phase == 0 { 3 } else { 5 });
        let mut restarted =
            Raft::recover(core.node, recovered.binding(), state, core.limits).unwrap();
        assert_eq!(
            restarted.complete(&DurableLog { tickets }),
            Err(RaftError::WrongCompletion)
        );
        let mut app = Counter::new(16).unwrap();
        let receipts = app.apply_batch(restarted.replay_committed()).unwrap();
        assert_eq!(receipts.len(), if phase == 0 { 0 } else { 2 });
        assert_eq!(app.applied_index(), if phase == 0 { 1 } else { 3 });
    }
}
