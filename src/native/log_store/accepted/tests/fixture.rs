// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    contracts::HardState,
    quorum::{Limits, Policy, Tree},
};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Copy, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Append,
    Sync,
    Publish,
}
#[derive(Default)]
pub(super) struct Device {
    pub(super) manifest: Option<Vec<u8>>,
    pub(super) bytes: Vec<u8>,
    pub(super) fault: Fault,
}
#[derive(Clone, Default)]
pub(super) struct Memory(pub(super) Rc<RefCell<Device>>);
impl JournalIo for Memory {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .manifest
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, _: usize) -> io::Result<Vec<u8>> {
        Ok(self.0.borrow().bytes.clone())
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if matches!(self.0.borrow().fault, Fault::Append) {
            return Err(io::Error::other("append uncertainty"));
        }
        self.0.borrow_mut().bytes.extend(bytes);
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        if matches!(self.0.borrow().fault, Fault::Sync) {
            return Err(io::Error::other("sync uncertainty"));
        }
        Ok(())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.0.borrow_mut().bytes.truncate(length as usize);
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        if matches!(self.0.borrow().fault, Fault::Publish) {
            return Err(io::Error::other("publication uncertainty"));
        }
        self.0.borrow_mut().manifest = Some(bytes.to_vec());
        Ok(())
    }
}
pub(super) type Store = NativeLogStore<Memory>;
pub(super) fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub(super) fn bootstrap(id: u128) -> Bootstrap {
    let node = NodeId::new(1).unwrap();
    Bootstrap {
        group: group(id),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: BTreeMap::from([(
            node,
            StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )]),
    }
}
pub(super) fn open(groups: u128) -> Store {
    let identity = bootstrap(1).voter_stores[&NodeId::new(1).unwrap()];
    let mut store =
        NativeLogStore::create(Memory::default(), identity, LogLimits::default()).unwrap();
    commit(
        &mut store,
        (1..=groups)
            .map(|id| LogMutation::Create(bootstrap(id)))
            .collect(),
    );
    store
}
pub(super) fn commit(store: &mut Store, mutations: Vec<LogMutation>) {
    let tickets = store.append_batch(mutations).unwrap();
    store.barrier(&tickets).unwrap();
}
pub(super) fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index)).unwrap(),
            bytes: vec![term as u8; 32].into(),
        },
    }
}
pub(super) fn mutation(
    store: &Store,
    id: u128,
    commit: u64,
    suffix: Option<Suffix>,
) -> LogMutation {
    let state = store
        .accepted
        .view(&store.durable, group(id))
        .unwrap()
        .0
        .materialize();
    mutation_raw(&state, commit, suffix)
}
pub(super) fn mutation_raw(state: &GroupLog, commit: u64, suffix: Option<Suffix>) -> LogMutation {
    let term = suffix
        .as_ref()
        .and_then(|s| s.entries.last())
        .map_or(state.hard_state.term.max(1), |entry| entry.term);
    LogMutation::Update(LogUpdate {
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: commit,
        suffix,
        snapshot: None,
        snapshot_membership: None,
    })
}
