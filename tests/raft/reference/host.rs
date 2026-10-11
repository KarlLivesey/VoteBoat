// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use raft_reference::{
    eraftpb::{Entry, EntryType, Message as ReferenceMessage},
    storage::MemStorage,
    Config, RawNode, StateRole,
};

struct Replica {
    core: RawNode<MemStorage>,
    applied: Vec<Entry>,
    reads: BTreeMap<u64, u64>,
    corrupted_value: bool,
}
pub(super) struct Reference {
    replicas: BTreeMap<u64, Replica>,
    messages: VecDeque<ReferenceMessage>,
    blocked: BTreeSet<(u64, u64)>,
}
impl Reference {
    pub(super) fn new() -> Self {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let replicas = (1..=3)
            .map(|id| {
                let config = Config {
                    id,
                    pre_vote: false,
                    check_quorum: false,
                    max_uncommitted_size: 65536,
                    ..Config::default()
                };
                let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
                (
                    id,
                    Replica {
                        core: RawNode::new(&config, store, &logger).unwrap(),
                        applied: Vec::new(),
                        reads: BTreeMap::new(),
                        corrupted_value: false,
                    },
                )
            })
            .collect();
        Self {
            replicas,
            messages: VecDeque::new(),
            blocked: BTreeSet::new(),
        }
    }
    pub(super) fn is_leader(&self, id: u64) -> bool {
        self.replicas[&id].core.raft.state == StateRole::Leader
    }
    pub(super) fn campaign(&mut self, id: u64) {
        self.replicas.get_mut(&id).unwrap().core.campaign().unwrap();
        self.ready(id);
    }
    pub(super) fn write(&mut self, id: u64, operation: u128, delta: i64) {
        let mut bytes = operation.to_le_bytes().to_vec();
        bytes.extend(delta.to_le_bytes());
        self.replicas
            .get_mut(&id)
            .unwrap()
            .core
            .propose(vec![], bytes)
            .unwrap();
        self.ready(id);
    }
    pub(super) fn heartbeat(&mut self, id: u64) {
        self.replicas.get_mut(&id).unwrap().core.ping();
        self.ready(id);
    }
    pub(super) fn read(&mut self, id: u64, request: u64) {
        self.replicas
            .get_mut(&id)
            .unwrap()
            .core
            .read_index(request.to_le_bytes().to_vec());
        self.ready(id);
    }
    fn ready(&mut self, id: u64) {
        let replica = self.replicas.get_mut(&id).unwrap();
        let mut count = 0;
        while replica.core.has_ready() {
            count += 1;
            assert!(count < 1000, "non-quiescent reference Ready");
            let mut ready = replica.core.ready();
            assert_eq!(
                ready.snapshot().get_metadata().index,
                0,
                "snapshot is outside this trace contract"
            );
            // This host models successful atomic durable persistence per Ready.
            // Even optional commit-only hard-state writes are persisted here.
            // No persisted message or advancement escapes before these writes.
            let store = replica.core.raft.raft_log.store.clone();
            {
                let mut disk = store.wl();
                disk.append(ready.entries()).unwrap();
                if let Some(hard) = ready.hs() {
                    disk.set_hardstate(hard.clone());
                }
            }
            self.messages.extend(ready.take_messages());
            self.messages.extend(ready.take_persisted_messages());
            for read in ready.take_read_states() {
                let request = u64::from_le_bytes(read.request_ctx.as_slice().try_into().unwrap());
                assert!(replica.reads.insert(request, read.index).is_none());
            }
            apply(&mut replica.applied, ready.take_committed_entries());
            let mut light = replica.core.advance(ready);
            if let Some(commit) = light.commit_index() {
                store.wl().mut_hard_state().commit = commit;
            }
            self.messages.extend(light.take_messages());
            apply(&mut replica.applied, light.take_committed_entries());
            replica.core.advance_apply();
            assert!(self.messages.len() < 10000);
        }
    }
    pub(super) fn pump(&mut self) {
        let mut count = 0;
        while let Some(message) = self.messages.pop_front() {
            count += 1;
            assert!(count < 100000, "non-quiescent reference messages");
            if !self.blocked.contains(&(message.from, message.to)) {
                let id = message.to;
                self.replicas
                    .get_mut(&id)
                    .unwrap()
                    .core
                    .step(message)
                    .unwrap();
                self.ready(id);
            }
        }
    }
    pub(super) fn isolate(&mut self, id: u64) {
        self.blocked = (1..=3)
            .flat_map(|a| (1..=3).filter_map(move |b| ((a == id) != (b == id)).then_some((a, b))))
            .collect();
    }
    pub(super) fn heal(&mut self) {
        self.blocked.clear();
    }
    pub(super) fn commands(&self, id: u64) -> Vec<(u128, i64)> {
        self.replicas[&id]
            .applied
            .iter()
            .filter_map(command)
            .collect()
    }
    pub(super) fn value(&self, id: u64, index: u64) -> i64 {
        let mut operations = BTreeMap::new();
        let mut value = 0i64;
        for (operation, delta) in self.replicas[&id]
            .applied
            .iter()
            .take_while(|e| e.index <= index)
            .filter_map(command)
        {
            match operations.entry(operation) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(delta);
                    value = value.checked_add(delta).unwrap();
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    assert_eq!(*entry.get(), delta)
                }
            }
        }
        value + i64::from(self.replicas[&id].corrupted_value && !operations.is_empty())
    }
    pub(super) fn read_value(&self, id: u64, request: u64) -> Option<i64> {
        self.replicas[&id].reads.get(&request).map(|index| {
            assert!(*index <= self.replicas[&id].applied.last().map_or(0, |e| e.index));
            self.value(id, *index)
        })
    }
    pub(super) fn corrupt(&mut self, id: u64) {
        self.replicas.get_mut(&id).unwrap().corrupted_value = true;
    }
    pub(super) fn assert_safety(&self) {
        let mut leaders = BTreeSet::new();
        for replica in self.replicas.values() {
            if replica.core.raft.state == StateRole::Leader {
                assert!(leaders.insert(replica.core.raft.term));
            }
            for other in self.replicas.values() {
                for (left, right) in replica.applied.iter().zip(&other.applied) {
                    assert_eq!(left, right, "reference committed-prefix divergence");
                }
            }
        }
    }
}
fn apply(applied: &mut Vec<Entry>, entries: Vec<Entry>) {
    for entry in entries {
        assert_eq!(entry.index, applied.last().map_or(1, |last| last.index + 1));
        assert_eq!(entry.get_entry_type(), EntryType::EntryNormal);
        applied.push(entry);
    }
}
fn command(entry: &Entry) -> Option<(u128, i64)> {
    if entry.data.is_empty() {
        return None;
    }
    assert_eq!(entry.data.len(), 24);
    Some((
        u128::from_le_bytes(entry.data[..16].try_into().unwrap()),
        i64::from_le_bytes(entry.data[16..].try_into().unwrap()),
    ))
}
