// SPDX-License-Identifier: RPL-1.5
use super::*;
use crate::{contracts::HardState, quorum::*};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
pub(super) struct Layout {
    root: PathBuf,
    pub lanes: Vec<PathBuf>,
}
impl Layout {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-stripe-primitives-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let lanes = (0..3)
            .map(|lane| root.join(format!("lane{lane}")))
            .collect();
        Self { root, lanes }
    }
    pub fn seeded(&self) -> (StripedLogStore<Interrupted>, Selector) {
        let io = Interrupted::new(FileStripedIo::create(self.lanes.clone()).unwrap());
        let selector = io.selector.clone();
        let mut store =
            StripedLogStore::create_striped(io, identity(), LogLimits::default(), stripes())
                .unwrap();
        commit(&mut store, vec![LogMutation::Create(bootstrap())]);
        let mutation = command(&store.state(group()).unwrap(), 0x31);
        commit(&mut store, vec![mutation]);
        (store, selector)
    }
    pub fn recover(&self) -> StripedLogStore<FileStripedIo> {
        StripedLogStore::recover_striped(
            FileStripedIo::open(self.lanes.clone()).unwrap(),
            identity(),
            LogLimits::default(),
            stripes(),
        )
        .unwrap()
    }
    pub fn generation(&self) -> u64 {
        let mut io = FileStripedIo::open(self.lanes.clone()).unwrap();
        StripedJournal::inspect_control(&mut io, identity(), LogLimits::default(), stripes())
            .unwrap()
            .payload_generation
    }
}
impl Drop for Layout {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
pub(super) fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(319).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub(super) fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(319).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap() -> Bootstrap {
    let node = NodeId::new(1).unwrap();
    Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: [(node, identity())].into(),
    }
}
pub(super) fn stripes() -> StripedLimits {
    StripedLimits {
        chunk_bytes: 256,
        ..Default::default()
    }
}
pub(super) fn command(state: &GroupLog, value: u8) -> LogMutation {
    let index = state.last_index() + 1;
    let term = state.hard_state.term.max(1);
    LogMutation::Update(LogUpdate {
        group: group(),
        expected_revision: state.revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: index,
        suffix: Some(Suffix {
            from: index,
            entries: vec![LogEntry {
                index,
                term,
                payload: EntryPayload::Command {
                    operation: OperationId::new(index.into()).unwrap(),
                    bytes: vec![value; 1300],
                },
            }],
        }),
        snapshot: None,
        snapshot_membership: None,
    })
}
pub(super) fn expected(old: &GroupLog, mutation: &LogMutation) -> GroupLog {
    let mut state = BTreeMap::from([(group(), old.clone())]);
    apply_batch(
        &mut state,
        std::slice::from_ref(mutation),
        LogLimits::default(),
    )
    .unwrap();
    state.remove(&group()).unwrap()
}
pub(super) fn commit(store: &mut impl LogStore, mutations: Vec<LogMutation>) -> Vec<LogTicket> {
    let tickets = store.append_batch(mutations).unwrap();
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    tickets
}
pub(super) fn fill(store: &mut impl LogStore) {
    for _ in 0..24 {
        let state = store.state(group()).unwrap();
        commit(
            store,
            vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: state.revision,
                hard_state: HardState {
                    term: state.hard_state.term + 1,
                    voted_for: None,
                },
                commit_index: state.commit_index,
                suffix: None,
                snapshot: None,
                snapshot_membership: None,
            })],
        );
    }
}
pub(super) fn exact(store: &impl LogStore, state: &GroupLog) {
    assert_eq!(store.state(group()).unwrap(), *state);
    assert_eq!(
        store
            .fetch_range(group(), state.generation, 1, 16, 32768)
            .unwrap(),
        state.entries
    );
}
pub(super) fn fenced(store: &mut impl LogStore, layout: &Layout, mutation: LogMutation) {
    assert_eq!(store.state(group()), Err(StorageError::Fenced));
    assert_eq!(
        store.append_batch(vec![mutation]),
        Err(StorageError::Fenced)
    );
    assert_eq!(
        store.reclaim(store.limits().max_wal_bytes),
        Err(StorageError::Fenced)
    );
    assert!(
        FileStripedIo::open(layout.lanes.clone()).is_err(),
        "failed original retains lane ownership"
    );
    for lane in &layout.lanes {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lane.join("LOCK"))
            .unwrap();
        assert!(lock.try_lock().is_err(), "every failed lane remains owned");
    }
}
