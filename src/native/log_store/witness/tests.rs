// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{contracts::*, identity::*, log::*, native::log_store::NativeLogStore, quorum::*};
use std::sync::atomic::{AtomicU64, Ordering};
mod crash;
mod selection;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "voteboat-witness-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn populated(path: &Path) -> NativeLogStore<WitnessLogIo> {
    let mut store = NativeLogStore::create(
        WitnessLogIo::create(path).unwrap(),
        identity(),
        LogLimits::default(),
    )
    .unwrap();
    let node = NodeId::new(1).unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Create(Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: [(node, identity())].into_iter().collect(),
        })])
        .unwrap();
    store.barrier(&tickets).unwrap();
    store
}
fn pending(store: &mut NativeLogStore<WitnessLogIo>, term: u64) -> Vec<LogTicket> {
    let state = store.state(group()).unwrap();
    store
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group: group(),
            expected_revision: state.revision,
            hard_state: HardState {
                term,
                voted_for: None,
            },
            commit_index: 0,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap()
}
fn recover(path: &Path) -> Result<NativeLogStore<WitnessLogIo>, StorageError> {
    let io = WitnessLogIo::open(path).map_err(|e| StorageError::Uncertain(e.to_string()))?;
    NativeLogStore::recover(io, identity(), LogLimits::default())
}
fn write_image(path: &Path, log: &[u8], acknowledgement: &[u8]) {
    fs::write(path.join("log.wal"), log).unwrap();
    fs::write(path.join("MANIFEST"), acknowledgement).unwrap();
}
#[test]
fn ordinary_barrier_has_exact_state_two_syncs_and_no_namespace_steps() {
    let path = Directory::new();
    let mut store = populated(&path.0);
    let timings = JournalTimings::default();
    store.io.timings = Some(timings.clone());
    let binding = store.binding();
    let tickets = pending(&mut store, 2);
    assert_eq!(store.state(group()).unwrap().hard_state.term, 0);
    store.barrier(&tickets).unwrap();
    let expected = store.state(group()).unwrap();
    assert_eq!(expected.hard_state.term, 2);
    assert_eq!(store.io.primary, store.io.secondary);
    let recorded = timings.snapshot();
    assert_eq!(recorded.log_sync.calls, 1);
    assert_eq!(recorded.manifest.calls, 1);
    assert_eq!(recorded.publication.write.calls, 1);
    assert_eq!(recorded.publication.file_sync.calls, 1);
    assert_eq!(
        recorded.publication.open.calls
            + recorded.publication.rename.calls
            + recorded.publication.directory_sync.calls,
        0
    );
    assert_eq!(store.barrier(&tickets), Err(StorageError::StaleTicket));
    drop(store);
    let reopened = recover(&path.0).unwrap();
    assert_eq!(reopened.state(group()).unwrap(), expected);
    assert!(reopened.binding().session > binding.session);
}
#[test]
fn standalone_manifest_publication_is_a_complete_safe_barrier() {
    let path = Directory::new();
    let mut store = populated(&path.0);
    pending(&mut store, 2);
    let bytes = crate::native::log_store::manifest(store.binding(), store.length as u64);
    store.io.publish_manifest(&bytes).unwrap();
    drop(store);
    assert_eq!(
        recover(&path.0)
            .unwrap()
            .state(group())
            .unwrap()
            .hard_state
            .term,
        2
    );
}
