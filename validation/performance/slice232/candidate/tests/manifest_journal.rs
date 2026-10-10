// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
use voteboat::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    native::log_store::*,
    quorum::*,
};
fn bootstrap() -> Bootstrap {
    let node = NodeId::new(1).unwrap();
    Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: [(
            node,
            StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )]
        .into(),
    }
}
#[test]
fn full_file_syncs_and_real_open_failure_preserve_scoped_recovery() {
    let root = std::env::temp_dir().join(format!("voteboat-journal-public-{}", std::process::id()));
    let timing = JournalTimings::default();
    let boot = bootstrap();
    let identity = boot.voter_stores[&NodeId::new(1).unwrap()];
    let mut store = NativeLogStore::create(
        FileLogIo::create_with_manifest_journal(&root)
            .unwrap()
            .with_timings(timing.clone()),
        identity,
        LogLimits::default(),
    )
    .unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Create(boot.clone())])
        .unwrap();
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    let before = timing.snapshot();
    assert_eq!(before.log_sync.calls, 2);
    assert_eq!(before.manifest.calls, 2);
    assert_eq!(before.publication.open.calls, 2);
    assert_eq!(before.publication.write.calls, 2);
    assert_eq!(before.publication.file_sync.calls, 2);
    assert_eq!(before.publication.rename.calls, 1);
    assert_eq!(before.publication.directory_sync.calls, 1);
    let state = store.state(boot.group).unwrap();
    let promise = HardState {
        term: 1,
        voted_for: Some(NodeId::new(1).unwrap()),
    };
    let pending = store
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group: boot.group,
            expected_revision: state.revision,
            hard_state: promise,
            commit_index: 0,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    std::fs::rename(root.join("MANIFEST"), root.join("saved-manifest")).unwrap();
    std::fs::create_dir(root.join("MANIFEST")).unwrap();
    assert!(store.barrier(&pending).is_err());
    assert_eq!(store.state(boot.group), Err(StorageError::Fenced));
    assert!(
        FileLogIo::open(&root).is_err(),
        "failed publication keeps writer lock"
    );
    let failed = timing.snapshot();
    assert_eq!(failed.log_sync.calls, 3);
    assert_eq!(failed.manifest.errors, 1);
    assert_eq!(failed.publication.open.errors, 1);
    assert_eq!(failed.publication.write, before.publication.write);
    assert_eq!(failed.publication.file_sync, before.publication.file_sync);
    std::fs::remove_dir(root.join("MANIFEST")).unwrap();
    std::fs::rename(root.join("saved-manifest"), root.join("MANIFEST")).unwrap();
    drop(store);
    let mut recovered = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        identity,
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(
        recovered.state(boot.group).unwrap().hard_state,
        promise,
        "actual complete synced bytes may recover without a success receipt"
    );
    assert_eq!(recovered.binding().session.get(), 2);
    assert!(matches!(
        recovered.barrier(&pending),
        Err(StorageError::StaleTicket)
    ));
    assert_eq!(
        timing.snapshot(),
        failed,
        "recovery does not own the prior reader"
    );
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}
