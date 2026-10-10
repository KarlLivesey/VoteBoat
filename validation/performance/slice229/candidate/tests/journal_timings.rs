// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
use voteboat::{identity::*, log::*, native::log_store::*, quorum::*};
fn binding() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(700).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap() -> Bootstrap {
    let node = NodeId::new(1).unwrap();
    Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(701).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: [(node, binding())].into(),
    }
}
#[test]
fn timing_selection_preserves_exact_barriers_default_recovery_and_reader_lifetime() {
    let root = std::env::temp_dir().join(format!("voteboat-journal-timing-{}", std::process::id()));
    let timing = JournalTimings::default();
    let retained = timing.clone();
    let io = FileLogIo::create(&root).unwrap().with_timings(timing);
    let mut store = NativeLogStore::create(io, binding(), LogLimits::default()).unwrap();
    let boot = bootstrap();
    let tickets = store
        .append_batch(vec![LogMutation::Create(boot.clone())])
        .unwrap();
    assert!(store.state(boot.group).is_err());
    let completion = store.barrier(&tickets).unwrap();
    assert_eq!(completion.tickets, tickets);
    assert_eq!(store.state(boot.group).unwrap().bootstrap, boot);
    let snapshot = retained.snapshot();
    assert_eq!(snapshot.log_sync.calls, 2);
    assert_eq!(snapshot.manifest.calls, 2);
    for step in publication_steps(snapshot.publication) {
        assert_eq!(step.calls, 2);
        assert_eq!(step.errors, 0);
        assert!(step.max_ns <= step.elapsed_ns);
    }
    assert_eq!(
        snapshot.log_sync.errors + snapshot.manifest.errors + snapshot.append.errors,
        0
    );
    assert!(store.barrier(&tickets).is_err());
    assert_eq!(
        retained.snapshot(),
        snapshot,
        "rejected stale barriers perform no file I/O"
    );
    drop(store);
    let recovered = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        binding(),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(recovered.state(boot.group).unwrap().bootstrap, boot);
    assert_eq!(
        retained.snapshot(),
        snapshot,
        "default recovery neither owns nor resets host reader"
    );
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn failed_publication_is_counted_without_becoming_durable_evidence() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-journal-timing-fault-{}",
        std::process::id()
    ));
    let timing = JournalTimings::default();
    let mut store = NativeLogStore::create(
        FileLogIo::create(&root)
            .unwrap()
            .with_timings(timing.clone()),
        binding(),
        LogLimits::default(),
    )
    .unwrap();
    let boot = bootstrap();
    let tickets = store
        .append_batch(vec![LogMutation::Create(boot.clone())])
        .unwrap();
    // A real native staging-open failure before the pending WAL sync is invoked.
    std::fs::create_dir(root.join("MANIFEST.tmp")).unwrap();
    assert!(store.barrier(&tickets).is_err());
    assert!(store.state(boot.group).is_err());
    let snapshot = timing.snapshot();
    assert_eq!(snapshot.log_sync.calls, 1);
    assert_eq!(snapshot.manifest.calls, 2);
    assert_eq!(snapshot.manifest.errors, 1);
    assert_eq!(snapshot.publication.open.calls, 2);
    assert_eq!(snapshot.publication.open.errors, 1);
    for step in publication_steps(snapshot.publication).into_iter().skip(1) {
        assert_eq!(step.calls, 1, "failed open must skip later operations");
        assert_eq!(step.errors, 0);
    }
    drop(store);
    std::fs::remove_dir(root.join("MANIFEST.tmp")).unwrap();
    let recovered = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        binding(),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(recovered.state(boot.group).unwrap().bootstrap, boot,
        "complete written bytes may recover despite a lost publication result; the error acknowledged nothing");
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}

fn publication_steps(timing: ManifestPublicationTiming) -> [JournalCallTiming; 5] {
    [
        timing.open,
        timing.write,
        timing.file_sync,
        timing.rename,
        timing.directory_sync,
    ]
}

#[test]
fn failed_rename_counts_only_invoked_steps_and_retains_staging_file() {
    let root = std::env::temp_dir().join(format!("voteboat-journal-rename-{}", std::process::id()));
    let timing = JournalTimings::default();
    let mut io = FileLogIo::create(&root)
        .unwrap()
        .with_timings(timing.clone());
    std::fs::create_dir(root.join("MANIFEST")).unwrap();
    assert!(io.publish_manifest(b"unselected test bytes").is_err());
    let snapshot = timing.snapshot();
    assert_eq!(snapshot.manifest.calls, 1);
    assert_eq!(snapshot.manifest.errors, 1);
    for step in publication_steps(snapshot.publication).into_iter().take(3) {
        assert_eq!(step.calls, 1);
        assert_eq!(step.errors, 0);
    }
    assert_eq!(snapshot.publication.rename.calls, 1);
    assert_eq!(snapshot.publication.rename.errors, 1);
    assert_eq!(
        snapshot.publication.directory_sync,
        JournalCallTiming::default()
    );
    assert_eq!(
        std::fs::read(root.join("MANIFEST.tmp")).unwrap(),
        b"unselected test bytes"
    );
    drop(io);
    assert!(NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        binding(),
        LogLimits::default()
    )
    .is_err());
    std::fs::remove_dir_all(root).unwrap();
}
