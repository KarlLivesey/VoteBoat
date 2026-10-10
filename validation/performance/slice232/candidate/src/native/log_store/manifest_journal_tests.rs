// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{contracts::HardState, native::log_store::*, quorum::*};
use std::collections::BTreeMap;
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
fn path(name: &str, case: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "voteboat-manifest-journal-{}-{name}-{case}",
        std::process::id()
    ))
}
fn populated(path: &Path, generation: u64) -> NativeLogStore<FileLogIo> {
    let mut store = NativeLogStore::create(
        FileLogIo::create_with_manifest_journal(path).unwrap(),
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
            voter_stores: [(node, identity())].into(),
        })])
        .unwrap();
    store.barrier(&tickets).unwrap();
    for _ in 0..30 {
        let old = store.state(group()).unwrap();
        let tickets = store
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: old.revision,
                hard_state: HardState {
                    term: old.hard_state.term + 1,
                    voted_for: Some(node),
                },
                commit_index: 0,
                suffix: None,
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap();
        store.barrier(&tickets).unwrap();
    }
    if generation == 1 {
        let reclaimed = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(reclaimed.after_bytes < reclaimed.before_bytes);
    }
    assert_eq!(store.io.generation, generation);
    store
}
fn recover(path: &Path) -> NativeLogStore<FileLogIo> {
    NativeLogStore::recover(
        FileLogIo::open(path).unwrap(),
        identity(),
        LogLimits::default(),
    )
    .unwrap()
}
fn next(store: &NativeLogStore<FileLogIo>) -> Vec<u8> {
    crate::native::log_store::manifest(
        StoreBinding {
            session: StoreSession::new(store.binding().session.get() + 1).unwrap(),
            ..store.binding()
        },
        store.length as u64,
    )
}
#[test]
fn append_boundaries_preserve_acknowledged_vote_and_exact_recovery_session() {
    for generation in 0..=1 {
        for cut in 1..=4 {
            let path = path("append", generation as usize * 4 + cut);
            let mut store = populated(&path, generation);
            let expected = store.state(group()).unwrap();
            let metadata = next(&store);
            let timing = JournalTimings::default();
            store.io.timings = Some(timing.clone());
            let mut step = 0;
            assert!(store
                .io
                .publish_journal_with(&metadata, || {
                    step += 1;
                    if step == cut {
                        Err(io::Error::other("append boundary cut"))
                    } else {
                        Ok(())
                    }
                })
                .is_err());
            assert!(
                store.io.metadata.is_none(),
                "error invalidates cached metadata"
            );
            assert!(
                FileLogIo::open(&path).is_err(),
                "error retains exclusive lock"
            );
            let calls = timing.snapshot().publication;
            assert_eq!(calls.open.calls, 1);
            assert_eq!(calls.write.calls, u64::from(cut >= 3));
            assert_eq!(calls.file_sync.calls, u64::from(cut >= 4));
            assert_eq!(calls.rename.calls + calls.directory_sync.calls, 0);
            drop(store);
            let recovered = recover(&path);
            assert_eq!(recovered.state(group()).unwrap(), expected);
            assert_eq!(
                recovered.binding().session.get(),
                if cut < 3 { 2 } else { 3 }
            );
            drop(recovered);
            fs::remove_dir_all(path).unwrap();
        }
    }
}
#[test]
fn initial_and_bounded_compaction_use_full_atomic_publication() {
    for cut in 1..=5 {
        let initial = path("initial", cut);
        let mut io = FileLogIo::create_with_manifest_journal(&initial).unwrap();
        io.sync_log().unwrap();
        let metadata = crate::native::log_store::manifest(
            StoreBinding {
                identity: identity(),
                session: StoreSession::new(1).unwrap(),
            },
            0,
        );
        let mut step = 0;
        assert!(io
            .publish_journal_with(&metadata, || {
                step += 1;
                if step == cut {
                    Err(io::Error::other("initial cut"))
                } else {
                    Ok(())
                }
            })
            .is_err());
        drop(io);
        if cut < 4 {
            assert!(FileLogIo::open(&initial).is_err());
        } else {
            assert_eq!(recover(&initial).binding().session.get(), 2);
        }
        fs::remove_dir_all(initial).unwrap();
        compaction(cut);
    }
}
fn compaction(cut: usize) {
    let path = path("compact", cut);
    let mut store = populated(&path, 0);
    let expected = store.state(group()).unwrap();
    let old = store.io.read_manifest().unwrap();
    let mut full = manifest_journal::MAGIC.to_vec();
    for _ in 0..manifest_journal::RECORDS {
        full.extend(&old);
    }
    fs::write(path.join("MANIFEST"), &full).unwrap();
    File::open(path.join("MANIFEST"))
        .unwrap()
        .sync_all()
        .unwrap();
    store.io.metadata = None;
    let mut step = 0;
    let metadata = next(&store);
    assert!(store
        .io
        .publish_journal_with(&metadata, || {
            step += 1;
            if step == cut {
                Err(io::Error::other("compaction cut"))
            } else {
                Ok(())
            }
        })
        .is_err());
    assert!(FileLogIo::open(&path).is_err());
    drop(store);
    let mut reopened = recover(&path);
    assert_eq!(reopened.state(group()).unwrap(), expected);
    assert!(
        fs::metadata(path.join("MANIFEST")).unwrap().len()
            <= (8 + 2 * manifest_journal::RECORD) as u64
    );
    let current = reopened.io.read_manifest().unwrap();
    reopened.io.publish_manifest(&current).unwrap();
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn every_partial_metadata_tail_recovers_the_last_complete_prefix() {
    for generation in 0..=1 {
        let base = path("tails-base", generation);
        let mut store = populated(&base, generation as u64);
        let expected = store.state(group()).unwrap();
        let metadata = next(&store);
        let name = manifest_name(generation as u64);
        let last = store.io.read_manifest().unwrap();
        drop(store);
        for length in 0..=manifest_journal::RECORD {
            let path = path("tail", generation * 53 + length);
            fs::create_dir(&path).unwrap();
            for item in fs::read_dir(&base).unwrap() {
                let item = item.unwrap();
                fs::copy(item.path(), path.join(item.file_name())).unwrap();
            }
            OpenOptions::new()
                .append(true)
                .open(path.join(&name))
                .unwrap()
                .write_all(&metadata[..length])
                .unwrap();
            let mut io = FileLogIo::open(&path).unwrap();
            assert_eq!(
                io.read_manifest().unwrap(),
                if length == 52 { &metadata } else { &last }.to_vec()
            );
            let recovered = NativeLogStore::recover(io, identity(), LogLimits::default()).unwrap();
            assert_eq!(recovered.state(group()).unwrap(), expected);
            assert_eq!(
                recovered.binding().session.get(),
                if length == 52 { 3 } else { 2 }
            );
            drop(recovered);
            assert!(fs::metadata(path.join(&name)).unwrap().len() <= (8 + 34 * 52) as u64);
            fs::remove_dir_all(path).unwrap();
        }
        fs::remove_dir_all(base).unwrap();
    }
}
#[test]
fn complete_corruption_regression_and_unsupported_containers_fail_closed() {
    let binding = StoreBinding {
        identity: identity(),
        session: StoreSession::new(2).unwrap(),
    };
    let row = crate::native::log_store::manifest(binding, 100);
    let mut valid = manifest_journal::MAGIC.to_vec();
    valid.extend(&row);
    valid.extend(&row);
    for at in 0..valid.len() {
        let mut corrupt = valid.clone();
        corrupt[at] ^= 1;
        assert!(
            ManifestJournal::decode(&corrupt).is_err(),
            "complete corrupt byte={at}"
        );
    }
    for (owner, boundary) in [
        (
            StoreBinding {
                session: StoreSession::new(1).unwrap(),
                ..binding
            },
            100,
        ),
        (binding, 99),
        (
            StoreBinding {
                identity: StoreIdentity {
                    id: StoreId::new(2).unwrap(),
                    ..identity()
                },
                ..binding
            },
            100,
        ),
    ] {
        let mut bytes = manifest_journal::MAGIC.to_vec();
        bytes.extend(&row);
        bytes.extend(crate::native::log_store::manifest(owner, boundary));
        assert!(ManifestJournal::decode(&bytes).is_err());
    }
    for length in 0..60 {
        assert!(ManifestJournal::decode(&valid[..length]).is_err());
    }
    assert!(
        crate::native::log_store::read_manifest(&valid).is_err(),
        "legacy readers reject rather than reinterpret new metadata"
    );
    let mut excessive = manifest_journal::MAGIC.to_vec();
    for _ in 0..=manifest_journal::RECORDS {
        excessive.extend(&row);
    }
    assert!(ManifestJournal::decode(&excessive).is_err());
}
#[test]
fn every_replacement_boundary_retains_one_selected_journal_format_and_state() {
    for generation in 0..=1 {
        for cut in 1..=13 {
            let path = path("replace", generation as usize * 13 + cut);
            let mut store = populated(&path, generation);
            let expected = store.state(group()).unwrap();
            let states: BTreeMap<_, _> = [(group(), expected.clone())].into();
            let bytes = NativeLogCodec
                .encode_checkpoint(
                    store.sequence,
                    &states,
                    store.limits(),
                    store.limits().max_wal_bytes,
                )
                .unwrap();
            let metadata = crate::native::log_store::manifest(store.binding(), bytes.len() as u64);
            let mut step = 0;
            assert!(store
                .io
                .replace_with(&bytes, &metadata, || {
                    step += 1;
                    if step == cut {
                        Err(io::Error::other("replacement cut"))
                    } else {
                        Ok(())
                    }
                })
                .is_err());
            assert!(FileLogIo::open(&path).is_err());
            drop(store);
            let mut recovered = recover(&path);
            assert!(recovered.io.journal);
            assert_eq!(recovered.state(group()).unwrap(), expected);
            let selected = recovered.io.read_manifest().unwrap();
            assert!(
                recovered.io.publish_manifest(&metadata).is_err(),
                "prior session metadata cannot replace recovered binding"
            );
            assert_eq!(recovered.io.read_manifest().unwrap(), selected);
            recovered.io.publish_manifest(&selected).unwrap();
            drop(recovered);
            fs::remove_dir_all(path).unwrap();
        }
    }
}
