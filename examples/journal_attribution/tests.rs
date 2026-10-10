// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use voteboat::{
    contracts::HardState,
    quorum::{Limits, Policy, Tree},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
type Store = NativeLogStore<FileLogIo>;
struct Fixture {
    root: PathBuf,
    store: Option<Store>,
}
fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn command(index: u64, operation: u128, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        },
    }
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-journal-attribution-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let mut store =
            NativeLogStore::create(FileLogIo::create(&root).unwrap(), identity(), limits())
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
        Self {
            root,
            store: Some(store),
        }
    }
    fn submit(&mut self, commit: u64, term: u64, suffix: Option<Suffix>) {
        let tickets = self.append(commit, term, suffix);
        self.store.as_mut().unwrap().barrier(&tickets).unwrap();
    }
    fn append(&mut self, commit: u64, term: u64, suffix: Option<Suffix>) -> Vec<LogTicket> {
        let store = self.store.as_mut().unwrap();
        let old = store.state(group()).unwrap();
        store
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: old.revision,
                hard_state: HardState {
                    term,
                    voted_for: None,
                },
                commit_index: commit,
                suffix,
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap()
    }
    fn close(&mut self) {
        self.store.take();
    }
    fn bytes(&self) -> (Vec<u8>, Vec<u8>) {
        (
            fs::read(self.root.join("MANIFEST")).unwrap(),
            fs::read(self.root.join("log.wal")).unwrap(),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.close();
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn combined_batch_commits_the_old_command_not_its_new_append() {
    let mut f = Fixture::new();
    f.submit(
        0,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 10, 1)],
        }),
    );
    f.submit(
        1,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![command(2, 20, 1)],
        }),
    );
    f.submit(2, 1, None);
    f.close();
    let original = f.bytes();
    let a = inspect(&f.root, identity()).unwrap();
    assert_eq!(a.batches.len(), 4);
    let combined = &a.batches[2];
    assert_eq!(combined.class, "combined");
    assert_eq!((combined.before, combined.after), (0, 1));
    assert_eq!(
        combined.events,
        [
            Event {
                kind: "append",
                operation: 20,
                index: 2,
                term: 1,
                delta: 1
            },
            Event {
                kind: "commit",
                operation: 10,
                index: 1,
                term: 1,
                delta: 1
            },
        ]
    );
    assert_eq!(a.batches[3].class, "commit_only");
    assert_eq!(a.batches[3].events[0].operation, 20);
    assert_eq!(a.state.commit_index, 2);
    assert_eq!(f.bytes(), original);
    let lock = FileLogIo::open(&f.root).unwrap();
    assert!(inspect(&f.root, identity()).is_err());
    assert_eq!(f.bytes(), original);
    drop(lock);
    assert!(inspect(&f.root, identity()).is_ok());
    assert_eq!(f.bytes(), original);
}

#[test]
fn replacement_keeps_the_discarded_append_distinct_from_the_committed_entry() {
    let mut f = Fixture::new();
    f.submit(
        0,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 10, 1)],
        }),
    );
    f.submit(
        0,
        2,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 20, 2)],
        }),
    );
    f.submit(1, 2, None);
    f.close();
    let a = inspect(&f.root, identity()).unwrap();
    assert_eq!(a.batches[1].events[0].operation, 10);
    assert_eq!(a.batches[2].events[0].operation, 20);
    assert!(a.batches[2].generation > a.batches[1].generation);
    let committed = &a.batches[3].events;
    assert_eq!(
        committed,
        &[Event {
            kind: "commit",
            operation: 20,
            index: 1,
            term: 2,
            delta: 1
        }]
    );
}

#[test]
fn complete_written_and_torn_tails_are_not_credited_as_original_durability() {
    let mut f = Fixture::new();
    let sealed = f.bytes();
    f.append(
        0,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 10, 1)],
        }),
    );
    f.close();
    let written = f.bytes();
    assert_eq!(written.0, sealed.0);
    assert!(written.1.len() > sealed.1.len());
    let error = match inspect(&f.root, identity()) {
        Ok(_) => panic!("written is not sealed"),
        Err(e) => e,
    };
    assert!(error.to_string().contains("unsealed journal"), "{error}");
    assert_eq!(f.bytes(), written);
    let mut torn = sealed.1.clone();
    torn.extend([1, 2, 3]);
    assert!(analyse(&sealed.0, &torn, identity()).is_err());
    assert_eq!(f.bytes(), written);
}

#[test]
fn native_identity_and_durable_corruption_refuse_without_modifying_source() {
    let mut f = Fixture::new();
    f.submit(
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 10, 1)],
        }),
    );
    f.close();
    let original = f.bytes();
    let wrong = StoreIdentity {
        id: StoreId::new(2).unwrap(),
        ..identity()
    };
    assert!(inspect(&f.root, wrong).is_err());
    let mut manifest = original.0.clone();
    manifest[48] ^= 1;
    assert!(analyse(&manifest, &original.1, identity()).is_err());
    let mut bytes = original.1.clone();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    assert!(analyse(&original.0, &bytes, identity()).is_err());
    assert_eq!(f.bytes(), original);
}

#[test]
fn current_selection_and_missing_lock_are_refused_without_creating_files() {
    let mut f = Fixture::new();
    f.submit(
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command(1, 10, 1)],
        }),
    );
    for term in 2..=8 {
        f.submit(1, term, None);
    }
    f.store
        .as_mut()
        .unwrap()
        .reclaim(limits().max_wal_bytes)
        .unwrap();
    f.close();
    assert!(f.root.join("CURRENT").exists());
    let files = || {
        fs::read_dir(&f.root)
            .unwrap()
            .map(|e| {
                let path = e.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>()
    };
    let before = files();
    assert!(inspect(&f.root, identity()).is_err());
    assert_eq!(files(), before);
    let mut legacy = Fixture::new();
    legacy.close();
    let original = legacy.bytes();
    fs::remove_file(legacy.root.join("LOCK")).unwrap();
    assert!(inspect(&legacy.root, identity()).is_err());
    assert!(!legacy.root.join("LOCK").exists());
    assert_eq!(legacy.bytes(), original);
}
