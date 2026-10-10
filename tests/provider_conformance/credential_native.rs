// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::credential_cases::{exercise, owner, record};
use std::{
    cell::RefCell,
    io::ErrorKind,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use voteboat::{credential_reload::*, native::credential_journal::*};

#[derive(Clone, Copy, Default)]
enum Failure {
    #[default]
    None,
    Certain,
    Before,
    After,
    Fenced,
}

#[derive(Default)]
struct State {
    bytes: Option<[u8; 128]>,
    failure: Failure,
    read_error: Option<CredentialJournalError>,
    writes: usize,
}

#[derive(Clone, Default)]
struct MemoryIo(Rc<RefCell<State>>);
impl CredentialRecordIo for MemoryIo {
    fn read(&mut self) -> Result<Option<[u8; 128]>, CredentialJournalError> {
        let state = self.0.borrow();
        state.read_error.map_or(Ok(state.bytes), Err)
    }
    fn replace(&mut self, bytes: &[u8; 128]) -> Result<(), CredentialJournalError> {
        let mut state = self.0.borrow_mut();
        state.writes += 1;
        if matches!(state.failure, Failure::None | Failure::After) {
            state.bytes = Some(*bytes);
        }
        match state.failure {
            Failure::None => Ok(()),
            Failure::Fenced => Err(CredentialJournalError::Fenced),
            other => Err(CredentialJournalError::Io {
                kind: ErrorKind::Other,
                uncertain: !matches!(other, Failure::Certain),
            }),
        }
    }
}

fn open<I: CredentialRecordIo>(io: I) -> NativeCredentialJournal<I> {
    NativeCredentialJournal::new(io, owner()).unwrap_or_else(|(error, _)| panic!("{error:?}"))
}

pub fn raw_roundtrip(io: &mut impl CredentialRecordIo) {
    assert_eq!(io.read(), Ok(None));
    let first = std::array::from_fn(|i| i as u8);
    let second = std::array::from_fn(|i| 255 - i as u8);
    io.replace(&first).unwrap();
    assert_eq!(io.read(), Ok(Some(first)));
    io.replace(&second).unwrap();
    assert_eq!(io.read(), Ok(Some(second)));
    io.replace(&second).unwrap();
    assert_eq!(io.read(), Ok(Some(second)));
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "voteboat-credential-contract-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn native_host_io_journal_satisfies_shared_contract_and_exact_retry_is_not_rewritten() {
    let io = MemoryIo::default();
    let mut journal = open(io.clone());
    let last = exercise(&mut journal);
    assert_eq!(io.0.borrow().writes, 3);
    let mut reopened = open(journal.into_io());
    assert_eq!(reopened.latest(), Ok(Some(last)));
    reopened.publish(last).unwrap();
    assert_eq!(io.0.borrow().writes, 3);
}

#[test]
fn file_journal_satisfies_shared_contract_and_independent_reopen() {
    let root = Scratch::new();
    let mut first = open(FileCredentialRecord::new(root.0.join("first")));
    let last = exercise(&mut first);
    let mut second = open(FileCredentialRecord::new(root.0.join("second")));
    assert_eq!(exercise(&mut second), last);
    let mut reopened = open(first.into_io());
    assert_eq!(reopened.latest(), Ok(Some(last)));
    reopened.publish(last).unwrap();
    assert_eq!(std::fs::metadata(root.0.join("first")).unwrap().len(), 128);
    drop(reopened);
    assert_eq!(second.latest(), Ok(Some(last)));
    assert_eq!(open(second.into_io()).latest(), Ok(Some(last)));
}

#[test]
fn host_and_file_record_io_preserve_exact_bounded_replacement() {
    raw_roundtrip(&mut MemoryIo::default());
    let root = Scratch::new();
    let path = root.0.join("record");
    raw_roundtrip(&mut FileCredentialRecord::new(&path));
    let last = std::array::from_fn(|i| 255 - i as u8);
    assert_eq!(FileCredentialRecord::new(&path).read(), Ok(Some(last)));
    for len in [0, 1, 127, 129, 256] {
        std::fs::write(&path, vec![7; len]).unwrap();
        assert_eq!(
            FileCredentialRecord::new(&path).read(),
            Err(CredentialJournalError::InvalidRecord)
        );
    }
}

#[test]
fn certain_and_uncertain_replacements_preserve_their_distinct_recovery_contracts() {
    for failure in [
        Failure::Certain,
        Failure::Before,
        Failure::After,
        Failure::Fenced,
    ] {
        let io = MemoryIo::default();
        let mut journal = open(io.clone());
        let first = record(1, 1, 2);
        let next = record(2, 2, 3);
        journal.publish(first).unwrap();
        io.0.borrow_mut().failure = failure;
        let expected_error = match failure {
            Failure::Fenced => CredentialJournalError::Fenced,
            _ => CredentialJournalError::Io {
                kind: ErrorKind::Other,
                uncertain: !matches!(failure, Failure::Certain),
            },
        };
        assert_eq!(journal.publish(next), Err(expected_error));
        assert_eq!(journal.owner(), owner());
        if matches!(failure, Failure::Certain) {
            assert_eq!(journal.latest(), Ok(Some(first)));
            io.0.borrow_mut().failure = Failure::None;
            journal.publish(next).unwrap();
            assert_eq!(journal.latest(), Ok(Some(next)));
        } else {
            assert_eq!(journal.latest(), Err(CredentialJournalError::Fenced));
            let writes = io.0.borrow().writes;
            assert_eq!(journal.publish(next), Err(CredentialJournalError::Fenced));
            assert_eq!(io.0.borrow().writes, writes);
            io.0.borrow_mut().failure = Failure::None;
            let mut recovered = open(journal.into_io());
            let expected = if matches!(failure, Failure::After) {
                next
            } else {
                first
            };
            assert_eq!(recovered.latest(), Ok(Some(expected)));
            recovered.publish(next).unwrap();
            assert_eq!(recovered.latest(), Ok(Some(next)));
        }
    }
}

#[test]
fn failed_construction_returns_the_same_io_owner_without_replacement() {
    let io = MemoryIo::default();
    let mut journal = open(io.clone());
    journal.publish(record(1, 1, 2)).unwrap();
    let original = io.0.borrow().bytes;
    drop(journal);
    let mut wrong_owners = [owner(); 3];
    wrong_owners[0].node = voteboat::identity::NodeId::new(99).unwrap();
    wrong_owners[1].store.id = voteboat::identity::StoreId::new(99).unwrap();
    wrong_owners[2].store.incarnation = voteboat::identity::StoreIncarnation::new(99).unwrap();
    for owner in wrong_owners {
        let Err((error, returned)) = NativeCredentialJournal::new(io.clone(), owner) else {
            panic!("foreign owner accepted");
        };
        assert_eq!(error, CredentialJournalError::WrongOwner);
        assert!(Rc::ptr_eq(&returned.0, &io.0));
        assert_eq!(returned.0.borrow().bytes, original);
    }
    for error in [
        CredentialJournalError::Io {
            kind: ErrorKind::PermissionDenied,
            uncertain: false,
        },
        CredentialJournalError::InvalidRecord,
    ] {
        io.0.borrow_mut().read_error = Some(error);
        let Err((actual, returned)) = NativeCredentialJournal::new(io.clone(), owner()) else {
            panic!("failed read accepted");
        };
        assert_eq!(actual, error);
        assert!(Rc::ptr_eq(&returned.0, &io.0));
    }
    io.0.borrow_mut().read_error = None;
    io.0.borrow_mut().bytes.as_mut().unwrap()[70] ^= 1;
    let Err((error, returned)) = NativeCredentialJournal::new(io.clone(), owner()) else {
        panic!("corrupt record accepted");
    };
    assert_eq!(error, CredentialJournalError::InvalidRecord);
    assert!(Rc::ptr_eq(&returned.0, &io.0));
    assert_eq!(io.0.borrow().writes, 1);
    io.0.borrow_mut().bytes = original;
    assert_eq!(open(returned).latest(), Ok(Some(record(1, 1, 2))));
}
