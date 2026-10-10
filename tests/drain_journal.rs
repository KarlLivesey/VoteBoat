// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
use std::{cell::RefCell, io::ErrorKind, rc::Rc};
use voteboat::{drain::*, identity::*, native::drain_journal::*, runtime::*, secure::PeerIdentity};
fn owner() -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(1).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    }
}
fn record() -> DrainRecord {
    DrainRecord {
        owner: owner(),
        sequence: 1,
        request: LocalDrainRequest {
            operation: OperationId::new(100).unwrap(),
            groups: (1..=2)
                .map(|id| DrainGroup {
                    group: GroupIdentity {
                        id: GroupId::new(id).unwrap(),
                        incarnation: GroupIncarnation::new(1).unwrap(),
                    },
                    configuration: ConfigurationId::new(1).unwrap(),
                })
                .collect(),
        },
        phase: DrainPhase::Active,
    }
}
#[derive(Default)]
struct State {
    bytes: Option<Vec<u8>>,
    fail: u8,
    writes: usize,
}
#[derive(Clone, Default)]
struct HostIo(Rc<RefCell<State>>);
impl DrainRecordIo for HostIo {
    fn read(&mut self) -> Result<Option<Vec<u8>>, DrainJournalError> {
        Ok(self.0.borrow().bytes.clone())
    }
    fn replace(&mut self, b: &[u8]) -> Result<(), DrainJournalError> {
        let mut state = self.0.borrow_mut();
        state.writes += 1;
        if state.fail == 0 || state.fail == 3 {
            state.bytes = Some(b.to_vec());
        }
        if state.fail == 0 {
            Ok(())
        } else {
            Err(DrainJournalError::Io {
                kind: ErrorKind::Other,
                uncertain: state.fail != 1,
            })
        }
    }
}
fn open(io: HostIo) -> NativeDrainJournal<HostIo> {
    NativeDrainJournal::new(io, owner()).ok().unwrap()
}
#[test]
fn exact_retries_and_cancellation_are_durable_but_stale_and_conflicting_intents_fail() {
    let io = HostIo::default();
    let mut journal = open(io.clone());
    let first = record();
    let mut cancelled = first.clone();
    cancelled.phase = DrainPhase::Cancelled;
    assert_eq!(
        journal.publish(cancelled.clone()),
        Err(DrainJournalError::Conflict)
    );
    journal.publish(first.clone()).unwrap();
    journal.publish(first.clone()).unwrap();
    assert_eq!(io.0.borrow().writes, 1);
    let mut changed = first.clone();
    changed.request.groups[1].configuration = ConfigurationId::new(2).unwrap();
    assert_eq!(journal.publish(changed), Err(DrainJournalError::Conflict));
    let mut next = first.clone();
    next.sequence = 2;
    next.request.operation = OperationId::new(101).unwrap();
    assert_eq!(
        journal.publish(next.clone()),
        Err(DrainJournalError::Conflict)
    );
    journal.publish(cancelled.clone()).unwrap();
    assert_eq!(journal.publish(first), Err(DrainJournalError::Conflict));
    journal.publish(next.clone()).unwrap();
    assert_eq!(journal.publish(cancelled), Err(DrainJournalError::Conflict));
    assert_eq!(open(journal.into_io()).latest().unwrap(), Some(next));
}
#[test]
fn failed_publication_retains_or_fences_until_old_or_complete_recovery() {
    for fail in 1..=3 {
        let io = HostIo::default();
        let mut journal = open(io.clone());
        journal.publish(record()).unwrap();
        let mut cancelled = record();
        cancelled.phase = DrainPhase::Cancelled;
        io.0.borrow_mut().fail = fail;
        assert!(journal.publish(cancelled.clone()).is_err());
        if fail == 1 {
            assert_eq!(journal.latest().unwrap(), Some(record()));
        } else {
            assert_eq!(journal.latest(), Err(DrainJournalError::Fenced));
        }
        io.0.borrow_mut().fail = 0;
        let expected = if fail == 3 { cancelled } else { record() };
        assert_eq!(open(journal.into_io()).latest().unwrap(), Some(expected));
    }
}
#[test]
fn every_truncation_and_corrupt_byte_refuses_instead_of_becoming_an_empty_journal() {
    let io = HostIo::default();
    open(io.clone()).publish(record()).unwrap();
    let bytes = io.0.borrow().bytes.clone().unwrap();
    for cut in 0..bytes.len() {
        io.0.borrow_mut().bytes = Some(bytes[..cut].to_vec());
        assert!(
            NativeDrainJournal::new(io.clone(), owner()).is_err(),
            "cut {cut}"
        );
    }
    for at in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[at] ^= 1;
        io.0.borrow_mut().bytes = Some(changed);
        assert!(
            NativeDrainJournal::new(io.clone(), owner()).is_err(),
            "byte {at}"
        );
    }
}
#[test]
fn owner_shape_and_maximum_retained_record_are_checked() {
    let io = HostIo::default();
    let mut journal = open(io.clone());
    let mut r = record();
    r.owner.node = NodeId::new(2).unwrap();
    assert_eq!(journal.publish(r), Err(DrainJournalError::WrongOwner));
    for shape in 0..3 {
        let mut r = record();
        match shape {
            0 => r.sequence = 0,
            1 => r.request.groups.reverse(),
            _ => r.request.groups[1] = r.request.groups[0],
        }
        assert_eq!(journal.publish(r), Err(DrainJournalError::InvalidRecord));
    }
    let mut maximum = record();
    maximum.request.groups = (1..=MAX_LOCAL_DRAIN_GROUPS)
        .map(|id| DrainGroup {
            group: GroupIdentity {
                id: GroupId::new(id as u128).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: ConfigurationId::new(1).unwrap(),
        })
        .collect();
    journal.publish(maximum.clone()).unwrap();
    assert_eq!(
        io.0.borrow().bytes.as_ref().unwrap().len(),
        MAX_DRAIN_RECORD_BYTES
    );
    assert_eq!(open(io.clone()).latest().unwrap(), Some(maximum));
    assert!(NativeDrainJournal::new(
        io,
        PeerIdentity {
            node: NodeId::new(2).unwrap(),
            ..owner()
        }
    )
    .is_err());
}

#[test]
fn valid_checksums_do_not_bypass_identity_length_phase_and_version_validation() {
    let io = HostIo::default();
    open(io.clone()).publish(record()).unwrap();
    let original = io.0.borrow().bytes.clone().unwrap();
    let mut malformed = Vec::new();
    for field in [
        8..16,
        16..24,
        24..40,
        40..48,
        48..64,
        80..96,
        96..104,
        104..112,
    ] {
        let mut b = original.clone();
        b[field].fill(0);
        malformed.push(b);
    }
    for (at, value) in [(0, b'X'), (64, 3), (65, 1)] {
        let mut b = original.clone();
        b[at] = value;
        malformed.push(b);
    }
    let mut count = original.clone();
    count[72..80].copy_from_slice(&u64::MAX.to_be_bytes());
    malformed.push(count);
    let mut length = original;
    length.push(0);
    malformed.push(length);
    for mut b in malformed {
        let end = b.len() - 32;
        let sum = ring::digest::digest(&ring::digest::SHA256, &b[..end]);
        b[end..].copy_from_slice(sum.as_ref());
        io.0.borrow_mut().bytes = Some(b);
        assert!(NativeDrainJournal::new(io.clone(), owner()).is_err());
    }
}
#[test]
fn native_file_ignores_interrupted_staging_and_rejects_corrupt_published_state() {
    let directory =
        std::env::temp_dir().join(format!("voteboat-drain-journal-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("DRAIN");
    let mut journal = NativeDrainJournal::new(FileDrainRecord::new(&path), owner())
        .ok()
        .unwrap();
    journal.publish(record()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    for cut in 0..bytes.len() {
        std::fs::write(path.with_extension("drain-staging"), &bytes[..cut]).unwrap();
        let reopened = NativeDrainJournal::new(FileDrainRecord::new(&path), owner())
            .ok()
            .unwrap();
        assert_eq!(reopened.latest().unwrap(), Some(record()));
    }
    let mut cancelled = record();
    cancelled.phase = DrainPhase::Cancelled;
    journal.publish(cancelled.clone()).unwrap();
    assert_eq!(
        NativeDrainJournal::new(FileDrainRecord::new(&path), owner())
            .ok()
            .unwrap()
            .latest()
            .unwrap(),
        Some(cancelled)
    );
    std::fs::write(&path, &bytes[..10]).unwrap();
    assert!(NativeDrainJournal::new(FileDrainRecord::new(&path), owner()).is_err());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn required_recovery_refuses_a_missing_record_with_returned_io() {
    let io = HostIo::default();
    let (error, io) = NativeDrainJournal::recover(io, owner()).err().unwrap();
    assert_eq!(error, DrainJournalError::MissingRecord);
    let mut journal = open(io);
    journal.publish(record()).unwrap();
    let recovered = NativeDrainJournal::recover(journal.into_io(), owner())
        .ok()
        .unwrap();
    assert_eq!(recovered.latest().unwrap(), Some(record()));
}

#[test]
fn initialized_empty_journal_recovers_without_inventing_an_operation() {
    let io = HostIo::default();
    let journal = NativeDrainJournal::initialize(io.clone(), owner())
        .ok()
        .unwrap();
    assert_eq!(journal.latest().unwrap(), None);
    assert_eq!(io.0.borrow().writes, 1);
    let mut journal = NativeDrainJournal::recover(journal.into_io(), owner())
        .ok()
        .unwrap();
    assert_eq!(journal.latest().unwrap(), None);
    journal.publish(record()).unwrap();
    let initialized = NativeDrainJournal::initialize(journal.into_io(), owner())
        .ok()
        .unwrap();
    assert_eq!(initialized.latest().unwrap(), Some(record()));
    assert_eq!(io.0.borrow().writes, 2);
}

#[test]
fn empty_envelope_refuses_wrong_owner_and_every_truncation_or_corruption() {
    let io = HostIo::default();
    NativeDrainJournal::initialize(io.clone(), owner())
        .ok()
        .unwrap();
    let bytes = io.0.borrow().bytes.clone().unwrap();
    let mut other = owner();
    other.node = NodeId::new(2).unwrap();
    assert!(matches!(
        NativeDrainJournal::recover(io.clone(), other),
        Err((DrainJournalError::WrongOwner, _))
    ));
    for cut in 0..bytes.len() {
        io.0.borrow_mut().bytes = Some(bytes[..cut].to_vec());
        assert!(NativeDrainJournal::recover(io.clone(), owner()).is_err());
    }
    for at in 0..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[at] ^= 1;
        io.0.borrow_mut().bytes = Some(corrupt);
        assert!(NativeDrainJournal::recover(io.clone(), owner()).is_err());
    }
}

#[test]
fn failed_initialization_preserves_old_or_complete_recovery() {
    for fail in 1..=3 {
        let io = HostIo::default();
        io.0.borrow_mut().fail = fail;
        assert!(NativeDrainJournal::initialize(io.clone(), owner()).is_err());
        if fail == 3 {
            assert_eq!(
                NativeDrainJournal::recover(io.clone(), owner())
                    .ok()
                    .unwrap()
                    .latest()
                    .unwrap(),
                None
            );
        } else {
            assert!(matches!(
                NativeDrainJournal::recover(io.clone(), owner()),
                Err((DrainJournalError::MissingRecord, _))
            ));
        }
    }
}
