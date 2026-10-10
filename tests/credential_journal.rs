// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
#![cfg(feature = "native")]
use std::{cell::RefCell, io::ErrorKind, rc::Rc};
use voteboat::{
    authorization::CredentialGeneration, credential_reload::*, identity::*,
    native::credential_journal::*, secure::PeerIdentity,
};
fn owner() -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(1).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    }
}
fn record(sequence: u64) -> CredentialReloadRecord {
    CredentialReloadRecord {
        owner: owner(),
        request: CredentialReloadRequest {
            sequence,
            expected: CredentialGeneration::new(sequence).unwrap(),
            replacement: CredentialGeneration::new(sequence + 1).unwrap(),
        },
        digest: [sequence as u8; 32],
    }
}
#[derive(Default)]
struct State {
    bytes: Option<[u8; 128]>,
    fail: u8,
    writes: usize,
}
#[derive(Clone, Default)]
struct HostIo(Rc<RefCell<State>>);
impl CredentialRecordIo for HostIo {
    fn read(&mut self) -> Result<Option<[u8; 128]>, CredentialJournalError> {
        Ok(self.0.borrow().bytes)
    }
    fn replace(&mut self, b: &[u8; 128]) -> Result<(), CredentialJournalError> {
        let mut state = self.0.borrow_mut();
        state.writes += 1;
        if state.fail != 1 {
            state.bytes = Some(*b);
        }
        if state.fail != 0 {
            Err(CredentialJournalError::Io {
                kind: ErrorKind::Other,
                uncertain: true,
            })
        } else {
            Ok(())
        }
    }
}
#[test]
fn latest_retry_is_idempotent_and_stale_or_cross_owner_records_are_refused() {
    let io = HostIo::default();
    let mut journal = NativeCredentialJournal::new(io.clone(), owner())
        .ok()
        .unwrap();
    journal.publish(record(1)).unwrap();
    journal.publish(record(1)).unwrap();
    assert_eq!(io.0.borrow().writes, 1);
    let mut wrong = record(1);
    wrong.digest[0] ^= 1;
    assert_eq!(
        journal.publish(wrong),
        Err(CredentialJournalError::SequenceConflict)
    );
    wrong = record(2);
    wrong.owner.node = NodeId::new(2).unwrap();
    assert_eq!(
        journal.publish(wrong),
        Err(CredentialJournalError::WrongOwner)
    );
    wrong = record(2);
    wrong.request.expected = CredentialGeneration::new(1).unwrap();
    assert_eq!(
        journal.publish(wrong),
        Err(CredentialJournalError::WrongGeneration)
    );
    journal.publish(record(2)).unwrap();
    assert_eq!(journal.latest().unwrap(), Some(record(2)));
}
#[test]
fn uncertain_completion_fences_until_reopen_with_old_or_new_record() {
    for cut in [1, 2] {
        let io = HostIo::default();
        let mut journal = NativeCredentialJournal::new(io.clone(), owner())
            .ok()
            .unwrap();
        journal.publish(record(1)).unwrap();
        io.0.borrow_mut().fail = cut;
        assert!(journal.publish(record(2)).is_err());
        assert_eq!(journal.latest(), Err(CredentialJournalError::Fenced));
        assert_eq!(
            journal.publish(record(3)),
            Err(CredentialJournalError::Fenced)
        );
        io.0.borrow_mut().fail = 0;
        let reopened = NativeCredentialJournal::new(io, owner()).ok().unwrap();
        assert_eq!(
            reopened.latest().unwrap(),
            Some(record(if cut == 1 { 1 } else { 2 }))
        );
    }
}
#[test]
fn every_byte_corruption_of_committed_record_fails_closed() {
    let io = HostIo::default();
    let mut journal = NativeCredentialJournal::new(io.clone(), owner())
        .ok()
        .unwrap();
    journal.publish(record(1)).unwrap();
    let original = io.0.borrow().bytes.unwrap();
    for offset in 0..128 {
        let mut changed = original;
        changed[offset] ^= 1;
        io.0.borrow_mut().bytes = Some(changed);
        assert!(
            NativeCredentialJournal::new(io.clone(), owner()).is_err(),
            "offset {offset}"
        );
    }
}
#[test]
fn native_file_recovery_ignores_partial_staging_and_validates_published_record() {
    let dir = std::env::temp_dir().join(format!(
        "voteboat-credential-journal-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("CREDENTIAL-RELOAD");
    let mut journal = NativeCredentialJournal::new(FileCredentialRecord::new(&path), owner())
        .ok()
        .unwrap();
    journal.publish(record(1)).unwrap();
    let io = HostIo::default();
    let mut staged = NativeCredentialJournal::new(io.clone(), owner())
        .ok()
        .unwrap();
    staged.publish(record(2)).unwrap();
    let bytes = io.0.borrow().bytes.unwrap();
    for cut in 0..128 {
        std::fs::write(path.with_extension("staging"), &bytes[..cut]).unwrap();
        let opened = NativeCredentialJournal::new(FileCredentialRecord::new(&path), owner())
            .ok()
            .unwrap();
        assert_eq!(opened.latest().unwrap(), Some(record(1)));
    }
    journal.publish(record(2)).unwrap();
    let opened = NativeCredentialJournal::new(FileCredentialRecord::new(&path), owner())
        .ok()
        .unwrap();
    assert_eq!(opened.latest().unwrap(), Some(record(2)));
    std::fs::write(&path, &bytes[..127]).unwrap();
    assert!(NativeCredentialJournal::new(FileCredentialRecord::new(&path), owner()).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
