// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use voteboat::{
    authorization::CredentialGeneration,
    credential_reload::*,
    identity::{NodeId, StoreId, StoreIdentity, StoreIncarnation},
    secure::PeerIdentity,
};

pub fn owner() -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(7).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(17).unwrap(),
            incarnation: StoreIncarnation::new(3).unwrap(),
        },
    }
}

pub fn record(sequence: u64, expected: u64, replacement: u64) -> CredentialReloadRecord {
    CredentialReloadRecord {
        owner: owner(),
        request: CredentialReloadRequest {
            sequence,
            expected: CredentialGeneration::new(expected).unwrap(),
            replacement: CredentialGeneration::new(replacement).unwrap(),
        },
        digest: [sequence as u8; 32],
    }
}

fn refused(
    journal: &mut impl CredentialJournal,
    value: CredentialReloadRecord,
    error: CredentialJournalError,
    prior: CredentialReloadRecord,
) {
    assert_eq!(journal.publish(value), Err(error));
    assert_eq!(journal.latest(), Ok(Some(prior)));
    assert_eq!(journal.owner(), owner());
}

pub fn exercise(journal: &mut impl CredentialJournal) -> CredentialReloadRecord {
    assert_eq!(journal.owner(), owner());
    assert_eq!(journal.latest(), Ok(None));
    let first = record(1, 3, 4);
    journal.publish(first).unwrap();
    journal.publish(first).unwrap();
    assert_eq!(journal.latest(), Ok(Some(first)));
    for wrong_owner in [
        PeerIdentity {
            node: NodeId::new(8).unwrap(),
            ..owner()
        },
        PeerIdentity {
            store: StoreIdentity {
                id: StoreId::new(18).unwrap(),
                ..owner().store
            },
            ..owner()
        },
        PeerIdentity {
            store: StoreIdentity {
                incarnation: StoreIncarnation::new(4).unwrap(),
                ..owner().store
            },
            ..owner()
        },
    ] {
        refused(
            journal,
            CredentialReloadRecord {
                owner: wrong_owner,
                ..record(2, 4, 5)
            },
            CredentialJournalError::WrongOwner,
            first,
        );
    }
    for invalid in [record(0, 4, 5), record(2, 4, 4), record(2, 5, 4)] {
        refused(
            journal,
            invalid,
            CredentialJournalError::InvalidRecord,
            first,
        );
    }
    let mut changed = first;
    changed.digest[0] ^= 1;
    for changed in [changed, record(1, 2, 4), record(1, 3, 5)] {
        refused(
            journal,
            changed,
            CredentialJournalError::SequenceConflict,
            first,
        );
    }
    refused(
        journal,
        record(2, 3, 5),
        CredentialJournalError::WrongGeneration,
        first,
    );
    // A host may have deliberately advanced its trusted startup generation.
    // The journal enforces the recorded floor, not a fictitious contiguous log.
    let second = record(7, 9, 10);
    journal.publish(second).unwrap();
    refused(
        journal,
        first,
        CredentialJournalError::SequenceConflict,
        second,
    );
    journal.publish(second).unwrap();
    let last = record(8, 10, 11);
    journal.publish(last).unwrap();
    assert_eq!(journal.latest(), Ok(Some(last)));
    last
}

struct HostJournal {
    value: Option<CredentialReloadRecord>,
}

impl CredentialJournal for HostJournal {
    fn owner(&self) -> PeerIdentity {
        owner()
    }
    fn latest(&self) -> Result<Option<CredentialReloadRecord>, CredentialJournalError> {
        Ok(self.value)
    }
    fn publish(&mut self, record: CredentialReloadRecord) -> Result<(), CredentialJournalError> {
        use CredentialJournalError::{
            InvalidRecord, SequenceConflict, WrongGeneration, WrongOwner,
        };
        if record.request.sequence == 0 || record.request.expected >= record.request.replacement {
            return Err(InvalidRecord);
        }
        if record.owner != owner() {
            return Err(WrongOwner);
        }
        match self.value {
            Some(prior) if prior == record => return Ok(()),
            Some(prior) if prior.request.sequence >= record.request.sequence => {
                return Err(SequenceConflict)
            }
            Some(prior) if prior.request.replacement > record.request.expected => {
                return Err(WrongGeneration)
            }
            _ => self.value = Some(record),
        }
        Ok(())
    }
}

#[test]
fn host_credential_journal_satisfies_shared_publication_contract_without_native() {
    let mut independent = HostJournal { value: None };
    let last = {
        let mut journal = HostJournal { value: None };
        let last = exercise(&mut journal);
        assert_eq!(exercise(&mut independent), last);
        last
    };
    assert_eq!(independent.latest(), Ok(Some(last)));
}
