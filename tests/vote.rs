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
//! Downstream core-only host injection, using only public APIs.
use std::collections::BTreeMap;
use voteboat::{contracts::*, identity::*, quorum::*, vote::*};
fn n(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn binding() -> StoreBinding {
    StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    }
}
fn record(term: u64, vote: Option<NodeId>) -> VoteRecord {
    VoteRecord {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        hard_state: HardState {
            term,
            voted_for: vote,
        },
    }
}
fn voter(state: VoteRecord, last_log: (u64, u64)) -> Voter {
    let policy = Policy::new(
        Tree::Majority((1..=3).map(|id| Tree::Voter(n(id))).collect()),
        Limits::default(),
    )
    .unwrap();
    Voter::recover(n(1), state, policy, binding(), last_log).unwrap()
}
fn request(candidate: u64, term: u64) -> VoteRequest {
    VoteRequest {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        candidate: n(candidate),
        term,
        last_log_term: 0,
        last_log_index: 0,
    }
}
#[test]
fn exact_dependency_is_required_and_only_one_vote_escapes() {
    let mut v = voter(record(0, None), (0, 0));
    assert!(matches!(
        v.request(request(2, 1)),
        Ok(VoteEffect::Persist(_))
    ));
    assert_eq!(v.hard_state().term, 0);
    assert_eq!(v.request(request(3, 1)), Err(VoteError::Busy));
    let ticket = WriteTicket {
        binding: binding(),
        sequence: 1,
    };
    v.admitted(ticket).unwrap();
    for wrong in [
        WriteTicket {
            sequence: 2,
            ..ticket
        },
        WriteTicket {
            binding: StoreBinding {
                session: StoreSession::new(2).unwrap(),
                ..binding()
            },
            ..ticket
        },
    ] {
        assert_eq!(
            v.durable(&DurableVotes {
                tickets: vec![wrong]
            }),
            Err(VoteError::WrongCompletion)
        );
    }
    assert!(
        v.durable(&DurableVotes {
            tickets: vec![ticket]
        })
        .unwrap()
        .granted
    );
    assert_eq!(
        v.durable(&DurableVotes {
            tickets: vec![ticket]
        }),
        Err(VoteError::WrongCompletion)
    );
    assert!(matches!(
        v.request(request(3, 1)),
        Ok(VoteEffect::Reply(VoteResponse { granted: false, .. }))
    ));
    assert!(matches!(
        v.request(request(2, 1)),
        Ok(VoteEffect::Reply(VoteResponse { granted: true, .. }))
    ));
    assert!(matches!(
        v.request(request(3, 2)),
        Ok(VoteEffect::Persist(_))
    ));
    assert_eq!(v.admitted(ticket), Err(VoteError::WrongCompletion));
    let next = WriteTicket {
        sequence: 2,
        ..ticket
    };
    v.admitted(next).unwrap();
    assert_eq!(
        v.durable(&DurableVotes {
            tickets: vec![ticket]
        }),
        Err(VoteError::WrongCompletion)
    );
    assert!(
        v.durable(&DurableVotes {
            tickets: vec![next]
        })
        .unwrap()
        .granted
    );
}
#[test]
fn a_denial_in_a_higher_term_also_waits_for_durability() {
    let mut v = voter(record(4, Some(n(2))), (4, 10));
    assert!(matches!(
        v.request(request(3, 5)),
        Ok(VoteEffect::Persist(VoteRecord {
            hard_state: HardState {
                term: 5,
                voted_for: None
            },
            ..
        }))
    ));
    let ticket = WriteTicket {
        binding: binding(),
        sequence: 1,
    };
    v.admitted(ticket).unwrap();
    let response = v
        .durable(&DurableVotes {
            tickets: vec![ticket],
        })
        .unwrap();
    assert!(!response.granted);
    assert_eq!(response.term, 5);
}
#[test]
fn log_freshness_context_and_failure_fencing() {
    let mut v = voter(record(4, None), (3, 10));
    let mut r = request(2, 4);
    r.last_log_term = 2;
    r.last_log_index = 999;
    assert!(matches!(
        v.request(r),
        Ok(VoteEffect::Reply(VoteResponse { granted: false, .. }))
    ));
    r.last_log_term = 3;
    r.last_log_index = 9;
    assert!(matches!(
        v.request(r),
        Ok(VoteEffect::Reply(VoteResponse { granted: false, .. }))
    ));
    r.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert_eq!(v.request(r), Err(VoteError::WrongContext));
    r.group = group();
    r.candidate = n(9);
    assert_eq!(v.request(r), Err(VoteError::WrongContext));
    r.candidate = n(2);
    r.configuration = ConfigurationId::new(2).unwrap();
    assert_eq!(v.request(r), Err(VoteError::WrongContext));
    v.storage_failed();
    assert_eq!(v.request(request(2, 5)), Err(VoteError::Fenced));
}

struct HostStore {
    records: BTreeMap<GroupIdentity, VoteRecord>,
    writes: usize,
    fail: bool,
}
impl VoteStore for HostStore {
    fn binding(&self) -> StoreBinding {
        binding()
    }
    fn recovered(&self) -> &BTreeMap<GroupIdentity, VoteRecord> {
        &self.records
    }
    fn append_votes(&mut self, records: Vec<VoteRecord>) -> Result<WriteTicket, StorageError> {
        self.writes += 1;
        for r in records {
            self.records.insert(r.group, r);
        }
        Ok(WriteTicket {
            binding: binding(),
            sequence: self.writes as u64,
        })
    }
    fn barrier(&mut self, tickets: &[WriteTicket]) -> Result<DurableVotes, StorageError> {
        if self.fail {
            return Err(StorageError::Uncertain("host sync failed".into()));
        }
        Ok(DurableVotes {
            tickets: tickets.to_vec(),
        })
    }
}
#[test]
fn host_can_supply_its_own_store_without_native_features() {
    let mut v = voter(record(0, None), (0, 0));
    let mut store = HostStore {
        records: BTreeMap::new(),
        writes: 0,
        fail: false,
    };
    assert!(
        drive_vote(&mut v, &mut store, request(2, 1))
            .unwrap()
            .granted
    );
    assert_eq!(store.writes, 1);
    store.fail = true;
    assert!(matches!(
        drive_vote(&mut v, &mut store, request(3, 2)),
        Err(DriveError::Storage(_))
    ));
    assert_eq!(v.request(request(2, 3)), Err(VoteError::Fenced));
}
