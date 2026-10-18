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
use super::*;
use crate::membership::{ConfigurationChange, ConfigurationRecord};
fn transition(index: u64, expected: u64, change: ConfigurationChange) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(if index == 1 { 100 } else { 101 }).unwrap(),
            expected: cid(expected),
            change,
        })),
    }
}
fn promote(a: &mut LogicalApplication) {
    let entries = vec![
        transition(
            1,
            1,
            ConfigurationChange::Learners(configuration(2, &[1, 3], &[4])),
        ),
        transition(
            2,
            2,
            ConfigurationChange::Joint {
                id: cid(3),
                next: configuration(4, &[1, 3, 4], &[]),
            },
        ),
        transition(3, 3, ConfigurationChange::Final { id: cid(4) }),
    ];
    let c = mutate(
        a,
        2,
        update(
            a,
            3,
            Some(Suffix {
                from: 1,
                entries: entries.into(),
            }),
        ),
    );
    assert_eq!(apply(a, &c).outcome, LogicalOutcome::Mutated);
}
#[test]
fn actual_grant_uses_retained_base_and_exact_committed_promotion_boundary() {
    let mut a = fixture();
    promote(&mut a);
    let grant = emitted(&a, Event::Receive(incoming(4)));
    assert_eq!(grant.configuration, cid(1));
    assert!(
        matches!(grant.rpc,Rpc::AuthorityReply {configuration,committed_index:3,committed_term:1,granted:true,..} if configuration==cid(4))
    );
    let c = authorized(&mut a, 3, grant.clone());
    let mut old_boundary = grant.clone();
    if let Rpc::AuthorityReply {
        committed_index, ..
    } = &mut old_boundary.rpc
    {
        *committed_index = 2;
    }
    refused(&mut a, 4, old_boundary);
    let mut denied = grant.clone();
    if let Rpc::AuthorityReply {
        committed_index,
        committed_term,
        granted,
        ..
    } = &mut denied.rpc
    {
        *committed_index = 0;
        *committed_term = 0;
        *granted = false;
    }
    refused(&mut a, 5, denied);
    let mut wrong_store = grant;
    if let Rpc::AuthorityReply { candidate, .. } = &mut wrong_store.rpc {
        candidate.store = store(9);
    }
    refused(&mut a, 6, wrong_store);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut restored = fixture();
    restored
        .restore_checkpoint(1, a.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(restored.state, a.state);
    let duplicate = apply(&mut restored, &c);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.first_index, 3);
    assert_eq!(duplicate.outcome, LogicalOutcome::CapsuleAuthorized);
}
