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
#[test]
fn pending_whole_records_and_all_destination_phase_ids_reserve_terminal_capacity() {
    let f = Fixture::new(1);
    let s = spec(1);
    let bytes = TransferApplication::start(s).unwrap();
    let newer = spec(2);
    let new_bytes = TransferApplication::start(newer).unwrap();
    assert_eq!(
        f.c.validate_proposal(
            newer.phases().coordinator()[0],
            &new_bytes,
            [(s.phases().coordinator()[0], bytes.as_slice())].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    assert!(f
        .c
        .validate_proposal(
            s.phases().coordinator()[0],
            &bytes,
            [(s.phases().coordinator()[0], bytes.as_slice())].into_iter()
        )
        .is_ok());
    let f = Fixture::new(2);
    for p in 0..2 {
        let mut aliases = newer.phases().participants();
        aliases[p][2] = s.phases().participants()[p][2];
        let aliased = TransferSpec::new(
            newer.id(),
            newer.profile(),
            newer.debit(),
            newer.credit(),
            newer.amount(),
            PhaseIds::new(newer.phases().coordinator(), aliases).unwrap(),
        )
        .unwrap();
        let request = TransferApplication::start(aliased).unwrap();
        assert_eq!(
            f.c.validate_proposal(
                aliased.phases().coordinator()[0],
                &request,
                [(s.phases().coordinator()[0], bytes.as_slice())].into_iter()
            ),
            Err(ApplicationError::OperationConflict)
        );
    }
}
#[test]
fn missing_account_and_incorrect_outer_operation_refuse_before_binding() {
    let mut f = Fixture::new(1);
    let s = spec(1);
    let mut wrong = command(s, TransferStep::Prepare, None, [None; 2]);
    let absent = TransferSpec::new(
        s.id(),
        s.profile(),
        AccountRef::new(gid(20), aid(9)),
        s.credit(),
        s.amount(),
        s.phases(),
    )
    .unwrap();
    wrong.spec = absent;
    assert_eq!(
        f.p[0].validate_proposal(
            s.phases().participants()[0][0],
            &codec::encode(wrong).unwrap(),
            std::iter::empty()
        ),
        Err(ApplicationError::InvalidCommand)
    );
    let before = f.c.clone();
    let entry = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: s.phases().coordinator()[1],
            bytes: TransferApplication::start(s).unwrap().into(),
        },
    };
    assert_eq!(
        f.c.apply_batch(&[entry]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(f.c, before);
}
#[test]
fn new_protocol_phase_requires_original_binding_but_pending_start_reserves_it() {
    let f = Fixture::new(1);
    let s = spec(1);
    let facts = std::array::from_fn(|p| {
        Some(PhaseFact {
            group: s.profile().participants()[p],
            operation: s.phases().participants()[p][0],
            index: 1,
        })
    });
    let decide = codec::encode(command(
        s,
        TransferStep::Decide,
        Some(TransferDecision::Commit),
        facts,
    ))
    .unwrap();
    assert_eq!(
        f.c.validate_proposal(s.phases().coordinator()[1], &decide, std::iter::empty()),
        Err(ApplicationError::InvalidCommand)
    );
    let start = TransferApplication::start(s).unwrap();
    assert!(f
        .c
        .validate_proposal(
            s.phases().coordinator()[1],
            &decide,
            [(s.phases().coordinator()[0], start.as_slice())].into_iter()
        )
        .is_ok());
}
