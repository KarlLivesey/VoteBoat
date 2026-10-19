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
//! Whole original read-result commands at an exhausted ordinary action quota.
use super::*;
fn query(q: ActivationQuery<()>) -> Vec<u8> {
    match q {
        ActivationQuery::Status(id) => {
            let mut bytes = vec![1];
            bytes.extend(id.get().to_le_bytes());
            bytes
        }
        ActivationQuery::Active => vec![2],
        ActivationQuery::Business(()) => vec![0],
    }
}
fn full_ordinary() -> LogicalApplication {
    let mut a = selected();
    claim(&mut a);
    let b = wrapper(4096).business(&5i64.to_le_bytes()).unwrap();
    let first = append(&a, 2, vec![parent_entry(1, 200, b)]);
    apply(&mut a, &first);
    a
}
fn read_command(a: &LogicalApplication, bytes: Vec<u8>) -> Command {
    let mut c = control_capsule(a);
    let CommandBody::Authorize(capsule) = &mut c.body else {
        panic!("capsule")
    };
    let LogicalCapsuleOrigin::Client { effect, .. } = capsule.origin else {
        panic!("client")
    };
    let request = ReadRequestId::new(1).unwrap();
    capsule.origin = LogicalCapsuleOrigin::Read {
        effect,
        ticket: LogicalReadTicket {
            owner: effect.visit.owner,
            generation: ReadInvocationGeneration::new(1).unwrap(),
            sequence: 1,
            group: group(1),
            request,
        },
        barrier: LogicalReadContext {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            term: 9,
            request,
            index: 0,
        },
    };
    capsule.body = LogicalCapsuleBody::ReadResult {
        schema: LOGICAL_ACTIVATION_RESPONSE_SCHEMA,
        query: bytes,
        result: vec![0, 1, 0],
    };
    c
}
#[test]
fn status_and_active_results_use_selected_reserve_without_creating_authority() {
    for q in [
        ActivationQuery::Status(OperationId::new(100).unwrap()),
        ActivationQuery::Active,
    ] {
        let mut a = full_ordinary();
        let c = read_command(&a, query(q));
        assert!(c.control(a.activation_control_profile()));
        assert!(!c.control(None));
        assert!(admitted(&a, &c).is_ok());
        // The deliberately wrong original barrier term remains invalid despite
        // complete control-query classification and available reserved credits.
        assert_eq!(
            apply(&mut a, &c).outcome,
            LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
        );
        assert_eq!(
            a.state.records[&c.operation].command,
            a.encode_command(&c, usize::MAX).unwrap()
        );
        let checkpoint = a.checkpoint(usize::MAX).unwrap();
        let mut restored = selected();
        restored
            .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &checkpoint)
            .unwrap();
        assert_eq!(restored.state, a.state);
        assert!(admitted(&restored, &c).is_ok());
        assert!(apply(&mut restored, &c).duplicate);
    }
}
#[test]
fn foreign_read_bindings_and_schema_do_not_borrow_control_credits() {
    let a = full_ordinary();
    let original = read_command(&a, query(ActivationQuery::Active));
    for field in 0..4 {
        let mut c = original.clone();
        let CommandBody::Authorize(capsule) = &mut c.body else {
            panic!("capsule")
        };
        let LogicalCapsuleOrigin::Read {
            effect,
            ticket,
            barrier,
        } = &mut capsule.origin
        else {
            panic!("read")
        };
        match field {
            0 => ticket.group = group(9),
            1 => barrier.group = group(9),
            2 => {
                let LogicalCapsuleBody::ReadResult { schema, .. } = &mut capsule.body else {
                    panic!("read result")
                };
                *schema = LOGICAL_RESPONSE_SCHEMA;
            }
            _ => capsule.origin = LogicalCapsuleOrigin::Protocol(*effect),
        }
        assert!(!c.control(a.activation_control_profile()));
        assert_eq!(admitted(&a, &c), Err(ApplicationError::DedupCapacity));
    }
}
#[test]
fn incomplete_trailing_zero_and_business_queries_remain_ordinary_at_quota() {
    let a = full_ordinary();
    let status = query(ActivationQuery::Status(OperationId::new(100).unwrap()));
    let mut trailing = status.clone();
    trailing.push(0);
    let mut zero = vec![1];
    zero.extend([0; 16]);
    let mut wrapped = vec![0];
    wrapped.extend(&status);
    let malformed = [vec![], vec![3], vec![2, 0], trailing, zero, wrapped];
    for bytes in malformed
        .into_iter()
        .chain((0..status.len()).map(|n| status[..n].to_vec()))
    {
        let c = read_command(&a, bytes);
        assert!(!c.control(a.activation_control_profile()));
        assert_eq!(admitted(&a, &c), Err(ApplicationError::DedupCapacity));
    }
}
