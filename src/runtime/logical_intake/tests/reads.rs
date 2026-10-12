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
//! Queue accounting controls; selected metadata grants no read authority.
use super::*;
fn event(id: u64) -> Event {
    Event::Read {
        request: ReadRequestId::new(id).unwrap(),
    }
}
fn scope<'a>(
    id: u64,
    profile: &'a LogicalActivationControlProfile,
    query: &'a [u8],
) -> LogicalReadInput<'a> {
    LogicalReadInput {
        request: ReadRequestId::new(id).unwrap(),
        profile,
        query,
    }
}
#[test]
fn exact_scoped_read_class_requires_original_profile_group_event_and_request() {
    let s = selected(limits());
    let p = profile();
    let mut status = vec![1];
    status.extend_from_slice(&10_u128.to_le_bytes());
    for query in [&[2][..], status.as_slice()] {
        assert_eq!(
            s.admission_shape_scoped(group(1), &event(1), Some(scope(1, &p, query)))
                .unwrap()
                .0,
            Class::Control
        );
        assert_eq!(
            s.admission_shape(group(1), &event(1)).unwrap().0,
            Class::Data
        );
    }
    for query in [&[0][..], &[2, 0], &[1; 1], &[0; 17]] {
        assert_eq!(
            s.admission_shape_scoped(group(1), &event(1), Some(scope(1, &p, query)))
                .unwrap()
                .0,
            Class::Data
        );
    }
    for original in [event(2), release()] {
        assert_eq!(
            s.admission_shape_scoped(group(1), &original, Some(scope(1, &p, &[2]))),
            Err(RuntimeError::WrongOwner)
        );
    }
    assert_eq!(
        shard(limits()).admission_shape_scoped(group(1), &event(1), Some(scope(1, &p, &[2]))),
        Err(RuntimeError::WrongOwner)
    );
    assert_eq!(
        s.input_class(group(2), &event(1), Class::Data, Some(scope(1, &p, &[2]))),
        Err(RuntimeError::WrongOwner)
    );
    assert_eq!(s.usage(), Usage::default());
    let other = profile_with_command_bytes(2048);
    assert_eq!(
        s.admission_shape_scoped(group(1), &event(1), Some(scope(1, &other, &[2]))),
        Err(RuntimeError::WrongOwner)
    );
}
#[test]
fn scoped_read_uses_reserved_queue_and_preserves_full_total_ceiling_and_cleanup() {
    let mut s = selected(limits());
    let p = profile();
    for id in 1..=2 {
        s.admit_tracked(group(1), event(id)).unwrap();
    }
    assert_eq!(
        s.admit_tracked(group(1), event(3)).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    let first = s
        .admit_record(group(1), event(3), None, true, Some(scope(3, &p, &[2])))
        .unwrap()
        .unwrap();
    s.admit_record(group(1), event(4), None, true, Some(scope(4, &p, &[2])))
        .unwrap();
    let sequence = s.admission_sequence;
    let rejected = s
        .admit_record(group(1), event(5), None, true, Some(scope(5, &p, &[2])))
        .unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Overloaded);
    assert!(matches!(*rejected.event, Event::Read { request } if request.get() == 5));
    assert_eq!(s.admission_sequence, sequence);
    assert_eq!(s.usage().items, 4);
    let visit = s.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(
        s.next_class(visit, MonoTime(0)),
        Ok(Some(MessageClass::Control))
    );
    let stepped = s.step_next(visit, MonoTime(0)).unwrap().unwrap();
    assert_eq!(stepped.admission, Some(first));
    assert_eq!(stepped.result, Err(RaftError::NotLeader));
    s.finish(visit).unwrap();
    assert_eq!(s.stop_group(group(1)).unwrap().admissions.len(), 3);
    assert_eq!(s.usage(), Usage::default());
}
#[test]
fn ordinary_byte_ceiling_preserves_scoped_read_and_rejection_sequence() {
    let mut s = selected(limits());
    let p = profile();
    s.admit_tracked(
        group(1),
        Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: vec![0; 1024],
        },
    )
    .unwrap();
    let usage = s.usage();
    let sequence = s.admission_sequence;
    assert_eq!(
        s.admit_record(group(1), event(2), None, true, Some(scope(3, &p, &[2])))
            .unwrap_err()
            .reason,
        RuntimeError::WrongOwner
    );
    assert_eq!(s.admission_sequence, sequence);
    assert_eq!(s.usage(), usage);
    assert_eq!(
        s.admit_tracked(group(1), event(2)).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    s.admit_record(group(1), event(2), None, true, Some(scope(2, &p, &[2])))
        .unwrap();
    assert_eq!(s.stop_group(group(1)).unwrap().admissions.len(), 2);
    assert_eq!(s.usage(), Usage::default());
}
#[cfg(feature = "native")]
#[test]
fn selected_metadata_cannot_grant_physical_runtime_reserved_read_authority() {
    use crate::native::runtime::{DeadlineQueue, JitterEntropy};
    let mut runtime = TimedShard::new(
        selected(limits()),
        DeadlineQueue::new(owner(), 1).unwrap(),
        JitterEntropy::new(1),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let p = profile();
    let rejected = runtime
        .admit_logical_read(group(1), event(1), scope(1, &p, &[2]))
        .unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::WrongOwner);
    assert!(matches!(*rejected.event, Event::Read { request } if request.get() == 1));
    assert_eq!(runtime.usage(), Usage::default());
    assert!(!runtime.authorization().is_logical());
    assert_eq!(runtime.core(group(1)).unwrap().read_request_floor(), 0);
    runtime.admit_tracked(group(1), event(1)).unwrap();
    assert_eq!(runtime.stop_group(group(1)).unwrap().admissions.len(), 1);
    assert!(runtime.is_drained());
}
