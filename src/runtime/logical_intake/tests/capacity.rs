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
fn selected_whole_input_windows_refuse_short_limits_without_installation() {
    shard(ShardLimits {
        max_groups: 1,
        ..ShardLimits::default()
    })
    .validate_control_profile(&profile())
    .unwrap();
    for field in 0..7 {
        let mut l = limits();
        match field {
            0 => l.control_items = 1,
            1 => l.group_control_items = 1,
            2 => l.control_bytes -= 1,
            3 => l.group_control_bytes -= 1,
            4 => {
                l.max_bytes -= 1;
                l.group_bytes = l.max_bytes;
                l.background_bytes -= 1;
            }
            5 => l.group_bytes -= 1,
            _ => l.max_event_bytes -= 1,
        }
        let shard = shard(l);
        assert_eq!(
            shard.validate_control_profile(&profile()),
            Err(RuntimeError::InvalidLimits)
        );
        assert!(shard.control_profile.is_none());
        assert_eq!(shard.usage(), Usage::default());
    }
    assert_eq!(
        selected(limits()).validate_control_profile(&profile()),
        Err(RuntimeError::WrongOwner)
    );
}
#[test]
fn ordinary_count_pressure_keeps_control_class_through_original_queue_and_visit() {
    let mut s = selected(limits());
    for id in 1..=2 {
        s.admit_tracked(
            group(1),
            Event::Read {
                request: ReadRequestId::new(id).unwrap(),
            },
        )
        .unwrap();
    }
    assert_eq!(
        s.admission_shape(
            group(1),
            &Event::Read {
                request: ReadRequestId::new(3).unwrap()
            }
        )
        .unwrap()
        .0,
        Class::Data
    );
    let rejected = s
        .admit_tracked(
            group(1),
            Event::Read {
                request: ReadRequestId::new(3).unwrap(),
            },
        )
        .unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Overloaded);
    let ticket = s.admit_tracked(group(1), release()).unwrap();
    let visit = s.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(
        s.next_class(visit, MonoTime(0)),
        Ok(Some(MessageClass::Control))
    );
    assert!(s
        .next_effect_reservation(visit, MonoTime(0))
        .unwrap()
        .is_some());
    let step = s.step_next(visit, MonoTime(0)).unwrap().unwrap();
    assert_eq!(step.admission, Some(ticket));
    assert_eq!(step.result, Err(RaftError::NotLeader));
    s.finish(visit).unwrap();
    assert_eq!(s.usage().items, 2);
    let stopped = s.stop_group(group(1)).unwrap();
    assert_eq!(stopped.admissions.len(), 2);
    assert_eq!(s.usage(), Usage::default());
}
#[test]
fn original_control_allocation_refuses_unchanged_and_business_cannot_select_control() {
    let s = selected(limits());
    let mut command = release();
    let Event::Propose { bytes, .. } = &mut command else {
        unreachable!()
    };
    bytes.reserve_exact(2048);
    let pointer = bytes.as_ptr();
    let capacity = bytes.capacity();
    assert_eq!(
        s.admission_shape(group(1), &command),
        Err(RuntimeError::EventTooLarge)
    );
    let mut s = s;
    let rejected = s.admit_tracked(group(1), command).unwrap_err();
    let Event::Propose { bytes, .. } = *rejected.event else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(bytes.capacity(), capacity);
    let mut ordinary = release();
    let Event::Propose { bytes, .. } = &mut ordinary else {
        unreachable!()
    };
    bytes[8] = 0;
    assert_eq!(
        s.admission_shape(group(1), &ordinary).unwrap().0,
        Class::Data
    );
    let mut malformed = release();
    let Event::Propose { bytes, .. } = &mut malformed else {
        unreachable!()
    };
    bytes.push(0);
    assert_eq!(
        s.admission_shape(group(1), &malformed).unwrap().0,
        Class::Data
    );
    assert_eq!(
        shard(limits())
            .admission_shape(group(1), &release())
            .unwrap()
            .0,
        Class::Data
    );
}
#[test]
fn ordinary_byte_pressure_cannot_consume_the_selected_input_window() {
    let mut s = selected(limits());
    let bytes = vec![0; window() - size_of::<Event>() - size_of::<AdmissionTicket>()];
    s.admit_tracked(
        group(1),
        Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes,
        },
    )
    .unwrap();
    assert_eq!(s.usage().bytes, window());
    let rejected = s
        .admit_tracked(
            group(1),
            Event::Read {
                request: ReadRequestId::new(2).unwrap(),
            },
        )
        .unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Overloaded);
    s.admit_tracked(group(1), release()).unwrap();
    assert_eq!(s.usage().items, 2);
    assert_eq!(s.stop_group(group(1)).unwrap().admissions.len(), 2);
    assert_eq!(s.usage(), Usage::default());
}
