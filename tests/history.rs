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
#[path = "support/history.rs"]
mod history;
use history::*;
fn write(operation: u128, delta: i64) -> Action {
    Action::Write { operation, delta }
}
fn call(time: u64, action: Action, reply: Reply) -> Call {
    Call {
        invoked: time,
        completed: time + 1,
        action,
        reply,
    }
}
fn valid(calls: &[Call]) -> Vec<usize> {
    check(calls, Limits::default()).unwrap()
}
fn invalid(calls: &[Call]) {
    assert_eq!(
        check(calls, Limits::default()),
        Err(CheckError::NotLinearizable)
    );
}
#[test]
fn observes_real_time_but_explores_overlapping_completions() {
    let overlap = [
        Call {
            invoked: 1,
            completed: 4,
            action: write(1, 5),
            reply: Reply::Value(12),
        },
        Call {
            invoked: 2,
            completed: 3,
            action: write(2, 7),
            reply: Reply::Value(7),
        },
        call(5, Action::Read, Reply::Value(12)),
    ];
    assert_eq!(valid(&overlap), [1, 0, 2]);
    invalid(&[
        call(1, write(1, 5), Reply::Value(5)),
        call(3, Action::Read, Reply::Value(0)),
    ]);
    invalid(&[
        call(1, write(1, 5), Reply::Value(12)),
        call(3, write(2, 7), Reply::Value(7)),
    ]);
}
#[test]
fn retries_are_idempotent_and_conflicting_payloads_cannot_mutate() {
    valid(&[
        call(1, write(1, 5), Reply::Value(5)),
        call(3, write(2, 7), Reply::Value(12)),
        call(5, write(1, 5), Reply::Value(5)),
        call(7, write(1, 99), Reply::Conflict),
        call(9, Action::Read, Reply::Value(12)),
    ]);
    invalid(&[
        call(1, write(1, 5), Reply::Value(5)),
        call(3, write(1, 5), Reply::Value(10)),
    ]);
    invalid(&[
        call(1, write(1, 5), Reply::Value(5)),
        call(3, write(1, 7), Reply::Value(12)),
    ]);
}
#[test]
fn unknown_can_be_omitted_applied_or_commit_after_uncertainty_was_reported() {
    assert_eq!(
        valid(&[
            call(1, write(1, 5), Reply::Unknown),
            call(3, Action::Read, Reply::Value(0))
        ]),
        [1]
    );
    assert_eq!(
        valid(&[
            call(1, write(1, 5), Reply::Unknown),
            call(3, Action::Read, Reply::Value(5))
        ]),
        [0, 1]
    );
    assert_eq!(
        valid(&[
            call(1, write(1, 5), Reply::Unknown),
            call(3, Action::Read, Reply::Value(0)),
            call(5, Action::Read, Reply::Value(5)),
        ]),
        [1, 0, 2]
    );
    invalid(&[
        call(1, Action::Read, Reply::Value(5)),
        call(3, write(1, 5), Reply::Unknown),
    ]);
    invalid(&[
        call(1, write(1, 5), Reply::Unknown),
        call(3, Action::Read, Reply::Value(10)),
    ]);
}
#[test]
fn overflow_and_non_admission_have_no_application_effect() {
    valid(&[
        call(1, write(1, i64::MAX), Reply::Value(i64::MAX)),
        call(3, write(2, 1), Reply::Overflow),
        call(5, write(3, -8), Reply::NotAdmitted),
        call(7, Action::Read, Reply::Value(i64::MAX)),
        call(9, write(2, 1), Reply::Overflow),
    ]);
    invalid(&[
        call(1, write(1, 5), Reply::NotAdmitted),
        call(3, Action::Read, Reply::Value(5)),
    ]);
}
#[test]
fn malformed_or_exhausted_search_is_never_reported_linearizable() {
    let mut broken = call(1, Action::Read, Reply::Value(0));
    broken.completed = 1;
    assert_eq!(
        check(&[broken], Limits::default()),
        Err(CheckError::InvalidInput)
    );
    let calls = [
        call(1, write(1, 5), Reply::Value(5)),
        call(3, Action::Read, Reply::Value(5)),
    ];
    assert_eq!(
        check(
            &calls,
            Limits {
                calls: 1,
                states: 100
            }
        ),
        Err(CheckError::InvalidInput)
    );
    assert_eq!(
        check(
            &calls,
            Limits {
                calls: 2,
                states: 1
            }
        ),
        Err(CheckError::SearchExhausted)
    );
    assert_eq!(
        check(&[calls[0].clone(), calls[0].clone()], Limits::default()),
        Err(CheckError::InvalidInput)
    );
    assert_eq!(valid(&[]), []);
}
