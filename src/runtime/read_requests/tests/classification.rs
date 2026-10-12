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
fn typed_wrapper_status_and_active_are_control_but_business_is_ordinary() {
    let r = selected();
    let a = app(group(1));
    assert_eq!(
        r.query_control(&a, group(1), &Query::Active)
            .map(|q| q.is_some()),
        Ok(true)
    );
    assert_eq!(
        r.query_control(&a, group(1), &Query::Status(OperationId::new(1).unwrap()))
            .map(|q| q.is_some()),
        Ok(true)
    );
    for bytes in [vec![2], vec![1; 16], vec![0; 32]] {
        assert_eq!(
            r.query_control(&a, group(1), &Query::Business(bytes))
                .map(|q| q.is_some()),
            Ok(false)
        );
    }
    assert_eq!(
        r.query_control(&app(group(2)), group(1), &Query::Active)
            .map(|q| q.is_some()),
        Err(ReadInvocationError::WrongBinding)
    );
    assert_eq!(
        r.query_control(&a, group(2), &Query::Active)
            .map(|q| q.is_some()),
        Err(ReadInvocationError::WrongBinding)
    );
}
#[test]
fn complete_encoding_rejects_trailing_zero_ids_overbound_and_faulty_encoder() {
    let r = selected();
    for bytes in [
        vec![],
        vec![0, 2],
        vec![2, 0],
        vec![1; 16],
        vec![1; 18],
        vec![0; 17],
    ] {
        assert!(r
            .encoded_control_query(bytes.len(), |out| {
                out.copy_from_slice(&bytes);
                Ok(bytes.len())
            })
            .is_none());
    }
    let mut zero = vec![1];
    zero.extend_from_slice(&[0; 16]);
    assert!(r
        .encoded_control_query(17, |out| {
            out.copy_from_slice(&zero);
            Ok(17)
        })
        .is_none());
    assert!(r
        .encoded_control_query(18, |_| panic!("must not call overbound encoder"))
        .is_none());
    assert!(r
        .encoded_control_query(1, |out| {
            out[0] = 2;
            Ok(2)
        })
        .is_none());
    assert!(r
        .encoded_control_query(1, |_| Err(ApplicationError::ReceiptBudget))
        .is_none());
    assert!(r
        .encoded_control_query(1, |out| {
            out[0] = 2;
            Ok(1)
        })
        .is_some());
}
#[test]
fn one_original_encoding_retains_complete_bytes_without_calling_encoder_again() {
    let r = selected();
    let calls = std::cell::Cell::new(0);
    let mut expected = vec![1];
    expected.extend_from_slice(&42_u128.to_le_bytes());
    let candidate = r
        .encoded_control_query(17, |out| {
            assert!(out.iter().all(|b| *b == 0));
            calls.set(calls.get() + 1);
            out.copy_from_slice(&expected);
            Ok(17)
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(candidate.bytes(), expected);
    assert_eq!(candidate.bytes(), expected);
    assert_eq!(calls.get(), 1);
    assert!(r
        .control_profile
        .as_ref()
        .unwrap()
        .is_control_query(candidate.bytes()));
}
