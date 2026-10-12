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
fn encoded(
    r: &LogicalReleaseCoordinator<CounterReceipt, i64>,
    group: GroupIdentity,
    schema: u64,
    bytes: &[u8],
) -> bool {
    r.encoded_control_query(group, schema, bytes.len(), |out| {
        out.copy_from_slice(bytes);
        Ok(bytes.len())
    })
}
#[test]
fn only_complete_canonical_original_query_bytes_select_control_capacity() {
    let r = selected();
    let schema = wrapper().control_profile().response_schema();
    let mut status = vec![1];
    status.extend_from_slice(&7u128.to_le_bytes());
    assert!(encoded(&r, group(1), schema, &status));
    assert!(encoded(&r, group(1), schema, &[2]));
    for bytes in [vec![], vec![0, 2], vec![1; 16], vec![2, 0], {
        let mut b = vec![1];
        b.extend_from_slice(&0u128.to_le_bytes());
        b
    }] {
        assert!(!encoded(&r, group(1), schema, &bytes));
    }
    assert!(!encoded(&r, group(2), schema, &status));
    assert!(!encoded(&r, group(1), schema + 1, &status));
    assert!(!encoded(&accounting(), group(1), schema, &status));
}
#[test]
fn invalid_declared_bound_or_encoder_result_cannot_classify_as_control() {
    let r = selected();
    let schema = wrapper().control_profile().response_schema();
    assert!(!r.encoded_control_query(group(1), schema, 18, |_| panic!(
        "oversized query encoder invoked"
    )));
    assert!(!r.encoded_control_query(group(1), schema, 1, |out| {
        out[0] = 2;
        Ok(2)
    }));
    assert!(!r.encoded_control_query(group(1), schema, 1, |_| Err(
        ApplicationError::InvalidCommand
    )));
}
