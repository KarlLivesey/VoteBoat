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
fn only_full_original_control_envelopes_use_selected_capacity() {
    let r = selected();
    assert_eq!(r.request_control(&release()), Ok(true));
    let mut foreign = release();
    foreign.group = group(2);
    assert_eq!(r.request_control(&foreign), Ok(false));
    let mut alias = release();
    alias.operation = OperationId::new(10).unwrap();
    assert_eq!(r.request_control(&alias), Ok(false));
    let mut trailing = release();
    trailing.bytes.push(0);
    assert_eq!(r.request_control(&trailing), Ok(false));
    let mut zero = release();
    zero.bytes[9..25].fill(0);
    assert_eq!(r.request_control(&zero), Ok(false));
    let mut wrapped = release();
    let mut bytes = b"VBACTA01\x00".to_vec();
    bytes.extend_from_slice(&wrapped.bytes);
    wrapped.bytes = bytes;
    assert_eq!(r.request_control(&wrapped), Ok(false));
    let mut empty = release();
    empty.bytes.clear();
    assert_eq!(r.request_control(&empty), Ok(false));
}
#[test]
fn oversized_original_control_allocation_refuses_without_touching_original() {
    let r = selected();
    let mut original = release();
    original.bytes.reserve_exact(r.limits.command_bytes + 1);
    let pointer = original.bytes.as_ptr();
    let capacity = original.bytes.capacity();
    let content = original.bytes.clone();
    assert_eq!(
        r.request_control(&original),
        Err(ClientError::RequestTooLarge)
    );
    assert_eq!(original.bytes.as_ptr(), pointer);
    assert_eq!(original.bytes.capacity(), capacity);
    assert_eq!(original.bytes, content);
    assert_eq!(r.usage(), ClientUsage::default());
    let mut exact = release();
    let mut bytes = Vec::with_capacity(r.limits.command_bytes);
    bytes.extend_from_slice(&exact.bytes);
    exact.bytes = bytes;
    assert_eq!(exact.bytes.capacity(), r.limits.command_bytes);
    assert_eq!(r.request_control(&exact), Ok(true));
}
