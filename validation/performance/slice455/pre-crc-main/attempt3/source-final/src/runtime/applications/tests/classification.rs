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
fn entry(index: u64, operation: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: bytes.into(),
        },
    }
}
fn release() -> Vec<u8> {
    // Complete v1 Release grammar. This is capacity classification, never a
    // consumed activation observation or permission to release a parent effect.
    let mut bytes = b"VBACTA01\x03".to_vec();
    bytes.extend_from_slice(&10u128.to_le_bytes());
    bytes.extend_from_slice(&4u64.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    bytes
}
#[test]
fn whole_mixed_batch_requires_complete_selected_envelopes() {
    let a = wrapper();
    let p = a.control_profile();
    let business = entry(1, 20, a.business(&5i64.to_le_bytes()).unwrap());
    let control = entry(2, 14, release());
    let original = vec![business.clone(), control.clone()];
    assert!(p.is_control_batch(group(1), &original));
    assert_eq!(original, vec![business.clone(), control.clone()]);
    assert!(!p.is_control_batch(group(2), &original));
    assert!(!p.is_control_batch(group(1), &[business]));
    assert!(!p.is_control_batch(group(1), &[entry(2, 10, release())]));
    assert!(p.is_control_batch(
        group(1),
        &[
            LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop
            },
            control
        ]
    ));
}
#[test]
fn any_malformed_entry_or_business_wrapped_control_keeps_the_whole_batch_ordinary() {
    let a = wrapper();
    let p = a.control_profile();
    let mut trailing = release();
    trailing.push(0);
    let invalid = [Vec::new(), vec![0], release()[..40].to_vec(), trailing];
    for bytes in invalid {
        assert!(!p.is_control_batch(group(1), &[entry(1, 14, release()), entry(2, 20, bytes)]));
    }
    assert!(!p.is_control_batch(group(1), &[entry(1, 14, a.business(&release()).unwrap())]));
}
