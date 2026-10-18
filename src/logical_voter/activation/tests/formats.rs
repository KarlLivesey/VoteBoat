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
//! Format pins. The v1 files were written by the code at 30a1f6c5 with the same
//! fixture (two intents, `Counter::new(16)`; populated = business `op(100)` at
//! index 1 and a v1 `Begin` at index 2). Old bytes are refused typed, never
//! reinterpreted; v2 bytes are pinned exactly.
use super::*;
const OLD_SCHEMA: u64 = 0x5642_4143_5449_0001;
const V1_EMPTY: &[u8] = include_bytes!("formats/v1-empty.bin");
const V1_POPULATED: &[u8] = include_bytes!("formats/v1-populated.bin");
const V1_BEGIN: &[u8] = include_bytes!("formats/v1-begin.bin");
const V1_PROFILE: &[u8] = include_bytes!("formats/v1-profile.bin");
const V2_EMPTY: &[u8] = include_bytes!("formats/v2-empty.bin");
const V2_POPULATED: &[u8] = include_bytes!("formats/v2-populated.bin");
const V2_BEGIN: &[u8] = include_bytes!("formats/v2-begin.bin");
const V2_PROFILE: &[u8] = include_bytes!("formats/v2-profile.bin");
fn populated() -> (LogicalActivationApplication<Counter>, Vec<u8>) {
    let mut a = app(2);
    let business = a.business(&5i64.to_le_bytes()).unwrap();
    a.apply_batch(&[entry(1, op(100), business)]).unwrap();
    let begin_bytes = a.begin(&intent(1)).unwrap();
    a.apply_batch(&[entry(2, ops(1).begin, begin_bytes.clone())])
        .unwrap();
    (a, begin_bytes)
}
#[test]
fn v2_encodings_are_pinned_and_round_trip() {
    let (a, begin_bytes) = populated();
    assert_eq!(app(2).checkpoint(usize::MAX).unwrap(), V2_EMPTY);
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), V2_POPULATED);
    assert_eq!(begin_bytes, V2_BEGIN);
    assert_eq!(
        a.control_profile().configuration_bytes(4096).unwrap(),
        V2_PROFILE
    );
    assert_eq!(&V2_EMPTY[..8], b"VBACTC02");
    assert_eq!(&V2_BEGIN[..8], b"VBACTA02");
    assert_eq!(&V2_PROFILE[..8], b"VBACTP02");
    assert_eq!(&V2_PROFILE[8..16], &2u64.to_le_bytes());
    assert_eq!(a.control_profile().format_version(), 2);
    for (applied, bytes) in [(0, V2_EMPTY), (2, V2_POPULATED)] {
        let mut restored = app(2);
        restored
            .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, applied, bytes)
            .unwrap();
        assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
    }
    let mut restored = app(2);
    restored
        .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, 2, V2_POPULATED)
        .unwrap();
    assert_eq!(restored.active(), Some(ops(1).begin));
    assert_eq!(restored.inner.read_at(2, ()).unwrap(), 5);
}
#[test]
fn v1_checkpoints_commands_and_profiles_are_refused_typed() {
    assert_eq!(&V1_EMPTY[..8], b"VBACTC01");
    assert_eq!(&V1_BEGIN[..8], b"VBACTA01");
    assert_eq!(&V1_PROFILE[..8], b"VBACTP01");
    assert_ne!(LOGICAL_ACTIVATION_SCHEMA, OLD_SCHEMA);
    let mut a = app(2);
    let before = a.checkpoint(usize::MAX).unwrap();
    for (applied, bytes) in [(0, V1_EMPTY), (2, V1_POPULATED)] {
        assert_eq!(
            a.restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, applied, bytes),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(
            a.restore_checkpoint(OLD_SCHEMA, applied, bytes),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    }
    // A v1 log entry is never decoded under the v2 validity rule.
    let begin = op(10);
    assert_eq!(
        codec::decode(V1_BEGIN, CONTROL_BYTES).err(),
        Some(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(
        a.validate_proposal(begin, V1_BEGIN, std::iter::empty()),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(
        a.apply_batch(&[entry(1, begin, V1_BEGIN.to_vec())]).err(),
        Some(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    assert_eq!(
        a.control_profile().classify_command(begin, V1_BEGIN),
        Err(ApplicationError::UnsupportedSchema)
    );
    // A v1 business envelope is refused the same way.
    let mut v1_business = b"VBACTA01\x00".to_vec();
    v1_business.extend(5i64.to_le_bytes());
    assert_eq!(
        a.validate_proposal(op(100), &v1_business, std::iter::empty()),
        Err(ApplicationError::UnsupportedSchema)
    );
    // The profile binds child stores: v1 and v2 differ, so a child store
    // written under v1 does not open under v2.
    assert_ne!(
        a.control_profile().configuration_bytes(4096).unwrap(),
        V1_PROFILE
    );
}
