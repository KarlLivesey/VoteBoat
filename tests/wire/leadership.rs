// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::native::wire::NativeWireCodec;
#[test]
fn targeted_handoff_requires_wire8_and_roundtrips_u128_operation_and_exact_scope() {
    let codec = NativeWireCodec::with_leadership_transfer(WireLimits::default()).unwrap();
    let mut m = message(Rpc::TimeoutNow {
        operation: OperationId::new(u128::MAX).unwrap(),
        index: 7,
        log_term: 3,
    });
    m.context.origin = m.sender;
    let frame = codec.encode_batch(scope(), &[m.clone()]).unwrap();
    assert_eq!(codec.decode_batch(scope(), &frame).unwrap(), [m.clone()]);
    let old = NativeWireCodec::with_committed_snapshot_repair(WireLimits::default()).unwrap();
    assert!(old.encode_batch(scope(), &[m.clone()]).is_err());
    assert!(old.decode_batch(scope(), &frame).is_err());
    for end in 0..frame.len() {
        assert!(codec.decode_batch(scope(), &frame[..end]).is_err());
    }
    let mut wrong = m.clone();
    wrong.context.origin = HostLogStore::new(2).binding;
    assert!(codec.encode_batch(scope(), &[wrong]).is_err());
    for (index, log_term) in [(0, 3), (7, 2), (7, 4)] {
        m.rpc = Rpc::TimeoutNow {
            operation: OperationId::new(1).unwrap(),
            index,
            log_term,
        };
        assert!(codec.encode_batch(scope(), &[m.clone()]).is_err());
    }
}

#[test]
fn commit_notice_wire9_tag19_is_explicit_bounded_and_refused_by_older_formats() {
    let codec = NativeWireCodec::with_commit_notices(WireLimits::default()).unwrap();
    let mut m = message(Rpc::CommitNotice {
        index: 7,
        log_term: 3,
    });
    m.context.origin = m.sender;
    let frame = codec.encode_batch(scope(), &[m.clone()]).unwrap();
    assert_eq!(codec.format_version(), 9);
    assert_eq!(frame[24 + 4 + 128], 19);
    assert_eq!(codec.decode_batch(scope(), &frame).unwrap(), [m.clone()]);
    assert_eq!(
        codec.encoded_length(scope(), &[m.clone()]).unwrap(),
        frame.len()
    );
    let old = [
        NativeWireCodec::new(WireLimits::default()).unwrap(),
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
        NativeWireCodec::with_authority(WireLimits::default()).unwrap(),
        NativeWireCodec::with_readiness(WireLimits::default()).unwrap(),
        NativeWireCodec::with_learner_repair(WireLimits::default()).unwrap(),
        NativeWireCodec::with_snapshot_repair(WireLimits::default()).unwrap(),
        NativeWireCodec::with_committed_snapshot_repair(WireLimits::default()).unwrap(),
        NativeWireCodec::with_leadership_transfer(WireLimits::default()).unwrap(),
    ];
    for old in old {
        assert!(old.encode_batch(scope(), &[m.clone()]).is_err());
        assert!(old.decode_batch(scope(), &frame).is_err());
    }
    for end in 0..frame.len() {
        assert!(codec.decode_batch(scope(), &frame[..end]).is_err());
    }
    for (index, log_term) in [(0, 3), (7, 0), (7, 4)] {
        let mut bad = m.clone();
        bad.rpc = Rpc::CommitNotice { index, log_term };
        assert!(codec.encode_batch(scope(), &[bad]).is_err());
    }
    m.context.origin = HostLogStore::new(2).binding;
    assert!(codec.encode_batch(scope(), &[m]).is_err());
}
