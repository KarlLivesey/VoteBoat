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
