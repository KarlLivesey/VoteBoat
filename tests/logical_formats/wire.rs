// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::fixture::*;
use voteboat::{
    wire::{WireCodec, WireLimits},
    wire_format::NativeWireCodec,
};
const ORIGINAL: [&[u8]; 8] = [
    include_bytes!("golden/wire1.bin"),
    include_bytes!("golden/wire2.bin"),
    include_bytes!("golden/wire3.bin"),
    include_bytes!("golden/wire4.bin"),
    include_bytes!("golden/wire5.bin"),
    include_bytes!("golden/wire6.bin"),
    include_bytes!("golden/wire7.bin"),
    include_bytes!("golden/wire8.bin"),
];
fn codecs() -> [NativeWireCodec; 8] {
    let l = WireLimits::default();
    [
        NativeWireCodec::new(l),
        NativeWireCodec::with_membership(l),
        NativeWireCodec::with_authority(l),
        NativeWireCodec::with_readiness(l),
        NativeWireCodec::with_learner_repair(l),
        NativeWireCodec::with_snapshot_repair(l),
        NativeWireCodec::with_committed_snapshot_repair(l),
        NativeWireCodec::with_leadership_transfer(l),
    ]
    .map(Result::unwrap)
}
#[test]
fn all_original_wire_versions_are_core_available_and_byte_identical() {
    for (codec, original) in codecs().into_iter().zip(ORIGINAL) {
        assert_eq!(codec.encode_batch(scope(), &messages()).unwrap(), original);
        assert_eq!(codec.decode_batch(scope(), original).unwrap(), messages());
        assert_eq!(
            codec.encoded_length(scope(), &messages()).unwrap(),
            original.len()
        );
        assert_eq!(
            codec
                .frame_length(&original[..codec.header_bytes()])
                .unwrap(),
            original.len()
        );
        let mut supplied = vec![0; original.len()];
        codec
            .encode_into(scope(), &messages(), &mut supplied)
            .unwrap();
        assert_eq!(supplied, original);
    }
}
#[test]
fn all_original_wire_versions_reject_truncation_corruption_and_wrong_scope() {
    for (codec, original) in codecs().into_iter().zip(ORIGINAL) {
        for cut in 0..original.len() {
            assert!(
                codec.decode_batch(scope(), &original[..cut]).is_err(),
                "wire{} cut{cut}",
                codec.format_version()
            );
        }
        let mut corrupt = original.to_vec();
        corrupt[codec.header_bytes()] ^= 1;
        assert!(codec.decode_batch(scope(), &corrupt).is_err());
        let mut wrong = scope();
        wrong.sender.session = voteboat::identity::StoreSession::new(6).unwrap();
        assert!(codec.decode_batch(wrong, original).is_err());
        assert!(codec.encode_batch(wrong, &messages()).is_err());
    }
}
#[cfg(feature = "native")]
#[test]
fn native_wire_public_export_keeps_the_original_contract() {
    let codec =
        voteboat::native::wire::NativeWireCodec::with_leadership_transfer(WireLimits::default())
            .unwrap();
    assert_eq!(
        codec.encode_batch(scope(), &messages()).unwrap(),
        ORIGINAL[7]
    );
    assert_eq!(
        codec.decode_batch(scope(), ORIGINAL[7]).unwrap(),
        messages()
    );
}
