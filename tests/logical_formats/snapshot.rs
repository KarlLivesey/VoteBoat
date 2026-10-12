// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::fixture;
use voteboat::{
    snapshot::SnapshotLimits,
    snapshot_format::{NativeSnapshotCodec, SnapshotCodec},
};
const STATIC: &[u8] = include_bytes!("golden/snapshot1.bin");
const MEMBER: &[u8] = include_bytes!("golden/snapshot2.bin");
fn encoded(member: bool) -> Vec<u8> {
    let snapshot = fixture::snapshot(member);
    let limits = SnapshotLimits::default();
    let mut bytes = NativeSnapshotCodec
        .prefix(&snapshot.metadata, snapshot.application.len(), limits)
        .unwrap();
    bytes.extend(snapshot.application);
    bytes.extend(NativeSnapshotCodec.finish(&bytes, limits).unwrap());
    bytes
}
#[test]
fn original_static_and_membership_snapshot_bytes_are_core_available() {
    assert_eq!(NativeSnapshotCodec.format_version(), 2);
    for (member, original) in [(false, STATIC), (true, MEMBER)] {
        assert_eq!(encoded(member), original);
        assert_eq!(
            NativeSnapshotCodec
                .decode(original, SnapshotLimits::default())
                .unwrap(),
            fixture::snapshot(member)
        );
    }
}
#[test]
fn original_snapshot_formats_reject_partial_corrupt_and_oversized_content() {
    let limits = SnapshotLimits::default();
    for original in [STATIC, MEMBER] {
        for cut in 0..original.len() {
            assert!(
                NativeSnapshotCodec
                    .decode(&original[..cut], limits)
                    .is_err(),
                "snapshot cut{cut}"
            );
        }
        let mut corrupt = original.to_vec();
        corrupt[44] ^= 1;
        assert!(NativeSnapshotCodec.decode(&corrupt, limits).is_err());
    }
    assert!(NativeSnapshotCodec
        .prefix(
            &fixture::snapshot(false).metadata,
            limits.max_application_bytes + 1,
            limits
        )
        .is_err());
}
#[cfg(feature = "native")]
#[test]
fn native_snapshot_public_exports_keep_the_original_contract() {
    use voteboat::native::snapshot_store::{
        NativeSnapshotCodec as Codec, SnapshotCodec as NativePath,
    };
    for (member, original) in [(false, STATIC), (true, MEMBER)] {
        let snapshot = fixture::snapshot(member);
        let limits = SnapshotLimits::default();
        let mut bytes = NativePath::prefix(
            &Codec,
            &snapshot.metadata,
            snapshot.application.len(),
            limits,
        )
        .unwrap();
        bytes.extend(snapshot.application);
        bytes.extend(NativePath::finish(&Codec, &bytes, limits).unwrap());
        assert_eq!(bytes, original);
        assert_eq!(
            NativePath::decode(&Codec, original, limits).unwrap(),
            fixture::snapshot(member)
        );
    }
}
