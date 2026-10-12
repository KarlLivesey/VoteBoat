// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::fixture::*;
use voteboat::{
    log::LogLimits,
    log_format::{LogCodec, NativeLogCodec},
};
const BATCH: &[u8] = include_bytes!("golden/log-batch.bin");
const CHECKPOINT: &[u8] = include_bytes!("golden/log-checkpoint.bin");
#[test]
fn original_batch_and_checkpoint_are_core_available_and_byte_identical() {
    let limits = LogLimits::default();
    assert_eq!(NativeLogCodec.format_version(), 2);
    assert!(NativeLogCodec.supports_checkpoint());
    assert_eq!(
        NativeLogCodec
            .encode_batch(2, &mutations(), limits)
            .unwrap(),
        BATCH
    );
    assert_eq!(
        NativeLogCodec.decode_batch(BATCH, limits).unwrap(),
        (2, mutations())
    );
    assert_eq!(
        NativeLogCodec.frame_length(&BATCH[..32], limits).unwrap(),
        BATCH.len()
    );
    assert_eq!(
        NativeLogCodec
            .encode_checkpoint(2, &state(), limits, 4096)
            .unwrap(),
        CHECKPOINT
    );
    assert!(NativeLogCodec.is_checkpoint(CHECKPOINT));
    assert_eq!(
        NativeLogCodec
            .decode_checkpoint(CHECKPOINT, limits)
            .unwrap(),
        (2, state(), CHECKPOINT.len())
    );
}
#[test]
fn original_log_formats_reject_incomplete_and_corrupt_bytes() {
    let limits = LogLimits::default();
    for cut in 0..BATCH.len() {
        assert!(
            NativeLogCodec.decode_batch(&BATCH[..cut], limits).is_err(),
            "batch cut{cut}"
        );
    }
    for cut in 0..CHECKPOINT.len() {
        assert!(
            NativeLogCodec
                .decode_checkpoint(&CHECKPOINT[..cut], limits)
                .is_err(),
            "checkpoint cut{cut}"
        );
    }
    let mut corrupt = BATCH.to_vec();
    corrupt[32] ^= 1;
    assert!(NativeLogCodec.decode_batch(&corrupt, limits).is_err());
    let mut corrupt = CHECKPOINT.to_vec();
    corrupt[32] ^= 1;
    assert!(NativeLogCodec.decode_checkpoint(&corrupt, limits).is_err());
    let small = LogLimits {
        max_batch_bytes: BATCH.len() - 1,
        ..limits
    };
    assert!(NativeLogCodec.encode_batch(2, &mutations(), small).is_err());
    assert!(NativeLogCodec.frame_length(&BATCH[..32], small).is_err());
}
#[cfg(feature = "native")]
#[test]
fn native_log_public_exports_keep_the_original_contract() {
    use voteboat::native::log_store::{LogCodec as NativePath, NativeLogCodec as Codec};
    assert_eq!(
        NativePath::encode_batch(&Codec, 2, &mutations(), LogLimits::default()).unwrap(),
        BATCH
    );
    assert_eq!(
        NativePath::encode_checkpoint(&Codec, 2, &state(), LogLimits::default(), 4096).unwrap(),
        CHECKPOINT
    );
}
