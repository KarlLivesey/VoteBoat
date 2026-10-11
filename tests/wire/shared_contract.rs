// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn provisioning(codec: &impl WireCodec, scope: WireScope, input: &[Message]) -> Vec<u8> {
    let limits = codec.limits().validate().unwrap();
    let encoded = codec.encode_batch(scope, input).unwrap();
    assert!(codec.header_bytes() > 0 && codec.header_bytes() <= encoded.len());
    assert!(encoded.capacity() <= limits.max_frame_bytes);
    assert_eq!(
        codec.frame_length(&encoded[..codec.header_bytes()]),
        Ok(encoded.len())
    );
    assert_eq!(codec.encoded_length(scope, input), Ok(encoded.len()));
    let mut supplied = vec![0xff; encoded.len()];
    codec.encode_into(scope, input, &mut supplied).unwrap();
    assert_eq!(supplied, encoded);
    assert_eq!(codec.decode_batch(scope, &supplied).unwrap(), input);
    // A refused destination is not a frame, and its bytes may have changed.
    assert_eq!(
        codec.encode_into(scope, input, &mut supplied[..encoded.len() - 1]),
        Err(WireError::TooLarge)
    );
    supplied.push(0xff);
    assert_eq!(
        codec.encode_into(scope, input, &mut supplied),
        Err(WireError::TooLarge)
    );
    // Reuse the original borrowed input after both refusals.
    assert_eq!(codec.encode_batch(scope, input).unwrap(), encoded);
    let mut supplied = vec![0xff; encoded.len()];
    codec.encode_into(scope, input, &mut supplied).unwrap();
    assert_eq!(supplied, encoded);
    encoded
}

pub(super) fn wrong_scope(
    codec: &impl WireCodec,
    original: WireScope,
    input: &[Message],
    frame: &[u8],
) {
    let mut wrong = original;
    wrong.sender.session = StoreSession::new(2).unwrap();
    assert_ne!(wrong, original);
    assert_eq!(
        codec.encoded_length(wrong, input),
        Err(WireError::WrongPeer)
    );
    assert_eq!(codec.encode_batch(wrong, input), Err(WireError::WrongPeer));
    assert_eq!(
        codec.encode_into(wrong, input, &mut vec![0; frame.len()]),
        Err(WireError::WrongPeer)
    );
    assert_eq!(codec.decode_batch(wrong, frame), Err(WireError::WrongPeer));
    assert_eq!(codec.decode_batch(original, frame).unwrap(), input);
}

#[path = "shared_contract/mutants.rs"]
mod mutants;
