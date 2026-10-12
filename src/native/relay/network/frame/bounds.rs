// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Declared native envelope capabilities before any original owner is accepted.
use super::*;
use std::mem::size_of;
const CAPTURE_BINDING_BYTES: usize = 90;
const ORIGINAL_TICKET_BYTES: usize = 64;
pub(in crate::native::relay::network) fn controls_fit(
    hops: usize,
    limits: RelayNetworkLimits,
) -> bool {
    // Fixed metadata stores the full bound, including unused capture slots.
    let encoded = FRAME_HEADER + 1 + 3 + hops * CAPTURE_BINDING_BYTES + ORIGINAL_TICKET_BYTES;
    let decoded = size_of::<RelayFrame>() + size_of::<RetiredFrame>();
    encoded <= limits.frame_bytes && decoded <= limits.decoded_bytes
}
pub(in crate::native::relay::network) fn codec_fit(
    hops: usize,
    limits: RelayNetworkLimits,
    wire: WireLimits,
) -> bool {
    let encoded = FRAME_HEADER
        + 1
        + 3
        + hops * CAPTURE_BINDING_BYTES
        + ORIGINAL_TICKET_BYTES
        + 8
        + 8
        + wire.max_frame_bytes
        + wire.max_messages * (1 + 1 + ORIGINAL_TICKET_BYTES);
    let decoded = size_of::<RelayFrame>()
        + size_of::<ForwardFrame>()
        + size_of::<OutputFrame>()
        + wire.max_frame_bytes
        + wire.max_messages * (size_of::<RecordKind>() + size_of::<Option<SendTicket>>());
    wire.max_messages <= 4096
        && controls_fit(hops, limits)
        && encoded <= limits.frame_bytes
        && decoded <= limits.decoded_bytes
        && wire.max_decoded_bytes <= limits.decoded_bytes
}
