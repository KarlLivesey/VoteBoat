// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
/// These outcomes permit another read-only observation. They say nothing about
/// whether a mutation was accepted and do not authorize replay.
pub(super) fn repeat_observation(reason: &str) -> bool {
    matches!(
        reason,
        "authentication deadline expired"
            | "request deadline expired"
            | "reply deadline expired"
            | "connection closed during request"
            | "connection closed without a complete reply"
    )
}
