// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn log(max_batch_bytes: usize) -> LogLimits {
    LogLimits {
        max_batch_bytes,
        max_wal_bytes: max_batch_bytes.max(LogLimits::default().max_wal_bytes),
        ..LogLimits::default()
    }
}

/// One idle data batch of `bytes` fits both byte budgets, one byte more
/// fits neither the batch budget nor (when raised) the budgets themselves.
fn admits_one_data_batch(outbound: OutboundLimits, bytes: usize) -> bool {
    let batch = OutboundUsage {
        batches: 1,
        messages: 1,
        bytes,
    };
    let idle = [OutboundUsage::default(); 3];
    bytes <= outbound.batch_bytes
        && outbound.peer.admits(idle, MessageClass::Data, batch)
        && outbound.node.admits(idle, MessageClass::Data, batch)
}

#[test]
fn outbound_batches_carry_every_selected_append() {
    // Up to the default 1 MiB the default limits are kept unchanged.
    for max in [256, 64 * 1024, 1024 * 1024] {
        let outbound = startup_outbound_limits(log(max)).unwrap();
        let default = OutboundLimits::default();
        assert_eq!(outbound.batch_bytes, default.batch_bytes);
        assert_eq!(outbound.peer.max.bytes, default.peer.max.bytes);
        assert_eq!(outbound.node.max.bytes, default.node.max.bytes);
        assert!(admits_one_data_batch(outbound, max));
    }
    // Larger configured batches raise exactly what one such batch needs.
    for max in [
        1024 * 1024 + 1,
        8 * 1024 * 1024,
        33_587_200,
        256 * 1024 * 1024,
    ] {
        let outbound = startup_outbound_limits(log(max)).unwrap();
        assert_eq!(outbound.batch_bytes, max);
        assert!(admits_one_data_batch(outbound, max), "{max}");
        assert!(!admits_one_data_batch(outbound, max + 1), "{max}");
        assert!(outbound.validate().is_ok());
    }
    // A batch beyond what an outbound budget can hold refuses startup.
    let refused = startup_outbound_limits(log(1024 * 1024 * 1024)).unwrap_err();
    assert_eq!(refused.stage, "outbound limits");
}
