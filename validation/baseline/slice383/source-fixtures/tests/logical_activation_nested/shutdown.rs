// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{lower::Lower, progress, Middle, Top, UpperBridge};
use std::time::{Duration, Instant};
use voteboat::logical_voter::*;
pub fn close(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top, count: usize) {
    let end = Instant::now() + Duration::from_secs(15);
    top.close_intake().unwrap();
    while !top.is_drained() || !bridge.is_drained() {
        progress::step(lower, bridge, top, count);
        assert!(Instant::now() < end, "same top and bridge original drain");
    }
}
pub fn middle(lower: &mut Lower<'_>, nodes: &mut [Middle]) {
    for node in &mut *nodes {
        node.close_intake().unwrap();
    }
    let end = Instant::now() + Duration::from_secs(15);
    while !nodes.iter().all(Middle::is_drained) {
        lower.pump(nodes);
        lower.advance();
        lower.pump(nodes);
        let now = lower.now();
        for node in &mut *nodes {
            node.poll_shutdown(now, LogicalNodePollBudget::default())
                .unwrap_or_else(|e| panic!("actual original middle shutdown {:?}", e.reason));
        }
        assert!(Instant::now() < end, "same original middle closure");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
