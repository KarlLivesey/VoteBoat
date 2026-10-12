// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{lower::Lower, progress, Middle, Top, UpperBridge};
use std::time::{Duration, Instant};
use voteboat::logical_voter::*;
pub fn close(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top, count: usize) {
    let end = Instant::now() + Duration::from_secs(15);
    top.close_intake().unwrap();
    // Return the bridge borrow only after its own observations settle. The
    // owning caller then closes and fully drains these same middle children.
    while !top.is_drained() || !bridge.observations_are_drained() {
        progress::step(lower, bridge, top, count);
        if Instant::now() >= end {
            super::diagnostic::shutdown(lower, bridge, top, count);
            panic!("same top and bridge original drain");
        }
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
        if Instant::now() >= end {
            super::diagnostic::middle_shutdown(lower, nodes);
            panic!("same original middle closure");
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    // This is the original full bridge's child backing predicate, checked after
    // each child's intake stops rather than while it admits live Raft work.
    assert!(nodes.iter().all(Middle::work_is_drained));
}
