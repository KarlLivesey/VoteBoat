// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::cluster::ChildNode;
use std::time::{Duration, Instant};
pub fn join(node: ChildNode) {
    let mut parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual logical child must drain"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("actual child connector must drain"));
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let (mut wal, mut snapshot) = (false, false);
    loop {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !wal {
            wal = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if dial && wal && snapshot {
            return;
        }
        assert!(Instant::now() < end, "actual child workers must join");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
