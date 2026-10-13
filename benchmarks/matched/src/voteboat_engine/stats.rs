// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(super) struct Stats {
    pub retained: AtomicUsize,
    pub peak_retained: AtomicUsize,
    pub peak_peer_queue: AtomicUsize,
    pub peak_client_queue: AtomicUsize,
    pub persistence_units: AtomicUsize,
    pub checkpoint_count: AtomicUsize,
    pub snapshot_installs: AtomicUsize,
}
#[derive(Serialize)]
pub(crate) struct Diagnostics {
    retained_entries: usize,
    peak_retained_entries: usize,
    peak_peer_queue: usize,
    peak_client_queue: usize,
    persistence_units: usize,
    checkpoint_count: usize,
    snapshot_installs: usize,
}
impl Stats {
    pub fn read(&self) -> Diagnostics {
        Diagnostics {
            retained_entries: self.retained.load(Ordering::Relaxed),
            peak_retained_entries: self.peak_retained.load(Ordering::Relaxed),
            peak_peer_queue: self.peak_peer_queue.load(Ordering::Relaxed),
            peak_client_queue: self.peak_client_queue.load(Ordering::Relaxed),
            persistence_units: self.persistence_units.load(Ordering::Relaxed),
            checkpoint_count: self.checkpoint_count.load(Ordering::Relaxed),
            snapshot_installs: self.snapshot_installs.load(Ordering::Relaxed),
        }
    }
}
