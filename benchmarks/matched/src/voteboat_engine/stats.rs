// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use crate::{Failure, MAX_PROPOSAL_ENTRIES};
use serde::Serialize;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Widths(Box<[AtomicUsize]>);
impl Default for Widths {
    fn default() -> Self {
        Self(
            (0..=MAX_PROPOSAL_ENTRIES)
                .map(|_| AtomicUsize::new(0))
                .collect(),
        )
    }
}

#[derive(Default)]
pub(super) struct Stats {
    #[cfg(test)]
    pub application_receipts: AtomicU64,
    #[cfg(test)]
    pub application_intervals: std::sync::Mutex<Vec<(u64, u64, usize)>>,
    pub retained: AtomicUsize,
    pub peak_retained: AtomicUsize,
    pub peak_peer_queue: AtomicUsize,
    pub peak_client_queue: AtomicUsize,
    pub persistence_units: AtomicUsize,
    pub checkpoint_count: AtomicUsize,
    pub snapshot_installs: AtomicUsize,
    proposal_widths: Widths,
}
#[derive(Serialize)]
struct ProposalWidth {
    original_entries: usize,
    batches: usize,
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
    proposal_batches: usize,
    proposed_originals: usize,
    maximum_proposal_width: usize,
    proposal_widths: Vec<ProposalWidth>,
}
impl Stats {
    pub fn record_proposals(&self, width: usize) -> Result<(), Failure> {
        if width == 0 {
            return Err("empty original proposal batch".into());
        }
        self.proposal_widths
            .0
            .get(width)
            .ok_or("original proposal width exceeds diagnostic bound")?
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    pub fn read(&self) -> Diagnostics {
        let proposal_widths: Vec<_> = self
            .proposal_widths
            .0
            .iter()
            .enumerate()
            .filter_map(|(original_entries, count)| {
                let batches = count.load(Ordering::Relaxed);
                (batches > 0).then_some(ProposalWidth {
                    original_entries,
                    batches,
                })
            })
            .collect();
        Diagnostics {
            retained_entries: self.retained.load(Ordering::Relaxed),
            peak_retained_entries: self.peak_retained.load(Ordering::Relaxed),
            peak_peer_queue: self.peak_peer_queue.load(Ordering::Relaxed),
            peak_client_queue: self.peak_client_queue.load(Ordering::Relaxed),
            persistence_units: self.persistence_units.load(Ordering::Relaxed),
            checkpoint_count: self.checkpoint_count.load(Ordering::Relaxed),
            snapshot_installs: self.snapshot_installs.load(Ordering::Relaxed),
            proposal_batches: proposal_widths.iter().map(|width| width.batches).sum(),
            proposed_originals: proposal_widths
                .iter()
                .map(|width| width.original_entries * width.batches)
                .sum(),
            maximum_proposal_width: proposal_widths
                .last()
                .map_or(0, |width| width.original_entries),
            proposal_widths,
        }
    }
}
