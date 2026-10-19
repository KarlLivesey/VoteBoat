// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! ST2 round 3 (I5): exact in-flight WAL accounting at data admission.
//!
//! Every staged durable core transition carries an upper bound of its WAL
//! encoding (`Raft::pending_wal_bytes`). The shard keeps their sum. At each
//! poll the node gives it the WAL's published length and its high-water byte
//! mark; the WAL thread publishes its length before it sends a window's
//! `Durable` events, so a transition that completed before that refresh is in
//! the published length, and bytes completed since are kept counted until the
//! next refresh. So `published + pending + released` is never below the bytes
//! the WAL will hold once everything staged is written (rewrites only lower
//! it). Before an event that would stage data, the shard checks that its bound
//! still fits under the mark: a proposal that does not is refused with
//! `RaftError::StoragePressure` before the log, and a follower declines that
//! append. Control transitions are never refused. Hence data bytes in the WAL
//! never exceed the high-water mark.
use crate::{
    log::{entry_wal_bound, EntryPayload, UPDATE_WAL_OVERHEAD},
    raft::{Event, Rpc},
};

/// The node's latest view of its bounded WAL: the high-water byte mark data
/// must stay under, and the length the WAL thread last published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalBudget {
    pub limit: usize,
    pub published: usize,
}
/// Shard-wide WAL accounting (volatile, rebuilt with the shard).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalAccount {
    /// Sum of the staged transitions' WAL bounds.
    pub pending: usize,
    /// Bounds of transitions completed or dropped since the last refresh.
    pub released: usize,
    pub budget: Option<WalBudget>,
    /// Proposals refused because their bound did not fit (saturating).
    pub refused: u64,
    /// Follower appends declined because their bound did not fit (saturating).
    pub declined: u64,
}
impl WalAccount {
    /// Room left under the mark, when the node has a bounded WAL.
    pub(super) fn room(&self) -> Option<usize> {
        self.budget.map(|b| {
            b.limit.saturating_sub(
                b.published
                    .saturating_add(self.pending)
                    .saturating_add(self.released),
            )
        })
    }
    pub(super) fn refresh(&mut self, budget: Option<WalBudget>) {
        self.budget = budget;
        self.released = 0;
    }
    /// A core's staged bound changed from `before` to `after`.
    pub(super) fn track(&mut self, before: usize, after: usize) {
        if after >= before {
            self.pending = self.pending.saturating_add(after - before);
        } else {
            let freed = before - after;
            self.pending = self.pending.saturating_sub(freed);
            self.released = self.released.saturating_add(freed);
        }
    }
}
/// Upper bound of the data bytes `event` would stage, when it stages data:
/// a proposal, a configuration entry, or a follower append with data entries.
pub(super) fn data_bound(event: &Event) -> Option<usize> {
    match event {
        Event::Propose { bytes, .. } => Some(proposal_bound(bytes.len())),
        Event::Configure(proposal) => Some(
            UPDATE_WAL_OVERHEAD.saturating_add(entry_wal_bound(proposal.record.retained_bytes())),
        ),
        Event::Receive(message) => match &message.rpc {
            Rpc::Append { entries, .. }
                if entries
                    .iter()
                    .any(|e| !matches!(e.payload, EntryPayload::Noop)) =>
            {
                Some(entries.iter().fold(UPDATE_WAL_OVERHEAD, |sum, e| {
                    sum.saturating_add(entry_wal_bound(e.payload_bytes()))
                }))
            }
            _ => None,
        },
        _ => None,
    }
}
/// Bound of one proposal of `payload` bytes staged on its own.
pub(super) fn proposal_bound(payload: usize) -> usize {
    UPDATE_WAL_OVERHEAD.saturating_add(entry_wal_bound(payload))
}
