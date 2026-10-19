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
//! ST2 round 3 (I5) and round 4 (R4-1): exact in-flight WAL accounting
//! before anything is appended to a log.
//!
//! Every staged durable core transition carries an upper bound of its WAL
//! encoding (`Raft::pending_wal_bytes`). The shard keeps their sum. At each
//! poll the node gives it the WAL's published length, its high-water byte
//! mark `H` and the control reserve `R`; the WAL thread publishes its length
//! before it sends a window's `Durable` events, so a transition that completed
//! before that refresh is in the published length, and bytes completed since
//! are kept counted until the next refresh. So `A = published + pending +
//! released` is never below the bytes the WAL will hold once everything staged
//! is written (rewrites only lower it).
//!
//! Live log growth is checked before it is staged:
//! - data (a proposal, a configuration entry, a follower append carrying any
//!   non-noop entry) needs `A + bound <= H`: a proposal that does not fit is
//!   refused with `RaftError::StoragePressure` before the log, a follower
//!   declines that append;
//! - noops (a leader's own noop, a follower append whose entries are all
//!   noops) need `A + bound <= H + R`: a follower declines such an append, and
//!   a core is told before every step whether one leader noop fits, so it does
//!   not campaign, or steps down typed, when it does not.
//!
//! Vote, term, commit and truncation records add no live bytes (one fixed
//! record per group in a rewrite image) and are never refused. Hence a
//! rewrite's live image stays at most `H + R`, and the node's headroom rule
//! (`U - H >= max_batch_bytes + R`) makes every admitted batch fit after one
//! rewrite. All groups share the one account, so the bound does not grow with
//! the number of groups.
use crate::{
    log::{entry_wal_bound, EntryPayload, NOOP_WAL_BOUND, UPDATE_WAL_OVERHEAD},
    raft::{Event, Rpc},
};

/// The node's latest view of its bounded WAL: the high-water byte mark data
/// must stay under, the reserve above it that only noops may use, and the
/// length the WAL thread last published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalBudget {
    pub limit: usize,
    pub published: usize,
    /// Bytes above `limit` that noop transitions may use (the store's
    /// control reserve, R4-1).
    pub reserve: usize,
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
    /// Noop transitions the account refused (R4-1, saturating): campaigns not
    /// started and leader step-downs because one noop did not fit, and
    /// declined noop-only follower appends.
    pub noop_refused: u64,
}
impl WalAccount {
    fn used(&self, b: WalBudget) -> usize {
        b.published
            .saturating_add(self.pending)
            .saturating_add(self.released)
    }
    /// Room left under the mark, when the node has a bounded WAL.
    pub(super) fn room(&self) -> Option<usize> {
        self.budget.map(|b| b.limit.saturating_sub(self.used(b)))
    }
    /// Room left under the mark plus the reserve, for noop transitions.
    pub(super) fn noop_room(&self) -> Option<usize> {
        self.budget.map(|b| {
            b.limit
                .saturating_add(b.reserve)
                .saturating_sub(self.used(b))
        })
    }
    /// Whether one leader noop transition fits (true without a bounded WAL).
    pub(super) fn noop_fits(&self) -> bool {
        self.noop_room().is_none_or(|room| room >= NOOP_WAL_BOUND)
    }
    /// Whether `event`'s live growth no longer fits: data under the mark
    /// (first), noop-only appends under the mark plus the reserve (second).
    pub(super) fn over(&self, event: &Event) -> (bool, bool) {
        let Some(room) = self.room() else {
            return (false, false);
        };
        match growth_bound(event) {
            Some((bound, true)) => (bound > room, false),
            Some((bound, false)) => (false, self.noop_room().is_some_and(|r| bound > r)),
            None => (false, false),
        }
    }
    /// Count one stepped event whose growth did not fit (`over`, including
    /// `noop_over`): a refused proposal or a declined follower append.
    pub(super) fn count_refusal(&mut self, proposal: bool, over: bool, noop_over: bool) {
        if !over {
            return;
        }
        if proposal {
            self.refused = self.refused.saturating_add(1);
        } else {
            self.declined = self.declined.saturating_add(1);
        }
        if noop_over {
            self.noop_refused = self.noop_refused.saturating_add(1);
        }
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
/// Upper bound of the live bytes `event` would stage, and whether they are
/// data (`true`) or noops only (`false`): a proposal or configuration entry
/// (data), or a follower append with entries.
pub(super) fn growth_bound(event: &Event) -> Option<(usize, bool)> {
    match event {
        Event::Propose { bytes, .. } => Some((proposal_bound(bytes.len()), true)),
        Event::Configure(proposal) => Some((
            UPDATE_WAL_OVERHEAD.saturating_add(entry_wal_bound(proposal.record.retained_bytes())),
            true,
        )),
        Event::Receive(message) => match &message.rpc {
            Rpc::Append { entries, .. } if !entries.is_empty() => {
                let data = entries
                    .iter()
                    .any(|e| !matches!(e.payload, EntryPayload::Noop));
                Some((
                    entries.iter().fold(UPDATE_WAL_OVERHEAD, |sum, e| {
                        sum.saturating_add(entry_wal_bound(e.payload_bytes()))
                    }),
                    data,
                ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{identity::*, log::LogEntry, raft::Message};
    fn append(payloads: &[Option<usize>]) -> Event {
        let id = |n| StoreIdentity {
            id: StoreId::new(n).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        };
        Event::Receive(Message {
            group: GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: ConfigurationId::new(1).unwrap(),
            from: NodeId::new(2).unwrap(),
            sender: StoreBinding {
                identity: id(2),
                session: StoreSession::new(1).unwrap(),
            },
            to: NodeId::new(1).unwrap(),
            term: 2,
            context: crate::raft::RequestContext {
                origin: StoreBinding {
                    identity: id(2),
                    session: StoreSession::new(1).unwrap(),
                },
                sequence: 1,
            },
            rpc: Rpc::Append {
                previous_index: 0,
                previous_term: 0,
                entries: payloads
                    .iter()
                    .enumerate()
                    .map(|(i, p)| LogEntry {
                        index: i as u64 + 1,
                        term: 2,
                        payload: match p {
                            None => EntryPayload::Noop,
                            Some(n) => EntryPayload::Command {
                                operation: crate::identity::OperationId::new(i as u128 + 1)
                                    .unwrap(),
                                bytes: vec![0; *n].into(),
                            },
                        },
                    })
                    .collect(),
                leader_commit: 0,
            },
        })
    }
    /// R4-1: noop-only appends are live growth checked against the reserve;
    /// heartbeats (no entries) are not growth; any data entry makes the whole
    /// append data.
    #[test]
    fn appends_are_classified_as_data_or_noop_growth() {
        assert_eq!(growth_bound(&append(&[])), None);
        assert_eq!(
            growth_bound(&append(&[None, None])),
            Some((UPDATE_WAL_OVERHEAD + 2 * entry_wal_bound(0), false))
        );
        assert_eq!(
            growth_bound(&append(&[None, Some(10)])),
            Some((
                UPDATE_WAL_OVERHEAD + entry_wal_bound(0) + entry_wal_bound(10),
                true
            ))
        );
        assert_eq!(NOOP_WAL_BOUND, UPDATE_WAL_OVERHEAD + entry_wal_bound(0));
    }
    /// R4-1: data room ends at the mark, noop room at the mark plus the
    /// reserve; staged and recently completed bytes count against both.
    #[test]
    fn noop_room_extends_past_the_mark_by_the_reserve_only() {
        let mut account = WalAccount::default();
        assert!(account.noop_fits(), "no bounded WAL: always room");
        account.refresh(Some(WalBudget {
            limit: 10_000,
            published: 9_000,
            reserve: 2_000,
        }));
        assert_eq!(account.room(), Some(1_000));
        assert_eq!(account.noop_room(), Some(3_000));
        account.track(0, 2_000);
        assert_eq!(account.room(), Some(0));
        assert_eq!(account.noop_room(), Some(1_000));
        assert!(account.noop_fits());
        account.track(2_000, 2_500);
        assert_eq!(account.noop_room(), Some(500));
        assert!(!account.noop_fits());
        // Completed bytes stay counted until the next published length.
        account.track(2_500, 0);
        assert_eq!(account.noop_room(), Some(500));
        account.refresh(Some(WalBudget {
            limit: 10_000,
            published: 11_500,
            reserve: 2_000,
        }));
        assert_eq!(account.noop_room(), Some(500));
        assert!(!account.noop_fits());
    }
}
