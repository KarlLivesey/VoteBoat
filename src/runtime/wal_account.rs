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
//! - data (a proposal, a configuration entry, a follower append whose new
//!   suffix carries any non-noop entry) needs `A + bound <= H`: a proposal
//!   that does not fit is refused with `RaftError::StoragePressure` before
//!   the log, a follower declines that append;
//! - noops (a leader's own noop, a follower append whose new suffix is all
//!   noops) need `A + bound <= H + R`: a follower declines such an append, and
//!   a core is told before every step whether one leader noop fits, so it does
//!   not campaign, or steps down typed, when it does not.
//!
//! Proposals are checked here. A follower append is checked by the core
//! (R5-1) on exactly the validated new suffix it would persist, after the
//! identity and log-matching checks, against the room this account gives it
//! before every step ([`WalAccount::rooms`]); entries it already holds are
//! not written again and are not counted. The core reports its declines,
//! which this account counts.
//!
//! Vote, term, commit and truncation records add no live bytes (one fixed
//! record per group in a rewrite image) and are never refused. Hence an
//! admitted transition never takes a rewrite's live image past `H + R`, and
//! the node's headroom rule (`U - H >= max_batch_bytes + R`) makes every
//! admitted batch fit after one rewrite. This covers admission; a live state
//! recovered above the mark (for example after lowering limits) is not
//! refused at construction, it is reported through the same typed capacity
//! refusals. All groups share the one account, so the bound does not grow
//! with the number of groups.
use crate::{
    log::{entry_wal_bound, NOOP_WAL_BOUND, UPDATE_WAL_OVERHEAD},
    raft::{Event, WalRoom},
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
    /// Noop transitions refused (R4-1, saturating): campaigns not started
    /// and leader step-downs because one noop did not fit, and follower
    /// appends declined because their noop-only new suffix did not fit.
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
    /// The room a core checks a follower append against (R5-1).
    pub(super) fn rooms(&self) -> Option<WalRoom> {
        self.budget.map(|b| {
            let used = self.used(b);
            WalRoom {
                data: b.limit.saturating_sub(used),
                noop: b.limit.saturating_add(b.reserve).saturating_sub(used),
            }
        })
    }
    /// Whether a proposal's live growth no longer fits under the mark.
    pub(super) fn over(&self, event: &Event) -> bool {
        let Some(room) = self.room() else {
            return false;
        };
        growth_bound(event).is_some_and(|bound| bound > room)
    }
    /// Count follower appends a core declined in one step because their new
    /// suffix did not fit, `noop` of them noop-only (R5-1).
    pub(super) fn count_declines(&mut self, declined: u64, noop: u64) {
        self.declined = self.declined.saturating_add(declined);
        self.noop_refused = self.noop_refused.saturating_add(noop);
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
/// Upper bound of the live bytes a proposal or configuration entry would
/// stage (always data). Follower appends are bounded by the core (R5-1).
pub(super) fn growth_bound(event: &Event) -> Option<usize> {
    match event {
        Event::Propose { bytes, .. } => Some(proposal_bound(bytes.len())),
        Event::Configure(proposal) => Some(
            UPDATE_WAL_OVERHEAD.saturating_add(entry_wal_bound(proposal.record.retained_bytes())),
        ),
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
    use crate::{
        identity::*,
        log::{EntryPayload, LogEntry},
        raft::{Message, Rpc},
    };
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
    /// R5-1: the shard bounds proposals; a follower append, whatever its
    /// entries, is bounded by the core on its validated new suffix
    /// (`wal_admission_tests`), never here on the whole received range.
    #[test]
    fn only_proposals_are_bounded_by_the_shard() {
        assert_eq!(growth_bound(&append(&[])), None);
        assert_eq!(growth_bound(&append(&[None, None])), None);
        assert_eq!(growth_bound(&append(&[None, Some(10)])), None);
        let propose = Event::Propose {
            operation: crate::identity::OperationId::new(1).unwrap(),
            bytes: vec![0; 10],
        };
        assert_eq!(growth_bound(&propose), Some(proposal_bound(10)));
        assert_eq!(
            proposal_bound(10),
            UPDATE_WAL_OVERHEAD + entry_wal_bound(10)
        );
        assert_eq!(NOOP_WAL_BOUND, UPDATE_WAL_OVERHEAD + entry_wal_bound(0));
        let mut account = WalAccount::default();
        assert!(!account.over(&propose), "no bounded WAL");
        account.refresh(Some(WalBudget {
            limit: proposal_bound(10) - 1,
            published: 0,
            reserve: 10_000,
        }));
        assert!(account.over(&propose), "data never uses the reserve");
        account.count_declines(2, 1);
        assert_eq!((account.declined, account.noop_refused), (2, 1));
    }
    /// R4-1: data room ends at the mark, noop room at the mark plus the
    /// reserve; staged and recently completed bytes count against both.
    #[test]
    fn noop_room_extends_past_the_mark_by_the_reserve_only() {
        let mut account = WalAccount::default();
        assert!(account.noop_fits(), "no bounded WAL: always room");
        assert_eq!(account.rooms(), None);
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
        assert_eq!(
            account.rooms(),
            Some(WalRoom {
                data: 0,
                noop: 1_000
            })
        );
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
