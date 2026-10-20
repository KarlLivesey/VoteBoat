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
//! Quorum-first held follower Appends. The rule decides only whether a ready
//! voter's next Append is created now or at its release; the Append itself
//! always comes from `append_for`, so its content, request bookkeeping, acks and
//! the commit rule are unchanged. A held voter's commit-only notice waits too:
//! its next data Append or the heartbeat carries the commit.
use super::*;

/// Whether the leader may hold a ready follower's next data Append.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AppendHold {
    /// Every ready follower gets its next Append at once (the core default).
    #[default]
    Off,
    /// A ready, caught-up voter whose Append the current quorum does not need
    /// waits until a later leader batch is durable. The host must deliver
    /// [`Event::ReleaseHeldAppends`] within its own bound; `TimedShard` does so
    /// after `TimerConfig::append_hold_ms`. Heartbeats also release holds.
    QuorumFirst,
}

impl Raft {
    /// Select the hold policy. Existing holds stay until their next release.
    pub fn set_append_hold(&mut self, hold: AppendHold) {
        self.append_hold = hold;
    }
    pub fn append_hold(&self) -> AppendHold {
        self.append_hold
    }
    /// True while a leader holds at least one follower's next Append; the host
    /// must then deliver [`Event::ReleaseHeldAppends`] within its hold bound.
    pub fn has_held_appends(&self) -> bool {
        self.role == Role::Leader && !self.held.is_empty()
    }
    /// Most recent successful acknowledgement first; voters only.
    pub(super) fn note_ack(&mut self, peer: NodeId) {
        if let Some(i) = self.ack_order.iter().position(|p| *p == peer) {
            self.ack_order[..=i].rotate_right(1);
        }
    }
    /// Highest index a peer holds or has been sent in an ordinary request.
    fn reach(&self, peer: NodeId) -> u64 {
        let acked = self.progress.get(&peer).copied().unwrap_or(0);
        self.requests
            .get(&peer)
            .filter(|sent| sent.snapshot.is_none())
            .map_or(acked, |sent| acked.max(sent.end))
    }
    /// The latest entry already reaches a quorum without `peer`.
    fn covered_without(&self, peer: NodeId, last: u64) -> bool {
        let mut reached = BTreeSet::new();
        if self.progress.get(&self.node).is_some_and(|p| *p >= last) {
            reached.insert(self.node);
        }
        for q in &self.ack_order {
            if *q != peer && self.reach(*q) >= last {
                reached.insert(*q);
            }
        }
        self.membership().is_satisfied(&reached)
    }
    /// The decision point, asked before `append_for` creates an Append for a
    /// ready peer on the ordinary-progress paths only.
    pub(super) fn hold_append(&self, peer: NodeId) -> bool {
        if self.append_hold == AppendHold::Off
            || self.role != Role::Leader
            || self.requests.contains_key(&peer)
            || !self.membership().is_voter(peer)
            || !self.local_voter()
            || !self.progress_sends_enabled()
        {
            return false;
        }
        let last = self.durable.last_index();
        let Some(&next) = self.next_index.get(&peer) else {
            return false;
        };
        if next > last {
            // Only a newer commit to report: the next data Append or the
            // heartbeat carries it, unless it is the first commit covering the
            // latest configuration entry, which goes out at once.
            return !self.configuration_notice;
        }
        // A data Append (no snapshot), from a peer that holds everything before
        // the latest leader batch and that one Append brings to the tip.
        next > self.durable.base_index()
            && next <= last
            && next > self.batch_floor
            && last - next < self.batch_limits.max_replication_entries as u64
            && self.covered_without(peer, last)
    }
    /// Hold `peer`'s next Append until a later leader batch than `last`.
    pub(super) fn hold(&mut self, peer: NodeId) {
        self.held.insert(peer, self.durable.last_index());
    }
    /// A held peer that owes entries up to its mark is released by any later
    /// durable leader batch, so a held Append carries at most two batches. A
    /// held commit notice (the peer had every entry) is decided afresh.
    fn hold_due(&self, peer: NodeId, last: u64) -> bool {
        self.held.get(&peer).is_some_and(|mark| {
            *mark < last && self.next_index.get(&peer).is_some_and(|next| next <= mark)
        })
    }
    /// Ordinary-progress sends with holds on: due holds go first, so their
    /// Appends count toward the quorum; then voters by ack recency, then the
    /// remaining peers in membership order.
    pub(super) fn broadcast_held_peers_into(
        &mut self,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let last = self.durable.last_index();
        let mut order: Vec<NodeId> = self
            .held
            .keys()
            .copied()
            .filter(|peer| self.hold_due(*peer, last))
            .collect();
        for peer in self.ack_order.iter().chain(&self.peers()) {
            if !order.contains(peer) {
                order.push(*peer);
            }
        }
        for peer in order {
            // Snapshot requests retain their existing fallback. An ordinary
            // request owns its exact range until ACK or explicit invalidation.
            if self
                .requests
                .get(&peer)
                .is_some_and(|sent| sent.snapshot.is_none())
            {
                continue;
            }
            if !self.hold_due(peer, last) && self.hold_append(peer) {
                // A new hold, or a held commit notice that now owes entries.
                if self
                    .held
                    .get(&peer)
                    .is_none_or(|mark| self.next_index[&peer] > *mark)
                {
                    self.hold(peer);
                }
                continue;
            }
            out.push_back(self.append_for(peer)?);
        }
        Ok(())
    }
    /// `Event::ReleaseHeldAppends`: send every held Append now.
    pub(super) fn release_held_into(
        &mut self,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        if self.role != Role::Leader {
            self.held.clear();
            return Ok(());
        }
        let peers: Vec<NodeId> = self.held.keys().copied().collect();
        for peer in peers {
            out.push_back(self.append_for(peer)?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
