// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! One admitted ordinary append overlaps its original local durability barrier.
use super::*;
use std::mem::size_of;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AdmittedRange {
    ticket: LogTicket,
    context: RequestContext,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EmptyAppendAlias {
    original: Replication,
    successor: RequestContext,
}
impl Raft {
    /// Additional bounded early-output reservation before exact append admission.
    /// None keeps the original serialized behavior. This is a resource bound,
    /// not durability, voting, application or response-release authority.
    pub fn admitted_replication_reservation(&self) -> Option<usize> {
        let suffix = self.early_suffix()?;
        let peers = self
            .membership()
            .replicas()
            .filter(|(peer, _)| self.early_peer(*peer))
            .count();
        if peers == 0 || self.request_sequence.checked_add(peers as u64).is_none() {
            return None;
        }
        let payload = suffix
            .entries
            .iter()
            .try_fold(0usize, |sum, entry| sum.checked_add(entry.payload_bytes()))?;
        let message = suffix
            .entries
            .len()
            .checked_mul(size_of::<LogEntry>())?
            .checked_add(payload)?
            .checked_add(size_of::<Message>())?
            .checked_add(4 * size_of::<Effect>())?
            .checked_add(size_of::<AdmittedRange>() + size_of::<EmptyAppendAlias>())?;
        peers.checked_mul(message)
    }
    /// Bind the unchanged exact append ticket, then optionally emit owned ordinary
    /// replication frames before its local barrier. The host must reserve the
    /// additional inspected output alongside the original durable continuation.
    /// Local matching/commit/application and all reply processing remain gated by
    /// the exact durable completion. Public `admitted` retains its original path.
    pub fn replicate_admitted_queue(
        &mut self,
        ticket: LogTicket,
    ) -> Result<EffectQueue, RaftError> {
        self.admitted(ticket)?;
        if self.admitted_replication_reservation().is_none() {
            return Ok(EffectQueue::new());
        }
        let peers: Vec<_> = self
            .membership()
            .replicas()
            .map(|(peer, _)| peer)
            .filter(|peer| self.early_peer(*peer))
            .collect();
        let mut effects = Vec::with_capacity(peers.len());
        for peer in peers {
            let suffix = self.early_suffix().ok_or(RaftError::WrongCompletion)?;
            let start = suffix.from;
            let entries = suffix.entries.clone();
            let end = entries.last().unwrap().index;
            let context = self.context()?;
            if let Some(original) = self.requests.get(&peer).copied() {
                self.empty_append_aliases.insert(
                    peer,
                    EmptyAppendAlias {
                        original,
                        successor: context,
                    },
                );
            }
            self.requests.insert(
                peer,
                Replication {
                    context,
                    configuration: self.membership().id(),
                    start,
                    end,
                    snapshot: None,
                    commit_at_start: self.durable.commit_index,
                },
            );
            self.admitted_ranges
                .insert(peer, AdmittedRange { ticket, context });
            effects.push(Effect::Send(self.message(
                peer,
                context,
                Rpc::Append {
                    previous_index: start - 1,
                    previous_term: self.durable.last_term(),
                    entries,
                    leader_commit: self.durable.commit_index,
                },
            )));
        }
        Ok(effects.into())
    }
    fn early_suffix(&self) -> Option<&Suffix> {
        let pending = self.pending.as_ref()?;
        let next = pending.next.metadata();
        if self.fenced
            || self.role != Role::Leader
            || !self.local_voter()
            || self.transfer.is_some()
            || !self.durable.summary().is_plain()
            || !pending.next.is_append()
            || pending.membership.is_some()
            || !matches!(pending.after, After::LeaderAppend)
            || self.durable.last_index() != self.durable.commit_index
            || self.durable.term_at(self.durable.commit_index) != Some(self.durable.hard_state.term)
            || next.hard_state != self.durable.hard_state
            || next.ballot_origin != self.durable.ballot_origin
            || next.generation != self.durable.generation
            || next.commit_index != self.durable.commit_index
        {
            return None;
        }
        let suffix = pending.next.suffix()?;
        if self.durable.last_index().checked_add(1) != Some(suffix.from)
            || suffix.entries.is_empty()
            || suffix.entries.len() > self.batch_limits.max_replication_entries
            || suffix
                .entries
                .iter()
                .any(|entry| !matches!(entry.payload, EntryPayload::Command { .. }))
        {
            return None;
        }
        // Match the existing ordinary append RPC envelope; this does not change
        // command admission or replace selected-provider exact frame validation.
        let bytes = suffix.entries.iter().try_fold(256usize, |sum, entry| {
            sum.checked_add(37)?.checked_add(entry.payload_bytes())
        })?;
        (bytes <= self.limits.max_batch_bytes).then_some(suffix)
    }
    fn early_peer(&self, peer: NodeId) -> bool {
        let tip = self.durable.last_index();
        if peer == self.node
            || self.progress.get(&peer).is_none_or(|index| *index < tip)
            || tip.checked_add(1) != self.next_index.get(&peer).copied()
            || self.admitted_ranges.contains_key(&peer)
            || self.empty_append_aliases.contains_key(&peer)
        {
            return false;
        }
        self.requests.get(&peer).is_none_or(|old| {
            old.snapshot.is_none()
                && old.start == tip + 1
                && old.end == tip
                && old.configuration == self.membership().id()
                && old.context.origin == self.binding
                && old.context.sequence != 0
        })
    }
    #[cfg(feature = "native")]
    pub(super) fn original_replication(
        &self,
        peer: NodeId,
        context: RequestContext,
        configuration: ConfigurationId,
    ) -> Option<Replication> {
        let primary = *self.requests.get(&peer)?;
        if primary.context == context && primary.configuration == configuration {
            return Some(primary);
        }
        let alias = self.empty_append_aliases.get(&peer)?;
        (alias.successor == primary.context
            && alias.original.context == context
            && alias.original.configuration == configuration
            && primary.configuration == configuration)
            .then_some(alias.original)
    }
    #[cfg(feature = "native")]
    pub(super) fn admitted_entry(
        &self,
        peer: NodeId,
        sent: &Replication,
        index: u64,
    ) -> Option<&LogEntry> {
        let range = self.admitted_ranges.get(&peer)?;
        let pending = self.pending.as_ref()?;
        let metadata = pending.next.metadata();
        if range.context != sent.context
            || pending.ticket != Some(range.ticket)
            || metadata.group != range.ticket.group
            || metadata.revision != range.ticket.revision
            || metadata.generation != range.ticket.generation
            || metadata.last_index != range.ticket.last_index
            || metadata.hard_state.term != range.ticket.term
        {
            return None;
        }
        pending.next.entry_at(index)
    }
    pub(super) fn is_empty_alias(&self, message: &Message) -> bool {
        self.empty_append_aliases
            .get(&message.from)
            .is_some_and(|alias| {
                alias.original.context == message.context
                    && alias.original.configuration == message.configuration
                    && self
                        .requests
                        .get(&message.from)
                        .is_some_and(|primary| primary.context == alias.successor)
            })
    }
    pub(super) fn consume_empty_alias(
        &mut self,
        message: &Message,
        success: bool,
        matching: u64,
    ) -> Result<EffectQueue, RaftError> {
        let alias = self.empty_append_aliases[&message.from];
        if success && matching != alias.original.end {
            return Err(RaftError::InvalidMessage);
        }
        self.empty_append_aliases.remove(&message.from);
        Ok(EffectQueue::new())
    }
    pub(super) fn retire_replication(&mut self, peer: NodeId) {
        self.requests.remove(&peer);
        self.admitted_ranges.remove(&peer);
        self.empty_append_aliases.remove(&peer);
    }
    pub(super) fn clear_replication_requests(&mut self) {
        self.requests.clear();
        self.admitted_ranges.clear();
        self.empty_append_aliases.clear();
    }
    pub(super) fn broadcast_after_admission_into(
        &mut self,
        out: &mut EffectQueue,
    ) -> Result<(), RaftError> {
        self.refresh_peer_order();
        for at in 0..self.peer_order.len() {
            let peer = self.peer_order[at];
            if self.admitted_ranges.get(&peer).is_some_and(|range| {
                range.ticket.revision == self.durable.revision
                    && self
                        .requests
                        .get(&peer)
                        .is_some_and(|sent| sent.context == range.context)
            }) {
                continue;
            }
            out.push_back(self.append_for(peer)?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl Raft {
    /// Explicit legacy owned-output convenience boundary.
    pub fn replicate_admitted(&mut self, ticket: LogTicket) -> Result<Vec<Effect>, RaftError> {
        self.replicate_admitted_queue(ticket)
            .map(EffectQueue::into_owned)
    }
}
