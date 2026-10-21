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
//! Checked ordinary peer messages and their state transitions.
use super::*;

impl Raft {
    pub(super) fn receive_inner(&mut self, m: Message) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.receive_inner_into(m, &mut effects)?;
        Ok(effects.into())
    }
    /// Appends this message's effects to `out`; the hot replication arms
    /// write in place, colder ones append their owned effects.
    pub(super) fn receive_inner_into(
        &mut self,
        m: Message,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        if matches!(
            m.rpc,
            Rpc::LearnerReadinessRequest(_) | Rpc::LearnerReadinessReply { .. }
        ) {
            return out.append_result(self.receive_readiness(m));
        }
        if matches!(
            m.rpc,
            Rpc::AuthorityRequest { .. } | Rpc::AuthorityReply { .. }
        ) {
            return out.append_result(self.receive_authority(m));
        }
        if let Some(index) = self.retirement_commit(&m) {
            let reply = self.reply(
                &m,
                Rpc::Appended {
                    success: true,
                    matching_index: index,
                },
            );
            return if index > self.durable.commit_index {
                self.persist_into(
                    self.durable.hard_state,
                    index,
                    None,
                    After::Reply,
                    Some(reply),
                    out,
                )
            } else {
                out.push_back(Effect::Send(reply));
                Ok(())
            };
        }
        self.authenticated_message_source(&m)?;
        if let Rpc::TimeoutNow {
            index, log_term, ..
        } = m.rpc
        {
            return out.append_result(self.receive_timeout_now(&m, index, log_term));
        }
        let old = self.durable.hard_state;
        let hard = if m.term > old.term {
            HardState {
                term: m.term,
                voted_for: None,
            }
        } else {
            old
        };
        if m.term > old.term {
            self.clear_reads();
            self.role = Role::Follower;
            self.vote_context = None;
            self.clear_replication_requests();
            self.repair_requests.clear();
        }
        let effects = match &m.rpc {
            Rpc::TimeoutNow { .. } => unreachable!(),
            Rpc::LearnerRepair { .. }
            | Rpc::LearnerRepaired { .. }
            | Rpc::LearnerRepairSnapshot { .. }
            | Rpc::CommittedLearnerRepairSnapshot { .. } => unreachable!(),
            Rpc::AuthorityRequest { .. }
            | Rpc::AuthorityReply { .. }
            | Rpc::LearnerReadinessRequest(_)
            | Rpc::LearnerReadinessReply { .. } => unreachable!(),
            Rpc::Vote {
                last_index,
                last_term,
            } => self.receive_vote(&m, hard, *last_index, *last_term),
            Rpc::Voted { granted } => self.receive_voted(&m, hard, *granted),
            Rpc::Append { .. } => return self.receive_append_into(m, hard, out),
            Rpc::Appended {
                success,
                matching_index,
            } => return self.receive_appended_into(&m, hard, *success, *matching_index, out),
            Rpc::ReadProbe => self.receive_read_probe(&m, hard),
            Rpc::ReadAck => self.receive_read_ack(&m, hard),
            Rpc::Snapshot { .. } => self.receive_snapshot(m, hard),
            Rpc::SnapshotAck { index } => self.receive_snapshot_ack(&m, hard, *index),
            Rpc::Compacted { index, term } => self.receive_compacted(&m, hard, *index, *term),
        };
        out.append_result(effects)
    }
    pub(super) fn validate_replica_message(&self, m: &Message) -> Result<(), RaftError> {
        // Replication and vote requests from an exact locally authorized voter
        // may bridge differing accepted heads. Replication proves only its
        // checked matching prefix. A vote uses the local electorate and freshness;
        // the request scope is echoed, never installed as membership. Read
        // authority and every response require the local current scope, and
        // response handlers also match their admitted request.
        let replication_request = matches!(&m.rpc, Rpc::Append { .. } | Rpc::Snapshot { .. });
        let election_request = matches!(&m.rpc, Rpc::Vote { .. });
        let permitted = self.permitted_replication(m);
        if m.to != self.node
            || m.from == self.node
            || m.group != self.durable.bootstrap.group
            || (!replication_request
                && !election_request
                && m.configuration != self.membership().id())
            || (!permitted && self.membership().replica_store(m.from) != Some(m.sender.identity))
        {
            return Err(RaftError::WrongIdentity);
        }
        // Learners return replication evidence but never vote, lead, or
        // establish read authority. Replication replies remain non-voting.
        if !permitted
            && !self.membership().is_voter(m.from)
            && !matches!(
                m.rpc,
                Rpc::Appended { .. } | Rpc::SnapshotAck { .. } | Rpc::Compacted { .. }
            )
        {
            return Err(RaftError::WrongIdentity);
        }
        if m.term == 0 || m.context.sequence == 0 {
            return Err(RaftError::InvalidMessage);
        }
        let request = matches!(
            &m.rpc,
            Rpc::Vote { .. }
                | Rpc::Append { .. }
                | Rpc::ReadProbe
                | Rpc::Snapshot { .. }
                | Rpc::TimeoutNow { .. }
        );
        if request && m.context.origin != m.sender {
            return Err(RaftError::WrongIdentity);
        }
        if !request && m.context.origin != self.binding {
            return Err(RaftError::WrongIdentity);
        }
        if matches!(m.rpc, Rpc::ReadProbe) && !self.local_voter() {
            return Err(RaftError::WrongIdentity);
        }
        Ok(())
    }
    fn receive_vote(
        &mut self,
        m: &Message,
        hard: HardState,
        last_index: u64,
        last_term: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if last_term > m.term || ((last_index == 0) != (last_term == 0)) {
            return Err(RaftError::InvalidMessage);
        }
        let granted = self.local_voter()
            && self.membership().is_voter(m.from)
            && m.term == hard.term
            && (hard.voted_for.is_none()
                || (hard.voted_for == Some(m.from)
                    && self
                        .durable
                        .ballot_origin
                        .is_some_and(|origin| origin.candidate_store == m.sender.identity)))
            && (last_term, last_index) >= (self.durable.last_term(), self.durable.last_index());
        let next = HardState {
            voted_for: if granted {
                Some(m.from)
            } else {
                hard.voted_for
            },
            ..hard
        };
        let mut reply = self.reply(m, Rpc::Voted { granted });
        reply.term = next.term;
        if next != old {
            self.persist(
                next,
                self.durable.commit_index,
                None,
                After::Reply,
                Some(reply),
            )
        } else {
            Ok(vec![Effect::Send(reply)])
        }
    }
    fn receive_voted(
        &mut self,
        m: &Message,
        hard: HardState,
        granted: bool,
    ) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if hard != old {
            return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
        }
        if self.role != Role::Candidate
            || m.term != old.term
            || self.vote_context != Some(m.context)
        {
            return Ok(Vec::new());
        }
        if granted {
            self.votes.insert(m.from);
        }
        if self.membership().is_satisfied(&self.votes) {
            self.become_leader()
        } else {
            Ok(Vec::new())
        }
    }
    fn receive_append_into(
        &mut self,
        mut m: Message,
        hard: HardState,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let Rpc::Append {
            previous_index,
            previous_term,
            entries,
            leader_commit,
        } = &m.rpc
        else {
            unreachable!()
        };
        let (previous_index, previous_term, leader_commit) =
            (*previous_index, *previous_term, *leader_commit);
        let old = self.durable.hard_state;
        if previous_term > m.term
            || ((previous_index == 0) != (previous_term == 0))
            || entries.len() > BatchLimits::MAX_REPLICATION_ENTRIES
            || entries.payload_bytes() > self.limits.max_batch_bytes
        {
            return Err(RaftError::InvalidMessage);
        }
        if m.term < old.term {
            out.push_back(Effect::Send(self.reply(
                &m,
                Rpc::Appended {
                    success: false,
                    matching_index: self.durable.last_index(),
                },
            )));
            return Ok(());
        }
        self.role = Role::Follower;
        self.clear_reads();
        self.vote_context = None;
        self.clear_replication_requests();
        self.repair_requests.clear();
        if !entries.valid_append(
            previous_index,
            previous_term,
            m.term,
            m.configuration,
            self.limits.max_command_bytes,
        ) {
            return Err(RaftError::InvalidMessage);
        }
        if self.durable.term_at(previous_index) != Some(previous_term) {
            let mut reply = self.reply(
                &m,
                if previous_index < self.durable.base_index() {
                    Rpc::Compacted {
                        index: self.durable.base_index(),
                        term: self.durable.base_term(),
                    }
                } else {
                    Rpc::Appended {
                        success: false,
                        matching_index: self.durable.last_index(),
                    }
                },
            );
            reply.term = hard.term;
            return if hard != old {
                self.persist_into(
                    hard,
                    self.durable.commit_index,
                    None,
                    After::Reply,
                    Some(reply),
                    out,
                )
            } else {
                out.push_back(Effect::Send(reply));
                Ok(())
            };
        }
        let suffix_offset = self.first_new_entry(entries)?;
        let matching = entries.last().map_or(previous_index, |e| e.index);
        let commit = self.durable.commit_index.max(leader_commit.min(matching));
        let mut reply = self.reply(
            &m,
            Rpc::Appended {
                success: true,
                matching_index: matching,
            },
        );
        reply.term = hard.term;
        // All checks and reply metadata precede consuming the original input.
        let suffix = suffix_offset.map(|offset| {
            let Rpc::Append { entries, .. } = &mut m.rpc else {
                unreachable!()
            };
            Self::take_append_suffix(entries, offset)
        });
        if hard != old || suffix.is_some() || commit != self.durable.commit_index {
            self.persist_into(hard, commit, suffix, After::Reply, Some(reply), out)
        } else {
            out.push_back(Effect::Send(reply));
            Ok(())
        }
    }
    /// Offset of the first entry not already held. Same-term overlap is
    /// compared by content: a separately allocated batch with the same index
    /// and term must still be identical.
    #[inline]
    fn first_new_entry(&self, entries: &EntryBatch) -> Result<Option<usize>, RaftError> {
        // Usually the first entry is not held at all: the loop's first step.
        match entries.first() {
            None => return Ok(None),
            Some(first) if self.durable.entry_at(first.index).is_none() => return Ok(Some(0)),
            Some(_) => {}
        }
        self.first_new_overlapping(entries)
    }
    fn first_new_overlapping(&self, entries: &EntryBatch) -> Result<Option<usize>, RaftError> {
        let mut offset = 0;
        for slice in entries.slices() {
            for entry in slice {
                match self.durable.entry_at(entry.index) {
                    Some(old_entry) if old_entry.term == entry.term => {
                        if old_entry != entry {
                            return Err(RaftError::InvalidMessage);
                        }
                    }
                    _ => return Ok(Some(offset)),
                }
                offset += 1;
            }
        }
        Ok(None)
    }
    /// The new suffix shares the received backings; nothing is copied.
    fn take_append_suffix(entries: &mut EntryBatch, offset: usize) -> Suffix {
        let from = entries[offset].index;
        let entries = if offset == 0 {
            std::mem::take(entries)
        } else {
            entries.slice(offset..entries.len()).unwrap_or_default()
        };
        Suffix { from, entries }
    }

    fn receive_appended_into(
        &mut self,
        m: &Message,
        hard: HardState,
        success: bool,
        matching_index: u64,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let old = self.durable.hard_state;
        if hard != old {
            return self.persist_into(
                hard,
                self.durable.commit_index,
                None,
                After::Reply,
                None,
                out,
            );
        }
        if self.role != Role::Leader || m.term != old.term {
            return Ok(());
        }
        if self.is_empty_alias(m) {
            return out.append_result(self.consume_empty_alias(m, success, matching_index));
        }
        let Some(sent) = self.requests.get(&m.from).copied() else {
            return Ok(());
        };
        if sent.context != m.context || sent.configuration != m.configuration {
            return Ok(());
        }
        if sent.snapshot.is_some() {
            return Ok(());
        }
        self.retire_replication(m.from);
        if success {
            if matching_index != sent.end {
                return Err(RaftError::InvalidMessage);
            }
            let prefix = self.progress[&m.from].max(matching_index);
            self.progress.insert(m.from, prefix);
            self.next_index.insert(m.from, prefix + 1);
            self.maybe_commit_into(out)?;
            if self.pending.is_none()
                && (prefix < self.durable.last_index()
                    || (self.progress_sends_enabled()
                        && sent.commit_at_start < self.durable.commit_index))
            {
                out.push_back(self.append_for(m.from)?);
            }
            Ok(())
        } else {
            let next = self.next_index[&m.from]
                .saturating_sub(1)
                .min(matching_index.saturating_add(1))
                .max(1);
            self.next_index.insert(m.from, next);
            out.push_back(self.append_for(m.from)?);
            Ok(())
        }
    }
    fn receive_read_probe(
        &mut self,
        m: &Message,
        hard: HardState,
    ) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if m.term < old.term {
            // The higher-term response makes the requester step down;
            // its term cannot satisfy the request's read quorum.
            return Ok(vec![Effect::Send(self.reply(m, Rpc::ReadAck))]);
        }
        self.role = Role::Follower;
        self.clear_reads();
        self.vote_context = None;
        self.clear_replication_requests();
        self.repair_requests.clear();
        let mut reply = self.reply(m, Rpc::ReadAck);
        reply.term = hard.term;
        if hard != old {
            self.persist(
                hard,
                self.durable.commit_index,
                None,
                After::Reply,
                Some(reply),
            )
        } else {
            Ok(vec![Effect::Send(reply)])
        }
    }
    fn receive_read_ack(&mut self, m: &Message, hard: HardState) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if hard != old {
            return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
        }
        if self.role != Role::Leader || m.term != old.term {
            return Ok(Vec::new());
        }
        if let Some(read) = &mut self.read {
            if read.barrier.context == m.context {
                read.acknowledgements.insert(m.from);
            }
        }
        Ok(self.maybe_read_ready())
    }
    fn receive_snapshot(&mut self, m: Message, hard: HardState) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        let Rpc::Snapshot { snapshot } = &m.rpc else {
            unreachable!()
        };

        let meta = &snapshot.metadata;
        if snapshot.application.is_empty()
            || snapshot.application.len() > self.limits.max_snapshot_bytes
            || meta.bootstrap != self.durable.bootstrap
            || meta.validate().is_err()
            || meta.configuration() > m.configuration
            || meta.term > m.term
        {
            return Err(RaftError::InvalidMessage);
        }
        if m.term < old.term {
            return Ok(vec![Effect::Send(
                self.reply(&m, Rpc::SnapshotAck { index: 0 }),
            )]);
        }
        self.role = Role::Follower;
        self.clear_reads();
        self.vote_context = None;
        self.clear_replication_requests();
        self.repair_requests.clear();
        if meta.index <= self.durable.commit_index {
            if self
                .durable
                .term_at(meta.index)
                .is_some_and(|t| t != meta.term)
            {
                return Err(RaftError::InvalidMessage);
            }
            let mut reply = self.reply(&m, Rpc::SnapshotAck { index: meta.index });
            reply.term = hard.term;
            return if hard != old {
                self.persist(
                    hard,
                    self.durable.commit_index,
                    None,
                    After::Reply,
                    Some(reply),
                )
            } else {
                Ok(vec![Effect::Send(reply)])
            };
        }
        self.staged_snapshot = Some(m.clone());
        self.staged_snapshot_repair = false;
        Ok(vec![Effect::StageSnapshot(m)])
    }
    fn receive_snapshot_ack(
        &mut self,
        m: &Message,
        hard: HardState,
        index: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if hard != old {
            return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
        }
        if self.role != Role::Leader || m.term != old.term {
            return Ok(Vec::new());
        }
        let Some(sent) = self.requests.get(&m.from).copied() else {
            return Ok(Vec::new());
        };
        if sent.context != m.context
            || sent.configuration != m.configuration
            || sent.snapshot.is_none()
        {
            return Ok(Vec::new());
        }
        if index != sent.end {
            return Err(RaftError::InvalidMessage);
        }
        self.retire_replication(m.from);
        let prefix = self.progress[&m.from].max(index);
        self.progress.insert(m.from, prefix);
        self.next_index.insert(m.from, prefix + 1);
        let mut effects = self.maybe_commit()?;
        if self.pending.is_none() {
            effects.push(self.append_for(m.from)?);
        }
        Ok(effects)
    }
    fn receive_compacted(
        &mut self,
        m: &Message,
        hard: HardState,
        index: u64,
        term: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        let old = self.durable.hard_state;
        if hard != old {
            return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
        }
        if self.role != Role::Leader || m.term != old.term {
            return Ok(Vec::new());
        }
        if self.is_empty_alias(m) {
            if index == 0
                || term == 0
                || index > self.durable.last_index()
                || self
                    .durable
                    .term_at(index)
                    .is_some_and(|found| found != term)
            {
                return Err(RaftError::InvalidMessage);
            }
            self.empty_append_aliases.remove(&m.from);
            return Ok(Vec::new());
        }
        let Some(sent) = self.requests.get(&m.from) else {
            return Ok(Vec::new());
        };
        if sent.context != m.context
            || sent.configuration != m.configuration
            || sent.snapshot.is_some()
        {
            return Ok(Vec::new());
        }
        if index == 0
            || term == 0
            || index > self.durable.last_index()
            || self.durable.term_at(index).is_some_and(|t| t != term)
        {
            return Err(RaftError::InvalidMessage);
        }
        self.retire_replication(m.from);
        if index < self.durable.base_index() {
            self.next_index.insert(m.from, self.durable.base_index());
        } else {
            self.progress
                .insert(m.from, self.progress[&m.from].max(index));
            self.next_index.insert(m.from, index + 1);
        }
        let mut effects = self.maybe_commit()?;
        if self.pending.is_none() {
            effects.push(self.append_for(m.from)?);
        }
        Ok(effects)
    }
}
