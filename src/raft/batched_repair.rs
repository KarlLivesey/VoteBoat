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
//! Bounded pre-election learner repair, separate from leader replication.
use super::*;
use crate::membership::ConfigurationChange;

impl Raft {
    /// Opt in during assembly only after selecting a transport/codec supporting
    /// LearnerRepair (native wire format 5). Restart must repeat this selection.
    /// This grants no membership, vote or serving authority.
    pub fn with_batched_joint_repair(mut self) -> Self {
        self.batched_joint_repair = true;
        self.repair_requests.clear();
        self
    }
    pub(crate) fn learner_repair_wire_version(&self) -> Option<u16> {
        if self.committed_snapshot_repair {
            Some(7)
        } else if self.snapshot_joint_repair {
            Some(6)
        } else if self.batched_joint_repair {
            Some(5)
        } else {
            None
        }
    }

    pub(super) fn batched_joint_repair_messages(&mut self) -> Result<Vec<Effect>, RaftError> {
        if self.snapshot_joint_repair {
            if let Some(effects) = self.repair_snapshot_messages()? {
                return Ok(effects);
            }
        }
        let peers = self
            .membership()
            .replicas()
            .map(|(node, _)| node)
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for peer in peers {
            if let Some((joint, start, _)) = self.repair_joint(peer, None)? {
                let EntryPayload::Configuration(record) = &joint.payload else {
                    unreachable!()
                };
                let ConfigurationChange::Joint { id, .. } = record.change else {
                    unreachable!()
                };
                effects.extend(self.repair_batch(peer, start, id)?);
            }
        }
        Ok(effects)
    }

    // Historical repair retains the exact old-view voter/learner relationship.
    // A repair acknowledgement never supplies ballot or commit authority.
    fn repair_joint(
        &self,
        peer: NodeId,
        configuration: Option<ConfigurationId>,
    ) -> Result<Option<(LogEntry, u64, StoreIdentity)>, RaftError> {
        for entry in self.durable.entries.iter().rev() {
            let EntryPayload::Configuration(record) = &entry.payload else {
                continue;
            };
            let ConfigurationChange::Joint { id, next } = &record.change else {
                continue;
            };
            if configuration.is_some_and(|wanted| wanted != *id) {
                continue;
            }
            if let Some(active) = self.membership().joint() {
                if active.index != entry.index {
                    continue;
                }
            } else if entry.index > self.durable.commit_index
                || self.membership().voter_store(self.node) != Some(self.binding.identity)
            {
                continue;
            }
            let old = self
                .durable
                .membership_at(entry.index - 1)
                .map_err(|_| RaftError::InvalidRecovery)?;
            let Some(&store) = old.stable().learners().get(&peer) else {
                continue;
            };
            if old.joint().is_some()
                || old.stable().voter_stores().get(&self.node) != Some(&self.binding.identity)
                || next.voter_stores().get(&peer) != Some(&store)
                || self.membership().voter_store(peer) != Some(store)
            {
                continue;
            }
            let start = old
                .last_configuration_index()
                .max(self.durable.base_index())
                + 1;
            return Ok(Some((entry.clone(), start, store)));
        }
        Ok(None)
    }

    fn repair_batch(
        &mut self,
        peer: NodeId,
        start: u64,
        configuration: ConfigurationId,
    ) -> Result<Vec<Effect>, RaftError> {
        let Some((joint, _, _)) = self.repair_joint(peer, Some(configuration))? else {
            return Ok(Vec::new());
        };
        let previous_index = start - 1;
        let Some(previous_term) = self.durable.term_at(previous_index) else {
            return Ok(Vec::new());
        };
        let mut bytes = 256usize
            .saturating_add(joint.retained_payload_bytes())
            .saturating_add(37);
        let mut entries = Vec::new();
        for entry in self
            .durable
            .entries
            .iter()
            .skip((start - self.durable.base_index() - 1) as usize)
            .take(64)
        {
            if entry.index > joint.index {
                break;
            }
            let Some(total) = entry
                .retained_payload_bytes()
                .checked_add(37)
                .and_then(|n| bytes.checked_add(n))
                .filter(|n| *n <= self.limits.max_batch_bytes)
            else {
                break;
            };
            entries.push(entry.clone());
            bytes = total;
        }
        let Some(end) = entries.last().map(|e| e.index) else {
            return Ok(Vec::new());
        };
        let context = self.context()?;
        self.repair_requests.insert(
            peer,
            Replication {
                context,
                configuration,
                start,
                end,
                snapshot: None,
            },
        );
        Ok(vec![Effect::Send(self.scoped_message(
            configuration,
            peer,
            context,
            Rpc::LearnerRepair {
                joint: Box::new(joint),
                previous_index,
                previous_term,
                entries,
            },
        ))])
    }

    pub(super) fn receive_learner_repair(
        &mut self,
        mut message: Message,
    ) -> Result<Vec<Effect>, RaftError> {
        if let Rpc::LearnerRepaired {
            success,
            matching_index,
            matching_term,
        } = message.rpc
        {
            return self.receive_learner_repaired(message, success, matching_index, matching_term);
        }
        self.validate_learner_repair_request(&message)?;
        let Rpc::LearnerRepair {
            previous_index,
            previous_term,
            entries,
            ..
        } = &message.rpc
        else {
            unreachable!()
        };
        let mut previous_index = *previous_index;
        let mut previous_term = *previous_term;
        let mut entries = entries.clone();
        if previous_index < self.durable.base_index()
            && entries.last().unwrap().index >= self.durable.base_index()
        {
            if !entries
                .iter()
                .any(|e| e.index == self.durable.base_index() && e.term == self.durable.base_term())
            {
                return Err(RaftError::InvalidMessage);
            }
            previous_index = self.durable.base_index();
            previous_term = self.durable.base_term();
            entries.retain(|e| e.index > previous_index);
        }
        message.rpc = Rpc::Append {
            previous_index,
            previous_term,
            entries,
            leader_commit: 0,
        };
        let mut effects = self.receive_inner(message)?;
        let convert = |reply: &mut Message, next: &GroupLog| {
            let (success, matching_index) = match reply.rpc {
                Rpc::Appended {
                    success,
                    matching_index,
                } => (success, matching_index),
                Rpc::Compacted { index, .. } => (false, index),
                _ => unreachable!(),
            };
            reply.rpc = Rpc::LearnerRepaired {
                success,
                matching_index,
                matching_term: next.term_at(matching_index).unwrap_or(0),
            };
        };
        if let Some(pending) = &mut self.pending {
            if let Some(reply) = &mut pending.reply {
                convert(reply, &pending.next);
            }
        }
        for effect in &mut effects {
            if let Effect::Send(reply) = effect {
                convert(reply, &self.durable);
            }
        }
        self.reset_election()?;
        Ok(effects)
    }
    fn receive_learner_repaired(
        &mut self,
        message: Message,
        success: bool,
        matching_index: u64,
        matching_term: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        let checkpoint = self
            .repair_requests
            .get(&message.from)
            .filter(|sent| sent.snapshot.is_some() && sent.snapshot == self.durable.snapshot)
            .and_then(|sent| {
                self.snapshot_repair_scope(message.from).filter(|scope| {
                    scope.configuration == sent.configuration
                        && scope.peer_store == message.sender.identity
                })
            });
        let historical = self
            .repair_requests
            .get(&message.from)
            .filter(|sent| sent.snapshot.is_none())
            .map(|sent| self.repair_joint(message.from, Some(sent.configuration)))
            .transpose()?
            .flatten();
        if message.to != self.node
            || message.from == self.node
            || message.group != self.durable.bootstrap.group
            || message.context.origin != self.binding
            || (self.membership().stable().learners().get(&message.from)
                != Some(&message.sender.identity)
                && historical.as_ref().map(|(_, _, store)| *store) != Some(message.sender.identity)
                && checkpoint.is_none())
        {
            return Err(RaftError::WrongIdentity);
        }
        if message.term > self.durable.hard_state.term {
            return self.receive_higher_repair_term(message, matching_index);
        }
        if self.role != Role::Candidate || message.term != self.durable.hard_state.term {
            return Ok(Vec::new());
        }
        let Some(sent) = self.repair_requests.get(&message.from).copied() else {
            return Ok(Vec::new());
        };
        if sent.context != message.context || sent.configuration != message.configuration {
            return Ok(Vec::new());
        }
        let joint_index = if sent.snapshot.is_some() {
            checkpoint.ok_or(RaftError::InvalidMessage)?.voting_index
        } else {
            historical.ok_or(RaftError::InvalidMessage)?.0.index
        };
        if (success && matching_index != sent.end)
            || matching_index >= joint_index && !success
            || self.durable.term_at(matching_index) != Some(matching_term)
        {
            return Err(RaftError::InvalidMessage);
        }
        self.repair_requests.remove(&message.from);
        if success && matching_index >= joint_index {
            let Some(context) = self.vote_context else {
                return Ok(Vec::new());
            };
            return Ok(vec![Effect::Send(self.message(
                message.from,
                context,
                Rpc::Vote {
                    last_index: self.durable.last_index(),
                    last_term: self.durable.last_term(),
                },
            ))]);
        }
        if matching_index < sent.start {
            return Ok(Vec::new());
        }
        self.repair_batch(message.from, matching_index + 1, sent.configuration)
    }
    fn receive_higher_repair_term(
        &mut self,
        mut message: Message,
        matching_index: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        if message.configuration != self.membership().id() {
            // A historical reply cannot pass the ordinary current-head
            // response gate. Observe only its exact admitted context,
            // without importing its old configuration or prefix claim.
            if !self.repair_requests.get(&message.from).is_some_and(|sent| {
                sent.context == message.context && sent.configuration == message.configuration
            }) {
                return Ok(Vec::new());
            }
            self.clear_reads();
            self.role = Role::Follower;
            self.vote_context = None;
            self.requests.clear();
            self.repair_requests.clear();
            return self.persist(
                HardState {
                    term: message.term,
                    voted_for: None,
                },
                self.durable.commit_index,
                None,
                After::Reply,
                None,
            );
        }
        message.rpc = Rpc::Appended {
            success: false,
            matching_index,
        };
        self.receive_inner(message)
    }
    fn validate_learner_repair_request(&self, message: &Message) -> Result<(), RaftError> {
        let Rpc::LearnerRepair {
            joint,
            previous_index,
            previous_term,
            entries,
        } = &message.rpc
        else {
            unreachable!()
        };
        let EntryPayload::Configuration(record) = &joint.payload else {
            return Err(RaftError::InvalidMessage);
        };
        let ConfigurationChange::Joint { id, next } = &record.change else {
            return Err(RaftError::InvalidMessage);
        };
        let committed = self
            .durable
            .membership_at(self.durable.commit_index)
            .map_err(|_| RaftError::InvalidMessage)?;
        if message.to != self.node
            || message.from == self.node
            || message.group != self.durable.bootstrap.group
            || message.context.origin != message.sender
            || message.context.sequence == 0
            || message.term == 0
            || message.term < self.durable.hard_state.term
            || committed != self.membership
            || committed.joint().is_some()
            || committed.stable().learners().get(&self.node) != Some(&self.binding.identity)
            || committed.voter_store(message.from) != Some(message.sender.identity)
            || next.voter_stores().get(&self.node) != Some(&self.binding.identity)
            || message.configuration != *id
            || joint.term == 0
            || joint.term > message.term
            || joint.payload_bytes() > self.limits.max_command_bytes
            || self
                .membership()
                .validate_next(joint.index, record, self.durable.commit_index)
                .is_err()
            || entries.is_empty()
            || entries.len() > 64
            || (*previous_index == 0) != (*previous_term == 0)
            || *previous_term > message.term
        {
            return Err(RaftError::InvalidMessage);
        }
        self.validate_learner_repair_entries(
            message.term,
            joint,
            *previous_index,
            *previous_term,
            entries,
        )?;
        Ok(())
    }
    fn validate_learner_repair_entries(
        &self,
        message_term: u64,
        joint: &LogEntry,
        previous_index: u64,
        previous_term: u64,
        entries: &[LogEntry],
    ) -> Result<(), RaftError> {
        let mut bytes = 256usize
            .saturating_add(joint.retained_payload_bytes())
            .saturating_add(37);
        let mut term = previous_term;
        for (offset, entry) in entries.iter().enumerate() {
            if previous_index.checked_add(offset as u64 + 1) != Some(entry.index)
                || entry.index > joint.index
                || entry.term == 0
                || entry.term < term
                || entry.term > message_term
                || entry.payload_bytes() > self.limits.max_command_bytes
                || (matches!(entry.payload, EntryPayload::Configuration(_))
                    && entry != joint)
                || (entry.index == joint.index && entry != joint)
                // Only an exact committed stable learner reaches this path.
                // A different-term uncommitted command suffix may be replaced
                // through ordinary atomic suffix persistence. Committed entries
                // and same-term payload forks remain immutable/rejected.
                || self.durable.entry_at(entry.index).is_some_and(|local| {
                    local != entry
                        && (entry.index <= self.durable.commit_index || local.term == entry.term)
                })
            {
                return Err(RaftError::InvalidMessage);
            }
            term = entry.term;
            bytes = entry
                .retained_payload_bytes()
                .checked_add(37)
                .and_then(|n| bytes.checked_add(n))
                .ok_or(RaftError::InvalidMessage)?;
            if bytes > self.limits.max_batch_bytes {
                return Err(RaftError::InvalidMessage);
            }
        }
        Ok(())
    }
}
