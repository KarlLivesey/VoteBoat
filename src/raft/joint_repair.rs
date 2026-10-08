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
//! Append-only configuration repair to a committed learner before election.
use super::*;
use crate::membership::ConfigurationChange;

impl Raft {
    pub(super) fn joint_repair_messages(&mut self) -> Result<Vec<Effect>, RaftError> {
        if self.membership().stable().voter_stores().get(&self.node) != Some(&self.binding.identity)
        {
            return Ok(Vec::new());
        }
        let Some(joint) = self.membership().joint() else {
            return Ok(Vec::new());
        };
        if joint.index <= self.durable.commit_index {
            return Ok(Vec::new());
        }
        let Some(entry) = self.durable.entry_at(joint.index).cloned() else {
            return Ok(Vec::new());
        };
        let previous_index = joint.index - 1;
        let Some(previous_term) = self.durable.term_at(previous_index) else {
            return Ok(Vec::new());
        };
        let targets = joint
            .next
            .voter_stores()
            .iter()
            .filter(|(node, store)| self.membership().stable().learners().get(node) == Some(store))
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        let mut effects = Vec::with_capacity(targets.len());
        for target in targets {
            let context = self.context()?;
            effects.push(Effect::Send(self.message(
                target,
                context,
                Rpc::Append {
                    previous_index,
                    previous_term,
                    entries: vec![entry.clone()],
                    // No commitment assertion or application progress from a candidate.
                    leader_commit: 0,
                },
            )));
        }
        Ok(effects)
    }

    pub(super) fn permits_joint_repair(&self, message: &Message) -> bool {
        let Rpc::Append {
            previous_index,
            previous_term,
            entries,
            leader_commit: 0,
        } = &message.rpc
        else {
            return false;
        };
        let [entry] = entries.as_slice() else {
            return false;
        };
        let EntryPayload::Configuration(record) = &entry.payload else {
            return false;
        };
        let ConfigurationChange::Joint { id, next } = &record.change else {
            return false;
        };
        let Ok(committed) = self.durable.membership_at(self.durable.commit_index) else {
            return false;
        };
        // Restrict to an exact committed learner, with no already accepted joint
        // or other configuration work. Pure extension cannot erase a voting log.
        message.to == self.node
            && message.from != self.node
            && message.group == self.durable.bootstrap.group
            && message.context.origin == message.sender
            && message.context.sequence != 0
            && message.term != 0
            && message.term >= self.durable.hard_state.term
            && self.membership().joint().is_none()
            && committed.joint().is_none()
            && committed == self.membership
            && committed.stable().learners().get(&self.node) == Some(&self.binding.identity)
            && committed.voter_store(message.from) == Some(message.sender.identity)
            && next.voter_stores().get(&self.node) == Some(&self.binding.identity)
            && message.configuration == *id
            && *previous_index == self.durable.last_index()
            && *previous_term == self.durable.last_term()
            && previous_index.checked_add(1) == Some(entry.index)
            && entry.term != 0
            && entry.term >= *previous_term
            && entry.term <= message.term
            && entry.payload_bytes() <= self.limits.max_command_bytes
            && entry.retained_payload_bytes() <= self.limits.max_batch_bytes
            && self
                .membership()
                .validate_next(entry.index, record, self.durable.commit_index)
                .is_ok()
    }
}
