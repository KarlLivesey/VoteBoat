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
//! Historical committed checkpoints for pre-election learner recovery.
use super::*;

impl Raft {
    /// Select during assembly with a compatible codec/transport (native wire 6).
    /// Only local committed, pinned checkpoint images can escape this path.
    pub fn with_snapshot_joint_repair(mut self) -> Self {
        self.batched_joint_repair = true;
        self.snapshot_joint_repair = true;
        self.repair_requests.clear();
        self
    }

    pub(super) fn repair_snapshot_messages(&mut self) -> Result<Option<Vec<Effect>>, RaftError> {
        let Some(joint) = self.membership().joint() else {
            return Ok(None);
        };
        let Some(reference) = self.durable.snapshot else {
            return Ok(None);
        };
        let Some(base) = self.durable.snapshot_membership.as_deref() else {
            return Ok(None);
        };
        if self.membership().stable().voter_stores().get(&self.node) != Some(&self.binding.identity)
            || base.stable() != self.membership().stable()
            || base
                .joint()
                .is_some_and(|j| Some(j) != self.membership().joint())
            || reference.index > self.durable.commit_index
        {
            return Ok(None);
        }
        let peers = joint
            .next
            .voter_stores()
            .iter()
            .filter(|(node, store)| self.membership().stable().learners().get(node) == Some(store))
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for peer in peers {
            let context = self.context()?;
            self.repair_requests.insert(
                peer,
                Replication {
                    context,
                    configuration: self.membership().id(),
                    start: 0,
                    end: reference.index,
                    snapshot: Some(reference),
                },
            );
            effects.push(Effect::SnapshotRequired {
                to: peer,
                context,
                reference,
            });
        }
        Ok(Some(effects))
    }

    pub(super) fn repair_snapshot_send(
        &self,
        to: NodeId,
        context: RequestContext,
        reference: SnapshotRef,
        snapshot: Snapshot,
    ) -> Result<Vec<Effect>, RaftError> {
        let Some(sent) = self.repair_requests.get(&to) else {
            return Err(RaftError::WrongCompletion);
        };
        if sent.context != context
            || sent.snapshot != Some(reference)
            || sent.configuration != self.membership().id()
            || self.durable.snapshot != Some(reference)
            || reference.index > self.durable.commit_index
            || !reference.matches(&snapshot)
            || snapshot.metadata.bootstrap != self.durable.bootstrap
            || snapshot.metadata.membership != self.durable.snapshot_membership
            || snapshot.application.len() > self.limits.max_snapshot_bytes
        {
            return Err(RaftError::WrongCompletion);
        }
        Ok(vec![Effect::Send(self.scoped_message(
            sent.configuration,
            to,
            context,
            Rpc::LearnerRepairSnapshot {
                snapshot: Box::new(snapshot),
            },
        ))])
    }

    pub(super) fn receive_repair_snapshot(
        &mut self,
        mut message: Message,
    ) -> Result<Vec<Effect>, RaftError> {
        let Rpc::LearnerRepairSnapshot { snapshot } = &message.rpc else {
            unreachable!()
        };
        let meta = &snapshot.metadata;
        let Some(incoming) = meta.membership.as_deref() else {
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
            || meta.bootstrap != self.durable.bootstrap
            || meta.validate().is_err()
            || incoming.stable() != committed.stable()
            || !incoming.operations().is_superset(committed.operations())
            || snapshot.application.is_empty()
            || snapshot.application.len() > self.limits.max_snapshot_bytes
            || meta.term > message.term
            || (meta.index <= self.durable.commit_index && incoming != &committed)
        {
            return Err(RaftError::InvalidMessage);
        }
        if let Some(joint) = incoming.joint() {
            if joint.id != message.configuration
                || joint.next.voter_stores().get(&self.node) != Some(&self.binding.identity)
            {
                return Err(RaftError::InvalidMessage);
            }
        } else if incoming != &committed || message.configuration <= incoming.id() {
            return Err(RaftError::InvalidMessage);
        }
        let index = meta.index;
        let term = meta.term;
        message.rpc = Rpc::Snapshot {
            snapshot: snapshot.clone(),
        };
        let mut effects = self.receive_inner(message)?;
        if self.staged_snapshot.is_some() {
            self.staged_snapshot_repair = true;
        } else {
            let convert = |reply: &mut Message| {
                reply.rpc = Rpc::LearnerRepaired {
                    success: true,
                    matching_index: index,
                    matching_term: term,
                };
            };
            if let Some(pending) = &mut self.pending {
                if let Some(reply) = &mut pending.reply {
                    convert(reply);
                }
            }
            for effect in &mut effects {
                if let Effect::Send(reply) = effect {
                    convert(reply);
                }
            }
        }
        self.reset_election()?;
        Ok(effects)
    }
}
