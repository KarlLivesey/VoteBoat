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

pub(super) struct CheckpointRepairScope {
    pub(super) configuration: ConfigurationId,
    pub(super) final_checkpoint: bool,
    pub(super) voting_index: u64,
    pub(super) peer_store: StoreIdentity,
}

impl Raft {
    /// Explicit wire7 capability. Only a receiver's already trusted old-view
    /// voter may supply a locally committed stable checkpoint after promotion.
    pub fn with_committed_snapshot_repair(mut self) -> Self {
        self = self.with_snapshot_joint_repair();
        self.committed_snapshot_repair = true;
        self
    }
    /// Select during assembly with a compatible codec/transport (native wire 6).
    /// Only local committed, pinned checkpoint images can escape this path.
    pub fn with_snapshot_joint_repair(mut self) -> Self {
        self.batched_joint_repair = true;
        self.snapshot_joint_repair = true;
        self.committed_snapshot_repair = false;
        self.repair_requests.clear();
        self
    }

    // One provenance calculation for admission, image completion and replies.
    // A historical joint checkpoint never certifies an accepted final entry.
    pub(super) fn snapshot_repair_scope(&self, peer: NodeId) -> Option<CheckpointRepairScope> {
        let reference = self.durable.snapshot?;
        let base = self.durable.snapshot_membership.as_deref()?;
        if peer == self.node || reference.index > self.durable.commit_index {
            return None;
        }
        if self.committed_snapshot_repair && self.membership().joint().is_none() {
            let peer_store = self.membership().voter_store(peer)?;
            if self.membership().voter_store(self.node) != Some(self.binding.identity) {
                return None;
            }
            if base == self.membership() {
                return Some(CheckpointRepairScope {
                    configuration: base.id(),
                    final_checkpoint: true,
                    voting_index: reference.index,
                    peer_store,
                });
            }
            let joint = base.joint()?;
            if &joint.next != self.membership().stable()
                || base.stable().voter_stores().get(&self.node) != Some(&self.binding.identity)
                || base.stable().learners().get(&peer) != Some(&peer_store)
                || joint.next.voter_stores().get(&peer) != Some(&peer_store)
                || joint.index > reference.index
            {
                return None;
            }
            return Some(CheckpointRepairScope {
                configuration: joint.id,
                final_checkpoint: false,
                voting_index: joint.index,
                peer_store,
            });
        }
        let joint = self.membership().joint()?;
        let peer_store = *self.membership().stable().learners().get(&peer)?;
        if self.membership().stable().voter_stores().get(&self.node) != Some(&self.binding.identity)
            || base.stable() != self.membership().stable()
            || base.joint().is_some_and(|j| j != joint)
            || joint.next.voter_stores().get(&peer) != Some(&peer_store)
        {
            return None;
        }
        Some(CheckpointRepairScope {
            configuration: joint.id,
            final_checkpoint: false,
            voting_index: joint.index,
            peer_store,
        })
    }

    pub(super) fn repair_snapshot_messages(&mut self) -> Result<Option<Vec<Effect>>, RaftError> {
        let Some(reference) = self.durable.snapshot else {
            return Ok(None);
        };
        let peers = self
            .membership()
            .replicas()
            .map(|(node, _)| node)
            .collect::<Vec<_>>();
        let mut effects = Vec::new();
        for peer in peers {
            let Some(scope) = self.snapshot_repair_scope(peer) else {
                continue;
            };
            let context = self.context()?;
            self.repair_requests.insert(
                peer,
                Replication {
                    context,
                    configuration: scope.configuration,
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
        Ok((!effects.is_empty()).then_some(effects))
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
        let scope = self
            .snapshot_repair_scope(to)
            .ok_or(RaftError::WrongCompletion)?;
        if sent.context != context
            || sent.snapshot != Some(reference)
            || sent.configuration != scope.configuration
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
            if scope.final_checkpoint {
                Rpc::CommittedLearnerRepairSnapshot {
                    snapshot: Box::new(snapshot),
                }
            } else {
                Rpc::LearnerRepairSnapshot {
                    snapshot: Box::new(snapshot),
                }
            },
        ))])
    }

    pub(super) fn receive_repair_snapshot(
        &mut self,
        mut message: Message,
    ) -> Result<Vec<Effect>, RaftError> {
        let (snapshot, final_checkpoint) = match &message.rpc {
            Rpc::LearnerRepairSnapshot { snapshot } => (snapshot, false),
            Rpc::CommittedLearnerRepairSnapshot { snapshot } if self.committed_snapshot_repair => {
                (snapshot, true)
            }
            _ => return Err(RaftError::InvalidMessage),
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
            || (!final_checkpoint && incoming.stable() != committed.stable())
            || !incoming.operations().is_superset(committed.operations())
            || snapshot.application.is_empty()
            || snapshot.application.len() > self.limits.max_snapshot_bytes
            || meta.term > message.term
            || (meta.index <= self.durable.commit_index && incoming != &committed)
        {
            return Err(RaftError::InvalidMessage);
        }
        if final_checkpoint {
            if incoming.joint().is_some()
                || incoming.id() != message.configuration
                || incoming.id() <= committed.id()
                || incoming.last_configuration_index() <= committed.last_configuration_index()
                || meta.index <= self.durable.commit_index
                || incoming.voter_store(self.node) != Some(self.binding.identity)
                || incoming.voter_store(message.from) != Some(message.sender.identity)
            {
                return Err(RaftError::InvalidMessage);
            }
        } else if let Some(joint) = incoming.joint() {
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
