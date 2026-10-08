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
//! Host-authorized local configuration proposals with promotion readiness.
use super::*;
use crate::membership::{ConfigurationChange, ConfigurationRecord, MembershipError};

/// A readiness token and the host's current authenticated peer binding at
/// execution. The host must revalidate bindings if it queues an administrative
/// event across connection changes. This is not network-derived authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionReadiness {
    pub ready: ReadyLearner,
    pub authenticated: StoreBinding,
}
/// Explicit administrative input, never an ordinary application command.
/// Placement/failure-domain authorization and requirements come from the host's
/// validated administration plan. Readiness is required only for new voters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationProposal {
    pub record: ConfigurationRecord,
    pub readiness: Vec<PromotionReadiness>,
    pub requirements: ReadinessRequirements,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationProposalError {
    AuthenticationRequired,
    UnsupportedWireVersion(u16),
    MissingPeerTransport,
    Placement(crate::placement::PlacementError),
    Membership(MembershipError),
    MissingReadiness(NodeId),
    UnexpectedReadiness(NodeId),
    WrongRequirements(NodeId),
    Readiness(ReadinessError),
}
impl From<ConfigurationProposalError> for RaftError {
    fn from(error: ConfigurationProposalError) -> Self {
        Self::Configuration(Box::new(error))
    }
}
impl Raft {
    /// Local durable journal status. Pending proposals do not advance it;
    /// absence is not a linearizable cluster-wide negative result.
    pub fn configuration_status(
        &self,
        operation: OperationId,
    ) -> Result<crate::membership::ConfigurationOperationStatus, RaftError> {
        if self.is_fenced() {
            return Err(RaftError::Fenced);
        }
        self.durable
            .configuration_status(operation)
            .map_err(|e| ConfigurationProposalError::Membership(e).into())
    }
    pub(super) fn configure(
        &mut self,
        proposal: ConfigurationProposal,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader);
        }
        // Establish commitment in this leader's term before an administration
        // operation. Finalization still separately requires joint commitment.
        if self.durable.commit_index == 0
            || self.durable.term_at(self.durable.commit_index) != Some(self.durable.hard_state.term)
        {
            return Err(RaftError::ReadNotReady);
        }
        if proposal.readiness.len() > crate::quorum::Limits::default().max_voters {
            return Err(StorageError::Rejected("promotion proof budget").into());
        }
        let index = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        // Validate using the same journal grammar as persistence and recovery,
        // before inspecting proofs or changing core state.
        self.membership
            .validate_next(index, &proposal.record, self.durable.commit_index)
            .map_err(ConfigurationProposalError::Membership)?;
        let mut proofs = BTreeMap::new();
        for proof in &proposal.readiness {
            let node = proof.ready.request().learner.node;
            if proofs.insert(node, proof).is_some() {
                return Err(ConfigurationProposalError::UnexpectedReadiness(node).into());
            }
        }
        if let ConfigurationChange::Joint { next, .. } = &proposal.record.change {
            for (&node, &store) in next.voter_stores() {
                if self.membership.stable().voter_stores().contains_key(&node) {
                    continue;
                }
                let proof = proofs
                    .remove(&node)
                    .ok_or(ConfigurationProposalError::MissingReadiness(node))?;
                if proof.ready.request().learner.store != store {
                    return Err(ConfigurationProposalError::Readiness(
                        ReadinessError::WrongBinding,
                    )
                    .into());
                }
                if proof.ready.request().requirements != proposal.requirements {
                    return Err(ConfigurationProposalError::WrongRequirements(node).into());
                }
                self.check_learner_readiness(&proof.ready, proof.authenticated)
                    .map_err(ConfigurationProposalError::Readiness)?;
            }
        }
        if let Some((&node, _)) = proofs.first_key_value() {
            return Err(ConfigurationProposalError::UnexpectedReadiness(node).into());
        }
        let entry = LogEntry {
            index,
            term: self.durable.hard_state.term,
            payload: EntryPayload::Configuration(Box::new(proposal.record)),
        };
        if entry.payload_bytes() > self.limits.max_command_bytes
            || entry.retained_payload_bytes() > self.limits.max_batch_bytes
        {
            return Err(StorageError::Rejected("configuration byte budget").into());
        }
        self.persist(
            self.durable.hard_state,
            self.durable.commit_index,
            Some(Suffix {
                from: index,
                entries: vec![entry],
            }),
            After::LeaderAppend,
            None,
        )
    }
}
