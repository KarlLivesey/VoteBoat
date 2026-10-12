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
//! Exact original Node administration, without synthetic positive outcomes.
use super::*;
use crate::raft::{LearnerReadinessRequest, PromotionReadiness, ReadinessRequirements};
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        L: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        N: PeerNetworkConfiguration,
    > LogicalActivationBackingChild
    for NetworkNode<S, T, E, LogicalApplication<L, V, B>, W, O, H, N>
where
    N::Endpoint: Clone,
{
    fn configuration_requests_are_drained(&self) -> bool {
        NetworkNode::configuration_requests_are_drained(self)
    }
    fn export_checkpoint(
        &mut self,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        NetworkNode::export_checkpoint(self, group)
    }
    fn poll_checkpoint_export(&mut self) -> Option<SnapshotExportCompletion> {
        NetworkNode::poll_checkpoint_export(self)
    }
    fn complete_checkpoint_export(
        &mut self,
        output: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        NetworkNode::complete_checkpoint_export(self, output)
    }
    fn cancel_checkpoint_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        NetworkNode::cancel_checkpoint_export(self, ticket)
    }
    fn configure_activation(
        &mut self,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected> {
        NetworkNode::configure(self, request)
    }
    fn cancel_activation_read(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<(), ReadInvocationError> {
        NetworkNode::cancel_read(self, ticket)
    }
    fn poll_activation_configuration(&mut self) -> Option<ConfigurationCompletion> {
        NetworkNode::poll_configuration(self)
    }
    fn cancel_activation_configuration(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        NetworkNode::cancel_configuration(self, ticket)
    }
    fn request_activation_readiness(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: ReadinessRequirements,
    ) -> Result<AdmissionTicket, LogicalBackingPollError> {
        NetworkNode::request_learner_readiness_tracked(self, group, learner, requirements)
            .map_err(|e| LogicalBackingPollError::Node(Box::new(e)))
    }
    fn cancel_activation_readiness(
        &mut self,
        group: GroupIdentity,
    ) -> Result<AdmissionTicket, LogicalBackingPollError> {
        NetworkNode::cancel_learner_readiness_tracked(self, group)
            .map_err(|e| LogicalBackingPollError::Node(Box::new(e)))
    }
    fn activation_readiness(
        &self,
        group: GroupIdentity,
        learner: NodeId,
    ) -> Option<PromotionReadiness> {
        let core = NetworkNode::local(self).owner.core(group)?;
        let ready = core.ready_learner()?;
        let source = NetworkNode::peers(self)?.source_view(learner)?;
        if source.source().node != learner {
            return None;
        }
        let binding = source.source().store;
        if ready.request().learner.node != learner || ready.binding() != binding {
            return None;
        }
        Some(PromotionReadiness {
            ready: ready.clone(),
            authenticated: binding,
        })
    }
    fn pending_activation_readiness(
        &self,
        group: GroupIdentity,
    ) -> Option<LearnerReadinessRequest> {
        NetworkNode::local(self)
            .owner
            .core(group)?
            .pending_learner_readiness()
    }
    fn activation_peer_binding(&self, learner: NodeId) -> Option<StoreBinding> {
        let source = NetworkNode::peers(self)?.source_view(learner)?;
        (source.source().node == learner).then_some(source.source().store)
    }
    fn poll_activation(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<Vec<OwnerStep>, LogicalBackingPollError> {
        NetworkNode::poll_with_configuration_authorization(self, now, budget, |core, proposal| {
            authorization.authorize(core, proposal)
        })
        .map(|p| p.replica.map_or_else(Vec::new, |r| r.steps))
        .map_err(|e| LogicalBackingPollError::Node(Box::new(e)))
    }
}
