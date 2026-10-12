// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Genuine logical adapter: same original owner, streams and live sessions.
use super::*;
use crate::raft::{LearnerReadinessRequest, PromotionReadiness, ReadinessRequirements};
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalActivationBackingChild
    for LogicalNode<S, T, E, LogicalApplication<C, V, B>, U, C, V, B>
{
    fn configuration_requests_are_drained(&self) -> bool {
        LogicalNode::configuration_requests_are_drained(self)
    }
    fn export_checkpoint(
        &mut self,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        LogicalNode::export_checkpoint(self, group)
    }
    fn poll_checkpoint_export(&mut self) -> Option<SnapshotExportCompletion> {
        LogicalNode::poll_checkpoint_export(self)
    }
    fn complete_checkpoint_export(
        &mut self,
        output: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        LogicalNode::complete_checkpoint_export(self, output)
    }
    fn cancel_checkpoint_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        LogicalNode::cancel_checkpoint_export(self, ticket)
    }
    fn configure_activation(
        &mut self,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected> {
        LogicalNode::configure(self, request)
    }
    fn cancel_activation_read(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<(), ReadInvocationError> {
        LogicalNode::cancel_read(self, ticket)
    }
    fn poll_activation_configuration(&mut self) -> Option<ConfigurationCompletion> {
        LogicalNode::poll_configuration(self)
    }
    fn cancel_activation_configuration(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        LogicalNode::cancel_configuration(self, ticket)
    }
    fn request_activation_readiness(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: ReadinessRequirements,
    ) -> Result<AdmissionTicket, LogicalBackingPollError> {
        self.request_learner_readiness_tracked(group, learner, requirements)
            .map_err(|e| LogicalBackingPollError::LogicalNode(Box::new(e)))
    }
    fn cancel_activation_readiness(
        &mut self,
        group: GroupIdentity,
    ) -> Result<AdmissionTicket, LogicalBackingPollError> {
        self.cancel_learner_readiness_tracked(group)
            .map_err(|e| LogicalBackingPollError::LogicalNode(Box::new(e)))
    }
    fn activation_readiness(
        &self,
        group: GroupIdentity,
        learner: NodeId,
    ) -> Option<PromotionReadiness> {
        let ready = self.local.owner.core(group)?.ready_learner()?;
        let binding = self.current_peer_binding(learner)?;
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
        self.local.owner.core(group)?.pending_learner_readiness()
    }
    fn activation_peer_binding(&self, learner: NodeId) -> Option<StoreBinding> {
        self.current_peer_binding(learner)
    }
    fn poll_activation(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<Vec<OwnerStep>, LogicalBackingPollError> {
        if authorization.intent().mapping().child != self.group
            || self.local.applications[&self.group].activation_control_profile()
                != Some(authorization.profile())
        {
            return Err(LogicalBackingPollError::LogicalNode(Box::new(
                LogicalNodeError::WrongBinding,
            )));
        }
        self.poll_activation_mode(now, budget, Some(authorization))
            .map(|p| p.steps)
            .map_err(|e| LogicalBackingPollError::LogicalNode(Box::new(e)))
    }
}
