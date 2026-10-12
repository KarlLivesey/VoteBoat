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
//! Administrative ownership extends the same sealed physical endpoint.
use super::*;
use crate::raft::{LearnerReadinessRequest, PromotionReadiness, ReadinessRequirements};
/// Original child administrative stream. Initially implemented by physical
/// Nodes; recursive logical endpoints require their own real owning path.
pub trait LogicalActivationBackingChild: LogicalBackingChild {
    fn configuration_requests_are_drained(&self) -> bool;
    fn export_checkpoint(
        &mut self,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError>;
    fn poll_checkpoint_export(&mut self) -> Option<SnapshotExportCompletion>;
    fn complete_checkpoint_export(
        &mut self,
        output: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected>;
    fn cancel_checkpoint_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError>;
    fn configure_activation(
        &mut self,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected>;
    fn cancel_activation_read(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<(), ReadInvocationError>;
    fn poll_activation_configuration(&mut self) -> Option<ConfigurationCompletion>;
    fn cancel_activation_configuration(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError>;
    fn request_activation_readiness(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: ReadinessRequirements,
    ) -> Result<AdmissionTicket, LogicalBackingPollError>;
    fn cancel_activation_readiness(
        &mut self,
        group: GroupIdentity,
    ) -> Result<AdmissionTicket, LogicalBackingPollError>;
    fn activation_readiness(
        &self,
        group: GroupIdentity,
        learner: NodeId,
    ) -> Option<PromotionReadiness>;
    fn pending_activation_readiness(&self, group: GroupIdentity)
        -> Option<LearnerReadinessRequest>;
    fn activation_peer_binding(&self, learner: NodeId) -> Option<StoreBinding>;
    fn poll_activation(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<Vec<OwnerStep>, LogicalBackingPollError>;
}
