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
//! One original child read consumer and exact original administrative stream.
use super::*;
use crate::raft::{ConfigurationProposal, ReadinessRequirements};
mod configuration;
mod continuation;
mod export;
mod readiness;
pub(super) use configuration::ConfigurationAttempt;
pub use continuation::{LogicalActivationAttachmentRejected, LogicalActivationContinuation};
pub(super) use readiness::ReadinessAttempt;
pub(crate) struct ObservedLogicalConfiguration {
    pub history: LogicalConfigurationOperation,
    pub ticket: ReadInvocationTicket,
    pub barrier: ReadBarrier,
    pub owner: RuntimeOwner,
}
impl<N: LogicalActivationBackingChild> LogicalBackingBridge<'_, N> {
    pub(crate) fn activation_preflight(
        &self,
        profile: &LogicalActivationControlProfile,
    ) -> Result<ReadinessRequirements, LogicalBridgeError> {
        if self.closed
            || self.failure.is_some()
            || !self.observations_are_drained()
            || self.nodes.iter().any(|n| {
                n.local()
                    .snapshots
                    .as_ref()
                    .is_some_and(|s| !s.router.checkpoint_exports_are_drained())
            })
            || self
                .nodes
                .iter()
                .any(|n| !n.configuration_requests_are_drained())
        {
            return Err(LogicalBridgeError::Busy);
        }
        if self
            .nodes
            .iter()
            .any(|n| n.pending_activation_readiness(self.mapping.child).is_some())
        {
            return Err(LogicalBridgeError::Busy);
        }
        let first = &self.nodes[0].local().applications[&self.mapping.child];
        if self.nodes.iter().any(|n| {
            n.local().applications[&self.mapping.child].activation_control_profile()
                != Some(profile)
        }) {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        first
            .deployment_requirements()
            .ok_or(LogicalBridgeError::InvalidAssembly)
    }
    pub(crate) fn activation_result_bound(&self) -> usize {
        self.nodes
            .iter()
            .map(|n| {
                n.local()
                    .reads
                    .limits()
                    .result_bytes
                    .saturating_add(std::mem::size_of::<ReadCompletion<LogicalRead>>())
            })
            .max()
            .unwrap_or(usize::MAX)
    }
    pub(crate) fn advance_activation(
        &mut self,
        now: MonoTime,
        budget: LogicalBridgeBudget<N::PollBudget>,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<LogicalBridgeUsage, LogicalBridgeError> {
        if budget.node_polls > 64
            || budget.completions > 128
            || budget.transitions > 64
            || authorization.intent().mapping() != self.mapping
            || self.nodes.iter().any(|n| {
                n.local().applications[&self.mapping.child].activation_control_profile()
                    != Some(authorization.profile())
            })
        {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        if self.now.is_some_and(|old| now < old) {
            return Err(LogicalBridgeError::TimeWentBack);
        }
        for _ in 0..budget.node_polls.min(self.nodes.len()) {
            let node = self.node_cursor;
            self.node_cursor = (node + 1) % self.nodes.len();
            match self.nodes[node].poll_activation(now, budget.node, authorization) {
                Ok(steps) => {
                    self.observe_readiness_steps(node, &steps);
                    self.observe_configuration_steps(node, &steps);
                }
                Err(reason) => {
                    self.failure.get_or_insert(LogicalBridgeError::Backing {
                        node,
                        reason: Box::new(reason),
                    });
                }
            }
        }
        if self.activation_stopping {
            self.cancel_activation_readiness()?;
        }
        self.advance(
            now,
            LogicalBridgeBudget {
                node_polls: 0,
                ..budget
            },
        )
    }
    pub(crate) fn begin_activation_history(
        &mut self,
        authorization: &LogicalActivationAuthorization,
        operation: OperationId,
    ) -> Result<(), LogicalBridgeError> {
        self.intake()?;
        if authorization.intent().mapping() != self.mapping
            || self.activation_history.is_some()
            || ![
                authorization.intent().operations().learners,
                authorization.intent().operations().membership,
            ]
            .contains(&operation)
        {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        self.work = Some(BridgeWork::new(
            if self.activation_verified {
                BridgeStage::ActivationHistory(operation)
            } else {
                BridgeStage::ActivationRepresentative(operation)
            },
            false,
        ));
        Ok(())
    }
    pub(crate) fn take_activation_history(&mut self) -> Option<ObservedLogicalConfiguration> {
        self.activation_history.take().map(|v| *v)
    }
    pub(crate) fn configure_activation(
        &mut self,
        authorization: &LogicalActivationAuthorization,
        phase: usize,
    ) -> Result<Option<(usize, ConfigurationTicket)>, LogicalBridgeError> {
        if self.configuration_attempt.is_some() {
            return Err(LogicalBridgeError::Busy);
        }
        let Some(node) = self.route() else {
            return Ok(None);
        };
        let mut proposal = ConfigurationProposal {
            record: authorization.record(phase).clone(),
            readiness: Vec::new(),
            requirements: authorization.requirements(),
        };
        if phase == 1 {
            let learner = authorization
                .intent()
                .target_voters()
                .iter()
                .find(|v| !authorization.intent().old_voters().contains(v))
                .ok_or(LogicalBridgeError::InvalidAssembly)?
                .node;
            let Some(ready) = self.nodes[node].activation_readiness(self.mapping.child, learner)
            else {
                return Ok(None);
            };
            proposal.readiness.push(ready);
        }
        match self.nodes[node].configure_activation(ConfigurationRequest {
            group: self.mapping.child,
            proposal,
        }) {
            Ok(ticket) => {
                self.configuration_attempt = Some(ConfigurationAttempt::new(node, ticket));
                Ok(Some((node, ticket)))
            }
            Err(rejected) if rejected.reason == ConfigurationRequestError::Overloaded => Ok(None),
            Err(rejected) => Err(LogicalBridgeError::Backing {
                node,
                reason: Box::new(LogicalBackingPollError::Node(Box::new(
                    NodeError::Configuration(rejected.reason),
                ))),
            }),
        }
    }
    pub(crate) fn cancel_activation_configuration(
        &mut self,
        node: usize,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        self.nodes[node].cancel_activation_configuration(ticket)
    }
    pub(crate) fn stop_activation(&mut self) -> Result<(), LogicalBridgeError> {
        self.activation_stopping = true;
        if let Some(ChildAttempt::Read { node, ticket, .. }) = self
            .work
            .as_ref()
            .filter(|w| {
                matches!(
                    w.stage,
                    BridgeStage::ActivationHistory(_) | BridgeStage::ActivationRepresentative(_)
                )
            })
            .and_then(|w| w.attempt)
        {
            self.nodes[node]
                .cancel_activation_read(ticket)
                .map_err(|reason| LogicalBridgeError::Read {
                    node,
                    reason: Box::new(reason),
                })?;
        }
        self.activation_history = None;
        self.cancel_activation_readiness()
    }
    pub(crate) fn activation_controls_are_drained(&self) -> bool {
        self.readiness.is_none()
            && self.configuration_attempt.is_none()
            && self.activation_history.is_none()
            && !self.activation_history_pending()
    }
    pub(crate) fn activation_history_pending(&self) -> bool {
        self.work.as_ref().is_some_and(|w| {
            matches!(
                w.stage,
                BridgeStage::ActivationHistory(_) | BridgeStage::ActivationRepresentative(_)
            )
        })
    }
}
