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
//! One tracked admission/cancellation and one exact volatile exchange.
use super::*;
use crate::raft::{LearnerReadinessRequest, RaftError};
#[derive(Clone)]
pub(in crate::logical_voter::bridge) enum ReadinessAttempt {
    Admitted {
        node: usize,
        ticket: AdmissionTicket,
        cancel: bool,
    },
    /// Boxed: the request carries the full application identity.
    Waiting {
        node: usize,
        request: Box<LearnerReadinessRequest>,
    },
}
impl<N: LogicalActivationBackingChild> LogicalBackingBridge<'_, N> {
    pub(super) fn observe_readiness_steps(&mut self, node: usize, steps: &[OwnerStep]) {
        let Some(ReadinessAttempt::Admitted {
            node: original,
            ticket,
            cancel,
        }) = self.readiness
        else {
            return;
        };
        if node != original {
            return;
        }
        let Some(step) = steps.iter().find(|s| s.admission == Some(ticket)) else {
            return;
        };
        self.readiness = None;
        if step.visit.owner != ticket.owner || step.visit.group != self.mapping.child {
            self.failure
                .get_or_insert(LogicalBridgeError::ProviderViolation);
            return;
        }
        if let Some(error) = &step.error {
            if !matches!(
                error,
                RaftError::Busy | RaftError::NotLeader | RaftError::InvalidMessage
            ) {
                self.failure
                    .get_or_insert(LogicalBridgeError::ProviderViolation);
            }
            return;
        }
        if !cancel {
            if let Some(request) = self.nodes[node].pending_activation_readiness(self.mapping.child)
            {
                self.readiness = Some(ReadinessAttempt::Waiting {
                    node,
                    request: Box::new(request),
                });
            }
        }
    }
    pub(crate) fn request_activation_readiness(
        &mut self,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<bool, LogicalBridgeError> {
        let learner = authorization.learner();
        if let Some(ReadinessAttempt::Admitted { .. }) = self.readiness {
            return Ok(false);
        }
        if let Some(ReadinessAttempt::Waiting { node, ref request }) = self.readiness {
            let request = **request;
            let current = self.nodes[node].activation_peer_binding(learner.node);
            if current
                != Some(StoreBinding {
                    identity: request.learner.store,
                    session: request.session,
                })
            {
                let ticket = self.nodes[node]
                    .cancel_activation_readiness(self.mapping.child)
                    .map_err(|reason| LogicalBridgeError::Backing {
                        node,
                        reason: Box::new(reason),
                    })?;
                self.readiness = Some(ReadinessAttempt::Admitted {
                    node,
                    ticket,
                    cancel: true,
                });
                return Ok(false);
            }
            if let Some(ready) =
                self.nodes[node].activation_readiness(self.mapping.child, learner.node)
            {
                if *ready.ready.request() == request
                    && request.requirements == authorization.requirements()
                {
                    self.readiness = None;
                    return Ok(true);
                }
                return Err(LogicalBridgeError::ProviderViolation);
            }
            if self.nodes[node].pending_activation_readiness(self.mapping.child) == Some(request) {
                return Ok(false);
            }
            // The original successful admission has ended (negative reply or
            // leadership/configuration invalidation). No positive fact exists.
            self.readiness = None;
        }
        let Some(node) = self.route() else {
            return Ok(false);
        };
        let core = self.nodes[node]
            .local()
            .owner
            .core(self.mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        if core.role() != Role::Leader {
            return Ok(false);
        }
        if self.nodes[node]
            .activation_readiness(self.mapping.child, learner.node)
            .is_some_and(|r| {
                r.ready.request().requirements == authorization.requirements()
                    && r.ready.request().configuration == core.membership().id()
                    && r.ready.request().term == core.state().hard_state.term
            })
        {
            return Ok(true);
        }
        if self.nodes[node]
            .activation_peer_binding(learner.node)
            .is_none()
        {
            return Ok(false);
        }
        let ticket = self.nodes[node]
            .request_activation_readiness(
                self.mapping.child,
                learner.node,
                authorization.requirements(),
            )
            .map_err(|reason| LogicalBridgeError::Backing {
                node,
                reason: Box::new(reason),
            })?;
        self.readiness = Some(ReadinessAttempt::Admitted {
            node,
            ticket,
            cancel: false,
        });
        Ok(false)
    }
    pub(super) fn cancel_activation_readiness(&mut self) -> Result<(), LogicalBridgeError> {
        if let Some(ReadinessAttempt::Waiting { node, .. }) = self.readiness {
            let ticket = self.nodes[node]
                .cancel_activation_readiness(self.mapping.child)
                .map_err(|reason| LogicalBridgeError::Backing {
                    node,
                    reason: Box::new(reason),
                })?;
            self.readiness = Some(ReadinessAttempt::Admitted {
                node,
                ticket,
                cancel: true,
            });
        }
        Ok(())
    }
}
