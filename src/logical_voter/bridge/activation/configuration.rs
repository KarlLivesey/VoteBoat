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
//! Observer cancellation cannot discard original queued dispatch ownership.
use super::*;
pub(in crate::logical_voter::bridge) struct ConfigurationAttempt {
    node: usize,
    ticket: ConfigurationTicket,
    dispatched: bool,
    output: Option<ConfigurationCompletion>,
}
impl ConfigurationAttempt {
    pub(in crate::logical_voter::bridge) fn has_output(&self) -> bool {
        self.output.is_some()
    }
    pub(super) fn new(node: usize, ticket: ConfigurationTicket) -> Self {
        Self {
            node,
            ticket,
            dispatched: false,
            output: None,
        }
    }
}
impl<N: LogicalActivationBackingChild> LogicalBackingBridge<'_, N> {
    pub(super) fn observe_configuration_steps(&mut self, node: usize, steps: &[OwnerStep]) {
        let Some(attempt) = &mut self.configuration_attempt else {
            return;
        };
        if attempt.node != node {
            return;
        }
        let admission = attempt.ticket.admission();
        let Some(step) = steps.iter().find(|s| s.admission == Some(admission)) else {
            return;
        };
        if step.visit.owner != admission.owner || step.visit.group != admission.group {
            self.failure
                .get_or_insert(LogicalBridgeError::ProviderViolation);
            return;
        }
        // Execution may refuse or start persistence. Neither is commitment;
        // exact fresh history remains the sole driver phase progression.
        attempt.dispatched = true;
    }
    pub(crate) fn poll_activation_configuration(
        &mut self,
        node: usize,
    ) -> Option<ConfigurationCompletion> {
        let attempt = self.configuration_attempt.as_mut()?;
        if attempt.node != node {
            return None;
        }
        if attempt.output.is_none() {
            let output = self.nodes[node].poll_activation_configuration()?;
            if output.ticket != attempt.ticket {
                self.failure
                    .get_or_insert(LogicalBridgeError::ProviderViolation);
                // Preserve the original unexpected output and original ticket.
                attempt.output = Some(output);
                return None;
            }
            attempt.output = Some(output);
        }
        if !attempt.dispatched || self.failure.is_some() {
            return None;
        }
        self.configuration_attempt.take()?.output
    }
}
