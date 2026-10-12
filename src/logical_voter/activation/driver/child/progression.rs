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
//! Canonical same-operation retry and terminal assertions from owned facts.
use super::*;
impl<P: LogicalActivationParent> ChildActivationDriver<'_, P> {
    pub(super) fn progress_child<N: LogicalActivationBackingChild>(
        &mut self,
        bridge: &mut LogicalBackingBridge<'_, N>,
    ) -> Result<bool, ChildActivationDriverError> {
        if let Some((node, ticket)) = self.child.configuration {
            let Some(output) = bridge.poll_activation_configuration(node) else {
                return Ok(false);
            };
            if output.ticket != ticket {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
            self.child.configuration = None;
            match output.outcome {
                ConfigurationOutcome::Committed(p) if p.index == 0 || p.term == 0 => {
                    return Err(self.fail(ChildActivationDriverError::ProviderViolation));
                }
                ConfigurationOutcome::NotProposed(e)
                    if !crate::logical_voter::bridge::polling::retry_raft(&e) =>
                {
                    return Err(self.fail(ChildActivationDriverError::Configuration(e)));
                }
                _ => (), // Resolve actual retained truth, including Unknown.
            }
        }
        if self.child.final_position.is_some() {
            return self.terminal_phase();
        }
        if self.child.waiting_history {
            let Some(history) = bridge.take_activation_history() else {
                return Ok(false);
            };
            self.child.waiting_history = false;
            let was_learners = self.child.phase == 0;
            let entries = self.child_history(history)?;
            if self.child.final_position.is_some() {
                return self.terminal_phase();
            }
            if was_learners
                && self.child.phase == 1
                && entries == 1
                && self.child.learners.is_some()
            {
                // Learners was just observed. Requery the separate Joint/Final operation.
                return Ok(true);
            }
            let phase = if self.child.phase == 0 {
                0
            } else if entries == 0 {
                1
            } else {
                2
            };
            let auth = self
                .authorization
                .as_ref()
                .expect("owned original capability");
            if phase == 1
                && !bridge
                    .request_activation_readiness(auth)
                    .map_err(|e| self.fail(ChildActivationDriverError::Lower(Box::new(e))))?
            {
                self.phase = ChildActivationDriverPhase::Enrolling;
                return Ok(false);
            }
            self.child.configuration = bridge
                .configure_activation(
                    self.authorization
                        .as_ref()
                        .expect("owned original capability"),
                    phase,
                )
                .map_err(|e| self.fail(ChildActivationDriverError::Lower(Box::new(e))))?;
            return Ok(false);
        }
        let auth = self
            .authorization
            .as_ref()
            .expect("owned original capability");
        let operation = if self.child.phase == 0 {
            self.intent.operations.learners
        } else {
            self.intent.operations.membership
        };
        match bridge.begin_activation_history(auth, operation) {
            Ok(()) => {
                self.child.waiting_history = true;
                Ok(false)
            }
            Err(LogicalBridgeError::Busy) => Ok(false),
            Err(e) => Err(self.fail(ChildActivationDriverError::Lower(Box::new(e)))),
        }
    }
    fn terminal_phase(&mut self) -> Result<bool, ChildActivationDriverError> {
        let position = self
            .child
            .final_position
            .expect("original child Final observation");
        let status = self
            .status
            .as_ref()
            .ok_or(ChildActivationDriverError::ProviderViolation)?;
        if status.release.is_some() {
            self.phase = ChildActivationDriverPhase::Released;
            return Ok(false);
        }
        let (operation, command) = if let Some(outcome) = status.outcome {
            if status.child_final != Some(position) {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
            (
                self.intent.operations.release,
                ControlCommand::Release {
                    begin: self.intent.operations.begin,
                    outcome: outcome.first,
                },
            )
        } else {
            (
                self.intent.operations.outcome,
                ControlCommand::Outcome {
                    begin: self.intent.operations.begin,
                    fact: super::super::super::state::FinalFact {
                        operation: self.intent.operations.membership,
                        configuration: self.intent.target_configuration,
                        position,
                    },
                },
            )
        };
        let bytes = super::super::super::codec::command(&command, CONTROL_BYTES)
            .map_err(ChildActivationDriverError::Application)?;
        if let Some((old, original)) = &self.terminal {
            if *old == operation && *original != bytes {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
        }
        self.terminal = Some((operation, bytes));
        let request = ClientRequest {
            group: self.intent.mapping.parent,
            operation,
            bytes: self.terminal.as_ref().unwrap().1.clone(),
        };
        match self.parent.propose(request) {
            Ok(ticket) => {
                self.pending = Some(Attempt::Client(ticket));
                Ok(false)
            }
            Err(rejected) if rejected.reason == ClientError::Overloaded => Ok(false),
            Err(rejected) => Err(self.fail(ChildActivationDriverError::Client(rejected.reason))),
        }
    }
}
