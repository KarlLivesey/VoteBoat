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
use super::*;
mod reads;
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    pub(super) fn consume_one(&mut self) -> bool {
        if self.retained.is_some() {
            return self.cleanup_retained();
        }
        for _ in 0..self.nodes.len() * 2 {
            let slot = self.output_cursor;
            self.output_cursor = (slot + 1) % (self.nodes.len() * 2);
            let node = slot / 2;
            if slot.is_multiple_of(2) {
                if let Some(output) = self.nodes[node].poll_client() {
                    self.consume_client(node, output);
                    return true;
                }
            } else if let Some(output) = self.nodes[node].poll_read() {
                self.consume_read(node, output);
                return true;
            }
        }
        false
    }
    fn consume_client(&mut self, node: usize, output: ClientCompletion<LogicalReceipt>) {
        let ticket = output.ticket();
        let matches = self.work.as_ref().is_some_and(|w| {
            matches!(w.attempt,
            Some(ChildAttempt::Client{node:n,ticket:t}) if n==node && t==ticket)
        }) && ticket.binding == self.nodes[node].local().clients.binding()
            && ticket.group == self.mapping.child;
        let outcome = match self.nodes[node].complete_client(output) {
            Ok(value) => value,
            Err(error) => {
                self.failure.get_or_insert(LogicalBridgeError::Client {
                    node,
                    reason: Box::new(error.reason),
                });
                self.retained = Some(RetainedChildOutput::Client {
                    node,
                    output: error.completion,
                });
                return;
            }
        };
        if !matches {
            self.failure
                .get_or_insert(LogicalBridgeError::ProviderViolation);
            return;
        }
        let mut work = self.work.take().expect("correlated original client");
        work.attempt = None;
        let result = match outcome {
            ClientOutcome::Applied { position, receipt }
                if receipt.index == position.index && receipt.operation == ticket.operation =>
            {
                self.finish_action(
                    &mut work,
                    receipt,
                    self.nodes[node].local().owner.identity(),
                )
            }
            ClientOutcome::Applied { .. } => Err(LogicalBridgeError::ProviderViolation),
            ClientOutcome::Unknown(_) => {
                if let BridgeStage::Applying { command, bytes } = &work.stage {
                    work.stage = BridgeStage::Resolving {
                        command: command.clone(),
                        bytes: bytes.clone(),
                    };
                    Ok(false)
                } else {
                    Err(LogicalBridgeError::ProviderViolation)
                }
            }
            ClientOutcome::NotProposed(reason) if super::polling::retry_raft(&reason) => Ok(false),
            ClientOutcome::NotProposed(reason) => Err(LogicalBridgeError::Client {
                node,
                reason: Box::new(ClientError::Consensus(reason)),
            }),
        };
        match result {
            Ok(true) => {}
            Ok(false) => self.work = Some(work),
            Err(error) => {
                self.work = Some(work);
                self.failure.get_or_insert(error);
            }
        }
    }
    fn consume_read(&mut self, node: usize, output: ReadCompletion<LogicalRead>) {
        let ticket = output.ticket();
        let attempt = self.work.as_ref().and_then(|w| w.attempt);
        let matches = matches!(attempt,Some(ChildAttempt::Read{node:n,ticket:t,..}) if n==node && t==ticket)
            && ticket.binding == self.nodes[node].local().reads.binding()
            && ticket.group == self.mapping.child;
        let outcome = match self.nodes[node].complete_read(output) {
            Ok(value) => value,
            Err(error) => {
                self.failure.get_or_insert(LogicalBridgeError::Read {
                    node,
                    reason: Box::new(error.reason),
                });
                self.retained = Some(RetainedChildOutput::Read {
                    node,
                    output: error.completion,
                });
                return;
            }
        };
        if !matches {
            self.failure
                .get_or_insert(LogicalBridgeError::ProviderViolation);
            return;
        }
        let Some(ChildAttempt::Read { query, .. }) = attempt else {
            unreachable!()
        };
        let mut work = self.work.take().expect("correlated original read");
        work.attempt = None;
        let result = if work.stopped {
            Ok(false) // Keep the exact stopped work; the original invocation was consumed.
        } else if self.activation_stopping
            && matches!(
                work.stage,
                BridgeStage::ActivationHistory(_) | BridgeStage::ActivationRepresentative(_)
            )
        {
            Ok(true) // Original cancelled or completed invocation was consumed.
        } else {
            match outcome {
                ReadOutcome::Read { barrier, result }
                    if barrier.group() == self.mapping.child
                        && barrier.request() == ticket.request =>
                {
                    result
                        .map_err(LogicalBridgeError::Application)
                        .and_then(|result| {
                            self.read_result(&mut work, node, ticket, query, barrier, result)
                        })
                }
                ReadOutcome::Read { .. } => Err(LogicalBridgeError::ProviderViolation),
                ReadOutcome::Unavailable(_) => Ok(false),
                ReadOutcome::NotRead(reason) if super::polling::retry_raft(&reason) => Ok(false),
                ReadOutcome::NotRead(reason) => Err(LogicalBridgeError::Read {
                    node,
                    reason: Box::new(ReadInvocationError::Consensus(reason)),
                }),
            }
        };
        match result {
            Ok(true) => {}
            Ok(false) => self.work = Some(work),
            Err(error) => {
                self.work = Some(work);
                self.failure.get_or_insert(error);
            }
        }
    }
    fn cleanup_retained(&mut self) -> bool {
        match self.retained.take().expect("retained original output") {
            RetainedChildOutput::Client { node, output } => {
                if let Err(error) = self.nodes[node].complete_client(*output) {
                    self.retained = Some(RetainedChildOutput::Client {
                        node,
                        output: error.completion,
                    });
                }
            }
            RetainedChildOutput::Read { node, output } => {
                if let Err(error) = self.nodes[node].complete_read(*output) {
                    self.retained = Some(RetainedChildOutput::Read {
                        node,
                        output: error.completion,
                    });
                }
            }
        }
        // A repeated unchanged provider rejection does not spin inside one pump.
        self.retained.is_none()
    }
}
