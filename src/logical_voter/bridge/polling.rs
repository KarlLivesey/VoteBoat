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
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// The host supplies monotonic time and each budget. A zero completion budget
    /// deliberately leaves original produced child observations unconsumed.
    pub fn advance(
        &mut self,
        now: MonoTime,
        budget: LogicalBridgeBudget<N::PollBudget>,
    ) -> Result<LogicalBridgeUsage, LogicalBridgeError> {
        if budget.node_polls > 64 || budget.completions > 128 || budget.transitions > 64 {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        if self.now.is_some_and(|old| now < old) {
            return Err(LogicalBridgeError::TimeWentBack);
        }
        self.now = Some(now);
        for _ in 0..budget.node_polls.min(self.nodes.len()) {
            let node = self.node_cursor;
            self.node_cursor = (node + 1) % self.nodes.len();
            if let Err(reason) = self.nodes[node].poll(now, budget.node) {
                let error = match reason {
                    LogicalBackingPollError::Node(reason) => {
                        LogicalBridgeError::Node { node, reason }
                    }
                    reason => LogicalBridgeError::Backing {
                        node,
                        reason: Box::new(reason),
                    },
                };
                self.failure.get_or_insert(error);
            }
        }
        for _ in 0..budget.completions {
            if !self.consume_one() {
                break;
            }
        }
        if self.failure.is_none() && budget.transitions > 0 {
            if let Some(mut work) = self.work.take() {
                if let Err(error) = self.validate_enrollment_work(&work) {
                    self.failure.get_or_insert(error);
                } else if work.attempt.is_none() && !work.stopped {
                    if let Err(error) = self.issue(&mut work) {
                        self.failure.get_or_insert(error);
                    }
                }
                self.work = Some(work);
            }
        }
        self.failure.clone().map_or(Ok(self.usage()), Err)
    }
    fn issue(&mut self, work: &mut BridgeWork) -> Result<(), LogicalBridgeError> {
        let Some(node) = self.route() else {
            return Ok(());
        };
        match &work.stage {
            BridgeStage::Applying { command, bytes } => {
                match self.nodes[node].propose(ClientRequest {
                    group: self.mapping.child,
                    operation: command.operation,
                    bytes: bytes.clone(),
                }) {
                    Ok(ticket) => work.attempt = Some(ChildAttempt::Client { node, ticket }),
                    Err(error) => {
                        self.route_cursor = (node + 1) % self.nodes.len();
                        if !retry_client(&error.reason) {
                            return Err(LogicalBridgeError::Client {
                                node,
                                reason: Box::new(error.reason),
                            });
                        }
                    }
                }
            }
            _ => {
                let query = match &work.stage {
                    BridgeStage::ActivationRepresentative(_) => LogicalQuery::Metadata,
                    BridgeStage::BeforeClaim(_)
                    | BridgeStage::BeforeEnrollment { .. }
                    | BridgeStage::AfterClaim { .. } => LogicalQuery::Image,
                    BridgeStage::Enrollment { .. } => LogicalQuery::Enrollment,
                    BridgeStage::ActivationHistory(operation) => {
                        LogicalQuery::ConfigurationOperation(*operation)
                    }
                    BridgeStage::Resolving { command, .. } => {
                        LogicalQuery::Outcome(command.operation)
                    }
                    BridgeStage::Snapshots(reads) => {
                        LogicalQuery::Snapshot(reads.references[reads.snapshots.len()])
                    }
                    BridgeStage::Applying { .. } => unreachable!(),
                };
                match self.nodes[node].read(self.mapping.child, query) {
                    Ok(ticket) => {
                        work.attempt = Some(ChildAttempt::Read {
                            node,
                            ticket,
                            query,
                        })
                    }
                    Err(error) => {
                        self.route_cursor = (node + 1) % self.nodes.len();
                        if !retry_read(&error.reason) {
                            return Err(LogicalBridgeError::Read {
                                node,
                                reason: Box::new(error.reason),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub(super) fn finish_action(
        &mut self,
        work: &mut BridgeWork,
        receipt: LogicalReceipt,
        child_owner: RuntimeOwner,
    ) -> Result<bool, LogicalBridgeError> {
        let (command, bytes) = match &work.stage {
            BridgeStage::Applying { command, bytes }
            | BridgeStage::Resolving { command, bytes } => (command, bytes),
            _ => return Err(LogicalBridgeError::ProviderViolation),
        };
        if receipt.operation != command.operation
            || receipt.first_index == 0
            || receipt.first_index > receipt.index
            || (receipt.outcome == LogicalOutcome::Refused(LogicalRefusal::Expired)
                && !receipt.duplicate)
        {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        if matches!(
            command.body,
            CommandBody::Claim { .. } | CommandBody::EnrollmentClaim(_)
        ) {
            if receipt.outcome != LogicalOutcome::Claimed {
                return match receipt.outcome {
                    LogicalOutcome::Refused(reason) => Err(LogicalBridgeError::Refused(reason)),
                    _ => Err(LogicalBridgeError::ProviderViolation),
                };
            }
            let representative = command.representative;
            if work.candidate {
                self.after_candidate_claim(work, receipt, representative);
            } else {
                work.stage = BridgeStage::AfterClaim {
                    claim: Box::new(receipt),
                    representative,
                };
            }
            Ok(false)
        } else {
            if receipt.outcome == LogicalOutcome::Refused(LogicalRefusal::Expired) {
                // The session is closed or this view is inconsistent: the
                // host must re-claim. Fail first so the host sees the cause,
                // then hand the committed refusal to its consumer, which
                // settles it without granting anything.
                self.failure
                    .get_or_insert(LogicalBridgeError::Refused(LogicalRefusal::Expired));
            }
            self.action = Some(CommittedLogicalAction {
                mapping: self.mapping,
                command: command.clone(),
                bytes: bytes.clone(),
                receipt,
                child_owner,
            });
            Ok(true)
        }
    }
    pub(super) fn finish_image(&mut self, reads: SnapshotReads) -> Result<(), LogicalBridgeError> {
        let endpoint = self.nodes[self.representative_node]
            .endpoint_chain(self.mapping.child)
            .and_then(|chain| chain.extend_observed(&reads.image, reads.claim, reads.barrier))
            .map_err(LogicalBridgeError::Endpoint)?;
        self.initialized = Some(reads.image.representative);
        self.initialized_at = Some(reads.barrier);
        self.activation_verified = true;
        self.image = Some(VerifiedLogicalImage {
            image: reads.image,
            claim: reads.claim,
            barrier: reads.barrier,
            snapshots: reads.snapshots,
            limits: self.nodes[0].local().applications[&self.mapping.child].limits(),
            endpoint,
            control_profile: self.nodes[0].local().applications[&self.mapping.child]
                .activation_control_profile()
                .cloned(),
            initialization: self.nodes[0].local().applications[&self.mapping.child]
                .initialization(),
            enrollment: reads.enrollment,
        });
        Ok(())
    }
}
pub(in crate::logical_voter) fn retry_raft(error: &RaftError) -> bool {
    matches!(
        error,
        RaftError::Busy
            | RaftError::NotLeader
            | RaftError::ReadNotReady
            | RaftError::ReadInFlight
            | RaftError::StaleRead
            | RaftError::NotApplied
    )
}
fn retry_client(error: &ClientError) -> bool {
    matches!(error, ClientError::Overloaded)
        || matches!(error,ClientError::Consensus(reason) if retry_raft(reason))
}
fn retry_read(error: &ReadInvocationError) -> bool {
    matches!(
        error,
        ReadInvocationError::Overloaded | ReadInvocationError::InFlight
    ) || matches!(error,ReadInvocationError::Consensus(reason) if retry_raft(reason))
}
