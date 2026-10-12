// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A moved original work item is not a successful import or startup certificate.
use super::*;
/// Stopped or failed original work. No scalar/public parser can construct it.
pub struct LogicalEnrollmentRecovery {
    mapping: LogicalStoreMapping,
    configuration: Vec<u8>,
    bindings: Vec<(RuntimeOwner, ClientRouterBinding, ReadInvocationBinding)>,
    representative: NodeId,
    work: BridgeWork,
}
pub struct LogicalEnrollmentRecoveryRejected {
    pub reason: LogicalBridgeError,
    pub recovery: LogicalEnrollmentRecovery,
}
impl<'a, N: LogicalBackingChild> LogicalBackingBridge<'a, N> {
    /// Return the same live lower owners with the full unresolved original input.
    /// Their independent accepted lower work remains theirs to pump and settle.
    pub fn into_enrollment_recovery(
        self,
    ) -> Result<(LogicalEnrollmentRecovery, &'a mut [N]), Box<Self>> {
        if self.work.as_ref().is_none_or(|w| {
            !w.candidate || !w.stopped && self.failure.is_none() || w.attempt.is_some()
        }) || self.retained.is_some()
            || self.image.is_some()
            || self.action.is_some()
            || self.release_ticket.is_some()
            || self.activation_history.is_some()
            || self.readiness.is_some()
            || self.configuration_attempt.is_some()
            || self
                .nodes
                .iter()
                .any(|n| !n.local().clients.is_drained() || !n.local().reads.is_drained())
        {
            return Err(Box::new(self));
        }
        let recovery = LogicalEnrollmentRecovery {
            mapping: self.mapping,
            configuration: self.nodes[0].local().applications[&self.mapping.child]
                .configuration
                .clone(),
            bindings: self
                .nodes
                .iter()
                .map(|n| {
                    (
                        n.local().owner.identity(),
                        n.local().clients.binding(),
                        n.local().reads.binding(),
                    )
                })
                .collect(),
            representative: self.nodes[self.representative_node]
                .local()
                .owner
                .core(self.mapping.child)
                .expect("checked target assembly")
                .local_node(),
            work: self.work.expect("checked owned enrollment"),
        };
        Ok((recovery, self.nodes))
    }
    /// Resume only against the same original live streams. Cold restart must
    /// replay the candidate application and obtain a new Claim instead.
    pub fn resume_enrollment(
        &mut self,
        mut recovery: LogicalEnrollmentRecovery,
    ) -> Result<(), Box<LogicalEnrollmentRecoveryRejected>> {
        let checked = self.intake().and_then(|()| {
            if recovery.mapping != self.mapping
                || recovery.configuration
                    != self.nodes[0].local().applications[&self.mapping.child].configuration
                || recovery.representative
                    != self.nodes[self.representative_node]
                        .local()
                        .owner
                        .core(self.mapping.child)
                        .ok_or(LogicalBridgeError::InvalidAssembly)?
                        .local_node()
                || !self
                    .nodes
                    .iter()
                    .map(|n| {
                        (
                            n.local().owner.identity(),
                            n.local().clients.binding(),
                            n.local().reads.binding(),
                        )
                    })
                    .eq(recovery.bindings.iter().copied())
            {
                return Err(LogicalBridgeError::StaleRepresentative);
            }
            Ok(())
        });
        if let Err(reason) = checked {
            return Err(Box::new(LogicalEnrollmentRecoveryRejected {
                reason,
                recovery,
            }));
        }
        recovery.work.stopped = false;
        recovery.work.cancellation_requested = false;
        match &recovery.work.stage {
            BridgeStage::Applying { command, bytes } => {
                recovery.work.stage = BridgeStage::Resolving {
                    command: command.clone(),
                    bytes: bytes.clone(),
                }
            }
            BridgeStage::AfterClaim {
                claim,
                representative,
            } => {
                recovery.work.stage = BridgeStage::Enrollment {
                    claim: claim.clone(),
                    representative: *representative,
                }
            }
            BridgeStage::Snapshots(reads) => {
                recovery.work.stage = BridgeStage::Enrollment {
                    claim: Box::new(reads.claim),
                    representative: reads.image.representative,
                }
            }
            _ => (),
        }
        recovery.work.enrollment = None;
        self.work = Some(recovery.work);
        Ok(())
    }
}
