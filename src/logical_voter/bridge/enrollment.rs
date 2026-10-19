// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original target input, observations and stopped-work handback.
use super::*;
mod reads;
mod recovery;
#[cfg(all(test, feature = "tls"))]
mod tests;
pub use recovery::{LogicalEnrollmentRecovery, LogicalEnrollmentRecoveryRejected};

impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Admit one consumed source capability. This does not import, claim or
    /// provision a route until the original lower command and reads complete.
    pub fn begin_enrollment(
        &mut self,
        operation: OperationId,
        enrollment: LogicalLearnerEnrollment,
    ) -> Result<(), Box<LogicalEnrollmentRejected>> {
        let checked = self.intake().and_then(|()| {
            let app = &self.nodes[0].local().applications[&self.mapping.child];
            if enrollment.mapping != self.mapping
                || operation.get() >= command::SESSION_SPACE
                || app.initialization()
                    != LogicalInitialization::EnrollmentCandidate(enrollment.import.plan)
                || import_charge(&enrollment.import)? > app.limits().max_command_bytes
            {
                return Err(LogicalBridgeError::InvalidAssembly);
            }
            if !app
                .valid_import(&enrollment.import)
                .map_err(LogicalBridgeError::Application)?
            {
                return Err(LogicalBridgeError::Refused(
                    LogicalRefusal::SnapshotConflict,
                ));
            }
            Ok(())
        });
        if let Err(reason) = checked {
            return Err(Box::new(LogicalEnrollmentRejected { reason, enrollment }));
        }
        self.work = Some(BridgeWork::new(
            BridgeStage::BeforeEnrollment {
                operation,
                enrollment: Box::new(enrollment),
            },
            true,
        ));
        Ok(())
    }
    /// Stop only the original observation. Accepted lower work remains owned;
    /// the recovery handback retains its immutable operation and bytes.
    pub fn stop_enrollment_waiting(&mut self) -> Result<(), LogicalBridgeError> {
        let work = self
            .work
            .as_mut()
            .filter(|w| w.candidate)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        work.stopped = true;
        if work.cancellation_requested {
            return Ok(());
        }
        match work.attempt {
            Some(ChildAttempt::Client { node, ticket }) => {
                self.nodes[node].cancel_client(ticket).map_err(|reason| {
                    LogicalBridgeError::Client {
                        node,
                        reason: Box::new(reason),
                    }
                })?
            }
            Some(ChildAttempt::Read { node, ticket, .. }) => {
                self.nodes[node]
                    .cancel_read(ticket)
                    .map_err(|reason| LogicalBridgeError::Read {
                        node,
                        reason: Box::new(reason),
                    })?
            }
            None => (),
        }
        work.cancellation_requested = true;
        Ok(())
    }
    pub(super) fn enrollment_bytes(&self) -> usize {
        let Some(work) = &self.work else { return 0 };
        if !work.candidate {
            return 0;
        }
        work_charge(work).unwrap_or(usize::MAX)
    }
    pub(super) fn prepare_enrollment(
        &self,
        work: &mut BridgeWork,
        node: usize,
        read: LogicalRead,
    ) -> Result<(), LogicalBridgeError> {
        if !matches!(read, LogicalRead::Uninitialized) {
            return Err(LogicalBridgeError::Refused(
                LogicalRefusal::SnapshotConflict,
            ));
        }
        let BridgeStage::BeforeEnrollment {
            operation,
            enrollment,
        } = &work.stage
        else {
            return Err(LogicalBridgeError::ProviderViolation);
        };
        let operation = *operation;
        let app = &self.nodes[node].local().applications[&self.mapping.child];
        let mut plain = BridgeWork::new(BridgeStage::BeforeClaim(operation), true);
        self.prepare_claim(&mut plain, operation, node, LogicalRead::Uninitialized)?;
        let BridgeStage::Applying { mut command, .. } = plain.stage else {
            unreachable!()
        };
        command.body = CommandBody::EnrollmentClaim(enrollment.import.clone());
        let bytes = app
            .encode_command(&command, app.limits().max_command_bytes)
            .map_err(LogicalBridgeError::Application)?;
        if bytes
            .capacity()
            .checked_add(import_charge(&enrollment.import)?)
            .is_none_or(|n| n > app.limits().control_action_bytes)
        {
            return Err(LogicalBridgeError::Application(
                ApplicationError::ReceiptBudget,
            ));
        }
        work.stage = BridgeStage::Applying { command, bytes };
        Ok(())
    }
    pub(super) fn after_candidate_claim(
        &self,
        work: &mut BridgeWork,
        claim: LogicalReceipt,
        representative: RepresentativeBinding,
    ) {
        let old = std::mem::replace(
            &mut work.stage,
            BridgeStage::Enrollment {
                claim: Box::new(claim),
                representative,
            },
        );
        let (BridgeStage::Applying { command, bytes } | BridgeStage::Resolving { command, bytes }) =
            old
        else {
            unreachable!("checked original candidate claim")
        };
        work.original_claim = Some((command, bytes));
    }
}
fn import_charge(import: &LogicalLearnerImport) -> Result<usize, LogicalBridgeError> {
    let entry = match &import.learners.payload {
        EntryPayload::Configuration(record) => record.retained_bytes(),
        _ => return Err(LogicalBridgeError::InvalidAssembly),
    };
    std::mem::size_of::<LogicalLearnerImport>()
        .checked_add(entry)
        .and_then(|n| n.checked_add(query::snapshot_charge(&import.snapshot)))
        .ok_or(LogicalBridgeError::InvalidAssembly)
}
fn command_charge(command: &Command, bytes: &[u8]) -> Result<usize, LogicalBridgeError> {
    let nested = match &command.body {
        CommandBody::EnrollmentClaim(import) => import_charge(import)?,
        CommandBody::Claim { .. } => 0,
        _ => return Err(LogicalBridgeError::ProviderViolation),
    };
    std::mem::size_of::<Command>()
        .checked_add(nested)
        .and_then(|n| n.checked_add(bytes.len()))
        .ok_or(LogicalBridgeError::InvalidAssembly)
}
fn work_charge(work: &BridgeWork) -> Result<usize, LogicalBridgeError> {
    let mut charge = std::mem::size_of::<BridgeWork>();
    let stage = match &work.stage {
        BridgeStage::BeforeEnrollment { enrollment, .. } => import_charge(&enrollment.import)?,
        BridgeStage::Applying { command, bytes } | BridgeStage::Resolving { command, bytes } => {
            command_charge(command, bytes)?
                .checked_add(bytes.capacity() - bytes.len())
                .ok_or(LogicalBridgeError::InvalidAssembly)?
        }
        BridgeStage::Snapshots(reads) => snapshot_reads_charge(reads)?,
        _ => 0,
    };
    charge = charge
        .checked_add(stage)
        .ok_or(LogicalBridgeError::InvalidAssembly)?;
    if let Some((command, bytes)) = &work.original_claim {
        charge = charge
            .checked_add(command_charge(command, bytes)?)
            .and_then(|n| n.checked_add(bytes.capacity() - bytes.len()))
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
    }
    if let Some(observed) = &work.enrollment {
        charge = charge
            .checked_add(observation_charge(observed)?)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
    }
    Ok(charge)
}
fn observation_charge(o: &startup::ObservedLogicalEnrollment) -> Result<usize, LogicalBridgeError> {
    let nested = o.view.record.as_ref().map_or(0, |r| {
        std::mem::size_of::<LogicalEnrollmentRecord>()
            .saturating_add(r.learners.retained_payload_bytes())
    });
    std::mem::size_of::<startup::ObservedLogicalEnrollment>()
        .checked_add(nested)
        .ok_or(LogicalBridgeError::InvalidAssembly)
}
fn snapshot_reads_charge(reads: &SnapshotReads) -> Result<usize, LogicalBridgeError> {
    let initial = std::mem::size_of::<SnapshotReads>()
        .checked_add(query::image_charge(&reads.image))
        .and_then(|n| {
            reads
                .references
                .capacity()
                .checked_mul(std::mem::size_of::<SnapshotRef>())
                .and_then(|r| n.checked_add(r))
        })
        .and_then(|n| {
            reads
                .snapshots
                .capacity()
                .checked_mul(std::mem::size_of::<(SnapshotRef, Snapshot)>())
                .and_then(|r| n.checked_add(r))
        })
        .ok_or(LogicalBridgeError::InvalidAssembly)?;
    let charge = reads
        .snapshots
        .iter()
        .try_fold(initial, |n, (_, snapshot)| {
            n.checked_add(query::snapshot_charge(snapshot))
                .ok_or(LogicalBridgeError::InvalidAssembly)
        })?;
    charge
        .checked_add(
            reads
                .enrollment
                .as_ref()
                .map_or(Ok(0), |o| observation_charge(o))?,
        )
        .ok_or(LogicalBridgeError::InvalidAssembly)
}
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    pub(super) fn validate_enrollment_work(
        &self,
        work: &BridgeWork,
    ) -> Result<(), LogicalBridgeError> {
        if work.candidate
            && work_charge(work)?
                > self.nodes[0].local().applications[&self.mapping.child]
                    .limits()
                    .max_child_checkpoint_bytes
        {
            return Err(LogicalBridgeError::Application(
                ApplicationError::ReceiptBudget,
            ));
        }
        Ok(())
    }
}
