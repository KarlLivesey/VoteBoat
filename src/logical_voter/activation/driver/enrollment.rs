// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The owning activation driver consumes the original live source export.
use super::*;
use crate::{
    application::{CheckpointStateMachine, ProposalAdmission},
    log_format::LogCodec,
    logical_voter::{
        LogicalApplication, LogicalEnrollmentOrigin, LogicalEnrollmentPlan, LogicalInitialization,
        LogicalLearnerEnrollment, LogicalLearnerImport,
    },
    runtime::LogicalResponseCodec,
    secure::SecureSession,
    snapshot::Snapshot,
    snapshot_format::SnapshotCodec,
    wire::WireCodec,
};
mod validation;

/// Original held observation, or its whole outcome after actual consumption.
#[derive(Debug)]
pub enum LogicalEnrollmentExport {
    Original(Box<SnapshotExportCompletion>),
    Consumed(Box<SnapshotExportOutcome>),
}
#[derive(Debug)]
pub struct LogicalEnrollmentExportRejected {
    pub reason: ChildActivationDriverError,
    pub export: LogicalEnrollmentExport,
}
impl<P: LogicalActivationParent> ChildActivationDriver<'_, P> {
    /// Consume a current original live source export under this driver's exact
    /// committed Learners observation. Caller-provided lineage is not authority.
    pub fn enroll_logical_learner<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    >(
        &mut self,
        source: &mut LogicalNode<S, T, E, A, U, C, V, B>,
        target: &LogicalApplication<C, V, B>,
        output: SnapshotExportCompletion,
    ) -> Result<LogicalLearnerEnrollment, Box<LogicalEnrollmentExportRejected>>
    where
        A::Receipt: ApplicationReceipt,
    {
        let checked = self.enrollment_source(source, target, &output);
        let (plan, origin) = match checked {
            Ok(scope) => scope,
            Err(reason) => {
                return Err(Box::new(LogicalEnrollmentExportRejected {
                    reason,
                    export: LogicalEnrollmentExport::Original(Box::new(output)),
                }))
            }
        };
        let outcome = match source.complete_checkpoint_export(output) {
            Ok(outcome) => outcome,
            Err(rejected) => {
                return Err(Box::new(LogicalEnrollmentExportRejected {
                    reason: ChildActivationDriverError::Snapshot(rejected.reason),
                    export: LogicalEnrollmentExport::Original(rejected.completion),
                }))
            }
        };
        let checked = match &outcome {
            SnapshotExportOutcome::Loaded(snapshot) => {
                self.enrollment_snapshot(target, plan, origin, snapshot)
            }
            SnapshotExportOutcome::Failed(_) => Err(ChildActivationDriverError::ProviderViolation),
        };
        if let Err(reason) = checked {
            return Err(Box::new(LogicalEnrollmentExportRejected {
                reason,
                export: LogicalEnrollmentExport::Consumed(Box::new(outcome)),
            }));
        }
        let SnapshotExportOutcome::Loaded(snapshot) = outcome else {
            unreachable!("checked original loaded outcome")
        };
        let learners = self
            .child
            .learners
            .as_ref()
            .expect("retained original Learners observation")
            .history
            .entries[0]
            .clone();
        let import = LogicalLearnerImport {
            plan,
            origin,
            learners,
            snapshot,
        };
        match target.valid_import(&import) {
            Ok(true) => Ok(LogicalLearnerEnrollment::mint(target.mapping(), import)),
            checked => Err(Box::new(LogicalEnrollmentExportRejected {
                reason: checked.err().map_or(
                    ChildActivationDriverError::WrongBinding,
                    ChildActivationDriverError::Application,
                ),
                export: LogicalEnrollmentExport::Consumed(Box::new(SnapshotExportOutcome::Loaded(
                    import.snapshot,
                ))),
            })),
        }
    }
}
