// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fresh candidate lineage precedes the Image that binds its current Claim.
use super::*;
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    pub(in crate::logical_voter::bridge) fn finish_enrollment_read(
        &self,
        work: &mut BridgeWork,
        node: usize,
        observation: (ReadInvocationTicket, ReadBarrier, LogicalEnrollmentView),
    ) -> Result<bool, LogicalBridgeError> {
        let BridgeStage::Enrollment {
            claim,
            representative,
        } = &work.stage
        else {
            return Err(LogicalBridgeError::ProviderViolation);
        };
        let (claim, representative) = (**claim, *representative);
        let (ticket, barrier, view) = observation;
        let app = &self.nodes[node].local().applications[&self.mapping.child];
        startup::validate_enrollment(&view, app.initialization(), self.mapping, barrier)
            .map_err(LogicalBridgeError::Storage)?;
        if barrier.index() < claim.first_index {
            return Err(LogicalBridgeError::ProviderViolation);
        }
        if let Some((command, _)) = &work.original_claim {
            if let CommandBody::EnrollmentClaim(import) = &command.body {
                let record = view.record.as_ref().expect("validated enrollment record");
                let reference = app
                    .snapshot_reference(&import.snapshot, SnapshotGeneration::new(1).unwrap())
                    .map_err(LogicalBridgeError::Application)?;
                if record.operation != command.operation
                    || record.first_index != claim.first_index
                    || record.plan != import.plan
                    || record.origin != import.origin
                    || record.learners != import.learners
                    || record.target_reference != reference
                {
                    return Err(LogicalBridgeError::ProviderViolation);
                }
            }
        }
        work.enrollment = Some(Box::new(startup::ObservedLogicalEnrollment {
            view,
            ticket,
            barrier,
            owner: self.nodes[node].local().owner.identity(),
        }));
        work.stage = BridgeStage::AfterClaim {
            claim: Box::new(claim),
            representative,
        };
        Ok(false)
    }
}
