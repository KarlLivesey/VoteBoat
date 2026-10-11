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
impl Work {
    fn spec(&self) -> Result<TransferSpec, DriverError> {
        self.spec.ok_or(DriverError::Inconsistent)
    }
    pub(super) fn command(&self) -> Result<(GroupIdentity, Command), DriverError> {
        let spec = self.spec()?;
        let mut command = Command::start(spec);
        let group = match self.stage {
            DriverStage::Start => spec.profile().coordinator(),
            DriverStage::Prepare(p) => {
                command.step = TransferStep::Prepare;
                spec.profile().participants()[p]
            }
            DriverStage::Decide => {
                command.step = TransferStep::Decide;
                command.decision = self.decision;
                command.facts = self.facts;
                command.refusal = self.refusal;
                spec.profile().coordinator()
            }
            DriverStage::ApplyDecision(p) => {
                command.step = TransferStep::ApplyDecision;
                command.decision = self.decision;
                command.facts = [self.decision_fact, None];
                spec.profile().participants()[p]
            }
            DriverStage::AppliedAll => {
                command.step = TransferStep::AppliedAll;
                command.decision = self.decision;
                command.facts = self.facts;
                spec.profile().coordinator()
            }
            DriverStage::Release(p) => {
                command.step = TransferStep::Release;
                command.decision = self.decision;
                command.facts = [self.all_fact, None];
                spec.profile().participants()[p]
            }
            DriverStage::Finish => {
                command.step = TransferStep::Finish;
                command.decision = self.decision;
                command.facts = self.facts;
                spec.profile().coordinator()
            }
            _ => return Err(DriverError::Inconsistent),
        };
        command.validate().map_err(DriverError::Application)?;
        Ok((group, command))
    }
    pub(super) fn retry_status(&mut self) {
        self.stage = DriverStage::CoordinatorStatus;
        self.route = self.route.wrapping_add(1);
    }
    pub(super) fn receipt(
        &mut self,
        group: GroupIdentity,
        command: Command,
        receipt: TransferReceipt,
    ) -> Result<(), DriverError> {
        if self.spec() != Ok(command.spec)
            || command.operation(group).map_err(DriverError::Application)? != receipt.operation
            || receipt.first_index == 0
            || receipt.first_index > receipt.index
        {
            return Err(DriverError::Inconsistent);
        }
        let fact = Some(PhaseFact {
            group,
            operation: receipt.operation,
            index: receipt.first_index,
        });
        match (self.stage, receipt.outcome) {
            (DriverStage::Start, TransferOutcome::Started) => self.stage = DriverStage::Prepare(0),
            (
                DriverStage::Prepare(p),
                TransferOutcome::Prepare(PrepareOutcome::Prepared { .. }),
            ) => {
                self.facts[p] = fact;
                if p == 0 {
                    self.stage = DriverStage::Prepare(1);
                } else {
                    self.decision = Some(TransferDecision::Commit);
                    self.stage = DriverStage::Decide;
                }
            }
            (
                DriverStage::Prepare(p),
                TransferOutcome::Prepare(PrepareOutcome::Refused(reason)),
            ) => {
                self.facts = [None; 2];
                self.facts[p] = fact;
                self.refusal = Some((p, reason));
                self.decision = Some(TransferDecision::Abort);
                self.stage = DriverStage::Decide;
            }
            (DriverStage::Prepare(_), TransferOutcome::Prepare(PrepareOutcome::Closed(_))) => {
                self.retry_status()
            }
            (DriverStage::Decide, TransferOutcome::Decided(d)) if Some(d) == self.decision => {
                self.decision_fact = fact;
                self.facts = [None; 2];
                self.stage = DriverStage::ApplyDecision(0);
            }
            (DriverStage::ApplyDecision(p), TransferOutcome::DecisionApplied(d))
                if Some(d) == self.decision =>
            {
                self.facts[p] = fact;
                self.stage = if p == 0 {
                    DriverStage::ApplyDecision(1)
                } else {
                    DriverStage::AppliedAll
                };
            }
            (DriverStage::AppliedAll, TransferOutcome::AppliedAll(d))
                if Some(d) == self.decision =>
            {
                self.all_fact = fact;
                self.facts = [None; 2];
                self.stage = DriverStage::Release(0);
            }
            (DriverStage::Release(p), TransferOutcome::Released(d)) if Some(d) == self.decision => {
                self.facts[p] = fact;
                self.stage = if p == 0 {
                    DriverStage::Release(1)
                } else {
                    DriverStage::Finish
                };
            }
            (DriverStage::Finish, TransferOutcome::Finished(d)) if Some(d) == self.decision => self
                .finish(DriverOutcome::Finished {
                    transaction: self.ticket.transaction,
                    decision: d,
                    first_index: receipt.first_index,
                }),
            _ => return Err(DriverError::Inconsistent),
        }
        Ok(())
    }
    pub(super) fn finish(&mut self, outcome: DriverOutcome) {
        self.outcome = Some(outcome);
        self.stage = DriverStage::Terminal;
    }
    pub(super) fn status(
        &mut self,
        status: Option<Box<TransferStatus>>,
    ) -> Result<(), DriverError> {
        let Some(status) = status else {
            self.finish(DriverOutcome::Unknown {
                transaction: self.ticket.transaction,
                reason: DriverUnknown::NoRetainedStart,
            });
            return Ok(());
        };
        if status.spec.id() != self.ticket.transaction
            || self.spec.is_some_and(|s| s != status.spec)
            || status.prepare.is_some()
        {
            return Err(DriverError::Inconsistent);
        }
        let (slot, decision) = match status.phase {
            TransferPhase::Started => (0, None),
            TransferPhase::Decided(d) => (1, Some(d)),
            TransferPhase::AppliedAll(d) => (2, Some(d)),
            TransferPhase::Finished(d) => (3, Some(d)),
            _ => return Err(DriverError::Inconsistent),
        };
        for i in 0..4 {
            match status.phases[i] {
                Some(phase) if i <= slot => validate_phase(status.spec, phase, i, decision)?,
                None if i > slot => {}
                _ => return Err(DriverError::Inconsistent),
            }
        }
        self.spec = Some(status.spec);
        self.decision = decision;
        self.facts = [None; 2];
        self.refusal = None;
        let phase = status.phases[slot].ok_or(DriverError::Inconsistent)?;
        let fact = Some(PhaseFact {
            group: status.spec.profile().coordinator(),
            operation: phase.operation,
            index: phase.index,
        });
        match slot {
            0 => self.stage = DriverStage::Prepare(0),
            1 => {
                self.decision_fact = fact;
                self.stage = DriverStage::ApplyDecision(0);
            }
            2 => {
                self.all_fact = fact;
                self.stage = DriverStage::Release(0);
            }
            _ => self.finish(DriverOutcome::Finished {
                transaction: self.ticket.transaction,
                decision: decision.ok_or(DriverError::Inconsistent)?,
                first_index: phase.index,
            }),
        }
        Ok(())
    }
}
fn validate_phase(
    spec: TransferSpec,
    phase: PhaseStatus,
    slot: usize,
    decision: Option<TransferDecision>,
) -> Result<(), DriverError> {
    let step = [
        TransferStep::Start,
        TransferStep::Decide,
        TransferStep::AppliedAll,
        TransferStep::Finish,
    ][slot];
    let expected = match slot {
        0 => TransferOutcome::Started,
        1 => TransferOutcome::Decided(decision.ok_or(DriverError::Inconsistent)?),
        2 => TransferOutcome::AppliedAll(decision.ok_or(DriverError::Inconsistent)?),
        _ => TransferOutcome::Finished(decision.ok_or(DriverError::Inconsistent)?),
    };
    if phase.index == 0
        || phase.operation != spec.phases().coordinator()[slot]
        || phase.step != step
        || phase.outcome != expected
        || phase.decision != if slot == 0 { None } else { decision }
    {
        return Err(DriverError::Inconsistent);
    }
    Command {
        spec,
        step,
        decision: phase.decision,
        facts: phase.facts,
        refusal: phase.refusal,
    }
    .validate()
    .map_err(|_| DriverError::Inconsistent)
}
