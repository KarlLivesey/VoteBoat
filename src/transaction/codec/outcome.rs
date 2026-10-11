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
use super::wire::*;
use super::*;
impl Reader<'_> {
    pub(super) fn outcome(&mut self) -> Result<Option<TransferOutcome>, ApplicationError> {
        let bytes = self.take(OUTCOME_BYTES)?;
        let mut r = Reader(bytes);
        let tag = r.byte()?;
        let decision = r.decision()?;
        let refusal = r.refusal()?;
        let before = r.i64()?;
        let after = r.i64()?;
        use TransferOutcome::*;
        let outcome = match tag {
            0 => None,
            1 => Some(Started),
            2 => Some(Prepare(PrepareOutcome::Prepared { before, after })),
            3 => Some(Prepare(PrepareOutcome::Refused(
                refusal.ok_or(ApplicationError::InvalidCommand)?,
            ))),
            4 => Some(Prepare(PrepareOutcome::Closed(
                decision.ok_or(ApplicationError::InvalidCommand)?,
            ))),
            5 => Some(Decided(decision.ok_or(ApplicationError::InvalidCommand)?)),
            6 => Some(DecisionApplied(
                decision.ok_or(ApplicationError::InvalidCommand)?,
            )),
            7 => Some(AppliedAll(
                decision.ok_or(ApplicationError::InvalidCommand)?,
            )),
            8 => Some(Released(decision.ok_or(ApplicationError::InvalidCommand)?)),
            9 => Some(Finished(decision.ok_or(ApplicationError::InvalidCommand)?)),
            10 => Some(Conflict(TransferConflict::Operation)),
            11 => Some(Conflict(TransferConflict::Transaction)),
            12 => Some(Conflict(TransferConflict::Phase)),
            _ => return Err(ApplicationError::InvalidCommand),
        };
        let mut canonical = Vec::with_capacity(OUTCOME_BYTES);
        super::wire::outcome(&mut canonical, outcome);
        if canonical != bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(outcome)
    }
    pub(super) fn phase(&mut self) -> Result<TransferPhase, ApplicationError> {
        let tag = self.byte()?;
        let d = self.decision()?;
        match (tag, d) {
            (0, None) => Ok(TransferPhase::Started),
            (1, Some(d)) => Ok(TransferPhase::Decided(d)),
            (2, Some(d)) => Ok(TransferPhase::AppliedAll(d)),
            (3, Some(d)) => Ok(TransferPhase::Finished(d)),
            (4, None) => Ok(TransferPhase::Absent),
            (5, None) => Ok(TransferPhase::Prepared),
            (6, Some(d)) => Ok(TransferPhase::DecisionApplied(d)),
            (7, Some(d)) => Ok(TransferPhase::Released(d)),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
}
