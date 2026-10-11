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
use crate::identity::OperationId;

/// Trusted protocol assertions, never business-supplied foreign certificates.
/// The driver preserves first retained phase positions on every same-ID retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Command {
    pub(crate) spec: TransferSpec,
    pub(crate) step: TransferStep,
    pub(crate) decision: Option<TransferDecision>,
    pub(crate) facts: [Option<PhaseFact>; 2],
    pub(crate) refusal: Option<(usize, PrepareRefusal)>,
}
impl Command {
    pub(crate) fn start(spec: TransferSpec) -> Self {
        Self {
            spec,
            step: TransferStep::Start,
            decision: None,
            facts: [None; 2],
            refusal: None,
        }
    }
    pub(crate) fn operation(self, group: GroupIdentity) -> Result<OperationId, ApplicationError> {
        let c = self.spec.phases().coordinator();
        match self.step {
            TransferStep::Start if group == self.spec.profile().coordinator() => Ok(c[0]),
            TransferStep::Decide if group == self.spec.profile().coordinator() => Ok(c[1]),
            TransferStep::AppliedAll if group == self.spec.profile().coordinator() => Ok(c[2]),
            TransferStep::Finish if group == self.spec.profile().coordinator() => Ok(c[3]),
            TransferStep::Prepare | TransferStep::ApplyDecision | TransferStep::Release => {
                let p = self
                    .spec
                    .profile()
                    .participant(group)
                    .ok_or(ApplicationError::InvalidCommand)?;
                let slot = match self.step {
                    TransferStep::Prepare => 0,
                    TransferStep::ApplyDecision => 1,
                    _ => 2,
                };
                Ok(self.spec.phases().participants()[p][slot])
            }
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub(crate) fn slot(self) -> usize {
        match self.step {
            TransferStep::Start | TransferStep::Prepare => 0,
            TransferStep::Decide | TransferStep::ApplyDecision => 1,
            TransferStep::AppliedAll | TransferStep::Release => 2,
            TransferStep::Finish => 3,
        }
    }
    pub(crate) fn validate(self) -> Result<(), ApplicationError> {
        use TransferStep::*;
        match (self.step, self.decision, self.refusal) {
            (Start | Prepare, None, None) if self.facts == [None; 2] => Ok(()),
            (Decide, Some(TransferDecision::Commit), None) => self.participant_facts(0),
            (Decide, Some(TransferDecision::Abort), Some((p, _)))
                if p < 2 && self.facts[1 - p].is_none() =>
            {
                self.participant_fact(p, 0)
            }
            (ApplyDecision, Some(_), None) => self.coordinator_fact(1),
            (AppliedAll, Some(_), None) => self.participant_facts(1),
            (Release, Some(_), None) => self.coordinator_fact(2),
            (Finish, Some(_), None) => self.participant_facts(2),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    fn coordinator_fact(self, slot: usize) -> Result<(), ApplicationError> {
        if self.facts[1].is_some() {
            return Err(ApplicationError::InvalidCommand);
        }
        self.fact(
            0,
            self.spec.profile().coordinator(),
            self.spec.phases().coordinator()[slot],
        )
    }
    fn participant_facts(self, slot: usize) -> Result<(), ApplicationError> {
        for p in 0..2 {
            self.participant_fact(p, slot)?;
        }
        Ok(())
    }
    fn participant_fact(self, p: usize, slot: usize) -> Result<(), ApplicationError> {
        self.fact(
            p,
            self.spec.profile().participants()[p],
            self.spec.phases().participants()[p][slot],
        )
    }
    fn fact(
        self,
        p: usize,
        group: GroupIdentity,
        operation: OperationId,
    ) -> Result<(), ApplicationError> {
        if self.facts[p]
            .is_none_or(|f| f.group != group || f.operation != operation || f.index == 0)
        {
            Err(ApplicationError::InvalidCommand)
        } else {
            Ok(())
        }
    }
}
