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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FinalFact {
    pub operation: OperationId,
    pub configuration: ConfigurationId,
    pub position: ActivationPosition,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ControlCommand {
    Begin(Box<ChildActivationIntent>),
    Outcome {
        begin: OperationId,
        fact: FinalFact,
    },
    Release {
        begin: OperationId,
        outcome: ActivationPosition,
    },
}
impl ControlCommand {
    pub fn begin(&self) -> OperationId {
        match self {
            Self::Begin(i) => i.operations.begin,
            Self::Outcome { begin, .. } | Self::Release { begin, .. } => *begin,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Phase {
    pub command: ControlCommand,
    pub receipt: ChildActivationReceipt,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub intent: Box<ChildActivationIntent>,
    pub begin: Phase,
    pub outcome: Option<Phase>,
    pub release: Option<Phase>,
}
impl Record {
    pub fn status(&self) -> ChildActivationStatus {
        ChildActivationStatus {
            intent: *self.intent.clone(),
            begin: self.begin.receipt,
            outcome: self.outcome.as_ref().map(|p| p.receipt),
            release: self.release.as_ref().map(|p| p.receipt),
            child_final: self.outcome.as_ref().and_then(|p| match p.command {
                ControlCommand::Outcome { fact, .. } => Some(fact.position),
                _ => None,
            }),
        }
    }
    pub fn phase(&self, c: &ControlCommand) -> Option<&Phase> {
        match c {
            ControlCommand::Begin(_) => Some(&self.begin),
            ControlCommand::Outcome { .. } => self.outcome.as_ref(),
            ControlCommand::Release { .. } => self.release.as_ref(),
        }
    }
}
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub(super) struct ControlState {
    pub active: Option<OperationId>,
    pub records: BTreeMap<OperationId, Record>,
}
impl ControlState {
    pub fn operation(&self, c: &ControlCommand) -> Result<OperationId, ApplicationError> {
        match c {
            ControlCommand::Begin(i) => Ok(i.operations.begin),
            ControlCommand::Outcome { begin, .. } => self
                .records
                .get(begin)
                .map(|r| r.intent.operations.outcome)
                .ok_or(ApplicationError::InvalidCommand),
            ControlCommand::Release { begin, .. } => self
                .records
                .get(begin)
                .map(|r| r.intent.operations.release)
                .ok_or(ApplicationError::InvalidCommand),
        }
    }
    pub fn contains_operation(&self, id: OperationId) -> bool {
        self.records
            .values()
            .any(|r| r.intent.operations.ids().contains(&id))
    }
    pub fn execute(
        &mut self,
        c: ControlCommand,
        index: u64,
        term: u64,
        limits: LogicalActivationLimits,
    ) -> Result<ChildActivationReceipt, ApplicationError> {
        let operation = self.operation(&c)?;
        if let Some(r) = self.records.get(&c.begin()) {
            if let Some(old) = r.phase(&c) {
                let mut receipt = old.receipt;
                receipt.index = index;
                receipt.duplicate = old.command == c;
                if !receipt.duplicate {
                    receipt.outcome = ChildActivationOutcome::OperationConflict;
                }
                return Ok(receipt);
            }
        }
        let mut receipt = ChildActivationReceipt {
            index,
            operation,
            first: ActivationPosition { index, term },
            outcome: ChildActivationOutcome::Begun,
            duplicate: false,
        };
        match &c {
            ControlCommand::Begin(intent) => {
                intent.validate()?;
                if self.records.len() >= limits.intents {
                    return Err(ApplicationError::DedupCapacity);
                }
                if intent
                    .operations
                    .ids()
                    .iter()
                    .any(|id| self.contains_operation(*id))
                {
                    return Err(ApplicationError::OperationConflict);
                }
                if let Some(active) = self.active {
                    receipt.outcome = ChildActivationOutcome::Busy(active);
                } else {
                    self.active = Some(operation);
                }
                self.records.insert(
                    operation,
                    Record {
                        intent: intent.clone(),
                        begin: Phase {
                            command: c,
                            receipt,
                        },
                        outcome: None,
                        release: None,
                    },
                );
            }
            ControlCommand::Outcome { begin, fact } => {
                let r = self
                    .records
                    .get_mut(begin)
                    .ok_or(ApplicationError::InvalidCommand)?;
                if self.active != Some(*begin)
                    || r.begin.receipt.outcome != ChildActivationOutcome::Begun
                    || fact.operation != r.intent.operations.membership
                    || fact.configuration != r.intent.target_configuration
                    || fact.position.index == 0
                    || fact.position.term == 0
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                receipt.outcome = ChildActivationOutcome::Completed;
                r.outcome = Some(Phase {
                    command: c,
                    receipt,
                });
            }
            ControlCommand::Release { begin, outcome } => {
                let r = self
                    .records
                    .get_mut(begin)
                    .ok_or(ApplicationError::InvalidCommand)?;
                if self.active != Some(*begin)
                    || r.outcome
                        .as_ref()
                        .is_none_or(|p| p.receipt.first != *outcome)
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                receipt.outcome = ChildActivationOutcome::Released;
                r.release = Some(Phase {
                    command: c,
                    receipt,
                });
                self.active = None;
            }
        }
        Ok(receipt)
    }
    pub fn phases(&self) -> Vec<&Phase> {
        let mut phases = Vec::with_capacity(self.records.len() * 3);
        for r in self.records.values() {
            phases.push(&r.begin);
            phases.extend(r.outcome.iter());
            phases.extend(r.release.iter());
        }
        phases.sort_by_key(|p| p.receipt.first.index);
        phases
    }
}
