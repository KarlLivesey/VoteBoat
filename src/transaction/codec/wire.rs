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
use crate::identity::{GroupId, GroupIncarnation, OperationId};
pub(super) const SPEC_BYTES: usize = 320;
pub(super) const OUTCOME_BYTES: usize = 35;
pub(super) fn group(out: &mut Vec<u8>, g: GroupIdentity) {
    out.extend(g.id.get().to_le_bytes());
    out.extend(g.incarnation.get().to_le_bytes());
}
pub(super) fn profile(out: &mut Vec<u8>, p: TransferProfile) {
    group(out, p.coordinator());
    for g in p.participants() {
        group(out, g);
    }
}
pub(super) fn spec(out: &mut Vec<u8>, s: TransferSpec) {
    out.extend(s.id().get().to_le_bytes());
    profile(out, s.profile());
    for a in [s.debit(), s.credit()] {
        group(out, a.group());
        out.extend(a.account().get().to_le_bytes());
    }
    out.extend(s.amount().to_le_bytes());
    for id in s
        .phases()
        .coordinator()
        .into_iter()
        .chain(s.phases().participants().into_iter().flatten())
    {
        out.extend(id.get().to_le_bytes());
    }
}
pub(super) fn decision_byte(d: Option<TransferDecision>) -> u8 {
    match d {
        None => 0,
        Some(TransferDecision::Commit) => 1,
        Some(TransferDecision::Abort) => 2,
    }
}
pub(super) fn step_byte(s: TransferStep) -> u8 {
    match s {
        TransferStep::Start => 0,
        TransferStep::Prepare => 1,
        TransferStep::Decide => 2,
        TransferStep::ApplyDecision => 3,
        TransferStep::AppliedAll => 4,
        TransferStep::Release => 5,
        TransferStep::Finish => 6,
    }
}
pub(super) fn fact(out: &mut Vec<u8>, f: Option<PhaseFact>) {
    if let Some(f) = f {
        group(out, f.group);
        out.extend(f.operation.get().to_le_bytes());
        out.extend(f.index.to_le_bytes());
    } else {
        out.extend([0; 48]);
    }
}
pub(super) fn refusal(out: &mut Vec<u8>, r: Option<PrepareRefusal>) {
    let (tag, id) = match r {
        None => (0, 0),
        Some(PrepareRefusal::Locked(tx)) => (1, tx.get()),
        Some(PrepareRefusal::InsufficientFunds) => (2, 0),
        Some(PrepareRefusal::CreditOverflow) => (3, 0),
    };
    out.push(tag);
    out.extend(id.to_le_bytes());
}
pub(super) fn outcome(out: &mut Vec<u8>, o: Option<TransferOutcome>) {
    use TransferOutcome::*;
    let (tag, d, r, before, after) = match o {
        None => (0, None, None, 0, 0),
        Some(Started) => (1, None, None, 0, 0),
        Some(Prepare(PrepareOutcome::Prepared { before, after })) => (2, None, None, before, after),
        Some(Prepare(PrepareOutcome::Refused(r))) => (3, None, Some(r), 0, 0),
        Some(Prepare(PrepareOutcome::Closed(d))) => (4, Some(d), None, 0, 0),
        Some(Decided(d)) => (5, Some(d), None, 0, 0),
        Some(DecisionApplied(d)) => (6, Some(d), None, 0, 0),
        Some(AppliedAll(d)) => (7, Some(d), None, 0, 0),
        Some(Released(d)) => (8, Some(d), None, 0, 0),
        Some(Finished(d)) => (9, Some(d), None, 0, 0),
        Some(Conflict(TransferConflict::Operation)) => (10, None, None, 0, 0),
        Some(Conflict(TransferConflict::Transaction)) => (11, None, None, 0, 0),
        Some(Conflict(TransferConflict::Phase)) => (12, None, None, 0, 0),
    };
    out.push(tag);
    out.push(decision_byte(d));
    refusal(out, r);
    out.extend(before.to_le_bytes());
    out.extend(after.to_le_bytes());
}
pub(super) fn phase(out: &mut Vec<u8>, p: TransferPhase) {
    let (tag, d) = match p {
        TransferPhase::Started => (0, None),
        TransferPhase::Decided(d) => (1, Some(d)),
        TransferPhase::AppliedAll(d) => (2, Some(d)),
        TransferPhase::Finished(d) => (3, Some(d)),
        TransferPhase::Absent => (4, None),
        TransferPhase::Prepared => (5, None),
        TransferPhase::DecisionApplied(d) => (6, Some(d)),
        TransferPhase::Released(d) => (7, Some(d)),
    };
    out.push(tag);
    out.push(decision_byte(d));
}
pub(super) struct Reader<'a>(pub &'a [u8]);
impl<'a> Reader<'a> {
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(ApplicationError::InvalidCommand)?;
        self.0 = b;
        Ok(a)
    }
    pub fn byte(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    pub fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> Result<i64, ApplicationError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn group(&mut self) -> Result<GroupIdentity, ApplicationError> {
        Ok(GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub fn profile(&mut self) -> Result<TransferProfile, ApplicationError> {
        TransferProfile::new(self.group()?, [self.group()?, self.group()?])
    }
    pub fn operation(&mut self) -> Result<OperationId, ApplicationError> {
        OperationId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub fn account(&mut self) -> Result<AccountRef, ApplicationError> {
        Ok(AccountRef::new(
            self.group()?,
            AccountId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)?,
        ))
    }
    pub fn spec(&mut self) -> Result<TransferSpec, ApplicationError> {
        let id = TransactionId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?;
        let profile = self.profile()?;
        let debit = self.account()?;
        let credit = self.account()?;
        let amount = self.i64()?;
        let coordinator = [
            self.operation()?,
            self.operation()?,
            self.operation()?,
            self.operation()?,
        ];
        let participants = [
            [self.operation()?, self.operation()?, self.operation()?],
            [self.operation()?, self.operation()?, self.operation()?],
        ];
        TransferSpec::new(
            id,
            profile,
            debit,
            credit,
            amount,
            PhaseIds::new(coordinator, participants)?,
        )
    }
    pub fn decision(&mut self) -> Result<Option<TransferDecision>, ApplicationError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(TransferDecision::Commit)),
            2 => Ok(Some(TransferDecision::Abort)),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub fn step(&mut self) -> Result<TransferStep, ApplicationError> {
        match self.byte()? {
            0 => Ok(TransferStep::Start),
            1 => Ok(TransferStep::Prepare),
            2 => Ok(TransferStep::Decide),
            3 => Ok(TransferStep::ApplyDecision),
            4 => Ok(TransferStep::AppliedAll),
            5 => Ok(TransferStep::Release),
            6 => Ok(TransferStep::Finish),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub fn fact(&mut self) -> Result<Option<PhaseFact>, ApplicationError> {
        let bytes = self.take(48)?;
        let mut r = Self(bytes);
        let id = r.u128()?;
        let incarnation = r.u64()?;
        let op = r.u128()?;
        let index = r.u64()?;
        if index == 0 {
            if bytes.iter().any(|v| *v != 0) {
                return Err(ApplicationError::InvalidCommand);
            }
            return Ok(None);
        }
        Ok(Some(PhaseFact {
            group: GroupIdentity {
                id: GroupId::new(id).ok_or(ApplicationError::InvalidCommand)?,
                incarnation: GroupIncarnation::new(incarnation)
                    .ok_or(ApplicationError::InvalidCommand)?,
            },
            operation: OperationId::new(op).ok_or(ApplicationError::InvalidCommand)?,
            index,
        }))
    }
    pub fn refusal(&mut self) -> Result<Option<PrepareRefusal>, ApplicationError> {
        let tag = self.byte()?;
        let id = self.u128()?;
        match (tag, id) {
            (0, 0) => Ok(None),
            (1, id) => Ok(Some(PrepareRefusal::Locked(
                TransactionId::new(id).ok_or(ApplicationError::InvalidCommand)?,
            ))),
            (2, 0) => Ok(Some(PrepareRefusal::InsufficientFunds)),
            (3, 0) => Ok(Some(PrepareRefusal::CreditOverflow)),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub fn finish(self) -> Result<(), ApplicationError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
}
