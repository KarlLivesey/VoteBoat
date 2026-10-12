// SPDX-License-Identifier: RPL-1.5
use super::*;
use crate::identity::{GroupId, GroupIncarnation};
pub(super) struct Reader<'a>(pub &'a [u8]);
impl<'a> Reader<'a> {
    pub(super) fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(ApplicationError::InvalidCommand)?;
        self.0 = b;
        Ok(a)
    }
    pub(super) fn byte(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub(super) fn i64(&mut self) -> Result<i64, ApplicationError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn group(&mut self) -> Result<GroupIdentity, ApplicationError> {
        Ok(GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub(super) fn key(&mut self) -> Result<KeyId, ApplicationError> {
        KeyId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub(super) fn transaction(&mut self) -> Result<TransactionId, ApplicationError> {
        TransactionId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub(super) fn operation(&mut self) -> Result<OperationId, ApplicationError> {
        OperationId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub(super) fn count_byte(
        &mut self,
        cap: usize,
        minimum: usize,
    ) -> Result<usize, ApplicationError> {
        let count = usize::from(self.byte()?);
        self.count(count, cap, minimum)
    }
    pub(super) fn count_u64(
        &mut self,
        cap: usize,
        minimum: usize,
    ) -> Result<usize, ApplicationError> {
        let count = usize::try_from(self.u64()?).map_err(|_| ApplicationError::InvalidCommand)?;
        self.count(count, cap, minimum)
    }
    fn count(&self, count: usize, cap: usize, minimum: usize) -> Result<usize, ApplicationError> {
        if count > cap || count.checked_mul(minimum).is_none_or(|n| n > self.0.len()) {
            Err(ApplicationError::InvalidCommand)
        } else {
            Ok(count)
        }
    }
    pub(super) fn decision(&mut self) -> Result<Option<Decision>, ApplicationError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(Decision::Commit)),
            2 => Ok(Some(Decision::Abort)),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub(super) fn step(&mut self) -> Result<ProgramStep, ApplicationError> {
        match self.byte()? {
            0 => Ok(ProgramStep::Start),
            1 => Ok(ProgramStep::Prepare),
            2 => Ok(ProgramStep::Decide),
            3 => Ok(ProgramStep::ApplyDecision),
            4 => Ok(ProgramStep::AppliedAll),
            5 => Ok(ProgramStep::Release),
            6 => Ok(ProgramStep::Finish),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub(super) fn finish(self) -> Result<(), ApplicationError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
}
