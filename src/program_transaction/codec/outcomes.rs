// SPDX-License-Identifier: RPL-1.5
use super::*;
pub(super) fn write_refusal(out: &mut Vec<u8>, refusal: Option<PrepareRefusal>) {
    let (tag, key, owner, version) = match refusal {
        None => (0, 0, 0, 0),
        Some(PrepareRefusal::Locked { key, owner }) => (1, key.get(), owner.get(), 0),
        Some(PrepareRefusal::Version { key, actual }) => (2, key.get(), 0, actual.get()),
        Some(PrepareRefusal::VersionExhausted { key }) => (3, key.get(), 0, 0),
    };
    out.push(tag);
    out.extend(key.to_le_bytes());
    out.extend(owner.to_le_bytes());
    out.extend(version.to_le_bytes());
}
pub(super) fn write_outcome(out: &mut Vec<u8>, outcome: ProgramOutcome) {
    use ProgramOutcome::*;
    let (tag, d, refusal) = match outcome {
        Started => (0, None, None),
        Prepare(PrepareOutcome::Prepared) => (1, None, None),
        Prepare(PrepareOutcome::Refused(r)) => (2, None, Some(r)),
        Prepare(PrepareOutcome::Closed(d)) => (3, Some(d), None),
        Decided(d) => (4, Some(d), None),
        DecisionApplied(d) => (5, Some(d), None),
        AppliedAll(d) => (6, Some(d), None),
        Released(d) => (7, Some(d), None),
        Finished(d) => (8, Some(d), None),
        Conflict(ProgramConflict::Operation) => (9, None, None),
        Conflict(ProgramConflict::Transaction) => (10, None, None),
        Conflict(ProgramConflict::Phase) => (11, None, None),
    };
    out.push(tag);
    out.push(decision_byte(d));
    write_refusal(out, refusal);
}
impl Reader<'_> {
    pub(super) fn refusal(&mut self) -> Result<Option<PrepareRefusal>, ApplicationError> {
        let bytes = self.take(33)?;
        let mut r = Reader(bytes);
        let tag = r.byte()?;
        let key = r.u64()?;
        let owner = r.u128()?;
        let version = r.u64()?;
        let value = match tag {
            0 if key == 0 && owner == 0 && version == 0 => None,
            1 if version == 0 => Some(PrepareRefusal::Locked {
                key: KeyId::new(key).ok_or(ApplicationError::InvalidCommand)?,
                owner: TransactionId::new(owner).ok_or(ApplicationError::InvalidCommand)?,
            }),
            2 if owner == 0 => Some(PrepareRefusal::Version {
                key: KeyId::new(key).ok_or(ApplicationError::InvalidCommand)?,
                actual: KeyVersion::new(version),
            }),
            3 if owner == 0 && version == 0 => Some(PrepareRefusal::VersionExhausted {
                key: KeyId::new(key).ok_or(ApplicationError::InvalidCommand)?,
            }),
            _ => return Err(ApplicationError::InvalidCommand),
        };
        Ok(value)
    }
    pub(super) fn outcome(&mut self) -> Result<ProgramOutcome, ApplicationError> {
        let bytes = self.take(35)?;
        let mut r = Reader(bytes);
        let tag = r.byte()?;
        let d = r.decision()?;
        let refusal = r.refusal()?;
        use ProgramOutcome::*;
        let value = match tag {
            0 => Started,
            1 => Prepare(PrepareOutcome::Prepared),
            2 => Prepare(PrepareOutcome::Refused(
                refusal.ok_or(ApplicationError::InvalidCommand)?,
            )),
            3 => Prepare(PrepareOutcome::Closed(
                d.ok_or(ApplicationError::InvalidCommand)?,
            )),
            4 => Decided(d.ok_or(ApplicationError::InvalidCommand)?),
            5 => DecisionApplied(d.ok_or(ApplicationError::InvalidCommand)?),
            6 => AppliedAll(d.ok_or(ApplicationError::InvalidCommand)?),
            7 => Released(d.ok_or(ApplicationError::InvalidCommand)?),
            8 => Finished(d.ok_or(ApplicationError::InvalidCommand)?),
            9 => Conflict(ProgramConflict::Operation),
            10 => Conflict(ProgramConflict::Transaction),
            11 => Conflict(ProgramConflict::Phase),
            _ => return Err(ApplicationError::InvalidCommand),
        };
        let mut canonical = Vec::with_capacity(35);
        write_outcome(&mut canonical, value);
        if canonical != bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(value)
    }
}
