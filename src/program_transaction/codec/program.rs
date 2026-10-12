// SPDX-License-Identifier: RPL-1.5
use super::*;
pub(super) fn write_profile(out: &mut Vec<u8>, p: &ProgramProfile) {
    out.extend(p.version().to_le_bytes());
    for groups in [p.coordinators(), p.participants()] {
        out.push(groups.len() as u8);
        for g in groups {
            group(out, *g);
        }
    }
}
pub(super) fn write_program(out: &mut Vec<u8>, p: &Program) {
    out.extend(p.id().get().to_le_bytes());
    write_profile(out, p.profile());
    out.push(p.participants().len() as u8);
    for local in p.participants() {
        group(out, local.group());
        out.push(local.keys().len() as u8);
        for key in local.keys() {
            out.extend(key.key.get().to_le_bytes());
            out.extend(key.expected_version.get().to_le_bytes());
            out.push(u8::from(key.write.is_some()));
            out.extend(key.write.unwrap_or(0).to_le_bytes());
        }
    }
}
impl Reader<'_> {
    pub(super) fn profile(&mut self) -> Result<ProgramProfile, ApplicationError> {
        let version = self.u64()?;
        let c = self.groups()?;
        let p = self.groups()?;
        ProgramProfile::new(version, c, p)
    }
    fn groups(&mut self) -> Result<Vec<GroupIdentity>, ApplicationError> {
        let n = self.count_byte(MAX_CATALOG_GROUPS, 24)?;
        (0..n).map(|_| self.group()).collect()
    }
    pub(super) fn program(&mut self) -> Result<Program, ApplicationError> {
        let id = self.transaction()?;
        let profile = self.profile()?;
        let n = self.count_byte(MAX_CATALOG_GROUPS, 25)?;
        let mut participants = Vec::with_capacity(n);
        for _ in 0..n {
            let group = self.group()?;
            let n = self.count_byte(MAX_LOCAL_KEYS, 25)?;
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let key = self.key()?;
                let expected_version = KeyVersion::new(self.u64()?);
                let tag = self.byte()?;
                let value = self.i64()?;
                let write = match (tag, value) {
                    (0, 0) => None,
                    (1, value) => Some(value),
                    _ => return Err(ApplicationError::InvalidCommand),
                };
                keys.push(KeyAccess {
                    key,
                    expected_version,
                    write,
                });
            }
            participants.push(ParticipantProgram::new(group, keys)?);
        }
        Program::new(id, profile, participants)
    }
}
