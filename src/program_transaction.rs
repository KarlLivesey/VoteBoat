// SPDX-License-Identifier: RPL-1.5
//! Bounded immutable numeric-key programs over explicit fixed ownership.
//!
//! Serialized internal phase assertions are trusted non-Byzantine host-driver
//! inputs. Application semantic tests do not establish consumed Node provenance;
//! the native program driver is a separate acceptance obligation.
mod driver;
pub use driver::*;
mod types;
pub use types::*;
mod status;
pub use status::*;
mod admission;
mod codec;
mod command;
mod state;
#[cfg(test)]
mod tests;
mod traits;
pub use crate::transaction::TransactionId;
use crate::{
    application::ApplicationError,
    identity::{GroupIdentity, OperationId},
};
use command::Command;
use state::{Key, Record};
use std::collections::BTreeMap;

pub const PROGRAM_SCHEMA: u64 = 0x5642_5052_0000_0001;
pub const PROGRAM_COMMAND_BYTES: usize = 8192;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramApplication {
    group: GroupIdentity,
    profile: ProgramProfile,
    limits: ProgramLimits,
    role: ProgramRole,
    applied: u64,
    keys: BTreeMap<KeyId, Key>,
    records: BTreeMap<TransactionId, Record>,
}
impl ProgramApplication {
    pub fn coordinator(
        group: GroupIdentity,
        profile: ProgramProfile,
        limits: ProgramLimits,
    ) -> Result<Self, ApplicationError> {
        if !profile.coordinators().contains(&group) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self::empty(
            group,
            profile,
            limits,
            ProgramRole::Coordinator,
        ))
    }
    pub fn participant(
        group: GroupIdentity,
        profile: ProgramProfile,
        keys: &[(KeyId, i64)],
        limits: ProgramLimits,
    ) -> Result<Self, ApplicationError> {
        if !profile.participants().contains(&group) || keys.is_empty() || keys.len() > limits.keys()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut app = Self::empty(group, profile, limits, ProgramRole::Participant);
        for &(id, value) in keys {
            if value < 0
                || app
                    .keys
                    .insert(
                        id,
                        Key {
                            opening: value,
                            value,
                            version: KeyVersion::default(),
                            lock: None,
                        },
                    )
                    .is_some()
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(app)
    }
    fn empty(
        group: GroupIdentity,
        profile: ProgramProfile,
        limits: ProgramLimits,
        role: ProgramRole,
    ) -> Self {
        Self {
            group,
            profile,
            limits,
            role,
            applied: 0,
            keys: BTreeMap::new(),
            records: BTreeMap::new(),
        }
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn profile(&self) -> &ProgramProfile {
        &self.profile
    }
    pub fn role(&self) -> ProgramRole {
        self.role
    }
    pub fn limits(&self) -> ProgramLimits {
        self.limits
    }
    pub fn transaction_capacity(&self) -> usize {
        match self.role {
            ProgramRole::Coordinator => self.limits.coordinator_transactions(),
            ProgramRole::Participant => self.limits.participant_transactions(&self.profile),
        }
    }
    pub fn remaining_transactions(&self) -> usize {
        self.transaction_capacity() - self.records.len()
    }
    pub fn contains_key(&self, key: KeyId) -> bool {
        self.keys.contains_key(&key)
    }
    pub fn key_ids(&self) -> impl Iterator<Item = KeyId> + '_ {
        self.keys.keys().copied()
    }
    pub fn key(&self, id: KeyId) -> KeyRead {
        match self.keys.get(&id) {
            None => KeyRead::Unknown,
            Some(key) => match key.lock {
                Some(id) => KeyRead::Locked(id),
                None => KeyRead::Value {
                    value: key.value,
                    version: key.version,
                },
            },
        }
    }
    /// Local diagnostic; distributed authority requires an original quorum read.
    pub fn status(&self, id: TransactionId) -> Option<ProgramStatus> {
        self.records.get(&id).map(|r| r.status(self.group))
    }
    /// Business Start only. Internal phase encoders require the trusted driver.
    pub fn start(program: Program) -> Result<Vec<u8>, ApplicationError> {
        codec::encode(&Command::start(program))
    }
}
