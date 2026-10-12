// SPDX-License-Identifier: RPL-1.5
use crate::{application::ApplicationError, identity::*, transaction::TransactionId};
use std::num::NonZeroU64;

pub const MAX_CATALOG_GROUPS: usize = 8;
pub const MAX_LOCAL_KEYS: usize = 16;
pub const MAX_PROGRAM_KEYS: usize = MAX_CATALOG_GROUPS * MAX_LOCAL_KEYS;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct KeyId(NonZeroU64);
impl KeyId {
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct KeyVersion(u64);
impl KeyVersion {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}
/// A fixed ownership catalog; voter membership is not frozen by this profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramProfile {
    version: NonZeroU64,
    coordinators: Box<[GroupIdentity]>,
    participants: Box<[GroupIdentity]>,
}
impl ProgramProfile {
    pub fn new(
        version: u64,
        coordinators: Vec<GroupIdentity>,
        participants: Vec<GroupIdentity>,
    ) -> Result<Self, ApplicationError> {
        if !ordered(&coordinators)
            || !ordered(&participants)
            || coordinators.iter().any(|g| participants.contains(g))
            || coordinators
                .iter()
                .chain(&participants)
                .enumerate()
                .any(|(i, g)| {
                    coordinators
                        .iter()
                        .chain(&participants)
                        .take(i)
                        .any(|old| old.id == g.id)
                })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            version: NonZeroU64::new(version).ok_or(ApplicationError::InvalidCommand)?,
            coordinators: coordinators.into_boxed_slice(),
            participants: participants.into_boxed_slice(),
        })
    }
    pub fn version(&self) -> u64 {
        self.version.get()
    }
    pub fn coordinators(&self) -> &[GroupIdentity] {
        &self.coordinators
    }
    pub fn participants(&self) -> &[GroupIdentity] {
        &self.participants
    }
    pub fn coordinator(&self, id: TransactionId) -> GroupIdentity {
        self.coordinators[(id.get() % self.coordinators.len() as u128) as usize]
    }
}
fn ordered(groups: &[GroupIdentity]) -> bool {
    !groups.is_empty()
        && groups.len() <= MAX_CATALOG_GROUPS
        && groups.windows(2).all(|p| p[0] < p[1])
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyAccess {
    pub key: KeyId,
    pub expected_version: KeyVersion,
    /// None retains a read dependency; Some sets the final value on commit.
    pub write: Option<i64>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantProgram {
    group: GroupIdentity,
    pub(super) keys: Box<[KeyAccess]>,
}
impl ParticipantProgram {
    pub fn new(group: GroupIdentity, keys: Vec<KeyAccess>) -> Result<Self, ApplicationError> {
        if keys.is_empty()
            || keys.len() > MAX_LOCAL_KEYS
            || !keys.windows(2).all(|k| k[0].key < k[1].key)
            || keys.iter().any(|k| k.write.is_some_and(|v| v < 0))
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            group,
            keys: keys.into_boxed_slice(),
        })
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn keys(&self) -> &[KeyAccess] {
        &self.keys
    }
}
/// Immutable bounded compare/version-check program. IDs never change on retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Program {
    id: TransactionId,
    profile: ProgramProfile,
    pub(super) participants: Box<[ParticipantProgram]>,
}
impl Program {
    pub fn new(
        id: TransactionId,
        profile: ProgramProfile,
        participants: Vec<ParticipantProgram>,
    ) -> Result<Self, ApplicationError> {
        if id
            .get()
            .checked_mul(4)
            .and_then(|v| v.checked_add(3))
            .is_none()
            || participants.is_empty()
            || participants.len() > MAX_CATALOG_GROUPS
            || !participants.windows(2).all(|p| p[0].group < p[1].group)
            || participants
                .iter()
                .any(|p| !profile.participants.contains(&p.group))
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            id,
            profile,
            participants: participants.into_boxed_slice(),
        })
    }
    pub fn id(&self) -> TransactionId {
        self.id
    }
    pub fn profile(&self) -> &ProgramProfile {
        &self.profile
    }
    pub fn coordinator(&self) -> GroupIdentity {
        self.profile.coordinator(self.id)
    }
    pub fn participants(&self) -> &[ParticipantProgram] {
        &self.participants
    }
    pub fn operation(&self, local_slot: usize) -> Result<OperationId, ApplicationError> {
        if local_slot > 3 {
            return Err(ApplicationError::InvalidCommand);
        }
        OperationId::new(self.id.get() * 4 + local_slot as u128)
            .ok_or(ApplicationError::InvalidCommand)
    }
    pub(crate) fn participant(&self, group: GroupIdentity) -> Option<usize> {
        self.participants.iter().position(|p| p.group == group)
    }
    pub(crate) fn nested_bytes(&self) -> usize {
        (self.profile.coordinators.len() + self.profile.participants.len())
            * std::mem::size_of::<GroupIdentity>()
            + self.participants.len() * std::mem::size_of::<ParticipantProgram>()
            + self
                .participants
                .iter()
                .map(|p| std::mem::size_of_val(&*p.keys))
                .sum::<usize>()
    }
}
/// Every coordinator retains this many transactions; every participant covers
/// the whole catalog's aggregate lifetime quota, including absent Abort records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramLimits {
    coordinator_transactions: usize,
    keys: usize,
}
impl ProgramLimits {
    pub fn new(coordinator_transactions: usize, keys: usize) -> Result<Self, ApplicationError> {
        if coordinator_transactions == 0
            || coordinator_transactions > 256
            || keys == 0
            || keys > MAX_PROGRAM_KEYS
        {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(Self {
            coordinator_transactions,
            keys,
        })
    }
    pub fn coordinator_transactions(self) -> usize {
        self.coordinator_transactions
    }
    pub fn keys(self) -> usize {
        self.keys
    }
    pub fn participant_transactions(self, profile: &ProgramProfile) -> usize {
        self.coordinator_transactions * profile.coordinators.len()
    }
}
