// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Local durable drain intent; never a remote quorum or decommission receipt.
use crate::{
    runtime::{LocalDrainRequest, MAX_LOCAL_DRAIN_GROUPS},
    secure::PeerIdentity,
};
mod membership;
pub use membership::*;
mod coordinator;
pub use coordinator::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainPhase {
    Active,
    Cancelled,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainRecord {
    pub owner: PeerIdentity,
    /// Strictly increasing local administrative identity, not a log index.
    pub sequence: u64,
    pub request: LocalDrainRequest,
    pub phase: DrainPhase,
    /// Immutable host plan binding; None denotes the original local-only gate.
    pub plan: Option<DrainPlanDigest>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainJournalError {
    InvalidRecord,
    MissingRecord,
    WrongOwner,
    Conflict,
    Fenced,
    Io {
        kind: std::io::ErrorKind,
        uncertain: bool,
    },
}
impl DrainRecord {
    pub fn validate(&self) -> Result<(), DrainJournalError> {
        if self.sequence == 0
            || self.request.groups.len() > MAX_LOCAL_DRAIN_GROUPS
            || self
                .request
                .groups
                .windows(2)
                .any(|w| w[0].group >= w[1].group)
        {
            return Err(DrainJournalError::InvalidRecord);
        }
        Ok(())
    }
    /// Only exact retries, cancellation of this intent, or a fresh intent after
    /// cancellation. Old sequence retries cannot become a new drain operation.
    pub fn follows(&self, previous: &Self) -> bool {
        self.owner == previous.owner
            && (self == previous
                || (self.sequence == previous.sequence
                    && self.request == previous.request
                    && self.plan == previous.plan
                    && previous.phase == DrainPhase::Active
                    && self.phase == DrainPhase::Cancelled)
                || (self.sequence > previous.sequence
                    && previous.phase == DrainPhase::Cancelled
                    && self.phase == DrainPhase::Active
                    && self.request.operation != previous.request.operation))
    }
}
/// One exclusive writer. `publish` succeeds only after recoverable publication;
/// rejected writes leave the previous record intact; uncertain writes fence
/// both methods until reopen. The first record must be Active. `latest` reads
/// a verified in-memory view without I/O, blocking or hidden resources.
/// Drive blocking publication outside the consensus owner. Retain the same
/// sequence and operation on retries. Only the latest operation is retained.
pub trait DrainJournal {
    fn owner(&self) -> PeerIdentity;
    fn latest(&self) -> Result<Option<DrainRecord>, DrainJournalError>;
    fn publish(&mut self, record: DrainRecord) -> Result<(), DrainJournalError>;
}
