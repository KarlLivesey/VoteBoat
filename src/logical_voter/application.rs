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
//! Single authoritative parent image and retained original child actions.
use super::*;
mod authorize;
mod enrollment;
mod execute;
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct State {
    pub applied: u64,
    pub applied_term: u64,
    pub membership: Membership,
    pub parent: Option<GroupLog>,
    pub representative: Option<RepresentativeBinding>,
    pub records: BTreeMap<OperationId, LogicalActionStatus>,
    pub configurations: Vec<LogEntry>,
    pub snapshots: Vec<(SnapshotRef, Snapshot)>,
    pub published: Option<SnapshotRef>,
    pub bytes: usize,
    pub enrollment: Option<Box<LogicalEnrollmentRecord>>,
    /// Session-space retirement watermark (a contiguous prefix). Always 0
    /// until retirement is implemented; carried by checkpoint v3.
    pub retired_below: u128,
    /// High-water mark (a maximum) of allocated parent snapshot generations,
    /// so numbering never depends on retained records.
    pub snapshot_generation: u64,
}
impl State {
    pub fn new(bootstrap: &Bootstrap) -> Result<Self, ApplicationError> {
        Ok(Self {
            applied: 0,
            applied_term: 0,
            membership: Membership::replay(bootstrap, &[], 0)
                .map_err(|_| ApplicationError::InvalidCommand)?,
            parent: None,
            representative: None,
            records: BTreeMap::new(),
            configurations: Vec::new(),
            snapshots: Vec::new(),
            published: None,
            bytes: 0,
            enrollment: None,
            retired_below: 0,
            snapshot_generation: 0,
        })
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn apply_entries(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<LogicalReceipt>, ApplicationError> {
        #[cfg(test)]
        WORK.with(|w| w.set((w.get().0 + 1, w.get().1)));
        let mut receipts = Vec::with_capacity(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if entry.index
                != self
                    .state
                    .applied
                    .checked_add(1)
                    .ok_or(ApplicationError::IndexGap)?
            {
                return Err(ApplicationError::IndexGap);
            }
            if entry.term == 0 || entry.term < self.state.applied_term {
                return Err(ApplicationError::InvalidCommand);
            }
            match &entry.payload {
                EntryPayload::Noop => {}
                EntryPayload::Configuration(record) => {
                    self.state.membership = self
                        .state
                        .membership
                        .preview_next(entry.index, record, entry.index - 1)
                        .map_err(|_| ApplicationError::InvalidCommand)?;
                    self.state.configurations.push(entry.clone());
                }
                EntryPayload::Command { operation, bytes } => {
                    receipts.push(self.apply_command(entry, *operation, bytes)?)
                }
            }
            self.state.applied = entry.index;
            self.state.applied_term = entry.term;
        }
        self.validate_read_envelopes()?;
        self.encode_state(self.limits.max_child_checkpoint_bytes)?;
        Ok(receipts)
    }
    fn apply_command(
        &mut self,
        entry: &LogEntry,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<LogicalReceipt, ApplicationError> {
        let command = self.decode_command(bytes)?;
        if command.operation != operation {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(old) = self.state.records.get(&operation) {
            if old.command != bytes {
                return Err(ApplicationError::OperationConflict);
            }
            let mut receipt = old.receipt;
            receipt.index = entry.index;
            receipt.duplicate = true;
            return Ok(receipt);
        }
        self.reserve(&[(
            operation,
            bytes,
            command.control(self.control_profile.as_ref()),
        )])?;
        let configuration = self.state.membership.id();
        let outcome = if self
            .state
            .membership
            .voter_store(command.representative.physical.node)
            != Some(command.representative.physical.store)
        {
            LogicalOutcome::Refused(LogicalRefusal::Ineligible)
        } else {
            self.execute(&command, entry.index, entry.term)?
        };
        let receipt = LogicalReceipt {
            index: entry.index,
            operation,
            first_index: entry.index,
            outcome,
            parent_revision: self.state.parent.as_ref().map(|p| p.revision),
            parent_generation: self.state.parent.as_ref().map(|p| p.generation),
            epoch: self.state.representative.map(|r| r.epoch),
            duplicate: false,
        };
        self.state.bytes = self
            .state
            .bytes
            .checked_add(bytes.len())
            .ok_or(ApplicationError::DedupCapacity)?;
        self.state.records.insert(
            operation,
            LogicalActionStatus {
                operation,
                first_index: entry.index,
                first_term: entry.term,
                preceding_configuration: configuration,
                command: bytes.to_vec(),
                receipt,
            },
        );
        Ok(receipt)
    }
}

// Per-thread counts of `apply_entries` and `encode_state` calls, so tests can
// show that restore does no replay and encodes once.
#[cfg(test)]
std::thread_local! {
    static WORK: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}
#[cfg(test)]
pub(super) fn count_encode() {
    WORK.with(|w| w.set((w.get().0, w.get().1 + 1)));
}
#[cfg(test)]
pub(super) fn work() -> (u64, u64) {
    WORK.with(std::cell::Cell::get)
}
#[cfg(test)]
mod tests;
