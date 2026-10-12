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
pub(super) struct RecoveryImage {
    pub binding: StoreBinding,
    pub image: LogicalImage,
    pub snapshots: Vec<(SnapshotRef, Snapshot)>,
    pub limits: LogicalLimits,
}
pub struct LogicalLogView {
    pub(super) state: Arc<RecoveryImage>,
}
pub struct LogicalSnapshotView {
    pub(super) state: Arc<RecoveryImage>,
}
const READ_ONLY: StorageError = StorageError::Rejected("immutable logical recovery view");
impl LogStore for LogicalLogView {
    fn binding(&self) -> StoreBinding {
        self.state.binding
    }
    fn limits(&self) -> LogLimits {
        self.state.limits.parent_log
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        if group != self.state.image.mapping.parent {
            return Err(StorageError::WrongIdentity);
        }
        Ok(self.state.image.parent.clone())
    }
    fn append_batch(
        &mut self,
        _mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        Err(READ_ONLY)
    }
    fn barrier(&mut self, _dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        Err(READ_ONLY)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let state = self.state(group)?;
        if generation != state.generation {
            return Err(StorageError::StaleTicket);
        }
        if from <= state.base_index() {
            return Err(StorageError::Compacted {
                first_index: state.base_index() + 1,
            });
        }
        let mut bytes = 0usize;
        let mut entries = Vec::new();
        for entry in state
            .entries
            .iter()
            .filter(|e| e.index >= from)
            .take(max_entries)
        {
            let cost = std::mem::size_of::<LogEntry>()
                .checked_add(entry.retained_payload_bytes())
                .ok_or(StorageError::Rejected("logical range overflow"))?;
            let next = bytes
                .checked_add(cost)
                .ok_or(StorageError::Rejected("logical range overflow"))?;
            if next > max_bytes {
                if entries.is_empty() {
                    return Err(StorageError::Rejected("logical range budget"));
                }
                break;
            }
            entries.push(entry.clone());
            bytes = next;
        }
        Ok(entries)
    }
}
impl SnapshotStore for LogicalSnapshotView {
    fn identity(&self) -> SnapshotIdentity {
        SnapshotIdentity {
            store: self.state.binding.identity,
            group: self.state.image.mapping.parent,
        }
    }
    fn binding(&self) -> StoreBinding {
        self.state.binding
    }
    fn limits(&self) -> SnapshotLimits {
        self.state.limits.parent_snapshots
    }
    fn begin(
        &mut self,
        _metadata: SnapshotMetadata,
        _application_bytes: usize,
    ) -> Result<SnapshotTicket, StorageError> {
        Err(READ_ONLY)
    }
    fn write_chunk(
        &mut self,
        _ticket: SnapshotTicket,
        _offset: usize,
        _bytes: &[u8],
    ) -> Result<(), StorageError> {
        Err(READ_ONLY)
    }
    fn seal(&mut self, _ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        Err(READ_ONLY)
    }
    fn publish(&mut self, _sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        Err(READ_ONLY)
    }
    fn abort(&mut self, _ticket: SnapshotTicket) -> Result<(), StorageError> {
        Err(READ_ONLY)
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        self.state
            .image
            .published
            .map(|r| self.load_image(r))
            .transpose()
    }
}
impl LogicalSnapshotView {
    fn load_image(&self, reference: SnapshotRef) -> Result<Snapshot, StorageError> {
        self.state
            .snapshots
            .iter()
            .find(|(r, _)| *r == reference)
            .map(|(_, s)| s.clone())
            .ok_or(StorageError::Corrupt(
                "logical snapshot missing from quorum image",
            ))
    }
}
impl SnapshotRetention for LogicalSnapshotView {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        Ok(self.state.image.published)
    }
    fn pin_for_log(&mut self, _reference: SnapshotRef) -> Result<(), StorageError> {
        Err(READ_ONLY)
    }
    fn load_pinned(&mut self, reference: SnapshotRef) -> Result<Snapshot, StorageError> {
        if !self.state.image.pins.contains(&reference) {
            return Err(StorageError::WrongIdentity);
        }
        self.load_image(reference)
    }
    fn reconcile_log(&mut self, _reference: Option<SnapshotRef>) -> Result<(), StorageError> {
        Err(READ_ONLY)
    }
}
