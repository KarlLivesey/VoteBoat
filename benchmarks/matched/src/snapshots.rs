// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded volatile publication and two exact log anchors; no disk durability.
use std::collections::BTreeMap;
use voteboat::snapshot::*;
use voteboat::{contracts::StorageError, identity::*};
/// Downstream logical provider; no native framing or files are required.
type HostStage = (
    SnapshotTicket,
    SnapshotMetadata,
    usize,
    Vec<u8>,
    Option<SealedSnapshot>,
);
#[derive(Clone)]
pub(super) struct MemorySnapshots {
    pub identity: SnapshotIdentity,
    pub binding: StoreBinding,
    pub limits: SnapshotLimits,
    generation: u64,
    pub current: Option<Snapshot>,
    pending: Option<HostStage>,
    reference: Option<SnapshotRef>,
    pub pins: BTreeMap<SnapshotGeneration, (SnapshotRef, Snapshot)>,
}
impl MemorySnapshots {
    pub fn new(binding: StoreBinding, group: GroupIdentity) -> Self {
        Self {
            identity: SnapshotIdentity {
                store: binding.identity,
                group,
            },
            binding,
            limits: SnapshotLimits::default(),
            generation: 0,
            current: None,
            pending: None,
            reference: None,
            pins: BTreeMap::new(),
        }
    }
}
fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
impl SnapshotStore for MemorySnapshots {
    fn identity(&self) -> SnapshotIdentity {
        self.identity
    }
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotLimits {
        self.limits
    }
    fn begin(&mut self, m: SnapshotMetadata, n: usize) -> Result<SnapshotTicket, StorageError> {
        m.validate()?;
        if m.bootstrap.group != self.identity.group {
            return Err(StorageError::WrongIdentity);
        }
        if self.pending.is_some()
            || self.pins.values().any(|(r, _)| Some(*r) != self.reference)
            || n == 0
            || n > self.limits.max_application_bytes
            || self
                .current
                .as_ref()
                .is_some_and(|s| !m.follows(&s.metadata))
        {
            return Err(StorageError::Rejected("stage limits or regression"));
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(StorageError::Rejected("snapshot generation exhausted"))?;
        let ticket = SnapshotTicket {
            binding: self.binding,
            group: self.identity.group,
            generation: SnapshotGeneration::new(self.generation).unwrap(),
        };
        self.pending = Some((ticket, m, n, Vec::new(), None));
        Ok(ticket)
    }
    fn write_chunk(
        &mut self,
        ticket: SnapshotTicket,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let p = self
            .pending
            .as_mut()
            .filter(|p| p.0 == ticket)
            .ok_or(StorageError::StaleTicket)?;
        if p.4.is_some()
            || offset != p.3.len()
            || bytes.is_empty()
            || bytes.len() > self.limits.max_chunk_bytes
            || bytes.len() > p.2.saturating_sub(p.3.len())
        {
            return Err(StorageError::Rejected("chunk limits"));
        }
        p.3.extend(bytes);
        Ok(())
    }
    fn seal(&mut self, ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        let p = self
            .pending
            .as_mut()
            .filter(|p| p.0 == ticket)
            .ok_or(StorageError::StaleTicket)?;
        if p.4.is_some() || p.3.len() != p.2 {
            return Err(StorageError::Rejected("incomplete stage"));
        }
        let sealed = SealedSnapshot {
            ticket,
            // Include the contract's minimum snapshot envelope even when an
            // empty application's checkpoint payload is smaller than 48 bytes.
            file_bytes: 48 + p.2 as u64,
            checksum: checksum(&p.3),
        };
        p.4 = Some(sealed);
        Ok(sealed)
    }
    fn publish(&mut self, sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        if self.pending.as_ref().is_none_or(|p| p.4 != Some(sealed)) {
            return Err(StorageError::StaleTicket);
        }
        let (_, metadata, _, application, _) = self.pending.take().unwrap();
        self.current = Some(Snapshot {
            metadata: metadata.clone(),
            application,
        });
        let receipt = SnapshotReceipt { sealed, metadata };
        self.reference = Some(receipt.reference());
        Ok(receipt)
    }
    fn abort(&mut self, ticket: SnapshotTicket) -> Result<(), StorageError> {
        if self.pending.as_ref().is_none_or(|p| p.0 != ticket) {
            return Err(StorageError::StaleTicket);
        }
        self.pending = None;
        Ok(())
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        if let Some(reference) = self.reference {
            let image = self
                .current
                .as_ref()
                .ok_or(StorageError::Corrupt("missing volatile publication"))?;
            if !reference.matches(image) || checksum(&image.application) != reference.checksum {
                return Err(StorageError::Corrupt("volatile publication checksum"));
            }
        } else if self.current.is_some() {
            return Err(StorageError::Corrupt("unreferenced volatile publication"));
        }
        Ok(self.current.clone())
    }
}
impl SnapshotRetention for MemorySnapshots {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        Ok(self.reference)
    }
    fn pin_for_log(&mut self, r: SnapshotRef) -> Result<(), StorageError> {
        if let Some((old, _)) = self.pins.get(&r.generation) {
            return if *old == r {
                Ok(())
            } else {
                Err(StorageError::StaleTicket)
            };
        }
        if self.reference != Some(r) || self.pins.len() >= 2 {
            return Err(StorageError::StaleTicket);
        }
        self.pins
            .insert(r.generation, (r, self.current.clone().unwrap()));
        Ok(())
    }
    fn load_pinned(&mut self, r: SnapshotRef) -> Result<Snapshot, StorageError> {
        let (_, image) = self
            .pins
            .get(&r.generation)
            .filter(|(p, _)| *p == r)
            .ok_or(StorageError::StaleTicket)?;
        if !r.matches(image) || checksum(&image.application) != r.checksum {
            return Err(StorageError::Corrupt("volatile pinned image checksum"));
        }
        Ok(image.clone())
    }
    fn reconcile_log(&mut self, r: Option<SnapshotRef>) -> Result<(), StorageError> {
        if self.pending.is_some() {
            return Err(StorageError::Rejected("stage pending"));
        }
        self.current = r.map(|p| self.load_pinned(p)).transpose()?;
        self.reference = r;
        self.pins.retain(|_, (p, _)| Some(*p) == r);
        Ok(())
    }
}
