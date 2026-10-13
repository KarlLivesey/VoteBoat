// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded volatile publication and two exact log anchors; no disk durability.
use crate::attribution::{Phase, Recorder};
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
pub(super) struct MemorySnapshots<const TRACE: bool = false> {
    pub attribution: Recorder<TRACE>,
    pub identity: SnapshotIdentity,
    pub binding: StoreBinding,
    pub limits: SnapshotLimits,
    generation: u64,
    pub current: Option<Snapshot>,
    pending: Option<HostStage>,
    reference: Option<SnapshotRef>,
    pub pins: BTreeMap<SnapshotGeneration, (SnapshotRef, Snapshot)>,
}
impl<const TRACE: bool> MemorySnapshots<TRACE> {
    pub fn new(binding: StoreBinding, group: GroupIdentity) -> Self {
        Self {
            attribution: Recorder::default(),
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
impl<const TRACE: bool> SnapshotStore for MemorySnapshots<TRACE> {
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
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotBegin, started, result.is_err(), 0);
        result
    }
    fn write_chunk(
        &mut self,
        ticket: SnapshotTicket,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotWrite, started, result.is_err(), bytes.len());
        result
    }
    fn seal(&mut self, ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotSeal, started, result.is_err(), 0);
        result
    }
    fn publish(&mut self, sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotPublish, started, result.is_err(), 0);
        result
    }
    fn abort(&mut self, ticket: SnapshotTicket) -> Result<(), StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
            if self.pending.as_ref().is_none_or(|p| p.0 != ticket) {
                return Err(StorageError::StaleTicket);
            }
            self.pending = None;
            Ok(())
        })();
        self.attribution
            .finish(Phase::SnapshotAbort, started, result.is_err(), 0);
        result
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotLoad, started, result.is_err(), 0);
        result
    }
}
impl<const TRACE: bool> SnapshotRetention for MemorySnapshots<TRACE> {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        let started = Recorder::<TRACE>::start();
        let result: Result<_, StorageError> = Ok(self.reference);
        self.attribution
            .finish(Phase::SnapshotLatest, started, result.is_err(), 0);
        result
    }
    fn pin_for_log(&mut self, r: SnapshotRef) -> Result<(), StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
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
        })();
        self.attribution
            .finish(Phase::SnapshotPin, started, result.is_err(), 0);
        result
    }
    fn load_pinned(&mut self, r: SnapshotRef) -> Result<Snapshot, StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
            let (_, image) = self
                .pins
                .get(&r.generation)
                .filter(|(p, _)| *p == r)
                .ok_or(StorageError::StaleTicket)?;
            if !r.matches(image) || checksum(&image.application) != r.checksum {
                return Err(StorageError::Corrupt("volatile pinned image checksum"));
            }
            Ok(image.clone())
        })();
        self.attribution
            .finish(Phase::SnapshotLoadPinned, started, result.is_err(), 0);
        result
    }
    fn reconcile_log(&mut self, r: Option<SnapshotRef>) -> Result<(), StorageError> {
        let started = Recorder::<TRACE>::start();
        let result = (|| {
            if self.pending.is_some() {
                return Err(StorageError::Rejected("stage pending"));
            }
            self.current = r.map(|p| self.load_pinned(p)).transpose()?;
            self.reference = r;
            self.pins.retain(|_, (p, _)| Some(*p) == r);
            Ok(())
        })();
        self.attribution
            .finish(Phase::SnapshotReconcile, started, result.is_err(), 0);
        result
    }
}
