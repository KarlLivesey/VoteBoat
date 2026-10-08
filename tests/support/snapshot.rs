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
use voteboat::snapshot::*;
/// Downstream logical provider; no native framing or files are required.
type HostStage = (
    SnapshotTicket,
    SnapshotMetadata,
    usize,
    Vec<u8>,
    Option<SealedSnapshot>,
);
#[derive(Clone)]
pub struct HostSnapshots {
    pub identity: SnapshotIdentity,
    pub binding: StoreBinding,
    pub limits: SnapshotLimits,
    generation: u64,
    pub current: Option<Snapshot>,
    pending: Option<HostStage>,
    reference: Option<SnapshotRef>,
    pub pins: BTreeMap<SnapshotGeneration, (SnapshotRef, Snapshot)>,
}
impl HostSnapshots {
    pub fn new() -> Self {
        Self {
            identity: SnapshotIdentity {
                store: identity(1),
                group: group(1),
            },
            binding: StoreBinding {
                identity: identity(1),
                session: StoreSession::new(1).unwrap(),
            },
            limits: SnapshotLimits {
                max_chunk_bytes: 4,
                ..SnapshotLimits::default()
            },
            generation: 0,
            current: None,
            pending: None,
            reference: None,
            pins: BTreeMap::new(),
        }
    }
}
impl SnapshotStore for HostSnapshots {
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
        self.generation += 1;
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
            file_bytes: p.2 as u64,
            checksum: ticket.generation.get() as u32,
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
        Ok(self.current.clone())
    }
}
impl SnapshotRetention for HostSnapshots {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        Ok(self.reference)
    }
    fn pin_for_log(&mut self, r: SnapshotRef) -> Result<(), StorageError> {
        if self.pins.get(&r.generation).is_some_and(|(p, _)| *p == r) {
            return Ok(());
        }
        if self.reference != Some(r) || self.pins.len() >= 2 {
            return Err(StorageError::StaleTicket);
        }
        self.pins
            .insert(r.generation, (r, self.current.clone().unwrap()));
        Ok(())
    }
    fn load_pinned(&mut self, r: SnapshotRef) -> Result<Snapshot, StorageError> {
        self.pins
            .get(&r.generation)
            .filter(|(p, _)| *p == r)
            .map(|(_, s)| s.clone())
            .ok_or(StorageError::StaleTicket)
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
