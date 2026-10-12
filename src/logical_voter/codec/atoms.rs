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
pub(in crate::logical_voter) struct Encoder {
    pub bytes: Vec<u8>,
    limit: usize,
}
impl Encoder {
    pub fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), ApplicationError> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(ApplicationError::InvalidCommand);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub fn u8(&mut self, n: u8) -> Result<(), ApplicationError> {
        self.put(&[n])
    }
    pub fn u64(&mut self, n: u64) -> Result<(), ApplicationError> {
        self.put(&n.to_le_bytes())
    }
    pub fn u128(&mut self, n: u128) -> Result<(), ApplicationError> {
        self.put(&n.to_le_bytes())
    }
    pub fn blob(&mut self, b: &[u8]) -> Result<(), ApplicationError> {
        self.u64(b.len() as u64)?;
        self.put(b)
    }
    pub fn group(&mut self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.u128(g.id.get())?;
        self.u64(g.incarnation.get())
    }
    pub fn store(&mut self, s: StoreIdentity) -> Result<(), ApplicationError> {
        self.u128(s.id.get())?;
        self.u64(s.incarnation.get())
    }
    pub fn binding(&mut self, s: StoreBinding) -> Result<(), ApplicationError> {
        self.store(s.identity)?;
        self.u64(s.session.get())
    }
    pub fn mapping(&mut self, m: LogicalStoreMapping) -> Result<(), ApplicationError> {
        self.group(m.parent)?;
        self.u64(m.voter.get())?;
        self.store(m.store)?;
        self.group(m.child)
    }
    pub fn representative(&mut self, r: RepresentativeBinding) -> Result<(), ApplicationError> {
        self.u64(r.epoch.get())?;
        self.u64(r.physical.node.get())?;
        self.store(r.physical.store)?;
        self.u64(r.physical_session.get())?;
        self.u64(r.logical_session.get())
    }
    pub fn reference(&mut self, r: SnapshotRef) -> Result<(), ApplicationError> {
        self.store(r.store)?;
        self.group(r.group)?;
        for n in [
            r.generation.get(),
            r.configuration.get(),
            r.index,
            r.term,
            r.application_schema,
            r.file_bytes,
            r.checksum as u64,
        ] {
            self.u64(n)?;
        }
        Ok(())
    }
    pub fn optional_reference(&mut self, r: Option<SnapshotRef>) -> Result<(), ApplicationError> {
        self.u8(u8::from(r.is_some()))?;
        if let Some(r) = r {
            self.reference(r)?;
        }
        Ok(())
    }
}
pub(in crate::logical_voter) struct Decoder<'a> {
    pub bytes: &'a [u8],
    pub offset: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let end = self
            .offset
            .checked_add(n)
            .filter(|n| *n <= self.bytes.len())
            .ok_or(ApplicationError::InvalidCommand)?;
        let b = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(b)
    }
    pub fn u8(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    pub fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn id<T>(&mut self, f: impl FnOnce(u64) -> Option<T>) -> Result<T, ApplicationError> {
        f(self.u64()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub fn id128<T>(&mut self, f: impl FnOnce(u128) -> Option<T>) -> Result<T, ApplicationError> {
        f(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub fn blob(&mut self, limit: usize) -> Result<&'a [u8], ApplicationError> {
        let n = usize::try_from(self.u64()?)
            .ok()
            .filter(|n| *n <= limit)
            .ok_or(ApplicationError::InvalidCommand)?;
        self.take(n)
    }
    pub fn flag(&mut self) -> Result<bool, ApplicationError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub fn group(&mut self) -> Result<GroupIdentity, ApplicationError> {
        Ok(GroupIdentity {
            id: self.id128(GroupId::new)?,
            incarnation: self.id(GroupIncarnation::new)?,
        })
    }
    pub fn store(&mut self) -> Result<StoreIdentity, ApplicationError> {
        Ok(StoreIdentity {
            id: self.id128(StoreId::new)?,
            incarnation: self.id(StoreIncarnation::new)?,
        })
    }
    pub fn binding(&mut self) -> Result<StoreBinding, ApplicationError> {
        Ok(StoreBinding {
            identity: self.store()?,
            session: self.id(StoreSession::new)?,
        })
    }
    pub fn mapping(&mut self) -> Result<LogicalStoreMapping, ApplicationError> {
        LogicalStoreMapping::new(
            self.group()?,
            self.id(NodeId::new)?,
            self.store()?,
            self.group()?,
        )
    }
    pub fn representative(&mut self) -> Result<RepresentativeBinding, ApplicationError> {
        Ok(RepresentativeBinding {
            epoch: self.id(RepresentativeEpoch::new)?,
            physical: crate::secure::PeerIdentity {
                node: self.id(NodeId::new)?,
                store: self.store()?,
            },
            physical_session: self.id(StoreSession::new)?,
            logical_session: self.id(StoreSession::new)?,
        })
    }
    pub fn reference(&mut self) -> Result<SnapshotRef, ApplicationError> {
        Ok(SnapshotRef {
            store: self.store()?,
            group: self.group()?,
            generation: self.id(SnapshotGeneration::new)?,
            configuration: self.id(ConfigurationId::new)?,
            index: self.u64()?,
            term: self.u64()?,
            application_schema: self.u64()?,
            file_bytes: self.u64()?,
            checksum: u32::try_from(self.u64()?).map_err(|_| ApplicationError::InvalidCommand)?,
        })
    }
    pub fn optional_reference(&mut self) -> Result<Option<SnapshotRef>, ApplicationError> {
        if self.flag()? {
            Ok(Some(self.reference()?))
        } else {
            Ok(None)
        }
    }
    pub fn finish(&self) -> Result<(), ApplicationError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
}
