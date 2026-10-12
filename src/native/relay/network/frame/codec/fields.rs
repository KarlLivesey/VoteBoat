// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fixed identity fields; no unbounded recursive metadata or compatibility fallback.
use super::*;
pub(super) struct Encoder {
    pub bytes: Vec<u8>,
    maximum: usize,
}
impl Encoder {
    pub fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
    pub fn put(&mut self, b: &[u8]) -> Result<(), RelayNetworkError> {
        if b.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(RelayNetworkError::TooLarge);
        }
        self.bytes.extend_from_slice(b);
        Ok(())
    }
    pub fn u8(&mut self, n: u8) -> Result<(), RelayNetworkError> {
        self.put(&[n])
    }
    pub fn u16(&mut self, n: u16) -> Result<(), RelayNetworkError> {
        self.put(&n.to_le_bytes())
    }
    pub fn u64(&mut self, n: u64) -> Result<(), RelayNetworkError> {
        self.put(&n.to_le_bytes())
    }
    fn store(&mut self, s: StoreBinding) -> Result<(), RelayNetworkError> {
        self.put(&s.identity.id.get().to_le_bytes())?;
        self.u64(s.identity.incarnation.get())?;
        self.u64(s.session.get())
    }
    fn local(&mut self, i: LocalIdentity) -> Result<(), RelayNetworkError> {
        self.u64(i.node.get())?;
        self.store(i.store)
    }
    pub fn source(&mut self, s: RelaySourceCapture) -> Result<(), RelayNetworkError> {
        self.u16(s.original_wire_version)?;
        self.u8(s.path.len() as u8)?;
        for binding in s.path.hops() {
            self.local(binding.local)?;
            self.local(binding.peer)?;
            self.u64(binding.generation.get())?;
            self.u16(binding.wire_version)?;
        }
        Ok(())
    }
    pub fn ticket(&mut self, t: SendTicket) -> Result<(), RelayNetworkError> {
        self.u64(t.binding.node.get())?;
        self.store(t.binding.store)?;
        self.u64(t.binding.generation.get())?;
        self.u64(t.peer.get())?;
        self.u64(t.sequence)
    }
    pub fn blob(&mut self, bytes: &[u8]) -> Result<(), RelayNetworkError> {
        self.u64(bytes.len() as u64)?;
        self.put(bytes)
    }
}
pub(super) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], RelayNetworkError> {
        let end = self
            .position
            .checked_add(n)
            .ok_or(RelayNetworkError::InvalidFrame)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(RelayNetworkError::InvalidFrame)?;
        self.position = end;
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8, RelayNetworkError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, RelayNetworkError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, RelayNetworkError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn positive(&mut self) -> Result<u64, RelayNetworkError> {
        let n = self.u64()?;
        if n == 0 {
            Err(RelayNetworkError::InvalidFrame)
        } else {
            Ok(n)
        }
    }
    fn store(&mut self) -> Result<StoreBinding, RelayNetworkError> {
        let id = StoreId::new(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
            .ok_or(RelayNetworkError::InvalidFrame)?;
        let incarnation = StoreIncarnation::new(self.positive()?).unwrap();
        let session = StoreSession::new(self.positive()?).unwrap();
        Ok(StoreBinding {
            identity: StoreIdentity { id, incarnation },
            session,
        })
    }
    fn local(&mut self) -> Result<LocalIdentity, RelayNetworkError> {
        let node = NodeId::new(self.positive()?).unwrap();
        Ok(LocalIdentity {
            node,
            store: self.store()?,
        })
    }
    pub fn source(&mut self) -> Result<RelaySourceCapture, RelayNetworkError> {
        let original_wire_version = self.u16()?;
        let count = usize::from(self.u8()?);
        if original_wire_version == 0 || !(1..=AUTHENTICATED_DELEGATION_MAX_HOPS).contains(&count) {
            return Err(RelayNetworkError::InvalidFrame);
        }
        let binding = self.session()?;
        let mut path = AuthenticatedDelegationPath::leaf(binding);
        for _ in 1..count {
            path = path
                .extended(self.session()?)
                .ok_or(RelayNetworkError::WrongSource)?;
        }
        Ok(RelaySourceCapture {
            binding,
            original_wire_version,
            path,
        })
    }
    fn session(&mut self) -> Result<SessionBinding, RelayNetworkError> {
        let local = self.local()?;
        let peer = self.local()?;
        let generation = SecureSessionGeneration::new(self.positive()?).unwrap();
        let wire_version = self.u16()?;
        if wire_version != RELAY_NETWORK_VERSION || local.node == peer.node {
            return Err(RelayNetworkError::WrongProfile);
        }
        Ok(SessionBinding {
            local,
            peer,
            generation,
            wire_version,
        })
    }
    pub fn ticket(&mut self) -> Result<SendTicket, RelayNetworkError> {
        let node = NodeId::new(self.positive()?).unwrap();
        let store = self.store()?;
        let generation = OutboundGeneration::new(self.positive()?).unwrap();
        let peer = NodeId::new(self.positive()?).unwrap();
        let sequence = self.positive()?;
        if node == peer {
            return Err(RelayNetworkError::WrongBinding);
        }
        Ok(SendTicket {
            binding: OutboundBinding {
                node,
                store,
                generation,
            },
            peer,
            sequence,
        })
    }
    pub fn blob(&mut self, maximum: usize) -> Result<&'a [u8], RelayNetworkError> {
        let n = usize::try_from(self.u64()?)
            .ok()
            .filter(|n| *n <= maximum)
            .ok_or(RelayNetworkError::TooLarge)?;
        self.take(n)
    }
    pub fn finish(self) -> Result<(), RelayNetworkError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(RelayNetworkError::InvalidFrame)
        }
    }
}
