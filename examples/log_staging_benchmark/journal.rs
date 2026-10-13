// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::{cell::RefCell, io, rc::Rc};
use voteboat::native::log_store::JournalIo;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub wal_bytes: usize,
    pub manifest_bytes: usize,
    pub appends: usize,
    pub syncs: usize,
    pub publications: usize,
}

pub(super) struct Device {
    bytes: Vec<u8>,
    synchronized: usize,
    manifest: Option<Vec<u8>>,
    limit: usize,
    pub counts: Counts,
}

impl Device {
    pub fn new(limit: usize, capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.min(limit)),
            synchronized: 0,
            manifest: None,
            limit,
            counts: Counts::default(),
        }
    }

    /// Simulates the selection of acknowledged bytes, never physical durability.
    pub fn power_loss(&mut self) {
        self.bytes.truncate(self.synchronized);
    }
}

#[derive(Clone)]
pub(super) struct VolatileIo(pub Rc<RefCell<Device>>);

impl JournalIo for VolatileIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .manifest
            .clone()
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let device = self.0.borrow();
        if device.bytes.len() > limit {
            return Err(io::Error::other("volatile journal read exceeds budget"));
        }
        Ok(device.bytes.clone())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        let ceiling = device.limit.min(device.bytes.capacity());
        if bytes.len() > ceiling.saturating_sub(device.bytes.len()) {
            return Err(io::Error::other("volatile journal append exceeds budget"));
        }
        device.bytes.extend_from_slice(bytes);
        device.counts.wal_bytes += bytes.len();
        device.counts.appends += 1;
        Ok(())
    }

    fn sync_log(&mut self) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        device.synchronized = device.bytes.len();
        device.counts.syncs += 1;
        Ok(())
    }

    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        let length = usize::try_from(length)
            .map_err(|_| io::Error::other("volatile journal length overflow"))?;
        let mut device = self.0.borrow_mut();
        if length > device.bytes.len() {
            return Err(io::Error::other(
                "volatile journal cannot extend via truncation",
            ));
        }
        device.bytes.truncate(length);
        device.synchronized = device.synchronized.min(length);
        Ok(())
    }

    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        device.manifest = Some(bytes.to_vec());
        device.counts.manifest_bytes += bytes.len();
        device.counts.publications += 1;
        Ok(())
    }
}
