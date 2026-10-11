// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::cell::RefCell;

#[derive(Clone, Copy, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Sync,
    FalseSync,
    PublishBefore,
    PublishAfter,
}
#[derive(Default)]
struct Device {
    slots: [Option<Vec<u8>>; 2],
    synced: [Option<Vec<u8>>; 2],
    root: Option<Vec<u8>>,
    fault: Fault,
}
#[derive(Clone, Default)]
pub(super) struct Image(Rc<RefCell<Device>>);
impl Image {
    pub(super) fn fault(&self, fault: Fault) {
        self.0.borrow_mut().fault = fault;
    }
    pub(super) fn crash(&self, mode: crash::Mode) {
        let mut device = self.0.borrow_mut();
        if matches!(mode, crash::Mode::PowerLoss) {
            device.slots = device.synced.clone();
        }
        device.fault = Fault::None;
    }
}
impl SnapshotIo for Image {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .root
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
        let device = self.0.borrow();
        let bytes = device.slots[usize::from(slot)]
            .as_ref()
            .ok_or(io::ErrorKind::NotFound)?;
        if bytes.len() > limit {
            return Err(io::Error::other("snapshot budget"));
        }
        Ok(bytes.clone())
    }
    fn begin_slot(&mut self, slot: u8, prefix: &[u8]) -> io::Result<()> {
        assert!(prefix.len() <= limits().max_file_bytes());
        self.0.borrow_mut().slots[usize::from(slot)] = Some(prefix.to_vec());
        Ok(())
    }
    fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        let slot = device.slots[usize::from(slot)]
            .as_mut()
            .ok_or(io::ErrorKind::NotFound)?;
        assert!(slot.len() + bytes.len() <= limits().max_file_bytes());
        slot.extend_from_slice(bytes);
        Ok(())
    }
    fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        match device.fault {
            Fault::Sync => Err(io::Error::other("snapshot sync failed")),
            Fault::FalseSync => Ok(()), // Explicit defective-provider negative only.
            _ => {
                device.synced[usize::from(slot)] = device.slots[usize::from(slot)].clone();
                Ok(())
            }
        }
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0.borrow_mut();
        if matches!(device.fault, Fault::PublishBefore) {
            return Err(io::Error::other("snapshot root before rename"));
        }
        // Atomic root publication includes the directory barrier. This device
        // deliberately does not model cuts inside the filesystem implementation.
        assert!(bytes.len() <= 256);
        device.root = Some(bytes.to_vec());
        if matches!(device.fault, Fault::PublishAfter) {
            Err(io::Error::other("snapshot root completion lost"))
        } else {
            Ok(())
        }
    }
}
