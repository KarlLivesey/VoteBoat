// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Owned analysis fixture. Its successful sync never represents disk durability.
use super::*;
use std::{cell::RefCell, io, rc::Rc};

#[derive(Clone)]
struct CopiedIo(Rc<RefCell<CopiedBytes>>);
struct CopiedBytes {
    manifest: Vec<u8>,
    wal: Vec<u8>,
}
impl JournalIo for CopiedIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.0.borrow().manifest.clone())
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let state = self.0.borrow();
        if state.wal.len() > limit {
            return Err(io::Error::other("copied WAL read limit"));
        }
        Ok(state.wal.clone())
    }
    fn append(&mut self, _: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "analysis cannot append",
        ))
    }
    fn sync_log(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        let length = usize::try_from(length).map_err(io::Error::other)?;
        let mut state = self.0.borrow_mut();
        if length > state.wal.len() {
            return Err(io::Error::other("copied truncate outside WAL"));
        }
        state.wal.truncate(length);
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().manifest = bytes.to_vec();
        Ok(())
    }
}

pub(super) fn validate(
    manifest: &[u8],
    bytes: &[u8],
    identity: StoreIdentity,
    limits: LogLimits,
) -> Result<GroupLog, Failure> {
    let io = CopiedIo(Rc::new(RefCell::new(CopiedBytes {
        manifest: manifest.to_vec(),
        wal: bytes.to_vec(),
    })));
    // The public native validator checks the entire manifest/checksum/identity
    // before this example inspects its known native-v2 boundary diagnostic.
    let store = checked(NativeLogStore::recover(io.clone(), identity, limits))?;
    let boundary = u64::from_le_bytes(
        manifest
            .get(40..48)
            .ok_or("native manifest boundary missing")?
            .try_into()?,
    );
    if boundary != bytes.len() as u64 || io.0.borrow().wal.as_slice() != bytes {
        return Err(
            "unsealed journal: written/torn tails are not original durable evidence".into(),
        );
    }
    checked(store.state(group()))
}
