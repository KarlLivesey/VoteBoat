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
//! Immutable bounded creation binding adjacent to the selected native WAL.
use crate::{
    contracts::StorageError, group_creation::*, identity::GroupIdentity, log::GroupLog,
    native::log_store::*,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
impl<I: JournalIo, C: LogCodec> CreationLogStore for NativeLogStore<I, C> {
    fn creation_state(&self, group: GroupIdentity) -> Result<Option<GroupLog>, StorageError> {
        self.creation_state_native(group)
    }
}
/// Open only an existing selected store directory. This owns one scoped lock;
/// no threads, automatic store creation, group reset or implicit recovery.
pub struct FileCreationBindings {
    directory: PathBuf,
    _lock: File,
}
impl FileCreationBindings {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        let directory = directory.as_ref().to_owned();
        File::open(&directory).map_err(uncertain)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("CREATION.LOCK"))
            .map_err(uncertain)?;
        lock.try_lock()
            .map_err(|e| StorageError::Uncertain(e.to_string()))?;
        Ok(Self {
            directory,
            _lock: lock,
        })
    }
    fn publish_with(
        &mut self,
        bytes: &[u8],
        mut boundary: impl FnMut(u8) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        if bytes.len() > MAX_CREATION_BINDING_BYTES || bytes.len() < 68 {
            return Err(StorageError::Rejected("creation record size"));
        }
        if let Some(previous) = self.load()? {
            if previous != bytes {
                return Err(StorageError::Rejected("immutable creation conflict"));
            }
            File::open(self.directory.join("CREATION"))
                .and_then(|f| f.sync_all())
                .map_err(uncertain)?;
            File::open(&self.directory)
                .and_then(|f| f.sync_all())
                .map_err(uncertain)?;
            return Ok(());
        }
        let pending = self.directory.join("CREATION.pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&pending)
            .map_err(uncertain)?;
        file.write_all(bytes).map_err(uncertain)?;
        boundary(0)?;
        file.sync_all().map_err(uncertain)?;
        boundary(1)?;
        // Exclusive scoped lock serializes publishers. hard_link installs the
        // immutable final name without ever overwriting an existing record.
        fs::hard_link(&pending, self.directory.join("CREATION")).map_err(uncertain)?;
        boundary(2)?;
        File::open(&self.directory)
            .and_then(|f| f.sync_all())
            .map_err(uncertain)?;
        boundary(3)?;
        fs::remove_file(pending).map_err(uncertain)?;
        File::open(&self.directory)
            .and_then(|f| f.sync_all())
            .map_err(uncertain)?;
        boundary(4)?;
        Ok(())
    }
}
impl CreationBindings for FileCreationBindings {
    fn load(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
        let file = match File::open(self.directory.join("CREATION")) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(uncertain(e)),
        };
        if file.metadata().map_err(uncertain)?.len() > MAX_CREATION_BINDING_BYTES as u64 {
            return Err(StorageError::Corrupt("oversized creation binding"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_CREATION_BINDING_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(uncertain)?;
        if bytes.len() > MAX_CREATION_BINDING_BYTES {
            return Err(StorageError::Corrupt("oversized creation binding"));
        }
        Ok(Some(bytes))
    }
    fn publish_exact(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.publish_with(bytes, |_| Ok(()))
    }
}
fn uncertain(error: std::io::Error) -> StorageError {
    StorageError::Uncertain(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immutable_file_binding_recovers_each_publication_boundary_and_rejects_other_bytes() {
        for failure in 0..=4 {
            let root = std::env::temp_dir().join(format!(
                "voteboat-binding-{}-{}-{failure}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&root).unwrap();
            let bytes = vec![17; 100];
            let mut bindings = FileCreationBindings::open(&root).unwrap();
            assert!(FileCreationBindings::open(&root).is_err());
            assert!(bindings
                .publish_with(&bytes, |at| if at == failure {
                    Err(StorageError::Uncertain(
                        "injected publication boundary".into(),
                    ))
                } else {
                    Ok(())
                })
                .is_err());
            drop(bindings);
            let mut bindings = FileCreationBindings::open(&root).unwrap();
            let observed = bindings.load().unwrap();
            if failure < 2 {
                assert!(observed.is_none());
            } else {
                assert_eq!(observed, Some(bytes.clone()));
            }
            bindings.publish_exact(&bytes).unwrap();
            assert_eq!(bindings.load().unwrap(), Some(bytes.clone()));
            let different = vec![18; 100];
            assert!(bindings.publish_exact(&different).is_err());
            assert_eq!(bindings.load().unwrap(), Some(bytes));
            drop(bindings);
            fs::remove_dir_all(root).unwrap();
        }
    }
}
