// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
/// Blocking record I/O under host-owned exclusive directory access. This does
/// not create a worker or acquire the node's directory lock on the host's behalf.
pub struct FileDrainRecord {
    path: PathBuf,
}
impl FileDrainRecord {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
fn error(e: std::io::Error, uncertain: bool) -> DrainJournalError {
    DrainJournalError::Io {
        kind: e.kind(),
        uncertain,
    }
}
impl DrainRecordIo for FileDrainRecord {
    fn read(&mut self) -> Result<Option<Vec<u8>>, DrainJournalError> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(error(e, false)),
        };
        let mut bytes = Vec::new();
        file.take(MAX_DRAIN_RECORD_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| error(e, false))?;
        if bytes.len() > MAX_DRAIN_RECORD_BYTES {
            return Err(DrainJournalError::InvalidRecord);
        }
        Ok(Some(bytes))
    }
    fn replace(&mut self, bytes: &[u8]) -> Result<(), DrainJournalError> {
        if bytes.len() > MAX_DRAIN_RECORD_BYTES {
            return Err(DrainJournalError::InvalidRecord);
        }
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stage = self.path.with_extension("drain-staging");
        let result = (|| {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&stage)?;
            f.write_all(bytes)?;
            f.sync_all()?;
            fs::rename(stage, &self.path)?;
            File::open(parent)?.sync_all()
        })();
        result.map_err(|e| error(e, true))
    }
}
