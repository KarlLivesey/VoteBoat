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
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
/// Atomic file replacement under caller-provided exclusive directory ownership.
/// The caller must retain that ownership until every worker has joined.
pub struct FileCredentialRecord {
    path: PathBuf,
}
impl FileCredentialRecord {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
fn error(e: std::io::Error, uncertain: bool) -> CredentialJournalError {
    CredentialJournalError::Io {
        kind: e.kind(),
        uncertain,
    }
}
impl CredentialRecordIo for FileCredentialRecord {
    fn read(&mut self) -> Result<Option<[u8; 128]>, CredentialJournalError> {
        let mut file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(error(e, false)),
        };
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(129)
            .read_to_end(&mut bytes)
            .map_err(|e| error(e, false))?;
        let b = bytes
            .try_into()
            .map_err(|_| CredentialJournalError::InvalidRecord)?;
        Ok(Some(b))
    }
    fn replace(&mut self, bytes: &[u8; 128]) -> Result<(), CredentialJournalError> {
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stage = self.path.with_extension("staging");
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
