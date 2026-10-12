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
use super::{invalid, StripedIo};
use crate::format_crc::crc32c;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
mod construction;
#[cfg(test)]
mod tests;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationPrimitive {
    StagingOpen,
    StagingWrite,
    StagingSync,
    Rename,
    DirectorySync,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PublicationBoundary {
    primitive: PublicationPrimitive,
    after: bool,
}
struct Lane {
    directory: PathBuf,
    payload: [File; 2],
    _lock: File,
}
/// Fixed caller-selected directories; no hidden workers or device placement.
/// Uses the ordinary native LOCK name, excluding concurrent backend owners.
pub struct FileStripedIo {
    lanes: Vec<Option<Lane>>,
}
impl FileStripedIo {
    pub fn create(directories: Vec<PathBuf>) -> io::Result<Self> {
        Self::assemble(directories, true, false)
    }
    pub fn open(directories: Vec<PathBuf>) -> io::Result<Self> {
        Self::assemble(directories, false, false)
    }
    /// Diagnostic access to surviving mirrored controls. Missing lanes retain
    /// their fixed positions and cannot synchronize or recover payloads.
    pub fn open_surviving(directories: Vec<PathBuf>) -> io::Result<Self> {
        Self::assemble(directories, false, true)
    }
    fn lane(&mut self, lane: usize) -> io::Result<&mut Lane> {
        self.lanes
            .get_mut(lane)
            .and_then(Option::as_mut)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "striped lane unavailable"))
    }
    // Private continuation seam: production always proceeds. Tests interrupt
    // these exact filesystem calls without replacing the file provider.
    fn publish_with(
        &mut self,
        lane: usize,
        bytes: &[u8],
        mut at: impl FnMut(PublicationBoundary) -> io::Result<()>,
    ) -> io::Result<()> {
        use PublicationPrimitive::*;
        let directory = &self.lane(lane)?.directory;
        let staging = directory.join("STRIPE-CATALOG.tmp");
        at(PublicationBoundary {
            primitive: StagingOpen,
            after: false,
        })?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&staging)?;
        at(PublicationBoundary {
            primitive: StagingOpen,
            after: true,
        })?;
        at(PublicationBoundary {
            primitive: StagingWrite,
            after: false,
        })?;
        file.write_all(bytes)?;
        at(PublicationBoundary {
            primitive: StagingWrite,
            after: true,
        })?;
        at(PublicationBoundary {
            primitive: StagingSync,
            after: false,
        })?;
        file.sync_all()?;
        at(PublicationBoundary {
            primitive: StagingSync,
            after: true,
        })?;
        at(PublicationBoundary {
            primitive: Rename,
            after: false,
        })?;
        fs::rename(staging, directory.join("STRIPE-CATALOG"))?;
        at(PublicationBoundary {
            primitive: Rename,
            after: true,
        })?;
        at(PublicationBoundary {
            primitive: DirectorySync,
            after: false,
        })?;
        sync_directory(directory)?;
        at(PublicationBoundary {
            primitive: DirectorySync,
            after: true,
        })
    }
}
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
fn read_bounded(mut file: File, limit: usize) -> io::Result<Vec<u8>> {
    if file.metadata()?.len() > limit as u64 {
        return Err(invalid("striped file exceeds read budget"));
    }
    let mut bytes = Vec::new();
    (&mut file).take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid("striped file grew during read"));
    }
    Ok(bytes)
}
impl StripedIo for FileStripedIo {
    fn lane_count(&self) -> usize {
        self.lanes.len()
    }
    fn read_catalog(&mut self, lane: usize, limit: usize) -> io::Result<Vec<u8>> {
        read_bounded(
            File::open(self.lane(lane)?.directory.join("STRIPE-CATALOG"))?,
            limit,
        )
    }
    fn read_payload(&mut self, lane: usize, generation: u64, limit: usize) -> io::Result<Vec<u8>> {
        if generation == 0 {
            return Err(invalid("zero payload generation"));
        }
        let file = &mut self.lane(lane)?.payload[(generation % 2) as usize];
        if file.metadata()?.len() > limit as u64 {
            return Err(invalid("striped payload exceeds budget"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(invalid("striped payload grew during read"));
        }
        Ok(bytes)
    }
    fn append_payload(&mut self, lane: usize, generation: u64, bytes: &[u8]) -> io::Result<()> {
        if generation == 0 {
            return Err(invalid("zero payload generation"));
        }
        let file = &mut self.lane(lane)?.payload[(generation % 2) as usize];
        file.seek(SeekFrom::End(0))?;
        file.write_all(bytes)
    }
    fn sync_payload(&mut self, lane: usize, generation: u64) -> io::Result<()> {
        if generation == 0 {
            return Err(invalid("zero payload generation"));
        }
        self.lane(lane)?.payload[(generation % 2) as usize].sync_all()
    }
    fn truncate_payload(&mut self, lane: usize, generation: u64, length: u64) -> io::Result<()> {
        if generation == 0 {
            return Err(invalid("zero payload generation"));
        }
        self.lane(lane)?.payload[(generation % 2) as usize].set_len(length)
    }
    fn publish_catalog(&mut self, lane: usize, bytes: &[u8]) -> io::Result<()> {
        self.publish_with(lane, bytes, |_| Ok(()))
    }
}
