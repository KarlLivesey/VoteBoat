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
//! Two bounded journal slots and an atomic durable CURRENT selection. Legacy
//! stores without CURRENT continue using log.wal/MANIFEST until first cleaning.
use super::{JournalIo, JournalTimings};
use crate::native::vote_store::crc32c;
use std::time::Instant;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub struct FileLogIo {
    directory: PathBuf,
    log: File,
    _lock: File,
    generation: u64,
    timings: Option<JournalTimings>,
}
impl FileLogIo {
    /// Attach native diagnostics before handing this file to a store/worker.
    /// This changes no durability, format, lifetime or JournalIo result.
    pub fn with_timings(mut self, timings: JournalTimings) -> Self {
        self.timings = Some(timings);
        self
    }
    fn timed<T>(
        &mut self,
        operation: usize,
        call: impl FnOnce(&mut Self) -> io::Result<T>,
    ) -> io::Result<T> {
        let start = self.timings.as_ref().map(|_| Instant::now());
        let result = call(self);
        if let (Some(start), Some(timing)) = (start, &self.timings) {
            timing.record(operation, start.elapsed(), result.is_ok());
        }
        result
    }
    pub fn create(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        match fs::create_dir(&directory) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
        let lock = lock(&directory)?;
        for name in [
            "CURRENT",
            "MANIFEST",
            "votes.wal",
            "log.wal",
            "log.0.wal",
            "log.1.wal",
            "MANIFEST.0",
            "MANIFEST.1",
        ] {
            if directory.join(name).exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "store already exists",
                ));
            }
        }
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join("log.wal"))?;
        sync_directory(&directory)?;
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_directory(parent)?;
        }
        Ok(Self {
            directory,
            log,
            _lock: lock,
            generation: 0,
            timings: None,
        })
    }
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        let lock = lock(&directory)?;
        let generation = match File::open(directory.join("CURRENT")) {
            Ok(file) => {
                let mut b = Vec::new();
                file.take(21).read_to_end(&mut b)?;
                if b.len() != 20
                    || &b[..8] != b"VBLCUR01"
                    || crc32c(&b[..16]) != u32::from_le_bytes(b[16..].try_into().unwrap())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "journal selection corrupt",
                    ));
                }
                let generation = u64::from_le_bytes(b[8..16].try_into().unwrap());
                if generation == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "zero journal generation",
                    ));
                }
                generation
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        File::open(directory.join(manifest_name(generation)))?;
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(log_name(generation)))?;
        Ok(Self {
            directory,
            log,
            _lock: lock,
            generation,
            timings: None,
        })
    }
    // The callback injects primitive-boundary errors in native file tests. It
    // carries no service policy; the normal path always continues.
    fn replace_with(
        &mut self,
        bytes: &[u8],
        manifest: &[u8],
        mut after: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        let next = self
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("journal generation exhausted"))?;
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.directory.join(log_name(next)))?;
        after()?;
        log.write_all(bytes)?;
        after()?;
        log.sync_all()?;
        after()?;
        let mut metadata = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.directory.join(manifest_name(next)))?;
        metadata.write_all(manifest)?;
        after()?;
        metadata.sync_all()?;
        after()?;
        sync_directory(&self.directory)?;
        after()?;
        let mut current = b"VBLCUR01".to_vec();
        current.extend(next.to_le_bytes());
        current.extend(crc32c(&current).to_le_bytes());
        let mut pointer = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.directory.join("CURRENT.tmp"))?;
        pointer.write_all(&current)?;
        after()?;
        pointer.sync_all()?;
        after()?;
        fs::rename(
            self.directory.join("CURRENT.tmp"),
            self.directory.join("CURRENT"),
        )?;
        after()?;
        sync_directory(&self.directory)?;
        after()?;
        // No old file can be removed before the selection's directory sync.
        let old_generation = self.generation;
        self.log = log;
        self.generation = next;
        fs::remove_file(self.directory.join(log_name(old_generation)))?;
        after()?;
        fs::remove_file(self.directory.join(manifest_name(old_generation)))?;
        after()?;
        // A crash during the first selection may leave the legacy pair. It is
        // never selected again, and subsequent cleaning also retires it.
        if old_generation != 0 {
            for name in ["log.wal", "MANIFEST"] {
                match fs::remove_file(self.directory.join(name)) {
                    Ok(()) => (),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e),
                }
            }
        }
        sync_directory(&self.directory)?;
        after()?;
        Ok(())
    }
}
fn log_name(generation: u64) -> String {
    if generation == 0 {
        "log.wal".into()
    } else {
        format!("log.{}.wal", generation % 2)
    }
}
fn manifest_name(generation: u64) -> String {
    if generation == 0 {
        "MANIFEST".into()
    } else {
        format!("MANIFEST.{}", generation % 2)
    }
}
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
fn lock(directory: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("LOCK"))?;
    file.try_lock().map_err(io::Error::other)?;
    Ok(file)
}
impl JournalIo for FileLogIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        let mut b = Vec::new();
        File::open(self.directory.join(manifest_name(self.generation)))?
            .take(53)
            .read_to_end(&mut b)?;
        Ok(b)
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        if self.log.metadata()?.len() > limit as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL exceeds capacity",
            ));
        }
        self.log.seek(SeekFrom::Start(0))?;
        let mut b = Vec::new();
        (&mut self.log).take(limit as u64 + 1).read_to_end(&mut b)?;
        if b.len() > limit {
            return Err(io::Error::other("WAL grew during recovery"));
        }
        Ok(b)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.timed(0, |this| {
            this.log.seek(SeekFrom::End(0))?;
            this.log.write_all(bytes)
        })
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.timed(1, |this| this.log.sync_all())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.log.set_len(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.timed(2, |this| {
            let staging = this.directory.join("MANIFEST.tmp");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&staging)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::rename(staging, this.directory.join(manifest_name(this.generation)))?;
            sync_directory(&this.directory)
        })
    }

    fn supports_replacement(&self) -> bool {
        true
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.replace_with(bytes, manifest, || Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{contracts::HardState, native::log_store::*, quorum::*};
    use std::collections::BTreeMap;
    fn identity() -> StoreIdentity {
        StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        }
    }
    fn group() -> GroupIdentity {
        GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        }
    }
    fn fill(store: &mut NativeLogStore<FileLogIo>) {
        for _ in 0..30 {
            let s = store.state(group()).unwrap();
            let tickets = store
                .append_batch(vec![LogMutation::Update(LogUpdate {
                    snapshot_membership: None,
                    group: group(),
                    expected_revision: s.revision,
                    hard_state: HardState {
                        term: s.hard_state.term + 1,
                        voted_for: None,
                    },
                    commit_index: 0,
                    suffix: None,
                    snapshot: None,
                })])
                .unwrap();
            store.barrier(&tickets).unwrap();
        }
    }
    #[test]
    fn every_file_replacement_boundary_recovers_selected_pair_without_dual_writer() {
        for generation in [0, 1] {
            for cut in 1..=13 {
                let path = std::env::temp_dir().join(format!(
                    "voteboat-reclaim-cut-{}-{generation}-{cut}",
                    std::process::id()
                ));
                let mut store = NativeLogStore::create(
                    FileLogIo::create(&path).unwrap(),
                    identity(),
                    LogLimits::default(),
                )
                .unwrap();
                let voter = NodeId::new(1).unwrap();
                let tickets = store
                    .append_batch(vec![LogMutation::Create(Bootstrap {
                        group: group(),
                        configuration: ConfigurationId::new(1).unwrap(),
                        policy: Policy::new(Tree::Voter(voter), Limits::default()).unwrap(),
                        voter_stores: [(voter, identity())].into_iter().collect(),
                    })])
                    .unwrap();
                store.barrier(&tickets).unwrap();
                fill(&mut store);
                if generation == 1 {
                    store.reclaim(store.limits().max_wal_bytes).unwrap();
                    fill(&mut store);
                }
                let expected = store.state(group()).unwrap();
                let state: BTreeMap<_, _> = [(group(), expected.clone())].into_iter().collect();
                let bytes = NativeLogCodec
                    .encode_checkpoint(
                        store.sequence,
                        &state,
                        store.limits(),
                        store.limits().max_wal_bytes,
                    )
                    .unwrap();
                let metadata = super::super::manifest(store.binding(), bytes.len() as u64);
                let mut step = 0;
                assert!(store
                    .io
                    .replace_with(&bytes, &metadata, || {
                        step += 1;
                        if step == cut {
                            Err(io::Error::other("injected primitive-boundary failure"))
                        } else {
                            Ok(())
                        }
                    })
                    .is_err());
                assert!(
                    FileLogIo::open(&path).is_err(),
                    "exclusive lock must survive every error"
                );
                drop(store);
                let mut recovered = NativeLogStore::recover(
                    FileLogIo::open(&path).unwrap(),
                    identity(),
                    LogLimits::default(),
                )
                .unwrap();
                assert_eq!(
                    recovered.state(group()).unwrap(),
                    expected,
                    "generation={generation} cut={cut}"
                );
                fill(&mut recovered);
                recovered.reclaim(recovered.limits().max_wal_bytes).unwrap();
                assert!(!path.join("log.wal").exists());
                drop(recovered);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}
