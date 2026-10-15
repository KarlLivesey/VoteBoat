// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit two-sync native journal. Existing journals require migration.
use super::{JournalIo, JournalTimings};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};

mod format;
mod publication;
#[cfg(test)]
mod tests;
use format::{Record, SIZE};

/// Opt-in native publication format with two fixed durability witnesses.
/// Creation accepts fresh stores only; opening never falls back to legacy files.
pub struct WitnessLogIo {
    directory: PathBuf,
    log: File,
    acknowledgement: File,
    _lock: File,
    generation: u64,
    primary: Option<Record>,
    secondary: Option<Record>,
    initializing: bool,
    timings: Option<JournalTimings>,
}
impl WitnessLogIo {
    pub fn with_timings(mut self, timings: JournalTimings) -> Self {
        self.timings = Some(timings);
        self
    }
    pub fn create(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        match fs::create_dir(&directory) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error),
        }
        let lock = lock(&directory)?;
        for name in [
            "CURRENT",
            "MANIFEST",
            "MANIFEST.tmp",
            "votes.wal",
            "log.wal",
            "log.0.wal",
            "log.1.wal",
            "MANIFEST.0",
            "MANIFEST.1",
            "STRIPE-LANE",
            "STRIPE-CATALOG",
            "WITNESS-CURRENT",
            "WITNESS-CURRENT.tmp",
        ] {
            if directory.join(name).exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "store already exists",
                ));
            }
        }
        let log = new_file(&directory.join(log_name(0)))?;
        let acknowledgement = new_file(&directory.join(ack_name(0)))?;
        log.set_len(SIZE as u64)?;
        acknowledgement.set_len(SIZE as u64)?;
        sync_directory(&directory)?;
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_directory(parent)?;
        }
        Ok(Self {
            directory,
            log,
            acknowledgement,
            _lock: lock,
            generation: 0,
            primary: None,
            secondary: None,
            initializing: true,
            timings: None,
        })
    }
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        let lock = lock(&directory)?;
        if directory.join("CURRENT").exists() {
            return Err(format::invalid("legacy journal selection is unsupported"));
        }
        let mut selector = Vec::new();
        File::open(directory.join("WITNESS-CURRENT"))?
            .take(21)
            .read_to_end(&mut selector)?;
        let generation = format::selection(&selector)?;
        let log = existing_file(&directory.join(log_name(generation)))?;
        let acknowledgement = existing_file(&directory.join(ack_name(generation)))?;
        let mut this = Self {
            directory,
            log,
            acknowledgement,
            _lock: lock,
            generation,
            primary: None,
            secondary: None,
            initializing: false,
            timings: None,
        };
        this.load()?;
        Ok(this)
    }
    fn load(&mut self) -> io::Result<Record> {
        self.primary = format::read_record(&mut self.log, 1, self.generation, false)?;
        self.secondary = format::read_record(&mut self.acknowledgement, 2, self.generation, true)?;
        format::mandatory(self.primary, self.secondary)
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
}
impl JournalIo for WitnessLogIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        if self.initializing {
            return Err(io::ErrorKind::NotFound.into());
        }
        let record = self.load()?;
        Ok(super::manifest(record.binding, record.length))
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let physical = self.log.metadata()?.len();
        let logical = physical
            .checked_sub(SIZE as u64)
            .ok_or_else(|| format::invalid("WAL header missing"))?;
        if logical > limit as u64 {
            return Err(format::invalid("WAL exceeds capacity"));
        }
        self.log.seek(SeekFrom::Start(SIZE as u64))?;
        let mut bytes = Vec::new();
        (&mut self.log)
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(format::invalid("WAL grew during recovery"));
        }
        Ok(bytes)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.timed(0, |this| {
            if this.log.seek(SeekFrom::End(0))? < SIZE as u64 {
                return Err(format::invalid("WAL header missing"));
            }
            this.log.write_all(bytes)
        })
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.timed(1, |this| this.log.sync_all())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.log.set_len(
            length
                .checked_add(SIZE as u64)
                .ok_or_else(|| format::invalid("physical WAL length overflow"))?,
        )
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.publish_barrier(bytes)
    }
    fn publish_barrier(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.publish_with(bytes, || Ok(()))
    }
    fn supports_replacement(&self) -> bool {
        true
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.replace_with(bytes, manifest, || Ok(()))
    }
}
fn log_name(generation: u64) -> String {
    if generation == 0 {
        "log.wal".into()
    } else {
        format!("log.{}.wal", generation % 2)
    }
}
fn ack_name(generation: u64) -> String {
    if generation == 0 {
        "MANIFEST".into()
    } else {
        format!("MANIFEST.{}", generation % 2)
    }
}
fn new_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}
fn existing_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
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
