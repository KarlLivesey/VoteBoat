// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl WitnessLogIo {
    fn next_record(&self, bytes: &[u8]) -> io::Result<Record> {
        let (binding, length) = super::super::read_manifest(bytes)
            .map_err(|_| format::invalid("invalid publication manifest"))?;
        let maximum = [self.primary, self.secondary]
            .into_iter()
            .flatten()
            .map(|r| r.counter)
            .max()
            .unwrap_or(0);
        let counter = maximum
            .checked_add(1)
            .ok_or_else(|| format::invalid("publication counter exhausted"))?;
        if !self.initializing {
            let old = format::mandatory(self.primary, self.secondary)?;
            let session = binding.session.get();
            if binding.identity != old.binding.identity
                || length < old.length
                || !(session == old.binding.session.get()
                    || old.binding.session.get().checked_add(1) == Some(session))
            {
                return Err(format::invalid(
                    "invalid publication identity/session/boundary",
                ));
            }
        }
        let logical = self
            .log
            .metadata()?
            .len()
            .checked_sub(SIZE as u64)
            .ok_or_else(|| format::invalid("WAL header missing"))?;
        if length != logical {
            return Err(format::invalid("publication must cover complete WAL"));
        }
        Ok(Record {
            generation: self.generation,
            counter,
            binding,
            length,
        })
    }
    fn write_primary(&mut self, record: Record) -> io::Result<()> {
        self.log.seek(SeekFrom::Start(0))?;
        self.log.write_all(&record.encode(1))
    }
    fn write_secondary(&mut self, record: Record) -> io::Result<()> {
        self.timed(4, |this| {
            this.acknowledgement.seek(SeekFrom::Start(0))?;
            this.acknowledgement.write_all(&record.encode(2))
        })
    }
    fn sync_secondary(&mut self) -> io::Result<()> {
        self.timed(5, |this| this.acknowledgement.sync_all())
    }
    fn repair(&mut self, after: &mut impl FnMut() -> io::Result<()>) -> io::Result<()> {
        match (self.primary, self.secondary) {
            (Some(record), None) => {
                self.acknowledgement.seek(SeekFrom::Start(0))?;
                self.acknowledgement.write_all(&record.encode(2))?;
                after()?;
                self.acknowledgement.sync_all()?;
                after()?;
                self.secondary = Some(record);
            }
            (None, Some(record)) => {
                self.write_primary(record)?;
                after()?;
                self.log.sync_all()?;
                after()?;
                self.primary = Some(record);
            }
            _ => (),
        }
        Ok(())
    }
    pub(super) fn publish_with(
        &mut self,
        bytes: &[u8],
        mut after: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        let record = self.next_record(bytes)?;
        if !self.initializing {
            self.repair(&mut after)?;
        }
        self.write_primary(record)?;
        after()?;
        self.sync_log()?;
        after()?;
        self.timed(2, |this| {
            this.write_secondary(record)?;
            after()?;
            this.sync_secondary()?;
            after()
        })?;
        if self.initializing {
            self.select_with(self.generation, &mut after)?;
            if let Some(parent) = self
                .directory
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
            {
                sync_directory(parent)?;
                after()?;
            }
        }
        self.primary = Some(record);
        self.secondary = Some(record);
        self.initializing = false;
        Ok(())
    }
    fn select_with(
        &self,
        generation: u64,
        after: &mut impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        let staging = self.directory.join("WITNESS-CURRENT.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&staging)?;
        after()?;
        file.write_all(&format::selector(generation))?;
        after()?;
        file.sync_all()?;
        after()?;
        fs::rename(staging, self.directory.join("WITNESS-CURRENT"))?;
        after()?;
        sync_directory(&self.directory)?;
        after()
    }
    pub(super) fn replace_with(
        &mut self,
        bytes: &[u8],
        manifest: &[u8],
        mut after: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        if self.initializing {
            return Err(format::invalid("replacement before initialization"));
        }
        let old = format::mandatory(self.primary, self.secondary)?;
        let (binding, length) = super::super::read_manifest(manifest)
            .map_err(|_| format::invalid("replacement manifest"))?;
        if binding != old.binding || length != bytes.len() as u64 {
            return Err(format::invalid("replacement binding/length"));
        }
        let next = self
            .generation
            .checked_add(1)
            .ok_or_else(|| format::invalid("journal generation exhausted"))?;
        let record = Record {
            generation: next,
            counter: 1,
            binding,
            length,
        };
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.directory.join(log_name(next)))?;
        after()?;
        log.write_all(&record.encode(1))?;
        after()?;
        log.write_all(bytes)?;
        after()?;
        log.sync_all()?;
        after()?;
        let mut acknowledgement = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.directory.join(ack_name(next)))?;
        after()?;
        acknowledgement.write_all(&record.encode(2))?;
        after()?;
        acknowledgement.sync_all()?;
        after()?;
        sync_directory(&self.directory)?;
        after()?;
        self.select_with(next, &mut after)?;
        let old_generation = self.generation;
        self.log = log;
        self.acknowledgement = acknowledgement;
        self.generation = next;
        self.primary = Some(record);
        self.secondary = Some(record);
        fs::remove_file(self.directory.join(log_name(old_generation)))?;
        after()?;
        fs::remove_file(self.directory.join(ack_name(old_generation)))?;
        after()?;
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
        after()
    }
}
