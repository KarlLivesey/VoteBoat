// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit fresh-store publication comparison; the default assembly is unchanged.
use super::*;
use observe::{ObservedIo, Trace};
use std::io;
use voteboat::native::log_store::{FileLogIo, JournalIo, JournalTimings, WitnessLogIo};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum Publication {
    #[default]
    Legacy,
    Atomic,
    Witness,
}
impl Publication {
    pub(super) fn extract(args: &mut Vec<String>) -> Result<Self, Failure> {
        let Some(position) = args.iter().position(|arg| arg == "--publication") else {
            return Ok(Self::Legacy);
        };
        if args
            .iter()
            .skip(position + 1)
            .any(|arg| arg == "--publication")
        {
            return Err("duplicate --publication".into());
        }
        let selected = match args.get(position + 1).map(String::as_str) {
            Some("atomic") => Self::Atomic,
            Some("witness") => Self::Witness,
            _ => return Err("--publication requires atomic or witness".into()),
        };
        args.drain(position..position + 2);
        Ok(selected)
    }
    pub(super) fn validate(self, offered: bool) -> Result<(), Failure> {
        if self != Self::Legacy && !offered {
            return Err("--publication requires offered mode".into());
        }
        Ok(())
    }
    pub(super) fn settings(self) -> String {
        match self {
            Self::Legacy => String::new(),
            Self::Atomic => " publication_requested=atomic publication_effective=atomic publication_provider=FileLogIo publication_timings=native publication_scope=ordinary_barrier storage_error_scope=overlapping_observations".into(),
            Self::Witness => " publication_requested=witness publication_effective=witness publication_provider=WitnessLogIo publication_timings=native publication_scope=ordinary_barrier storage_error_scope=overlapping_observations".into(),
        }
    }
    pub(super) fn write_storage(
        self,
        root: &Path,
        snapshots: &[(String, usize, observe::Totals)],
    ) -> Result<(), Failure> {
        observe::write_csv(&mut exclusive(&root.join("storage.csv"))?, snapshots)?;
        if self != Self::Legacy {
            observe::write_publication(root, snapshots)?;
        }
        Ok(())
    }
    pub(super) fn open(
        self,
        directory: &Path,
        mode: NativeOpenMode,
        replica: u64,
    ) -> Result<(BenchJournal, Trace), Failure> {
        let create = mode == NativeOpenMode::Create;
        let timing = JournalTimings::default();
        let trace = if self == Self::Legacy {
            Trace::default()
        } else {
            Trace::journal(timing.clone())
        };
        let io = match self {
            Self::Legacy => BenchJournal::Legacy(ObservedIo {
                inner: if create {
                    FileLogIo::create(directory)?
                } else {
                    FileLogIo::open(directory)?
                },
                trace: trace.clone(),
            }),
            Self::Atomic => BenchJournal::Atomic(
                if create {
                    FileLogIo::create(directory)?
                } else {
                    FileLogIo::open(directory)?
                }
                .with_timings(timing),
                trace.clone(),
            ),
            Self::Witness => BenchJournal::Witness(
                if create {
                    WitnessLogIo::create(directory)?
                } else {
                    WitnessLogIo::open(directory)?
                }
                .with_timings(timing),
                trace.clone(),
            ),
        };
        if io.publication() != self {
            return Err("publication provider readback mismatch".into());
        }
        if self != Self::Legacy {
            eprintln!(
                "publication_readback replica={replica} mode={mode:?}{}",
                io.publication().settings()
            );
        }
        Ok((io, trace))
    }
}
pub(super) enum BenchJournal {
    Legacy(ObservedIo),
    Atomic(FileLogIo, Trace),
    Witness(WitnessLogIo, Trace),
}
impl BenchJournal {
    fn publication(&self) -> Publication {
        match self {
            Self::Legacy(_) => Publication::Legacy,
            Self::Atomic(_, _) => Publication::Atomic,
            Self::Witness(_, _) => Publication::Witness,
        }
    }
    fn io(&mut self) -> &mut dyn JournalIo {
        match self {
            Self::Legacy(io) => io,
            Self::Atomic(io, _) => io,
            Self::Witness(io, _) => io,
        }
    }
}
impl JournalIo for BenchJournal {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.io().read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.io().read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let result = self.io().append(bytes);
        if let Self::Atomic(_, trace) | Self::Witness(_, trace) = self {
            trace.encoded_append(bytes.len());
        }
        result
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.io().sync_log()
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.io().truncate_log(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.io().publish_manifest(bytes)
    }
    fn publish_barrier(&mut self, manifest: &[u8]) -> io::Result<()> {
        self.io().publish_barrier(manifest)
    }
    fn supports_replacement(&self) -> bool {
        match self {
            Self::Legacy(io) => io.supports_replacement(),
            Self::Atomic(io, _) => io.supports_replacement(),
            Self::Witness(io, _) => io.supports_replacement(),
        }
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.io().replace_log(bytes, manifest)
    }
}
#[cfg(test)]
#[path = "publication/tests.rs"]
mod tests;
