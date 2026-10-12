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
impl FileStripedIo {
    pub(super) fn assemble(
        directories: Vec<PathBuf>,
        create: bool,
        allow_missing: bool,
    ) -> io::Result<Self> {
        if !(2..=8).contains(&directories.len())
            || directories
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != directories.len()
        {
            return Err(invalid("striped directories must be distinct fixed lanes"));
        }
        let count = directories.len();
        let lanes = directories
            .into_iter()
            .enumerate()
            .map(
                |(index, directory)| match Lane::open(directory, index, count, create) {
                    Ok(lane) => Ok(Some(lane)),
                    Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => {
                        Ok(None)
                    }
                    Err(error) => Err(error),
                },
            )
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self { lanes })
    }
}
impl Lane {
    fn open(directory: PathBuf, index: usize, count: usize, create: bool) -> io::Result<Self> {
        if create {
            match fs::create_dir(&directory) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(e),
            }
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("LOCK"))?;
        lock.try_lock().map_err(io::Error::other)?;
        if create {
            initialize(&directory, index, count)?;
        }
        let identity = read_bounded(File::open(directory.join("STRIPE-LANE"))?, 20)?;
        if identity != lane_identity(index, count) {
            return Err(invalid("striped physical lane identity"));
        }
        let payload = [
            open_payload(&directory, 0, create)?,
            open_payload(&directory, 1, create)?,
        ];
        if create {
            for file in &payload {
                file.sync_all()?;
            }
            sync_directory(&directory)?;
            if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
                sync_directory(parent)?;
            }
        }
        Ok(Self {
            directory,
            payload,
            _lock: lock,
        })
    }
}
fn initialize(directory: &Path, index: usize, count: usize) -> io::Result<()> {
    for name in [
        "CURRENT",
        "MANIFEST",
        "votes.wal",
        "log.wal",
        "log.0.wal",
        "log.1.wal",
        "MANIFEST.0",
        "MANIFEST.1",
        "STRIPE-LANE",
        "STRIPE-CATALOG",
        "stripe.0.wal",
        "stripe.1.wal",
    ] {
        if directory.join(name).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "striped create requires a new native backend",
            ));
        }
    }
    let mut identity = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("STRIPE-LANE"))?;
    identity.write_all(&lane_identity(index, count))?;
    identity.sync_all()
}
fn lane_identity(index: usize, count: usize) -> Vec<u8> {
    let mut bytes = b"VBSTRID1".to_vec();
    bytes.extend((index as u32).to_le_bytes());
    bytes.extend((count as u32).to_le_bytes());
    bytes.extend(crc32c(&bytes).to_le_bytes());
    bytes
}
fn open_payload(directory: &Path, slot: usize, create: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create {
        options.create_new(true);
    }
    options.open(directory.join(format!("stripe.{slot}.wal")))
}
