// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::{
    fs, io,
    ops::Deref,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct Directory(PathBuf);

fn candidate(parent: &Path, sequence: u64) -> PathBuf {
    parent.join(format!(
        "voteboat-transfer-service-{}-root-{sequence}",
        std::process::id()
    ))
}

impl Directory {
    // Called under fixture_gate, including by the filesystem-only controls.
    // Port leases are recyclable; storage identities must be independent.
    pub(super) fn new() -> Self {
        Self::create(&std::env::temp_dir())
    }

    fn create(parent: &Path) -> Self {
        loop {
            let sequence = NEXT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .expect("transfer fixture root sequence exhausted");
            let root = candidate(parent, sequence);
            match fs::create_dir(&root) {
                Ok(()) => return Self(root),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create transfer fixture root {}: {error}", root.display()),
            }
        }
    }
}

impl Deref for Directory {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        // Normal callers retain their explicit post-child-shutdown removal
        // assertions. Unwind also covers partially prepared/started fixtures;
        // Cluster::drop kills and waits children before this field is dropped.
        if std::thread::panicking() {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                eprintln!(
                    "unwind cleanup of transfer root {}: {error}",
                    self.0.display()
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "root_tests.rs"]
mod tests;
