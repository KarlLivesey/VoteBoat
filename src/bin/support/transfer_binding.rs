// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Immutable startup binding, written only under the native directory lock.
use super::{
    profile::{Binding, Profile, Role},
    setup::Failure,
    wire,
};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use voteboat::native::startup::NativeMemberStartup;

pub struct SourceBinding {
    path: PathBuf,
    expected: Option<String>,
}
impl SourceBinding {
    pub fn new(config: &NativeMemberStartup, p: &Profile, b: Binding) -> Option<Self> {
        if b.role != Role::Source {
            return None;
        }
        let c = &config.startup;
        Some(Self {
            path: c.directory.join("transfer-source-profile"),
            expected: p.retirement.then(|| {
                format!(
                    "voteboat-retirable-source-v1 {} {} {} {} {} {}\n",
                    wire::hex(&p.digest.0),
                    c.node.get(),
                    c.store.id.get(),
                    c.store.incarnation.get(),
                    b.group.id.get(),
                    b.group.incarnation.get()
                )
            }),
        })
    }
    pub fn check(&self, create: bool) -> Result<(), Failure> {
        match (self.path.try_exists()?, self.expected.as_ref(), create) {
            (false, _, true) | (false, None, false) => Ok(()),
            (true, Some(expected), false) if wire::file(&self.path, 512)? == *expected => Ok(()),
            _ => Err(
                "transfer source profile binding mismatch or missing; no automatic migration"
                    .into(),
            ),
        }
    }
    pub fn finish(&self, create: bool) -> Result<(), Failure> {
        self.check(create)?;
        if create {
            if let Some(expected) = &self.expected {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&self.path)?;
                file.write_all(expected.as_bytes())?;
                file.sync_all()?;
                File::open(self.path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_binding_is_required_immutable_and_fail_closed() {
        let root =
            std::env::temp_dir().join(format!("voteboat-source-binding-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let record = SourceBinding {
            path: root.join("profile"),
            expected: Some("bound profile\n".into()),
        };
        assert!(record.check(false).is_err());
        record.finish(true).unwrap();
        record.check(false).unwrap();
        assert!(record.finish(true).is_err());
        let changed = SourceBinding {
            path: record.path.clone(),
            expected: Some("different profile\n".into()),
        };
        assert!(changed.check(false).is_err());
        let plain = SourceBinding {
            path: record.path.clone(),
            expected: None,
        };
        assert!(plain.check(false).is_err());
        std::fs::write(&record.path, "bound").unwrap();
        assert!(record.check(false).is_err());
        std::fs::remove_file(&record.path).unwrap();
        plain.check(false).unwrap();
        assert!(record.check(false).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
