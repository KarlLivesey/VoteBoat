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
//! One owned off-thread credential preparation; publication stays on the host.
use super::{
    service_access::{Access, ActiveAccess},
    setup::Failure,
};
use std::{
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};
use voteboat::{
    authorization::CredentialGeneration, credential_reload::*, native::credential_journal::*,
    secure::PeerIdentity,
};
#[cfg(test)]
#[path = "credential_reload_tests.rs"]
mod tests;
#[derive(Clone)]
struct Paths {
    access: PathBuf,
    tls: PathBuf,
    journal: PathBuf,
    node: u64,
    owner: PeerIdentity,
}
struct Prepared {
    access: Access,
    record: CredentialReloadRecord,
}
struct Rejected {
    message: String,
    uncertain: bool,
}
struct Pending {
    request: CredentialReloadRequest,
    handle: JoinHandle<Result<Prepared, Rejected>>,
}
pub struct Reload {
    paths: Paths,
    latest: Option<CredentialReloadRecord>,
    pending: Option<Pending>,
    failure: Option<(u64, String)>,
}
pub struct Credentials {
    pub access: Option<ActiveAccess>,
    reload: Option<Reload>,
}
pub struct Commands<'a> {
    reload: &'a mut Option<Reload>,
    current: Option<CredentialGeneration>,
}
impl Credentials {
    pub fn load(
        root: &Path,
        path: Option<&Path>,
        tls: &Path,
        node: u64,
        owner: PeerIdentity,
    ) -> Result<Self, Failure> {
        let Some(path) = path else {
            return Ok(Self {
                access: None,
                reload: None,
            });
        };
        let access = Access::load(path, tls, node)?;
        let reload = Reload::new(root, path, tls, node, owner, &access)?;
        Ok(Self {
            access: Some(ActiveAccess::new(access.policy.generation(), access)),
            reload: Some(reload),
        })
    }
    pub fn poll(&mut self) -> bool {
        match (&mut self.reload, &mut self.access) {
            (Some(reload), Some(active)) => reload.poll(active),
            _ => false,
        }
    }
    pub fn finish(&mut self) -> Result<(), Failure> {
        match (&mut self.reload, &mut self.access) {
            (Some(reload), Some(active)) => reload.finish(active),
            _ => Ok(()),
        }
    }
    pub fn split(&mut self) -> (Option<&ActiveAccess>, Commands<'_>) {
        (
            self.access.as_ref(),
            Commands {
                reload: &mut self.reload,
                current: self.access.as_ref().and_then(ActiveAccess::generation),
            },
        )
    }
}
impl Commands<'_> {
    pub fn command(&mut self, words: &[&str]) -> Result<String, String> {
        let reload = self
            .reload
            .as_mut()
            .ok_or("credential reload requires authenticated service mode")?;
        let current = self.current.ok_or("credentials unavailable")?;
        let generation = |value: &str| {
            value
                .parse::<u64>()
                .ok()
                .and_then(CredentialGeneration::new)
                .ok_or("invalid credential generation")
        };
        match words {
            ["credential-status", sequence] => Ok(reload.status(
                sequence.parse().map_err(|_| "invalid reload sequence")?,
                current,
            )),
            ["reload-access", sequence, expected, replacement] => reload.submit(
                CredentialReloadRequest {
                    sequence: sequence.parse().map_err(|_| "invalid reload sequence")?,
                    expected: generation(expected)?,
                    replacement: generation(replacement)?,
                },
                current,
            ),
            _ => Err(
                "expected credential-status REQUEST or reload-access REQUEST EXPECTED NEXT".into(),
            ),
        }
    }
}
impl Drop for Reload {
    fn drop(&mut self) {
        // Error exits also retain the worker until its durable outcome is known.
        // Publication is skipped on exit; startup reconciles its durable record.
        if let Some(pending) = self.pending.take() {
            let _ = pending.handle.join();
        }
    }
}
fn journal(paths: &Paths) -> Result<NativeCredentialJournal, CredentialJournalError> {
    NativeCredentialJournal::new(FileCredentialRecord::new(&paths.journal), paths.owner)
        .map_err(|(e, _)| e)
}
fn prepare(paths: Paths, request: CredentialReloadRequest) -> Result<Prepared, Rejected> {
    let access = Access::load(&paths.access, &paths.tls, paths.node).map_err(|e| Rejected {
        message: e.to_string().chars().take(256).collect(),
        uncertain: false,
    })?;
    if access.policy.generation() != request.replacement {
        return Err(Rejected {
            message: "replacement generation does not match file".into(),
            uncertain: false,
        });
    }
    let mut journal = journal(&paths).map_err(|e| Rejected {
        message: format!("{e:?}"),
        uncertain: true,
    })?;
    let record = CredentialReloadRecord {
        owner: paths.owner,
        request,
        digest: access.digest,
    };
    journal.publish(record).map_err(|e| Rejected {
        message: format!("{e:?}"),
        uncertain: matches!(
            e,
            CredentialJournalError::Io {
                uncertain: true,
                ..
            } | CredentialJournalError::Fenced
        ),
    })?;
    Ok(Prepared { access, record })
}
impl Reload {
    pub fn new(
        root: &Path,
        path: &Path,
        tls: &Path,
        node: u64,
        owner: PeerIdentity,
        access: &Access,
    ) -> Result<Self, Failure> {
        let paths = Paths {
            access: path.to_owned(),
            tls: tls.to_owned(),
            journal: root.join("CREDENTIAL-RELOAD"),
            node,
            owner,
        };
        let latest = journal(&paths)
            .and_then(|j| j.latest())
            .map_err(|e| format!("credential journal: {e:?}"))?;
        if let Some(record) = latest {
            let generation = access.policy.generation();
            if generation < record.request.replacement
                || (generation == record.request.replacement && access.digest != record.digest)
            {
                return Err("credential files conflict with durable reload record".into());
            }
        }
        Ok(Self {
            paths,
            latest,
            pending: None,
            failure: None,
        })
    }
    pub fn submit(
        &mut self,
        request: CredentialReloadRequest,
        current: CredentialGeneration,
    ) -> Result<String, String> {
        if request.sequence == 0 || request.replacement <= request.expected {
            return Err("invalid credential reload request".into());
        }
        if self.latest.is_some_and(|r| r.request == request) {
            return Ok(format!(
                "OK reload={} already_recorded=true",
                request.sequence
            ));
        }
        if let Some(p) = &self.pending {
            return if p.request == request {
                Ok(format!("OK reload={} pending=true", request.sequence))
            } else {
                Err("credential reload busy".into())
            };
        }
        if self
            .latest
            .is_some_and(|r| request.sequence <= r.request.sequence)
        {
            return Err("stale credential reload sequence".into());
        }
        if request.expected != current {
            return Err("credential generation changed".into());
        }
        let paths = self.paths.clone();
        let handle = thread::Builder::new()
            .name("voteboat-credential-reload".into())
            .spawn(move || prepare(paths, request))
            .map_err(|e| e.to_string())?;
        self.pending = Some(Pending { request, handle });
        self.failure = None;
        Ok(format!("OK reload={} queued=true", request.sequence))
    }
    /// true means uncertain durable state: fence command credentials and stop.
    pub fn poll(&mut self, active: &mut ActiveAccess) -> bool {
        if !self
            .pending
            .as_ref()
            .is_some_and(|p| p.handle.is_finished())
        {
            return false;
        }
        self.complete(active)
    }
    fn complete(&mut self, active: &mut ActiveAccess) -> bool {
        let pending = self.pending.take().unwrap();
        let result = pending.handle.join().unwrap_or_else(|_| {
            Err(Rejected {
                message: "credential worker panicked".into(),
                uncertain: true,
            })
        });
        match result {
            Ok(p) => {
                if active.generation() != Some(p.record.request.expected) {
                    active.close();
                    self.failure = Some((
                        pending.request.sequence,
                        "credential publication generation changed".into(),
                    ));
                    return true;
                }
                match active.replace(p.record.request.replacement, p.access) {
                    Ok(_) => {
                        self.latest = Some(p.record);
                        false
                    }
                    Err((e, _)) => {
                        active.close();
                        self.failure = Some((pending.request.sequence, format!("{e:?}")));
                        true
                    }
                }
            }
            Err(e) => {
                self.failure = Some((pending.request.sequence, e.message));
                if e.uncertain {
                    active.close();
                }
                e.uncertain
            }
        }
    }
    pub fn status(&self, sequence: u64, current: CredentialGeneration) -> String {
        let state = if self
            .pending
            .as_ref()
            .is_some_and(|p| p.request.sequence == sequence)
        {
            "pending"
        } else if self.failure.as_ref().is_some_and(|(s, _)| *s == sequence) {
            "failed"
        } else if self.latest.is_some_and(|r| r.request.sequence == sequence) {
            "recorded"
        } else {
            "unknown"
        };
        format!(
            "OK generation={} reload={} state={} recorded_generation={}",
            current.get(),
            sequence,
            state,
            self.latest.map_or(0, |r| r.request.replacement.get())
        )
    }
    pub fn finish(&mut self, active: &mut ActiveAccess) -> Result<(), Failure> {
        if self.pending.is_some() && self.complete(active) {
            return Err("uncertain credential reload; inspect durable record and files".into());
        }
        Ok(())
    }
}
