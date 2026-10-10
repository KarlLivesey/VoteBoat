// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Shared single-flight preparation and durable credential record ordering.
use crate::setup::Failure;
use std::{
    path::PathBuf,
    thread::{self, JoinHandle},
};
use voteboat::{
    authorization::CredentialGeneration, credential_reload::*, native::credential_journal::*,
    secure::PeerIdentity,
};

pub struct Loaded<M> {
    pub material: M,
    pub generation: CredentialGeneration,
    pub digest: [u8; 32],
}
pub trait Source: Clone + Send + 'static {
    type Material: Send + 'static;
    fn load(&self) -> Result<Loaded<Self::Material>, Failure>;
}
#[derive(Clone)]
pub struct Paths<S> {
    pub source: S,
    pub journal: PathBuf,
    pub owner: PeerIdentity,
}
pub struct Prepared<M> {
    pub material: M,
    pub record: CredentialReloadRecord,
}
pub struct Rejected {
    pub message: String,
    pub uncertain: bool,
}
pub struct Pending<M> {
    pub request: CredentialReloadRequest,
    pub handle: JoinHandle<Result<Prepared<M>, Rejected>>,
}
pub struct Reload<S: Source> {
    pub(super) paths: Paths<S>,
    pub(super) latest: Option<CredentialReloadRecord>,
    pub(super) pending: Option<Pending<S::Material>>,
    failure: Option<(u64, String)>,
    fenced: bool,
}
pub fn journal<S>(paths: &Paths<S>) -> Result<NativeCredentialJournal, CredentialJournalError> {
    NativeCredentialJournal::new(FileCredentialRecord::new(&paths.journal), paths.owner)
        .map_err(|(e, _)| e)
}
pub fn prepare<S: Source>(
    paths: Paths<S>,
    request: CredentialReloadRequest,
) -> Result<Prepared<S::Material>, Rejected> {
    let loaded = paths.source.load().map_err(|e| Rejected {
        message: e.to_string().chars().take(256).collect(),
        uncertain: false,
    })?;
    if loaded.generation != request.replacement {
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
        digest: loaded.digest,
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
    Ok(Prepared {
        material: loaded.material,
        record,
    })
}
impl<S: Source> Reload<S> {
    pub fn new(
        paths: Paths<S>,
        generation: CredentialGeneration,
        digest: [u8; 32],
    ) -> Result<Self, Failure> {
        let latest = journal(&paths)
            .and_then(|j| j.latest())
            .map_err(|e| format!("credential journal: {e:?}"))?;
        if latest.is_some_and(|r| {
            generation < r.request.replacement
                || generation == r.request.replacement && digest != r.digest
        }) {
            return Err("credential files conflict with durable reload record".into());
        }
        Ok(Self {
            paths,
            latest,
            pending: None,
            failure: None,
            fenced: false,
        })
    }
    pub fn latest(&self) -> Option<CredentialReloadRecord> {
        self.latest
    }
    pub fn submit(
        &mut self,
        request: CredentialReloadRequest,
        current: CredentialGeneration,
    ) -> Result<String, String> {
        if self.fenced {
            return Err("credential preparation fenced; restart required".into());
        }
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
    pub fn poll(
        &mut self,
        current: Option<CredentialGeneration>,
        publish: impl FnOnce(Prepared<S::Material>) -> Result<(), String>,
    ) -> bool {
        if !self
            .pending
            .as_ref()
            .is_some_and(|p| p.handle.is_finished())
        {
            return false;
        }
        self.complete(current, publish)
    }
    fn complete(
        &mut self,
        current: Option<CredentialGeneration>,
        publish: impl FnOnce(Prepared<S::Material>) -> Result<(), String>,
    ) -> bool {
        let pending = self.pending.take().unwrap();
        let result = join(pending.handle).and_then(|p| {
            if current != Some(p.record.request.expected) {
                return Err(Rejected {
                    message: "credential publication generation changed".into(),
                    uncertain: true,
                });
            }
            let record = p.record;
            publish(p).map_err(|message| Rejected {
                message,
                uncertain: true,
            })?;
            Ok(record)
        });
        match result {
            Ok(record) => {
                self.latest = Some(record);
                false
            }
            Err(e) => {
                self.failure = Some((pending.request.sequence, e.message));
                self.fenced = e.uncertain;
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
        } else if self
            .latest()
            .is_some_and(|r| r.request.sequence == sequence)
        {
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
    pub fn finish(
        &mut self,
        current: Option<CredentialGeneration>,
        publish: impl FnOnce(Prepared<S::Material>) -> Result<(), String>,
    ) -> Result<(), Failure> {
        if self.fenced || self.pending.is_some() && self.complete(current, publish) {
            return Err("uncertain credential reload; inspect durable record and files".into());
        }
        Ok(())
    }
    /// A stopped node cannot publish into its closed peer connector. Join any
    /// accepted preparation before releasing its store; startup reconciles it.
    pub fn finish_for_restart(&mut self) -> Result<(), Failure> {
        if let Some(pending) = self.pending.take() {
            if let Err(e) = join(pending.handle) {
                if e.uncertain {
                    self.fenced = true;
                    return Err("uncertain peer reload; inspect durable record and files".into());
                }
            }
        }
        if self.fenced {
            return Err("uncertain credential preparation; restart required".into());
        }
        Ok(())
    }
}
impl<S: Source> Drop for Reload<S> {
    fn drop(&mut self) {
        let _ = self.finish_for_restart();
    }
}
fn join<M>(handle: JoinHandle<Result<Prepared<M>, Rejected>>) -> Result<Prepared<M>, Rejected> {
    handle.join().unwrap_or_else(|_| {
        Err(Rejected {
            message: "credential worker panicked".into(),
            uncertain: true,
        })
    })
}
pub fn request(words: &[&str]) -> Result<CredentialReloadRequest, String> {
    let [sequence, expected, replacement] = words else {
        return Err("expected REQUEST EXPECTED NEXT".into());
    };
    let generation = |value: &str| {
        value
            .parse::<u64>()
            .ok()
            .and_then(CredentialGeneration::new)
            .ok_or_else(|| "invalid credential generation".to_string())
    };
    Ok(CredentialReloadRequest {
        sequence: sequence.parse().map_err(|_| "invalid reload sequence")?,
        expected: generation(expected)?,
        replacement: generation(replacement)?,
    })
}
