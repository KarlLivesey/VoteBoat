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
//! Command access adapter over the shared owned credential worker.
use super::{
    credential_worker as worker,
    service_access::{Access, ActiveAccess},
    setup::Failure,
};
use std::path::{Path, PathBuf};
use voteboat::{authorization::CredentialGeneration, secure::PeerIdentity};
#[cfg(test)]
#[path = "credential_reload_tests.rs"]
mod tests;
#[derive(Clone)]
struct Source {
    access: PathBuf,
    tls: PathBuf,
    node: u64,
}
impl worker::Source for Source {
    type Material = Access;
    fn load(&self) -> Result<worker::Loaded<Access>, Failure> {
        let access = Access::load(&self.access, &self.tls, self.node)?;
        Ok(worker::Loaded {
            generation: access.policy.generation(),
            digest: access.digest,
            material: access,
        })
    }
}
type Reload = worker::Reload<Source>;
fn reload(
    root: &Path,
    path: &Path,
    tls: &Path,
    node: u64,
    owner: PeerIdentity,
    access: &Access,
) -> Result<Reload, Failure> {
    Reload::new(
        worker::Paths {
            source: Source {
                access: path.to_owned(),
                tls: tls.to_owned(),
                node,
            },
            journal: root.join("CREDENTIAL-RELOAD"),
            owner,
        },
        access.policy.generation(),
        access.digest,
    )
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
        let reload = reload(root, path, tls, node, owner, &access)?;
        Ok(Self {
            access: Some(ActiveAccess::new(access.policy.generation(), access)),
            reload: Some(reload),
        })
    }
    pub fn poll(&mut self) -> bool {
        match (&mut self.reload, &mut self.access) {
            (Some(reload), Some(active)) => {
                let stop = reload.poll(active.generation(), |p| publish(active, p));
                if stop {
                    active.close();
                }
                stop
            }
            _ => false,
        }
    }
    pub fn finish(&mut self) -> Result<(), Failure> {
        match (&mut self.reload, &mut self.access) {
            (Some(reload), Some(active)) => {
                let result = reload.finish(active.generation(), |p| publish(active, p));
                if result.is_err() {
                    active.close();
                }
                result
            }
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
fn publish(active: &mut ActiveAccess, p: worker::Prepared<Access>) -> Result<(), String> {
    active
        .replace(p.record.request.replacement, p.material)
        .map(|_| ())
        .map_err(|(e, _)| format!("{e:?}"))
}
impl Commands<'_> {
    pub fn command(&mut self, words: &[&str]) -> Result<String, String> {
        let reload = self
            .reload
            .as_mut()
            .ok_or("credential reload requires authenticated service mode")?;
        let current = self.current.ok_or("credentials unavailable")?;
        match words {
            ["credential-status", sequence] => Ok(reload.status(
                sequence.parse().map_err(|_| "invalid reload sequence")?,
                current,
            )),
            ["reload-access", fields @ ..] => reload.submit(worker::request(fields)?, current),
            _ => Err(
                "expected credential-status REQUEST or reload-access REQUEST EXPECTED NEXT".into(),
            ),
        }
    }
}
