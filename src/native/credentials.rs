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
//! Explicit publication of prepared credentials and revocation of old sessions.
use crate::{
    authorization::CredentialGeneration,
    secure::{SessionError, SessionValidity},
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
#[derive(Clone)]
pub struct CredentialLease {
    current: Arc<AtomicU64>,
    generation: CredentialGeneration,
}
impl CredentialLease {
    pub fn generation(&self) -> CredentialGeneration {
        self.generation
    }
}
impl SessionValidity for CredentialLease {
    fn validate(&self) -> Result<(), SessionError> {
        if self.current.load(Ordering::Acquire) == self.generation.get() {
            Ok(())
        } else {
            Err(SessionError::Revoked)
        }
    }
}
/// One current, already validated host bundle; no file I/O or credential parser.
/// P may combine TLS material, peer pins and service policy. Publish only after
/// validating the whole bundle. Generations must remain monotonic across restart
/// in the host's configuration; they do not establish Raft or storage authority.
pub struct NativeCredentialSet<P> {
    current: Arc<AtomicU64>,
    material: Option<P>,
}
impl<P> NativeCredentialSet<P> {
    pub fn new(generation: CredentialGeneration, material: P) -> Self {
        Self {
            current: Arc::new(AtomicU64::new(generation.get())),
            material: Some(material),
        }
    }
    pub fn generation(&self) -> Option<CredentialGeneration> {
        CredentialGeneration::new(self.current.load(Ordering::Acquire))
    }
    pub fn material(&self) -> Option<&P> {
        self.generation().and(self.material.as_ref())
    }
    pub fn lease(&self) -> Result<CredentialLease, SessionError> {
        Ok(CredentialLease {
            current: self.current.clone(),
            generation: self.generation().ok_or(SessionError::Revoked)?,
        })
    }
    /// Returns rejected input intact. Old leases are irrevocably invalid after
    /// publication. No user callbacks or old-bundle destruction run in publish.
    pub fn replace(
        &mut self,
        generation: CredentialGeneration,
        material: P,
    ) -> Result<P, (SessionError, P)> {
        let Some(current) = self.generation() else {
            return Err((SessionError::Revoked, material));
        };
        if generation <= current {
            return Err((SessionError::InvalidCredentials, material));
        }
        let old = self
            .material
            .replace(material)
            .expect("open credential set has material");
        self.current.store(generation.get(), Ordering::Release);
        Ok(old)
    }
    pub fn close(&mut self) {
        self.current.store(0, Ordering::Release);
    }
    pub fn into_material(mut self) -> P {
        self.close();
        self.material
            .take()
            .expect("credential set retains material")
    }
}
impl<P> Drop for NativeCredentialSet<P> {
    fn drop(&mut self) {
        self.current.store(0, Ordering::Release);
    }
}
