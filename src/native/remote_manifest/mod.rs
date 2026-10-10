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
//! Authenticated remote directory hints. These never carry local read authority.
mod channel;
mod client;
mod codec;
mod server;
use crate::{routing::ManifestDiscoveryError, secure::SessionError};
pub use client::{ManifestRefresh, ManifestRefreshCompletion, NativeRemoteManifestDiscovery};
pub use server::NativeManifestResponder;

pub const REMOTE_MANIFEST_WIRE_VERSION: u8 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteManifestError {
    Discovery(ManifestDiscoveryError),
    Session(SessionError),
    Protocol,
    Timeout,
    Cancelled,
}
impl From<ManifestDiscoveryError> for RemoteManifestError {
    fn from(error: ManifestDiscoveryError) -> Self {
        Self::Discovery(error)
    }
}
impl From<SessionError> for RemoteManifestError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct RemoteManifestConfig {
    pub timeout_ms: u64,
    pub retry_ms: u64,
    pub max_lifetime_ms: u64,
}
impl Default for RemoteManifestConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 10_000,
            retry_ms: 100,
            max_lifetime_ms: 60_000,
        }
    }
}
impl RemoteManifestConfig {
    fn validate(self) -> Result<(), RemoteManifestError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 60_000
            || self.retry_ms == 0
            || self.retry_ms > 60_000
            || self.max_lifetime_ms == 0
            || self.max_lifetime_ms > 3_600_000
        {
            return Err(ManifestDiscoveryError::InvalidLimits.into());
        }
        Ok(())
    }
}
