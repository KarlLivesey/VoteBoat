// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Authenticated adjacent hops retain physical origin and original packet scope.
use super::*;
use crate::{identity::*, secure::*, transport::*};
use std::{collections::BTreeMap, sync::Arc};
mod plan;
pub use plan::*;
mod frame;
pub use frame::{RelayFrame, RelayFrameKind, RelayRecordKind};
mod hop;
pub use hop::*;
mod forwarder;
pub use forwarder::*;
mod provider;
pub use provider::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayNetworkError {
    InvalidLimits,
    WrongBinding,
    WrongSource,
    WrongProfile,
    InvalidFrame,
    TooLarge,
    Closed,
    Overloaded,
    Truncated,
    TimeWentBack,
    Exhausted,
    ProviderViolation,
    Session(SessionError),
    Wire(WireError),
    Output(RelayOutputError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayNetworkLimits {
    pub frame_bytes: usize,
    pub decoded_bytes: usize,
    pub retained_bytes: usize,
    pub request_slots: usize,
    pub queued_frames: usize,
    pub control_frames: usize,
    pub source_slots: usize,
    pub hop_slots: usize,
}
impl Default for RelayNetworkLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 4 * 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
            retained_bytes: 64 * 1024 * 1024,
            request_slots: 4096,
            queued_frames: 64,
            control_frames: 8,
            source_slots: 1024,
            hop_slots: 64,
        }
    }
}
impl RelayNetworkLimits {
    pub fn validate(self) -> Result<Self, RelayNetworkError> {
        if !(1024..=16 * 1024 * 1024).contains(&self.frame_bytes)
            || !(1024..=256 * 1024 * 1024).contains(&self.decoded_bytes)
            || self.retained_bytes < self.frame_bytes
            || self.retained_bytes > 512 * 1024 * 1024
            || self.request_slots == 0
            || self.request_slots > 65536
            || self.queued_frames < 3
            || self.queued_frames > 4096
            || self.control_frames == 0
            || self.control_frames >= self.queued_frames
            || self.source_slots == 0
            || self.source_slots > 4096
            || self.hop_slots == 0
            || self.hop_slots > 4096
        {
            return Err(RelayNetworkError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Actual helper-side leaf-session capture. Catalogs have no constructor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelaySourceCapture {
    binding: SessionBinding,
    original_wire_version: u16,
    path: AuthenticatedDelegationPath,
}
impl RelaySourceCapture {
    pub fn binding(self) -> SessionBinding {
        self.binding
    }
    pub fn source(self) -> LocalIdentity {
        self.binding.peer
    }
    pub fn original_wire_version(self) -> u16 {
        self.original_wire_version
    }
    pub fn path(self) -> AuthenticatedDelegationPath {
        self.path
    }
    pub(super) fn extended(mut self, actual: SessionBinding) -> Result<Self, RelayNetworkError> {
        self.path = self
            .path
            .extended(actual)
            .ok_or(RelayNetworkError::WrongSource)?;
        Ok(self)
    }
}
pub(crate) struct VerifiedDelegatedSource {
    receiving: SessionBinding,
    captured: RelaySourceCapture,
}
impl VerifiedDelegatedSource {
    pub(crate) fn delegation_path(&self) -> AuthenticatedDelegationPath {
        self.captured.path
    }
    pub(crate) fn receiving(&self) -> SessionBinding {
        self.receiving
    }
    pub(crate) fn captured(&self) -> SessionBinding {
        self.captured.binding
    }
    pub(crate) fn source(&self) -> LocalIdentity {
        self.captured.source()
    }
    pub(crate) fn original_wire_version(&self) -> u16 {
        self.captured.original_wire_version
    }
}
