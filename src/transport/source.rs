// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Observed authenticated source identity, separate from a static peer catalog.
use super::*;
mod path;
pub use path::*;

/// Identity observation only. This is never readiness, availability, durability
/// or read authority. The network owner must recheck its current access path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedSourceView {
    source: LocalIdentity,
    original_wire_version: u16,
    access: AuthenticatedSourcePath,
    delegation: Option<AuthenticatedDelegationPath>,
}
/// Original source identity and actual authenticated delivery path stay distinct.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticatedSourcePath {
    Direct(SessionBinding),
    Delegated {
        receiving: SessionBinding,
        captured: SessionBinding,
    },
}
impl AuthenticatedSourceView {
    /// Capture a real direct transport's authenticated binding. Claimed source
    /// identities and construction catalogs cannot produce this observation.
    pub fn from_direct_transport(
        transport: &(impl PeerTransport + ?Sized),
    ) -> Result<Self, TransportError> {
        let access = transport.binding();
        if transport.security() != SessionSecurity::Authenticated
            || transport.state() != TransportState::Open
            || access.local.node == access.peer.node
            || access.wire_version == 0
        {
            return Err(TransportError::WrongBinding);
        }
        Ok(Self {
            source: access.peer,
            original_wire_version: access.wire_version,
            access: AuthenticatedSourcePath::Direct(access),
            delegation: None,
        })
    }
    #[cfg(feature = "native")]
    pub(crate) fn from_delegated_capture(
        verified: &crate::native::relay::VerifiedDelegatedSource,
    ) -> Self {
        Self {
            source: verified.source(),
            original_wire_version: verified.original_wire_version(),
            access: AuthenticatedSourcePath::Delegated {
                receiving: verified.receiving(),
                captured: verified.captured(),
            },
            delegation: Some(verified.delegation_path()),
        }
    }
    pub fn source(self) -> LocalIdentity {
        self.source
    }
    pub fn original_wire_version(self) -> u16 {
        self.original_wire_version
    }
    /// Actual direct authenticated access; this never claims a relay is a leaf.
    pub fn direct_access(self) -> Option<SessionBinding> {
        match self.access {
            AuthenticatedSourcePath::Direct(binding) => Some(binding),
            _ => None,
        }
    }
    pub fn receiving_access(self) -> SessionBinding {
        match self.access {
            AuthenticatedSourcePath::Direct(binding) => binding,
            AuthenticatedSourcePath::Delegated { receiving, .. } => receiving,
        }
    }
    pub fn path(self) -> AuthenticatedSourcePath {
        self.access
    }
    pub fn delegation_path(self) -> Option<AuthenticatedDelegationPath> {
        self.delegation
    }
}
