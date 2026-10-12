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
//! Sealed construction authority for physical and child-backed runtime owners.
use super::*;
use crate::logical_voter::{LogicalStoreMapping, RepresentativeBinding, VerifiedLogicalImage};

/// Identity inspection does not grant logical release authority. Physical hosts
/// may create metadata bindings; only verified logical startup mints logical mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeAuthorization {
    owner: RuntimeOwner,
    logical: Option<LogicalRuntimeAuthority>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalRuntimeAuthority {
    pub(crate) mapping: LogicalStoreMapping,
    pub(crate) representative: RepresentativeBinding,
}
impl RuntimeAuthorization {
    pub const fn physical(owner: RuntimeOwner) -> Self {
        Self {
            owner,
            logical: None,
        }
    }
    pub const fn owner(self) -> RuntimeOwner {
        self.owner
    }
    pub const fn is_logical(self) -> bool {
        self.logical.is_some()
    }
    /// The caller has consumed the original fresh-quorum image observation and
    /// checked the claimed representative's original outcome. Public images alone
    /// are not authority; this mint is confined to verified crate assembly.
    pub(crate) fn from_verified_image(
        owner: RuntimeOwner,
        verified: &VerifiedLogicalImage,
    ) -> Result<Self, EffectOwnerError> {
        let image = verified.image();
        if image.mapping.store != owner.store.identity
            || image.representative.logical_session != owner.store.session
            || image.parent.bootstrap.group != image.mapping.parent
        {
            return Err(EffectOwnerError::WrongAuthorization);
        }
        Ok(Self {
            owner,
            logical: Some(LogicalRuntimeAuthority {
                mapping: image.mapping,
                representative: image.representative,
            }),
        })
    }
    pub(crate) fn logical(self) -> Option<LogicalRuntimeAuthority> {
        self.logical
    }
}
