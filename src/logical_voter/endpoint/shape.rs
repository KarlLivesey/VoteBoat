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
//! Bounded shape validation. Shape metadata never constructs provenance or authority.
use super::*;
pub(super) fn validate(
    mappings: &[LogicalStoreMapping],
    leaf_group: GroupIdentity,
    leaf: LocalIdentity,
) -> Result<(), LogicalEndpointError> {
    if mappings.len() > LOGICAL_ENDPOINT_MAX_DEPTH {
        return Err(LogicalEndpointError::TooDeep);
    }
    for (i, m) in mappings.iter().enumerate() {
        let next_group = mappings.get(i + 1).map_or(leaf_group, |next| next.parent);
        if m.child != next_group {
            return Err(LogicalEndpointError::WrongBinding);
        }
        if m.parent.id == m.child.id
            || m.parent.id == leaf_group.id
            || mappings[..i].iter().any(|old| old.parent.id == m.parent.id)
        {
            return Err(LogicalEndpointError::Cycle);
        }
        if m.voter == leaf.node
            || m.store.id == leaf.store.identity.id
            || mappings[..i]
                .iter()
                .any(|old| old.voter == m.voter || old.store.id == m.store.id)
        {
            return Err(LogicalEndpointError::DuplicateIdentity);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
