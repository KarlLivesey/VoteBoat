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
//! Caller-owned, bounded volatile manifest hints and explicit byte partitioning.
use crate::{identity::*, routing::*};
use std::collections::BTreeMap;

/// Scheme 1/version 1: exactly one unsigned byte, mapped to bucket 0..=255.
/// Half-open bucket boundaries are defined by BucketRange. No runtime hash.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeBytePartition;
impl PartitionPolicy for NativeBytePartition {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [value] => Ok(u16::from(*value)),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}

#[derive(Debug)]
pub struct NativeManifestCache {
    limits: ManifestCacheLimits,
    entries: BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>,
    bytes: usize,
    local_reparenting: bool,
    cross_reparenting: bool,
    metadata_moves: bool,
    metadata_locators: bool,
}
impl NativeManifestCache {
    pub fn new(limits: ManifestCacheLimits) -> Result<Self, RoutingError> {
        limits.validate()?;
        Ok(Self {
            limits,
            entries: BTreeMap::new(),
            bytes: 0,
            local_reparenting: false,
            cross_reparenting: false,
            metadata_moves: false,
            metadata_locators: false,
        })
    }
    /// Opt into trusted same-authority reparenting views before admitting hints.
    #[allow(clippy::result_large_err)]
    pub fn with_local_reparenting(mut self) -> Result<Self, (RoutingError, Self)> {
        if !self.entries.is_empty() || self.local_reparenting {
            return Err((RoutingError::InvalidLimits, self));
        }
        self.local_reparenting = true;
        Ok(self)
    }
    /// Select trusted cross-authority parent observations before admitting hints.
    /// Hosts authenticate the metadata observations; cache entries are not proof.
    #[allow(clippy::result_large_err)]
    pub fn with_cross_authority_reparenting(self) -> Result<Self, (RoutingError, Self)> {
        let mut selected = self.with_local_reparenting()?;
        selected.cross_reparenting = true;
        Ok(selected)
    }
    /// Select before admitting hints. Hosts authenticate the completed metadata
    /// move; this cache only checks the exact ownership-preserving transformation.
    #[allow(clippy::result_large_err)]
    pub fn with_metadata_authority_moves(mut self) -> Result<Self, (RoutingError, Self)> {
        if !self.entries.is_empty() || self.metadata_moves {
            return Err((RoutingError::InvalidLimits, self));
        }
        self.metadata_moves = true;
        Ok(self)
    }
    /// Accept authenticated foreign locator refreshes, selected before hints.
    #[allow(clippy::result_large_err)]
    pub fn with_metadata_locator_updates(mut self) -> Result<Self, (RoutingError, Self)> {
        if !self.entries.is_empty() || self.metadata_locators {
            return Err((RoutingError::InvalidLimits, self));
        }
        self.metadata_locators = true;
        Ok(self)
    }
    fn admission(&self, manifest: &ResponsibilityManifest) -> Result<usize, RoutingError> {
        let next = manifest.input();
        let mut old_bytes = 0;
        if let Some(old) = self.entries.get(&next.responsibility) {
            let prior = old.input();
            if next.generation < prior.generation {
                return Err(RoutingError::StaleGeneration);
            }
            if next.generation == prior.generation {
                return if manifest == old {
                    Ok(self.bytes)
                } else {
                    Err(RoutingError::GenerationConflict)
                };
            }
            let metadata_locator =
                self.metadata_locators && manifest.refreshes_metadata_locators(old);
            let metadata_move = self.metadata_moves && manifest.moves_metadata_from(old);
            if (next.parent != prior.parent
                && !metadata_move
                && !metadata_locator
                && !(self.local_reparenting && manifest.reparents_within_authority(old))
                && !(self.cross_reparenting && manifest.reparents_preserving_owner(old)))
                || (next.authority != prior.authority && !metadata_move)
                || next.scope != prior.scope
                || next.application != prior.application
                || next.scheme != prior.scheme
            {
                return Err(RoutingError::IdentityChange);
            }
            if next.epoch < prior.epoch {
                return Err(RoutingError::EpochRegression);
            }
            if next.epoch == prior.epoch
                && next.execution != prior.execution
                && !manifest.refreshes_child_epochs(old)
                && !metadata_move
                && !metadata_locator
                && !manifest.retires_child_slots(old)
                && !(self.local_reparenting && manifest.fills_vacant_child_slots(old))
            {
                return Err(RoutingError::EpochMismatch);
            }
            if prior.state == ResponsibilityState::Fenced
                && next.state == ResponsibilityState::Active
                && next.epoch == prior.epoch
            {
                return Err(RoutingError::Fenced);
            }
            old_bytes = old.retained_bytes();
        } else if self.entries.len() == self.limits.manifests {
            return Err(RoutingError::Capacity);
        }
        let bytes = self.bytes - old_bytes + manifest.retained_bytes();
        if bytes > self.limits.bytes {
            return Err(RoutingError::Capacity);
        }
        Ok(bytes)
    }
}
impl ManifestCache for NativeManifestCache {
    fn get(&self, responsibility: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.entries.get(&responsibility)
    }
    fn admit(
        &mut self,
        manifest: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        let bytes = match self.admission(&manifest) {
            Ok(bytes) => bytes,
            Err(error) => return Err((error, manifest)),
        };
        let next = manifest.input();
        if self
            .entries
            .get(&next.responsibility)
            .is_some_and(|old| old.input().generation == next.generation)
        {
            return Ok(());
        }
        self.entries.insert(next.responsibility, manifest);
        self.bytes = bytes;
        Ok(())
    }
    fn invalidate(
        &mut self,
        responsibility: ResponsibilityIdentity,
        observed: RouteGeneration,
    ) -> bool {
        if self
            .entries
            .get(&responsibility)
            .is_some_and(|manifest| manifest.input().generation == observed)
        {
            let old = self.entries.remove(&responsibility).unwrap();
            self.bytes -= old.retained_bytes();
            true
        } else {
            false
        }
    }
    fn limits(&self) -> ManifestCacheLimits {
        self.limits
    }
    fn usage(&self) -> ManifestCacheUsage {
        ManifestCacheUsage {
            manifests: self.entries.len(),
            bytes: self.bytes,
        }
    }
}
