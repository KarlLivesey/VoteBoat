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
//! Finite explicit peer-hint cache; no implicit fetch, workers or authority.
use crate::{discovery::*, identity::*, runtime::MonoTime, secure::PeerIdentity};
use std::collections::BTreeMap;
type Key = (NodeId, StoreId, StoreIncarnation);
fn key(peer: PeerIdentity) -> Key {
    (peer.node, peer.store.id, peer.store.incarnation)
}
struct Entry {
    floor: PeerEndpointHint,
    live: bool,
}
pub struct NativePeerDiscovery {
    capacity: usize,
    entries: BTreeMap<Key, Entry>,
    now: MonoTime,
    closed: bool,
}
impl NativePeerDiscovery {
    pub fn new(capacity: usize, now: MonoTime) -> Result<Self, DiscoveryError> {
        if capacity == 0 || capacity > 1024 {
            return Err(DiscoveryError::InvalidLimits);
        }
        Ok(Self {
            capacity,
            entries: BTreeMap::new(),
            now,
            closed: false,
        })
    }
    /// Invalidation/expiry retain a floor slot. No eviction silently forgets it.
    pub fn retained_peers(&self) -> usize {
        self.entries.len()
    }
    pub fn publish(
        &mut self,
        hint: PeerEndpointHint,
        now: MonoTime,
    ) -> Result<(), (DiscoveryError, PeerEndpointHint)> {
        self.publish_hint(hint, now, false)
    }
    /// Only a fresh, authenticated request/response may renew an unchanged endpoint.
    /// Local publication still cannot revive an invalidated generation or alter TTL.
    pub(super) fn revalidate(
        &mut self,
        hint: PeerEndpointHint,
        now: MonoTime,
    ) -> Result<(), (DiscoveryError, PeerEndpointHint)> {
        self.publish_hint(hint, now, true)
    }
    fn publish_hint(
        &mut self,
        hint: PeerEndpointHint,
        now: MonoTime,
        renewal: bool,
    ) -> Result<(), (DiscoveryError, PeerEndpointHint)> {
        let check = (|| {
            if self.closed {
                return Err(DiscoveryError::Closed);
            }
            if now < self.now {
                return Err(DiscoveryError::TimeWentBack);
            }
            hint.validate(hint.peer, now)?;
            if let Some(entry) = self.entries.get(&key(hint.peer)) {
                if hint.generation < entry.floor.generation {
                    return Err(DiscoveryError::StaleGeneration);
                }
                if hint.generation == entry.floor.generation {
                    if hint.endpoint != entry.floor.endpoint
                        || (!renewal && hint.expires_at != entry.floor.expires_at)
                    {
                        return Err(DiscoveryError::ConflictingGeneration);
                    }
                    if !renewal && !entry.live {
                        return Err(DiscoveryError::StaleGeneration);
                    }
                }
            } else if self.entries.len() >= self.capacity {
                return Err(DiscoveryError::Overloaded);
            }
            Ok(())
        })();
        if let Err(error) = check {
            return Err((error, hint));
        }
        self.now = now;
        self.entries.insert(
            key(hint.peer),
            Entry {
                floor: hint,
                live: true,
            },
        );
        Ok(())
    }
}
impl PeerDiscovery for NativePeerDiscovery {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        self.now = now;
        let entry = self
            .entries
            .get_mut(&key(peer))
            .ok_or(DiscoveryError::Missing)?;
        if now >= entry.floor.expires_at {
            entry.live = false;
            return Err(DiscoveryError::Expired);
        }
        if !entry.live {
            return Err(DiscoveryError::Missing);
        }
        entry.floor.validate(peer, now)
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        if self.closed {
            return false;
        }
        let Some(entry) = self.entries.get_mut(&key(peer)) else {
            return false;
        };
        if entry.floor.generation != generation || !entry.live {
            return false;
        }
        entry.live = false;
        true
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
