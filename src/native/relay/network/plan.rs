// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit checked physical-source/tree and nonvoting adjacent-hop permissions.
use super::*;
mod tree;
pub use tree::RelayTreePlan;
#[derive(Clone)]
pub struct NativeRelayPlan {
    local: LocalIdentity,
    helper: PeerIdentity,
    pub(super) tree: RelayTreePlan,
}
#[derive(Clone)]
pub struct RelayHelperPlan {
    local: LocalIdentity,
    pub(super) tree: RelayTreePlan,
}
impl NativeRelayPlan {
    pub fn new(
        local: LocalIdentity,
        helper: PeerIdentity,
        sources: BTreeMap<NodeId, StoreIdentity>,
        original_wire_version: u16,
    ) -> Result<Self, RelayNetworkError> {
        Self::for_tree(
            local,
            RelayTreePlan::star(sources, helper, original_wire_version)?,
        )
    }
    pub fn for_tree(local: LocalIdentity, tree: RelayTreePlan) -> Result<Self, RelayNetworkError> {
        if tree.sources.get(&local.node) != Some(&local.store.identity) {
            return Err(RelayNetworkError::WrongBinding);
        }
        let helper = tree
            .attached(local.node)
            .and_then(|n| tree.helper(n))
            .ok_or(RelayNetworkError::WrongBinding)?;
        Ok(Self {
            local,
            helper,
            tree,
        })
    }
    pub fn local(&self) -> LocalIdentity {
        self.local
    }
    pub fn helper(&self) -> PeerIdentity {
        self.helper
    }
    pub fn source(&self, node: NodeId) -> Option<PeerIdentity> {
        self.tree
            .sources
            .get(&node)
            .map(|&store| PeerIdentity { node, store })
    }
    pub fn sources(&self) -> impl Iterator<Item = PeerIdentity> + '_ {
        self.tree
            .sources
            .iter()
            .map(|(&node, &store)| PeerIdentity { node, store })
    }
    pub fn original_wire_version(&self) -> u16 {
        self.tree.original_wire_version
    }
}
impl RelayHelperPlan {
    pub fn new(
        local: LocalIdentity,
        sources: BTreeMap<NodeId, StoreIdentity>,
        original_wire_version: u16,
    ) -> Result<Self, RelayNetworkError> {
        Self::for_tree(
            local,
            RelayTreePlan::star(
                sources,
                PeerIdentity {
                    node: local.node,
                    store: local.store.identity,
                },
                original_wire_version,
            )?,
        )
    }
    pub fn for_tree(local: LocalIdentity, tree: RelayTreePlan) -> Result<Self, RelayNetworkError> {
        if tree
            .helper(local.node)
            .is_none_or(|p| p.store != local.store.identity)
        {
            return Err(RelayNetworkError::WrongBinding);
        }
        Ok(Self { local, tree })
    }
    pub fn local(&self) -> LocalIdentity {
        self.local
    }
    pub fn source(&self, node: NodeId) -> Option<PeerIdentity> {
        self.tree
            .sources
            .get(&node)
            .map(|&store| PeerIdentity { node, store })
    }
    pub fn sources(&self) -> impl Iterator<Item = PeerIdentity> + '_ {
        self.tree
            .sources
            .iter()
            .map(|(&node, &store)| PeerIdentity { node, store })
    }
    pub fn original_wire_version(&self) -> u16 {
        self.tree.original_wire_version
    }
    pub(super) fn adjacent(&self, peer: LocalIdentity) -> bool {
        self.tree.adjacent(self.local.node, peer.node)
            && self
                .source(peer.node)
                .or_else(|| self.tree.helper(peer.node))
                .is_some_and(|p| p.store == peer.store.identity)
    }
    pub(super) fn next_hop(&self, target: NodeId) -> Option<NodeId> {
        self.tree.next_hop(self.local.node, target)
    }
}
