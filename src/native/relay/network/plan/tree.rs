// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Checked static tree routes are permissions, never captured source authority.
use super::*;
use std::collections::{BTreeSet, VecDeque};
#[derive(Clone, Eq, PartialEq)]
pub struct RelayTreePlan {
    pub(super) sources: Arc<BTreeMap<NodeId, StoreIdentity>>,
    helpers: Arc<BTreeMap<NodeId, StoreIdentity>>,
    attachments: Arc<BTreeMap<NodeId, NodeId>>,
    adjacency: Arc<BTreeMap<NodeId, BTreeSet<NodeId>>>,
    pub(super) original_wire_version: u16,
    pub(super) max_hops: usize,
}
impl RelayTreePlan {
    pub fn new(
        sources: BTreeMap<NodeId, StoreIdentity>,
        helpers: BTreeMap<NodeId, StoreIdentity>,
        edges: BTreeSet<(NodeId, NodeId)>,
        attachments: BTreeMap<NodeId, NodeId>,
        original_wire_version: u16,
        max_hops: usize,
    ) -> Result<Self, RelayNetworkError> {
        if sources.is_empty()
            || sources.len() > 4096
            || helpers.is_empty()
            || helpers.len() > 4096
            || original_wire_version == 0
            || !(1..=AUTHENTICATED_DELEGATION_MAX_HOPS).contains(&max_hops)
            || sources.keys().any(|n| helpers.contains_key(n))
            || sources
                .values()
                .chain(helpers.values())
                .map(|s| (s.id.get(), s.incarnation.get()))
                .collect::<BTreeSet<_>>()
                .len()
                != sources.len() + helpers.len()
            || attachments.len() != sources.len()
            || sources
                .keys()
                .any(|n| attachments.get(n).is_none_or(|h| !helpers.contains_key(h)))
            || edges.len() != helpers.len() - 1
        {
            return Err(RelayNetworkError::WrongBinding);
        }
        let mut adjacency: BTreeMap<_, BTreeSet<_>> =
            helpers.keys().map(|n| (*n, BTreeSet::new())).collect();
        for (a, b) in edges {
            if a >= b || !helpers.contains_key(&a) || !helpers.contains_key(&b) {
                return Err(RelayNetworkError::WrongBinding);
            }
            adjacency.get_mut(&a).unwrap().insert(b);
            adjacency.get_mut(&b).unwrap().insert(a);
        }
        let tree = Self {
            sources: Arc::new(sources),
            helpers: Arc::new(helpers),
            attachments: Arc::new(attachments),
            adjacency: Arc::new(adjacency),
            original_wire_version,
            max_hops,
        };
        for &start in tree.helpers.keys() {
            let distances = tree.distances(start);
            if distances.len() != tree.helpers.len() || distances.values().any(|d| *d >= max_hops) {
                return Err(RelayNetworkError::InvalidLimits);
            }
        }
        Ok(tree)
    }
    pub fn max_hops(&self) -> usize {
        self.max_hops
    }
    fn distances(&self, start: NodeId) -> BTreeMap<NodeId, usize> {
        let mut distances = BTreeMap::from([(start, 0)]);
        let mut queue = VecDeque::from([start]);
        while let Some(at) = queue.pop_front() {
            for &next in &self.adjacency[&at] {
                if !distances.contains_key(&next) {
                    distances.insert(next, distances[&at] + 1);
                    queue.push_back(next);
                }
            }
        }
        distances
    }
    pub(in crate::native::relay::network) fn star(
        sources: BTreeMap<NodeId, StoreIdentity>,
        helper: PeerIdentity,
        version: u16,
    ) -> Result<Self, RelayNetworkError> {
        let attachments = sources.keys().map(|n| (*n, helper.node)).collect();
        Self::new(
            sources,
            BTreeMap::from([(helper.node, helper.store)]),
            BTreeSet::new(),
            attachments,
            version,
            1,
        )
    }
    pub(in crate::native::relay::network) fn helper(&self, n: NodeId) -> Option<PeerIdentity> {
        self.helpers.get(&n).map(|store| PeerIdentity {
            node: n,
            store: *store,
        })
    }
    pub(in crate::native::relay::network) fn attached(&self, n: NodeId) -> Option<NodeId> {
        self.attachments.get(&n).copied()
    }
    pub(in crate::native::relay::network) fn adjacent(&self, local: NodeId, peer: NodeId) -> bool {
        self.adjacency
            .get(&local)
            .is_some_and(|a| a.contains(&peer))
            || self.attached(peer) == Some(local)
    }
    pub(in crate::native::relay::network) fn route(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Option<Vec<NodeId>> {
        let mut queue = VecDeque::from([vec![from]]);
        while let Some(route) = queue.pop_front() {
            let at = *route.last()?;
            if at == to {
                return Some(route);
            }
            for &next in self.adjacency.get(&at)? {
                if !route.contains(&next) {
                    let mut path = route.clone();
                    path.push(next);
                    queue.push_back(path);
                }
            }
        }
        None
    }
    pub(in crate::native::relay::network) fn next_hop(
        &self,
        helper: NodeId,
        target: NodeId,
    ) -> Option<NodeId> {
        let end = self.attached(target)?;
        if helper == end {
            Some(target)
        } else {
            self.route(helper, end)?.get(1).copied()
        }
    }
    pub(in crate::native::relay::network) fn path_matches(
        &self,
        capture: RelaySourceCapture,
        end: LocalIdentity,
    ) -> bool {
        let first = capture.path.first();
        let Some(route) = self
            .attached(first.peer.node)
            .and_then(|s| self.route(s, end.node))
        else {
            return false;
        };
        if capture.binding != first
            || capture.original_wire_version != self.original_wire_version
            || capture.path.len() != route.len()
            || capture.path.len() > self.max_hops
            || self.sources.get(&first.peer.node) != Some(&first.peer.store.identity)
            || capture.path.last().local != end
        {
            return false;
        }
        let mut previous = None;
        for (binding, node) in capture.path.hops().zip(route) {
            if binding.local.node != node
                || self.helpers.get(&node) != Some(&binding.local.store.identity)
                || binding.wire_version != RELAY_NETWORK_VERSION
                || previous.is_some_and(|local| binding.peer != local)
            {
                return false;
            }
            previous = Some(binding.local);
        }
        true
    }
}

#[cfg(test)]
mod stale_capture_tests {
    use super::*;
    fn store(n: u64) -> StoreIdentity {
        StoreIdentity {
            id: StoreId::new(u128::from(n)).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        }
    }
    fn local(n: u64, session: u64) -> LocalIdentity {
        LocalIdentity {
            node: NodeId::new(n).unwrap(),
            store: StoreBinding {
                identity: store(n),
                session: StoreSession::new(session).unwrap(),
            },
        }
    }
    /// A source channel captured by helper 9 in `session`.
    fn capture(session: u64, generation: u64) -> RelaySourceCapture {
        let binding = SessionBinding {
            local: local(9, session),
            peer: local(1, 1),
            generation: SecureSessionGeneration::new(generation).unwrap(),
            wire_version: RELAY_NETWORK_VERSION,
        };
        RelaySourceCapture {
            binding,
            original_wire_version: 8,
            path: AuthenticatedDelegationPath::leaf(binding),
        }
    }

    /// NT1 round 2 (Sol 2): helper 9 restarted from store session 1 into 2,
    /// and its connection generations restarted at 1. A capture from session
    /// 1 is refused by the recovered helper whether its numeric generation is
    /// higher than or equal to the fresh one: the path compares the full
    /// local identity, including the store session.
    #[test]
    fn a_recovered_helper_refuses_captures_of_its_previous_session() {
        let sources = (1..=3)
            .map(|n| (NodeId::new(n).unwrap(), store(n)))
            .collect();
        let helper = PeerIdentity {
            node: NodeId::new(9).unwrap(),
            store: store(9),
        };
        let tree = RelayTreePlan::star(sources, helper, 8).unwrap();
        let recovered = local(9, 2);
        assert!(tree.path_matches(capture(2, 1), recovered), "control");
        for generation in [1, 50_000] {
            assert!(!tree.path_matches(capture(1, generation), recovered));
        }
        // The old capture still matches only the session that made it.
        assert!(tree.path_matches(capture(1, 50_000), local(9, 1)));
    }
}
