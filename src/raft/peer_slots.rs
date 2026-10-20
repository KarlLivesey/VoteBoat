// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Per-peer leader slots that keep their keys across replication rounds.
//!
//! A slot table replaces a `BTreeMap<NodeId, T>` that emptied and refilled
//! every round. Keys are sorted and unique; `remove` empties a slot in place
//! and keeps its key, so the next round's `insert` for that peer writes in
//! place and allocates nothing. `clear` drops every key. The leader clears at
//! every configuration and leadership change, so keys never outlive the
//! membership that created them. Reads see exactly the occupied slots, in
//! `NodeId` order, as the map did.
use crate::identity::NodeId;
#[cfg(debug_assertions)]
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Index;

pub(super) struct PeerSlots<T> {
    slots: Vec<(NodeId, Option<T>)>,
    occupied: usize,
    // Debug builds run the old map form beside the slots and compare every
    // result, so every debug test exercises both.
    #[cfg(debug_assertions)]
    oracle: BTreeMap<NodeId, T>,
}

impl<T: Copy + PartialEq + Debug> PeerSlots<T> {
    pub(super) fn new() -> Self {
        Self {
            slots: Vec::new(),
            occupied: 0,
            #[cfg(debug_assertions)]
            oracle: BTreeMap::new(),
        }
    }
    fn position(&self, peer: NodeId) -> Result<usize, usize> {
        self.slots.binary_search_by_key(&peer, |(node, _)| *node)
    }
    pub(super) fn get(&self, peer: &NodeId) -> Option<&T> {
        let found = self
            .position(*peer)
            .ok()
            .and_then(|at| self.slots[at].1.as_ref());
        #[cfg(debug_assertions)]
        assert_eq!(found, self.oracle.get(peer), "peer slot read");
        found
    }
    pub(super) fn contains_key(&self, peer: &NodeId) -> bool {
        self.get(peer).is_some()
    }
    pub(super) fn insert(&mut self, peer: NodeId, value: T) -> Option<T> {
        let old = match self.position(peer) {
            Ok(at) => self.slots[at].1.replace(value),
            Err(at) => {
                self.slots.insert(at, (peer, Some(value)));
                None
            }
        };
        if old.is_none() {
            self.occupied += 1;
        }
        #[cfg(debug_assertions)]
        {
            assert_eq!(old, self.oracle.insert(peer, value), "peer slot insert");
            self.check();
        }
        old
    }
    pub(super) fn remove(&mut self, peer: &NodeId) -> Option<T> {
        let old = self
            .position(*peer)
            .ok()
            .and_then(|at| self.slots[at].1.take());
        if old.is_some() {
            self.occupied -= 1;
        }
        #[cfg(debug_assertions)]
        {
            assert_eq!(old, self.oracle.remove(peer), "peer slot remove");
            self.check();
        }
        old
    }
    pub(super) fn clear(&mut self) {
        self.slots.clear();
        self.occupied = 0;
        #[cfg(debug_assertions)]
        {
            self.oracle.clear();
            self.check();
        }
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.occupied
    }
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.occupied == 0
    }
    /// Occupied slots in `NodeId` order (the map's iteration order). Only
    /// the debug oracle and tests read the whole view.
    #[cfg(any(test, debug_assertions))]
    pub(super) fn iter(&self) -> impl Iterator<Item = (&NodeId, &T)> {
        self.slots
            .iter()
            .filter_map(|(node, value)| value.as_ref().map(|value| (node, value)))
    }
    /// Every retained key, occupied or not: the table's lifetime footprint.
    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.slots.iter().map(|(node, _)| *node)
    }
    #[cfg(debug_assertions)]
    fn check(&self) {
        assert!(
            self.slots.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "peer slot keys sorted and unique"
        );
        assert_eq!(self.occupied, self.oracle.len(), "peer slot count");
        assert!(self.iter().eq(self.oracle.iter()), "peer slot view");
        #[cfg(test)]
        tests::count_check();
    }
}

impl<T: Copy + PartialEq + Debug> Index<&NodeId> for PeerSlots<T> {
    type Output = T;
    fn index(&self, peer: &NodeId) -> &T {
        self.get(peer).expect("peer slot is occupied")
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        static CHECKS: Cell<u64> = const { Cell::new(0) };
    }
    #[cfg(debug_assertions)]
    pub(super) fn count_check() {
        CHECKS.with(|c| c.set(c.get() + 1));
    }
    /// Map-oracle comparisons made on this thread so far.
    pub(in crate::raft) fn checks() -> u64 {
        CHECKS.with(Cell::get)
    }

    fn node(n: u64) -> NodeId {
        NodeId::new(n).unwrap()
    }

    /// Deterministic xorshift; no clock or global state.
    pub(in crate::raft) struct Rng(pub(in crate::raft) u64);
    impl Rng {
        pub(in crate::raft) fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        pub(in crate::raft) fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn slots_match_the_map_over_a_random_operation_history() {
        let mut slots = PeerSlots::new();
        let mut map = std::collections::BTreeMap::new();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut capacity = 0;
        for step in 0..20_000u64 {
            let peer = node(1 + rng.below(9));
            match rng.below(16) {
                0 => {
                    slots.clear();
                    map.clear();
                    assert_eq!(slots.keys().count(), 0);
                }
                1..=6 => assert_eq!(slots.insert(peer, step), map.insert(peer, step)),
                7..=11 => assert_eq!(slots.remove(&peer), map.remove(&peer)),
                _ => {
                    assert_eq!(slots.get(&peer), map.get(&peer));
                    assert_eq!(slots.contains_key(&peer), map.contains_key(&peer));
                }
            }
            assert_eq!(slots.len(), map.len());
            assert_eq!(slots.is_empty(), map.is_empty());
            assert!(slots.iter().eq(map.iter()));
            // Keys are only peers inserted since the last clear.
            assert!(slots.keys().count() <= 9);
            capacity = capacity.max(slots.slots.capacity());
        }
        // The table stops growing once it has held every peer once.
        assert!(capacity <= 16, "capacity {capacity}");
    }

    #[test]
    fn a_retired_slot_is_rewritten_in_place_without_allocating() {
        let mut slots = PeerSlots::new();
        slots.insert(node(2), 1u64);
        slots.insert(node(3), 2u64);
        let buffer = slots.slots.as_ptr();
        for round in 0..1_000 {
            assert_eq!(slots.remove(&node(2)), Some(round + 1));
            assert_eq!(slots.remove(&node(3)), Some(round + 2));
            assert!(slots.is_empty());
            assert_eq!(slots.keys().count(), 2);
            slots.insert(node(3), round + 3);
            slots.insert(node(2), round + 2);
            assert_eq!(slots[&node(2)], round + 2);
        }
        assert!(std::ptr::eq(buffer, slots.slots.as_ptr()));
        slots.clear();
        assert_eq!(slots.keys().count(), 0);
        assert!(slots.get(&node(2)).is_none());
    }
}
