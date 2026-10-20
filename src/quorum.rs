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
//! Validated recursive strict-majority quorum predicates.
use std::collections::{BTreeMap, BTreeSet};

use crate::identity::NodeId;
mod explanation;
pub use explanation::{JointExplanation, QuorumExplanation, QuorumNode, QuorumRule};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Tree {
    Voter(NodeId),
    Majority(Vec<Tree>),
    Weighted(Vec<WeightedChild>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeightedChild {
    pub weight: u64,
    pub node: Tree,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_depth: usize,
    pub max_voters: usize,
    pub max_tree_nodes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_voters: 4096,
            max_tree_nodes: 16384,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyError {
    EmptyBranch,
    DuplicateVoter(NodeId),
    ZeroWeight,
    WeightOverflow,
    TooDeep,
    TooManyVoters,
    TooManyTreeNodes,
    VoterSetMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Policy {
    tree: Tree,
    voters: BTreeSet<NodeId>,
}

impl Policy {
    /// Validate structural safety before constructing an evaluable policy.
    /// Application integration must also compare the exact allowed voter set.
    pub fn new(tree: Tree, limits: Limits) -> Result<Self, PolicyError> {
        let mut voters = BTreeSet::new();
        let mut count = 0;
        validate(&tree, 0, limits, &mut voters, &mut count)?;
        Ok(Self { tree, voters })
    }

    /// Checks the exact authorized voter set, including absent and unknown leaves.
    pub fn for_voters(
        tree: Tree,
        limits: Limits,
        expected: &BTreeSet<NodeId>,
    ) -> Result<Self, PolicyError> {
        let policy = Self::new(tree, limits)?;
        if &policy.voters != expected {
            return Err(PolicyError::VoterSetMismatch);
        }
        Ok(policy)
    }

    pub fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    /// Immutable validated structure, used by versioned persistence codecs.
    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    /// `acks` must already have been authenticated and scoped to the correct
    /// group, incarnation, term, configuration and request by the caller.
    pub fn is_satisfied(&self, acks: &BTreeSet<NodeId>) -> bool {
        satisfied(&self.tree, acks)
    }

    /// Highest index satisfying the policy from *matching durable prefixes*.
    /// This is NOT the Raft commit rule: term, leadership, configuration and
    /// persistence-effect conditions are deliberately outside this model.
    pub fn frontier(&self, durable_prefixes: &BTreeMap<NodeId, u64>) -> u64 {
        frontier(&self.tree, durable_prefixes)
    }
}

#[derive(Clone, Debug)]
pub struct JointPolicy {
    pub old: Policy,
    pub new: Policy,
}

impl JointPolicy {
    pub fn is_satisfied(&self, acks: &BTreeSet<NodeId>) -> bool {
        self.old.is_satisfied(acks) && self.new.is_satisfied(acks)
    }

    pub fn frontier(&self, durable_prefixes: &BTreeMap<NodeId, u64>) -> u64 {
        self.old
            .frontier(durable_prefixes)
            .min(self.new.frontier(durable_prefixes))
    }
}

fn validate(
    tree: &Tree,
    depth: usize,
    limits: Limits,
    voters: &mut BTreeSet<NodeId>,
    count: &mut usize,
) -> Result<(), PolicyError> {
    if depth > limits.max_depth {
        return Err(PolicyError::TooDeep);
    }
    *count = count.checked_add(1).ok_or(PolicyError::TooManyTreeNodes)?;
    if *count > limits.max_tree_nodes {
        return Err(PolicyError::TooManyTreeNodes);
    }
    match tree {
        Tree::Voter(id) => {
            if !voters.insert(*id) {
                return Err(PolicyError::DuplicateVoter(*id));
            }
            if voters.len() > limits.max_voters {
                return Err(PolicyError::TooManyVoters);
            }
        }
        Tree::Majority(children) => {
            if children.is_empty() {
                return Err(PolicyError::EmptyBranch);
            }
            for child in children {
                validate(child, depth + 1, limits, voters, count)?;
            }
        }
        Tree::Weighted(children) => {
            if children.is_empty() {
                return Err(PolicyError::EmptyBranch);
            }
            let mut total = 0u64;
            for child in children {
                if child.weight == 0 {
                    return Err(PolicyError::ZeroWeight);
                }
                total = total
                    .checked_add(child.weight)
                    .ok_or(PolicyError::WeightOverflow)?;
                validate(&child.node, depth + 1, limits, voters, count)?;
            }
        }
    }
    Ok(())
}

fn satisfied(tree: &Tree, acks: &BTreeSet<NodeId>) -> bool {
    match tree {
        Tree::Voter(id) => acks.contains(id),
        Tree::Majority(children) => {
            children.iter().filter(|c| satisfied(c, acks)).count() > children.len() / 2
        }
        Tree::Weighted(children) => {
            // Construction proved these sums fit u64 and all weights are positive.
            let total: u64 = children.iter().map(|c| c.weight).sum();
            let yes: u64 = children
                .iter()
                .filter(|c| satisfied(&c.node, acks))
                .map(|c| c.weight)
                .sum();
            yes > total / 2
        }
    }
}

/// Branch values up to this width are gathered in a stack buffer; wider
/// branches use a `Vec`. Depth is bounded by `Limits::max_depth`.
const INLINE_CHILDREN: usize = 16;

fn with_buffer<T: Copy + Default, R>(len: usize, f: impl FnOnce(&mut [T]) -> R) -> R {
    if len <= INLINE_CHILDREN {
        let mut buffer = [T::default(); INLINE_CHILDREN];
        f(&mut buffer[..len])
    } else {
        f(&mut vec![T::default(); len])
    }
}

fn frontier(tree: &Tree, progress: &BTreeMap<NodeId, u64>) -> u64 {
    let value = frontier_buffered(tree, progress);
    #[cfg(debug_assertions)]
    {
        assert_eq!(value, reference_frontier(tree, progress), "quorum frontier");
        #[cfg(test)]
        tests::count_check();
    }
    value
}

fn frontier_buffered(tree: &Tree, progress: &BTreeMap<NodeId, u64>) -> u64 {
    match tree {
        Tree::Voter(id) => *progress.get(id).unwrap_or(&0),
        Tree::Majority(children) => with_buffer(children.len(), |values: &mut [u64]| {
            for (value, child) in values.iter_mut().zip(children) {
                *value = frontier_buffered(child, progress);
            }
            values.sort_unstable_by(|a, b| b.cmp(a));
            values[values.len() / 2]
        }),
        Tree::Weighted(children) => {
            let total: u64 = children.iter().map(|c| c.weight).sum();
            with_buffer(children.len(), |values: &mut [(u64, u64)]| {
                for (value, child) in values.iter_mut().zip(children) {
                    *value = (frontier_buffered(&child.node, progress), child.weight);
                }
                values.sort_unstable_by_key(|a| std::cmp::Reverse(a.0));
                let mut weight = 0u64;
                for (index, w) in values.iter() {
                    weight += w;
                    if weight > total / 2 {
                        return *index;
                    }
                }
                // A validated nonempty positive-weight branch always returns above.
                0
            })
        }
    }
}

/// The previous implementation, kept as the debug-build and test oracle.
#[cfg(any(test, debug_assertions))]
fn reference_frontier(tree: &Tree, progress: &BTreeMap<NodeId, u64>) -> u64 {
    match tree {
        Tree::Voter(id) => *progress.get(id).unwrap_or(&0),
        Tree::Majority(children) => {
            let mut values: Vec<u64> = children
                .iter()
                .map(|c| reference_frontier(c, progress))
                .collect();
            values.sort_unstable_by(|a, b| b.cmp(a));
            values[values.len() / 2]
        }
        Tree::Weighted(children) => {
            let total: u64 = children.iter().map(|c| c.weight).sum();
            let mut values: Vec<(u64, u64)> = children
                .iter()
                .map(|c| (reference_frontier(&c.node, progress), c.weight))
                .collect();
            values.sort_unstable_by_key(|a| std::cmp::Reverse(a.0));
            let mut weight = 0u64;
            for (index, w) in values {
                weight += w;
                if weight > total / 2 {
                    return index;
                }
            }
            0
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        static CHECKS: Cell<u64> = const { Cell::new(0) };
    }
    #[cfg(debug_assertions)]
    pub(super) fn count_check() {
        CHECKS.with(|c| c.set(c.get() + 1));
    }
    /// Frontier oracle comparisons made on this thread so far.
    pub(crate) fn checks() -> u64 {
        CHECKS.with(Cell::get)
    }
    /// A policy's frontier by the previous `Vec` implementation.
    pub(crate) fn reference(policy: &Policy, progress: &BTreeMap<NodeId, u64>) -> u64 {
        reference_frontier(policy.tree(), progress)
    }

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    fn tree(rng: &mut Rng, next: &mut u64, depth: u32) -> Tree {
        if depth == 0 || rng.below(3) == 0 {
            *next += 1;
            return Tree::Voter(NodeId::new(*next).unwrap());
        }
        // Root widths 1–40 cover both the stack buffer (≤ 16) and the `Vec`;
        // inner widths stay small so the policy fits the default limits.
        let wide = depth == 3 && rng.below(2) == 0;
        let width = 1 + rng.below(if wide { 40 } else { 5 }) as usize;
        let weighted = rng.below(2) == 0;
        let mut children = Vec::with_capacity(width);
        for _ in 0..width {
            let node = tree(rng, next, depth - 1);
            let weight = 1 + rng.below(5);
            children.push(WeightedChild { weight, node });
        }
        if weighted {
            Tree::Weighted(children)
        } else {
            Tree::Majority(children.into_iter().map(|c| c.node).collect())
        }
    }

    #[test]
    fn buffered_frontier_matches_the_vec_form_on_random_trees() {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        let (mut wide, mut narrow) = (0, 0);
        for _ in 0..400 {
            let mut voters = 0;
            let tree = tree(&mut rng, &mut voters, 3);
            let policy = Policy::new(tree.clone(), Limits::default()).unwrap();
            for _ in 0..8 {
                // Some voters absent (0), many ties.
                let mut progress = BTreeMap::new();
                for id in 1..=voters {
                    if rng.below(5) != 0 {
                        progress.insert(NodeId::new(id).unwrap(), rng.below(6));
                    }
                }
                assert_eq!(
                    frontier_buffered(&tree, &progress),
                    reference_frontier(&tree, &progress)
                );
                assert_eq!(
                    policy.frontier(&progress),
                    reference_frontier(&tree, &progress)
                );
            }
            let width = match &tree {
                Tree::Majority(c) => c.len(),
                Tree::Weighted(c) => c.len(),
                Tree::Voter(_) => 1,
            };
            if width > INLINE_CHILDREN {
                wide += 1;
            } else {
                narrow += 1;
            }
        }
        assert!(wide > 10 && narrow > 10, "wide {wide} narrow {narrow}");
    }
}
