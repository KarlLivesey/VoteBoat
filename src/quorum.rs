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

#[derive(Clone, Debug)]
pub enum Tree {
    Voter(NodeId),
    Majority(Vec<Tree>),
    Weighted(Vec<WeightedChild>),
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

fn frontier(tree: &Tree, progress: &BTreeMap<NodeId, u64>) -> u64 {
    match tree {
        Tree::Voter(id) => *progress.get(id).unwrap_or(&0),
        Tree::Majority(children) => {
            let mut values: Vec<u64> = children.iter().map(|c| frontier(c, progress)).collect();
            values.sort_unstable_by(|a, b| b.cmp(a));
            values[values.len() / 2]
        }
        Tree::Weighted(children) => {
            let total: u64 = children.iter().map(|c| c.weight).sum();
            let mut values: Vec<(u64, u64)> = children
                .iter()
                .map(|c| (frontier(&c.node, progress), c.weight))
                .collect();
            values.sort_unstable_by_key(|a| std::cmp::Reverse(a.0));
            let mut weight = 0u64;
            for (index, w) in values {
                weight += w;
                if weight > total / 2 {
                    return index;
                }
            }
            // A validated nonempty positive-weight branch always returns above.
            0
        }
    }
}
