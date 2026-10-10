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
//! Bounded diagnostic views of validated policies; never an acknowledgement proof.
use super::{JointPolicy, Policy, Tree};
use crate::identity::NodeId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuorumRule {
    Voter(NodeId),
    Majority,
    Weighted,
}
/// One preorder row. Parent indexes refer to this explanation's node vector.
/// Weight is this node's contribution to its parent, if this node is satisfied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuorumNode {
    pub parent: Option<usize>,
    pub weight: u64,
    pub rule: QuorumRule,
    pub observed_weight: u64,
    pub required_weight: u64,
    pub total_weight: u64,
    pub satisfied: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuorumExplanation {
    pub nodes: Vec<QuorumNode>,
    pub unknown_voters: usize,
}
impl QuorumExplanation {
    pub fn is_satisfied(&self) -> bool {
        self.nodes.first().is_some_and(|n| n.satisfied)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JointExplanation {
    pub old: QuorumExplanation,
    pub new: QuorumExplanation,
}
impl JointExplanation {
    pub fn is_satisfied(&self) -> bool {
        self.old.is_satisfied() && self.new.is_satisfied()
    }
}
impl Policy {
    /// Explain a hypothetical set of voter IDs using the validated tree.
    /// Unknown IDs never contribute. No identity, term, request, durability or
    /// liveness evidence is established by this diagnostic method. Work/output
    /// scale with the bounded policy and supplied set; there is one row per node.
    pub fn explain(&self, voters: &BTreeSet<NodeId>) -> QuorumExplanation {
        let mut nodes = Vec::new();
        visit(self.tree(), voters, None, 1, &mut nodes);
        QuorumExplanation {
            nodes,
            unknown_voters: voters.difference(self.voters()).count(),
        }
    }
}
impl JointPolicy {
    pub fn explain(&self, voters: &BTreeSet<NodeId>) -> JointExplanation {
        JointExplanation {
            old: self.old.explain(voters),
            new: self.new.explain(voters),
        }
    }
}
fn visit(
    tree: &Tree,
    voters: &BTreeSet<NodeId>,
    parent: Option<usize>,
    weight: u64,
    nodes: &mut Vec<QuorumNode>,
) -> bool {
    let index = nodes.len();
    let rule = match tree {
        Tree::Voter(id) => QuorumRule::Voter(*id),
        Tree::Majority(_) => QuorumRule::Majority,
        Tree::Weighted(_) => QuorumRule::Weighted,
    };
    nodes.push(QuorumNode {
        parent,
        weight,
        rule,
        observed_weight: 0,
        required_weight: 1,
        total_weight: 1,
        satisfied: false,
    });
    let (observed, total) = match tree {
        Tree::Voter(id) => (u64::from(voters.contains(id)), 1),
        Tree::Majority(children) => {
            let observed = children
                .iter()
                .filter(|child| visit(child, voters, Some(index), 1, nodes))
                .count();
            (observed as u64, children.len() as u64)
        }
        Tree::Weighted(children) => {
            let mut observed = 0;
            let mut total = 0;
            for child in children {
                total += child.weight;
                if visit(&child.node, voters, Some(index), child.weight, nodes) {
                    observed += child.weight;
                }
            }
            (observed, total)
        }
    };
    let node = &mut nodes[index];
    node.observed_weight = observed;
    node.total_weight = total;
    node.required_weight = total / 2 + 1;
    node.satisfied = observed >= node.required_weight;
    node.satisfied
}
