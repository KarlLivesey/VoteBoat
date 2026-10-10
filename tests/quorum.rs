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
use std::collections::{BTreeMap, BTreeSet};
use voteboat::{identity::NodeId, quorum::*};
#[path = "quorum/explanation.rs"]
mod explanation;
fn n(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn majority(ids: &[u64]) -> Tree {
    Tree::Majority(ids.iter().map(|&id| Tree::Voter(n(id))).collect())
}
fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().map(|&id| n(id)).collect()
}
fn sites() -> Policy {
    Policy::for_voters(
        Tree::Majority(vec![
            majority(&[1, 2, 3]),
            majority(&[4, 5, 6]),
            majority(&[7, 8, 9]),
        ]),
        Limits::default(),
        &(1..=9).map(n).collect(),
    )
    .unwrap()
}
#[test]
fn every_nine_voter_quorum_intersects_and_frontier_matches_reference() {
    let policy = sites();
    let mut quorums = Vec::new();
    for mask in 0u16..512 {
        let acks = (0..9)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| n(i + 1))
            .collect();
        if policy.is_satisfied(&acks) {
            quorums.push(mask);
        }
    }
    assert_eq!(quorums.len(), 256);
    for a in &quorums {
        for b in &quorums {
            assert_ne!(a & b, 0);
        }
    }
    assert!(policy.is_satisfied(&set(&[1, 2, 4, 5])));
    assert!(!policy.is_satisfied(&set(&[1, 2, 3, 4, 7])));
    // Differential frontier calculation for many deterministic progress maps.
    for seed in 0..512u64 {
        let progress: BTreeMap<_, _> = (1..=9)
            .map(|id| (n(id), (seed * id * 17 + id * id) % 23))
            .collect();
        let reference = (0..=23)
            .filter(|index| {
                let acks = progress
                    .iter()
                    .filter(|(_, prefix)| *prefix >= index)
                    .map(|(n, _)| *n)
                    .collect();
                policy.is_satisfied(&acks)
            })
            .max()
            .unwrap();
        assert_eq!(policy.frontier(&progress), reference);
    }
}
#[test]
fn validates_exact_membership_weights_depth_and_budgets() {
    assert_eq!(NodeId::new(0), None);
    assert!(matches!(
        Policy::for_voters(majority(&[1, 2]), Limits::default(), &set(&[1, 2, 3])),
        Err(PolicyError::VoterSetMismatch)
    ));
    assert!(matches!(
        Policy::new(majority(&[1, 1]), Limits::default()),
        Err(PolicyError::DuplicateVoter(_))
    ));
    assert!(matches!(
        Policy::new(majority(&[]), Limits::default()),
        Err(PolicyError::EmptyBranch)
    ));
    for (weights, expected) in [
        ([0, 1], PolicyError::ZeroWeight),
        ([u64::MAX, 1], PolicyError::WeightOverflow),
    ] {
        let tree = Tree::Weighted(vec![
            WeightedChild {
                weight: weights[0],
                node: Tree::Voter(n(1)),
            },
            WeightedChild {
                weight: weights[1],
                node: Tree::Voter(n(2)),
            },
        ]);
        assert!(matches!(Policy::new(tree, Limits::default()), Err(e) if e == expected));
    }
    assert!(matches!(
        Policy::new(
            majority(&[1, 2]),
            Limits {
                max_voters: 1,
                ..Limits::default()
            }
        ),
        Err(PolicyError::TooManyVoters)
    ));
    assert!(matches!(
        Policy::new(
            majority(&[1]),
            Limits {
                max_tree_nodes: 1,
                ..Limits::default()
            }
        ),
        Err(PolicyError::TooManyTreeNodes)
    ));
    assert!(matches!(
        Policy::new(
            Tree::Majority(vec![majority(&[1])]),
            Limits {
                max_depth: 1,
                ..Limits::default()
            }
        ),
        Err(PolicyError::TooDeep)
    ));
}
#[test]
fn weighted_and_joint_predicates_are_strict() {
    let weighted = Policy::new(
        Tree::Weighted(vec![
            WeightedChild {
                weight: 2,
                node: Tree::Voter(n(1)),
            },
            WeightedChild {
                weight: 1,
                node: Tree::Voter(n(2)),
            },
            WeightedChild {
                weight: 1,
                node: Tree::Voter(n(3)),
            },
        ]),
        Limits::default(),
    )
    .unwrap();
    assert!(!weighted.is_satisfied(&set(&[1])));
    assert!(!weighted.is_satisfied(&set(&[2, 3])));
    assert!(weighted.is_satisfied(&set(&[1, 2])));
    let joint = JointPolicy {
        old: weighted,
        new: Policy::new(majority(&[4, 5, 6]), Limits::default()).unwrap(),
    };
    assert!(!joint.is_satisfied(&set(&[1, 2])));
    assert!(joint.is_satisfied(&set(&[1, 2, 4, 5])));
}
