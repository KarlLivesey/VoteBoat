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
use super::*;
#[test]
fn every_nine_voter_explanation_matches_reference_and_direct_branch_weights() {
    let policy = sites();
    for mask in 0u16..512 {
        let voters = (0..9)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| n(i + 1))
            .collect();
        let report = policy.explain(&voters);
        assert_eq!(report.is_satisfied(), policy.is_satisfied(&voters));
        assert_eq!(report.nodes.len(), 13);
        assert_eq!(report.nodes[0].parent, None);
        for (index, row) in report.nodes.iter().enumerate() {
            if let Some(parent) = row.parent {
                assert!(parent < index);
            }
            match row.rule {
                QuorumRule::Voter(id) => {
                    assert_eq!(row.observed_weight, u64::from(voters.contains(&id)))
                }
                QuorumRule::Majority | QuorumRule::Weighted => {
                    let children = report
                        .nodes
                        .iter()
                        .filter(|r| r.parent == Some(index))
                        .collect::<Vec<_>>();
                    assert_eq!(
                        row.observed_weight,
                        children
                            .iter()
                            .filter(|r| r.satisfied)
                            .map(|r| r.weight)
                            .sum()
                    );
                    assert_eq!(row.total_weight, children.iter().map(|r| r.weight).sum());
                }
            }
            assert_eq!(row.required_weight, row.total_weight / 2 + 1);
            assert_eq!(row.satisfied, row.observed_weight >= row.required_weight);
        }
    }
}
#[test]
fn five_of_nine_ids_do_not_hide_two_unsatisfied_sites() {
    let report = sites().explain(&set(&[1, 2, 3, 4, 7, 99]));
    assert!(!report.is_satisfied());
    assert_eq!(report.unknown_voters, 1);
    assert_eq!(report.nodes[0].observed_weight, 1);
    assert_eq!(report.nodes[0].required_weight, 2);
    for index in [5, 9] {
        assert_eq!(report.nodes[index].observed_weight, 1);
        assert_eq!(report.nodes[index].required_weight, 2);
        assert!(!report.nodes[index].satisfied);
    }
}
#[test]
fn maximum_weights_and_joint_policies_preserve_separate_thresholds() {
    let policy = Policy::new(
        Tree::Weighted(vec![
            WeightedChild {
                weight: u64::MAX - 2,
                node: Tree::Voter(n(1)),
            },
            WeightedChild {
                weight: 2,
                node: Tree::Voter(n(2)),
            },
        ]),
        Limits::default(),
    )
    .unwrap();
    let report = policy.explain(&set(&[1]));
    assert_eq!(report.nodes[0].total_weight, u64::MAX);
    assert_eq!(report.nodes[0].required_weight, (1u64 << 63));
    assert_eq!(report.nodes[1].weight, u64::MAX - 2);
    assert!(report.is_satisfied());
    assert!(!policy.explain(&set(&[2])).is_satisfied());
    let joint = JointPolicy {
        old: Policy::new(majority(&[1, 2, 3]), Limits::default()).unwrap(),
        new: Policy::new(majority(&[3, 4, 5]), Limits::default()).unwrap(),
    };
    let report = joint.explain(&set(&[1, 2]));
    assert!(report.old.is_satisfied());
    assert!(!report.new.is_satisfied());
    assert!(!report.is_satisfied());
    assert!(joint.explain(&set(&[1, 3, 4])).is_satisfied());
}
