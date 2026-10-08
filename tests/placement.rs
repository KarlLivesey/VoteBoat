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
#![cfg(feature = "native")]
use std::collections::BTreeMap;
use voteboat::{identity::*, log::*, membership::*, native::placement::*, placement::*, quorum::*};
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn group(n: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn config(id: u64, tree: Tree, learners: &[u64]) -> Configuration {
    let policy = Policy::new(tree, Limits::default()).unwrap();
    let stores = policy
        .voters()
        .iter()
        .map(|n| (*n, store(n.get())))
        .collect();
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        policy,
        stores,
        learners.iter().map(|n| (node(*n), store(*n))).collect(),
    )
    .unwrap()
}
fn majority(voters: &[u64]) -> Tree {
    Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect())
}
fn membership(c: &Configuration) -> Membership {
    Membership::replay(
        &Bootstrap {
            group: group(1),
            configuration: c.id(),
            policy: c.policy().clone(),
            voter_stores: c.voter_stores().clone(),
        },
        &[],
        0,
    )
    .unwrap()
}
fn plan(domains: &[(u64, u64)], minimum: usize, survive: bool) -> NativePlacementAuthorizer {
    NativePlacementAuthorizer::new(
        group(1),
        assignments(domains),
        PlacementRequirements {
            minimum_voting_domains: minimum,
            survive_any_single_domain_loss: survive,
        },
    )
    .unwrap()
}
fn assignments(domains: &[(u64, u64)]) -> BTreeMap<NodeId, ReplicaPlacement> {
    domains
        .iter()
        .map(|(n, d)| {
            (
                node(*n),
                ReplicaPlacement {
                    store: store(*n),
                    domain: FailureDomainId::new(*d).unwrap(),
                },
            )
        })
        .collect()
}
fn change(c: Configuration) -> ConfigurationRecord {
    ConfigurationRecord {
        operation: OperationId::new(1).unwrap(),
        expected: ConfigurationId::new(1).unwrap(),
        change: ConfigurationChange::Joint {
            id: ConfigurationId::new(2).unwrap(),
            next: c,
        },
    }
}
#[test]
fn exact_group_incarnation_store_and_learner_assignment_are_required() {
    let base = config(1, majority(&[1, 2, 3]), &[]);
    let current = membership(&base);
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3), (4, 4)], 3, true);
    let record = change(config(3, majority(&[1, 2, 3]), &[4]));
    assert_eq!(authorizer.authorize(group(1), &current, &record), Ok(()));
    assert_eq!(
        authorizer.authorize(group(2), &current, &record),
        Err(PlacementError::WrongGroup)
    );
    let mut incarnation = group(1);
    incarnation.incarnation = GroupIncarnation::new(2).unwrap();
    assert_eq!(
        authorizer.authorize(incarnation, &current, &record),
        Err(PlacementError::WrongGroup)
    );
    let unknown = change(config(3, majority(&[1, 2, 3]), &[5]));
    assert_eq!(
        authorizer.authorize(group(1), &current, &unknown),
        Err(PlacementError::UnknownReplica(node(5)))
    );
    let mut bad = assignments(&[(1, 1), (2, 2), (3, 3), (4, 4)]);
    bad.get_mut(&node(4)).unwrap().store.incarnation = StoreIncarnation::new(2).unwrap();
    let bad = NativePlacementAuthorizer::new(group(1), bad, authorizer.requirements()).unwrap();
    assert_eq!(
        bad.authorize(group(1), &current, &record),
        Err(PlacementError::WrongStore(node(4)))
    );
}
#[test]
fn learners_never_supply_voting_domain_count_or_quorum_survival() {
    let base = config(1, majority(&[1]), &[]);
    let current = membership(&base);
    let record = change(config(3, majority(&[1]), &[2, 3]));
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3)], 3, false);
    assert_eq!(
        authorizer.authorize(group(1), &current, &record),
        Err(PlacementError::TooFewVotingDomains)
    );
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3)], 1, true);
    assert_eq!(
        authorizer.authorize(group(1), &current, &record),
        Err(PlacementError::DomainLossPreventsQuorum(
            FailureDomainId::new(1).unwrap()
        ))
    );
}
#[test]
fn domain_counts_do_not_substitute_for_weighted_or_recursive_quorum_evaluation() {
    let base = config(1, majority(&[1, 2, 3]), &[]);
    let current = membership(&base);
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3)], 3, true);
    let weighted = Tree::Weighted(vec![
        WeightedChild {
            weight: 10,
            node: Tree::Voter(node(1)),
        },
        WeightedChild {
            weight: 1,
            node: Tree::Voter(node(2)),
        },
        WeightedChild {
            weight: 1,
            node: Tree::Voter(node(3)),
        },
    ]);
    assert_eq!(
        authorizer.authorize(group(1), &current, &change(config(3, weighted, &[]))),
        Err(PlacementError::DomainLossPreventsQuorum(
            FailureDomainId::new(1).unwrap()
        ))
    );
    let nested = Tree::Majority(vec![Tree::Voter(node(1)), majority(&[2, 3])]);
    assert_eq!(
        authorizer.authorize(group(1), &current, &change(config(3, nested, &[]))),
        Err(PlacementError::DomainLossPreventsQuorum(
            FailureDomainId::new(1).unwrap()
        ))
    );
    let record = change(config(3, majority(&[1, 2, 3]), &[]));
    assert_eq!(authorizer.authorize(group(1), &current, &record), Ok(()));
    let balanced_weighted = Tree::Weighted(
        (1..=3)
            .map(|n| WeightedChild {
                weight: 2,
                node: Tree::Voter(node(n)),
            })
            .collect(),
    );
    assert_eq!(
        authorizer.authorize(
            group(1),
            &current,
            &change(config(3, balanced_weighted, &[]))
        ),
        Ok(())
    );
    let nested_surviving = Tree::Majority(vec![
        majority(&[1, 2, 3]),
        Tree::Voter(node(4)),
        Tree::Voter(node(5)),
    ]);
    let expanded = plan(&[(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)], 3, true);
    assert_eq!(
        expanded.authorize(
            group(1),
            &current,
            &change(config(3, nested_surviving, &[]))
        ),
        Ok(())
    );
    let colocated = plan(&[(1, 1), (2, 1), (3, 2)], 2, true);
    assert_eq!(
        colocated.authorize(group(1), &current, &record),
        Err(PlacementError::DomainLossPreventsQuorum(
            FailureDomainId::new(1).unwrap()
        ))
    );
}
#[test]
fn finalization_cannot_skip_the_accepted_joint_target_policy() {
    let base = config(1, majority(&[1, 2, 3]), &[]);
    let bootstrap = Bootstrap {
        group: group(1),
        configuration: base.id(),
        policy: base.policy().clone(),
        voter_stores: base.voter_stores().clone(),
    };
    let joint = change(config(3, majority(&[1, 2]), &[]));
    let current = Membership::replay(
        &bootstrap,
        &[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(joint)),
        }],
        1,
    )
    .unwrap();
    let final_record = ConfigurationRecord {
        operation: OperationId::new(1).unwrap(),
        expected: ConfigurationId::new(2).unwrap(),
        change: ConfigurationChange::Final {
            id: ConfigurationId::new(3).unwrap(),
        },
    };
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3)], 1, true);
    assert_eq!(
        authorizer.authorize(group(1), &current, &final_record),
        Err(PlacementError::DomainLossPreventsQuorum(
            FailureDomainId::new(1).unwrap()
        ))
    );
}
#[test]
fn placement_permission_does_not_waive_journal_grammar() {
    let base = config(1, majority(&[1, 2, 3]), &[]);
    let current = membership(&base);
    let authorizer = plan(&[(1, 1), (2, 2), (3, 3), (4, 4)], 3, true);
    let record = change(config(3, majority(&[1, 2, 4]), &[]));
    assert_eq!(authorizer.authorize(group(1), &current, &record), Ok(()));
    let bootstrap = Bootstrap {
        group: group(1),
        configuration: base.id(),
        policy: base.policy().clone(),
        voter_stores: base.voter_stores().clone(),
    };
    assert_eq!(
        Membership::replay(
            &bootstrap,
            &[LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(record)),
            }],
            0
        ),
        Err(MembershipError::UnpreparedVoter)
    );
}
#[test]
fn invalid_or_oversized_plans_return_original_assignments() {
    let requirements = PlacementRequirements {
        minimum_voting_domains: 1,
        survive_any_single_domain_loss: false,
    };
    for mut map in [
        BTreeMap::new(),
        assignments(&(1..=65).map(|n| (n, n)).collect::<Vec<_>>()),
        assignments(&(1..=4097).map(|n| (n, 1)).collect::<Vec<_>>()),
        assignments(&[(1, 1), (2, 2)]),
    ] {
        if map.len() == 2 {
            map.get_mut(&node(2)).unwrap().store = store(1);
        }
        let expected = map.clone();
        let (error, returned) =
            NativePlacementAuthorizer::new(group(1), map, requirements).unwrap_err();
        assert_eq!(error, PlacementError::InvalidPlan);
        assert_eq!(returned, expected);
    }
    assert!(FailureDomainId::new(0).is_none());
}
