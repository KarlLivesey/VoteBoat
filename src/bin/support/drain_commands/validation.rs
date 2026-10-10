// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn configuration(record: &DrainRecord) -> Result<ConfigurationId, String> {
    match record.request.groups.as_slice() {
        [entry] if entry.group == group() => Ok(entry.configuration),
        _ => Err("drain journal does not match this single-group service profile".into()),
    }
}
pub(super) fn handoff(service: &Service, operation: OperationId) -> Option<LeadershipRecord> {
    leadership_commands::application(service)
        .ok()?
        .record(operation)
}
pub(super) fn retained_configuration(
    service: &Service,
    expected: ConfigurationId,
) -> Result<(), String> {
    let core = service.local().owner.core(group()).ok_or("missing group")?;
    let membership = core.membership();
    if membership.joint().is_some()
        || membership.stable().id() != expected
        || membership.last_configuration_index() > core.state().commit_index
    {
        return Err("drain requires the original stable configuration".into());
    }
    if !retained_policy(membership.stable(), core.local_node()) {
        return Err(
            "membership_change_required: remaining configured voters cannot satisfy policy".into(),
        );
    }
    Ok(())
}
fn retained_policy(configuration: &voteboat::membership::Configuration, local: NodeId) -> bool {
    let remaining = configuration
        .voter_stores()
        .keys()
        .copied()
        .filter(|node| *node != local)
        .collect();
    configuration.policy().is_satisfied(&remaining)
}
pub(super) fn intent(service: &Service, words: &[&str]) -> Result<LeadershipIntent, String> {
    let ["drain-node", _, op, config, target, store, incarnation] = words else {
        return Err("expected drain-node SEQUENCE OP CONFIG TARGET STORE INC".into());
    };
    let operation = op
        .parse()
        .ok()
        .and_then(OperationId::new)
        .ok_or("invalid operation")?;
    let configuration = config
        .parse()
        .ok()
        .and_then(ConfigurationId::new)
        .ok_or("invalid configuration")?;
    retained_configuration(service, configuration)?;
    let core = service.local().owner.core(group()).ok_or("missing group")?;
    let source = PeerIdentity {
        node: core.local_node(),
        store: core.storage_binding().identity,
    };
    let target = PeerIdentity {
        node: target
            .parse()
            .ok()
            .and_then(NodeId::new)
            .ok_or("invalid target")?,
        store: StoreIdentity {
            id: store
                .parse()
                .ok()
                .and_then(StoreId::new)
                .ok_or("invalid store")?,
            incarnation: incarnation
                .parse()
                .ok()
                .and_then(StoreIncarnation::new)
                .ok_or("invalid incarnation")?,
        },
    };
    if target.node == source.node
        || core.membership().stable().voter_stores().get(&target.node) != Some(&target.store)
    {
        return Err("drain requires a distinct exact configured voter target".into());
    }
    Ok(LeadershipIntent {
        source,
        request: LeadershipTransferRequest {
            operation,
            configuration,
            target,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use voteboat::{
        membership::Configuration,
        quorum::{Limits, Policy, Tree, WeightedChild},
    };
    fn configuration(tree: Tree) -> Configuration {
        let policy = Policy::new(tree, Limits::default()).unwrap();
        let stores = policy
            .voters()
            .iter()
            .map(|node| {
                (
                    *node,
                    StoreIdentity {
                        id: StoreId::new(u128::from(node.get())).unwrap(),
                        incarnation: StoreIncarnation::new(1).unwrap(),
                    },
                )
            })
            .collect();
        Configuration::new(
            ConfigurationId::new(1).unwrap(),
            policy,
            stores,
            Default::default(),
        )
        .unwrap()
    }
    #[test]
    fn retained_capacity_uses_recursive_policy_not_voter_count() {
        let voter = |id| Tree::Voter(NodeId::new(id).unwrap());
        let local = NodeId::new(1).unwrap();
        let ordinary = configuration(Tree::Majority(vec![voter(1), voter(2), voter(3)]));
        assert!(retained_policy(&ordinary, local));
        let recursive = configuration(Tree::Majority(vec![
            Tree::Majority(vec![voter(1)]),
            Tree::Majority(vec![voter(2), voter(3)]),
        ]));
        assert!(!retained_policy(&recursive, local));
        let weighted = configuration(Tree::Weighted(vec![
            WeightedChild {
                weight: 3,
                node: voter(1),
            },
            WeightedChild {
                weight: 1,
                node: voter(2),
            },
            WeightedChild {
                weight: 1,
                node: voter(3),
            },
        ]));
        assert!(!retained_policy(&weighted, local));
        assert!(retained_policy(&weighted, NodeId::new(2).unwrap()));
    }
}
