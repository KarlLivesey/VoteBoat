// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::quorum::Tree;
use ring::digest::{Context, SHA256};
pub(super) fn plan(
    owner: PeerIdentity,
    operation: OperationId,
    groups: &[DrainMembershipGroup],
) -> DrainPlanDigest {
    let mut hash = Context::new(&SHA256);
    hash.update(b"VoteBoat-membership-drain-plan-v1");
    peer(&mut hash, owner);
    hash.update(&operation.get().to_be_bytes());
    hash.update(&(groups.len() as u64).to_be_bytes());
    for group in groups {
        hash.update(&group.group.id.get().to_be_bytes());
        hash.update(&group.group.incarnation.get().to_be_bytes());
        peer(&mut hash, group.handoff);
        configuration(&mut hash, &group.original);
        hash.update(&group.change.joint.operation.get().to_be_bytes());
        hash.update(&group.change.joint.expected.get().to_be_bytes());
        let ConfigurationChange::Joint { id, next } = &group.change.joint.change else {
            unreachable!()
        };
        hash.update(&id.get().to_be_bytes());
        configuration(&mut hash, next);
        hash.update(&group.change.finalize.operation.get().to_be_bytes());
        hash.update(&group.change.finalize.expected.get().to_be_bytes());
    }
    DrainPlanDigest(hash.finish().as_ref().try_into().unwrap())
}
fn peer(hash: &mut Context, peer: PeerIdentity) {
    hash.update(&peer.node.get().to_be_bytes());
    hash.update(&peer.store.id.get().to_be_bytes());
    hash.update(&peer.store.incarnation.get().to_be_bytes());
}
fn configuration(hash: &mut Context, configuration: &Configuration) {
    hash.update(&configuration.id().get().to_be_bytes());
    tree(hash, configuration.policy().tree());
    for stores in [configuration.voter_stores(), configuration.learners()] {
        hash.update(&(stores.len() as u64).to_be_bytes());
        for (&node, &store) in stores {
            peer(hash, PeerIdentity { node, store });
        }
    }
}
fn tree(hash: &mut Context, node: &Tree) {
    match node {
        Tree::Voter(node) => {
            hash.update(&[0]);
            hash.update(&node.get().to_be_bytes());
        }
        Tree::Majority(children) => {
            hash.update(&[1]);
            hash.update(&(children.len() as u64).to_be_bytes());
            for child in children {
                tree(hash, child);
            }
        }
        Tree::Weighted(children) => {
            hash.update(&[2]);
            hash.update(&(children.len() as u64).to_be_bytes());
            for child in children {
                hash.update(&child.weight.to_be_bytes());
                tree(hash, &child.node);
            }
        }
    }
}
