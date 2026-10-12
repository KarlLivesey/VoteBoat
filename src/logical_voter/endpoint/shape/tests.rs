// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Synthetic topology guards only. No Claim, ReadBarrier, chain or authority minted.
use super::*;
fn group(id: u64) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(u128::from(id)).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn leaf() -> LocalIdentity {
    LocalIdentity {
        node: node(999),
        store: StoreBinding {
            identity: store(999),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn mappings(n: usize) -> Vec<LogicalStoreMapping> {
    (0..n)
        .map(|i| {
            LogicalStoreMapping::new(
                group(100 + i as u64),
                node(1000 + i as u64),
                store(1000 + i as u128),
                group(101 + i as u64),
            )
            .unwrap()
        })
        .collect()
}
#[test]
fn synthetic_shape_guard_accepts_declared_eight_and_refuses_nine_before_mint() {
    let valid = mappings(8);
    assert_eq!(validate(&valid, group(108), leaf()), Ok(()));
    assert_eq!(
        validate(&mappings(9), group(109), leaf()),
        Err(LogicalEndpointError::TooDeep)
    );
}
#[test]
fn synthetic_shape_guard_refuses_cycles_and_wrong_incarnation_edges() {
    let mut cycle = mappings(3);
    cycle[2].child = cycle[0].parent;
    assert_eq!(
        validate(&cycle, group(100), leaf()),
        Err(LogicalEndpointError::Cycle)
    );
    let mut same = mappings(1);
    same[0].parent = same[0].child;
    assert_eq!(
        validate(&same, group(101), leaf()),
        Err(LogicalEndpointError::Cycle)
    );
    let mut stale = mappings(2);
    stale[0].child.incarnation = GroupIncarnation::new(2).unwrap();
    assert_eq!(
        validate(&stale, group(102), leaf()),
        Err(LogicalEndpointError::WrongBinding)
    );
}
#[test]
fn synthetic_shape_guard_refuses_reused_voter_store_and_physical_leaf() {
    let mut voters = mappings(2);
    voters[1].voter = voters[0].voter;
    assert_eq!(
        validate(&voters, group(102), leaf()),
        Err(LogicalEndpointError::DuplicateIdentity)
    );
    let mut stores = mappings(2);
    stores[1].store = stores[0].store;
    stores[1].store.incarnation = StoreIncarnation::new(2).unwrap();
    assert_eq!(
        validate(&stores, group(102), leaf()),
        Err(LogicalEndpointError::DuplicateIdentity)
    );
    let mut physical = mappings(1);
    physical[0].voter = leaf().node;
    assert_eq!(
        validate(&physical, group(101), leaf()),
        Err(LogicalEndpointError::DuplicateIdentity)
    );
    physical[0].voter = node(1000);
    physical[0].store = leaf().store.identity;
    assert_eq!(
        validate(&physical, group(101), leaf()),
        Err(LogicalEndpointError::DuplicateIdentity)
    );
}
