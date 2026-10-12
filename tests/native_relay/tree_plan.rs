// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Finite routing permissions refuse impossible assemblies before any native IO.
use super::support::*;
use std::collections::{BTreeMap, BTreeSet};
use voteboat::native::relay::*;
#[test]
fn checked_tree_rejects_cycles_disconnection_unprovisioned_leaves_and_excess_depth() {
    let sources = BTreeMap::from([(node(1), identity(1)), (node(2), identity(2))]);
    let helpers = (9..=12)
        .map(|n| (node(n), identity(u128::from(n))))
        .collect::<BTreeMap<_, _>>();
    let attachment = BTreeMap::from([(node(1), node(9)), (node(2), node(12))]);
    let disconnected = BTreeSet::from([
        (node(9), node(10)),
        (node(9), node(11)),
        (node(10), node(11)),
    ]);
    assert!(matches!(
        RelayTreePlan::new(
            sources.clone(),
            helpers.clone(),
            disconnected,
            attachment.clone(),
            8,
            4
        ),
        Err(RelayNetworkError::InvalidLimits)
    ));
    let route = BTreeSet::from([
        (node(9), node(10)),
        (node(10), node(11)),
        (node(11), node(12)),
    ]);
    assert!(RelayTreePlan::new(
        sources.clone(),
        helpers.clone(),
        route.clone(),
        attachment.clone(),
        8,
        4
    )
    .is_ok());
    assert!(matches!(
        RelayTreePlan::new(
            sources.clone(),
            helpers.clone(),
            route.clone(),
            attachment.clone(),
            8,
            3
        ),
        Err(RelayNetworkError::InvalidLimits)
    ));
    let mut missing = attachment;
    missing.insert(node(2), node(13));
    assert!(matches!(
        RelayTreePlan::new(sources, helpers, route, missing, 8, 4),
        Err(RelayNetworkError::WrongBinding)
    ));
}
