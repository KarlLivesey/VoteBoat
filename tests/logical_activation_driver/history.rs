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
use std::collections::BTreeMap;
use voteboat::{
    application::StateMachine,
    membership::Configuration,
    quorum::{Limits, Policy, Tree},
};
#[test]
fn original_parent_begin_fresh_status_retry_busy_and_cancel_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn original_parent_begin_fresh_status_retry_busy_and_cancel_quic() {
    history(NativePeerProtocol::Quic);
}
pub(super) fn intent(base: u128) -> ChildActivationIntent {
    let c = |nodes: [u64; 3], id| {
        Configuration::new(
            ConfigurationId::new(id).unwrap(),
            Policy::new(
                Tree::Majority(nodes.map(|n| Tree::Voter(support::node(n))).to_vec()),
                Limits::default(),
            )
            .unwrap(),
            nodes
                .into_iter()
                .map(|n| (support::node(n), support::identity(n.into())))
                .collect(),
            BTreeMap::new(),
        )
        .unwrap()
    };
    let op = |offset| OperationId::new(base + offset).unwrap();
    ChildActivationIntent::new(
        setup::mapping(),
        ChildActivationOperations {
            begin: op(0),
            learners: op(1),
            membership: op(2),
            outcome: op(3),
            release: op(4),
        },
        &c([1, 2, 3], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &c([1, 2, 4], 4),
    )
    .unwrap()
}
fn driver(
    node: &mut fixture::FacadeNode,
    i: ChildActivationIntent,
) -> ChildActivationDriver<'_, fixture::FacadeNode> {
    ChildActivationDriver::new(node, i, ChildActivationDriverLimits::default()).unwrap_or_else(
        |e| {
            panic!(
                "actual parent driver {:?}, op {:?}, running {:?}, observers {}",
                e.reason,
                e.intent.operations().begin,
                e.parent.state(),
                voteboat::logical_voter::LogicalActivationParent::observers_are_drained(e.parent)
            )
        },
    )
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut cohort = cluster::Cluster::simulated(protocol, fixture::child_application());
    let clock = &cohort.clock.clone();
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 8000, false);
    let mut parent = fixture::FacadeNode::from_parts(
        &image,
        fixture::build(&image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual parent {:?}", e.reason));
    parent.campaign().unwrap();
    progress::until(&mut bridge, &mut parent, clock, |n| {
        let c = n.local().owner.core(setup::mapping().parent).unwrap();
        c.role() == Role::Leader
            && c.state().commit_index > 0
            && n.local().applications[&setup::mapping().parent].applied_index()
                >= c.state().commit_index
            && n.local().clients.is_drained()
            && n.local().reads.is_drained()
            && n.local().results.is_drained()
    });
    let original = intent(100);
    let mut d = driver(&mut parent, original.clone());
    for _ in 0..16 {
        progress::advance(&mut d, &mut bridge, clock, 0);
        assert_eq!(d.phase(), ChildActivationDriverPhase::Observe);
        assert!(d.original_begin_position().is_none());
    }
    progress::phase(
        &mut d,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::AwaitBegin,
    );
    for _ in 0..16 {
        progress::advance(&mut d, &mut bridge, clock, 0);
        assert_eq!(d.phase(), ChildActivationDriverPhase::AwaitBegin);
        assert!(d.original_begin_position().is_none());
    }
    // Cancellation gives up this observer only. The admitted immutable Begin
    // keeps its actual backing dependencies, then fresh read resolves its truth.
    let node = progress::stop(d, &mut bridge, clock);
    let mut resumed = driver(node, original.clone());
    progress::phase(
        &mut resumed,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::Begun,
    );
    let first = resumed.original_begin_position().unwrap();
    let node = progress::stop(resumed, &mut bridge, clock);
    let status = node.local().applications[&setup::mapping().parent]
        .status(original.operations().begin)
        .unwrap();
    assert_eq!(status.intent, original);
    assert_eq!(status.begin.first, first);
    let mut retry = driver(node, original.clone());
    progress::phase(
        &mut retry,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::Begun,
    );
    assert_eq!(retry.original_begin_position(), Some(first));
    let node = progress::stop(retry, &mut bridge, clock);
    let mut competitor = driver(node, intent(200));
    progress::phase(
        &mut competitor,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::Busy,
    );
    assert!(competitor
        .original_begin_position()
        .is_some_and(|p| p.index > first.index));
    let node = progress::stop(competitor, &mut bridge, clock);
    assert_eq!(
        node.local().applications[&setup::mapping().parent].active(),
        Some(original.operations().begin)
    );
    node.close_intake().unwrap();
    progress::drain(&mut bridge, node, clock);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    let parts = parent
        .into_parts()
        .unwrap_or_else(|_| panic!("original facade drained"));
    assert!(parts.persistence.is_drained());
    drop(parts);
    cohort.finish();
}
