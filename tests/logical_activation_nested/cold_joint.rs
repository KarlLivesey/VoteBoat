// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! All original physical stores restart from real committed Joint membership.
use super::harness::{self, SimClock};
use super::{
    cluster::{serial, Cluster},
    history,
    lower::Lower,
    progress, setup, Middle, Top, UpperBridge,
};
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::{ConfigurationId, StoreBinding},
    log::{EntryPayload, GroupLog},
    logical_voter::*,
    membership::ConfigurationChange,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
};
#[path = "cold_joint/restore.rs"]
mod restore;
struct Facts {
    middle: Vec<GroupLog>,
    bindings: Vec<StoreBinding>,
    top: GroupLog,
    top_binding: StoreBinding,
    begin: ActivationPosition,
}
#[test]
fn tcp_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final() {
    run(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final() {
    run(NativePeerProtocol::Quic);
}
fn run(protocol: NativePeerProtocol) {
    let _serial = serial();
    let clock = SimClock::default();
    let mut clusters = (0..4)
        .map(|slot| Cluster::new(protocol, slot, &clock))
        .collect::<Vec<_>>();
    let facts = original(&mut clusters, protocol, &clock);
    for cluster in &mut clusters {
        cluster.close();
    }
    eprintln!("367 all12 original physical workers joined and sockets rebound at Joint");
    for cluster in &mut clusters {
        cluster.open(NativeOpenMode::Recover);
    }
    restore::complete(&mut clusters, protocol, &clock, facts);
    for cluster in clusters {
        cluster.finish();
    }
    eprintln!("367 restored all12 original provider owners joined and sockets rebound");
}
fn original(clusters: &mut [Cluster], protocol: NativePeerProtocol, clock: &SimClock) -> Facts {
    let (source, target) = clusters.split_at_mut(3);
    let mut lower = Lower::new(source, clock);
    let images = (0..3)
        .map(|i| lower.claim(i, 367010 + i as u128))
        .collect::<Vec<_>>();
    let (mut middle, channels, mut descriptors) =
        super::assembly::originals(&lower, &images, protocol);
    progress::elect_middle(&mut lower, &mut middle);
    let mut bridge = UpperBridge::new(
        &mut middle,
        setup::upper_mapping(),
        super::support::node(201),
    )
    .unwrap();
    let mut top = history::initial_top(&mut lower, &mut bridge);
    let mut driver = history::begin_driver(&mut top, setup::intent());
    progress::phase(
        &mut lower,
        &mut bridge,
        &mut driver,
        3,
        ChildActivationDriverPhase::Enrolling,
    );
    let begin = driver.original_begin_position().unwrap();
    let export = super::enrollment::export(&mut lower, &mut bridge, &mut driver);
    let (continuation, _) = super::enrollment::detach(&mut lower, bridge, &mut driver);
    let grant = super::controls::grant(&mut driver, &mut middle, export);
    let target_channels = history::target_member(
        &mut lower,
        &mut middle,
        &mut descriptors,
        &mut target[0],
        grant,
        (protocol, false, false, false),
    );
    let mut bridge = driver
        .attach_enrolled_child(continuation, &mut middle)
        .unwrap_or_else(|e| panic!("original Joint attachment {:?}", e.reason));
    let returned = stop_joint(&mut lower, &mut bridge, driver);
    assert_eq!(
        returned.local().applications[&setup::top().group]
            .status(setup::intent().operations().begin)
            .unwrap()
            .begin
            .first,
        begin
    );
    super::shutdown::close(&mut lower, &mut bridge, returned, 4);
    let top_binding = returned.local().owner.identity().store;
    let top_state = returned
        .local()
        .owner
        .core(setup::top().group)
        .unwrap()
        .state()
        .clone();
    bridge.close();
    assert!(bridge.observations_are_drained());
    drop(bridge);
    drop(
        top.into_parts()
            .unwrap_or_else(|_| panic!("original parent closed")),
    );
    super::shutdown::middle(&mut lower, &mut middle);
    let facts = capture(&middle, top_state, top_binding, begin);
    history::reclaim_middle(
        &mut lower,
        middle,
        channels,
        target_channels,
        None,
        Vec::new(),
    );
    lower.close();
    facts
}
fn stop_joint<'p>(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    mut driver: ChildActivationDriver<'p, Top>,
) -> &'p mut Top {
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        progress::driver_step(lower, bridge, &mut driver, 4);
        let committed = bridge
            .inspect_backing(0, |p| {
                let core = p.owner.core(setup::middle().group).unwrap();
                no_final(core.state());
                core.membership().id() == ConfigurationId::new(3).unwrap()
                    && core.membership().joint().is_some_and(|joint| {
                        joint.index <= core.state().commit_index
                            && p.applications[&setup::middle().group].applied_index() >= joint.index
                    })
            })
            .unwrap();
        if committed {
            break;
        }
        assert!(
            end.within(&lower.clock),
            "actual committed Joint before Final admission"
        );
    }
    driver.stop_waiting_with_child(bridge).unwrap();
    while !driver.is_drained() {
        progress::driver_step(lower, bridge, &mut driver, 4);
        assert!(
            end.within(&lower.clock),
            "same original stopped Joint observations drain"
        );
    }
    driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("same original stopped parent"))
}
fn capture(
    nodes: &[Middle],
    top: GroupLog,
    top_binding: StoreBinding,
    begin: ActivationPosition,
) -> Facts {
    let states = nodes
        .iter()
        .map(|n| {
            let core = n.local().owner.core(setup::middle().group).unwrap();
            assert_eq!(core.membership().id(), ConfigurationId::new(3).unwrap());
            assert!(core.membership().joint().is_some());
            no_final(core.state());
            assert!(n.configuration_requests_are_drained());
            let app = &n.local().applications[&setup::middle().group];
            let LogicalRead::ConfigurationOperation(status) = app
                .read_at(
                    app.applied_index(),
                    LogicalQuery::ConfigurationOperation(setup::intent().operations().membership),
                )
                .unwrap()
            else {
                panic!("actual retained Joint history")
            };
            assert_eq!(
                status.entries.len(),
                1,
                "no committed/applied Final before Joint interruption"
            );
            assert!(status.entries[0].index <= core.state().commit_index);
            core.state().clone()
        })
        .collect();
    Facts {
        middle: states,
        bindings: nodes
            .iter()
            .map(|n| n.local().owner.identity().store)
            .collect(),
        top,
        top_binding,
        begin,
    }
}

fn no_final(state: &GroupLog) {
    assert!(state.entries.iter().all(|e| !matches!(&e.payload, EntryPayload::Configuration(record) if matches!(record.change, ConfigurationChange::Final { .. }))), "no retained accepted Final entry at this original Joint boundary");
}
