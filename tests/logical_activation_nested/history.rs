// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    business,
    cluster::{serial, Cluster},
    enrollment,
    lower::Lower,
    parts, progress, setup, Top,
};
use std::time::Instant;
use voteboat::{application::StateMachine, logical_voter::*, native::connect::NativePeerProtocol};
#[test]
fn tcp_twelve_native_owners_original_logical_learner_activation() {
    history(NativePeerProtocol::TcpTls, false, false, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_twelve_native_owners_original_logical_learner_activation() {
    history(NativePeerProtocol::Quic, false, false, false, false);
}
#[test]
fn tcp_fifteen_native_owners_two_original_logical_replacements() {
    history(NativePeerProtocol::TcpTls, true, false, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_fifteen_native_owners_two_original_logical_replacements() {
    history(NativePeerProtocol::Quic, true, false, false, false);
}
#[test]
fn tcp_original_import_lost_image_target_stores_cold_claim_then_activation() {
    history(NativePeerProtocol::TcpTls, false, true, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_import_lost_image_target_stores_cold_claim_then_activation() {
    history(NativePeerProtocol::Quic, false, true, false, false);
}
#[test]
fn tcp_original_unknown_import_target_cold_resolves_then_activation() {
    history(NativePeerProtocol::TcpTls, false, true, true, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_unknown_import_target_cold_resolves_then_activation() {
    history(NativePeerProtocol::Quic, false, true, true, false);
}
#[test]
fn tcp_original_uncommitted_unknown_import_wal_cold_then_real_activation() {
    history(NativePeerProtocol::TcpTls, false, true, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_uncommitted_unknown_import_wal_cold_then_real_activation() {
    history(NativePeerProtocol::Quic, false, true, true, true);
}
fn history(
    protocol: NativePeerProtocol,
    repeat: bool,
    cold: bool,
    unknown: bool,
    uncommitted: bool,
) {
    let _serial = serial();
    let clock = Instant::now();
    eprintln!("356 original_nine_bootstraps");
    let mut clusters = (0..3)
        .map(|slot| Cluster::new(protocol, slot, clock))
        .collect::<Vec<_>>();
    let mut lower = Lower::new(&mut clusters, clock);
    let images = (0..3)
        .map(|slot| lower.claim(slot, 356010 + slot as u128))
        .collect::<Vec<_>>();
    let (mut middle, original_channels, mut descriptors) =
        super::assembly::originals(&lower, &images, protocol);
    progress::elect_middle(&mut lower, &mut middle);
    super::readiness::cancellation(&mut lower, &mut middle);
    let mut bridge = LogicalBackingBridge::new(
        &mut middle,
        setup::upper_mapping(),
        super::support::node(201),
    )
    .unwrap();
    let mut top = initial_top(&mut lower, &mut bridge);
    let intent = setup::intent();
    let mut driver = begin_driver(&mut top, intent.clone());
    progress::phase(
        &mut lower,
        &mut bridge,
        &mut driver,
        3,
        ChildActivationDriverPhase::Enrolling,
    );
    let begin = driver.original_begin_position().unwrap();
    eprintln!("356 original_Learners_source_export");
    let output = enrollment::export(&mut lower, &mut bridge, &mut driver);
    let (continuation, _) = enrollment::detach(&mut lower, bridge, &mut driver);
    let grant = super::controls::grant(&mut driver, &mut middle, output);
    assert!(middle[0].poll_checkpoint_export().is_none());
    let mut target_cluster = Cluster::new(protocol, 3, clock);
    let target_channels = target_member(
        &mut lower,
        &mut middle,
        &mut descriptors,
        &mut target_cluster,
        grant,
        (protocol, cold, unknown, uncommitted),
    );
    let mut bridge = driver
        .attach_enrolled_child(continuation, &mut middle)
        .unwrap_or_else(|e| panic!("same original children {:?}", e.reason));
    eprintln!("356 original_readiness_Joint_Final_Outcome_Release");
    progress::phase(
        &mut lower,
        &mut bridge,
        &mut driver,
        4,
        ChildActivationDriverPhase::Released,
    );
    let returned = finish_first(&mut lower, &mut bridge, driver, begin, &intent);
    let mut second_cluster = None;
    let mut second_channels = None;
    let count = if repeat {
        let (second_driver, continuation, output) =
            super::repeated::prepare(&mut lower, bridge, returned);
        let cluster = second_cluster.insert(Cluster::new(protocol, 4, clock));
        add_lower(&mut lower, cluster);
        let (next_bridge, channels) = super::repeated::finish(
            &mut lower,
            &mut middle,
            second_driver,
            continuation,
            output,
            &descriptors,
            protocol,
        );
        bridge = next_bridge;
        second_channels = Some(channels);
        business::request(&mut lower, &mut bridge, returned, 5, 356302, 4, 16);
        5
    } else {
        4
    };
    super::shutdown::close(&mut lower, &mut bridge, returned, count);
    drop(
        top.into_parts()
            .unwrap_or_else(|_| panic!("actual top drain")),
    );
    bridge.close();
    assert!(bridge.observations_are_drained());
    drop(bridge);
    reclaim_middle(
        &mut lower,
        middle,
        original_channels,
        target_channels,
        second_channels,
        Vec::new(),
    );
    lower.close();
    finish_clusters(clusters, target_cluster, second_cluster);
    eprintln!("356 actual12 original WAL/snapshot/dialer owners joined and TCP/UDP rebound");
}
pub(super) fn terminal(top: &Top, bridge: &super::UpperBridge<'_>, intent: &ChildActivationIntent) {
    let status = top.local().applications[&setup::top().group]
        .status(intent.operations().begin)
        .unwrap();
    assert_eq!(status.intent, *intent);
    assert!(status.outcome.is_some());
    assert!(status.release.is_some());
    let final_position = status.child_final.unwrap();
    for slot in [0, 1, 3] {
        bridge
            .inspect_backing(slot, |p| {
                let core = p.owner.core(setup::middle().group).unwrap();
                assert_eq!(core.membership().id(), intent.target_configuration());
                assert!(
                    p.applications[&setup::middle().group].applied_index() >= final_position.index
                );
            })
            .unwrap();
    }
}

pub(super) fn reclaim_middle(
    lower: &mut Lower<'_>,
    mut middle: Vec<super::Middle>,
    original_channels: super::channels::Channels,
    target_channels: super::channels::Channels,
    second_channels: Option<super::channels::Channels>,
    retired: Vec<super::repeated::Session>,
) {
    super::shutdown::middle(lower, &mut middle);
    super::readiness::closed(&mut middle);
    let sessions = middle
        .into_iter()
        .flat_map(|n| {
            n.into_parts()
                .unwrap_or_else(|_| panic!("same middle original drain"))
                .sessions
                .into_values()
        })
        .collect::<Vec<_>>();
    original_channels.close(
        sessions
            .into_iter()
            .chain(retired)
            .chain(lower.take_retired_for_close()),
        lower.now(),
    );
    target_channels.close(std::iter::empty(), lower.now());
    if let Some(channels) = second_channels {
        channels.close(std::iter::empty(), lower.now());
    }
}

pub(super) fn initial_top(lower: &mut Lower<'_>, bridge: &mut super::UpperBridge<'_>) -> Top {
    let upper = progress::claim_upper(lower, bridge);
    let mut top = Top::from_parts(
        &upper,
        parts::build(&upper, setup::top_app(), setup::upper_limits(), lower.now()),
        parts::node_limits::<
            ActivationReceipt<voteboat::application::CounterReceipt>,
            ActivationRead<i64>,
        >(setup::upper_limits()),
        lower.now(),
    )
    .unwrap_or_else(|e| panic!("actual top {:?}", e.reason));
    progress::elect_top(lower, bridge, &mut top);
    business::request(lower, bridge, &mut top, 3, 356300, 7, 7);
    top
}

pub(super) fn begin_driver(
    top: &mut Top,
    intent: ChildActivationIntent,
) -> ChildActivationDriver<'_, Top> {
    ChildActivationDriver::new(
        top,
        intent,
        ChildActivationDriverLimits {
            retained_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap_or_else(|e| panic!("actual driver {:?}", e.reason))
}

pub(super) fn target_member<'a>(
    lower: &mut Lower<'a>,
    middle: &mut Vec<super::Middle>,
    descriptors: &mut Vec<LogicalPeerDescriptor>,
    target: &'a mut Cluster,
    grant: LogicalLearnerEnrollment,
    mode: (NativePeerProtocol, bool, bool, bool),
) -> super::channels::Channels {
    let target_image = if mode.1 {
        let (image, bridge) = if mode.3 {
            super::uncommitted_target::recover(lower, middle, target, grant)
        } else if mode.2 {
            super::unknown_target::recover(lower, middle, target, grant)
        } else {
            super::cold_target::recover(lower, middle, target, grant)
        };
        lower.bridges.push(bridge);
        image
    } else {
        add_lower(lower, target);
        eprintln!("356 actual_targetB_enrollment");
        enrollment::target(lower, middle, grant)
    };
    super::assembly::target(lower, middle, descriptors, &target_image, mode.0)
}
fn finish_clusters(mut originals: Vec<Cluster>, mut first: Cluster, mut second: Option<Cluster>) {
    for cluster in &mut originals {
        cluster.checkpoint();
    }
    first.checkpoint();
    if let Some(cluster) = &mut second {
        cluster.checkpoint();
    }
    for cluster in originals {
        cluster.finish();
    }
    first.finish();
    if let Some(cluster) = second {
        cluster.finish();
    }
}

fn add_lower<'a>(lower: &mut Lower<'a>, cluster: &'a mut Cluster) {
    lower.bridges.push(
        LogicalBackingBridge::new(
            &mut cluster.nodes,
            setup::lower_mapping(cluster.slot),
            cluster.representative,
        )
        .unwrap(),
    );
}

fn finish_first<'p>(
    lower: &mut Lower<'_>,
    bridge: &mut super::UpperBridge<'_>,
    driver: ChildActivationDriver<'p, Top>,
    begin: ActivationPosition,
    intent: &ChildActivationIntent,
) -> &'p mut Top {
    assert_eq!(driver.original_begin_position(), Some(begin));
    let top = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("actual parent original observations drain"));
    terminal(top, bridge, intent);
    business::request(lower, bridge, top, 4, 356301, 5, 12);
    top
}
