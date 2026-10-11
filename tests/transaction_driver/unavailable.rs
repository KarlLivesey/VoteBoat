// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, drive::*};
use std::time::{Duration, Instant};
use voteboat::{native::connect::NativePeerProtocol, transaction::*};

fn unavailable(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut host = Cluster::new(protocol, 8, [40, 10]);
    let original = spec(60, 4, false);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(original).unwrap();
        pause(&mut driver, clock, ticket, DriverStage::Prepare(1));
        lose_observation(&mut driver, clock);
    }
    let before = groups().map(|g| host.status(g, original.id()).unwrap());
    host.wait(|h| {
        h.nodes.iter().all(|node| {
            groups().into_iter().enumerate().all(|(slot, group)| {
                node.local().applications[&group].status(original.id()) == Some(before[slot])
            })
        })
    });
    for slot in 0..2 {
        assert_eq!(host.balance(slot), AccountRead::Locked(original.id()));
    }
    let survivor = host.leader(profile().coordinator()).unwrap();
    let live = host.nodes.remove(survivor);
    let mut unavailable = std::mem::replace(&mut host.nodes, vec![live]);
    // Two real original replica owners stay alive but receive no polling. The
    // remaining replica cannot obtain new majority read/commit acknowledgments.
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        assert_eq!(
            driver.progress(ticket).unwrap().stage,
            DriverStage::CoordinatorStatus
        );
        let end = Instant::now() + Duration::from_secs(2);
        while Instant::now() < end {
            step(&mut driver, clock, DriverBudget::default());
            assert!(
                driver.poll_completion().is_none(),
                "unavailable quorum cannot authorize a terminal decision"
            );
        }
        stop(&mut driver, clock);
    }
    for (slot, group) in groups().into_iter().enumerate() {
        assert_eq!(
            host.nodes[0].local().applications[&group]
                .status(original.id())
                .unwrap(),
            before[slot]
        );
    }
    host.nodes.append(&mut unavailable);
    host.wait(|h| groups().into_iter().all(|g| h.leader(g).is_some()));
    for (slot, group) in groups().into_iter().enumerate() {
        assert_eq!(host.status(group, original.id()).unwrap(), before[slot]);
    }
    for slot in 0..2 {
        assert_eq!(host.balance(slot), AccountRead::Locked(original.id()));
    }
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        finished(&mut driver, clock, ticket, TransferDecision::Commit);
        assert!(driver.is_drained());
    }
    assert_eq!(host.balance(0), AccountRead::Balance(36));
    assert_eq!(host.balance(1), AccountRead::Balance(14));
    host.finish();
}
#[test]
fn native_tcp_quorum_unavailable_keeps_locks_and_resumes_after_repair() {
    unavailable(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_quorum_unavailable_keeps_locks_and_resumes_after_repair() {
    unavailable(NativePeerProtocol::Quic);
}
