// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn history(protocol: NativePeerProtocol) {
    let mut h = Harness::new(protocol, false);
    h.nodes[1]
        .control(group_id(1), NodeControl::Campaign)
        .unwrap();
    h.wait(|h| h.leader(group_id(1)) == Some(1));
    let mut driver = Driver::new(h.plan.clone());
    let mut batch = driver
        .coordinator
        .poll(&h.journal, 1, |g| (g == group_id(1)).then(|| h.core(1, g)))
        .unwrap();
    assert!(batch.errors.is_empty());
    assert_eq!(batch.requests.len(), 1);
    let dispatch = batch.requests.pop().unwrap();
    let MembershipDrainAction::Configure(record) = dispatch.action else {
        panic!("original Joint dispatch")
    };
    assert_eq!(record, h.plan.groups()[0].change.joint);
    let requirements = h.nodes[1].local().applications[&group_id(1)]
        .deployment_requirements()
        .unwrap();
    let ticket = h.nodes[1]
        .configure(ConfigurationRequest {
            group: group_id(1),
            proposal: ConfigurationProposal {
                record,
                requirements,
                readiness: vec![],
            },
        })
        .unwrap();
    h.nodes[1].cancel_configuration(ticket).unwrap();
    driver.pending.push(Pending {
        ticket: dispatch.ticket,
        owner: 1,
        kind: WaitKind::Configuration(ConfigurationWait::new(ticket)),
    });
    driver.complete(&mut h);
    assert_eq!(h.core(1, group_id(1)).membership().id().get(), 1);
    assert!(!h.core(1, group_id(1)).has_pending_dependency());
    assert_eq!(
        driver.pending.len(),
        1,
        "cancelled wait is still queued work"
    );
    assert_eq!(driver.coordinator.in_flight(), 1);
    let batch = driver
        .coordinator
        .poll(&h.journal, 1, |g| (g == group_id(1)).then(|| h.core(1, g)))
        .unwrap();
    assert!(batch.errors.is_empty() && batch.requests.is_empty());
    let deadline = Instant::now() + Duration::from_secs(25);
    while !h.nodes[0]
        .membership_drain_ready(&h.plan, &h.journal)
        .unwrap()
        || !driver.pending.is_empty()
        || !(1..=3).all(|g| committed(&h, g, 3))
    {
        driver.tick(&mut h, false);
        assert!(
            Instant::now() < deadline,
            "queued cancellation recovery: {:?}",
            h.progress()
        );
    }
    assert!((1..=3).all(|g| committed(&h, g, 3)));
    h.reopen();
    h.wait(|h| {
        h.nodes[0]
            .membership_drain_ready(&h.plan, &h.journal)
            .unwrap()
            && (1..=3).all(|g| committed(h, g, 3))
    });
    assert!((1..=3).all(|g| committed(&h, g, 3)));
    for g in 1..=3 {
        h.write(group_id(g), 100 + g as u128, 7, true, 7);
    }
    close(std::mem::take(&mut h.nodes), &h.clock).unwrap();
    std::fs::remove_dir_all(&h.root).unwrap();
}

#[test]
fn tcp_cancelled_wait_retains_queued_owner_admission() {
    history(NativePeerProtocol::TcpTls);
}

#[cfg(feature = "quic")]
#[test]
fn quic_cancelled_wait_retains_queued_owner_admission() {
    history(NativePeerProtocol::Quic);
}
