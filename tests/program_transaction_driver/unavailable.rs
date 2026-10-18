// SPDX-License-Identifier: RPL-1.5
use super::{cluster::*, drive::*, history::assert_values};
use std::time::{Duration, Instant};
use voteboat::{native::connect::NativePeerProtocol, program_transaction::*};
fn unavailable(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [100, 100]);
    let original = program(60, 0, 96);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(original.clone()).unwrap();
        pause(&mut driver, clock, ticket, ProgramDriverStage::Prepare(2));
        lose_observation(&mut driver, clock);
    }
    let before = groups().map(|g| host.status(g, original.id()));
    host.wait(|h| {
        h.nodes.iter().all(|n| {
            groups()
                .iter()
                .zip(&before)
                .all(|(g, status)| n.local().applications[g].status(original.id()) == *status)
        })
    });
    for slot in 0..3 {
        assert!(host
            .values(slot)
            .iter()
            .all(|(_, r)| *r == KeyRead::Locked(original.id())))
    }
    let survivor = host.leader(original.coordinator()).unwrap();
    let live = host.nodes.remove(survivor);
    let mut unavailable = std::mem::replace(&mut host.nodes, vec![live]);
    // Original owners are retained but unpolled, not a claimed physical partition.
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        assert_eq!(
            driver.progress(ticket).unwrap().stage,
            ProgramDriverStage::CoordinatorStatus
        );
        let end = Instant::now() + Duration::from_secs(2);
        while Instant::now() < end {
            step(&mut driver, clock, ProgramDriverBudget::default());
            assert!(
                driver.poll_completion().is_none(),
                "unavailable quorum cannot authorize decision"
            );
        }
        stop(&mut driver, clock);
    }
    for (g, status) in groups().iter().zip(&before) {
        assert_eq!(
            host.nodes[0].local().applications[g].status(original.id()),
            *status
        );
    }
    host.nodes.append(&mut unavailable);
    host.wait(|h| groups().into_iter().all(|g| h.leader(g).is_some()));
    for (g, status) in groups().iter().zip(before) {
        assert_eq!(host.status(*g, original.id()), status)
    }
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        finished(&mut driver, clock, ticket, Decision::Commit);
        assert!(driver.is_drained());
    }
    assert_values(&mut host, 96, 1);
    host.finish();
}
#[test]
fn tcp_unavailable_real_quorum_keeps_locks_until_restored() {
    unavailable(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_unavailable_real_quorum_keeps_locks_until_restored() {
    unavailable(NativePeerProtocol::Quic)
}
