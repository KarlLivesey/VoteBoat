// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A bridge-borrowed child's pending dial holds protocol time.
//!
//! Each test withholds one TCP child cluster's connector polls after its
//! nodes submit their first dials, which leaves every dial pending with its
//! worker's completion uncollected: the state of a dial worker the OS has not
//! yet scheduled. It then runs more rounds than the attempts' whole
//! protocol-time deadline before releasing the completions.
use super::{
    cluster::{serial, Cluster},
    harness::{self, FixtureClock, Observed, SimClock},
    lower::Lower,
};
use std::time::Duration;
use voteboat::{native::connect::NativePeerProtocol, runtime::NodePollBudget};

/// The roster's protocol-time deadline for one connection attempt.
fn attempt_ms(cluster: &Cluster) -> u64 {
    let peers = cluster.nodes[0].peers().unwrap();
    peers.roster().limits().connect_timeout_ms
}

#[test]
fn tcp_borrowed_child_dial_holds_protocol_time() {
    let _serial = serial();
    let clock = SimClock::default();
    let mut clusters = vec![Cluster::unelected(NativePeerProtocol::TcpTls, 0, &clock)];
    let dials = clusters[0].dials.clone();
    let attempt = attempt_ms(&clusters[0]);
    dials.withhold(true);
    let mut lower = Lower::new(&mut clusters, &clock);
    // The first round polls the borrowed nodes, which submit their dials.
    lower.advance();
    let (submitted, _, _) = dials.outcomes();
    assert!(submitted > 0, "the borrowed nodes dial their peers");
    assert!(clock.dial_work().get() > 0, "pending dials are reported");
    let held = clock.now();
    for round in 0..=attempt {
        lower.advance();
        assert_eq!(
            clock.now(),
            held,
            "protocol time advanced in round {round} while a borrowed child's dial was pending: {}",
            clock.describe()
        );
    }
    dials.withhold(false);
    let end = harness::Wait::new(&clock, harness::HANDSHAKE_MS);
    loop {
        lower.advance();
        let (submitted, connected, lost) = dials.outcomes();
        assert_eq!(lost, 0, "a held dial must not expire");
        if connected == submitted {
            break;
        }
        assert!(
            end.within(&clock),
            "held dials connect: {}",
            clock.describe()
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(clock.dial_work().get(), 0);
    lower.close();
    clusters[0].elect();
    Cluster::finish_all(clusters);
}

/// Control for the test above: the same withheld dials on a clock that does
/// not see them are lost, so the hold is what keeps them.
#[test]
fn tcp_withheld_child_dial_is_lost_on_a_clock_blind_to_it() {
    let _serial = serial();
    let reported = SimClock::default();
    let mut cluster = Cluster::unelected(NativePeerProtocol::TcpTls, 0, &reported);
    let blind = SimClock::default();
    cluster.clock = blind.clone();
    let dials = cluster.dials.clone();
    let attempt = attempt_ms(&cluster);
    dials.withhold(true);
    for _ in 0..=attempt + 1 {
        let now = blind.round(Observed::default());
        for node in &mut cluster.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
    }
    assert!(blind.now().0 > attempt, "blind time passed the deadline");
    assert!(
        reported.dial_work().get() > 0,
        "the dials are still pending"
    );
    dials.withhold(false);
    let end = harness::Wait::new(&blind, harness::HANDSHAKE_MS);
    while dials.outcomes().2 == 0 {
        cluster.poll();
        assert!(
            end.within(&blind),
            "withheld dials past their deadline end without a session"
        );
    }
    cluster.elect();
    Cluster::finish_all(vec![cluster]);
}
