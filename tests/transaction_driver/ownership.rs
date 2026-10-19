// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, drive::*};
use std::time::{Duration, Instant};
use voteboat::{native::connect::NativePeerProtocol, runtime::*, transaction::*};

fn nonempty_stream(host: &mut Cluster) {
    let original = spec(40, 2, false);
    let node = host.leader(profile().coordinator()).unwrap();
    let ticket = host.nodes[node]
        .propose(ClientRequest {
            group: profile().coordinator(),
            operation: original.phases().coordinator()[0],
            bytes: TransferApplication::start(original).unwrap(),
        })
        .unwrap();
    let construction = Driver::new(
        profile(),
        TransferDriverGeneration::new(u64::MAX).unwrap(),
        TransferDriverLimits::new(2).unwrap(),
        &mut host.nodes,
    );
    assert!(matches!(construction, Err(DriverError::InvalidAssembly)));
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        host.poll();
        if let Some(output) = host.nodes[node].poll_client() {
            assert_eq!(output.ticket(), ticket);
            assert!(matches!(
                host.nodes[node].complete_client(output).unwrap(),
                ClientOutcome::Applied { .. }
            ));
            break;
        }
        assert!(
            Instant::now() < end,
            "original host-owned Start must complete"
        );
    }
    let clock = host.clock;
    let mut driver = attach(host, 2);
    let resumed = driver.resume(original.id()).unwrap();
    finished(&mut driver, clock, resumed, TransferDecision::Commit);
    assert!(driver.is_drained());
}
fn stale_completion(host: &mut Cluster) {
    let clock = host.clock;
    let original = spec(41, 3, false);
    let (old_ticket, old_output) = {
        let mut driver = attach(host, 2);
        let ticket = driver.submit(original).unwrap();
        let output = output(&mut driver, clock);
        assert_eq!(output.ticket(), ticket);
        assert!(matches!(
            output.outcome(),
            DriverOutcome::Finished {
                decision: TransferDecision::Commit,
                ..
            }
        ));
        assert_eq!(driver.usage().exported, 1);
        assert!(!driver.is_drained());
        // Drop only ends the borrow. This deliberately loses driver observation
        // ownership after actual Node outputs were consumed; it does no I/O.
        (ticket, output)
    };
    let mut driver = attach(host, 2);
    let fresh = driver.resume(original.id()).unwrap();
    assert_ne!(fresh.binding(), old_ticket.binding());
    assert_eq!(driver.progress(old_ticket), Err(DriverError::WrongTicket));
    let usage = driver.usage();
    let rejected = driver.complete(old_output).unwrap_err();
    assert_eq!(rejected.reason, DriverError::WrongTicket);
    assert_eq!(rejected.completion.ticket(), old_ticket);
    assert_eq!(driver.usage(), usage);
    finished(&mut driver, clock, fresh, TransferDecision::Commit);
    assert!(driver.is_drained());
}
fn absent_resume(host: &mut Cluster) {
    let clock = host.clock;
    let absent = TransactionId::new(42).unwrap();
    {
        let mut driver = attach(host, 2);
        let ticket = driver.resume(absent).unwrap();
        let output = output(&mut driver, clock);
        assert_eq!(output.ticket(), ticket);
        assert_eq!(
            driver.complete(output).unwrap(),
            DriverOutcome::Unknown {
                transaction: absent,
                reason: DriverUnknown::NoRetainedStart,
            }
        );
        assert!(driver.is_drained());
    }
    for group in groups() {
        assert!(host.status(group, absent).is_none());
    }
}
fn ownership(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [50, 0]);
    nonempty_stream(&mut host);
    stale_completion(&mut host);
    absent_resume(&mut host);
    assert_eq!(host.balance(0), AccountRead::Balance(45));
    assert_eq!(host.balance(1), AccountRead::Balance(5));
    host.finish();
}
#[test]
fn native_tcp_nonempty_stream_and_real_stale_driver_completion() {
    ownership(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_nonempty_stream_and_real_stale_driver_completion() {
    ownership(NativePeerProtocol::Quic);
}
