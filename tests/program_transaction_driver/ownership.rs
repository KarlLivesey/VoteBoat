// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, drive::*};
use std::time::{Duration, Instant};
use voteboat::{native::connect::NativePeerProtocol, program_transaction::*, runtime::*};

fn nonempty_stream(host: &mut Cluster) {
    let original = program(40, 0, 98);
    let node = host.leader(original.coordinator()).unwrap();
    let ticket = host.nodes[node]
        .propose(ClientRequest {
            group: original.coordinator(),
            operation: voteboat::identity::OperationId::new(original.id().get() * 4).unwrap(),
            bytes: ProgramApplication::start(original.clone()).unwrap(),
        })
        .unwrap();
    let construction = Driver::new(
        profile(),
        ProgramDriverGeneration::new(u64::MAX).unwrap(),
        ProgramDriverLimits::new(2).unwrap(),
        &mut host.nodes,
    );
    assert!(matches!(
        construction,
        Err(ProgramDriverError::InvalidAssembly)
    ));
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
    finished(&mut driver, clock, resumed, Decision::Commit);
    assert!(driver.is_drained());
}
fn stale_completion(host: &mut Cluster) {
    let clock = host.clock;
    let original = program(41, 1, 97);
    let (old_ticket, old_output) = {
        let mut driver = attach(host, 2);
        let ticket = driver.submit(original.clone()).unwrap();
        let output = output(&mut driver, clock);
        assert_eq!(output.ticket(), ticket);
        assert!(matches!(
            output.outcome(),
            ProgramDriverOutcome::Finished {
                decision: Decision::Commit,
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
    assert_eq!(
        driver.progress(old_ticket),
        Err(ProgramDriverError::WrongTicket)
    );
    let usage = driver.usage();
    let rejected = driver.complete(old_output).unwrap_err();
    assert_eq!(rejected.reason, ProgramDriverError::WrongTicket);
    assert_eq!(rejected.completion.ticket(), old_ticket);
    assert_eq!(driver.usage(), usage);
    finished(&mut driver, clock, fresh, Decision::Commit);
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
            ProgramDriverOutcome::Unknown {
                transaction: absent,
                reason: ProgramDriverUnknown::NoRetainedStart,
            }
        );
        assert!(driver.is_drained());
    }
    for group in groups() {
        assert!(host.status(group, absent).is_none());
    }
}
fn ownership(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut host = Cluster::new(protocol, 8, [100, 100]);
    nonempty_stream(&mut host);
    stale_completion(&mut host);
    absent_resume(&mut host);
    refused_intake(&mut host);
    super::history::assert_values(&mut host, 97, 2);
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

fn refused_intake(host: &mut Cluster) {
    let unknown = Program::new(
        TransactionId::new(44).unwrap(),
        profile(),
        vec![ParticipantProgram::new(
            profile().participants()[0],
            vec![KeyAccess {
                key: KeyId::new(999).unwrap(),
                expected_version: KeyVersion::new(0),
                write: Some(1),
            }],
        )
        .unwrap()],
    )
    .unwrap();
    {
        let mut driver = attach(host, 2);
        let rejected = driver.submit(unknown.clone()).unwrap_err();
        assert_eq!(*rejected.program, unknown);
        assert_eq!(
            rejected.reason,
            ProgramDriverError::Application(
                voteboat::application::ApplicationError::InvalidCommand
            )
        );
        assert_eq!(driver.usage(), ProgramDriverUsage::default());
        assert!(driver.is_drained());
        let changed = program(41, 1, 96);
        let rejected = driver.submit(changed.clone()).unwrap_err();
        assert_eq!(*rejected.program, changed);
        assert_eq!(
            rejected.reason,
            ProgramDriverError::Application(
                voteboat::application::ApplicationError::OperationConflict
            )
        );
        assert!(driver.is_drained());
    }
    for group in groups() {
        assert!(host.status(group, unknown.id()).is_none());
    }
    assert_eq!(
        host.status(
            program(41, 1, 97).coordinator(),
            TransactionId::new(41).unwrap()
        )
        .unwrap()
        .program,
        program(41, 1, 97)
    );
}
