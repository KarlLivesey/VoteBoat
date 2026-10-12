// SPDX-License-Identifier: RPL-1.5
use super::{cluster::*, drive::*};
use voteboat::{native::connect::NativePeerProtocol, program_transaction::*};
pub fn assert_values(host: &mut Cluster, value: i64, version: u64) {
    for slot in 0..3 {
        assert_eq!(
            &*host.values(slot),
            &[
                (
                    key(0),
                    KeyRead::Value {
                        value,
                        version: KeyVersion::new(version)
                    }
                ),
                (
                    key(1),
                    KeyRead::Value {
                        value: 100,
                        version: KeyVersion::new(0)
                    }
                )
            ]
        );
    }
}
fn commit_refusal(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut host = Cluster::new(protocol, 4, [100, 100]);
    let original = program(1, 0, 90);
    let clock = host.clock;
    let first;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(original.clone()).unwrap();
        first = finished(&mut driver, clock, ticket, Decision::Commit);
        assert!(driver.is_drained());
    }
    let retained = groups()[1..]
        .iter()
        .map(|g| host.status(*g, original.id()).unwrap())
        .collect::<Vec<_>>();
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let retry = driver.submit(original.clone()).unwrap();
        assert_eq!(finished(&mut driver, clock, retry, Decision::Commit), first);
        let next = driver.submit(program(2, 1, 80)).unwrap();
        finished(&mut driver, clock, next, Decision::Commit);
        let stale = driver.submit(program(3, 0, 70)).unwrap();
        finished(&mut driver, clock, stale, Decision::Abort);
        driver.close();
        assert_eq!(
            driver.submit(program(4, 2, 60)).unwrap_err().reason,
            ProgramDriverError::Closed
        );
        assert!(driver.is_drained());
        assert_eq!(driver.usage(), ProgramDriverUsage::default());
    }
    for (g, before) in groups()[1..].iter().zip(retained) {
        assert_eq!(host.status(*g, original.id()).unwrap(), before);
    }
    read_dependency_refusal(&mut host);
    assert_values(&mut host, 80, 2);
    host.finish();
}
#[test]
fn tcp_commit_original_retry_other_coordinator_and_version_refusal() {
    commit_refusal(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_commit_original_retry_other_coordinator_and_version_refusal() {
    commit_refusal(NativePeerProtocol::Quic)
}
fn capacity(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut host = Cluster::new(protocol, 1, [100, 100]);
    let original = program(10, 0, 90);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 1);
        let ticket = driver.submit(original.clone()).unwrap();
        let refused = program(12, 0, 80);
        let rejected = driver.submit(refused.clone()).unwrap_err();
        assert_eq!(rejected.reason, ProgramDriverError::Overloaded);
        assert_eq!(*rejected.program, refused);
        finished(&mut driver, clock, ticket, Decision::Commit);
        let retry = driver.submit(original.clone()).unwrap();
        finished(&mut driver, clock, retry, Decision::Commit);
        let rejected = driver.submit(refused.clone()).unwrap_err();
        assert_eq!(*rejected.program, refused);
        assert_eq!(
            rejected.reason,
            ProgramDriverError::Application(voteboat::application::ApplicationError::DedupCapacity)
        );
        let other = driver.submit(program(11, 1, 80)).unwrap();
        finished(&mut driver, clock, other, Decision::Commit);
        assert!(driver.is_drained());
    }
    for g in groups() {
        assert!(host.status(g, TransactionId::new(12).unwrap()).is_none());
    }
    assert_values(&mut host, 80, 2);
    host.finish();
}
#[test]
fn tcp_active_and_per_coordinator_lifetime_bound() {
    capacity(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_active_and_per_coordinator_lifetime_bound() {
    capacity(NativePeerProtocol::Quic)
}

fn read_dependency_refusal(host: &mut Cluster) {
    let original = Program::new(
        TransactionId::new(4).unwrap(),
        profile(),
        profile()
            .participants()
            .iter()
            .map(|g| {
                ParticipantProgram::new(
                    *g,
                    vec![
                        KeyAccess {
                            key: key(0),
                            expected_version: KeyVersion::new(2),
                            write: Some(70),
                        },
                        KeyAccess {
                            key: key(1),
                            expected_version: KeyVersion::new(1),
                            write: None,
                        },
                    ],
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let clock = host.clock;
    {
        let mut driver = attach(host, 2);
        let ticket = driver.submit(original.clone()).unwrap();
        finished(&mut driver, clock, ticket, Decision::Abort);
        assert!(driver.is_drained());
    }
    assert_eq!(
        host.status(profile().participants()[0], original.id())
            .unwrap()
            .prepare,
        Some(PrepareOutcome::Refused(PrepareRefusal::Version {
            key: key(1),
            actual: KeyVersion::new(0)
        }))
    );
}
