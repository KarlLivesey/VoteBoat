// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, drive::*};
use voteboat::{
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    transaction::*,
};

fn prepared(host: &mut Cluster, original: &TransferSpec) {
    for group in profile().participants() {
        let status = host.status(group, original.id()).unwrap();
        assert_eq!(status.spec, *original);
        assert_eq!(status.phase, TransferPhase::Prepared);
        assert!(matches!(
            status.prepare,
            Some(PrepareOutcome::Prepared { .. })
        ));
        assert!(status.phases[0].as_ref().unwrap().index > 0);
    }
    for slot in 0..2 {
        assert_eq!(host.balance(slot), AccountRead::Locked(original.id()));
    }
}
fn cold_resume(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [100, 10]);
    let original = spec(20, 9, true);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(original).unwrap();
        pause(&mut driver, clock, ticket, DriverStage::Prepare(0));
        lose_observation(&mut driver, clock);
    }
    // Reverse-direction debit is in participant1, yet participant0 prepares
    // first. No business observation can enable participant1 early.
    let lower = host
        .status(profile().participants()[0], original.id())
        .unwrap();
    assert_eq!(lower.phase, TransferPhase::Prepared);
    assert!(host
        .status(profile().participants()[1], original.id())
        .is_none());
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        pause(&mut driver, clock, ticket, DriverStage::Prepare(1));
        lose_observation(&mut driver, clock);
    }
    prepared(&mut host, &original);
    let before = profile()
        .participants()
        .map(|g| host.status(g, original.id()).unwrap());
    host.checkpoint();
    let bases = host
        .nodes
        .iter()
        .map(|node| {
            node.local()
                .owner
                .core(profile().coordinator())
                .unwrap()
                .state()
                .base_index()
        })
        .collect::<Vec<_>>();
    // A suffix after the prepared checkpoint includes the actual replicated
    // decision and first participant application, with its observation lost.
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        pause(&mut driver, clock, ticket, DriverStage::ApplyDecision(1));
        lose_observation(&mut driver, clock);
    }
    let decided = host.status(profile().coordinator(), original.id()).unwrap();
    assert_eq!(
        decided.phase,
        TransferPhase::Decided(TransferDecision::Commit)
    );
    for (node, base) in host.nodes.iter().zip(&bases) {
        assert_eq!(
            node.local()
                .owner
                .core(profile().coordinator())
                .unwrap()
                .state()
                .base_index(),
            *base
        );
        assert!(decided.phases[1].as_ref().unwrap().index > *base);
    }
    host.close();
    host.open(NativeOpenMode::Recover);
    let recovered = host.status(profile().coordinator(), original.id()).unwrap();
    assert_eq!(recovered, decided);
    for (slot, group) in profile().participants().into_iter().enumerate() {
        let retained = host.status(group, original.id()).unwrap();
        assert_eq!(retained.phases[0], before[slot].phases[0]);
        assert_eq!(retained.spec, original);
        assert_eq!(host.balance(slot), AccountRead::Locked(original.id()));
    }
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        finished(&mut driver, clock, ticket, TransferDecision::Commit);
        assert!(driver.is_drained());
    }
    assert_eq!(host.balance(0), AccountRead::Balance(109));
    assert_eq!(host.balance(1), AccountRead::Balance(1));
    host.finish();
}
#[test]
fn native_tcp_lost_prepare_checkpoint_suffix_and_cold_resume() {
    cold_resume(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_lost_prepare_checkpoint_suffix_and_cold_resume() {
    cold_resume(NativePeerProtocol::Quic);
}

fn locked_and_leader(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [50, 20]);
    let original = spec(30, 5, false);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(original).unwrap();
        pause(&mut driver, clock, ticket, DriverStage::Prepare(1));
        lose_observation(&mut driver, clock);
    }
    prepared(&mut host, &original);
    let preserved = host
        .status(profile().participants()[0], original.id())
        .unwrap();
    let blocked = spec(31, 1, true);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(blocked).unwrap();
        finished(&mut driver, clock, ticket, TransferDecision::Abort);
        assert!(driver.is_drained());
    }
    let closed_absent = host
        .status(profile().participants()[1], blocked.id())
        .unwrap();
    assert_eq!(closed_absent.prepare, None);
    assert_eq!(
        closed_absent.phase,
        TransferPhase::Released(TransferDecision::Abort)
    );
    assert_eq!(host.balance(0), AccountRead::Locked(original.id()));
    host.replace_coordinator_leader();
    assert_eq!(
        host.status(profile().participants()[0], original.id())
            .unwrap(),
        preserved
    );
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.resume(original.id()).unwrap();
        finished(&mut driver, clock, ticket, TransferDecision::Commit);
        assert!(driver.is_drained());
    }
    assert_eq!(host.balance(0), AccountRead::Balance(45));
    assert_eq!(host.balance(1), AccountRead::Balance(25));
    host.finish();
}
#[test]
fn native_tcp_locked_refusal_absent_abort_and_leader_replacement() {
    locked_and_leader(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_locked_refusal_absent_abort_and_leader_replacement() {
    locked_and_leader(NativePeerProtocol::Quic);
}

fn lost_release_and_finish(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [50, 0]);
    for (id, stage) in [(50, DriverStage::Release(0)), (51, DriverStage::Finish)] {
        let original = spec(id, 2, false);
        let clock = host.clock;
        {
            let mut driver = attach(&mut host, 2);
            let ticket = driver.submit(original).unwrap();
            pause(&mut driver, clock, ticket, stage);
            lose_observation(&mut driver, clock);
        }
        let retained = host.status(profile().coordinator(), original.id()).unwrap();
        let phase = if stage == DriverStage::Finish {
            TransferPhase::Finished(TransferDecision::Commit)
        } else {
            assert_eq!(host.balance(1), AccountRead::Locked(original.id()));
            TransferPhase::AppliedAll(TransferDecision::Commit)
        };
        assert_eq!(retained.phase, phase);
        let clock = host.clock;
        {
            let mut driver = attach(&mut host, 2);
            let resumed = driver.resume(original.id()).unwrap();
            finished(&mut driver, clock, resumed, TransferDecision::Commit);
            assert!(driver.is_drained());
        }
        let terminal = host.status(profile().coordinator(), original.id()).unwrap();
        assert_eq!(terminal.phases[0..3], retained.phases[0..3]);
    }
    assert_eq!(host.balance(0), AccountRead::Balance(46));
    assert_eq!(host.balance(1), AccountRead::Balance(4));
    host.finish();
}
#[test]
fn native_tcp_lost_applied_all_release_and_finish_observations() {
    lost_release_and_finish(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_lost_applied_all_release_and_finish_observations() {
    lost_release_and_finish(NativePeerProtocol::Quic);
}
