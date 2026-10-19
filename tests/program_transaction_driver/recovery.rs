// SPDX-License-Identifier: RPL-1.5
use super::{cluster::*, drive::*, history::assert_values};
use voteboat::{
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    program_transaction::*,
};
fn prepared(host: &mut Cluster, original: &Program) {
    for (slot, p) in original.participants().iter().enumerate() {
        let status = host.status(p.group(), original.id()).unwrap();
        assert_eq!(status.program, *original);
        assert_eq!(status.phase, ProgramPhase::Prepared);
        assert_eq!(status.prepare, Some(PrepareOutcome::Prepared));
        assert_eq!(
            &*host.values(slot),
            &[
                (key(0), KeyRead::Locked(original.id())),
                (key(1), KeyRead::Locked(original.id()))
            ]
        );
    }
}
fn pause_at(host: &mut Cluster, original: &Program, stage: ProgramDriverStage, start: bool) {
    let clock = host.clock;
    let mut driver = attach(host, 2);
    let ticket = if start {
        driver.submit(original.clone()).unwrap()
    } else {
        driver.resume(original.id()).unwrap()
    };
    pause(&mut driver, clock, ticket, stage);
    lose_observation(&mut driver, clock);
}
fn complete(host: &mut Cluster, original: &Program) {
    let clock = host.clock;
    let mut driver = attach(host, 2);
    let ticket = driver.resume(original.id()).unwrap();
    finished(&mut driver, clock, ticket, Decision::Commit);
    assert!(driver.is_drained());
}
fn cold(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [100, 100]);
    let original = program(20, 0, 91);
    pause_at(&mut host, &original, ProgramDriverStage::Prepare(0), true);
    assert_eq!(
        host.status(profile().participants()[0], original.id())
            .unwrap()
            .phase,
        ProgramPhase::Prepared
    );
    for g in &profile().participants()[1..] {
        assert!(host.status(*g, original.id()).is_none())
    }
    pause_at(&mut host, &original, ProgramDriverStage::Prepare(2), false);
    prepared(&mut host, &original);
    let before = original
        .participants()
        .iter()
        .map(|p| host.status(p.group(), original.id()).unwrap())
        .collect::<Vec<_>>();
    host.checkpoint();
    let bases = host
        .nodes
        .iter()
        .map(|n| {
            n.local()
                .owner
                .core(original.coordinator())
                .unwrap()
                .state()
                .base_index()
        })
        .collect::<Vec<_>>();
    pause_at(
        &mut host,
        &original,
        ProgramDriverStage::ApplyDecision(1),
        false,
    );
    let decided = host.status(original.coordinator(), original.id()).unwrap();
    assert_eq!(decided.phase, ProgramPhase::Decided(Decision::Commit));
    for (node, base) in host.nodes.iter().zip(bases) {
        assert_eq!(
            node.local()
                .owner
                .core(original.coordinator())
                .unwrap()
                .state()
                .base_index(),
            base
        );
        assert!(decided.phases[1].as_ref().unwrap().index > base)
    }
    let suffix = original
        .participants()
        .iter()
        .map(|p| host.status(p.group(), original.id()).unwrap())
        .collect::<Vec<_>>();
    let images = groups().map(|g| host.status(g, original.id()));
    host.wait(|h| {
        h.nodes.iter().all(|n| {
            groups()
                .iter()
                .zip(&images)
                .all(|(g, status)| n.local().applications[g].status(original.id()) == *status)
        })
    });
    host.close();
    recover_own_images(&mut host, &original, &images);
    assert_eq!(
        host.status(original.coordinator(), original.id()).unwrap(),
        decided
    );
    for ((p, retained), old) in original.participants().iter().zip(suffix).zip(before) {
        assert_eq!(host.status(p.group(), original.id()).unwrap(), retained);
        assert_eq!(retained.phases[0], old.phases[0]);
    }
    for slot in 0..3 {
        assert!(host
            .values(slot)
            .iter()
            .all(|(_, r)| *r == KeyRead::Locked(original.id())))
    }
    complete(&mut host, &original);
    assert_values(&mut host, 91, 1);
    host.finish();
}
#[test]
fn tcp_original_prepares_checkpoint_suffix_cold_reopen() {
    cold(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_prepares_checkpoint_suffix_cold_reopen() {
    cold(NativePeerProtocol::Quic)
}
fn contention(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [100, 100]);
    let original = program(30, 0, 95);
    pause_at(&mut host, &original, ProgramDriverStage::Prepare(2), true);
    prepared(&mut host, &original);
    let preserved = original
        .participants()
        .iter()
        .map(|p| host.status(p.group(), original.id()).unwrap())
        .collect::<Vec<_>>();
    let blocked = program(31, 0, 90);
    let clock = host.clock;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(blocked.clone()).unwrap();
        finished(&mut driver, clock, ticket, Decision::Abort);
        assert!(driver.is_drained());
    }
    for g in &profile().participants()[1..] {
        let closed = host.status(*g, blocked.id()).unwrap();
        assert_eq!(closed.prepare, None);
        assert_eq!(closed.phase, ProgramPhase::Released(Decision::Abort));
    }
    prepared(&mut host, &original);
    host.replace_coordinator_leader(original.coordinator());
    for (p, retained) in original.participants().iter().zip(preserved) {
        assert_eq!(host.status(p.group(), original.id()).unwrap(), retained)
    }
    complete(&mut host, &original);
    assert_values(&mut host, 95, 1);
    host.finish();
}
#[test]
fn tcp_read_dependency_locks_contention_abort_and_real_leader_replacement() {
    contention(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_read_dependency_locks_contention_abort_and_real_leader_replacement() {
    contention(NativePeerProtocol::Quic)
}
fn lost(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 8, [100, 100]);
    for (version, stage) in [
        ProgramDriverStage::Decide,
        ProgramDriverStage::AppliedAll,
        ProgramDriverStage::Release(0),
        ProgramDriverStage::Finish,
    ]
    .into_iter()
    .enumerate()
    {
        let original = program(40 + version as u128, version as u64, 90 - version as i64);
        pause_at(&mut host, &original, stage, true);
        let retained = host.status(original.coordinator(), original.id()).unwrap();
        let expected = match stage {
            ProgramDriverStage::Decide => ProgramPhase::Decided(Decision::Commit),
            ProgramDriverStage::AppliedAll | ProgramDriverStage::Release(_) => {
                ProgramPhase::AppliedAll(Decision::Commit)
            }
            ProgramDriverStage::Finish => ProgramPhase::Finished(Decision::Commit),
            _ => unreachable!(),
        };
        assert_eq!(retained.phase, expected);
        if stage != ProgramDriverStage::Finish {
            assert!(host
                .values(2)
                .iter()
                .all(|(_, r)| *r == KeyRead::Locked(original.id())))
        }
        complete(&mut host, &original);
        let terminal = host.status(original.coordinator(), original.id()).unwrap();
        for (old, new) in retained.phases.iter().zip(&terminal.phases) {
            if old.is_some() {
                assert_eq!(old, new)
            }
        }
        assert_values(&mut host, 90 - version as i64, version as u64 + 1);
    }
    host.finish();
}
#[test]
fn tcp_original_decide_applied_all_release_finish_observation_loss() {
    lost(NativePeerProtocol::TcpTls)
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_decide_applied_all_release_finish_observation_loss() {
    lost(NativePeerProtocol::Quic)
}

fn recover_own_images(host: &mut Cluster, original: &Program, images: &[Option<ProgramStatus>; 5]) {
    host.open_unpolled(NativeOpenMode::Recover);
    for node in &host.nodes {
        for (group, status) in groups().iter().zip(images) {
            assert_eq!(
                node.local().applications[group].status(original.id()),
                *status
            );
        }
        for participant in original.participants() {
            assert_eq!(
                node.local().applications[&participant.group()].key(key(1)),
                KeyRead::Locked(original.id())
            );
        }
    }
    host.campaign();
}
