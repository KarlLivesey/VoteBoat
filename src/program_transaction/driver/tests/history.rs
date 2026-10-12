// SPDX-License-Identifier: RPL-1.5
//! Real single-voter, five-group Node controls. Full multi-replica transport and
//! crash qualification remain the separate downstream native acceptance.
use super::{native::*, *};
use crate::native::{
    connect::NativeServiceConnector,
    log_store::{FileLogIo, NativeLogStore},
    outbound::NativeOutbound,
    runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
    snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
    snapshot_worker::NativeSnapshotWorker,
    transport::NativeTransportFactory,
    wire::NativeWireCodec,
    worker::NativeLogWorker,
};
use std::time::{Duration, Instant};
type Driver<'a> = ProgramDriver<
    'a,
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    NativeLogWorker<NativeLogStore<FileLogIo>>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativeServiceConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
fn attach(host: &mut Host, generation: u64, active: usize) -> Driver<'_> {
    ProgramDriver::new(
        profile(),
        ProgramDriverGeneration::new(generation).unwrap(),
        ProgramDriverLimits::new(active).unwrap(),
        &mut host.nodes,
    )
    .unwrap()
}
fn tick(driver: &mut Driver<'_>, clock: Instant, budget: ProgramDriverBudget) {
    driver
        .advance(MonoTime(clock.elapsed().as_millis() as u64), budget)
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
fn finish(driver: &mut Driver<'_>, clock: Instant) -> ProgramDriverOutcome {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        tick(driver, clock, ProgramDriverBudget::default());
        if let Some(output) = driver.poll_completion() {
            return driver.complete(output).unwrap();
        }
        assert!(Instant::now() < end)
    }
}
fn with_id(id: u128) -> Program {
    Program::new(
        TransactionId::new(id).unwrap(),
        profile(),
        spec().participants().to_vec(),
    )
    .unwrap()
}
#[test]
fn actual_commit_then_version_refusal_preserves_original_first_positions() {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = attach(&mut host, 1, 2);
    let original = spec();
    driver.submit(original.clone()).unwrap();
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Commit,
            ..
        }
    ));
    let coordinator = driver.nodes[0].local().applications[&original.coordinator()]
        .status(original.id())
        .unwrap();
    for (slot, phase) in coordinator.phases.iter().enumerate() {
        assert_eq!(
            phase.as_ref().unwrap().operation,
            original.operation(slot).unwrap()
        );
    }
    for participant in original.participants() {
        let app = &driver.nodes[0].local().applications[&participant.group()];
        assert_eq!(
            app.key(KeyId::new(1).unwrap()),
            KeyRead::Value {
                value: 90,
                version: KeyVersion::new(1)
            }
        );
        assert_eq!(
            app.key(KeyId::new(2).unwrap()),
            KeyRead::Value {
                value: 100,
                version: KeyVersion::new(0)
            }
        );
    }
    let next = with_id(2);
    assert_ne!(next.coordinator(), original.coordinator());
    driver.submit(next.clone()).unwrap();
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Abort,
            ..
        }
    ));
    let end = Instant::now() + Duration::from_secs(5);
    while !driver.is_drained() {
        tick(&mut driver, clock, ProgramDriverBudget::default());
        assert!(Instant::now() < end)
    }
    driver.submit(original.clone()).unwrap();
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Commit,
            ..
        }
    ));
    assert_eq!(
        driver.nodes[0].local().applications[&original.coordinator()]
            .status(original.id())
            .unwrap()
            .phases,
        coordinator.phases
    );
    drop(driver);
    host.finish();
}
#[test]
fn unconsumed_actual_start_cannot_supply_prepare_then_fresh_resume_finishes() {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = attach(&mut host, 1, 1);
    let ticket = driver.submit(spec()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while driver.nodes[0].local().applications[&spec().coordinator()]
        .status(spec().id())
        .is_none()
    {
        tick(
            &mut driver,
            clock,
            ProgramDriverBudget {
                completions: 0,
                transitions: 0,
                ..ProgramDriverBudget::default()
            },
        );
        assert!(Instant::now() < end)
    }
    assert_eq!(
        driver.progress(ticket).unwrap().stage,
        ProgramDriverStage::Start
    );
    assert!(driver.usage().command_bytes > 0 && driver.usage().nested_bytes > 0);
    for g in profile().participants() {
        assert!(driver.nodes[0].local().applications[g]
            .status(spec().id())
            .is_none())
    }
    driver.stop_waiting().unwrap();
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Unknown {
            reason: ProgramDriverUnknown::Stopped,
            ..
        }
    ));
    while !driver.is_drained() {
        tick(&mut driver, clock, ProgramDriverBudget::default());
        assert!(Instant::now() < end)
    }
    drop(driver);
    let mut resumed = attach(&mut host, 2, 1);
    resumed.resume(spec().id()).unwrap();
    assert!(matches!(
        finish(&mut resumed, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Commit,
            ..
        }
    ));
    drop(resumed);
    host.finish();
}
#[test]
fn real_exported_completion_is_rejected_intact_by_fresh_driver() {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = attach(&mut host, 1, 1);
    driver.submit(spec()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let original = loop {
        tick(&mut driver, clock, ProgramDriverBudget::default());
        if let Some(output) = driver.poll_completion() {
            break output;
        }
        assert!(Instant::now() < end)
    };
    assert!(!driver.is_drained());
    drop(driver);
    let mut driver = attach(&mut host, 2, 1);
    driver.resume(spec().id()).unwrap();
    let before = driver.usage();
    let rejected = driver.complete(original).unwrap_err();
    assert_eq!(rejected.reason, ProgramDriverError::WrongTicket);
    assert_eq!(rejected.completion.ticket().binding().generation.get(), 1);
    assert_eq!(driver.usage(), before);
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Commit,
            ..
        }
    ));
    drop(driver);
    host.finish();
}

#[test]
fn actual_lifetime_quota_refuses_new_start_but_original_controls_still_finish() {
    let mut host = Host::new();
    let clock = host.clock;
    let mut driver = attach(&mut host, 1, 1);
    for id in [1, 3, 5, 7] {
        let original = with_id(id);
        driver.submit(original).unwrap();
        let expected = if id == 1 {
            Decision::Commit
        } else {
            Decision::Abort
        };
        assert!(
            matches!(finish(&mut driver,clock),ProgramDriverOutcome::Finished{decision,..} if decision==expected)
        );
    }
    let rejected_program = with_id(9);
    let rejected = driver.submit(rejected_program.clone()).unwrap_err();
    assert_eq!(*rejected.program, rejected_program);
    assert!(matches!(
        rejected.reason,
        ProgramDriverError::Application(_)
    ));
    assert_eq!(driver.usage().requests, 0);
    assert!(
        driver.nodes[0].local().applications[&rejected_program.coordinator()]
            .status(rejected_program.id())
            .is_none()
    );
    driver.submit(spec()).unwrap();
    assert!(matches!(
        finish(&mut driver, clock),
        ProgramDriverOutcome::Finished {
            decision: Decision::Commit,
            ..
        }
    ));
    assert!(driver.is_drained());
    drop(driver);
    host.finish();
}
