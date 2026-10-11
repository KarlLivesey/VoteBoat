// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::cluster::{profile, Cluster};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use voteboat::{
    native::{
        connect::NativeServiceConnector,
        log_store::{FileLogIo, NativeLogStore},
        outbound::NativeOutbound,
        runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        snapshot_worker::NativeSnapshotWorker,
        transport::NativeTransportFactory,
        wire::NativeWireCodec,
        worker::NativeLogWorker,
    },
    runtime::MonoTime,
    transaction::*,
};

pub type Driver<'a> = TransferDriver<
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
static GENERATION: AtomicU64 = AtomicU64::new(1);
pub fn attach(host: &mut Cluster, active: usize) -> Driver<'_> {
    Driver::new(
        profile(),
        TransferDriverGeneration::new(GENERATION.fetch_add(1, Ordering::Relaxed)).unwrap(),
        TransferDriverLimits::new(active).unwrap(),
        &mut host.nodes,
    )
    .unwrap()
}
pub fn now(clock: Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
pub fn step(driver: &mut Driver<'_>, clock: Instant, budget: DriverBudget) {
    driver.advance(now(clock), budget).unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
pub fn output(driver: &mut Driver<'_>, clock: Instant) -> DriverCompletion {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(driver, clock, DriverBudget::default());
        if let Some(output) = driver.poll_completion() {
            return output;
        }
        assert!(
            Instant::now() < end,
            "actual driver completion timed out: {:?}",
            driver.usage()
        );
    }
}
pub fn finished(
    driver: &mut Driver<'_>,
    clock: Instant,
    ticket: DriverTicket,
    decision: TransferDecision,
) -> u64 {
    let output = output(driver, clock);
    assert_eq!(output.ticket(), ticket);
    let outcome = driver.complete(output).unwrap();
    match outcome {
        DriverOutcome::Finished {
            transaction,
            decision: actual,
            first_index,
        } => {
            assert_eq!(transaction, ticket.transaction());
            assert_eq!(actual, decision);
            assert!(first_index > 0);
            first_index
        }
        DriverOutcome::Unknown { .. } => panic!("transaction unexpectedly unresolved: {outcome:?}"),
    }
}
pub fn stop(driver: &mut Driver<'_>, clock: Instant) {
    driver.stop_waiting().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(
            driver,
            clock,
            DriverBudget {
                transitions: 0,
                ..DriverBudget::default()
            },
        );
        while let Some(output) = driver.poll_completion() {
            assert!(matches!(
                driver.complete(output).unwrap(),
                DriverOutcome::Unknown {
                    reason: DriverUnknown::Stopped,
                    ..
                }
            ));
        }
        if driver.is_drained() {
            return;
        }
        assert!(
            Instant::now() < end,
            "original driver ownership must drain: {:?}",
            driver.usage()
        );
    }
}
pub fn pause(driver: &mut Driver<'_>, clock: Instant, ticket: DriverTicket, stage: DriverStage) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let progress = driver.progress(ticket).unwrap();
        if progress.stage == stage && progress.outstanding {
            return;
        }
        step(
            driver,
            clock,
            DriverBudget {
                transitions: 1,
                ..DriverBudget::default()
            },
        );
        assert!(
            Instant::now() < end,
            "phase boundary not reached: {progress:?}"
        );
    }
}
/// Poll actual Nodes without consuming the original phase observation. Cancellation
/// subsequently returns observation ownership; it does not undo durable progress.
pub fn lose_observation(driver: &mut Driver<'_>, clock: Instant) {
    let end = Instant::now() + Duration::from_millis(150);
    while Instant::now() < end {
        step(
            driver,
            clock,
            DriverBudget {
                completions: 0,
                transitions: 0,
                ..DriverBudget::default()
            },
        );
    }
    stop(driver, clock);
}
