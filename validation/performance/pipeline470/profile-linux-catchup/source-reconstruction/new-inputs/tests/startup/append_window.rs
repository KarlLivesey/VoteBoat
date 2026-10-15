// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    native::connect::NativePeerProtocol,
    raft::{AppendWindowLimits, Role},
    worker::PersistenceWorker,
};

fn selection() -> AppendWindowLimits {
    AppendWindowLimits {
        max_units: 4,
        max_originals: 64,
        max_retained_bytes: LogLimits::default().max_batch_bytes,
    }
}
fn rejected<T, A>(result: Result<T, Box<NativeStartupRejected<A>>>, expected: Option<&str>) {
    let mut rejection = result.err().expect("incompatible append window admitted");
    if let Some(stage) = expected {
        assert_eq!(rejection.reason.stage, stage);
    }
    assert!(rejection.application.is_some());
    assert!(rejection.try_cleanup().unwrap());
}
#[test]
fn explicit_window_requires_application_opt_in_before_listeners_or_files() {
    let directory = root();
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let startup = || {
        let mut config = config(directory.clone(), NativeOpenMode::Create);
        config.listen = occupied.local_addr().unwrap();
        config.append_window = Some(selection());
        config
    };
    let wake = || Arc::new(ThreadWake::current());
    rejected(
        startup().open(app(), wake(), MonoTime(0)),
        Some("append window"),
    );
    rejected(
        startup().prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            app(),
            wake(),
            MonoTime(0),
        ),
        Some("append window"),
    );
    let mut config = startup();
    config.tls = config.tls.with_wire_version(8).unwrap();
    let multi = NativeMultiStartup {
        provisioned_stores: config.bootstrap.voter_stores.clone(),
        startup: config,
        additional_groups: vec![],
    };
    rejected(
        multi.open(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            [(group(), app())].into(),
            wake(),
            MonoTime(0),
        ),
        Some("append window"),
    );
    assert!(!directory.exists());
}
#[test]
fn invalid_window_or_ordinary_request_capacity_refuses_before_resources() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    for units in [0, 1, 56, 57, 64, 65, usize::MAX] {
        let directory = root();
        let mut config = config(directory.clone(), NativeOpenMode::Create);
        config.listen = occupied.local_addr().unwrap();
        config.append_window = Some(AppendWindowLimits {
            max_units: units,
            ..selection()
        });
        rejected(
            config.open(
                Counter::new(100).unwrap(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            ),
            None,
        );
        assert!(!directory.exists());
    }
}
fn open(directory: &std::path::Path, mode: NativeOpenMode, selected: bool) -> NativeNode<Counter> {
    let mut config = config(directory.to_path_buf(), mode);
    config.append_window = selected.then(selection);
    config
        .open(
            Counter::new(100).unwrap(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap()
}
fn poll(node: &mut NativeNode<Counter>, clock: Instant) {
    node.poll(
        MonoTime(clock.elapsed().as_millis() as u64),
        NodePollBudget::default(),
    )
    .unwrap();
}
fn campaign(node: &mut NativeNode<Counter>, clock: Instant) {
    node.control(group(), NodeControl::Campaign).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        poll(node, clock);
        let core = node.local().owner.core(group()).unwrap();
        if core.role() == Role::Leader
            && !core.has_pending_dependency()
            && node.local().applications[&group()].applied_index() == core.state().commit_index
        {
            return;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn command(operation: u128, value: i64) -> ClientRequest {
    ClientRequest {
        group: group(),
        operation: OperationId::new(operation).unwrap(),
        bytes: value.to_le_bytes().to_vec(),
    }
}
fn collect(node: &mut NativeNode<Counter>, clock: Instant, count: usize) -> Vec<CounterReceipt> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut receipts = Vec::new();
    while receipts.len() < count {
        poll(node, clock);
        while let Some(completion) = node.poll_client() {
            let ClientOutcome::Applied { position, receipt } =
                node.complete_client(completion).unwrap()
            else {
                panic!("original lost its applied outcome")
            };
            assert_eq!(receipt.index, position.index);
            receipts.push(receipt);
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    receipts
}
#[test]
fn selected_native_window_applies_individual_outcomes_and_recovers_into_serial_default() {
    let directory = root();
    let clock = Instant::now();
    let mut node = open(&directory, NativeOpenMode::Create, true);
    assert_eq!(
        node.local()
            .owner
            .core(group())
            .unwrap()
            .append_window_limits(),
        Some(selection())
    );
    assert_eq!(
        node.local().persistence.max_outstanding_units_per_group(),
        4
    );
    let local = node.local();
    local
        .owner
        .validate_append_pipeline(&local.persistence)
        .unwrap();
    let binding = local.persistence.binding();
    let worker_limits = local.persistence.limits();
    let mut foreign = binding;
    foreign.generation = StorageWorkerGeneration::new(2).unwrap();
    assert_eq!(
        local
            .owner
            .validate_append_pipeline_limits(foreign, worker_limits, 4),
        Err(EffectOwnerError::WrongWorker)
    );
    for capacity in [0, 1, 65] {
        assert_eq!(
            local
                .owner
                .validate_append_pipeline_limits(binding, worker_limits, capacity),
            Err(EffectOwnerError::InvalidLimits)
        );
    }
    let required = append_window_effect_reservation(
        local.owner.core(group()).unwrap(),
        ShardLimits::default().max_event_bytes,
    )
    .unwrap();
    assert!(required <= local.owner.limits().reserved_bytes - local.owner.limits().control_bytes);
    campaign(&mut node, clock);
    for operation in 1..=32 {
        node.propose(command(operation, 1)).unwrap();
    }
    for (offset, receipt) in collect(&mut node, clock, 32).into_iter().enumerate() {
        assert_eq!(
            receipt.operation,
            OperationId::new(offset as u128 + 1).unwrap()
        );
        assert_eq!(receipt.outcome, CounterOutcome::Value(offset as i64 + 1));
        assert!(!receipt.duplicate);
    }
    close(node);
    let clock = Instant::now();
    let mut recovered = open(&directory, NativeOpenMode::Recover, false);
    assert_eq!(
        recovered
            .local()
            .owner
            .core(group())
            .unwrap()
            .append_window_limits(),
        None
    );
    assert_eq!(
        recovered
            .local()
            .persistence
            .max_outstanding_units_per_group(),
        1
    );
    assert_eq!(
        recovered.local().applications[&group()].read_applied(33),
        Ok(32)
    );
    campaign(&mut recovered, clock);
    for value in [1, 2] {
        recovered.propose(command(1, value)).unwrap();
    }
    for (receipt, outcome) in collect(&mut recovered, clock, 2)
        .into_iter()
        .zip([CounterOutcome::Value(1), CounterOutcome::OperationConflict])
    {
        assert_eq!(receipt.outcome, outcome);
        assert!(receipt.duplicate);
    }
    close(recovered);
    std::fs::remove_dir_all(directory).unwrap();
}
