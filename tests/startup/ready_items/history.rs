// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn open(directory: &std::path::Path, mode: NativeOpenMode, items: usize) -> NativeNode<Counter> {
    let mut startup = config(directory.to_path_buf(), mode);
    startup.ready_visit_items = items;
    startup
        .open(
            Counter::new(128).unwrap(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap()
}
fn campaign(node: &mut NativeNode<Counter>, clock: Instant) {
    node.control(group(), NodeControl::Campaign).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        node.poll(
            MonoTime(clock.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        )
        .unwrap();
        let core = node.local().owner.core(group()).unwrap();
        if core.role() == Role::Leader
            && node.local().applications[&group()].applied_index() == core.state().commit_index
        {
            return;
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn command(operation: u128, value: i64) -> ClientRequest {
    ClientRequest {
        group: group(),
        operation: Counter::operation(1, operation as u64).unwrap(),
        bytes: Counter::command(0, value),
    }
}
fn collect(
    node: &mut NativeNode<Counter>,
    clock: Instant,
    count: usize,
) -> (
    Vec<ClientOutcome<CounterReceipt>>,
    Vec<(VisitTicket, usize)>,
) {
    let mut results = Vec::new();
    let mut widths: Vec<(VisitTicket, usize)> = Vec::new();
    let end = Instant::now() + Duration::from_secs(10);
    while results.len() < count {
        let progress = node
            .poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        for step in progress
            .replica
            .into_iter()
            .flat_map(|progress| progress.steps)
        {
            if step.proposed.is_some() {
                if let Some((_, width)) = widths.iter_mut().find(|(visit, _)| *visit == step.visit)
                {
                    *width += 1;
                } else {
                    widths.push((step.visit, 1));
                }
            }
        }
        while let Some(completion) = node.poll_client() {
            results.push(node.complete_client(completion).unwrap());
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    (results, widths)
}
#[test]
fn native_selected_quantums_bound_original_visits_and_recover_exact_individual_retries() {
    for items in [1, 16, 32, 64] {
        let directory = root();
        let clock = Instant::now();
        let mut node = open(&directory, NativeOpenMode::Create, items);
        campaign(&mut node, clock);
        let tickets: Vec<_> = (1..=64)
            .map(|operation| node.propose(command(operation, 1)).unwrap())
            .collect();
        let (results, widths) = collect(&mut node, clock, 64);
        assert_eq!(widths.iter().map(|(_, width)| *width).sum::<usize>(), 64);
        assert!(widths.iter().all(|(_, width)| *width <= items));
        assert!(widths.iter().any(|(_, width)| *width > items / 2));
        for (offset, result) in results.into_iter().enumerate() {
            let ClientOutcome::Applied { position, receipt } = result else {
                panic!("original not applied")
            };
            assert_eq!(position.index, offset as u64 + 2);
            assert_eq!(receipt.operation, tickets[offset].operation);
            assert_eq!(receipt.index, position.index);
            assert_eq!(receipt.outcome, CounterOutcome::Value(offset as i64 + 1));
            assert!(!receipt.duplicate);
        }
        assert_eq!(node.local().applications[&group()].read_applied(65), Ok(64));
        close(node);
        let clock = Instant::now();
        let mut recovered = open(
            &directory,
            NativeOpenMode::Recover,
            if items == 64 { 16 } else { 64 },
        );
        assert_eq!(
            recovered.local().applications[&group()].read_applied(65),
            Ok(64)
        );
        campaign(&mut recovered, clock);
        for request in [command(1, 1), command(1, 2)] {
            recovered.propose(request).unwrap();
        }
        let (retry, _) = collect(&mut recovered, clock, 2);
        for (result, expected) in retry
            .into_iter()
            .zip([CounterOutcome::Value(1), CounterOutcome::OperationConflict])
        {
            let ClientOutcome::Applied { receipt, .. } = result else {
                panic!("retry not applied")
            };
            assert!(receipt.duplicate);
            assert_eq!(receipt.outcome, expected);
        }
        close(recovered);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
