// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{harness, lower::Lower, progress, setup, Top, UpperBridge};
use voteboat::{
    application::{Counter, CounterOutcome},
    logical_voter::*,
    runtime::*,
};
pub fn request(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    top: &mut Top,
    count: usize,
    operation: u128,
    value: i64,
    expected: i64,
) {
    // `operation` is the sequence in the counter's built-in session.
    let operation = Counter::operation(1, operation as u64).unwrap();
    let bytes = top.local().applications[&setup::top().group]
        .business(&Counter::command(0, value))
        .unwrap();
    let ticket = top
        .propose(ClientRequest {
            group: setup::top().group,
            operation,
            bytes,
        })
        .unwrap();
    // The original 15 s bound over all three waits, in protocol time.
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        progress::step(lower, bridge, top, count);
        if let Some(output) = top.poll_client() {
            assert_eq!(output.ticket(), ticket);
            let ClientOutcome::Applied {
                receipt: ActivationReceipt::Business(receipt),
                position,
            } = top.complete_client(output).unwrap()
            else {
                panic!("actual business Applied");
            };
            assert_eq!(receipt.outcome, CounterOutcome::Value(expected));
            assert_eq!(receipt.operation, operation);
            assert_eq!(receipt.index, position.index);
            break;
        }
        assert!(end.within(&lower.clock), "actual top business result");
    }
    let ticket = top
        .read(setup::top().group, ActivationQuery::Business(()))
        .unwrap();
    loop {
        progress::step(lower, bridge, top, count);
        if let Some(output) = top.poll_read() {
            assert_eq!(output.ticket(), ticket);
            assert!(
                matches!(top.complete_read(output).unwrap(),ReadOutcome::Read {
            result:Ok(ActivationRead::Business(value)),..} if value==expected)
            );
            break;
        }
        assert!(end.within(&lower.clock), "actual top quorum read");
    }
    while !top.work_is_drained() || !bridge.observations_are_drained() {
        progress::step(lower, bridge, top, count);
        assert!(
            end.within(&lower.clock),
            "business original observations drain"
        );
    }
}
