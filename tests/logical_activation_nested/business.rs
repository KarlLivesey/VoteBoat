// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{lower::Lower, progress, setup, Top, UpperBridge};
use std::time::{Duration, Instant};
use voteboat::{application::CounterOutcome, identity::OperationId, logical_voter::*, runtime::*};
pub fn request(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    top: &mut Top,
    count: usize,
    operation: u128,
    value: i64,
    expected: i64,
) {
    let operation = OperationId::new(operation).unwrap();
    let bytes = top.local().applications[&setup::top().group]
        .business(&value.to_le_bytes())
        .unwrap();
    let ticket = top
        .propose(ClientRequest {
            group: setup::top().group,
            operation,
            bytes,
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
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
        assert!(Instant::now() < end, "actual top business result");
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
        assert!(Instant::now() < end, "actual top quorum read");
    }
    while !top.work_is_drained() || !bridge.observations_are_drained() {
        progress::step(lower, bridge, top, count);
        assert!(Instant::now() < end, "business original observations drain");
    }
}
