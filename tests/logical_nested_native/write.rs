// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    convergence::{WriteProgress, WriteResult},
    host::Host,
    requests,
    watchdog::ClientAttempt,
};
use voteboat::{application::*, raft::RaftError, runtime::*};
pub fn write(host: &mut Host<'_, '_>, operation: u128, value: i64, duplicate: bool) -> WriteResult {
    host.phase = if duplicate {
        "upper_retry"
    } else {
        "upper_write"
    };
    // `operation` is the sequence in the counter's built-in session.
    let operation = voteboat::application::Counter::operation(1, operation as u64).unwrap();
    let bytes: [u8; 16] = voteboat::application::Counter::command(0, value)
        .try_into()
        .unwrap();
    assert!(host.writes.len() < 2);
    let record = host.writes.len();
    host.writes.push(WriteProgress::new(operation, bytes));
    let mut uncertain = false;
    for _ in 0..requests::ATTEMPTS {
        let (node, ticket, before) = requests::client(host, operation, &bytes);
        let attempt = ClientAttempt::new(host, node, ticket, before, uncertain);
        host.writes[record].admit(attempt);
        let outcome = observe(host, record);
        eprintln!(
            "nested original consumed client now={:?} ticket={ticket:?} outcome={outcome:?}",
            host.lower.now()
        );
        match outcome {
            ClientOutcome::Applied { position, receipt } => {
                assert_eq!(host.writes[record].attempt().position, Some(position));
                assert_eq!(receipt.index, position.index);
                assert_eq!(receipt.operation, operation);
                if duplicate || !uncertain {
                    assert_eq!(receipt.duplicate, duplicate);
                }
                assert_eq!(receipt.outcome, CounterOutcome::Value(5));
                let result = WriteResult {
                    ticket,
                    position,
                    receipt,
                };
                let now = host.lower.now();
                host.writes[record].consumed(now, result);
                return result;
            }
            ClientOutcome::Unknown(ClientUnknown::LeadershipChanged) => uncertain = true,
            ClientOutcome::NotProposed(RaftError::Busy | RaftError::NotLeader) => {}
            outcome => panic!(
                "original client outcome: ticket={ticket:?}; {outcome:?}; {}",
                super::diagnostic::retained(host)
            ),
        }
    }
    panic!(
        "original client accepted attempt cap: {}",
        super::diagnostic::retained(host)
    );
}
fn observe(host: &mut Host<'_, '_>, record: usize) -> ClientOutcome<CounterReceipt> {
    let current = host.writes[record].attempt();
    let (node, ticket) = (current.node, current.ticket);
    let output = loop {
        host.poll();
        if let Some(output) = host.nodes[node].poll_client() {
            break output;
        }
    };
    assert_eq!(output.ticket(), ticket);
    let charged = host.nodes[node].local().clients.usage();
    for _ in 0..8 {
        host.poll();
        assert_eq!(host.nodes[node].local().clients.usage(), charged);
    }
    let outcome = host.nodes[node].complete_client(output).unwrap();
    host.writes[record].stop_observing();
    outcome
}
