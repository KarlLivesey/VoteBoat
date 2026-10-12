// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{host::Host, requests, setup, watchdog::Watchdog};
use voteboat::{raft::RaftError, runtime::*};
pub fn read(host: &mut Host<'_, '_>) {
    host.phase = "upper_read";
    let mut watch = Watchdog::new();
    for ordinal in 0..requests::ATTEMPTS {
        let (node, ticket) = requests::read(host, &watch);
        let output = loop {
            host.poll();
            watch.read(host, node, ticket, ordinal);
            check(host, &watch, node, ticket);
            if let Some(output) = host.nodes[node].poll_read() {
                break output;
            }
        };
        assert_eq!(output.ticket(), ticket);
        watch.read_output(ticket, &output, host.lower.now(), ordinal);
        let charged = host.nodes[node].local().reads.usage();
        for _ in 0..8 {
            host.poll();
            watch.read(host, node, ticket, ordinal);
            check(host, &watch, node, ticket);
            assert_eq!(host.nodes[node].local().reads.usage(), charged);
        }
        let outcome = host.nodes[node].complete_read(output).unwrap();
        eprintln!(
            "nested original consumed read now={:?} ticket={ticket:?} outcome={outcome:?}",
            host.lower.now()
        );
        match outcome {
            ReadOutcome::Read {
                barrier,
                result: Ok(value),
            } => {
                assert_eq!(barrier.group(), setup::top().group);
                assert_eq!(barrier.request(), ticket.request);
                assert_eq!(value, 5);
                return;
            }
            ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged)
            | ReadOutcome::NotRead(
                RaftError::Busy | RaftError::NotLeader | RaftError::ReadNotReady,
            ) => {}
            outcome => panic!(
                "original quorum read outcome: {outcome:?}; {}",
                super::diagnostic::retained(host)
            ),
        }
    }
    panic!(
        "original read accepted attempt cap: {}",
        super::diagnostic::retained(host)
    );
}
fn check(host: &Host<'_, '_>, watch: &Watchdog, node: usize, ticket: ReadInvocationTicket) {
    assert!(
        watch.within_absolute_bound(),
        "original fresh quorum read functional timeout: {}; watch={watch:?}; {}",
        super::read_inspection::retained(host, node, ticket),
        super::diagnostic::retained(host)
    );
}
