// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Accepted original attempts are held; this does not pretend to inspect opaque produced output.
use super::{host::Host, setup};
use std::time::{Duration, Instant};
use voteboat::{identity::OperationId, runtime::*, worker::PersistenceWorker};
pub fn submit(host: &mut Host<'_, '_>, operation: u128, value: i64) -> ClientTicket {
    host.until(Host::drained);
    let before = host.nodes[0]
        .local()
        .owner
        .core(setup::top().group)
        .unwrap()
        .state()
        .clone();
    host.lower.held = true;
    let ticket = host.nodes[0]
        .propose(ClientRequest {
            group: setup::top().group,
            operation: voteboat::application::Counter::operation(1, operation as u64).unwrap(),
            bytes: voteboat::application::Counter::command(0, value),
        })
        .unwrap();
    host.until(|h| {
        h.nodes[0].local().persistence.usage().requests == 1
            && h.lower
                .bridges
                .iter()
                .any(|b| b.usage().child_attempts == 1)
    });
    let charged = host.nodes[0].local().clients.usage();
    let end = Instant::now() + Duration::from_millis(64);
    while Instant::now() < end {
        host.poll();
        assert_eq!(
            host.nodes[0]
                .local()
                .owner
                .core(setup::top().group)
                .unwrap()
                .state(),
            &before
        );
        assert_eq!(host.nodes[0].local().persistence.usage().requests, 1);
        assert_eq!(host.nodes[0].local().clients.usage(), charged);
        assert!(host.nodes[0].poll_client().is_none());
        assert!(!host.drained());
    }
    host.lower.held = false;
    ticket
}
