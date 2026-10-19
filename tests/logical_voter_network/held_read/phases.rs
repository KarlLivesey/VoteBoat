// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub fn write(host: &mut Host<'_>) {
    let parent = &mut host.parents[0];
    let ticket = parent
        .routers
        .clients
        .submit(
            &mut parent.owner,
            &parent.apps[&parent.mapping.parent],
            ClientRequest {
                group: parent.mapping.parent,
                operation: Counter::operation(1, 2).unwrap(),
                bytes: Counter::command(0, 5),
            },
        )
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    let output = loop {
        host.poll();
        if let Some(output) = host.parents[0].routers.clients.poll() {
            break output;
        }
        assert!(Instant::now() < end, "real original write must complete");
    };
    assert_eq!(output.ticket(), ticket);
    let ClientOutcome::Applied { position, receipt } =
        host.parents[0].routers.clients.complete(output).unwrap()
    else {
        panic!("real write receipt");
    };
    assert_eq!(receipt.index, position.index);
    assert_eq!(receipt.operation, ticket.operation);
    assert_eq!(receipt.outcome, CounterOutcome::Value(5));
    assert!(!receipt.duplicate);
    host.until(|h| {
        h.is_drained()
            && h.parents
                .iter()
                .all(|p| p.apps[&p.mapping.parent].applied_index() == position.index)
    });
}
pub fn submit(host: &mut Host<'_>, node: usize) -> ReadInvocationTicket {
    let parent = &mut host.parents[node];
    parent
        .routers
        .reads
        .submit(
            &mut parent.owner,
            &parent.apps[&parent.mapping.parent],
            parent.mapping.parent,
            (),
        )
        .unwrap()
}
pub fn resume(
    host: &mut Host<'_>,
    ticket: ReadInvocationTicket,
    held: &Held,
    charged: ReadInvocationUsage,
) {
    held.assert_same(host);
    let end = Instant::now() + Duration::from_secs(15);
    let output = loop {
        host.poll();
        if let Some(output) = host.parents[0].routers.reads.poll() {
            break output;
        }
        assert!(
            Instant::now() < end,
            "same original read must resume through real peers"
        );
    };
    assert_eq!(output.ticket(), ticket);
    assert_eq!(host.parents[0].routers.reads.usage(), charged);
    let view = host.parents[0].routers.reads.inspect(ticket).unwrap();
    assert!(view.has_output() && view.exported() && view.terminal());
    for _ in 0..8 {
        host.poll();
        assert_eq!(host.parents[0].routers.reads.usage(), charged);
        assert_eq!(host.parents[0].routers.reads.inspect(ticket).unwrap(), view);
    }
    let ReadOutcome::Read {
        barrier,
        result: Ok(value),
    } = host.parents[0].routers.reads.complete(output).unwrap()
    else {
        panic!("real original peer-quorum read");
    };
    assert_eq!(barrier.group(), ticket.group);
    assert_eq!(barrier.request(), ticket.request);
    assert_eq!(value, 5);
    assert_eq!(
        host.parents[0].routers.reads.usage(),
        ReadInvocationUsage::default()
    );
    assert_eq!(
        host.parents[0].routers.reads.inspect(ticket),
        Err(ReadInvocationError::StaleTicket)
    );
}
