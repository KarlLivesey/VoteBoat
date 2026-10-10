// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    native::{outbound::NativeOutbound, transport::NativePeerTransport, wire::NativeWireCodec},
    outbound::*,
    raft::*,
    transport::*,
    wire::*,
};

type Transport = NativePeerTransport<NativeQuicSession, NativeWireCodec>;
struct Link {
    sender: Transport,
    receiver: Transport,
    queue: NativeOutbound,
    start: u64,
    last: [TransportProgress; 2],
    arrivals: Arrivals,
}
impl Link {
    fn new() -> Self {
        let ((mut a, mut b), arrivals) =
            configured_pair_with(1, 1, SessionLimits::default(), Arrivals::capture);
        let start = arrivals.ready(&mut a, &mut b);
        let build = |n| {
            NativeOutbound::new(
                OutboundBinding {
                    node: local(n).node,
                    store: local(n).store,
                    generation: OutboundGeneration::new(1).unwrap(),
                },
                OutboundLimits::default(),
            )
            .unwrap()
        };
        let queue = build(1);
        let other = build(2);
        let codec = || NativeWireCodec::new(WireLimits::default()).unwrap();
        Self {
            sender: NativePeerTransport::new(a, codec(), &queue, TransportLimits::default())
                .unwrap(),
            receiver: NativePeerTransport::new(b, codec(), &other, TransportLimits::default())
                .unwrap(),
            queue,
            start,
            last: [TransportProgress::default(); 2],
            arrivals,
        }
    }
    fn poll(&mut self, elapsed: u64) {
        let budget = TransportPollBudget::default();
        for (index, transport) in [&mut self.sender, &mut self.receiver]
            .into_iter()
            .enumerate()
        {
            let p = transport
                .poll(MonoTime(self.start + elapsed), budget)
                .unwrap();
            assert!(p.plaintext_calls <= budget.plaintext_calls);
            assert!(p.read_bytes <= budget.read_bytes && p.written_bytes <= budget.write_bytes);
            assert!(p.session.io_calls <= budget.session.io_calls);
            assert!(p.session.read_bytes <= budget.session.read_bytes);
            assert!(p.session.written_bytes <= budget.session.write_bytes);
            self.last[index] = p;
            self.arrivals.emitted(index, p.session.written_bytes);
        }
    }
}
fn message(sequence: u64) -> Message {
    Message {
        group: group(u128::from(sequence)),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        to: node(2),
        sender: local(1).store,
        term: 1,
        context: RequestContext {
            origin: local(1).store,
            sequence,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        },
    }
}
fn history(cadence: usize, multiplexed: bool) {
    let mut link = Link::new();
    let expected = (1..=3).map(message).collect::<Vec<_>>();
    if multiplexed {
        link.queue.submit(expected.clone()).unwrap();
    } else {
        for message in &expected {
            link.queue.submit(vec![message.clone()]).unwrap();
        }
    }
    let total = if multiplexed { 1 } else { 3 };
    let original = link.queue.poll(3);
    assert_eq!(original.len(), total);
    let tickets = original
        .iter()
        .map(|batch| batch.ticket)
        .collect::<Vec<_>>();
    let mut waiting = std::collections::VecDeque::from(original);
    let mut delivered = Vec::new();
    let mut completed = 0;
    let mut elapsed = 0;
    let mut trace = Vec::with_capacity(50usize.div_ceil(cadence));
    // Real encrypted UDP/framing at selected virtual time; no wall-clock or
    // macOS cadence claim. Do not weaken local ACK/flush completion semantics.
    for time in (0..50).step_by(cadence) {
        elapsed = time;
        if !link.sender.usage().sending && !link.sender.usage().completion {
            if let Some(batch) = waiting.pop_front() {
                link.sender.submit(batch).unwrap();
            }
        }
        link.poll(time);
        if let Some(received) = link.receiver.take_received() {
            assert_eq!(received.connection, link.receiver.binding());
            delivered.extend(received.messages);
        }
        assert_eq!(link.queue.usage().batches, total - completed);
        if let Some(done) = link.sender.take_send() {
            assert_eq!(done.connection, link.sender.binding());
            assert_eq!(done.batch.ticket, tickets[completed]);
            assert_eq!(done.result, LocalSendResult::Sent);
            let messages = if multiplexed {
                expected.as_slice()
            } else {
                &expected[completed..completed + 1]
            };
            assert_eq!(done.batch.messages, messages);
            link.queue.complete(done.batch, done.result).unwrap();
            completed += 1;
        }
        trace.push((
            time,
            completed,
            delivered.len(),
            link.sender.usage(),
            waiting.len(),
            link.last,
        ));
        if completed == total && delivered == expected {
            break;
        }
    }
    println!("cadence={cadence} multiplexed={multiplexed} elapsed={elapsed} completed={completed}/{total} delivered={} sending={} completion={} waiting={}", delivered.len(), link.sender.usage().sending, link.sender.usage().completion, waiting.len());
    println!("tick/completed/delivered/sender-usage/waiting/polls: {trace:#?}");
    assert_eq!(
        delivered, expected,
        "group control burst did not arrive within selected heartbeat interval"
    );
    assert_eq!(
        completed, total,
        "original frames still wait for acknowledged flush"
    );
    assert_eq!(link.queue.usage(), OutboundUsage::default());
}
#[test]
fn separate_group_frames_complete_with_fine_owner_cadence() {
    history(1, false);
}
#[test]
fn separate_group_frames_complete_with_coarse_owner_cadence() {
    history(6, false);
}
#[test]
fn multiplexed_group_frame_completes_with_coarse_owner_cadence() {
    history(6, true);
}
