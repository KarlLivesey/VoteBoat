// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Shared semantic checks, not a replacement codec or socket implementation.
use super::*;
mod host;

enum Flush {
    Native(Arc<Mutex<Channel>>),
    Host(Arc<Mutex<host::Pipe>>),
}
impl Flush {
    fn set(&self, flushed: bool) {
        match self {
            Self::Native(pipe) => pipe.lock().unwrap().flushed = flushed,
            Self::Host(pipe) => pipe.lock().unwrap().flushed = flushed,
        }
    }
}
struct Link {
    queue: NativeOutbound,
    sender: Box<dyn PeerTransport>,
    receiver: Box<dyn PeerTransport>,
    flush: Flush,
}
impl Link {
    fn new(native: bool, early_flush: bool) -> Self {
        let other = queue(2);
        let queue = queue(1);
        if native {
            let (a, b) = sessions(7);
            let flush = Flush::Native(a.outgoing.clone());
            Self {
                sender: Box::new(
                    NativePeerTransport::new(a, codec(), &queue, TransportLimits::default())
                        .unwrap(),
                ),
                receiver: Box::new(
                    NativePeerTransport::new(b, codec(), &other, TransportLimits::default())
                        .unwrap(),
                ),
                queue,
                flush,
            }
        } else {
            let (sender, receiver, pipe) =
                host::pair(queue.binding(), other.binding(), early_flush);
            Self {
                queue,
                sender: Box::new(sender),
                receiver: Box::new(receiver),
                flush: Flush::Host(pipe),
            }
        }
    }
    fn submit(&mut self, expected: Vec<Message>) -> SendTicket {
        let owned = batch(&mut self.queue, expected);
        let ticket = owned.ticket;
        self.sender.submit(owned).unwrap();
        ticket
    }
    fn step(&mut self, time: u64) {
        bounded_poll(self.sender.as_mut(), time);
        bounded_poll(self.receiver.as_mut(), time);
    }
    fn receive(&mut self) -> ReceiveInfo {
        for time in 0..2000 {
            self.step(time);
            if let Some(info) = self.receiver.received_info() {
                return info;
            }
        }
        panic!("finite shared-group transfer did not reach decoded slot");
    }
    fn complete(&mut self, ticket: SendTicket, messages: &[Message], result: LocalSendResult) {
        let done = self.sender.take_send().unwrap();
        assert_eq!(done.connection, binding(1, 2));
        assert_eq!(done.batch.ticket, ticket);
        assert_eq!(done.batch.messages, messages);
        assert_eq!(done.result, result);
        assert!(self.sender.take_send().is_none());
        assert!(self.queue.usage().batches > 0);
        self.queue.complete(done.batch, done.result).unwrap();
    }
}
fn bounded_poll(transport: &mut dyn PeerTransport, time: u64) {
    let budget = TransportPollBudget {
        plaintext_calls: 3,
        read_bytes: 29,
        write_bytes: 31,
        ..TransportPollBudget::default()
    };
    let progress = transport.poll(MonoTime(time), budget).unwrap();
    assert!(progress.plaintext_calls <= budget.plaintext_calls);
    assert!(progress.read_bytes <= budget.read_bytes);
    assert!(progress.written_bytes <= budget.write_bytes);
    let usage = transport.usage();
    let limits = transport.limits();
    assert!(usage.send_frame_bytes <= limits.send_frame_bytes);
    assert!(usage.receive_frame_bytes <= limits.receive_frame_bytes);
    assert!(usage.decoded_bytes <= limits.decoded_bytes);
}
fn rejected(link: &mut Link, expected: &[Message]) -> OutboundBatch {
    let original = batch(&mut link.queue, expected.to_vec());
    let ticket = original.ticket;
    let refusal = link.sender.submit(original).unwrap_err();
    assert_eq!(refusal.reason, TransportError::Overloaded);
    assert_eq!(refusal.batch.ticket, ticket);
    assert_eq!(refusal.batch.messages, expected);
    *refusal.batch
}
fn take(link: &mut Link, info: ReceiveInfo, expected: &[Message]) {
    assert_eq!(link.receiver.received_info(), Some(info));
    let received = link.receiver.take_received().unwrap();
    assert_eq!(received.connection, binding(2, 1));
    assert_eq!(
        received.info(link.receiver.limits().decoded_bytes).unwrap(),
        info
    );
    assert_eq!(received.messages, expected);
    assert!(link.receiver.take_received().is_none());
    assert!(link.receiver.received_info().is_none());
    assert_eq!(link.receiver.usage().decoded_bytes, 0);
}
fn shared_groups(link: &mut Link) {
    assert_eq!(link.sender.security(), SessionSecurity::Authenticated);
    assert_eq!(link.sender.binding(), binding(1, 2));
    assert_eq!(link.sender.state(), TransportState::Open);
    link.flush.set(false);
    let first = vec![message(1, 2, 11), message(1, 2, 12), message(1, 2, 13)];
    let second = vec![message(1, 2, 13), message(1, 2, 11), message(1, 2, 12)];
    let first_ticket = link.submit(first.clone());
    let usage = link.queue.usage();
    let zero = TransportPollBudget {
        plaintext_calls: 0,
        read_bytes: 0,
        write_bytes: 0,
        ..TransportPollBudget::default()
    };
    assert_eq!(
        link.sender.poll(MonoTime(0), zero).unwrap(),
        TransportProgress::default()
    );
    assert_eq!(link.queue.usage(), usage);
    assert!(link.sender.take_send().is_none());
    let retry = rejected(link, &second);
    let second_ticket = retry.ticket;
    let info = link.receive();
    assert_eq!(info.messages, 3);
    assert_eq!(info.connection, binding(2, 1));
    assert_eq!(info.class, MessageClass::Control);
    // Remote decode cannot complete the original local send before flush.
    assert!(
        link.sender.take_send().is_none(),
        "local flush was bypassed"
    );
    assert!(link.sender.usage().sending);
    for time in 2000..2010 {
        link.step(time);
        assert_eq!(link.receiver.received_info(), Some(info));
    }
    link.flush.set(true);
    link.step(2010);
    assert!(link.sender.usage().completion);
    let extra = rejected(link, &first);
    link.queue.complete(extra, LocalSendResult::Failed).unwrap();
    link.complete(first_ticket, &first, LocalSendResult::Sent);
    link.sender.submit(retry).unwrap();
    for time in 2011..2111 {
        link.step(time);
        assert_eq!(link.receiver.received_info(), Some(info));
    }
    take(link, info, &first);
    let info = link.receive();
    take(link, info, &second);
    for time in 2111..4111 {
        if link.sender.usage().completion {
            break;
        }
        link.step(time);
    }
    link.complete(second_ticket, &second, LocalSendResult::Sent);
    assert_eq!(link.queue.usage(), OutboundUsage::default());
}
fn draining(link: &mut Link) {
    link.flush.set(false);
    let messages = vec![message(1, 2, 20), message(1, 2, 21)];
    let ticket = link.submit(messages.clone());
    let info = link.receive();
    link.sender.close();
    assert_eq!(link.sender.state(), TransportState::Draining);
    let original = batch(&mut link.queue, vec![message(1, 2, 22)]);
    let refused_ticket = original.ticket;
    let refused = link.sender.submit(original).unwrap_err();
    assert_eq!(refused.reason, TransportError::Closed);
    assert_eq!(refused.batch.ticket, refused_ticket);
    link.queue
        .complete(*refused.batch, LocalSendResult::Failed)
        .unwrap();
    link.step(0);
    assert_eq!(link.sender.state(), TransportState::Draining);
    assert!(link.sender.take_send().is_none());
    link.flush.set(true);
    link.step(1);
    assert_eq!(link.sender.state(), TransportState::Closed);
    link.complete(ticket, &messages, LocalSendResult::Sent);
    // Already validated inbound data survives local abort and stays observable.
    link.receiver.abort();
    take(link, info, &messages);
    assert_eq!(link.queue.usage(), OutboundUsage::default());
}
fn abort_and_replacement(link: &mut Link) {
    link.flush.set(false);
    let messages = vec![message(1, 2, 30), message(1, 2, 31)];
    let ticket = link.submit(messages.clone());
    link.step(0); // partial frame, never a decoded batch
    assert!(link.receiver.received_info().is_none());
    assert!(link.sender.usage().send_frame_bytes > 0);
    link.sender.abort();
    assert_eq!(link.sender.state(), TransportState::Failed);
    assert_eq!(link.sender.usage().send_frame_bytes, 0);
    assert_eq!(link.queue.usage().batches, 1);
    let done = link.sender.take_send().unwrap();
    assert_eq!(done.batch.ticket, ticket);
    assert_eq!(done.batch.messages, messages);
    assert_eq!(done.result, LocalSendResult::Failed);
    let mut next = link.queue.binding();
    next.generation = OutboundGeneration::new(2).unwrap();
    let mut replacement = NativeOutbound::new(next, OutboundLimits::default()).unwrap();
    let rejected = replacement.complete(done.batch, done.result).unwrap_err();
    assert_eq!(rejected.reason, OutboundError::WrongBinding);
    assert_eq!(rejected.batch.ticket, ticket);
    assert_eq!(link.queue.usage().batches, 1);
    link.queue
        .complete(*rejected.batch, LocalSendResult::Failed)
        .unwrap();
    assert_eq!(link.queue.usage(), OutboundUsage::default());
    assert!(link.sender.take_send().is_none());
}
fn invalid_admission(link: &mut Link) {
    let mut wrong = link.queue.binding();
    wrong.generation = OutboundGeneration::new(2).unwrap();
    let mut other = NativeOutbound::new(wrong, OutboundLimits::default()).unwrap();
    let expected = vec![message(1, 2, 40)];
    let original = batch(&mut other, expected.clone());
    let ticket = original.ticket;
    let before = link.sender.usage();
    let rejected = link.sender.submit(original).unwrap_err();
    assert_eq!(rejected.reason, TransportError::WrongBinding);
    assert_eq!(rejected.batch.ticket, ticket);
    assert_eq!(rejected.batch.messages, expected);
    assert_eq!(link.sender.usage(), before);
    other
        .complete(*rejected.batch, LocalSendResult::Failed)
        .unwrap();
    let owned = link.submit(expected.clone());
    let before = link.sender.usage();
    let rejected_budget = TransportPollBudget {
        plaintext_calls: 4097,
        ..TransportPollBudget::default()
    };
    assert_eq!(
        link.sender.poll(MonoTime(0), rejected_budget),
        Err(TransportError::InvalidLimits)
    );
    assert_eq!(link.sender.usage(), before);
    assert!(link.sender.take_send().is_none());
    assert!(link.receiver.received_info().is_none());
    link.sender.abort();
    link.complete(owned, &expected, LocalSendResult::Failed);
    assert_eq!(link.queue.usage(), OutboundUsage::default());
}
#[test]
fn independent_host_transport_preserves_shared_groups_budgets_and_terminal_ownership() {
    shared_groups(&mut Link::new(false, false));
    draining(&mut Link::new(false, false));
    abort_and_replacement(&mut Link::new(false, false));
    invalid_admission(&mut Link::new(false, false));
}
#[test]
fn native_transport_preserves_same_shared_groups_budgets_and_terminal_ownership() {
    shared_groups(&mut Link::new(true, false));
    draining(&mut Link::new(true, false));
    abort_and_replacement(&mut Link::new(true, false));
    invalid_admission(&mut Link::new(true, false));
}
#[test]
fn shared_checker_detects_completion_before_local_flush() {
    let failure = std::panic::catch_unwind(|| shared_groups(&mut Link::new(false, true)));
    let cause = failure.expect_err("faulty provider escaped the shared checker");
    let message = cause
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| cause.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(message.contains("local flush was bypassed"));
}
