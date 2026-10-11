// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn outgoing(link: &mut Link) -> (Message, Message, SendTicket, SendTicket) {
    let left = message(1);
    let mut right = message(2);
    right.from = node(2);
    right.to = node(1);
    right.sender = local(2).store;
    right.context.origin = local(2).store;
    let left_ticket = link.queue.submit(vec![left.clone()]).unwrap();
    let right_ticket = link.receiver_queue.submit(vec![right.clone()]).unwrap();
    link.sender
        .submit(link.queue.poll(1).pop().unwrap())
        .unwrap();
    link.receiver
        .submit(link.receiver_queue.poll(1).pop().unwrap())
        .unwrap();
    (left, right, left_ticket, right_ticket)
}

fn simultaneous(cadence: usize) {
    let mut link = Link::new();
    let (left, right, left_ticket, right_ticket) = outgoing(&mut link);
    link.sender.close();
    link.receiver.close();
    // Keep decoded inputs and send completions unread throughout close. They
    // belong to the caller and may not be silently consumed to claim drain.
    for elapsed in (0..5000).step_by(cadence) {
        link.poll(elapsed);
        assert_eq!(link.queue.usage().batches, 1);
        assert_eq!(link.receiver_queue.usage().batches, 1);
        if link.sender.state() == TransportState::Closed
            && link.receiver.state() == TransportState::Closed
        {
            break;
        }
    }
    assert_eq!(link.sender.state(), TransportState::Closed);
    assert_eq!(link.receiver.state(), TransportState::Closed);
    for (transport, queue, ticket, input) in [
        (&mut link.sender, &mut link.queue, left_ticket, &left),
        (
            &mut link.receiver,
            &mut link.receiver_queue,
            right_ticket,
            &right,
        ),
    ] {
        let completion = transport.take_send().unwrap();
        assert_eq!(completion.connection, transport.binding());
        assert_eq!(completion.batch.ticket, ticket);
        assert_eq!(
            completion.batch.messages.as_slice(),
            std::slice::from_ref(input)
        );
        assert_eq!(completion.result, LocalSendResult::Sent);
        queue.complete(completion.batch, completion.result).unwrap();
        assert!(queue.is_drained());
        assert!(transport.take_send().is_none());
    }
    for (transport, input) in [(&mut link.sender, right), (&mut link.receiver, left)] {
        let received = transport.take_received().unwrap();
        assert_eq!(received.connection, transport.binding());
        assert_eq!(received.messages, [input]);
        assert!(transport.take_received().is_none());
        assert_eq!(transport.usage(), TransportUsage::default());
    }
}

#[test]
fn simultaneous_frame_close_retains_original_sends_and_unread_input_fine() {
    simultaneous(1);
}

#[test]
fn simultaneous_frame_close_retains_original_sends_and_unread_input_coarse() {
    simultaneous(6);
}

#[test]
fn closing_partially_sent_frame_on_peer_loss_returns_original_failed_ownership() {
    let mut link = Link::new();
    let original = message(1);
    let ticket = link.queue.submit(vec![original.clone()]).unwrap();
    link.sender
        .submit(link.queue.poll(1).pop().unwrap())
        .unwrap();
    link.sender.close();
    let mut written = false;
    for elapsed in 0..4 {
        let progress = link
            .sender
            .poll(
                MonoTime(link.start + elapsed),
                TransportPollBudget::default(),
            )
            .unwrap();
        link.arrivals.emitted(0, progress.session.written_bytes);
        written |= progress.written_bytes > 0;
        if written {
            break;
        }
    }
    assert!(written, "cut requires actual plaintext progress");
    assert!(link.sender.usage().sending);
    assert!(link.sender.take_send().is_none());
    assert_eq!(link.queue.usage().batches, 1);
    drop(link.receiver);
    assert!(matches!(
        link.sender
            .poll(MonoTime(link.start + 60000), TransportPollBudget::default()),
        Err(TransportError::Session(SessionError::Truncated))
    ));
    assert_eq!(link.sender.state(), TransportState::Failed);
    assert_eq!(link.queue.usage().batches, 1);
    let completion = link.sender.take_send().unwrap();
    assert_eq!(completion.connection, link.sender.binding());
    assert_eq!(completion.batch.ticket, ticket);
    assert_eq!(completion.batch.messages, [original]);
    assert_eq!(completion.result, LocalSendResult::Failed);
    link.queue
        .complete(completion.batch, completion.result)
        .unwrap();
    assert!(link.queue.is_drained());
    assert!(link.sender.take_send().is_none());
    assert_eq!(link.sender.usage(), TransportUsage::default());
}
