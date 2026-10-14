// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::mem::size_of;

#[tokio::test]
async fn zero_identity_never_publishes_a_partial_request_and_precedes_send_error() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = Client(sender);
    assert_eq!(
        client
            .write(&[u128::MAX, 0, 17])
            .await
            .unwrap_err()
            .to_string(),
        "zero original identity"
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    receiver.close();
    assert_eq!(
        client.write(&[1, 0]).await.unwrap_err().to_string(),
        "zero original identity"
    );
    assert_eq!(
        client.write(&[1]).await.unwrap_err().to_string(),
        "channel closed"
    );
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn queued_client_cancellation_drops_every_original_wait_without_losing_request() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = Client(sender);
    let ids = [u128::MAX, 1 << 127, (1 << 64) | 1, 19];
    let mut write = Box::pin(client.write(&ids));
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut write)
        .await
        .is_err());
    let request = receiver.try_recv().unwrap();
    assert_eq!(request.originals.len(), ids.len());
    drop(write);
    for (original, id) in request.originals.into_iter().zip(ids) {
        assert_eq!(original.operation.get(), id);
        assert!(original.bytes.is_empty());
        assert!(original.reply.is_closed());
        assert_eq!(
            original.reply.send(original.operation).unwrap_err(),
            original.operation
        );
    }
}

#[tokio::test]
async fn first_wrong_receipt_returns_while_later_sender_remains_owned() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = Client(sender);
    let writer = tokio::spawn(async move { client.write(&[u128::MAX, 17]).await });
    let mut request = receiver.recv().await.unwrap();
    let tail = request.originals.pop().unwrap();
    let first = request.originals.pop().unwrap();
    first.reply.send(OperationId::new(9).unwrap()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string(),
        "VoteBoat original applied result differs"
    );
    assert!(tail.reply.is_closed());
}

#[tokio::test]
async fn first_closed_receipt_returns_without_waiting_for_the_tail() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = Client(sender);
    let writer = tokio::spawn(async move { client.write(&[1 << 127, 7]).await });
    let mut request = receiver.recv().await.unwrap();
    let tail = request.originals.pop().unwrap();
    drop(request);
    let result = tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), "channel closed");
    assert!(tail.reply.is_closed());
}

#[tokio::test]
async fn flat_request_layout_and_every_channel_capacity_are_reported_at_queue_boundary() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = Client(sender);
    let ids = [u128::MAX, 1 << 127, (1 << 64) | 1, 19];
    let writer = tokio::spawn(async move { client.write(&ids).await });
    let request = receiver.recv().await.unwrap();
    assert_eq!(request.originals.len(), ids.len());
    assert!(request.originals.capacity() >= ids.len());
    println!("old_proposal_bytes={} old_reply_bytes={} old_element_sum={} old_two_vec_header_bytes={} flat_original_bytes={} flat_request_header_bytes={} originals={} original_capacity={} completion_channels={}", size_of::<Proposal>(), size_of::<Reply>(), size_of::<Proposal>() + size_of::<Reply>(), size_of::<(Vec<Proposal>, Vec<Reply>)>(), size_of::<Original>(), size_of::<Request>(), request.originals.len(), request.originals.capacity(), request.originals.len());
    for (original, id) in request.originals.into_iter().zip(ids) {
        assert_eq!(original.operation.get(), id);
        original.reply.send(original.operation).unwrap();
    }
    assert_eq!(writer.await.unwrap().unwrap(), ids.len());
}
