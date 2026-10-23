// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::client_fixture::{client, originals as observed};
use super::*;
use std::mem::size_of;

#[tokio::test]
async fn zero_identity_never_publishes_a_partial_request_and_precedes_send_error() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (_domain, mut client) = client(sender);
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
    let (domain, mut client) = client(sender);
    let ids = [u128::MAX, 1 << 127, (1 << 64) | 1, 19];
    let mut write = Box::pin(client.write(&ids));
    assert!(futures::poll!(&mut write).is_pending());
    let request = observed(&domain, receiver.try_recv().unwrap());
    assert_eq!(request.len(), ids.len());
    drop(write);
    for (original, id) in request.into_iter().zip(ids) {
        assert_eq!(original.operation.get(), id);
        assert!(original.bytes.is_empty());
        assert_eq!(
            original.reply.send(original.operation).unwrap_err(),
            original.operation
        );
    }
}

#[tokio::test]
async fn first_wrong_receipt_returns_while_later_sender_remains_owned() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (domain, mut client) = client(sender);
    let writer = tokio::spawn(async move { client.write(&[u128::MAX, 17]).await });
    let mut request = observed(&domain, receiver.recv().await.unwrap());
    let tail = request.pop().unwrap();
    let first = request.pop().unwrap();
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
    assert!(tail.reply.send(tail.operation).is_err());
}

#[tokio::test]
async fn first_closed_receipt_returns_without_waiting_for_the_tail() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (domain, mut client) = client(sender);
    let writer = tokio::spawn(async move { client.write(&[1 << 127, 7]).await });
    let mut request = observed(&domain, receiver.recv().await.unwrap());
    let tail = request.pop().unwrap();
    drop(request);
    let result = tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), "channel closed");
    assert!(tail.reply.send(tail.operation).is_err());
}

#[tokio::test]
async fn public_request_keeps_every_original_and_reports_its_declared_capacity() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (domain, mut client) = client(sender);
    let ids = [u128::MAX, 1 << 127, (1 << 64) | 1, 19];
    let writer = tokio::spawn(async move { client.write(&ids).await });
    let request = observed(&domain, receiver.recv().await.unwrap());
    assert_eq!(request.len(), ids.len());
    assert!(request.capacity() >= ids.len());
    println!("scope=unit_routing_fixture_not_allocator_or_CPU selected_port_originals=17 normal_request_bytes={} original_owner_bytes={} originals={} fixture_collected_capacity={}",size_of::<Request>(),size_of::<Original>(),request.len(),request.capacity());
    for (original, id) in request.into_iter().zip(ids) {
        assert_eq!(original.operation.get(), id);
        original.reply.send(original.operation).unwrap();
    }
    assert_eq!(writer.await.unwrap().unwrap(), ids.len());
}

#[tokio::test]
async fn one_applied_original_cannot_complete_the_request_before_every_original_result() {
    let (sender, mut receiver) = mpsc::channel(1);
    let (domain, mut client) = client(sender);
    let ids = [u128::MAX, 17, 1u128 << 111];
    let mut write = Box::pin(client.write(&ids));
    assert!(futures::poll!(&mut write).is_pending());
    let mut request = observed(&domain, receiver.try_recv().unwrap()).into_iter();
    let first = request.next().unwrap();
    first.reply.send(first.operation).unwrap();
    assert!(
        futures::poll!(&mut write).is_pending(),
        "one result is not batch success"
    );
    drop(request);
    assert_eq!(write.await.unwrap_err().to_string(), "channel closed");
}
