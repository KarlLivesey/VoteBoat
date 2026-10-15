// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::super::{Client, Request};
use super::*;
use tokio::sync::mpsc;

fn ids(width: usize) -> Vec<u128> {
    (0..width).map(|n| u128::MAX - n as u128).collect()
}
async fn success(client: &mut Client, receiver: &mut mpsc::Receiver<Request>, originals: &[u128]) {
    let mut write = Box::pin(client.write(originals));
    if originals.is_empty() {
        assert_eq!(write.await.unwrap(), 0);
        assert!(receiver.try_recv().unwrap().originals.is_empty());
        return;
    }
    assert!(futures::poll!(&mut write).is_pending());
    let request = receiver.try_recv().unwrap();
    assert_eq!(request.originals.len(), originals.len());
    for (original, &expected) in request.originals.into_iter().zip(originals) {
        assert_eq!(original.operation.get(), expected);
        original.reply.send(original.operation).unwrap();
    }
    assert_eq!(write.await.unwrap(), originals.len());
}
fn empty(client: &Client) {
    assert!(client.waits.waits.is_empty());
    assert!(client.waits.waits.capacity() <= client.waits.retain_limit);
}

#[tokio::test]
async fn storage_is_cold_then_reused_and_clone_starts_independently_empty() {
    for limit in [1, 4, 5, 17] {
        let (sender, mut receiver) = mpsc::channel(2);
        let mut client = Client::new(sender, limit);
        assert_eq!(client.waits.waits.capacity(), 0);
        success(&mut client, &mut receiver, &ids(limit)).await;
        empty(&client);
        let capacity = client.waits.waits.capacity();
        let pointer = client.waits.waits.as_ptr();
        assert!(capacity >= limit);
        let mut cloned = client.clone();
        assert_eq!(cloned.waits.retain_limit, limit);
        assert_eq!(cloned.waits.waits.capacity(), 0);
        success(&mut client, &mut receiver, &ids(limit)).await;
        assert_eq!(client.waits.waits.as_ptr(), pointer);
        assert_eq!(client.waits.waits.capacity(), capacity);
        success(&mut cloned, &mut receiver, &ids(limit)).await;
        empty(&cloned);
        assert_ne!(cloned.waits.waits.as_ptr(), pointer);
        assert_eq!(client.waits.waits.as_ptr(), pointer);
        let originals = ids(limit);
        let other: Vec<_> = originals.iter().map(|id| id - 32).collect();
        let mut first_write = Box::pin(client.write(&originals));
        let mut other_write = Box::pin(cloned.write(&other));
        assert!(futures::poll!(&mut first_write).is_pending());
        assert!(futures::poll!(&mut other_write).is_pending());
        let first_request = receiver.try_recv().unwrap();
        let other_request = receiver.try_recv().unwrap();
        for original in other_request.originals {
            original.reply.send(original.operation).unwrap();
        }
        assert_eq!(other_write.await.unwrap(), limit);
        assert!(futures::poll!(&mut first_write).is_pending());
        for original in first_request.originals {
            original.reply.send(original.operation).unwrap();
        }
        assert_eq!(first_write.await.unwrap(), limit);
        empty(&client);
        empty(&cloned);
        success(&mut client, &mut receiver, &[]).await;
        assert_eq!(client.waits.waits.as_ptr(), pointer);
    }
}

#[tokio::test]
async fn validation_and_receipt_errors_close_tails_and_reuse_empty_storage() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut client = Client::new(sender, 5);
    success(&mut client, &mut receiver, &ids(5)).await;
    let pointer = client.waits.waits.as_ptr();
    assert_eq!(
        client
            .write(&[u128::MAX, 0, 7])
            .await
            .unwrap_err()
            .to_string(),
        "zero original identity"
    );
    assert!(receiver.try_recv().is_err());
    empty(&client);
    assert_eq!(client.waits.waits.as_ptr(), pointer);
    for wrong in [false, true] {
        let originals = ids(5);
        let mut write = Box::pin(client.write(&originals));
        assert!(futures::poll!(&mut write).is_pending());
        let mut queued = receiver.try_recv().unwrap().originals.into_iter();
        let first = queued.next().unwrap();
        if wrong {
            first
                .reply
                .send(OperationId::new(first.operation.get() ^ (1 << 96)).unwrap())
                .unwrap();
        } else {
            drop(first);
        }
        let error = write.await.unwrap_err().to_string();
        assert_eq!(
            error,
            if wrong {
                "VoteBoat original applied result differs"
            } else {
                "channel closed"
            }
        );
        for tail in queued {
            assert!(tail.reply.is_closed());
        }
        empty(&client);
        assert_eq!(client.waits.waits.as_ptr(), pointer);
        success(&mut client, &mut receiver, &originals).await;
    }
    receiver.close();
    assert_eq!(
        client.write(&ids(5)).await.unwrap_err().to_string(),
        "channel closed"
    );
    empty(&client);
    assert_eq!(client.waits.waits.as_ptr(), pointer);
}

#[tokio::test]
async fn canceled_send_and_accepted_wait_return_empty_reusable_storage() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut client = Client::new(sender, 4);
    success(&mut client, &mut receiver, &ids(4)).await;
    let pointer = client.waits.waits.as_ptr();
    assert!(client
        .sender
        .try_send(Request {
            originals: Vec::new()
        })
        .is_ok());
    let originals = ids(4);
    let mut write = Box::pin(client.write(&originals));
    assert!(futures::poll!(&mut write).is_pending());
    drop(write);
    empty(&client);
    assert_eq!(client.waits.waits.as_ptr(), pointer);
    assert!(receiver.try_recv().unwrap().originals.is_empty());
    assert!(receiver.try_recv().is_err());
    success(&mut client, &mut receiver, &originals).await;
    let mut write = Box::pin(client.write(&originals));
    assert!(futures::poll!(&mut write).is_pending());
    let request = receiver.try_recv().unwrap();
    drop(write);
    for original in request.originals {
        assert!(original.reply.is_closed());
    }
    empty(&client);
    assert_eq!(client.waits.waits.as_ptr(), pointer);
    success(&mut client, &mut receiver, &originals).await;
}

#[tokio::test]
async fn oversized_requests_remain_supported_but_leave_no_storage_or_endpoints() {
    for limit in [0, 1, 4, 5] {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut client = Client::new(sender, limit);
        for width in [limit + 1, 17] {
            success(&mut client, &mut receiver, &ids(width)).await;
            empty(&client);
            assert_eq!(client.waits.waits.capacity(), 0);
            let originals = ids(width);
            let mut write = Box::pin(client.write(&originals));
            assert!(futures::poll!(&mut write).is_pending());
            let request = receiver.try_recv().unwrap();
            drop(write);
            for original in request.originals {
                assert!(original.reply.is_closed());
            }
            empty(&client);
            assert_eq!(client.waits.waits.capacity(), 0);
        }
        success(&mut client, &mut receiver, &ids(limit)).await;
        empty(&client);
    }
}

#[test]
fn unwind_closes_every_endpoint_and_discards_oversized_backing() {
    let mut cache = WaitCache::new(1);
    let (reply, wait) = oneshot::channel();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut lease = cache.lease(5);
        lease.push(wait);
        panic!("test-only interruption with an active lease");
    }));
    assert!(panic.is_err());
    assert!(reply.is_closed());
    assert!(cache.waits.is_empty());
    assert_eq!(cache.waits.capacity(), 0);
}

#[test]
fn actual_current_test_executable_client_cache_lease_and_write_future_layouts() {
    let (sender, _receiver) = mpsc::channel(1);
    let mut client = Client::new(sender, 4);
    let future = client.write(&[]);
    println!("scope=actual_current_test_executable_see_recorded_profile_unpolled_not_RSS Client={} WaitCache={} Lease={} Receiver={} ReceiverVec={} write_future={}",
        std::mem::size_of::<Client>(), std::mem::size_of::<WaitCache>(),
        std::mem::size_of::<Lease<'_>>(), std::mem::size_of::<Wait>(),
        std::mem::size_of::<Vec<Wait>>(), std::mem::size_of_val(&future));
}
