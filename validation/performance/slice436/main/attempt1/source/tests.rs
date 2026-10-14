// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Client-visible order, every receipt, cancellation and generic-width controls.
use super::*;
use crate::voteboat_engine::{Client, Failure, Original, Request};
use std::mem::{size_of, size_of_val};
use tokio::sync::mpsc;

fn ids(width: usize) -> Vec<u128> {
    (0..width)
        .map(|n| match n % 3 {
            0 => u128::MAX - n as u128,
            1 => (1 << 127) | (n as u128 + 1),
            _ => (1 << 64) | (n as u128 + 1),
        })
        .collect()
}

#[tokio::test]
async fn every_inline_and_heap_receipt_is_checked_in_original_order() {
    for width in [0, 1, 4, 5, 17] {
        let (sender, mut receiver) = mpsc::channel(1);
        let client = Client(sender);
        let originals = ids(width);
        let mut write = Box::pin(client.write(&originals));
        if width == 0 {
            assert!(matches!(
                futures::poll!(&mut write),
                std::task::Poll::Ready(Ok(0))
            ));
            assert!(receiver.try_recv().unwrap().originals.is_empty());
            continue;
        }
        assert!(futures::poll!(&mut write).is_pending());
        let request = receiver.try_recv().unwrap();
        assert_eq!(request.originals.len(), width);
        assert_eq!(
            request
                .originals
                .iter()
                .map(|o| o.operation.get())
                .collect::<Vec<_>>(),
            originals
        );
        let mut queued = request.originals;
        while queued.len() > 1 {
            let original = queued.pop().unwrap();
            original.reply.send(original.operation).unwrap();
            // A later result cannot release the write before its first result.
            assert!(futures::poll!(&mut write).is_pending());
        }
        let first = queued.pop().unwrap();
        first.reply.send(first.operation).unwrap();
        assert_eq!(write.await.unwrap(), width);
    }
}

#[tokio::test]
async fn invalid_inline_and_heap_ids_never_send_and_precede_closed_channel_error() {
    for width in [1, 4, 5, 17] {
        for index in [0, width - 1] {
            for closed in [false, true] {
                let (sender, mut receiver) = mpsc::channel(1);
                if closed {
                    receiver.close();
                }
                let client = Client(sender);
                let mut originals = ids(width);
                originals[index] = 0;
                assert_eq!(
                    client.write(&originals).await.unwrap_err().to_string(),
                    "zero original identity"
                );
                assert!(receiver.try_recv().is_err());
            }
        }
    }
}

#[tokio::test]
async fn empty_and_all_receiver_widths_preserve_closed_send_error() {
    for width in [0, 1, 4, 5, 17] {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let client = Client(sender);
        assert_eq!(
            client.write(&ids(width)).await.unwrap_err().to_string(),
            "channel closed"
        );
    }
}

#[tokio::test]
async fn canceling_full_queue_send_never_publishes_a_partial_request() {
    for width in [0, 1, 4, 5, 17] {
        let (sender, mut receiver) = mpsc::channel(1);
        assert!(sender
            .try_send(Request {
                originals: Vec::new()
            })
            .is_ok());
        let client = Client(sender);
        let originals = ids(width);
        let mut write = Box::pin(client.write(&originals));
        assert!(futures::poll!(&mut write).is_pending());
        drop(write);
        assert!(receiver.try_recv().unwrap().originals.is_empty());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        // Canceling a blocked send has not closed or replaced the client.
        let mut next = Box::pin(client.write(&[u128::MAX]));
        assert!(futures::poll!(&mut next).is_pending());
        let request = receiver.try_recv().unwrap();
        let original = request.originals.into_iter().next().unwrap();
        original.reply.send(original.operation).unwrap();
        assert_eq!(next.await.unwrap(), 1);
    }
}

#[tokio::test]
async fn accepted_inline_and_heap_cancellation_closes_every_receiver_without_losing_request() {
    for width in [1, 4, 5, 17] {
        let (sender, mut receiver) = mpsc::channel(1);
        let client = Client(sender);
        let originals = ids(width);
        let mut write = Box::pin(client.write(&originals));
        assert!(futures::poll!(&mut write).is_pending());
        let request = receiver.try_recv().unwrap();
        drop(write);
        assert_eq!(request.originals.len(), width);
        for (original, expected) in request.originals.into_iter().zip(originals) {
            assert_eq!(original.operation.get(), expected);
            assert!(original.reply.is_closed());
            assert_eq!(
                original.reply.send(original.operation).unwrap_err().get(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn wrong_or_closed_first_and_middle_receipts_cancel_all_unvisited_receivers() {
    for width in [4, 5, 17] {
        for failed in [0, width / 2] {
            for wrong in [false, true] {
                let (sender, mut receiver) = mpsc::channel(1);
                let client = Client(sender);
                let originals = ids(width);
                let mut write = Box::pin(client.write(&originals));
                assert!(futures::poll!(&mut write).is_pending());
                let request = receiver.try_recv().unwrap();
                let mut queued = request.originals.into_iter();
                for original in queued.by_ref().take(failed) {
                    original.reply.send(original.operation).unwrap();
                }
                let original = queued.next().unwrap();
                if wrong {
                    original.reply.send(OperationId::new(7).unwrap()).unwrap();
                } else {
                    drop(original);
                }
                let std::task::Poll::Ready(result) = futures::poll!(&mut write) else {
                    panic!("first error must return without waiting for unvisited tail")
                };
                let error = result.unwrap_err().to_string();
                assert_eq!(
                    error,
                    if wrong {
                        "VoteBoat original applied result differs"
                    } else {
                        "channel closed"
                    }
                );
                for unvisited in queued {
                    assert!(unvisited.reply.is_closed());
                }
            }
        }
    }
}

#[tokio::test]
async fn queued_wrong_tail_cannot_bypass_the_original_first_wait() {
    for width in [4, 5, 17] {
        let (sender, mut receiver) = mpsc::channel(1);
        let client = Client(sender);
        let originals = ids(width);
        let mut write = Box::pin(client.write(&originals));
        assert!(futures::poll!(&mut write).is_pending());
        let request = receiver.try_recv().unwrap();
        let mut queued = request.originals.into_iter();
        let first = queued.next().unwrap();
        let second = queued.next().unwrap();
        second.reply.send(OperationId::new(7).unwrap()).unwrap();
        assert!(futures::poll!(&mut write).is_pending());
        first.reply.send(first.operation).unwrap();
        let std::task::Poll::Ready(result) = futures::poll!(&mut write) else {
            panic!("wrong result must return after the preceding original completes")
        };
        assert_eq!(
            result.unwrap_err().to_string(),
            "VoteBoat original applied result differs"
        );
        for unvisited in queued {
            assert!(unvisited.reply.is_closed());
        }
    }
}

// Literal435 source behavior, used only to report a diagnostic future layout.
// This replica is not the published435 executable or a performance baseline.
async fn diagnostic435_write(client: &Client, originals: &[u128]) -> Result<usize, Failure> {
    let mut entries = Vec::with_capacity(originals.len());
    let mut waits = Vec::with_capacity(originals.len());
    for &original in originals {
        let operation = OperationId::new(original).ok_or("zero original identity")?;
        let (reply, wait) = oneshot::channel();
        entries.push(Original {
            operation,
            bytes: Vec::new(),
            reply,
        });
        waits.push((operation, wait));
    }
    client.0.send(Request { originals: entries }).await?;
    for (operation, wait) in waits {
        if wait.await? != operation {
            return Err("VoteBoat original applied result differs".into());
        }
    }
    Ok(originals.len())
}

#[test]
fn observed_wait_and_future_layouts_are_reported_without_portable_size_assumptions() {
    let (sender, _receiver) = mpsc::channel(1);
    let client = Client(sender);
    for width in [0, 1, 4, 5, 17] {
        let originals = ids(width);
        let waits = Waits::new(width);
        let (storage, heap_capacity) = match &waits {
            Waits::Inline(_) => ("inline", 0),
            Waits::Heap(slots) => ("heap", slots.capacity()),
        };
        println!("width={width} storage={storage} receiver_bytes={} optional_receiver_bytes={} old_tuple_bytes={} old_wait_vec_header_bytes={} wait_container_bytes={} candidate_future_bytes={} diagnostic435_replica_future_bytes={} heap_available_capacity={heap_capacity}", size_of::<Receiver>(), size_of::<Option<Receiver>>(), size_of::<(OperationId, Receiver)>(), size_of::<Vec<(OperationId, Receiver)>>(), size_of::<Waits>(), size_of_val(&client.write(&originals)), size_of_val(&diagnostic435_write(&client, &originals)));
    }
}
