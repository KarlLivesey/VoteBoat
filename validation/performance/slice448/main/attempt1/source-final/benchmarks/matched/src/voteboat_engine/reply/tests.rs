// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier, Weak,
    },
    task::Wake,
};
use voteboat::identity::OperationId;

#[test]
fn arbitrary_widths_independent_original_values_and_group_lifetime() {
    for width in [0, 1, 4, 5, 17] {
        let mut factory = channels(width);
        assert!(factory.group.is_none());
        let mut pairs: Vec<_> = factory.by_ref().collect();
        assert_eq!(pairs.len(), width);
        assert!(factory.next().is_none());
        if width == 0 {
            assert!(factory.group.is_none());
            continue;
        }
        let weak = Arc::downgrade(factory.group.as_ref().unwrap());
        {
            let group = factory.group.as_ref().unwrap().lock().unwrap();
            assert_eq!(group.overflow.len(), width.saturating_sub(4));
        }
        drop(factory);
        let mut ready = Vec::new();
        for (index, (sender, receiver)) in pairs.drain(..).enumerate().rev() {
            let original = OperationId::new((1u128 << (index % 128)) | (1u128 << 127)).unwrap();
            sender.send(original).unwrap();
            ready.push((original, receiver));
        }
        for (original, receiver) in &mut ready {
            assert_eq!(receiver.try_recv(), Ok(*original));
        }
        // Consumed endpoints intentionally retain the shared allocation until Drop.
        assert!(weak.upgrade().is_some());
        drop(ready);
        assert!(weak.upgrade().is_none());
    }
}

struct Value {
    group: Weak<Mutex<Group<Value>>>,
    drops: Arc<AtomicUsize>,
}

impl Drop for Value {
    fn drop(&mut self) {
        if let Some(group) = self.group.upgrade() {
            assert!(
                group.try_lock().is_ok(),
                "value dropped with reply lock held"
            );
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancellation_closed_send_and_value_drops_are_per_original_and_unlocked() {
    let mut factory = channels(4);
    let (s0, r0) = factory.next().unwrap();
    let (s1, mut r1) = factory.next().unwrap();
    let (s2, r2) = factory.next().unwrap();
    let (s3, mut r3) = factory.next().unwrap();
    let group = Arc::downgrade(factory.group.as_ref().unwrap());
    let drops = Arc::new(AtomicUsize::new(0));
    let value = || Value {
        group: group.clone(),
        drops: drops.clone(),
    };
    drop(r0);
    assert!(s0.is_closed());
    let rejected = s0.send(value()).err().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(rejected);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(s1);
    assert!(matches!(r1.try_recv(), Err(error::TryRecvError::Closed)));
    assert!(s2.send(value()).is_ok());
    drop(r2);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(r3.try_recv().err(), Some(error::TryRecvError::Empty));
    assert!(s3.send(value()).is_ok());
    let received = r3.try_recv().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(received);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn notification_and_sender_close_wake_only_the_registered_original() {
    let mut factory = channels(4);
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    let counts: Vec<_> = (0..4)
        .map(|_| Arc::new(CountWake(AtomicUsize::new(0))))
        .collect();
    for (sender, mut receiver) in factory.by_ref() {
        let waker = Waker::from(counts[receivers.len()].clone());
        assert!(Pin::new(&mut receiver)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        senders.push(Some(sender));
        receivers.push(receiver);
    }
    senders[2].take().unwrap().send(19).unwrap();
    drop(senders[0].take());
    assert_eq!(
        counts
            .iter()
            .map(|w| w.0.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        [1, 0, 1, 0]
    );
    assert_eq!(receivers[2].try_recv(), Ok(19));
    assert_eq!(receivers[0].try_recv(), Err(error::TryRecvError::Closed));
    for index in [1, 3] {
        assert_eq!(receivers[index].try_recv(), Err(error::TryRecvError::Empty));
        senders[index].take().unwrap().send(index).unwrap();
        assert_eq!(receivers[index].try_recv(), Ok(index));
    }
}

#[test]
fn unlocked_registration_gap_rechecks_send_and_close() {
    for send in [false, true] {
        let (sender, mut receiver) = channel();
        let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
        let group = receiver.group.clone();
        let result = receiver.poll_inner(&mut Context::from_waker(&waker), |waker| {
            assert!(group.try_lock().is_ok());
            if send {
                sender.send(17).unwrap();
            } else {
                drop(sender);
            }
            waker.clone()
        });
        assert_eq!(
            result,
            if send {
                Poll::Ready(Ok(17))
            } else {
                Poll::Ready(Err(RecvError))
            }
        );
        assert_eq!(receiver.try_recv(), Err(error::TryRecvError::Closed));
    }
}

struct ReentrantWake {
    group: Weak<Mutex<Group<usize>>>,
    next: Mutex<Option<Sender<usize>>>,
    drops: Arc<AtomicUsize>,
}
impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        assert!(self.group.upgrade().unwrap().try_lock().is_ok());
        if let Some(next) = self.next.lock().unwrap().take() {
            next.send(29).unwrap();
        }
    }
}
impl Drop for ReentrantWake {
    fn drop(&mut self) {
        assert!(self.group.upgrade().unwrap().try_lock().is_ok());
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn replaced_cancelled_and_notified_wakers_reenter_without_reply_lock() {
    let mut factory = channels(4);
    let (s0, mut r0) = factory.next().unwrap();
    let (s1, mut r1) = factory.next().unwrap();
    let (s2, mut r2) = factory.next().unwrap();
    let (_, mut r3) = factory.next().unwrap();
    let group = Arc::downgrade(factory.group.as_ref().unwrap());
    let drops = Arc::new(AtomicUsize::new(0));
    let make_waker = |next| {
        Waker::from(Arc::new(ReentrantWake {
            group: group.clone(),
            next: Mutex::new(next),
            drops: drops.clone(),
        }))
    };
    let waker = make_waker(Some(s1));
    assert!(Pin::new(&mut r0)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    s0.send(11).unwrap();
    assert_eq!(r0.try_recv(), Ok(11));
    assert_eq!(r1.try_recv(), Ok(29));
    let old = make_waker(None);
    assert!(Pin::new(&mut r2)
        .poll(&mut Context::from_waker(&old))
        .is_pending());
    drop(old);
    let new = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
    assert!(Pin::new(&mut r2)
        .poll(&mut Context::from_waker(&new))
        .is_pending());
    let cancelled = make_waker(None);
    // r3's sender was dropped, so use a live registration on r2 before cancellation.
    assert_eq!(r3.try_recv(), Err(error::TryRecvError::Closed));
    assert!(Pin::new(&mut r2)
        .poll(&mut Context::from_waker(&cancelled))
        .is_pending());
    drop(cancelled);
    drop(r2);
    assert!(s2.is_closed());
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

#[test]
fn concurrent_publish_registration_and_receiver_cancellation_keep_one_value_owner() {
    for round in 0..128 {
        let (sender, mut receiver) = channel();
        let start = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let barrier = start.clone();
            let publish = scope.spawn(move || {
                barrier.wait();
                sender.send(round)
            });
            start.wait();
            let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
            if round % 2 == 0 {
                let result = Pin::new(&mut receiver).poll(&mut Context::from_waker(&waker));
                let value = match result {
                    Poll::Ready(result) => result.unwrap(),
                    Poll::Pending => futures::executor::block_on(receiver).unwrap(),
                };
                assert_eq!(value, round);
                assert_eq!(publish.join().unwrap(), Ok(()));
            } else {
                drop(receiver);
                let result = publish.join().unwrap();
                assert!(result == Ok(()) || result == Err(round));
            }
        });
    }
}

#[tokio::test]
async fn cooperative_budget_matches_tokio_for_ready_and_closed_receipts() {
    async fn custom(closed: bool) -> usize {
        tokio::spawn(async move {
            let pairs: Vec<_> = channels(257).collect();
            let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
            let mut completed = 0;
            for (sender, mut receiver) in pairs {
                if closed {
                    drop(sender);
                } else {
                    sender.send(1).unwrap();
                }
                match Pin::new(&mut receiver).poll(&mut Context::from_waker(&waker)) {
                    Poll::Ready(result) => {
                        assert_eq!(result, if closed { Err(RecvError) } else { Ok(1) });
                        completed += 1;
                    }
                    Poll::Pending => {
                        assert_eq!(
                            receiver.try_recv(),
                            if closed {
                                Err(error::TryRecvError::Closed)
                            } else {
                                Ok(1)
                            }
                        );
                        return completed;
                    }
                }
            }
            panic!("cooperative budget did not yield")
        })
        .await
        .unwrap()
    }
    async fn original(closed: bool) -> usize {
        tokio::spawn(async move {
            let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
            let mut completed = 0;
            for _ in 0..257 {
                let (sender, mut receiver) = tokio::sync::oneshot::channel();
                if closed {
                    drop(sender);
                } else {
                    sender.send(1).unwrap();
                }
                if Pin::new(&mut receiver)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
                {
                    return completed;
                }
                completed += 1;
            }
            panic!("Tokio cooperative budget did not yield")
        })
        .await
        .unwrap()
    }
    for closed in [false, true] {
        let actual = custom(closed).await;
        assert!(actual > 0);
        assert_eq!(actual, original(closed).await);
    }
}

#[tokio::test]
async fn pending_restores_budget_and_cancellation_keeps_neighbours_live() {
    tokio::spawn(async {
        let mut factory = channels(3);
        let (s0, mut r0) = factory.next().unwrap();
        let (s1, mut r1) = factory.next().unwrap();
        let (s2, mut r2) = factory.next().unwrap();
        let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
        for _ in 0..1024 {
            assert!(Pin::new(&mut r0)
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            assert!(tokio::task::coop::has_budget_remaining());
        }
        drop(r0);
        assert!(s0.is_closed());
        s1.send(31).unwrap();
        drop(s2);
        assert_eq!(
            Pin::new(&mut r1).poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(31))
        );
        assert_eq!(
            Pin::new(&mut r2).poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Err(RecvError))
        );
    })
    .await
    .unwrap();
}

#[test]
fn actual_dev_reply_group_endpoint_and_client_future_layouts() {
    let (queue, _) = tokio::sync::mpsc::channel(1);
    let client = super::super::Client(queue);
    let future = client.write(&[1, 2, 3, 4]);
    println!("actual_dev Sender={} Receiver={} OptionSender={} Slot={} Group={} MutexGroup={} ArcHandle={} CohortBlock={} Request={} ClientFuture={} scope=actual_current_dev_not_release_or_RSS", size_of::<Sender<OperationId>>(), size_of::<Receiver<OperationId>>(), size_of::<Option<Sender<OperationId>>>(), size_of::<Slot<OperationId>>(), size_of::<Group<OperationId>>(), size_of::<Mutex<Group<OperationId>>>(), size_of::<Arc<Mutex<Group<OperationId>>>>(), size_of::<[Option<Sender<OperationId>>;4]>(), size_of::<super::super::Request>(), std::mem::size_of_val(&future));
    for width in [0, 1, 4, 5, 17] {
        let mut factory = channels::<OperationId>(width);
        assert!(factory.group.is_none());
        if let Some(pair) = factory.next() {
            let group = factory.group.as_ref().unwrap().lock().unwrap();
            println!("width={width} inline_slots={} overflow_len={} overflow_capacity={} issued={} group_strong_handles={}", group.inline.len(), group.overflow.len(), group.overflow.capacity(), factory.next, Arc::strong_count(factory.group.as_ref().unwrap()));
            drop(group);
            drop(pair);
        } else {
            println!("width=0 group_allocated=false");
        }
    }
}
