// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{ready, Context, Poll, Waker},
};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) mod error {
    pub use tokio::sync::oneshot::error::TryRecvError;
}

struct Slot<T> {
    value: Option<T>,
    sender_open: bool,
    receiver_open: bool,
    waker: Option<Waker>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            value: None,
            sender_open: true,
            receiver_open: true,
            waker: None,
        }
    }

    fn receive(&mut self) -> Poll<Result<T, RecvError>> {
        if let Some(value) = self.value.take() {
            self.receiver_open = false;
            Poll::Ready(Ok(value))
        } else if !self.sender_open {
            self.receiver_open = false;
            Poll::Ready(Err(RecvError))
        } else {
            Poll::Pending
        }
    }
}

struct Group<T> {
    inline: [Slot<T>; 4],
    overflow: Vec<Slot<T>>,
}

impl<T> Group<T> {
    fn new(width: usize) -> Self {
        Self {
            inline: std::array::from_fn(|_| Slot::new()),
            overflow: (4..width).map(|_| Slot::new()).collect(),
        }
    }

    fn slot(&mut self, index: NonZeroUsize) -> &mut Slot<T> {
        let index = index.get() - 1;
        if index < 4 {
            &mut self.inline[index]
        } else {
            &mut self.overflow[index - 4]
        }
    }
}

pub(super) struct Channels<T> {
    group: Option<Arc<Mutex<Group<T>>>>,
    width: usize,
    next: usize,
}

pub(super) fn channels<T>(width: usize) -> Channels<T> {
    Channels {
        group: None,
        width,
        next: 0,
    }
}

impl<T> Iterator for Channels<T> {
    type Item = (Sender<T>, Receiver<T>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.width {
            return None;
        }
        let group = self
            .group
            .get_or_insert_with(|| Arc::new(Mutex::new(Group::new(self.width))));
        self.next += 1;
        let index = NonZeroUsize::new(self.next);
        Some((
            Sender {
                group: group.clone(),
                index,
            },
            Receiver {
                group: group.clone(),
                index,
            },
        ))
    }
}

#[cfg(test)]
pub(super) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    channels(1).next().expect("one channel")
}

pub(super) struct Sender<T> {
    group: Arc<Mutex<Group<T>>>,
    index: Option<NonZeroUsize>,
}

impl<T> Sender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        let index = self.index.take().expect("unconsumed sender");
        let (result, waker) = {
            let mut group = self.group.lock().expect("reply group poisoned");
            let slot = group.slot(index);
            slot.sender_open = false;
            let result = if slot.receiver_open {
                slot.value = Some(value);
                Ok(())
            } else {
                Err(value)
            };
            (result, slot.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        result
    }

    #[cfg(test)]
    pub fn is_closed(&self) -> bool {
        let mut group = self.group.lock().expect("reply group poisoned");
        !group
            .slot(self.index.expect("unconsumed sender"))
            .receiver_open
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            let waker = {
                let mut group = self.group.lock().expect("reply group poisoned");
                let slot = group.slot(index);
                slot.sender_open = false;
                slot.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel closed")
    }
}

impl std::error::Error for RecvError {}

pub(super) struct Receiver<T> {
    group: Arc<Mutex<Group<T>>>,
    index: Option<NonZeroUsize>,
}

impl<T> Receiver<T> {
    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<T, error::TryRecvError> {
        let Some(index) = self.index else {
            return Err(error::TryRecvError::Closed);
        };
        let (result, waker) = {
            let mut group = self.group.lock().expect("reply group poisoned");
            let slot = group.slot(index);
            let result = slot.receive();
            let waker = result.is_ready().then(|| slot.waker.take());
            (result, waker)
        };
        drop(waker);
        match result {
            Poll::Ready(result) => {
                self.index = None;
                result.map_err(|_| error::TryRecvError::Closed)
            }
            Poll::Pending => Err(error::TryRecvError::Empty),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_inner(cx, Waker::clone)
    }
}

impl<T> Receiver<T> {
    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
        clone_waker: impl FnOnce(&Waker) -> Waker,
    ) -> Poll<Result<T, RecvError>> {
        let index = self.index.expect("called after complete");
        let coop = ready!(tokio::task::coop::poll_proceed(cx));
        let (result, old_waker, registered) = {
            let mut group = self.group.lock().expect("reply group poisoned");
            let slot = group.slot(index);
            let result = slot.receive();
            let registered = slot.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker()));
            let old_waker = result.is_ready().then(|| slot.waker.take());
            (result, old_waker, registered)
        };
        drop(old_waker);
        if result.is_ready() {
            self.index = None;
            coop.made_progress();
            return result;
        }
        if registered {
            return Poll::Pending;
        }
        // Arbitrary RawWaker clone callbacks run unlocked. Recheck after this gap.
        let mut candidate = Some(clone_waker(cx.waker()));
        let (result, old_waker) = {
            let mut group = self.group.lock().expect("reply group poisoned");
            let slot = group.slot(index);
            let result = slot.receive();
            let old_waker = if result.is_ready() {
                slot.waker.take()
            } else {
                std::mem::replace(&mut slot.waker, candidate.take())
            };
            (result, old_waker)
        };
        drop(old_waker);
        drop(candidate);
        if result.is_ready() {
            self.index = None;
            coop.made_progress();
        }
        result
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            let (value, waker) = {
                let mut group = self.group.lock().expect("reply group poisoned");
                let slot = group.slot(index);
                slot.receiver_open = false;
                (slot.value.take(), slot.waker.take())
            };
            drop(value);
            drop(waker);
        }
    }
}
