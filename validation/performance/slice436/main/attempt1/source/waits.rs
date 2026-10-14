// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Request-local receiver ownership; every original keeps its own channel.
use super::{oneshot, OperationId};

type Receiver = oneshot::Receiver<OperationId>;

pub(super) enum Waits {
    Inline([Option<Receiver>; 4]),
    Heap(Vec<Option<Receiver>>),
}

impl Waits {
    pub(super) fn new(width: usize) -> Self {
        if width <= 4 {
            Self::Inline(std::array::from_fn(|_| None))
        } else {
            Self::Heap(Vec::with_capacity(width))
        }
    }

    /// Index comes from the single original-validation/channel-creation loop.
    pub(super) fn push(&mut self, index: usize, receiver: Receiver) {
        match self {
            Self::Inline(slots) => slots[index] = Some(receiver),
            Self::Heap(slots) => slots.push(Some(receiver)),
        }
    }

    /// The client zips these slots with its immutable original input. Each
    /// initialized receiver is taken once; untouched slots drop on any exit.
    pub(super) fn slots(&mut self) -> &mut [Option<Receiver>] {
        match self {
            Self::Inline(slots) => slots,
            Self::Heap(slots) => slots,
        }
    }
}

#[cfg(test)]
mod tests;
