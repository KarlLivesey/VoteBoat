// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::collections::{hash_map::Entry, HashMap};
use voteboat::identity::OperationId;

#[cfg(test)]
mod tests;

struct Block<T> {
    slots: [Option<T>; 4],
    occupied: u8,
}

impl<T> Block<T> {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            occupied: 0,
        }
    }
}

/// Actor-owned replies. The cohort and slot together retain every identity bit.
pub(super) struct PendingReplies<T> {
    index: HashMap<u128, usize>,
    blocks: Vec<Block<T>>,
    free: Vec<usize>,
    cached: Option<(u128, usize)>,
    occupied: usize,
}

impl<T> PendingReplies<T> {
    pub fn new(maximum_originals: usize) -> Self {
        Self {
            index: HashMap::with_capacity(maximum_originals),
            // Reserve the worst-case sparse shape; the actor retains its logical gate.
            blocks: Vec::with_capacity(maximum_originals),
            free: Vec::with_capacity(maximum_originals),
            cached: None,
            occupied: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.occupied
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        // Dictionary available capacity counts cohorts, not individual reply slots.
        self.index.capacity()
    }

    pub fn insert(&mut self, operation: OperationId, value: T) -> Option<T> {
        let (key, slot) = location(operation);
        let index = match self.cached {
            Some((cached_key, index)) if cached_key == key => index,
            _ => match self.index.entry(key) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let index = self.free.pop().unwrap_or_else(|| {
                        let index = self.blocks.len();
                        self.blocks.push(Block::new());
                        index
                    });
                    debug_assert_eq!(self.blocks[index].occupied, 0);
                    entry.insert(index);
                    index
                }
            },
        };
        self.cached = Some((key, index));
        let block = &mut self.blocks[index];
        let previous = block.slots[slot].replace(value);
        if previous.is_none() {
            block.occupied |= 1 << slot;
            self.occupied += 1;
        }
        previous
    }

    pub fn remove(&mut self, operation: &OperationId) -> Option<T> {
        let (key, slot) = location(*operation);
        let index = match self.cached {
            Some((cached_key, index)) if cached_key == key => index,
            _ => *self.index.get(&key)?,
        };
        let block = &mut self.blocks[index];
        let value = block.slots[slot].take()?;
        block.occupied &= !(1 << slot);
        self.occupied -= 1;
        self.cached = Some((key, index));
        if block.occupied == 0 {
            self.index.remove(&key);
            self.cached = None;
            self.free.push(index);
        }
        Some(value)
    }
}

fn location(operation: OperationId) -> (u128, usize) {
    let offset = operation.get() - 1;
    (offset >> 2, (offset & 3) as usize)
}
