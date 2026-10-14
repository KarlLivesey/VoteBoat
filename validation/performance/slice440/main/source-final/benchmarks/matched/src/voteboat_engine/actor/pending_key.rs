// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Lossless private reply keys with the same full-width standard hash input.
use std::hash::{Hash, Hasher};
use voteboat::identity::OperationId;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReplyKey([u64; 2]);

impl From<OperationId> for ReplyKey {
    fn from(operation: OperationId) -> Self {
        let value = operation.get();
        Self([value as u64, (value >> 64) as u64])
    }
}

impl Hash for ReplyKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let value = u128::from(self.0[0]) | (u128::from(self.0[1]) << 64);
        value.hash(state);
    }
}

#[cfg(test)]
mod tests;
