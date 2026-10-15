// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact temporary Counter reservations for one immutable ready prefix.
use super::{ApplicationError, Counter, CounterReceipt, ReadyAdmission};
use crate::identity::OperationId;
use std::collections::BTreeSet;

pub(super) fn prepare<'app, 'pending>(
    application: &'app Counter,
    pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
) -> ReadyAdmission<'app> {
    let remaining = application.remaining_operations();
    let mut reserved = BTreeSet::new();
    let mut pending_error = None;
    for (operation, _) in pending {
        if !application.dedup.contains_key(&operation) {
            reserved.insert(operation);
        }
        if reserved.len() > remaining {
            pending_error = Some(ApplicationError::DedupCapacity);
            break;
        }
    }
    Box::new(move |operation, bytes| {
        // The legacy validator checks syntax before traversing pending. Defer
        // any preparation failure until after this candidate's syntax check.
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(error) = pending_error {
            return Err(error);
        }
        if !application.dedup.contains_key(&operation)
            && reserved.insert(operation)
            && reserved.len() > remaining
        {
            // Only successful predecessors contribute to later reservations.
            reserved.remove(&operation);
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(std::mem::size_of::<CounterReceipt>())
    })
}
