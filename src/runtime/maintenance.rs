// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Bounded, opt-in physical WAL scheduling driven by the host clock.
use super::{MonoTime, NodeError, ReplicaError};
use crate::{contracts::StorageError, worker::*};

/// Physical replacement only: logical retention and checkpoint authorization
/// remain with the existing core/snapshot contracts. No latency guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalMaintenancePolicy {
    /// Delay from configuration or the last successful completion.
    pub interval_ms: u64,
    /// Delay after overload or a safely rejected storage operation.
    pub retry_ms: u64,
    /// Maximum encoded replacement image, not a byte-per-second rate.
    pub max_bytes: usize,
}

/// Latest-only local diagnostics. A new completion replaces the previous one;
/// manual reclaim receipts remain separately owned by the caller.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalMaintenanceStatus {
    pub policy: Option<WalMaintenancePolicy>,
    /// None with an installed policy means stopped (nonretryable error or clock
    /// exhaustion). An in-flight request also has no next deadline.
    pub next_deadline: Option<MonoTime>,
    pub pending: Option<ReclaimTicket>,
    pub last_completion: Option<ReclaimEvent>,
    pub last_admission_error: Option<WorkerError>,
}
impl WalMaintenanceStatus {
    pub(super) fn configure(
        &mut self,
        policy: Option<WalMaintenancePolicy>,
        now: MonoTime,
        ceiling: Option<usize>,
    ) -> Result<(), NodeError> {
        if self.pending.is_some() {
            return Err(NodeError::Replica(ReplicaError::Worker(
                WorkerError::Overloaded,
            )));
        }
        let next = if let Some(p) = policy {
            if p.interval_ms == 0
                || p.retry_ms == 0
                || p.max_bytes == 0
                || p.max_bytes > u32::MAX as usize
            {
                return Err(NodeError::InvalidLimits);
            }
            let ceiling = ceiling.ok_or(NodeError::Replica(ReplicaError::Worker(
                WorkerError::Unsupported,
            )))?;
            if p.max_bytes > ceiling {
                return Err(NodeError::InvalidLimits);
            }
            Some(MonoTime(
                now.0
                    .checked_add(p.interval_ms)
                    .ok_or(NodeError::InvalidLimits)?,
            ))
        } else {
            None
        };
        self.policy = policy;
        self.next_deadline = next;
        self.last_admission_error = None;
        Ok(())
    }
    pub(super) fn due(&self, now: MonoTime) -> Option<WalMaintenancePolicy> {
        if self.pending.is_none() && self.next_deadline.is_some_and(|d| now >= d) {
            self.policy
        } else {
            None
        }
    }
    pub(super) fn submitted(&mut self, ticket: ReclaimTicket) {
        self.pending = Some(ticket);
        self.next_deadline = None;
        self.last_admission_error = None;
    }
    pub(super) fn rejected(&mut self, error: WorkerError, now: MonoTime) {
        self.next_deadline = if error == WorkerError::Overloaded {
            self.after(now, true)
        } else {
            None
        };
        self.last_admission_error = Some(error);
    }
    pub(super) fn completed(&mut self, event: ReclaimEvent, now: MonoTime) {
        let retry = matches!(event.result, Err(StorageError::Rejected(_)));
        self.pending = None;
        self.next_deadline = match event.result {
            Ok(_) => self.after(now, false),
            Err(_) if retry => self.after(now, true),
            Err(_) => None,
        };
        self.last_completion = Some(event);
    }
    fn after(&self, now: MonoTime, retry: bool) -> Option<MonoTime> {
        let p = self.policy?;
        now.0
            .checked_add(if retry { p.retry_ms } else { p.interval_ms })
            .map(MonoTime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deadline_exhaustion_stops_instead_of_wrapping_or_spinning() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy {
            interval_ms: 10,
            retry_ms: 2,
            max_bytes: 100,
        };
        state
            .configure(Some(policy), MonoTime(u64::MAX - 10), Some(100))
            .unwrap();
        assert_eq!(state.next_deadline, Some(MonoTime(u64::MAX)));
        state.rejected(WorkerError::Overloaded, MonoTime(u64::MAX));
        assert_eq!(state.next_deadline, None);
        assert_eq!(state.due(MonoTime(u64::MAX)), None);
        assert!(state
            .configure(Some(policy), MonoTime(u64::MAX), Some(100))
            .is_err());
    }
}
