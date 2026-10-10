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
use super::{EffectTicket, MonoTime, ReplicaError};

pub(super) struct SnapshotSendRetries {
    pending: Vec<(EffectTicket, MonoTime)>,
    capacity: usize,
    delay_ms: u64,
}
impl SnapshotSendRetries {
    pub(super) fn new(capacity: usize, delay_ms: u64) -> Self {
        Self {
            pending: Vec::with_capacity(capacity),
            capacity,
            delay_ms,
        }
    }
    pub(super) fn expired(
        &mut self,
        ticket: EffectTicket,
        now: MonoTime,
    ) -> Result<bool, ReplicaError> {
        let since = if let Some((_, since)) = self.pending.iter().find(|(t, _)| *t == ticket) {
            *since
        } else {
            if self.pending.len() == self.capacity {
                return Err(ReplicaError::ProviderContract);
            }
            self.pending.push((ticket, now));
            now
        };
        Ok(now.0.saturating_sub(since.0) >= self.delay_ms)
    }
    pub(super) fn complete(&mut self, ticket: EffectTicket) {
        if let Some(i) = self.pending.iter().position(|(t, _)| *t == ticket) {
            self.pending.swap_remove(i);
        }
    }
    pub(super) fn clear(&mut self) {
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        identity::*,
        runtime::{RuntimeOwner, VisitTicket},
    };
    fn ticket(sequence: u64) -> EffectTicket {
        EffectTicket {
            visit: VisitTicket {
                owner: RuntimeOwner {
                    store: StoreBinding {
                        identity: StoreIdentity {
                            id: StoreId::new(1).unwrap(),
                            incarnation: StoreIncarnation::new(1).unwrap(),
                        },
                        session: StoreSession::new(1).unwrap(),
                    },
                    lane: ExecutionLaneId::new(1).unwrap(),
                    generation: RuntimeGeneration::new(1).unwrap(),
                },
                group: GroupIdentity {
                    id: GroupId::new(1).unwrap(),
                    incarnation: GroupIncarnation::new(1).unwrap(),
                },
                sequence: 1,
            },
            sequence,
        }
    }
    #[test]
    fn exact_retry_window_capacity_and_cleanup() {
        let mut retries = SnapshotSendRetries::new(1, 50);
        assert!(!retries.expired(ticket(1), MonoTime(10)).unwrap());
        assert!(!retries.expired(ticket(1), MonoTime(59)).unwrap());
        assert_eq!(
            retries.expired(ticket(2), MonoTime(59)),
            Err(ReplicaError::ProviderContract)
        );
        assert!(retries.expired(ticket(1), MonoTime(60)).unwrap());
        retries.complete(ticket(2));
        assert_eq!(retries.pending.len(), 1);
        retries.complete(ticket(1));
        assert!(!retries.expired(ticket(2), MonoTime(60)).unwrap());
        retries.clear();
        assert!(retries.pending.is_empty());
    }
    #[test]
    fn zero_delay_and_clock_exhaustion_do_not_wrap() {
        let mut retries = SnapshotSendRetries::new(1, 0);
        assert!(retries.expired(ticket(1), MonoTime(u64::MAX)).unwrap());
        let mut retries = SnapshotSendRetries::new(1, 50);
        assert!(!retries.expired(ticket(1), MonoTime(u64::MAX - 50)).unwrap());
        assert!(!retries.expired(ticket(1), MonoTime(u64::MAX - 1)).unwrap());
        assert!(retries.expired(ticket(1), MonoTime(u64::MAX)).unwrap());
    }
}
