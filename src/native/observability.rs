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
//! Fixed-size native counters, selected explicitly by the host.
use crate::{observability::*, runtime::RuntimeOwner};

pub struct NativeCounterObserver {
    snapshot: CounterSnapshot,
}
impl NativeCounterObserver {
    pub fn new(owner: RuntimeOwner) -> Self {
        Self {
            snapshot: CounterSnapshot {
                owner,
                sampled_at: None,
                state: None,
                counters: NodeCounters::default(),
                closed: false,
            },
        }
    }
}
impl Observer for NativeCounterObserver {
    fn record_bounded(&mut self, sample: NodeObservation) -> Result<(), ObservationError> {
        if self.snapshot.closed {
            return Err(ObservationError::Closed);
        }
        if sample.owner != self.snapshot.owner {
            return Err(ObservationError::WrongOwner);
        }
        if self
            .snapshot
            .sampled_at
            .is_some_and(|t| sample.sampled_at < t)
        {
            return Err(ObservationError::ClockRegressed);
        }
        self.snapshot.counters = self.snapshot.counters.saturating_add(sample.counters);
        self.snapshot.sampled_at = Some(sample.sampled_at);
        self.snapshot.state = Some(sample.state);
        Ok(())
    }
    fn snapshot_counters(&self) -> CounterSnapshot {
        self.snapshot
    }
    fn close(&mut self) {
        self.snapshot.closed = true;
    }
}
