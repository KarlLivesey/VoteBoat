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
//! Independent downstream budget with one fixed diagnostic sample.
use std::sync::{Arc, Mutex};
use voteboat::{admission::*, outbound::*};
struct State {
    max: OutboundUsage,
    usage: OutboundUsage,
    calls: usize,
    last: Option<AdmissionRequest>,
}
#[derive(Clone)]
pub struct HostPolicy {
    state: Arc<Mutex<State>>,
    closed: bool,
}
impl HostPolicy {
    pub fn new(max: OutboundUsage) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                max,
                usage: OutboundUsage::default(),
                calls: 0,
                last: None,
            })),
            closed: false,
        }
    }
    pub fn usage(&self) -> OutboundUsage {
        self.state.lock().unwrap().usage
    }
    pub fn calls(&self) -> usize {
        self.state.lock().unwrap().calls
    }
    pub fn last(&self) -> AdmissionRequest {
        self.state.lock().unwrap().last.unwrap()
    }
}
struct Token {
    state: Arc<Mutex<State>>,
    cost: OutboundUsage,
}
impl Drop for Token {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.usage.batches -= self.cost.batches;
        state.usage.messages -= self.cost.messages;
        state.usage.bytes -= self.cost.bytes;
    }
}
impl AdmissionPolicy for HostPolicy {
    fn reserve(&self, request: AdmissionRequest) -> Result<AdmissionLease, AdmissionError> {
        if self.closed {
            return Err(AdmissionError::Closed);
        }
        let mut state = self
            .state
            .try_lock()
            .map_err(|_| AdmissionError::Overloaded)?;
        state.calls += 1;
        state.last = Some(request);
        if request.class == MessageClass::Control {
            return Err(AdmissionError::InvalidRequest);
        }
        if !state.usage.fits(request.cost, state.max) {
            return Err(AdmissionError::Overloaded);
        }
        state.usage.batches += request.cost.batches;
        state.usage.messages += request.cost.messages;
        state.usage.bytes += request.cost.bytes;
        Ok(AdmissionLease::new(Token {
            state: self.state.clone(),
            cost: request.cost,
        }))
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
