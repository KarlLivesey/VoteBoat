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
//! Additional bulk-work admission; mandatory queue limits remain authoritative.
use crate::{
    identity::NodeId,
    outbound::{MessageClass, OutboundBinding, OutboundUsage},
};
use std::{fmt, sync::Arc};
pub const ADMISSION_CONTRACT_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    InvalidLimits,
    InvalidRequest,
    Overloaded,
    Closed,
}
#[derive(Clone, Copy, Debug)]
pub struct AdmissionRequest {
    pub owner: OutboundBinding,
    pub peer: NodeId,
    pub class: MessageClass,
    pub cost: OutboundUsage,
    /// Includes queued and dispatched, but not this prospective reservation.
    pub node_usage: OutboundUsage,
    pub peer_usage: OutboundUsage,
}
trait Held: Send + Sync {}
impl<T: Send + Sync> Held for T {}
/// Opaque owned provider reservation. Clone extends the same reservation's
/// lifetime; it never acquires duplicate credits. The provider token is dropped
/// only after all owners release it. Never strip this lease from accepted work.
/// This is volatile resource accounting, not a durability or delivery receipt.
#[derive(Clone)]
pub struct AdmissionLease {
    _held: Arc<dyn Held>,
}
impl AdmissionLease {
    pub fn new<T: Send + Sync + 'static>(provider_token: T) -> Self {
        Self {
            _held: Arc::new(provider_token),
        }
    }
}
impl fmt::Debug for AdmissionLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdmissionLease { opaque }")
    }
}
/// Bounded nonblocking reserve before ownership transfer. Refusal retains no
/// credits. Successful tokens own all provider credits until their last Drop,
/// even after this view closes/drops. Shared providers scope close to the view.
/// Native queues invoke this only for data/background after mandatory checks;
/// control bypasses optional policy. Policy cannot relax core ceilings or change
/// consensus state. Arbitrary host callbacks remain trusted to obey these bounds.
pub trait AdmissionPolicy {
    fn reserve(&self, request: AdmissionRequest) -> Result<AdmissionLease, AdmissionError>;
    fn close(&mut self);
}
