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
//! Authenticated physical channels carrying child-authorized logical envelopes.
use super::*;
use crate::{
    outbound::*,
    raft::Message,
    runtime::*,
    secure::*,
    transport::{TransportPollBudget, TransportProgress, TransportState},
};
use std::{collections::VecDeque, mem::size_of};
pub const LOGICAL_NETWORK_VERSION: u16 = 0x8001;
mod frame;
mod plan;
mod poll;
mod queue;
mod session;
pub use frame::{LogicalFrameCodec, LogicalReceived, LogicalReceivedRejected};
pub use plan::*;
pub use queue::*;
pub use session::*;
#[derive(Clone, Copy, Debug)]
pub struct LogicalNetworkLimits {
    pub frame_bytes: usize,
    pub decoded_bytes: usize,
}
impl Default for LogicalNetworkLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        }
    }
}
impl LogicalNetworkLimits {
    pub fn validate(self) -> Result<Self, LogicalNetworkError> {
        if self.frame_bytes < 1024
            || self.frame_bytes > 64 * 1024 * 1024
            || self.decoded_bytes < 1024
            || self.decoded_bytes > 256 * 1024 * 1024
        {
            return Err(LogicalNetworkError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalNetworkError {
    InvalidLimits,
    WrongBinding,
    WrongPeer,
    StaleOwner,
    InvalidFrame,
    TooLarge,
    Closed,
    Truncated,
    TimeWentBack,
    ProviderViolation,
    Session(SessionError),
    Outbound(OutboundError),
}
