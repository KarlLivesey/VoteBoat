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
//! Mandatory child-authorized publication of original logical parent outputs.
use super::*;
use crate::{
    application::*, log::LogEntry, logical_voter::*, outbound::*, worker::PersistenceWorker,
};
mod capture;
mod control;
mod publication;
#[cfg(test)]
mod tests;
mod types;
use types::*;
pub use types::{
    AuthorizedLogicalSend, LogicalOutboundQueue, LogicalOutboundRejected, LogicalReleaseError,
    LogicalReleaseLimits, LogicalReleaseUsage, LogicalResponseCodec, LogicalSendProvenance,
    LOGICAL_RESPONSE_CODEC_CONTRACT_VERSION,
};
pub(crate) use types::{
    LogicalCaptureError, LogicalCommittedRejected, LogicalReleaseRequest, LogicalReleaseTicket,
};

/// Captures retain original typed values and router credits. Public output queues
/// receive success only after the child bridge consumes the exact capsule outcome.
pub struct LogicalReleaseCoordinator<R, V> {
    authorization: RuntimeAuthorization,
    control_profile: Option<LogicalActivationControlProfile>,
    limits: LogicalReleaseLimits,
    pending: BTreeMap<u64, Pending<R, V>>,
    requests: VecDeque<LogicalReleaseRequest>,
    sequence: u64,
    usage: LogicalReleaseUsage,
    closed: bool,
    failed_domain: bool,
}
impl<R, V> LogicalReleaseCoordinator<R, V> {
    pub fn new(
        authorization: RuntimeAuthorization,
        limits: LogicalReleaseLimits,
    ) -> Result<Self, LogicalReleaseError> {
        limits.validate()?;
        if !authorization.is_logical() {
            return Err(LogicalReleaseError::WrongBinding);
        }
        Ok(Self {
            authorization,
            control_profile: None,
            limits,
            pending: BTreeMap::new(),
            requests: VecDeque::new(),
            sequence: 0,
            usage: LogicalReleaseUsage::default(),
            closed: false,
            failed_domain: false,
        })
    }
    pub fn authorization(&self) -> RuntimeAuthorization {
        self.authorization
    }
    pub fn usage(&self) -> LogicalReleaseUsage {
        self.usage
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty() && self.requests.is_empty()
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn accepts_work(&self) -> bool {
        !self.closed && !self.failed_domain
    }
    pub(crate) fn front_request(&self) -> Option<&LogicalReleaseRequest> {
        self.requests.front()
    }
    pub(crate) fn mark_submitted(
        &mut self,
        ticket: LogicalReleaseTicket,
    ) -> Result<(), LogicalReleaseError> {
        if ticket.authorization != self.authorization
            || self.requests.front().is_none_or(|r| r.ticket != ticket)
        {
            return Err(LogicalReleaseError::StalePermit);
        }
        let expected = self
            .pending
            .get_mut(&ticket.sequence)
            .and_then(|p| p.capsules.get_mut(ticket.capsule))
            .ok_or(LogicalReleaseError::StalePermit)?;
        if expected.submitted {
            return Err(LogicalReleaseError::StalePermit);
        }
        expected.submitted = true;
        self.requests.pop_front();
        Ok(())
    }
    pub(crate) fn accept_committed(
        &mut self,
        ticket: LogicalReleaseTicket,
        observation: crate::logical_voter::CommittedLogicalAction,
    ) -> Result<(), LogicalCommittedRejected> {
        let valid = ticket.authorization == self.authorization
            && self
                .pending
                .get(&ticket.sequence)
                .and_then(|p| p.capsules.get(ticket.capsule))
                .is_some_and(|expected| {
                    expected.submitted
                        && !expected.settled
                        && observation.capsule() == Some(&expected.capsule)
                });
        if !valid {
            return Err(LogicalCommittedRejected {
                reason: LogicalReleaseError::StalePermit,
                observation: Box::new(observation),
            });
        }
        if self.failed_domain {
            let logical = self.authorization.logical().unwrap();
            if observation.mapping() != logical.mapping
                || observation.representative() != logical.representative
                || !matches!(
                    observation.receipt().outcome,
                    LogicalOutcome::CapsuleAuthorized | LogicalOutcome::Refused(_)
                )
            {
                return Err(LogicalCommittedRejected {
                    reason: LogicalReleaseError::StalePermit,
                    observation: Box::new(observation),
                });
            }
            // Exact consumed terminal truth settles accepted child ownership;
            // semantic refusal creates no release authority in failed cleanup.
            drop(observation);
        } else {
            let permit = ReleasePermit::from_committed(self.authorization, observation)?;
            self.pending.get_mut(&ticket.sequence).unwrap().capsules[ticket.capsule].permit =
                Some(permit);
        }
        self.pending.get_mut(&ticket.sequence).unwrap().capsules[ticket.capsule].settled = true;
        Ok(())
    }
}
