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
//! Private whole-capture reservation; classification grants no protocol authority.
use super::*;
impl<R: ApplicationReceipt, V> LogicalReleaseCoordinator<R, V> {
    pub(crate) fn new_for_results(
        authorization: RuntimeAuthorization,
        limits: LogicalReleaseLimits,
        results: &ApplicationRouter<R>,
    ) -> Result<Self, LogicalReleaseError> {
        if results.binding().authorization() != authorization {
            return Err(LogicalReleaseError::WrongBinding);
        }
        let release = Self::new(authorization, limits)?;
        let count = results
            .logical_batch_reservation()
            .ok_or(LogicalReleaseError::WrongBinding)?;
        release.check_static_reservation(count)?;
        match results.activation_reservation() {
            Some((profile, count)) => release.select_activation(profile.clone(), count),
            None => Ok(release),
        }
    }
    pub(super) fn check_static_reservation(&self, count: usize) -> Result<(), LogicalReleaseError> {
        let invalid = LogicalReleaseError::InvalidLimits;
        let ordinary = self.capture_charge(count).map_err(|_| invalid.clone())?;
        let control = self.capture_charge(1).map_err(|_| invalid.clone())?;
        if count == 0
            || control > self.limits.control_bytes
            || self
                .limits
                .bytes
                .checked_sub(self.limits.control_bytes)
                .is_none_or(|n| n < ordinary)
        {
            return Err(invalid);
        }
        Ok(())
    }
    pub(super) fn select_activation(
        mut self,
        profile: LogicalActivationControlProfile,
        count: usize,
    ) -> Result<Self, LogicalReleaseError> {
        let invalid = LogicalReleaseError::InvalidLimits;
        let l = self.limits;
        let worst = self.capture_charge(count).map_err(|_| invalid.clone())?;
        let control = worst
            .checked_mul(l.control_pending)
            .ok_or_else(|| invalid.clone())?;
        if count == 0
            || l.control_pending < 2
            || control > l.control_bytes
            || l.bytes
                .checked_sub(l.control_bytes)
                .is_none_or(|n| n < worst)
        {
            return Err(invalid);
        }
        self.control_profile = Some(profile);
        Ok(self)
    }
    pub(super) fn capture_charge(&self, count: usize) -> Result<usize, LogicalReleaseError> {
        self.limits.capture_reservation_bytes::<R, V>(count)
    }
    pub(super) fn is_control_capture<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &self,
        p: &ReplicaParts<'_, S, T, E, A, W, O>,
        lease: &EffectLease,
    ) -> bool {
        match &lease.effect {
            Effect::Send(_) => true,
            Effect::Committed(entries) => self
                .control_profile
                .as_ref()
                .is_some_and(|profile| profile.is_control_batch(lease.ticket.visit.group, entries)),
            Effect::ReadReady(_) => {
                let Some(app) = p.applications.get(&lease.ticket.visit.group) else {
                    return false;
                };
                let Ok((_, query)) = p.reads.capture_query(p.owner, lease) else {
                    return false;
                };
                self.is_control_query(lease.ticket.visit.group, app, query)
            }
            _ => false,
        }
    }
    fn is_control_query<A: LogicalResponseCodec<Receipt = R, ReadResult = V>>(
        &self,
        group: GroupIdentity,
        app: &A,
        query: Option<&A::Query>,
    ) -> bool {
        let Some(profile) = self.control_profile.as_ref() else {
            return false;
        };
        if group != profile.parent() || app.logical_response_schema() != profile.response_schema() {
            return false;
        }
        let Some(query) = query else {
            return true;
        };
        let Ok(bound) = app.query_encoding_bound(query) else {
            return false;
        };
        self.encoded_control_query(group, app.logical_response_schema(), bound, |out| {
            app.encode_query(query, out)
        })
    }
    pub(super) fn encoded_control_query(
        &self,
        group: GroupIdentity,
        schema: u64,
        bound: usize,
        encode: impl FnOnce(&mut [u8]) -> Result<usize, ApplicationError>,
    ) -> bool {
        let Some(profile) = self.control_profile.as_ref() else {
            return false;
        };
        if group != profile.parent() || schema != profile.response_schema() {
            return false;
        }
        let mut bytes = [0; 17];
        if bound > bytes.len() {
            return false;
        }
        encode(&mut bytes[..bound])
            .is_ok_and(|used| used <= bound && profile.is_control_query(&bytes[..used]))
    }
}
