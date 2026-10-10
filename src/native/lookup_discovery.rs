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
//! Explicitly driven automatic directory reads for manifest cache misses.
#[path = "manifest_read_source.rs"]
mod mapped;
use super::authority_discovery::NativeAuthorityDiscovery;
use crate::{
    application::*, connect::PeerConnector, outbound::OutboundQueue, routing::*, runtime::*,
    snapshot_worker::SnapshotWorker, transport::PeerTransportFactory, worker::PersistenceWorker,
};
pub use mapped::MappedManifestReadSource;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission
            + BoundedReadableStateMachine<
                Query = crate::identity::ResponsibilityIdentity,
                ReadResult = Option<ResponsibilityManifest>,
            > + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > ManifestReadSource for Node<S, T, E, A, W, O, H, C, F>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    type ReadResult = Option<ResponsibilityManifest>;
    fn binding(&self) -> ReadInvocationBinding {
        self.local().reads.binding()
    }
    fn pending_reads(&self) -> usize {
        self.local().reads.usage().requests
    }
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<crate::identity::ResponsibilityIdentity>>
    {
        self.read(request.locator.authority, request.locator.responsibility)
    }
    fn poll_result(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<
        Option<ReadOutcome<Option<ResponsibilityManifest>>>,
        ReadCompletionRejected<Option<ResponsibilityManifest>>,
    > {
        let Some(output) = self.poll_read() else {
            return Ok(None);
        };
        if output.ticket() != ticket {
            return Err(ReadCompletionRejected {
                reason: ReadInvocationError::WrongBinding,
                completion: Box::new(output),
            });
        }
        self.complete_read(output).map(Some)
    }
    fn cancel(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.cancel_read(ticket)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct PendingManifestLookup {
    pub request: ManifestLookup,
    pub ticket: ReadInvocationTicket,
    pub deadline: MonoTime,
    pub cancelled: bool,
}
#[derive(Debug)]
pub enum ManifestLookupPollError<R = Option<ResponsibilityManifest>> {
    Discovery(ManifestDiscoveryError),
    Source(ReadCompletionRejected<R>),
}
/// One accepted read at a time; caller explicitly polls the original source Node.
/// Cache observations are volatile hints and do not replace server ownership checks.
pub struct NativeManifestLookup<S: ManifestReadSource> {
    source: S,
    cache: NativeAuthorityDiscovery,
    binding: ReadInvocationBinding,
    timeout_ms: u64,
    retry_ms: u64,
    pending: Option<PendingManifestLookup>,
    negative: Option<(ManifestLookup, ManifestDiscoveryError, MonoTime)>,
    now: MonoTime,
    closed: bool,
}
impl<S: ManifestReadSource> NativeManifestLookup<S> {
    pub fn new(
        source: S,
        limits: ManifestCacheLimits,
        lifetime_ms: u64,
        timeout_ms: u64,
        retry_ms: u64,
        now: MonoTime,
    ) -> Result<Self, (ManifestDiscoveryError, S)> {
        if timeout_ms == 0
            || timeout_ms > 3_600_000
            || retry_ms == 0
            || retry_ms > 60_000
            || source.pending_reads() != 0
        {
            return Err((ManifestDiscoveryError::InvalidLimits, source));
        }
        let binding = source.binding();
        let cache = match NativeAuthorityDiscovery::new(limits, lifetime_ms, binding, now) {
            Ok(c) => c,
            Err(e) => return Err((e, source)),
        };
        Ok(Self {
            source,
            cache,
            binding,
            timeout_ms,
            retry_ms,
            pending: None,
            negative: None,
            now,
            closed: false,
        })
    }
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }
    pub fn pending(&self) -> Option<PendingManifestLookup> {
        self.pending
    }
    pub fn next_deadline(&self) -> Option<MonoTime> {
        self.pending.filter(|p| !p.cancelled).map(|p| p.deadline)
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_none()
    }
    pub fn into_source(self) -> Result<S, Box<Self>> {
        if !self.closed || !self.is_drained() {
            return Err(Box::new(self));
        }
        Ok(self.source)
    }
    /// Explicitly abandon lookup observation while preserving unresolved source work.
    pub fn into_recovery(self) -> (S, Option<PendingManifestLookup>) {
        (self.source, self.pending)
    }
    fn time(&mut self, now: MonoTime) -> Result<(), ManifestDiscoveryError> {
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack);
        }
        if self.source.binding() != self.binding {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        self.now = now;
        Ok(())
    }
    pub fn cancel_pending(&mut self) -> Result<(), ReadInvocationError> {
        let Some(p) = &mut self.pending else {
            return Ok(());
        };
        if p.cancelled {
            return Ok(());
        }
        p.cancelled = true;
        self.source.cancel(p.ticket)
    }
    pub fn poll(&mut self, now: MonoTime) -> Result<bool, ManifestLookupPollError<S::ReadResult>> {
        self.time(now).map_err(ManifestLookupPollError::Discovery)?;
        let Some(p) = self.pending else {
            return Ok(false);
        };
        if now >= p.deadline && !p.cancelled {
            let _ = self.cancel_pending(); // Still retain the accepted slot until receipt.
        }
        let outcome = match self.source.poll_result(p.ticket) {
            Ok(Some(outcome)) => outcome,
            Ok(None) => return Ok(false),
            Err(original) => {
                self.closed = true;
                self.cache.close();
                let _ = self.cancel_pending();
                return Err(ManifestLookupPollError::Source(original));
            }
        };
        let pending = self.pending.take().unwrap();
        let result = if pending.cancelled {
            Err(if self.closed {
                ManifestDiscoveryError::Closed
            } else if now >= pending.deadline {
                ManifestDiscoveryError::Expired
            } else {
                ManifestDiscoveryError::Cancelled
            })
        } else {
            self.cache
                .observe(pending.request, pending.ticket, outcome, now)
                .map(|_| ())
                .map_err(|(e, _)| e)
        };
        if let Err(error) = result {
            let retry =
                now.0
                    .checked_add(self.retry_ms)
                    .ok_or(ManifestLookupPollError::Discovery(
                        ManifestDiscoveryError::Exhausted,
                    ))?;
            self.negative = Some((pending.request, error, MonoTime(retry)));
            return Err(ManifestLookupPollError::Discovery(error));
        }
        self.negative = None;
        Ok(true)
    }
}
impl<S: ManifestReadSource> ManifestDiscovery for NativeManifestLookup<S> {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        self.time(now)?;
        if let Some((prior, error, until)) = self.negative {
            if prior == request && now < until {
                return Err(error);
            }
        }
        match self.cache.lookup(request, now) {
            Ok(observation) => return Ok(observation),
            Err(
                ManifestDiscoveryError::Missing
                | ManifestDiscoveryError::Expired
                | ManifestDiscoveryError::Routing(
                    RoutingError::EpochRegression | RoutingError::StaleGeneration,
                ),
            ) => (),
            Err(e) => return Err(e),
        }
        if let Some(pending) = self.pending {
            return Err(if pending.request == request {
                ManifestDiscoveryError::Unavailable
            } else {
                ManifestDiscoveryError::Overloaded
            });
        }
        let deadline = now
            .0
            .checked_add(self.timeout_ms)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        let ticket = self.source.submit(request).map_err(|r| match r.reason {
            ReadInvocationError::Overloaded | ReadInvocationError::InFlight => {
                ManifestDiscoveryError::Overloaded
            }
            ReadInvocationError::UnknownGroup => ManifestDiscoveryError::WrongAuthority,
            ReadInvocationError::Exhausted => ManifestDiscoveryError::Exhausted,
            _ => ManifestDiscoveryError::Unavailable,
        })?;
        self.pending = Some(PendingManifestLookup {
            request,
            ticket,
            deadline: MonoTime(deadline),
            cancelled: false,
        });
        if ticket.binding != self.binding
            || ticket.group != request.locator.authority
            || ticket.sequence == 0
        {
            self.closed = true;
            let _ = self.cancel_pending();
            return Err(ManifestDiscoveryError::ProviderViolation);
        }
        Err(ManifestDiscoveryError::Unavailable)
    }
    fn invalidate(&mut self, locator: AuthorityLocator, observed: ManifestObservationId) -> bool {
        self.cache.invalidate(locator, observed)
    }
    fn close(&mut self) {
        self.closed = true;
        self.cache.close();
        let _ = self.cancel_pending();
    }
}
