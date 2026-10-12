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
//! Original capture, with typed-result and encoded-content credits retained.
use super::*;
impl<R: ApplicationReceipt, V> LogicalReleaseCoordinator<R, V> {
    pub(crate) fn capture<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        now: MonoTime,
    ) -> Result<(), LogicalCaptureError> {
        let count = match &lease.effect {
            Effect::Committed(entries) => entries
                .iter()
                .filter(|e| matches!(e.payload, crate::log::EntryPayload::Command { .. }))
                .count()
                .max(1),
            _ => 1,
        };
        let control = self.is_control_capture(p, &lease);
        let charged = match self.reserve(p.owner, &lease, count, control) {
            Ok(n) => n,
            Err(reason) => {
                return Err(LogicalCaptureError::Rejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        match &lease.effect {
            Effect::Send(_) => self.capture_send(p.owner, lease, charged),
            Effect::Committed(_) => self.capture_application(p, lease, charged, control),
            Effect::ReadReady(_) => self.capture_read(p, lease, charged, control, now),
            _ => Err(LogicalCaptureError::Rejected {
                reason: LogicalReleaseError::ProviderViolation,
                lease: Box::new(lease),
            }),
        }
    }
    fn reserve<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<S, T, E>,
        lease: &EffectLease,
        count: usize,
        control: bool,
    ) -> Result<usize, LogicalReleaseError> {
        if owner.authorization() != self.authorization {
            return Err(LogicalReleaseError::WrongBinding);
        }
        owner
            .validate_lease(lease)
            .map_err(LogicalReleaseError::Owner)?;
        let charged = self.capture_charge(count)?;
        self.check_capacity(charged, control)?;
        Ok(charged)
    }
    pub(super) fn check_capacity(
        &self,
        charged: usize,
        control: bool,
    ) -> Result<(), LogicalReleaseError> {
        if self.sequence == u64::MAX {
            return Err(LogicalReleaseError::Exhausted);
        }
        if self.usage.pending >= self.limits.pending
            || charged > self.limits.bytes.saturating_sub(self.usage.bytes)
        {
            return Err(LogicalReleaseError::Overloaded);
        }
        if !control {
            let bulk_count = self.pending.values().filter(|p| !p.control).count();
            let bulk_bytes = self
                .pending
                .values()
                .filter(|p| !p.control)
                .map(|p| p.charged)
                .sum::<usize>();
            if bulk_count >= self.limits.pending - self.limits.control_pending
                || charged
                    > (self.limits.bytes - self.limits.control_bytes).saturating_sub(bulk_bytes)
            {
                return Err(LogicalReleaseError::Overloaded);
            }
        }
        Ok(())
    }
    fn capsule<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        owner: &EffectOwner<S, T, E>,
        effect: EffectTicket,
        origin: LogicalCapsuleOrigin,
        body: LogicalCapsuleBody,
    ) -> Result<LogicalCapsule, LogicalReleaseError> {
        let core = owner
            .core(effect.visit.group)
            .ok_or(LogicalReleaseError::WrongBinding)?;
        Ok(LogicalCapsule {
            expected_revision: core.state().revision,
            expected_generation: core.state().generation,
            local_configuration: core.membership().id(),
            origin,
            body,
        })
    }
    fn capture_send<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<S, T, E>,
        lease: EffectLease,
        charged: usize,
    ) -> Result<(), LogicalCaptureError> {
        let Effect::Send(message) = &lease.effect else {
            unreachable!()
        };
        if message_cost(message, self.limits.capsule_bytes).is_err() {
            return Err(LogicalCaptureError::Rejected {
                reason: LogicalReleaseError::Overloaded,
                lease: Box::new(lease),
            });
        }
        let capsule = match Self::capsule(
            owner,
            lease.ticket,
            LogicalCapsuleOrigin::Protocol(lease.ticket),
            LogicalCapsuleBody::Protocol(message.clone()),
        ) {
            Ok(c) => c,
            Err(reason) => {
                return Err(LogicalCaptureError::Rejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        self.install(Payload::Send(Box::new(lease)), vec![capsule], charged, true);
        Ok(())
    }
    fn install(
        &mut self,
        payload: Payload<R, V>,
        capsules: Vec<LogicalCapsule>,
        charged: usize,
        control: bool,
    ) {
        self.sequence += 1;
        let sequence = self.sequence;
        let mut expected = Vec::with_capacity(capsules.len());
        for (i, capsule) in capsules.into_iter().enumerate() {
            self.requests.push_back(LogicalReleaseRequest {
                ticket: LogicalReleaseTicket {
                    authorization: self.authorization,
                    sequence,
                    capsule: i,
                },
                authorization: self.authorization,
                capsule: capsule.clone(),
            });
            expected.push(ExpectedCapsule {
                capsule,
                permit: None,
                submitted: false,
                settled: false,
            });
        }
        self.usage.pending += 1;
        self.usage.bytes += charged;
        self.usage.capsules += expected.len();
        self.pending.insert(
            sequence,
            Pending {
                payload,
                capsules: expected,
                charged,
                control,
            },
        );
    }
    fn encode(
        bound: usize,
        limit: usize,
        encoder: impl FnOnce(&mut [u8]) -> Result<usize, ApplicationError>,
    ) -> Result<Vec<u8>, LogicalReleaseError> {
        if bound > limit {
            return Err(LogicalReleaseError::Overloaded);
        }
        let mut out = vec![0; bound];
        let used = encoder(&mut out).map_err(LogicalReleaseError::Application)?;
        if used > bound {
            return Err(LogicalReleaseError::ProviderViolation);
        }
        out.truncate(used);
        Ok(out)
    }
    fn capture_application<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        charged: usize,
        control: bool,
    ) -> Result<(), LogicalCaptureError> {
        let group = lease.ticket.visit.group;
        let checked = (|| {
            let app = p
                .applications
                .get(&group)
                .ok_or(LogicalReleaseError::WrongBinding)?;
            let Effect::Committed(entries) = &lease.effect else {
                unreachable!()
            };
            if app.logical_response_schema() == 0 {
                return Err(LogicalReleaseError::ProviderViolation);
            }
            app.receipt_encoding_bound(entries)
                .map_err(LogicalReleaseError::Application)
        })();
        let bound = match checked {
            Ok(n) if n <= self.limits.capsule_bytes => n,
            Ok(_) => {
                return Err(LogicalCaptureError::Rejected {
                    reason: LogicalReleaseError::Overloaded,
                    lease: Box::new(lease),
                })
            }
            Err(reason) => {
                return Err(LogicalCaptureError::Rejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        let app = p.applications.get_mut(&group).unwrap();
        let capture = p.results.capture_owned(p.owner, lease, app).map_err(|r| {
            LogicalCaptureError::Rejected {
                reason: LogicalReleaseError::ApplicationRoute(r.reason),
                lease: r.lease,
            }
        })?;
        let capsules = self.application_capsules(p.owner, p.clients, &capture.results, app, bound);
        match capsules {
            Ok(capsules) => {
                self.install(
                    Payload::Applied(Box::new(capture)),
                    capsules,
                    charged,
                    control,
                );
                Ok(())
            }
            Err(reason) => {
                let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                let _ = p.results.complete_owned(capture.results);
                let _ = p.owner.discard_failed(capture.lease);
                Err(LogicalCaptureError::Failed { reason })
            }
        }
    }
    fn application_capsules<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: LogicalResponseCodec<Receipt = R, ReadResult = V>,
    >(
        &self,
        owner: &EffectOwner<S, T, E>,
        clients: &ClientRouter<R>,
        output: &ApplicationResults<R>,
        app: &A,
        bound: usize,
    ) -> Result<Vec<LogicalCapsule>, LogicalReleaseError> {
        let mut capsules = Vec::new();
        for (i, ticket, position, command) in clients.capture_clients(output) {
            let receipt = &output.receipts()[i];
            if receipt.operation() != ticket.operation || receipt.index() != position.index {
                return Err(LogicalReleaseError::ProviderViolation);
            }
            let encoded = Self::encode(bound, self.limits.capsule_bytes, |out| {
                app.encode_receipt(receipt, out)
            })?;
            if command
                .len()
                .checked_add(encoded.capacity())
                .and_then(|n| n.checked_add(1024))
                .is_none_or(|n| n > self.limits.capsule_bytes)
            {
                return Err(LogicalReleaseError::ProviderViolation);
            }
            capsules.push(Self::capsule(
                owner,
                output.effect(),
                LogicalCapsuleOrigin::Client {
                    effect: output.effect(),
                    ticket: ticket.into(),
                    position,
                },
                LogicalCapsuleBody::ClientApplied {
                    command: command.to_vec(),
                    schema: app.logical_response_schema(),
                    receipt: encoded,
                },
            )?);
        }
        Ok(capsules)
    }
    fn capture_read<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission<Receipt = R>
            + LogicalResponseCodec<ReadResult = V>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        lease: EffectLease,
        charged: usize,
        control: bool,
        now: MonoTime,
    ) -> Result<(), LogicalCaptureError> {
        let group = lease.ticket.visit.group;
        let Some(app) = p.applications.get(&group) else {
            return Err(LogicalCaptureError::Rejected {
                reason: LogicalReleaseError::WrongBinding,
                lease: Box::new(lease),
            });
        };
        let checked = (|| {
            let (_, query) = p
                .reads
                .capture_query(p.owner, &lease)
                .map_err(LogicalReleaseError::Read)?;
            let Some(query) = query else {
                return Ok((Vec::new(), 0));
            };
            let qb = app
                .query_encoding_bound(query)
                .map_err(LogicalReleaseError::Application)?;
            let rb = app
                .read_encoding_bound(query)
                .map_err(LogicalReleaseError::Application)?;
            if app.logical_response_schema() == 0
                || qb
                    .checked_add(rb)
                    .and_then(|n| n.checked_add(1024))
                    .is_none_or(|n| n > self.limits.capsule_bytes)
            {
                return Err(LogicalReleaseError::Overloaded);
            }
            Ok((
                Self::encode(qb, self.limits.capsule_bytes, |out| {
                    app.encode_query(query, out)
                })?,
                rb,
            ))
        })();
        let (query, bound) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(LogicalCaptureError::Rejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        let capture = match p.reads.capture_owned(p.owner, lease, app, now) {
            Ok(Some(c)) => c,
            Ok(None) => return Ok(()),
            Err(ReadExecutionError::Rejected { reason, lease }) => {
                return Err(LogicalCaptureError::Rejected {
                    reason: LogicalReleaseError::Read(reason),
                    lease,
                })
            }
            Err(ReadExecutionError::Failed { reason }) => {
                return Err(LogicalCaptureError::Failed {
                    reason: LogicalReleaseError::Read(reason),
                })
            }
        };
        match self.read_capsule(p.owner, &capture, app, query, bound) {
            Ok(capsule) => {
                self.install(
                    Payload::Read(Box::new(capture)),
                    vec![capsule],
                    charged,
                    control,
                );
                Ok(())
            }
            Err(reason) => {
                let _ = p.owner.fail::<()>(EffectOwnerError::ProviderContract);
                p.reads.discard_capture(p.owner, capture);
                Err(LogicalCaptureError::Failed { reason })
            }
        }
    }
    fn read_capsule<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: LogicalResponseCodec<Receipt = R, ReadResult = V>,
    >(
        &self,
        owner: &EffectOwner<S, T, E>,
        capture: &CapturedInvocation<V>,
        app: &A,
        query: Vec<u8>,
        bound: usize,
    ) -> Result<LogicalCapsule, LogicalReleaseError> {
        let result = match capture.read.results.result() {
            Ok(v) => {
                let capacity = bound
                    .checked_add(1)
                    .ok_or(LogicalReleaseError::Overloaded)?;
                Self::encode(capacity, self.limits.capsule_bytes, |out| {
                    out[0] = 0;
                    let used = app.encode_read(v, &mut out[1..])?;
                    if used > bound {
                        return Err(ApplicationError::ReceiptBudget);
                    }
                    Ok(used + 1)
                })?
            }
            Err(error) => vec![
                1,
                match error {
                    ApplicationError::IndexGap => 0,
                    ApplicationError::InvalidCommand => 1,
                    ApplicationError::OperationConflict => 2,
                    ApplicationError::DedupCapacity => 3,
                    ApplicationError::ReceiptBudget => 4,
                    ApplicationError::NotApplied => 5,
                    ApplicationError::InvalidCheckpoint => 6,
                    ApplicationError::UnsupportedSchema => 7,
                },
            ],
        };
        let b = capture.read.results.barrier();
        Self::capsule(
            owner,
            capture.read.lease.ticket,
            LogicalCapsuleOrigin::Read {
                effect: capture.read.lease.ticket,
                ticket: capture.ticket.into(),
                barrier: LogicalReadContext {
                    group: b.group(),
                    configuration: b.configuration(),
                    term: b.term(),
                    request: b.request(),
                    index: b.index(),
                },
            },
            LogicalCapsuleBody::ReadResult {
                schema: app.logical_response_schema(),
                query,
                result,
            },
        )
    }
}
