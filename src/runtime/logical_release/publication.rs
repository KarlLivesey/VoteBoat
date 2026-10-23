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
//! Original publication only from all exact retained child permits.
use super::*;
type Publication<R, V> =
    Result<Option<Payload<R, V>>, (LogicalReleaseError, Option<Payload<R, V>>)>;
impl<R: ApplicationReceipt, V> LogicalReleaseCoordinator<R, V> {
    pub(crate) fn publish<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: LogicalOutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
        now: MonoTime,
        limit: usize,
    ) -> Result<usize, LogicalReleaseError> {
        if p.owner.authorization() != self.authorization {
            return Err(LogicalReleaseError::WrongBinding);
        }
        if self.failed_domain || p.owner.is_failed() {
            return Err(LogicalReleaseError::Owner(EffectOwnerError::Runtime(
                RuntimeError::Fenced,
            )));
        }
        if limit > 4096 {
            return Err(LogicalReleaseError::InvalidLimits);
        }
        let mut completed = 0;
        let keys = self
            .pending
            .iter()
            .filter(|(_, p)| p.capsules.iter().all(|c| c.permit.is_some()))
            .take(limit)
            .map(|(s, _)| *s)
            .collect::<Vec<_>>();
        for sequence in keys {
            let Pending {
                payload,
                mut capsules,
                charged,
                control,
            } = self.pending.remove(&sequence).unwrap();
            let result = match payload {
                Payload::Send(lease) => {
                    self.publish_send(p.owner, p.outbound, *lease, &mut capsules, now)
                }
                payload @ Payload::RejectedSend { .. } => {
                    Err((LogicalReleaseError::ProviderViolation, Some(payload)))
                }
                Payload::Applied(capture) => {
                    Self::publish_application(p.owner, p.results, p.clients, *capture, now)
                }
                Payload::Read(capture) => match p.reads.publish_owned(p.owner, *capture) {
                    Ok(()) => Ok(None),
                    Err(r) => Err((
                        LogicalReleaseError::Read(r.reason),
                        Some(Payload::Read(r.capture)),
                    )),
                },
            };
            match result {
                Ok(None) => {
                    self.uncharge(charged, capsules.len());
                    completed += 1;
                }
                Ok(Some(payload)) => {
                    self.pending.insert(
                        sequence,
                        Pending {
                            payload,
                            capsules,
                            charged,
                            control,
                        },
                    );
                }
                Err((reason, payload)) => {
                    if let Some(payload) = payload {
                        self.pending.insert(
                            sequence,
                            Pending {
                                payload,
                                capsules,
                                charged,
                                control,
                            },
                        );
                    } else {
                        self.uncharge(charged, capsules.len());
                    }
                    return Err(reason);
                }
            }
        }
        Ok(completed)
    }
    fn uncharge(&mut self, charged: usize, capsules: usize) {
        self.usage.pending -= 1;
        self.usage.bytes -= charged;
        self.usage.capsules -= capsules;
    }
    fn publish_send<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O: LogicalOutboundQueue,
    >(
        &self,
        owner: &mut EffectOwner<S, T, E>,
        outbound: &mut O,
        lease: EffectLease,
        capsules: &mut [ExpectedCapsule],
        now: MonoTime,
    ) -> Publication<R, V> {
        let Effect::Send(message) = &lease.effect else {
            unreachable!()
        };
        let same = capsules.len() == 1
            && capsules[0].permit.as_ref().is_some_and(|permit| {
                permit.capsule().origin == LogicalCapsuleOrigin::Protocol(lease.ticket)
                    && matches!(&permit.capsule().body,
                        LogicalCapsuleBody::Protocol(captured) if captured.as_ref() == message)
            });
        if !same {
            return Err((
                LogicalReleaseError::StalePermit,
                Some(Payload::Send(Box::new(lease))),
            ));
        }
        let EffectLease {
            ticket,
            effect: Effect::Send(message),
        } = lease
        else {
            unreachable!()
        };
        let peer = message.to;
        // Vote/granted Voted carry only scalars. Keep exact correlation across transfer;
        // do not clone Append/Snapshot payloads merely to update a local timer.
        let vote = matches!(
            message.rpc,
            crate::raft::Rpc::Vote { .. } | crate::raft::Rpc::Voted { granted: true }
        )
        .then(|| message.clone());
        let send = AuthorizedLogicalSend {
            effect: ticket,
            authorization: self.authorization,
            message,
            provenance: LogicalSendProvenance {
                permit: capsules[0].permit.take().unwrap(),
            },
        };
        match outbound.submit_authorized(send) {
            Ok(accepted) => {
                if accepted.binding != outbound.binding()
                    || accepted.peer != peer
                    || accepted.sequence == 0
                {
                    let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                    let _ = owner.discard_failed_transfer(ticket);
                    return Err((LogicalReleaseError::ProviderViolation, None));
                }
                if let Err(error) = owner.release_accepted_logical_send(ticket, vote.as_ref(), now)
                {
                    let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                    let _ = owner.discard_failed_transfer(ticket);
                    return Err((LogicalReleaseError::Owner(error), None));
                }
                Ok(None)
            }
            Err(rejected) => {
                let send = *rejected.send;
                let same = send.effect == ticket
                    && send.authorization == self.authorization
                    && send.provenance.capsule() == &capsules[0].capsule
                    && matches!(&send.provenance.capsule().body,
                        LogicalCapsuleBody::Protocol(captured) if captured.as_ref() == &send.message);
                if !same {
                    return Err((
                        LogicalReleaseError::ProviderViolation,
                        Some(Payload::RejectedSend {
                            original: Box::new(ticket),
                            output: Box::new(send),
                        }),
                    ));
                }
                capsules[0].permit = Some(send.provenance.permit);
                let payload = Some(Payload::Send(Box::new(EffectLease {
                    ticket: send.effect,
                    effect: Effect::Send(send.message),
                })));
                if rejected.reason == OutboundError::Overloaded {
                    Ok(payload)
                } else {
                    Err((LogicalReleaseError::Outbound(rejected.reason), payload))
                }
            }
        }
    }
    fn publish_application<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        owner: &mut EffectOwner<S, T, E>,
        results: &mut ApplicationRouter<R>,
        clients: &mut ClientRouter<R>,
        capture: CapturedApplication<R>,
        now: MonoTime,
    ) -> Publication<R, V> {
        if let Err(error) = clients.validate_delivery(owner, results, &capture.results) {
            return Err((
                LogicalReleaseError::Client(error),
                Some(Payload::Applied(Box::new(capture))),
            ));
        }
        let CapturedApplication {
            lease,
            results: output,
        } = capture;
        if let Err(rejected) = owner.complete_applied_capture(lease, output.through(), now) {
            return Err((
                LogicalReleaseError::Owner(rejected.reason),
                Some(Payload::Applied(Box::new(CapturedApplication {
                    lease: *rejected.lease,
                    results: output,
                }))),
            ));
        }
        match clients.deliver_owned(owner, results, output) {
            Ok(_) => Ok(None),
            Err(rejected) => {
                let _ = results.complete_owned(*rejected.results);
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                Err((LogicalReleaseError::Client(rejected.reason), None))
            }
        }
    }
    fn validate_discard<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &self,
        p: &ReplicaParts<'_, S, T, E, A, W, O>,
        payload: &Payload<R, V>,
    ) -> Result<(), LogicalReleaseError> {
        let ticket = match payload {
            Payload::Send(lease) => lease.ticket,
            Payload::RejectedSend { original, .. } => **original,
            Payload::Applied(capture) => {
                p.results
                    .validate_result(&capture.results)
                    .map_err(LogicalReleaseError::ApplicationRoute)?;
                capture.lease.ticket
            }
            Payload::Read(capture) => {
                p.reads
                    .validate_discard_capture(capture)
                    .map_err(LogicalReleaseError::Read)?;
                capture.read.lease.ticket
            }
        };
        p.owner
            .validate_failed_transfer(ticket)
            .map_err(LogicalReleaseError::Owner)
    }
    pub(crate) fn discard_failed<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine<Receipt = R> + ReadableStateMachine<ReadResult = V>,
        W: PersistenceWorker,
        O: OutboundQueue,
    >(
        &mut self,
        p: &mut ReplicaParts<'_, S, T, E, A, W, O>,
    ) -> Result<(), LogicalReleaseError> {
        if p.owner.authorization() != self.authorization || !p.owner.is_failed() {
            return Err(LogicalReleaseError::WrongBinding);
        }
        self.failed_domain = true;
        self.closed = true;
        self.requests.clear();
        if self.pending.values().any(|pending| {
            pending
                .capsules
                .iter()
                .any(|capsule| capsule.submitted && !capsule.settled)
        }) {
            // Child admission is not completion. Keep every original parent
            // capture and credit until actual submitted child work is consumed.
            return Err(LogicalReleaseError::Overloaded);
        }
        while let Some((&sequence, pending)) = self.pending.first_key_value() {
            self.validate_discard(p, &pending.payload)?;
            let pending = self.pending.remove(&sequence).unwrap();
            let charged = pending.charged;
            let capsules = pending.capsules.len();
            match pending.payload {
                Payload::Send(lease) => {
                    p.owner
                        .discard_failed(*lease)
                        .map_err(|r| LogicalReleaseError::Owner(r.reason))?;
                }
                Payload::RejectedSend { original, output } => {
                    p.owner
                        .discard_failed_transfer(*original)
                        .map_err(LogicalReleaseError::Owner)?;
                    // Explicit failed-domain consumption of the misrouted output;
                    // it grants no completion to the output's foreign owner.
                    drop(output);
                }
                Payload::Applied(capture) => {
                    p.results
                        .complete_owned(capture.results)
                        .map_err(|r| LogicalReleaseError::ApplicationRoute(r.reason))?;
                    p.owner
                        .discard_failed(capture.lease)
                        .map_err(|r| LogicalReleaseError::Owner(r.reason))?;
                }
                Payload::Read(capture) => p.reads.discard_capture(p.owner, *capture),
            }
            self.uncharge(charged, capsules);
        }
        Ok(())
    }
}
