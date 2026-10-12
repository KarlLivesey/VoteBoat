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
use super::*;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalNode<S, T, E, A, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
    pub(super) fn poll_shutdown_network(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
        progress: &mut LogicalNodeShutdownProgress,
    ) -> Result<(), LogicalNodeError> {
        self.pending
            .extend(self.local.outbound.poll_logical(b.dispatch));
        for _ in 0..b.dispatch.min(self.pending.len()) {
            let batch = self.pending.pop_front().expect("counted original batch");
            match self.local.outbound.cancel_dispatched(batch) {
                Ok(done) => progress.completions.push(done),
                Err(e) => {
                    self.pending.push_front(*e.batch);
                    return Err(LogicalNodeError::Network(LogicalNetworkError::Outbound(
                        e.reason,
                    )));
                }
            }
        }
        for _ in 0..b.sessions.min(self.sessions.len()) {
            let next = self
                .session_cursor
                .and_then(|old| {
                    self.sessions
                        .range((std::ops::Bound::Excluded(old), std::ops::Bound::Unbounded))
                        .next()
                        .map(|(&id, _)| id)
                })
                .or_else(|| self.sessions.keys().next().copied())
                .expect("nonempty sessions");
            self.session_cursor = Some(next);
            let slot = self.sessions.get_mut(&next).unwrap();
            if slot.shutdown_closure.is_some() {
                continue;
            }
            let terminal = match slot.session.poll(now, b.transport) {
                Ok(_) => None,
                Err(reason)
                    if matches!(
                        reason,
                        LogicalNetworkError::Closed
                            | LogicalNetworkError::Session(crate::secure::SessionError::Closed)
                    ) && slot.session.state() == TransportState::Failed =>
                {
                    Some(reason)
                }
                Err(reason) => return Err(LogicalNodeError::Network(reason)),
            };
            if let Some(completion) = slot.session.take_send() {
                match self.local.outbound.complete_logical(completion) {
                    Ok(done) => progress.completions.push(done),
                    Err(e) => {
                        slot.failed_completion = Some(*e.completion);
                        return Err(LogicalNodeError::Network(LogicalNetworkError::Outbound(
                            e.reason,
                        )));
                    }
                }
            }
            slot.incoming = None;
            let _ = slot
                .session
                .take_received(self.local.owner.authorization())
                .map_err(LogicalNodeError::Network)?;
            if let Some(reason) = terminal {
                if !slot.session.is_drained() {
                    return Err(LogicalNodeError::Network(
                        LogicalNetworkError::ProviderViolation,
                    ));
                }
                slot.shutdown_closure = Some(reason);
                progress
                    .peer_closures
                    .push(LogicalPeerClosure { peer: next, reason });
            }
        }
        Ok(())
    }
}
