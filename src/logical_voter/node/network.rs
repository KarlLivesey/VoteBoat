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
use std::ops::Bound::{Excluded, Unbounded};
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
    pub(super) fn poll_network(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
    ) -> Result<(), LogicalNodeError> {
        self.poll_network_record(now, b, None)
    }
    pub(super) fn poll_network_record(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
        mut completions: Option<&mut Vec<LocalSendCompletion>>,
    ) -> Result<(), LogicalNodeError> {
        self.refresh_deferred()?;
        self.dispatch_pending(b.dispatch)?;
        for _ in 0..b.sessions.min(self.sessions.len()) {
            let next = self
                .session_cursor
                .and_then(|old| {
                    self.sessions
                        .range((Excluded(old), Unbounded))
                        .next()
                        .map(|(&id, _)| id)
                })
                .or_else(|| self.sessions.keys().next().copied())
                .expect("nonempty sessions");
            self.session_cursor = Some(next);
            let slot = self.sessions.get_mut(&next).unwrap();
            slot.session
                .poll(now, b.transport)
                .map_err(LogicalNodeError::Network)?;
            if let Some(completion) = slot.session.take_send() {
                match self.local.outbound.complete_logical(completion) {
                    Ok(done) => {
                        if let Some(out) = completions.as_mut() {
                            out.push(done);
                        }
                    }
                    Err(e) => {
                        slot.failed_completion = Some(*e.completion);
                        return Err(LogicalNodeError::Network(LogicalNetworkError::Outbound(
                            e.reason,
                        )));
                    }
                }
            }
            if self.state == NodeState::Draining {
                // Admission is closed. Cancel only input that has never become
                // an owner event; original send completions above still settle.
                slot.incoming = None;
                let _ = slot
                    .session
                    .take_received(self.local.owner.authorization())
                    .map_err(LogicalNodeError::Network)?;
                continue;
            }
            if slot.incoming.is_none() {
                if let Some(received) = slot
                    .session
                    .take_received(self.local.owner.authorization())
                    .map_err(LogicalNodeError::Network)?
                {
                    slot.incoming = Some(
                        received
                            .into_message(self.local.owner.authorization())
                            .unwrap_or_else(|_| panic!("unchanged exact original authorization")),
                    );
                }
            }
            if let Some(message) = slot.incoming.take() {
                let core = self
                    .local
                    .owner
                    .core(self.group)
                    .ok_or(LogicalNodeError::WrongBinding)?;
                let binding = slot
                    .session
                    .node_binding()
                    .ok_or(LogicalNodeError::WrongBinding)?;
                if message.group != self.group
                    || message.from != binding.peer.node
                    || message.sender != binding.logical
                {
                    return Err(LogicalNodeError::WrongBinding);
                }
                match core.authenticated_message_source(&message) {
                    Ok(()) => (),
                    Err(crate::raft::RaftError::Busy) => {
                        slot.incoming = Some(message);
                        continue;
                    }
                    Err(
                        crate::raft::RaftError::WrongIdentity
                        | crate::raft::RaftError::InvalidMessage,
                    ) => continue,
                    Err(reason) => {
                        return Err(LogicalNodeError::Owner(EffectOwnerError::Consensus(reason)))
                    }
                }
                if let Err(e) = self
                    .local
                    .owner
                    .admit(message.group, Event::Receive(message))
                {
                    let Event::Receive(message) = *e.event else {
                        unreachable!("original Receive")
                    };
                    slot.incoming = Some(message);
                    if e.reason != RuntimeError::Overloaded {
                        return Err(LogicalNodeError::Owner(EffectOwnerError::Runtime(e.reason)));
                    }
                }
            }
        }
        Ok(())
    }
    fn dispatch_pending(&mut self, dispatch: usize) -> Result<(), LogicalNodeError> {
        self.pending
            .extend(self.local.outbound.poll_logical(dispatch));
        for _ in 0..dispatch.min(self.pending.len()) {
            let batch = self.pending.pop_front().expect("counted original batch");
            let Some(peer) = self.sessions.get_mut(&batch.ticket().peer) else {
                if self
                    .deferred
                    .is_some_and(|(node, _)| node == batch.ticket().peer)
                {
                    self.pending.push_back(batch);
                    continue;
                }
                self.pending.push_front(batch);
                return Err(LogicalNodeError::MissingPeers);
            };
            if let Err(e) = peer.session.submit(batch) {
                self.pending.push_back(*e.batch);
                if e.reason != LogicalNetworkError::Outbound(OutboundError::Overloaded) {
                    return Err(LogicalNodeError::Network(e.reason));
                }
            }
        }
        Ok(())
    }
}
