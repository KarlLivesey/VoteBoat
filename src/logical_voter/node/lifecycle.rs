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
    pub fn poll(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
    ) -> Result<ReplicaProgress, LogicalNodeError> {
        self.poll_activation_mode(now, b, None)
    }
    pub(super) fn poll_activation_mode(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
        authorization: Option<&LogicalActivationAuthorization>,
    ) -> Result<ReplicaProgress, LogicalNodeError> {
        if b.sessions > 65536
            || b.dispatch > 65536
            || !b.replica.valid()
            || b.transport.validate().is_err()
        {
            return Err(LogicalNodeError::InvalidLimits);
        }
        if now < self.now {
            return Err(LogicalNodeError::TimeWentBack);
        }
        if self.state == NodeState::RecoveryRequired {
            return Err(LogicalNodeError::RecoveryRequired);
        }
        self.now = now;
        if self.state == NodeState::Drained {
            return Ok(ReplicaProgress::default());
        }
        let result = self.poll_inner(now, b, authorization);
        if let Err(e) = &result {
            self.enter_recovery(e.clone())
        }
        result
    }
    fn poll_inner(
        &mut self,
        now: MonoTime,
        b: LogicalNodePollBudget,
        authorization: Option<&LogicalActivationAuthorization>,
    ) -> Result<ReplicaProgress, LogicalNodeError> {
        self.poll_network(now, b)?;
        let sessions = &self.sessions;
        let deferred = &self.deferred;
        let max_peers = self.local.outbound.limits().max_peers;
        let progress = self
            .replica
            .poll_authorized(&mut self.local.as_parts(), now, b.replica, |core, event| {
                Self::authorize_activation(
                    sessions,
                    deferred,
                    max_peers,
                    core,
                    event,
                    authorization,
                )
            })
            .map_err(LogicalNodeError::Replica)?;
        self.configuration
            .observe(&self.local.owner, &progress.steps)
            .map_err(LogicalNodeError::Configuration)?;
        if self.state == NodeState::Quiescing
            && self.local.clients.is_drained()
            && self.local.reads.is_drained()
            && self.local.results.is_drained()
            && self.configuration.is_drained()
        {
            self.local
                .owner
                .close_admission()
                .map_err(LogicalNodeError::Owner)?;
            // These decoded packets have not entered the owner. Already
            // admitted visits and proof-bearing sends retain their obligations.
            for s in self.sessions.values_mut() {
                s.incoming = None;
            }
            self.state = NodeState::Draining;
        }
        self.finish_draining();
        Ok(progress)
    }
    pub(super) fn finish_draining(&mut self) {
        if self.state == NodeState::Draining && self.accepted_work_is_drained() {
            self.local.persistence.close();
            self.local.outbound.close();
            self.local.results.close();
            if let Some(s) = &mut self.local.snapshots {
                s.worker.close()
            }
            for s in self.sessions.values_mut() {
                s.session.close()
            }
        }
        if self.state == NodeState::Draining
            && self.work_is_drained()
            && self.sessions.values().all(|s| s.session.is_drained())
        {
            self.state = NodeState::Drained;
        }
    }
    pub fn close_intake(&mut self) -> Result<(), LogicalNodeError> {
        if self.state == NodeState::Running {
            self.replica
                .close_intake(&mut self.local.as_parts())
                .map_err(LogicalNodeError::Replica)?;
            self.configuration.close(ConfigurationUnknown::Shutdown);
            self.state = NodeState::Quiescing;
        } else if self.state == NodeState::RecoveryRequired {
            return Err(LogicalNodeError::RecoveryRequired);
        }
        Ok(())
    }
    /// Work quiescence is distinct from closing healthy authenticated sessions.
    pub fn work_is_drained(&self) -> bool {
        self.accepted_work_is_drained()
            && self
                .sessions
                .values()
                .all(|s| s.incoming.is_none() && s.session.work_is_drained())
    }
    /// Unadmitted receive bytes may be canceled by session close after every
    /// accepted original send has retired; they cannot prolong timer admission.
    fn accepted_work_is_drained(&self) -> bool {
        self.local.owner.is_drained()
            && self.replica.is_drained()
            && self.local.persistence.is_drained()
            && self.persistence.is_drained()
            && self.local.results.is_drained()
            && self.local.clients.is_drained()
            && self.local.reads.is_drained()
            && self.configuration.is_drained()
            && self.local.outbound.is_drained()
            && self
                .local
                .snapshots
                .as_ref()
                .is_none_or(|s| s.router.is_drained() && s.worker.is_drained())
            && self
                .snapshots
                .as_ref()
                .is_none_or(LogicalSnapshotPort::is_drained)
            && self.pending.is_empty()
            && self
                .sessions
                .values()
                .all(|s| s.failed_completion.is_none() && s.session.sends_are_drained())
    }
    pub fn is_drained(&self) -> bool {
        self.state == NodeState::Drained
    }
    pub(super) fn enter_recovery(&mut self, reason: LogicalNodeError) {
        let _ = self.local.clients.abort(&mut self.local.owner);
        let _ = self.local.reads.abort(&mut self.local.owner);
        self.configuration.close(ConfigurationUnknown::OwnerFailed);
        self.failure.get_or_insert(reason);
        self.state = NodeState::RecoveryRequired;
    }
    /// Unknown accepted work remains owned together with its lower ports/driver.
    pub fn abort(&mut self) {
        if !matches!(self.state, NodeState::Drained | NodeState::RecoveryRequired) {
            self.enter_recovery(LogicalNodeError::Closed);
        }
    }
    pub fn into_parts(self) -> LogicalNodeReclaim<S, T, E, A, U, C, V, B> {
        if !self.is_drained() {
            return Err(Box::new(self));
        }
        Ok(LogicalNodeParts {
            local: self.local,
            persistence: self.persistence,
            snapshots: self.snapshots,
            sessions: self
                .sessions
                .into_iter()
                .map(|(peer, s)| (peer, s.session))
                .collect(),
        })
    }
    /// Recovery transfers the original owner and all retained work, without
    /// manufacturing a durable observation or silently dropping a lower request.
    pub fn into_recovery(self) -> LogicalNodeRecover<S, T, E, A, U, C, V, B> {
        if self.state != NodeState::RecoveryRequired {
            return Err(Box::new(self));
        }
        let mut incoming = BTreeMap::new();
        let mut completions = BTreeMap::new();
        let sessions = self
            .sessions
            .into_iter()
            .map(|(peer, mut s)| {
                if let Some(m) = s.incoming.take() {
                    incoming.insert(peer, m);
                }
                if let Some(c) = s.failed_completion.take() {
                    completions.insert(peer, c);
                }
                (peer, s.session)
            })
            .collect();
        Ok(LogicalNodeRecovery {
            parts: LogicalNodeParts {
                local: self.local,
                persistence: self.persistence,
                snapshots: self.snapshots,
                sessions,
            },
            replica: self.replica,
            configuration: self.configuration,
            pending: self.pending,
            deferred: self.deferred,
            incoming,
            completions,
            endpoint: self.endpoint,
            reason: self.failure.expect("latched recovery cause"),
        })
    }
}
