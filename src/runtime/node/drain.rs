// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeSet;

pub const MAX_LOCAL_DRAIN_GROUPS: usize = 1024;
const GROUPS_PER_POLL: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainGroup {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
}
/// Sorted, complete local assignment manifest. The host owns durable intent,
/// authorization and remote availability checks; this object is volatile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalDrainRequest {
    pub operation: OperationId,
    pub groups: Vec<DrainGroup>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalDrainError {
    Closed,
    TooManyGroups,
    AssignmentMismatch,
    UnstableConfiguration,
    ConflictingRequest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainGroupState {
    SuppressionPending,
    Leader,
    ConfigurationChanged,
    CorePending,
    Fenced,
    LocallyQuiescent,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalDrainStatus {
    pub operation: OperationId,
    pub groups: Vec<(DrainGroup, DrainGroupState)>,
    pub outstanding_requests: usize,
    /// Ephemeral local observation only. Does not establish remote quorum,
    /// replica removal, durable operation completion or permission to stop.
    pub locally_quiescent: bool,
}
pub(super) struct LocalDrain {
    request: LocalDrainRequest,
    queued: BTreeSet<GroupIdentity>,
    cursor: usize,
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > Node<S, T, E, A, W, O, H, C, F>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    /// Close ordinary client admission immediately, then disable campaigns in
    /// bounded owner polls. Existing leaders keep heartbeats for handoff.
    /// This gate is one-way until shutdown. A recovered Node starts ungated:
    /// reapply durable host intent before polling or accepting recovered work.
    pub fn begin_local_drain(&mut self, request: LocalDrainRequest) -> Result<(), LocalDrainError> {
        if self.state != NodeState::Running {
            return Err(LocalDrainError::Closed);
        }
        if let Some(current) = &self.drain {
            return if current.request == request {
                Ok(())
            } else {
                Err(LocalDrainError::ConflictingRequest)
            };
        }
        if request.groups.len() > MAX_LOCAL_DRAIN_GROUPS {
            return Err(LocalDrainError::TooManyGroups);
        }
        if !request
            .groups
            .iter()
            .map(|g| g.group)
            .eq(self.local.owner.groups())
        {
            return Err(LocalDrainError::AssignmentMismatch);
        }
        for expected in &request.groups {
            let core = self.local.owner.core(expected.group).unwrap();
            if !configuration_matches(core, *expected) {
                return Err(LocalDrainError::UnstableConfiguration);
            }
        }
        self.drain = Some(LocalDrain {
            request,
            queued: BTreeSet::new(),
            cursor: 0,
        });
        Ok(())
    }

    pub fn local_drain_status(&self) -> Option<LocalDrainStatus> {
        let drain = self.drain.as_ref()?;
        let groups = drain
            .request
            .groups
            .iter()
            .map(|expected| {
                let state = match self.local.owner.core(expected.group) {
                    None => DrainGroupState::ConfigurationChanged,
                    Some(core) => group_state(core, *expected),
                };
                (*expected, state)
            })
            .collect::<Vec<_>>();
        let outstanding_requests =
            self.local.clients.usage().requests + self.local.reads.usage().requests;
        let locally_quiescent = self.state == NodeState::Running
            && outstanding_requests == 0
            && self.configuration.is_drained()
            && self.local.results.is_drained()
            && groups
                .iter()
                .all(|(_, s)| *s == DrainGroupState::LocallyQuiescent);
        Some(LocalDrainStatus {
            operation: drain.request.operation,
            groups,
            outstanding_requests,
            locally_quiescent,
        })
    }

    pub(super) fn poll_drain_gate(&mut self) -> Result<(), NodeError> {
        let Some(drain) = &mut self.drain else {
            return Ok(());
        };
        for _ in 0..GROUPS_PER_POLL.min(drain.request.groups.len()) {
            let group = drain.request.groups[drain.cursor].group;
            if !drain.queued.contains(&group) {
                match self
                    .local
                    .owner
                    .admit(group, Event::SetCampaigning { enabled: false })
                {
                    Ok(()) => {
                        drain.queued.insert(group);
                    }
                    Err(rejected) if rejected.reason == RuntimeError::Overloaded => break,
                    Err(rejected) => {
                        return Err(NodeError::Owner(EffectOwnerError::Runtime(rejected.reason)))
                    }
                }
            }
            drain.cursor = (drain.cursor + 1) % drain.request.groups.len();
        }
        Ok(())
    }
}
fn configuration_matches(core: &Raft, expected: DrainGroup) -> bool {
    let membership = core.membership();
    membership.id() == expected.configuration
        && membership.joint().is_none()
        && membership.last_configuration_index() <= core.state().commit_index
}
fn group_state(core: &Raft, expected: DrainGroup) -> DrainGroupState {
    if !configuration_matches(core, expected) {
        DrainGroupState::ConfigurationChanged
    } else if core.is_fenced() {
        DrainGroupState::Fenced
    } else if core.campaigning_enabled() {
        DrainGroupState::SuppressionPending
    } else if core.role() == Role::Leader {
        DrainGroupState::Leader
    } else if core.has_pending_dependency() {
        DrainGroupState::CorePending
    } else {
        DrainGroupState::LocallyQuiescent
    }
}
