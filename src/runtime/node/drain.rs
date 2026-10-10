// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod recovery;
pub use recovery::*;

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
    pub assignments_match: bool,
    pub resuming: bool,
    /// Ephemeral local observation only. Does not establish remote quorum,
    /// replica removal, durable operation completion or permission to stop.
    pub locally_quiescent: bool,
}
pub(super) struct LocalDrain {
    request: LocalDrainRequest,
    gate_groups: Vec<GroupIdentity>,
    queued: BTreeMap<GroupIdentity, (AdmissionTicket, bool)>,
    cursor: usize,
    resuming: bool,
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
    /// There is no unrecorded reset: release through `restore_drain` after a
    /// durable cancellation, or shut down. A recovered Node starts ungated:
    /// restore durable host intent before polling or accepting recovered work.
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
        self.install_drain_gate(request, false);
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
        let assignments_match = drain
            .request
            .groups
            .iter()
            .map(|g| g.group)
            .eq(self.local.owner.groups());
        let locally_quiescent = self.state == NodeState::Running
            && assignments_match
            && !drain.resuming
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
            assignments_match,
            resuming: drain.resuming,
            locally_quiescent,
        })
    }

    pub(super) fn poll_drain_gate(&mut self) -> Result<(), NodeError> {
        let Some(drain) = &mut self.drain else {
            return Ok(());
        };
        for _ in 0..GROUPS_PER_POLL.min(drain.gate_groups.len()) {
            let group = drain.gate_groups[drain.cursor];
            if !drain.queued.contains_key(&group) {
                match self.local.owner.admit_tracked(
                    group,
                    Event::SetCampaigning {
                        enabled: drain.resuming,
                    },
                ) {
                    Ok(ticket) => {
                        drain.queued.insert(group, (ticket, false));
                    }
                    Err(rejected) if rejected.reason == RuntimeError::Overloaded => break,
                    Err(rejected) => {
                        return Err(NodeError::Owner(EffectOwnerError::Runtime(rejected.reason)))
                    }
                }
            }
            drain.cursor = (drain.cursor + 1) % drain.gate_groups.len();
        }
        Ok(())
    }
    fn install_drain_gate(&mut self, request: LocalDrainRequest, resuming: bool) {
        self.drain = Some(LocalDrain {
            request,
            gate_groups: self.local.owner.groups().collect(),
            queued: BTreeMap::new(),
            cursor: 0,
            resuming,
        });
    }
    pub(super) fn observe_drain_gate(&mut self, steps: &[OwnerStep]) -> Result<(), NodeError> {
        let Some(drain) = &mut self.drain else {
            return Ok(());
        };
        for step in steps {
            let Some(admission) = step.admission else {
                continue;
            };
            if let Some((ticket, complete)) = drain.queued.get_mut(&admission.group) {
                if *ticket == admission {
                    if let Some(error) = &step.error {
                        return Err(NodeError::Owner(EffectOwnerError::Consensus(error.clone())));
                    }
                    *complete = true;
                }
            }
        }
        if drain.resuming
            && drain.queued.len() == drain.gate_groups.len()
            && drain.queued.values().all(|(_, complete)| *complete)
        {
            self.drain = None;
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
