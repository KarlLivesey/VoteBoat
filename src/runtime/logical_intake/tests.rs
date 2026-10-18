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
//! Private input-capacity fixtures; no logical authorization or effects minted.
use super::*;
use crate::{application::Counter, contracts::HardState, log::*, logical_voter::*, quorum::*};
mod capacity;
mod reads;
struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        1
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&group) {
            self.0.push_back(group);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.0.retain(|g| *g != group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn owner() -> RuntimeOwner {
    RuntimeOwner {
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn profile() -> LogicalActivationControlProfile {
    profile_with_command_bytes(1024)
}
fn profile_with_command_bytes(command_bytes: usize) -> LogicalActivationControlProfile {
    LogicalActivationApplication::new(
        group(1),
        vec![LogicalStoreMapping::new(
            group(1),
            NodeId::new(1).unwrap(),
            owner().store.identity,
            group(2),
        )
        .unwrap()],
        LogicalActivationLimits {
            command_bytes,
            ..LogicalActivationLimits::default()
        },
        Counter::new(16).unwrap(),
    )
    .unwrap()
    .control_profile()
}
fn window() -> usize {
    1024 + size_of::<Event>() + size_of::<AdmissionTicket>()
}
fn limits() -> ShardLimits {
    ShardLimits {
        max_groups: 1,
        max_items: 4,
        group_items: 4,
        control_items: 2,
        group_control_items: 2,
        max_bytes: 3 * window(),
        group_bytes: 3 * window(),
        control_bytes: 2 * window(),
        group_control_bytes: 2 * window(),
        background_items: 1,
        background_bytes: window(),
        max_event_bytes: window(),
        visit_items: 1,
        visit_bytes: window(),
        ..ShardLimits::default()
    }
}
fn shard(limits: ShardLimits) -> Shard<Ready> {
    let id = NodeId::new(1).unwrap();
    let state = GroupLog {
        bootstrap: Bootstrap {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(id), Limits::default()).unwrap(),
            voter_stores: BTreeMap::from([(id, owner().store.identity)]),
        },
        revision: LogRevision::new(1).unwrap(),
        generation: LogGeneration::new(1).unwrap(),
        hard_state: HardState::default(),
        ballot_origin: None,
        commit_index: 0,
        snapshot: None,
        snapshot_membership: None,
        entries: vec![].into(),
    };
    let core = Raft::recover(id, owner().store, state, LogLimits::default()).unwrap();
    let mut shard = Shard::new(owner(), limits, Ready(VecDeque::new())).unwrap();
    shard.register(core).unwrap();
    shard
}
fn selected(limits: ShardLimits) -> Shard<Ready> {
    let mut shard = shard(limits);
    shard.validate_control_profile(&profile()).unwrap();
    // Capacity selection alone deliberately does not install runtime authority.
    shard.control_profile = Some(profile());
    shard
}
fn release() -> Event {
    let mut bytes = b"VBACTA01\x03".to_vec();
    bytes.extend_from_slice(&10_u128.to_le_bytes());
    bytes.extend_from_slice(&4_u64.to_le_bytes());
    bytes.extend_from_slice(&1_u64.to_le_bytes());
    Event::Propose {
        operation: OperationId::new(14).unwrap(),
        bytes,
    }
}
