// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host assertions use real deterministic transitions; no filesystem claims.
use super::*;
use crate::{
    contracts::HardState,
    log::*,
    quorum::{Limits, Policy, Tree},
};

pub(super) struct Ready(VecDeque<GroupIdentity>);
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
        self.0.retain(|id| *id != group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
pub(super) fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub(super) fn command(id: u128) -> Event {
    Event::Propose {
        operation: OperationId::new(id).unwrap(),
        bytes: id.to_le_bytes().to_vec(),
    }
}
pub(super) fn shard(mut limits: ShardLimits, log: LogLimits) -> Shard<Ready> {
    let identity = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let binding = StoreBinding {
        identity,
        session: StoreSession::new(1).unwrap(),
    };
    let node = NodeId::new(1).unwrap();
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, identity)]),
    };
    let state = GroupLog {
        bootstrap,
        revision: LogRevision::new(1).unwrap(),
        generation: LogGeneration::new(1).unwrap(),
        hard_state: HardState::default(),
        ballot_origin: None,
        commit_index: 0,
        snapshot: None,
        snapshot_membership: None,
        entries: Vec::new(),
    };
    let mut core = Raft::recover(node, binding, state, log).unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    finish(&mut core, effects);
    assert_eq!(core.role(), Role::Leader);
    limits.max_groups = 1;
    let owner = RuntimeOwner {
        store: binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(owner, limits, Ready(VecDeque::new())).unwrap();
    shard.register(core).unwrap();
    shard
}
pub(super) fn finish(core: &mut Raft, effects: Vec<Effect>) {
    let mut effects = VecDeque::from(effects);
    while let Some(effect) = effects.pop_front() {
        match effect {
            Effect::Persist(update) => {
                let mut states = BTreeMap::from([(group(), core.state().clone())]);
                apply_batch(
                    &mut states,
                    &[LogMutation::Update(update)],
                    core.proposal_limits(),
                )
                .unwrap();
                let state = &states[&group()];
                let ticket = LogTicket {
                    binding: core.storage_binding(),
                    batch: state.revision.get() * 10,
                    group: group(),
                    revision: state.revision,
                    generation: state.generation,
                    last_index: state.last_index(),
                    term: state.hard_state.term,
                };
                core.admitted(ticket).unwrap();
                effects.extend(
                    core.complete(&DurableLog {
                        tickets: vec![ticket],
                    })
                    .unwrap(),
                );
            }
            Effect::Committed(_) => (),
            _ => panic!("unexpected single voter fixture effect"),
        }
    }
}
