// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real election/durable Noop and outstanding primary replies, no synthetic role.
use super::*;
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
        self.0.retain(|g| *g != group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
pub(super) fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
pub(super) fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn binding(n: u128) -> StoreBinding {
    StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(n).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    }
}
pub(super) fn command(id: u128) -> Event {
    Event::Propose {
        operation: OperationId::new(id).unwrap(),
        bytes: 1i64.to_le_bytes().to_vec(),
    }
}
fn durable(core: &mut Raft, mut effects: Vec<Effect>) -> Vec<Effect> {
    let mut output = Vec::new();
    while let Some(effect) = effects.pop() {
        if let Effect::Persist(update) = effect {
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
                batch: state.revision.get(),
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
        } else {
            output.push(effect);
        }
    }
    output
}
pub(super) fn leader(batch: BatchLimits) -> (Raft, Vec<Message>) {
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3)
            .map(|n| (node(n), binding(u128::from(n)).identity))
            .collect(),
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
    let mut core = Raft::recover(node(1), binding(1), state, LogLimits::default()).unwrap();
    core.configure_batch_limits(batch).unwrap();
    let election = core.step(Event::Campaign).unwrap();
    let votes = durable(&mut core, election);
    let vote = votes
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(2) => Some(m),
            _ => None,
        })
        .unwrap();
    let voted = response(&vote, Rpc::Voted { granted: true });
    let election = core.step(Event::Receive(voted)).unwrap();
    let append = durable(&mut core, election);
    assert_eq!(core.role(), Role::Leader);
    assert_eq!(core.state().last_index(), 1);
    assert_eq!(core.state().commit_index, 0);
    let mut replies: Vec<_> = append
        .into_iter()
        .filter_map(|e| match e {
            Effect::Send(m) if matches!(m.rpc, Rpc::Append { .. }) => Some(response(
                &m,
                Rpc::Appended {
                    success: true,
                    matching_index: 1,
                },
            )),
            _ => None,
        })
        .collect();
    replies.sort_by_key(|m| m.from);
    assert_eq!(replies.len(), 2);
    (core, replies)
}
fn response(sent: &Message, rpc: Rpc) -> Message {
    Message {
        group: sent.group,
        configuration: sent.configuration,
        from: sent.to,
        sender: binding(u128::from(sent.to.get())),
        to: sent.from,
        term: sent.term,
        context: sent.context,
        rpc,
    }
}
pub(super) fn prepared(
    mut limits: ShardLimits,
    batch: BatchLimits,
) -> (Shard<Ready>, Vec<Message>) {
    let (core, replies) = leader(batch);
    limits.max_groups = 1;
    let owner = RuntimeOwner {
        store: core.storage_binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(owner, limits, Ready(VecDeque::new())).unwrap();
    shard.register(core).unwrap();
    (shard, replies)
}
pub(super) fn token(shard: &Shard<Ready>) -> TimerToken {
    TimerToken {
        owner: shard.owner,
        group: group(),
        kind: TimerKind::Heartbeat,
        sequence: 1,
        deadline: MonoTime(50),
    }
}
