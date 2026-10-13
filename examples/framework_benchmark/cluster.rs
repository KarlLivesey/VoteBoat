// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{store::MemoryStore, Failure, Settings};
use std::collections::{BTreeMap, VecDeque};
use voteboat::{identity::*, log::*, quorum::*, raft::*};

struct Replica {
    core: Raft,
    store: MemoryStore,
    applied: u64,
}
pub(super) struct Cluster {
    replicas: BTreeMap<NodeId, Replica>,
    messages: VecDeque<Message>,
    message_limit: usize,
    completed: VecDeque<OperationId>,
    pub peak_messages: usize,
    pub persistence_units: usize,
    pub delivered: usize,
}
fn leader() -> NodeId {
    NodeId::new(1).unwrap()
}
impl Cluster {
    pub fn new(settings: Settings) -> Result<Self, Failure> {
        let group = GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        };
        let stores: BTreeMap<_, _> = (1..=3)
            .map(|n| {
                (
                    NodeId::new(n).unwrap(),
                    StoreIdentity {
                        id: StoreId::new(u128::from(n)).unwrap(),
                        incarnation: StoreIncarnation::new(1).unwrap(),
                    },
                )
            })
            .collect();
        let bootstrap = Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            voter_stores: stores.clone(),
            policy: Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            )
            .map_err(|e| format!("{e:?}"))?,
        };
        let limits = LogLimits {
            max_entries_per_group: settings.retained_entries,
            ..LogLimits::default()
        }
        .validate()?;
        let mut replicas = BTreeMap::new();
        for (node, identity) in stores {
            let binding = StoreBinding {
                identity,
                session: StoreSession::new(1).unwrap(),
            };
            let store = MemoryStore::new(binding, limits, bootstrap.clone())?;
            let core = Raft::recover(node, binding, store.state(group)?, limits)
                .map_err(|e| format!("{e:?}"))?;
            replicas.insert(
                node,
                Replica {
                    core,
                    store,
                    applied: 0,
                },
            );
        }
        let mut cluster = Self {
            replicas,
            messages: VecDeque::new(),
            completed: VecDeque::new(),
            message_limit: settings.clients * 32 + 1024,
            peak_messages: 0,
            persistence_units: 0,
            delivered: 0,
        };
        cluster.act(leader(), Event::Campaign)?;
        cluster.drain()?;
        if cluster.replicas[&leader()].core.role() != Role::Leader {
            return Err("campaign did not elect leader".into());
        }
        Ok(cluster)
    }
    pub fn propose(&mut self, operation: OperationId) -> Result<(), Failure> {
        self.act(
            leader(),
            Event::Propose {
                operation,
                bytes: Vec::new(),
            },
        )
    }
    pub fn propose_batch(&mut self, proposals: Vec<Proposal>) -> Result<(), Failure> {
        let effects = self
            .replicas
            .get_mut(&leader())
            .unwrap()
            .core
            .propose_batch(proposals)
            .map_err(|e| format!("{e:?}"))?;
        self.effects(leader(), effects)
    }
    fn act(&mut self, node: NodeId, event: Event) -> Result<(), Failure> {
        let effects = self
            .replicas
            .get_mut(&node)
            .ok_or("unknown replica")?
            .core
            .step(event)
            .map_err(|e| format!("{e:?}"))?;
        self.effects(node, effects)
    }
    fn effects(&mut self, node: NodeId, effects: Vec<Effect>) -> Result<(), Failure> {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Send(message) => {
                    if self.messages.len() == self.message_limit {
                        return Err("message queue budget exhausted".into());
                    }
                    self.messages.push_back(message);
                    self.peak_messages = self.peak_messages.max(self.messages.len());
                }
                Effect::Persist(update) => {
                    let replica = self.replicas.get_mut(&node).unwrap();
                    effects.extend(
                        persist_effect(&mut replica.core, &mut replica.store, update)
                            .map_err(|e| format!("{e:?}"))?,
                    );
                    self.persistence_units += 1;
                }
                Effect::Committed(entries) => self.apply(node, &entries)?,
                _ => {
                    return Err(
                        "unexpected read/membership/snapshot effect in fixed-membership scaffold"
                            .into(),
                    )
                }
            }
        }
        Ok(())
    }
    fn apply(&mut self, node: NodeId, entries: &[LogEntry]) -> Result<(), Failure> {
        let replica = self.replicas.get_mut(&node).unwrap();
        for entry in entries {
            if entry.index != replica.applied + 1 {
                return Err("noncontiguous application frontier".into());
            }
            replica.applied = entry.index;
            if let EntryPayload::Command { operation, .. } = entry.payload {
                if node == leader() {
                    self.completed.push_back(operation);
                }
            }
        }
        Ok(())
    }
    pub fn take_completed(&mut self) -> Option<OperationId> {
        self.completed.pop_front()
    }
    pub fn deliver_one(&mut self) -> Result<bool, Failure> {
        let Some(message) = self.messages.pop_front() else {
            return Ok(false);
        };
        self.delivered += 1;
        self.act(message.to, Event::Receive(message))?;
        Ok(true)
    }
    pub fn drain(&mut self) -> Result<(), Failure> {
        let mut delivered = 0usize;
        while self.deliver_one()? {
            delivered += 1;
            if delivered > self.message_limit * 64 {
                return Err("message drain progress budget exhausted".into());
            }
        }
        Ok(())
    }
    pub fn frontiers(&self) -> [u64; 3] {
        std::array::from_fn(|slot| self.replicas[&NodeId::new(slot as u64 + 1).unwrap()].applied)
    }
    pub fn retained_entries(&self) -> [usize; 3] {
        std::array::from_fn(|slot| {
            self.replicas[&NodeId::new(slot as u64 + 1).unwrap()]
                .core
                .state()
                .entries
                .len()
        })
    }
}
