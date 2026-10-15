// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

enum Held {
    Accepted(LogUpdate),
    Written(LogTicket),
    Synced(DurableLog),
}

pub(super) struct Native {
    pub(super) cluster: Cluster<NativeLogStore<ModelIo>>,
    images: BTreeMap<u64, ModelIo>,
    paused: BTreeSet<u64>,
    held: BTreeMap<u64, Held>,
    previous: BTreeMap<u64, DurableLog>,
}

impl Native {
    pub(super) fn new() -> Self {
        let images: BTreeMap<_, _> = (1..=3).map(|id| (id, ModelIo::default())).collect();
        let cluster = Cluster::new(bootstrap(1, 3), |id| {
            NativeLogStore::create(
                images[&id].clone(),
                identity(u128::from(id)),
                LogLimits::default(),
            )
            .unwrap()
        });
        Self {
            cluster,
            images,
            paused: BTreeSet::new(),
            held: BTreeMap::new(),
            previous: BTreeMap::new(),
        }
    }

    pub(super) fn act(&mut self, id: u64, event: Event) -> Result<(), Mismatch> {
        let delivery = matches!(event, Event::Receive(_));
        let replica = self
            .cluster
            .replicas
            .get_mut(&id)
            .ok_or(Mismatch::Action(id))?;
        match replica.core.step(event) {
            Ok(effects) => self.effects(id, effects),
            Err(RaftError::Busy) if self.held.contains_key(&id) => Ok(()),
            Err(RaftError::WrongIdentity) if delivery => Ok(()),
            Err(_) => Err(Mismatch::Action(id)),
        }
    }

    fn effects(&mut self, id: u64, effects: Vec<Effect>) -> Result<(), Mismatch> {
        let mut pending = VecDeque::from(effects);
        while let Some(effect) = pending.pop_front() {
            match effect {
                Effect::Persist(update) if self.paused.contains(&id) => {
                    if self.held.contains_key(&id) {
                        return Err(Mismatch::Action(id));
                    }
                    self.held.insert(id, Held::Accepted(update));
                }
                Effect::Persist(update) => {
                    let replica = self
                        .cluster
                        .replicas
                        .get_mut(&id)
                        .ok_or(Mismatch::Action(id))?;
                    let more = persist_effect(&mut replica.core, &mut replica.store, update)
                        .map_err(|_| Mismatch::Action(id))?;
                    pending.extend(more);
                }
                Effect::Send(_) | Effect::Committed(_) | Effect::ReadReady(_) => {
                    self.cluster.effects(id, vec![effect]);
                }
                _ => return Err(Mismatch::Action(id)),
            }
        }
        self.cluster.assert_safety();
        Ok(())
    }

    pub(super) fn pump(&mut self) -> Result<(), Mismatch> {
        let mut count = 0;
        while let Some(message) = self.cluster.messages.pop_front() {
            count += 1;
            if count >= 100_000 {
                return Err(Mismatch::Action(message.to.get()));
            }
            if !self
                .cluster
                .blocked
                .contains(&(message.from.get(), message.to.get()))
            {
                self.act(message.to.get(), Event::Receive(message))?;
            }
        }
        Ok(())
    }

    pub(super) fn phase(&self, id: u64) -> Option<Phase> {
        self.held.get(&id).map(|held| match held {
            Held::Accepted(_) => Phase::Accepted,
            Held::Written(_) => Phase::Written,
            Held::Synced(_) => Phase::Synced,
        })
    }

    pub(super) fn hold(&mut self, id: u64) {
        self.paused.insert(id);
    }

    pub(super) fn write_pending(&mut self, id: u64) -> Result<(), Mismatch> {
        let Some(Held::Accepted(update)) = self.held.get(&id) else {
            return Err(Mismatch::Action(id));
        };
        let update = update.clone();
        let replica = self
            .cluster
            .replicas
            .get_mut(&id)
            .ok_or(Mismatch::Action(id))?;
        let tickets = replica
            .store
            .append_batch(vec![LogMutation::Update(update)])
            .map_err(|_| Mismatch::Action(id))?;
        let [ticket] = tickets.as_slice() else {
            return Err(Mismatch::Action(id));
        };
        replica
            .core
            .admitted(*ticket)
            .map_err(|_| Mismatch::Action(id))?;
        self.held.insert(id, Held::Written(*ticket));
        Ok(())
    }

    pub(super) fn sync(&mut self, id: u64) -> Result<(), Mismatch> {
        let Some(Held::Written(ticket)) = self.held.get(&id) else {
            return Err(Mismatch::Action(id));
        };
        let replica = self
            .cluster
            .replicas
            .get_mut(&id)
            .ok_or(Mismatch::Action(id))?;
        let completion = replica
            .store
            .barrier(&[*ticket])
            .map_err(|_| Mismatch::Action(id))?;
        self.held.insert(id, Held::Synced(completion));
        Ok(())
    }

    pub(super) fn complete(&mut self, id: u64) -> Result<(), Mismatch> {
        if !matches!(self.held.get(&id), Some(Held::Synced(_))) {
            return Err(Mismatch::Action(id));
        }
        let Some(Held::Synced(completion)) = self.held.remove(&id) else {
            return Err(Mismatch::Action(id));
        };
        self.paused.remove(&id);
        let replica = self
            .cluster
            .replicas
            .get_mut(&id)
            .ok_or(Mismatch::Action(id))?;
        let effects = replica
            .core
            .complete(&completion)
            .map_err(|_| Mismatch::Action(id))?;
        self.previous.insert(id, completion);
        self.effects(id, effects)
    }

    pub(super) fn restart(&mut self, id: u64, power: bool) -> Result<(), Mismatch> {
        let acknowledged = self
            .cluster
            .applied
            .get(&id)
            .ok_or(Mismatch::Action(id))?
            .clone();
        let replica = self
            .cluster
            .replicas
            .remove(&id)
            .ok_or(Mismatch::Action(id))?;
        let old_session = replica.store.binding().session.get();
        drop(replica);
        if let Some(Held::Synced(completion)) = self.held.remove(&id) {
            self.previous.insert(id, completion);
        }
        self.paused.remove(&id);
        let image = self.images.get(&id).ok_or(Mismatch::Action(id))?;
        if power {
            image.0.borrow_mut().power_loss();
        }
        let first_operation = acknowledged.iter().find_map(operation).unwrap_or(0);
        let store = NativeLogStore::recover(
            image.clone(),
            identity(u128::from(id)),
            LogLimits::default(),
        )
        .map_err(|_| Mismatch::NativeRecovery(id, first_operation))?;
        if Some(store.binding().session.get()) != old_session.checked_add(1) {
            return Err(Mismatch::Action(id));
        }
        let state = store.state(group(1)).map_err(|_| Mismatch::Action(id))?;
        let core = Raft::recover(node(id), store.binding(), state, store.limits())
            .map_err(|_| Mismatch::NativeRecovery(id, first_operation))?;
        preserve_commands(id, &acknowledged, &core.replay_committed())?;
        self.cluster
            .applied
            .insert(id, core.replay_committed().to_vec());
        self.cluster.reads.remove(&id);
        self.cluster.replicas.insert(id, Replica { core, store });
        self.cluster.assert_safety();
        Ok(())
    }

    pub(super) fn stale(&mut self, id: u64) -> Result<(), Mismatch> {
        let completion = self.previous.get(&id).ok_or(Mismatch::Action(id))?;
        let replica = self
            .cluster
            .replicas
            .get_mut(&id)
            .ok_or(Mismatch::Action(id))?;
        let before = (
            replica.core.state().clone(),
            replica.core.role(),
            replica.core.has_pending_dependency(),
        );
        if replica.core.complete(completion) != Err(RaftError::WrongCompletion) {
            return Err(Mismatch::Action(id));
        }
        let after = (
            replica.core.state().clone(),
            replica.core.role(),
            replica.core.has_pending_dependency(),
        );
        if before != after {
            return Err(Mismatch::Action(id));
        }
        Ok(())
    }
}

fn operation(entry: &LogEntry) -> Option<u128> {
    match entry.payload {
        EntryPayload::Command { operation, .. } => Some(operation.get()),
        _ => None,
    }
}

fn preserve_commands(
    id: u64,
    acknowledged: &[LogEntry],
    recovered: &[LogEntry],
) -> Result<(), Mismatch> {
    let mut recovered = recovered.iter().filter(|entry| operation(entry).is_some());
    for original in acknowledged
        .iter()
        .filter(|entry| operation(entry).is_some())
    {
        if recovered.next() != Some(original) {
            return Err(Mismatch::NativeRecovery(id, operation(original).unwrap()));
        }
    }
    Ok(())
}
