// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn enabled() -> Cluster<HostLogStore> {
    let mut c = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id.into()));
    for replica in c.replicas.values_mut() {
        replica.core.enable_commit_notices().unwrap();
        replica.core.enable_commit_notices().unwrap();
    }
    c.act(1, Event::Campaign);
    c.pump();
    c
}
fn hold_notices(c: &mut Cluster<HostLogStore>) -> Vec<Message> {
    c.act(
        1,
        Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: vec![7],
        },
    );
    let mut held = Vec::new();
    while let Some(m) = c.messages.pop_front() {
        if matches!(m.rpc, Rpc::CommitNotice { .. }) {
            held.push(m);
        } else {
            c.deliver(m);
        }
    }
    assert_eq!(held.len(), 2);
    held
}
#[test]
fn public_startup_selection_has_no_notice_reply_or_early_application_and_recovery_resets_it() {
    let mut c = enabled();
    let held = hold_notices(&mut c);
    for m in held {
        let id = m.to.get();
        let replica = c.replicas.get_mut(&id).unwrap();
        let old = replica.core.state().clone();
        let effects = replica.core.step(Event::Receive(m.clone())).unwrap();
        assert_eq!(replica.core.state(), &old);
        assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
        assert!(c.commands(id).is_empty());
        c.effects(id, effects);
        assert_eq!(c.commands(id), vec![vec![7]]);
        assert!(c.messages.is_empty());
        assert!(c
            .replicas
            .get_mut(&id)
            .unwrap()
            .core
            .step(Event::Receive(m.clone()))
            .unwrap()
            .is_empty());
        let replica = c.replicas.get_mut(&id).unwrap();
        let state = replica.store.state(group(1)).unwrap();
        let mut restarted = Raft::recover(
            node(id),
            replica.store.binding(),
            state,
            replica.store.limits(),
        )
        .unwrap();
        assert!(restarted.step(Event::Receive(m.clone())).is_err());
        restarted.enable_commit_notices().unwrap();
        assert!(restarted.step(Event::Receive(m)).unwrap().is_empty());
    }
    assert!(c
        .replicas
        .get_mut(&1)
        .unwrap()
        .core
        .enable_commit_notices()
        .is_err());
}
struct FailedBarrier<'a>(&'a mut HostLogStore);
impl LogStore for FailedBarrier<'_> {
    fn binding(&self) -> StoreBinding {
        self.0.binding()
    }
    fn limits(&self) -> LogLimits {
        self.0.limits()
    }
    fn state(&self, g: GroupIdentity) -> Result<GroupLog, voteboat::contracts::StorageError> {
        self.0.state(g)
    }
    fn append_batch(
        &mut self,
        m: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, voteboat::contracts::StorageError> {
        self.0.append_batch(m)
    }
    fn barrier(
        &mut self,
        _: &[LogTicket],
    ) -> Result<DurableLog, voteboat::contracts::StorageError> {
        Err(voteboat::contracts::StorageError::Rejected(
            "CPU1 injected failed barrier",
        ))
    }
    fn fetch_range(
        &self,
        g: GroupIdentity,
        v: LogGeneration,
        f: u64,
        n: usize,
        b: usize,
    ) -> Result<Vec<LogEntry>, voteboat::contracts::StorageError> {
        self.0.fetch_range(g, v, f, n, b)
    }
}
#[test]
fn public_failed_notice_persistence_fences_without_releasing_application() {
    let mut c = enabled();
    let mut notice = hold_notices(&mut c).remove(0);
    notice.term += 1;
    let id = notice.to.get();
    let replica = c.replicas.get_mut(&id).unwrap();
    let before = replica.core.state().clone();
    let effects = replica.core.step(Event::Receive(notice)).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let result = persist_effect_borrowed(
        &mut replica.core,
        &mut FailedBarrier(&mut replica.store),
        update.clone(),
    );
    assert!(result.is_err());
    assert!(replica.core.is_fenced());
    assert_eq!(replica.core.state(), &before);
    assert!(c.commands(id).is_empty());
}
