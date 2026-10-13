// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn ready_actor(envelopes: crate::Envelopes) -> (Actor, mpsc::Sender<Request>) {
    let node = NodeId::new(1).unwrap();
    let mut bootstrap = bootstrap().unwrap();
    bootstrap.voter_stores.retain(|member, _| *member == node);
    bootstrap.policy = Policy::new(Tree::Voter(node), Limits::default()).unwrap();
    let group = bootstrap.group;
    let binding = StoreBinding {
        identity: bootstrap.voter_stores[&node],
        session: StoreSession::new(1).unwrap(),
    };
    let limits = LogLimits::default();
    let store = MemoryStore::new(binding, limits, bootstrap).unwrap();
    let mut core = Raft::recover(node, binding, store.state(group).unwrap(), limits).unwrap();
    core.configure_batch_limits(envelopes.voteboat_limits())
        .unwrap();
    let settings = Settings {
        clients: 1024,
        batch: 4,
        requested_operations: 4096,
        operations: 4096,
        warmup: 0,
        maintenance: Maintenance::DefaultRetention,
        envelopes,
    }
    .validate()
    .unwrap();
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let parts = Parts {
        slot: 0,
        core,
        store,
        snapshots: MemorySnapshots::new(binding, group),
        application: Noop { group, applied: 0 },
        receiver,
        peer_receiver: mpsc::channel(CHANNEL_CAPACITY).1,
        peers: Vec::new(),
        stopped: watch::channel(false).1,
        stopping: Arc::new(AtomicBool::new(false)),
        stats: Arc::new(std::array::from_fn(|_| stats::Stats::default())),
        paused: Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
        paused_proposals: Arc::new(AtomicBool::new(false)),
        proposals_pause_observed: Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
        frontiers: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
        settings,
    };
    let mut actor = Actor::new(parts);
    actor.step(Event::Campaign).unwrap();
    assert_eq!(actor.parts.core.role(), Role::Leader);
    (actor, sender)
}

fn preload(sender: &mpsc::Sender<Request>) -> Vec<(OperationId, oneshot::Receiver<OperationId>)> {
    let mut waits = Vec::new();
    for start in (1..=4096).step_by(4) {
        let mut proposals = Vec::new();
        let mut replies = Vec::new();
        for original in start..start + 4 {
            let operation = OperationId::new(original as u128).unwrap();
            let (reply, wait) = oneshot::channel();
            proposals.push(Proposal {
                operation,
                bytes: Vec::new(),
            });
            replies.push(reply);
            waits.push((operation, wait));
        }
        assert!(sender.try_send(Request { proposals, replies }).is_ok());
    }
    waits
}

#[test]
fn preloaded_actor_admits_the_exact_wide_bound_and_completes_all_originals() {
    for envelopes in [
        crate::Envelopes::UpstreamDefault,
        crate::Envelopes::Common64,
    ] {
        let (mut actor, sender) = ready_actor(envelopes);
        let waits = preload(&sender);
        while let Ok(request) = actor.parts.receiver.try_recv() {
            actor.requests(request).unwrap();
        }
        for (operation, mut wait) in waits {
            assert_eq!(wait.try_recv().unwrap(), operation);
        }
        assert!(actor.pending.is_empty());
        assert_eq!(actor.parts.application.applied, 4097);
        assert_eq!(actor.parts.core.state().commit_index, 4097);
        let diagnostics = serde_json::to_value(actor.parts.stats[0].read()).unwrap();
        let width = envelopes.voteboat_limits().max_proposal_entries;
        assert_eq!(diagnostics["maximum_proposal_width"], width);
        assert_eq!(diagnostics["proposed_originals"], 4096);
        assert_eq!(
            diagnostics["proposal_widths"],
            serde_json::json!([
                { "original_entries": width, "batches": 4096 / width }
            ])
        );
    }
}
