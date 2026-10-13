// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn ready_actor<const TRACE: bool>(
    envelopes: crate::Envelopes,
) -> (Actor<TRACE>, mpsc::Sender<Request>) {
    let node = NodeId::new(1).unwrap();
    let mut bootstrap = bootstrap().unwrap();
    bootstrap.voter_stores.retain(|member, _| *member == node);
    bootstrap.policy = Policy::new(Tree::Voter(node), Limits::default()).unwrap();
    let group = bootstrap.group;
    let binding = StoreBinding {
        identity: bootstrap.voter_stores[&node],
        session: StoreSession::new(1).unwrap(),
    };
    let limits = Settings {
        clients: 4096,
        batch: 4,
        requested_operations: 16384,
        operations: 16384,
        warmup: 0,
        maintenance: Maintenance::DefaultRetention,
        envelopes,
        attribution: TRACE,
    }
    .log_limits()
    .unwrap();
    let store = MemoryStore::<TRACE>::new(binding, limits, bootstrap).unwrap();
    let mut core = Raft::recover(node, binding, store.state(group).unwrap(), limits).unwrap();
    core.configure_batch_limits(envelopes.voteboat_limits(4).unwrap())
        .unwrap();
    let settings = Settings {
        clients: 4096,
        batch: 4,
        requested_operations: 16384,
        operations: 16384,
        warmup: 0,
        maintenance: Maintenance::DefaultRetention,
        envelopes,
        attribution: TRACE,
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
    for start in (1..=16384).step_by(4) {
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
    checked_ready::<false>();
    checked_ready::<true>();
}
fn checked_ready<const TRACE: bool>() {
    for envelopes in [
        crate::Envelopes::UpstreamDefault,
        crate::Envelopes::Common64,
    ] {
        let (mut actor, sender) = ready_actor::<TRACE>(envelopes);
        let waits = preload(&sender);
        while let Ok(request) = actor.parts.receiver.try_recv() {
            actor.requests(request).unwrap();
        }
        for (operation, mut wait) in waits {
            assert_eq!(wait.try_recv().unwrap(), operation);
        }
        assert!(actor.pending.is_empty());
        assert_eq!(actor.parts.application.applied, 16385);
        assert_eq!(actor.parts.core.state().commit_index, 16385);
        let diagnostics = serde_json::to_value(actor.parts.stats[0].read()).unwrap();
        let width = envelopes.voteboat_limits(4).unwrap().max_proposal_entries;
        assert_eq!(diagnostics["maximum_proposal_width"], width);
        assert_eq!(diagnostics["proposed_originals"], 16384);
        assert_eq!(
            diagnostics["proposal_widths"],
            serde_json::json!([
                { "original_entries": width, "batches": 16384 / width }
            ])
        );
    }
}

#[test]
fn checkpoint_scopes_observe_actual_helpers_and_nested_retention() {
    checked_checkpoint_scopes::<false>();
    checked_checkpoint_scopes::<true>();
}
fn checked_checkpoint_scopes<const TRACE: bool>() {
    let (mut actor, _sender) = ready_actor::<TRACE>(crate::Envelopes::UpstreamDefault);
    for start in (1..=12_000).step_by(4) {
        let mut proposals = Vec::new();
        let mut replies = Vec::new();
        let mut waits = Vec::new();
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
        actor.requests(Request { proposals, replies }).unwrap();
        actor.maintain().unwrap();
        for (expected, mut wait) in waits {
            assert_eq!(wait.try_recv().unwrap(), expected);
        }
    }
    assert_eq!(actor.parts.application.applied, 12_001);
    assert_eq!(actor.parts.core.state().base_index(), 9001);
    assert!(actor.pending.is_empty());
    let report = actor
        .attribution
        .report(
            0,
            &actor.parts.store.attribution,
            &actor.parts.snapshots.attribution,
        )
        .unwrap();
    if !TRACE {
        assert!(report.is_none());
        return;
    }
    let value = serde_json::to_value(report.unwrap()).unwrap();
    let phases = value["phases"].as_array().unwrap();
    for name in [
        "checkpoint_reserve_inclusive",
        "checkpoint_publish_inclusive",
        "checkpoint_compact_inclusive",
        "checkpoint_effects_inclusive",
    ] {
        let phase = phases.iter().find(|v| v["phase"] == name).unwrap();
        assert_eq!(phase["calls"], 2);
        assert_eq!(phase["failures"], 0);
    }
    let methods = value["checkpoint_provider_scopes"][2]["methods"]
        .as_array()
        .unwrap();
    let count = |name| {
        methods.iter().find(|v| v["method"] == name).unwrap()["calls"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(count("store_state_clone_nested"), 2);
    assert_eq!(count("store_append_nested"), 2);
    assert_eq!(count("store_barrier_nested"), 2);
    assert_eq!(count("snapshot_pin_for_log_nested"), 2);
    assert_eq!(count("snapshot_reconcile_log_inclusive_nested"), 2);
    assert_eq!(count("snapshot_load_pinned_nested"), 4);
}

#[test]
fn provider_failures_keep_original_outcomes_and_are_scoped() {
    let (mut actor, _sender) = ready_actor::<true>(crate::Envelopes::UpstreamDefault);
    let before = actor.provider_snapshot();
    let reference = checkpoint_application(
        &actor.parts.core,
        &actor.parts.application,
        &mut actor.parts.snapshots,
    )
    .unwrap()
    .reference();
    assert_eq!(
        actor.parts.snapshots.load_pinned(reference),
        Err(voteboat::contracts::StorageError::StaleTicket)
    );
    actor.parts.snapshots.pin_for_log(reference).unwrap();
    actor
        .parts
        .snapshots
        .pins
        .get_mut(&reference.generation)
        .unwrap()
        .1
        .application[0] ^= 1;
    assert!(matches!(
        actor.parts.snapshots.reconcile_log(Some(reference)),
        Err(voteboat::contracts::StorageError::Corrupt(_))
    ));
    // Failed reconciliation preserves the provider's original pinned image/anchor state.
    assert_eq!(actor.parts.snapshots.pins.len(), 1);
    assert!(actor.parts.snapshots.current.is_some());
    actor.attribution.finish_scope(
        Phase::CheckpointCompact,
        before,
        &actor.parts.store.attribution,
        &actor.parts.snapshots.attribution,
    );
    let report = serde_json::to_value(
        actor
            .attribution
            .report(
                0,
                &actor.parts.store.attribution,
                &actor.parts.snapshots.attribution,
            )
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let methods = report["checkpoint_provider_scopes"][2]["methods"]
        .as_array()
        .unwrap();
    let failures = |name| {
        methods.iter().find(|v| v["method"] == name).unwrap()["failures"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(failures("snapshot_load_pinned_nested"), 2);
    assert_eq!(failures("snapshot_reconcile_log_inclusive_nested"), 1);
}
