// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Collection passes take the client requests already queued when they start.
//! Requests and peer progress that arrive during a pass wait for that pass, and
//! the biased loop handles the peer progress before the next pass.
use super::*;
mod prepared;

// A non-zero slot leads so the actor loop does not start with a campaign.
// Only slot 0 delivers client results, so this test checks application.
const LEADER: usize = 1;

struct Replicas {
    actors: Vec<Actor>,
    clients: Vec<mpsc::Sender<Request>>,
    stop: watch::Sender<bool>,
}

fn replicas() -> Replicas {
    replicas_clients(16)
}

fn replicas_clients(clients: usize) -> Replicas {
    let settings = Settings {
        clients,
        server_workers: crate::DEFAULT_SERVER_WORKERS,
        batch: 4,
        requested_operations: 16384,
        operations: 16384,
        warmup: 0,
        maintenance: Maintenance::DefaultRetention,
        envelopes: crate::Envelopes::UpstreamDefault,
        attribution: false,
        campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
    }
    .validate()
    .unwrap();
    let bootstrap = bootstrap().unwrap();
    let group = bootstrap.group;
    let limits = settings.log_limits().unwrap();
    let batch_limits = settings.envelopes.voteboat_limits(settings.batch).unwrap();
    let (clients, receivers): (Vec<_>, Vec<_>) =
        (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
    let (peers, peer_receivers): (Vec<_>, Vec<_>) =
        (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
    let (stop, stopped) = watch::channel(false);
    let stopping = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(std::array::from_fn(|_| stats::Stats::default()));
    let frontiers = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    let paused = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
    let paused_proposals = Arc::new(AtomicBool::new(false));
    let proposals_pause_observed = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
    let actors = receivers
        .into_iter()
        .zip(peer_receivers)
        .enumerate()
        .map(|(slot, (receiver, peer_receiver))| {
            let node = NodeId::new(slot as u64 + 1).unwrap();
            let binding = StoreBinding {
                identity: bootstrap.voter_stores[&node],
                session: StoreSession::new(1).unwrap(),
            };
            let store = VolatileStore::new(binding, limits).unwrap();
            let state = store.log.bootstrap(&bootstrap).unwrap();
            let mut core = Raft::recover_volatile(node, &store.log, state).unwrap();
            core.configure_batch_limits(batch_limits).unwrap();
            Actor::new(Parts {
                slot,
                core,
                store,
                snapshots: MemorySnapshots::new(binding, group),
                application: Noop { group, applied: 0 },
                receiver,
                peer_receiver,
                peers: peers.clone(),
                stopped: stopped.clone(),
                stopping: stopping.clone(),
                stats: stats.clone(),
                paused: paused.clone(),
                paused_proposals: paused_proposals.clone(),
                proposals_pause_observed: proposals_pause_observed.clone(),
                frontiers: frontiers.clone(),
                settings,
            })
        })
        .collect();
    Replicas {
        actors,
        clients,
        stop,
    }
}

/// Delivers every queued peer message until no actor has any left.
fn settle(actors: &mut [Actor]) {
    loop {
        let mut delivered = false;
        for actor in actors.iter_mut() {
            while let Ok(message) = actor.parts.peer_receiver.try_recv() {
                actor.step(Event::Receive(message)).unwrap();
                delivered = true;
            }
        }
        if !delivered {
            return;
        }
    }
}

fn request(ids: std::ops::RangeInclusive<u128>) -> Request {
    let originals = ids
        .map(|id| Original {
            operation: OperationId::new(id).unwrap(),
            bytes: Vec::new(),
            reply: oneshot::channel().0,
        })
        .collect();
    Request { originals }
}

fn leader_widths(actors: &[Actor]) -> serde_json::Value {
    serde_json::to_value(actors[LEADER].parts.stats[LEADER].read()).unwrap()["proposal_widths"]
        .clone()
}

fn widths(pairs: &[(usize, usize)]) -> serde_json::Value {
    pairs
        .iter()
        .map(|&(original_entries, batches)| {
            serde_json::json!({ "original_entries": original_entries, "batches": batches })
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn peer_progress_during_a_pass_waits_for_it_and_precedes_the_next_pass() {
    let Replicas {
        mut actors,
        clients,
        stop,
    } = replicas();
    actors[LEADER].step(Event::Campaign).unwrap();
    settle(&mut actors);
    assert_eq!(actors[LEADER].parts.core.role(), Role::Leader);
    let started = actors[LEADER].parts.core.state().commit_index;
    assert_eq!(actors[LEADER].parts.core.state().last_index(), started);

    // Followers acknowledge the first proposal. Those replies reach the
    // leader's peer queue only after the next pass has started, as when a
    // reply lands between the biased select and the end of collection.
    actors[LEADER].requests(request(1..=4)).unwrap();
    let acknowledged = actors[LEADER].parts.core.state().last_index();
    assert_eq!(acknowledged, started + 4);
    for slot in [0, 2] {
        while let Ok(message) = actors[slot].parts.peer_receiver.try_recv() {
            actors[slot].step(Event::Receive(message)).unwrap();
        }
    }
    let replies = actors[LEADER].parts.peer_receiver.len();
    assert!(replies > 0);

    // The pass takes every request that was ready when it began.
    for ids in [9..=12, 13..=16] {
        assert!(clients[LEADER].try_send(request(ids)).is_ok());
    }
    actors[LEADER].requests(request(5..=8)).unwrap();
    assert_eq!(leader_widths(&actors), widths(&[(4, 1), (12, 1)]));
    assert!(actors[LEADER].parts.receiver.is_empty());
    assert_eq!(actors[LEADER].parts.peer_receiver.len(), replies);
    assert_eq!(actors[LEADER].parts.core.state().commit_index, started);
    // The first append is still in flight, so the pass sent nothing yet.
    assert!(actors[0].parts.peer_receiver.is_empty());
    assert!(actors[2].parts.peer_receiver.is_empty());

    // The loop handles the queued replies before the next pass proposes.
    assert!(clients[LEADER].try_send(request(17..=20)).is_ok());
    let stats = actors[LEADER].parts.stats.clone();
    let proposed =
        || serde_json::to_value(stats[LEADER].read()).unwrap()["proposed_originals"] == 20;
    {
        let run = actors[LEADER].run_inner();
        tokio::pin!(run);
        // Every input is already queued, so the first poll of the loop runs
        // the next pass and the watcher sees it in that same poll; stop is
        // then sent before any timer tick can be serviced. Bounding polls (not
        // time) makes an early exit or a stall fail rather than hang.
        let watcher = async {
            for _ in 0..10_000 {
                if proposed() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("actor loop did not run the next pass");
        };
        tokio::select! {
            biased;
            result = &mut run => panic!("actor loop ended before the next pass: {result:?}"),
            () = watcher => {}
        }
        stop.send(true).unwrap();
        run.await.unwrap();
    }
    assert_eq!(
        leader_widths(&actors),
        widths(&[(4, 2), (12, 1)]),
        "the next pass proposes only the next request"
    );
    let state = actors[LEADER].parts.core.state();
    assert_eq!(state.commit_index, acknowledged);
    assert_eq!(state.last_index(), acknowledged + 16);
    // Processing the replies sends follower 0 every entry the leader holds
    // beyond the acknowledgement. It ends at the ready pass, so the next pass
    // had not proposed yet; that pass waits behind the in-flight append.
    let mut appended = Vec::new();
    while let Ok(message) = actors[0].parts.peer_receiver.try_recv() {
        if let Rpc::Append {
            previous_index,
            entries,
            leader_commit,
            ..
        } = message.rpc
        {
            let last = entries.last().map(|entry| entry.index);
            appended.push((previous_index, last, leader_commit));
        }
    }
    assert_eq!(
        appended,
        vec![(acknowledged, Some(acknowledged + 12), acknowledged)]
    );
    assert_eq!(actors[LEADER].parts.application.applied, acknowledged);
}

#[test]
fn a_request_queued_after_the_pass_starts_waits_for_the_next_pass() {
    let Replicas {
        mut actors,
        clients,
        ..
    } = replicas();
    actors[LEADER].step(Event::Campaign).unwrap();
    settle(&mut actors);
    assert_eq!(actors[LEADER].parts.core.role(), Role::Leader);
    let started = actors[LEADER].parts.core.state().last_index();
    let arrive = |actor: &mut Actor, ids: std::ops::RangeInclusive<u128>| {
        let client = clients[LEADER].clone();
        actor.after_ready = Some(Box::new(move || {
            assert!(client.try_send(request(ids)).is_ok());
        }));
    };

    // Nothing is ready, so the pass is its first request only.
    arrive(&mut actors[LEADER], 5..=8);
    actors[LEADER].requests(request(1..=4)).unwrap();
    assert!(actors[LEADER].after_ready.is_none(), "hook ran");
    assert_eq!(leader_widths(&actors), widths(&[(4, 1)]));
    assert_eq!(actors[LEADER].parts.receiver.len(), 1);

    // Two requests are ready behind the first; one more arrives mid-pass.
    for ids in [9..=12, 13..=16] {
        assert!(clients[LEADER].try_send(request(ids)).is_ok());
    }
    arrive(&mut actors[LEADER], 17..=20);
    let first = actors[LEADER].parts.receiver.try_recv().unwrap();
    actors[LEADER].requests(first).unwrap();
    assert!(actors[LEADER].after_ready.is_none(), "hook ran");
    assert_eq!(leader_widths(&actors), widths(&[(4, 1), (12, 1)]));
    assert_eq!(actors[LEADER].parts.receiver.len(), 1);
    assert_eq!(actors[LEADER].parts.core.state().last_index(), started + 16);

    // The next pass takes the late request alone.
    let late = actors[LEADER].parts.receiver.try_recv().unwrap();
    let late_ids: Vec<_> = late.originals.iter().map(|o| o.operation).collect();
    let expected: Vec<_> = (17..=20).map(|id| OperationId::new(id).unwrap()).collect();
    assert_eq!(late_ids, expected);
    actors[LEADER].requests(late).unwrap();
    assert_eq!(leader_widths(&actors), widths(&[(4, 2), (12, 1)]));
    assert_eq!(actors[LEADER].parts.core.state().last_index(), started + 20);
}
