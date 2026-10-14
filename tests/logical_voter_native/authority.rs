// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{Cluster, HISTORY},
    setup,
};
#[path = "authority/vote_window.rs"]
mod vote_window;
use voteboat::{
    application::CounterReceipt,
    identity::*,
    log::LogStore,
    logical_voter::*,
    native::{connect::NativePeerProtocol, runtime::*},
    raft::Raft,
    runtime::*,
    secure::PeerIdentity,
};
type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
fn owner(image: &VerifiedLogicalImage) -> Owner {
    let (log, _) = image.recovery_views();
    let identity = RuntimeOwner {
        store: image.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        identity,
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        FairScheduler::new(8).unwrap(),
    )
    .unwrap();
    shard
        .register(
            Raft::recover(
                image.mapping().voter,
                image.binding(),
                log.state(image.mapping().parent).unwrap(),
                log.limits(),
            )
            .unwrap(),
        )
        .unwrap();
    let runtime = TimedShard::new(
        shard,
        DeadlineQueue::new(identity, 8).unwrap(),
        JitterEntropy::new(19),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let worker = image.worker_binding(StorageWorkerGeneration::new(1).unwrap());
    image
        .into_owner(runtime, worker, EffectOwnerLimits::default())
        .unwrap_or_else(|e| panic!("verified logical construction: {:?}", e.reason))
}
fn physical_constructors_reject(owner: &Owner) {
    let auth = owner.authorization();
    assert!(matches!(
        ApplicationRouter::<CounterReceipt>::new(
            ApplicationRouterBinding::new(auth, ApplicationRouterGeneration::new(1).unwrap()),
            ApplicationRouterLimits::default()
        ),
        Err(ApplicationRouteError::LogicalReleaseRequired)
    ));
    assert!(matches!(
        ClientRouter::<CounterReceipt>::new(
            ClientRouterBinding::new(auth, ClientRouterGeneration::new(1).unwrap()),
            ClientRouterLimits::default()
        ),
        Err(ClientError::LogicalReleaseRequired)
    ));
    assert!(matches!(
        ReadRouter::<i64>::new(
            ReadRouterBinding::new(auth, ReadRouterGeneration::new(1).unwrap()),
            ReadRouterLimits::default()
        ),
        Err(ReadRouteError::LogicalReleaseRequired)
    ));
    assert!(LogicalReleaseCoordinator::<CounterReceipt, i64>::new(
        RuntimeAuthorization::physical(owner.identity()),
        LogicalReleaseLimits::default()
    )
    .is_err());
}
#[test]
fn actual_verified_child_authority_blocks_raw_transfer_and_physical_router_construction() {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let image = cluster.claim(1, 7020, false);
    let mut owner = owner(&image);
    physical_constructors_reject(&owner);
    let mut routes = image
        .routers::<CounterReceipt, (), i64, _, _, _>(&owner, LogicalRouterLimits::default())
        .unwrap();
    let usage = owner.usage();
    assert_eq!(
        owner.take_effect().unwrap_err(),
        EffectOwnerError::LogicalReleaseRequired
    );
    assert_eq!(owner.usage(), usage);
    assert!(routes.results.poll().is_none());
    assert!(routes.clients.poll().is_none());
    assert!(routes.reads.poll().is_none());
    assert_eq!(
        routes.results.binding().authorization(),
        owner.authorization()
    );
    assert_eq!(
        routes.clients.binding().authorization(),
        owner.authorization()
    );
    assert_eq!(
        routes.reads.binding().authorization(),
        owner.authorization()
    );
    assert_eq!(image.mapping(), setup::mapping());
    cluster.finish();
}

fn discard(parent: &mut super::parent::Parent) -> Result<(), ReplicaError> {
    parent.driver.discard_failed(&mut ReplicaParts {
        owner: &mut parent.owner,
        persistence: &mut parent.worker,
        applications: &mut parent.apps,
        results: &mut parent.routers.results,
        clients: &mut parent.routers.clients,
        reads: &mut parent.routers.reads,
        outbound: &mut parent.outbound,
        snapshots: None,
    })
}
fn pure_reply(parent: &mut super::parent::Parent) {
    use voteboat::raft::*;
    let sender = StoreBinding {
        identity: super::support::identity(102),
        session: StoreSession::new(1).unwrap(),
    };
    parent
        .owner
        .admit(
            setup::mapping().parent,
            Event::Receive(Message {
                group: setup::mapping().parent,
                configuration: ConfigurationId::new(1).unwrap(),
                from: super::support::node(12),
                sender,
                to: setup::mapping().voter,
                term: 1,
                context: RequestContext {
                    origin: sender,
                    sequence: 1,
                },
                rpc: Rpc::AuthorityRequest {
                    candidate: PeerIdentity {
                        node: super::support::node(13),
                        store: super::support::identity(103),
                    },
                    configuration: ConfigurationId::new(2).unwrap(),
                },
            }),
        )
        .unwrap();
    parent.poll();
    use voteboat::worker::PersistenceWorker;
    // This core response changes no durable parent state, but still requires
    // actual child authorization; worker-only gating cannot release it.
    assert_eq!(parent.driver.release_usage().pending, 1);
    assert!(parent.worker.is_drained());
    assert!(parent.take_sends().is_empty());
}
#[test]
fn accepted_capsule_retains_original_parent_credits_until_actual_failed_cleanup_observation() {
    use std::time::{Duration, Instant};
    use voteboat::{outbound::OutboundQueue, worker::PersistenceWorker};
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        super::support::node(1),
    )
    .unwrap();
    let image = super::drive::claim_in(&mut bridge, clock, 7021, false);
    let mut parent = super::parent::Parent::new(&image);
    pure_reply(&mut parent);
    assert!(bridge.pump_release(parent.driver.releases()).unwrap());
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            LogicalBridgeBudget {
                node_polls: 3,
                completions: 0,
                transitions: 1,
                node: NodePollBudget::default(),
            },
        )
        .unwrap();
    assert_eq!(bridge.usage().child_attempts, 1);
    let retained = parent.driver.release_usage();
    let owner_credits = parent.owner.usage();
    parent.owner.advance(MonoTime(1), 1).unwrap();
    assert_eq!(
        parent.owner.advance(MonoTime(0), 1).unwrap_err(),
        EffectOwnerError::Runtime(RuntimeError::ClockRegressed)
    );
    assert!(parent.owner.is_failed());
    assert_eq!(
        discard(&mut parent),
        Err(ReplicaError::Logical(LogicalReleaseError::Overloaded))
    );
    assert_eq!(parent.driver.release_usage(), retained);
    assert_eq!(parent.owner.usage(), owner_credits);
    assert!(!parent.driver.is_drained());
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        bridge
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                LogicalBridgeBudget {
                    node_polls: 3,
                    completions: 4,
                    transitions: 1,
                    node: NodePollBudget::default(),
                },
            )
            .unwrap();
        bridge.pump_release(parent.driver.releases()).unwrap();
        match discard(&mut parent) {
            Ok(()) => break,
            Err(ReplicaError::Logical(LogicalReleaseError::Overloaded)) => (),
            Err(error) => panic!("actual failed cleanup: {error:?}"),
        }
        assert!(Instant::now() < end, "accepted child original must settle");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(parent.owner.is_failed());
    assert_eq!(parent.owner.usage(), EffectOwnerUsage::default());
    assert!(parent.driver.is_drained());
    assert_eq!(
        parent.driver.release_usage(),
        LogicalReleaseUsage::default()
    );
    assert!(parent.take_sends().is_empty());
    assert!(bridge.is_drained());
    parent.worker.close();
    parent.outbound.close();
    assert!(parent.worker.is_drained());
    assert!(parent.port.is_drained());
    assert!(parent.outbound.is_drained());
    bridge.close();
    drop(bridge);
    authorized_terminal(&mut cluster, &image);
    cluster.finish();
}

fn authorized_terminal(cluster: &mut Cluster, image: &VerifiedLogicalImage) {
    let operation =
        OperationId::new((u128::from(image.representative().logical_session.get()) << 64) | 1)
            .unwrap();
    let LogicalRead::Outcome(Some(status)) = cluster.read(LogicalQuery::Outcome(operation)) else {
        panic!("original accepted capsule lacks fresh child quorum outcome");
    };
    assert_eq!(status.operation, operation);
    assert_eq!(status.receipt.operation, operation);
    assert_eq!(status.receipt.outcome, LogicalOutcome::CapsuleAuthorized);
    assert_eq!(status.receipt.epoch, Some(image.representative().epoch));
    assert!(status.first_index > image.claim().first_index);
    assert_eq!(status.receipt.first_index, status.first_index);
    assert!(!status.command.is_empty());
}

fn tracked_input(
    parent: &mut super::parent::Parent,
    rpc: voteboat::raft::Rpc,
    sequence: u64,
) -> AdmissionTicket {
    use voteboat::raft::{Event, Message, RequestContext};
    let sender = StoreBinding {
        identity: super::support::identity(102),
        session: StoreSession::new(1).unwrap(),
    };
    parent
        .owner
        .admit_tracked(
            setup::mapping().parent,
            Event::Receive(Message {
                group: setup::mapping().parent,
                configuration: ConfigurationId::new(1).unwrap(),
                from: super::support::node(12),
                sender,
                to: setup::mapping().voter,
                term: 7,
                context: RequestContext {
                    origin: sender,
                    sequence,
                },
                rpc,
            }),
        )
        .unwrap()
}
fn ordering_pump(
    bridge: &mut super::aliases::Bridge<'_>,
    parent: &mut super::parent::Parent,
    clock: std::time::Instant,
    completions: usize,
) -> ReplicaProgress {
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            LogicalBridgeBudget {
                node_polls: 3,
                completions,
                transitions: 1,
                node: NodePollBudget::default(),
            },
        )
        .unwrap();
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    parent.poll()
}
fn pending_append(
    parent: &super::parent::Parent,
    later: AdmissionTicket,
    progress: &ReplicaProgress,
) {
    use voteboat::{application::StateMachine, worker::PersistenceWorker};
    assert!(progress
        .steps
        .iter()
        .all(|step| step.admission != Some(later)));
    assert_eq!(progress.persistence_batches, 0);
    assert_eq!(parent.worker.usage().requests, 0);
    assert_eq!(parent.owner.usage().persistence_requests, 0);
    assert_eq!(parent.driver.release_usage().pending, 1);
    let core = parent.owner.core(setup::mapping().parent).unwrap();
    assert_eq!(core.state().hard_state.term, 7);
    assert_eq!(core.state().last_index(), 0);
    assert_eq!(core.state().commit_index, 0);
    assert_eq!(parent.apps[&setup::mapping().parent].applied_index(), 0);
}
#[test]
fn already_admitted_append_cannot_overtake_retained_original_vote_capsule() {
    use std::time::{Duration, Instant};
    use voteboat::{log::*, raft::Rpc};
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        super::support::node(1),
    )
    .unwrap();
    let image = super::drive::claim_in(&mut bridge, clock, 7022, false);
    let mut parent = super::parent::Parent::new(&image);
    let vote = tracked_input(
        &mut parent,
        Rpc::Vote {
            last_index: 0,
            last_term: 0,
        },
        1,
    );
    let entry = LogEntry {
        index: 1,
        term: 7,
        payload: EntryPayload::Command {
            operation: OperationId::new(8051).unwrap(),
            bytes: 5_i64.to_le_bytes().to_vec().into(),
        },
    };
    let later = tracked_input(
        &mut parent,
        Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![entry.clone()],
            leader_commit: 1,
        },
        2,
    );
    assert!(later.sequence > vote.sequence);
    let first = parent.poll();
    assert_eq!(first.steps.len(), 1);
    assert_eq!(first.steps[0].admission, Some(vote));
    assert_eq!(first.persistence_batches, 1);
    let end = Instant::now() + Duration::from_secs(15);
    while parent.driver.release_usage().pending == 0 {
        let progress = ordering_pump(&mut bridge, &mut parent, clock, 4);
        assert!(progress
            .steps
            .iter()
            .all(|step| step.admission != Some(later)));
        assert!(
            Instant::now() < end,
            "first original vote response must be captured"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(bridge.pump_release(parent.driver.releases()).unwrap());
    let retained = parent.driver.release_usage();
    for _ in 0..32 {
        let progress = ordering_pump(&mut bridge, &mut parent, clock, 0);
        pending_append(&parent, later, &progress);
        assert_eq!(parent.driver.release_usage(), retained);
        assert!(parent.take_sends().is_empty());
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut messages = Vec::new();
    let mut later_observed = false;
    while !bridge.is_drained() || !parent.is_drained() {
        let progress = ordering_pump(&mut bridge, &mut parent, clock, 4);
        later_observed |= progress
            .steps
            .iter()
            .any(|step| step.admission == Some(later));
        messages.extend(parent.take_sends());
        assert!(Instant::now() < end, "both original admissions must settle");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(later_observed);
    assert_completed_order(&parent, &messages, entry);
    bridge.close();
    drop(bridge);
    parent.close();
    cluster.finish();
}

fn assert_completed_order(
    parent: &super::parent::Parent,
    messages: &[voteboat::raft::Message],
    entry: voteboat::log::LogEntry,
) {
    use voteboat::{application::StateMachine, raft::Rpc};
    assert_eq!(messages.len(), 2);
    assert!(matches!(messages[0].rpc, Rpc::Voted { granted: true }));
    assert!(matches!(
        messages[1].rpc,
        Rpc::Appended {
            success: true,
            matching_index: 1
        }
    ));
    assert_eq!(messages[0].context.sequence, 1);
    assert_eq!(messages[1].context.sequence, 2);
    assert_eq!(
        parent
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .entries,
        vec![entry]
    );
    assert_eq!(parent.apps[&setup::mapping().parent].applied_index(), 1);
    assert_eq!(
        parent.apps[&setup::mapping().parent]
            .0
            .read_applied(1)
            .unwrap(),
        5
    );
}

#[path = "authority/snapshot_ack.rs"]
mod snapshot_ack;
