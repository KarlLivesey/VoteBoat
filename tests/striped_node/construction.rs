// SPDX-License-Identifier: RPL-1.5
use super::{cluster::Cluster, peers, support};
use std::{collections::BTreeMap, sync::Arc};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{
        connect::NativeServiceConnector,
        log_store::{striped::*, NativeLogStore},
        outbound::NativeOutbound,
        runtime::*,
        snapshot_store::*,
        snapshot_worker::NativeSnapshotWorker,
        startup::NativeTimingProfile,
        transport::NativeTransportFactory,
        wire::NativeWireCodec,
        worker::*,
    },
    outbound::*,
    runtime::*,
    snapshot::*,
    snapshot_worker::*,
    worker::*,
};
pub type Store = StripedLogStore<FileStripedIo>;
pub type Boat = Node<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    Counter,
    NativeLogWorker<Store>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativeServiceConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
type Local = NodeLocalParts<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    Counter,
    NativeLogWorker<Store>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
>;
pub fn open(host: &Cluster, id: u64, recover: bool) -> Boat {
    let store = load_store(host, id, recover);
    let mut snapshots = load_snapshots(host, id, recover);
    let mut app = Counter::new(100).unwrap();
    let core = recover_replica(
        support::node(id),
        host.group,
        &store,
        &mut snapshots,
        &mut app,
    )
    .unwrap()
    .0;
    // Check reconstruction before any worker, scheduler or network can progress.
    if recover {
        assert_eq!(app.read_applied(app.applied_index()).unwrap(), 14);
        assert!(core.state().base_index() > 0);
        assert!(core.state().commit_index > core.state().base_index());
    }
    let runtime = RuntimeOwner {
        store: store.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        runtime,
        ShardLimits {
            max_groups: 1,
            ..Default::default()
        },
        FairScheduler::new(1).unwrap(),
    )
    .unwrap();
    shard.register(core).unwrap();
    let timed = TimedShard::new(
        shard,
        DeadlineQueue::new(runtime, 1).unwrap(),
        JitterEntropy::new(id * 17),
        NativeTimingProfile::Throughput.timers(),
        host.now(),
    )
    .unwrap();
    let wake = Arc::new(ThreadWake::current());
    let worker = NativeLogWorker::spawn(
        store,
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        wake.clone(),
    )
    .unwrap();
    let owner = EffectOwner::new(timed, worker.binding(), EffectOwnerLimits::default()).unwrap();
    let snapshots = NativeSnapshotWorker::spawn(
        [(host.group, snapshots)].into(),
        SnapshotWorkerBinding {
            store: runtime.store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        },
        SnapshotWorkLimits::default(),
        wake,
    )
    .unwrap();
    let router = SnapshotRouter::new(
        runtime,
        snapshots.binding(),
        SnapshotRouterLimits::default(),
    )
    .unwrap();
    let outbound = NativeOutbound::new(
        OutboundBinding {
            node: support::node(id),
            store: runtime.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        OutboundLimits::default(),
    )
    .unwrap();
    let peers = peers::make(host, runtime, outbound.binding(), id);
    let local = local(
        owner,
        worker,
        app,
        outbound,
        NodeSnapshots {
            router,
            worker: snapshots,
        },
        host.group,
    );
    Node::from_parts(
        NodeParts {
            local,
            peers: Some(peers),
        },
        NodeLimits::default(),
        host.now(),
    )
    .unwrap_or_else(|e| panic!("striped Node assembly: {:?}", e.reason))
}
fn load_store(host: &Cluster, id: u64, recover: bool) -> Store {
    let limits = LogLimits::default();
    let striped = StripedLimits {
        chunk_bytes: 256,
        ..Default::default()
    };
    if recover {
        NativeLogStore::recover_striped(
            FileStripedIo::open(host.lanes(id)).unwrap(),
            support::identity(id.into()),
            limits,
            striped,
        )
        .unwrap()
    } else {
        let mut store = NativeLogStore::create_striped(
            FileStripedIo::create(host.lanes(id)).unwrap(),
            support::identity(id.into()),
            limits,
            striped,
        )
        .unwrap();
        support::append(
            &mut store,
            vec![LogMutation::Create(support::bootstrap(
                host.group.id.get(),
                3,
            ))],
        );
        store
    }
}
fn load_snapshots(host: &Cluster, id: u64, recover: bool) -> NativeSnapshotStore<FileSnapshotIo> {
    let path = host.root.join(id.to_string()).join("snapshots");
    let identity = SnapshotIdentity {
        store: support::identity(id.into()),
        group: host.group,
    };
    if recover {
        NativeSnapshotStore::recover(
            FileSnapshotIo::open(path).unwrap(),
            identity,
            SnapshotLimits::default(),
        )
        .unwrap()
    } else {
        NativeSnapshotStore::create(
            FileSnapshotIo::create(path).unwrap(),
            identity,
            SnapshotLimits::default(),
        )
        .unwrap()
    }
}
fn local(
    owner: EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>,
    worker: NativeLogWorker<Store>,
    app: Counter,
    outbound: NativeOutbound,
    snapshots: NodeSnapshots<NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>>,
    group: GroupIdentity,
) -> Local {
    let auth = owner.authorization();
    Local {
        owner,
        persistence: worker,
        applications: BTreeMap::from([(group, app)]),
        outbound,
        snapshots: Some(snapshots),
        results: ApplicationRouter::new(
            ApplicationRouterBinding::new(auth, ApplicationRouterGeneration::new(1).unwrap()),
            ApplicationRouterLimits::default(),
        )
        .unwrap(),
        clients: ClientRouter::new(
            ClientRouterBinding::new(auth, ClientRouterGeneration::new(1).unwrap()),
            ClientRouterLimits::default(),
        )
        .unwrap(),
        reads: ReadRequests::new(
            ReadInvocationBinding::new(auth, ReadInvocationGeneration::new(1).unwrap()),
            ReadInvocationLimits::default(),
            ReadRouter::new(
                ReadRouterBinding::new(auth, ReadRouterGeneration::new(1).unwrap()),
                ReadRouterLimits::default(),
            )
            .unwrap(),
        )
        .unwrap(),
    }
}
