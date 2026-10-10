// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Benchmark assembly: one WAL, snapshot worker and peer endpoint per replica.
use super::observe::{ObservedIo, ObservedLog, Trace};
use super::*;
pub(super) type SharedLog = ObservedLog<NativeLogStore<ObservedIo<FileLogIo>>>;
use voteboat::{
    connect::*,
    dial::*,
    native::{
        connect::*, dial::*, log_store::*, outbound::*, runtime::*, snapshot_store::*,
        snapshot_worker::*, transport::*, wire::*, worker::*,
    },
    outbound::*,
    secure::*,
    snapshot::*,
    snapshot_worker::*,
    transport::*,
    worker::*,
};

pub(super) fn open(
    root: &Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &Instant,
    groups: usize,
) -> Result<(Vec<Replica<SharedLog>>, Vec<Trace>), Failure> {
    open_partition(
        root,
        mode,
        protocol,
        capacity,
        clock,
        Partition {
            offset: 0,
            groups,
            lane: 1,
        },
    )
}

#[derive(Clone, Copy)]
pub(super) struct Partition {
    pub offset: usize,
    pub groups: usize,
    pub lane: u64,
}

pub(super) fn open_partition(
    root: &Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &Instant,
    partition: Partition,
) -> Result<(Vec<Replica<SharedLog>>, Vec<Trace>), Failure> {
    if partition.groups == 0
        || partition.groups > 32
        || partition.lane == 0
        || partition.lane > 4
        || partition
            .offset
            .checked_add(partition.groups)
            .is_none_or(|end| end > 32)
    {
        return Err("invalid lane partition".into());
    }
    // Hold every endpoint reservation until all addresses have been selected.
    let reservations = (0..3)
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                    let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                    Some((tcp, udp))
                })
                .ok_or("no local TCP/UDP endpoint")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let addresses = (1..=3)
        .zip(reservations.iter().map(|(t, _)| t.local_addr().unwrap()))
        .collect::<BTreeMap<_, _>>();
    let policy = checked(Policy::new(
        Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
        Limits::default(),
    ))?;
    let bootstraps = (partition.offset + 1..=partition.offset + partition.groups)
        .map(|g| Bootstrap {
            group: group_id(g),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: policy.clone(),
            voter_stores: (1..=3)
                .map(|n| (node(n), store((partition.lane - 1) * 3 + n)))
                .collect(),
        })
        .collect::<Vec<_>>();
    drop(reservations);
    let mut replicas = Vec::new();
    let mut traces = Vec::new();
    for n in 1..=3 {
        let setup = ClusterSetup {
            root,
            mode,
            protocol,
            capacity,
            clock,
            bootstraps: &bootstraps,
            addresses: &addresses,
            lane: partition.lane,
        };
        let (replica, trace) = match setup.open_replica(n) {
            Ok(opened) => opened,
            Err(error) => return failure::cleanup(replicas, clock, root, error),
        };
        replicas.push(replica);
        traces.push(trace);
    }
    Ok((replicas, traces))
}

fn open_log(
    directory: &Path,
    identity: StoreIdentity,
    create: bool,
    bootstraps: &[Bootstrap],
    trace: &Trace,
) -> Result<SharedLog, Failure> {
    let log = if create {
        let mut log = ObservedLog {
            inner: checked(NativeLogStore::create(
                ObservedIo {
                    inner: FileLogIo::create(directory)?,
                    trace: trace.clone(),
                },
                identity,
                LogLimits::default(),
            ))?,
            trace: trace.clone(),
        };
        let tickets = checked(
            log.append_batch(
                bootstraps
                    .iter()
                    .cloned()
                    .map(LogMutation::Create)
                    .collect(),
            ),
        )?;
        checked(log.barrier(&tickets))?;
        log
    } else {
        ObservedLog {
            inner: NativeLogStore::recover(
                ObservedIo {
                    inner: FileLogIo::open(directory)?,
                    trace: trace.clone(),
                },
                identity,
                LogLimits::default(),
            )?,
            trace: trace.clone(),
        }
    };
    Ok(log)
}
struct RecoveredGroups {
    cores: Vec<voteboat::raft::Raft>,
    applications: BTreeMap<GroupIdentity, Counter>,
    snapshots: BTreeMap<GroupIdentity, NativeSnapshotStore<FileSnapshotIo>>,
}
fn recover_groups(
    directory: &Path,
    n: u64,
    create: bool,
    bootstraps: &[Bootstrap],
    capacity: usize,
    log: &SharedLog,
    member: bool,
) -> Result<RecoveredGroups, Failure> {
    let mut cores = Vec::new();
    let mut applications = BTreeMap::new();
    let mut snapshots = BTreeMap::new();
    for bootstrap in bootstraps {
        if log.state(bootstrap.group)?.bootstrap != *bootstrap {
            return Err("recovered bootstrap differs from selected cluster".into());
        }
        let path = directory.join(format!("snapshot-{}", bootstrap.group.id.get()));
        let identity = SnapshotIdentity {
            store: log.binding().identity,
            group: bootstrap.group,
        };
        let mut snap = if create {
            NativeSnapshotStore::create(
                FileSnapshotIo::create(path)?,
                identity,
                SnapshotLimits::default(),
            )?
        } else {
            NativeSnapshotStore::recover(
                FileSnapshotIo::open(path)?,
                identity,
                SnapshotLimits::default(),
            )?
        };
        let mut app = checked(Counter::new(capacity))?;
        let core = checked(if member {
            recover_member_replica(node(n), bootstrap.group, log, &mut snap, &mut app)
        } else {
            recover_replica(node(n), bootstrap.group, log, &mut snap, &mut app)
        })?
        .0;
        cores.push(if member {
            core.with_committed_snapshot_repair()
                .with_configuration_replication()
        } else {
            core
        });
        applications.insert(bootstrap.group, app);
        snapshots.insert(bootstrap.group, snap);
    }
    Ok(RecoveredGroups {
        cores,
        applications,
        snapshots,
    })
}
fn timed_shard(
    cores: Vec<voteboat::raft::Raft>,
    owner_id: RuntimeOwner,
    n: u64,
    groups: usize,
    clock: &Instant,
) -> Result<
    (
        MonoTime,
        TimedShard<FairScheduler, DeadlineQueue, JitterEntropy>,
    ),
    Failure,
> {
    let mut shard = checked(Shard::new(
        owner_id,
        ShardLimits {
            max_groups: groups,
            ..ShardLimits::default()
        },
        checked(FairScheduler::new(groups))?,
    ))?;
    for core in cores {
        checked(shard.register(core))?;
    }
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    let timed = checked(TimedShard::new(
        shard,
        checked(DeadlineQueue::new(owner_id, groups))?,
        JitterEntropy::new(n + 17),
        TimerConfig {
            heartbeat_ms: 50,
            election_min_ms: 10000,
            election_spread_ms: 10000,
            expirations_per_poll: 32,
        },
        now,
    ))?;
    Ok((now, timed))
}
struct Routers {
    ingress: IngressRouter,
    results: ApplicationRouter<CounterReceipt>,
    clients: ClientRouter<CounterReceipt>,
    reads: ReadRequests<(), i64>,
}
fn application_routers(owner_id: RuntimeOwner, local: LocalIdentity) -> Result<Routers, Failure> {
    let ingress = checked(IngressRouter::new(
        IngressBinding {
            owner: owner_id,
            local,
            generation: IngressGeneration::new(1).unwrap(),
        },
        IngressLimits::default(),
    ))?;
    let results = checked(ApplicationRouter::new(
        ApplicationRouterBinding {
            owner: owner_id,
            generation: ApplicationRouterGeneration::new(1).unwrap(),
        },
        ApplicationRouterLimits::default(),
    ))?;
    let clients = checked(ClientRouter::new(
        ClientRouterBinding {
            owner: owner_id,
            generation: ClientRouterGeneration::new(1).unwrap(),
        },
        ClientRouterLimits::default(),
    ))?;
    let reads = checked(ReadRequests::new(
        ReadInvocationBinding {
            owner: owner_id,
            generation: ReadInvocationGeneration::new(1).unwrap(),
        },
        ReadInvocationLimits::default(),
        checked(ReadRouter::new(
            ReadRouterBinding {
                owner: owner_id,
                generation: ReadRouterGeneration::new(1).unwrap(),
            },
            ReadRouterLimits::default(),
        ))?,
    ))?;
    Ok(Routers {
        ingress,
        results,
        clients,
        reads,
    })
}
fn connector(
    n: u64,
    local: LocalIdentity,
    remote: &BTreeMap<NodeId, StoreIdentity>,
    addresses: &BTreeMap<u64, std::net::SocketAddr>,
    profile: (NativePeerProtocol, NativeTlsConfig),
    wake: Arc<dyn WorkerWake>,
    now: MonoTime,
) -> Result<NativeServiceConnector, Failure> {
    let pins = remote
        .iter()
        .map(|(p, s)| {
            (
                *p,
                TlsPeer {
                    identity: PeerIdentity {
                        node: *p,
                        store: *s,
                    },
                    certificate: cert(p.get()).to_vec(),
                    server_name: format!("node{}.voteboat.test", p.get()),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let config = NativeConnectConfig {
        local,
        limits: ConnectLimits::default(),
        session: SessionLimits::default(),
    };
    let (protocol, tls) = profile;
    let connector = match protocol {
        NativePeerProtocol::TcpTls => {
            let dialer = checked(NativeTcpDialer::spawn(
                local,
                remote.clone(),
                DialLimits::default(),
                wake,
            ))?;
            NativeServiceConnector::Tcp(Box::new(checked(NativePeerConnector::new(
                config,
                tls,
                pins,
                dialer,
                Some(TcpListener::bind(addresses[&n])?),
                now,
            ))?))
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => NativeServiceConnector::Quic(Box::new(checked(
            voteboat::native::quic_connect::NativeQuicConnector::new(
                config,
                tls,
                pins.into_iter()
                    .map(|(p, pin)| (p, (addresses[&p.get()], pin)))
                    .collect(),
                UdpSocket::bind(addresses[&n])?,
                now,
            ),
        )?)),
    };
    Ok(connector)
}
struct ClusterSetup<'a> {
    root: &'a Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &'a Instant,
    bootstraps: &'a [Bootstrap],
    addresses: &'a BTreeMap<u64, std::net::SocketAddr>,
    lane: u64,
}
impl ClusterSetup<'_> {
    fn open_replica(&self, n: u64) -> Result<(Replica<SharedLog>, Trace), Failure> {
        self.open_replica_with_recovery(n, None)
    }
    fn open_replica_with_recovery(
        &self,
        n: u64,
        recovery: Option<SnapshotRecoveryLimits>,
    ) -> Result<(Replica<SharedLog>, Trace), Failure> {
        self.open_profile(n, recovery, false)
    }
    fn open_profile(
        &self,
        n: u64,
        recovery: Option<SnapshotRecoveryLimits>,
        member: bool,
    ) -> Result<(Replica<SharedLog>, Trace), Failure> {
        let Self {
            root,
            mode,
            protocol,
            capacity,
            clock,
            bootstraps,
            addresses,
            lane,
        } = *self;
        let directory = root.join(format!("replica{n}"));
        let create = mode == NativeOpenMode::Create;
        let trace = Trace::default();
        let identity = bootstraps.first().ok_or("empty lane")?.voter_stores[&node(n)];
        let log = open_log(&directory, identity, create, bootstraps, &trace)?;
        let RecoveredGroups {
            cores,
            applications,
            snapshots,
        } = recover_groups(&directory, n, create, bootstraps, capacity, &log, member)?;
        let owner_id = RuntimeOwner {
            store: log.binding(),
            lane: ExecutionLaneId::new(lane).ok_or("invalid lane identity")?,
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        let first = owner_id
            .store
            .session
            .get()
            .checked_mul(10000)
            .ok_or("session exhausted")?;
        let last = first.checked_add(9999).ok_or("session exhausted")?;
        let local = LocalIdentity {
            node: node(n),
            store: owner_id.store,
        };
        let remote = checked(PeerAssignments::from_cores(
            local,
            cores.iter(),
            PeerRosterLimits::default().peers,
        ))?
        .peers()
        .collect::<BTreeMap<_, _>>();
        let (now, timed) = timed_shard(cores, owner_id, n, bootstraps.len(), clock)?;
        let (version, codec) = wire_profile(member)?;
        let (outbound, roster) = peer_io(local, first, last, &remote, now, version)?;
        let Routers {
            ingress,
            results,
            clients,
            reads,
        } = application_routers(owner_id, local)?;
        let factory = checked(NativeTransportFactory::new(codec, Default::default()))?;
        let wake: Arc<dyn WorkerWake> = Arc::new(ThreadWake::current());
        let mut workers = spawn_workers(log, timed, snapshots, owner_id, wake.clone())?;
        workers.limit_recovery(recovery)?;
        let Workers {
            owner,
            persistence,
            snapshots: snapshot_workers,
        } = workers;
        let profile = (protocol, checked(tls(n).with_wire_version(version))?);
        let connector = connector(n, local, &remote, addresses, profile, wake, now)?;
        let parts = NodeParts {
            local: NodeLocalParts {
                owner,
                persistence,
                applications,
                results,
                clients,
                reads,
                outbound,
                snapshots: Some(snapshot_workers),
            },
            peers: Some(PeerParts {
                admission_routes: None,
                connector,
                roster,
                factory,
                ingress,
                routes: remote
                    .keys()
                    .map(|p| {
                        (
                            *p,
                            if node(n) < *p {
                                ConnectDirection::Dial(addresses[&p.get()])
                            } else {
                                ConnectDirection::Accept
                            },
                        )
                    })
                    .collect(),
            }),
        };
        // Failed construction retains its directory; successful runs join workers explicitly.
        let replica = Node::from_parts(parts, NodeLimits::default(), now)
            .map_err(|e| format!("node assembly failed: {:?}", e.reason))?;
        Ok((replica, trace))
    }
}

struct Workers {
    owner: EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>,
    persistence: NativeLogWorker<SharedLog>,
    snapshots: NodeSnapshots<NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>>,
}
fn wire_profile(member: bool) -> Result<(u16, NativeWireCodec), Failure> {
    if member {
        Ok((
            8,
            checked(NativeWireCodec::with_leadership_transfer(Default::default()))?,
        ))
    } else {
        Ok((1, checked(NativeWireCodec::new(Default::default()))?))
    }
}
impl Workers {
    fn limit_recovery(&mut self, limits: Option<SnapshotRecoveryLimits>) -> Result<(), Failure> {
        if let Some(limits) = limits {
            self.snapshots.router = checked(SnapshotRouter::new_with_recovery_limits(
                self.owner.identity(),
                self.snapshots.worker.binding(),
                SnapshotRouterLimits::default(),
                limits,
            ))?;
        }
        Ok(())
    }
}
fn spawn_workers(
    log: SharedLog,
    timed: TimedShard<FairScheduler, DeadlineQueue, JitterEntropy>,
    snapshots: BTreeMap<GroupIdentity, NativeSnapshotStore<FileSnapshotIo>>,
    owner_id: RuntimeOwner,
    wake: Arc<dyn WorkerWake>,
) -> Result<Workers, Failure> {
    let persistence = checked(NativeLogWorker::spawn(
        log,
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        wake.clone(),
    ))?;
    let owner = checked(EffectOwner::new(
        timed,
        persistence.binding(),
        EffectOwnerLimits::default(),
    ))?;
    let worker = checked(NativeSnapshotWorker::spawn(
        snapshots,
        SnapshotWorkerBinding {
            store: owner_id.store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        },
        SnapshotWorkLimits::default(),
        wake.clone(),
    ))?;
    let router = checked(SnapshotRouter::new(
        owner.identity(),
        worker.binding(),
        SnapshotRouterLimits::default(),
    ))?;
    Ok(Workers {
        owner,
        persistence,
        snapshots: NodeSnapshots { router, worker },
    })
}

fn peer_io(
    local: LocalIdentity,
    first: u64,
    last: u64,
    remote: &BTreeMap<NodeId, StoreIdentity>,
    now: MonoTime,
    version: u16,
) -> Result<(NativeOutbound, BenchmarkRoster), Failure> {
    let outbound = checked(NativeOutbound::new(
        OutboundBinding {
            node: local.node,
            store: local.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        OutboundLimits::default(),
    ))?;
    let roster = checked(PeerRoster::new(
        PeerRosterConfig {
            local,
            outbound: outbound.binding(),
            first_generation: SecureSessionGeneration::new(first).ok_or("session exhausted")?,
            last_generation: SecureSessionGeneration::new(last).ok_or("session exhausted")?,
            wire_version: version,
            limits: PeerRosterLimits::default(),
            transport_limits: TransportLimits::default(),
        },
        remote.clone(),
        now,
    ))?;
    Ok((outbound, roster))
}

type BenchmarkRoster = PeerRoster<NativePeerTransport<Box<dyn SecureSession>, NativeWireCodec>>;

#[cfg(test)]
#[path = "shared_drain.rs"]
mod drain;
#[cfg(test)]
#[path = "shared_recovery.rs"]
mod recovery;
