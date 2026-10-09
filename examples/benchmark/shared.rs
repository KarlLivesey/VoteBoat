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
type SharedLog = ObservedLog<NativeLogStore<ObservedIo<FileLogIo>>>;
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
    let bootstraps = (1..=groups)
        .map(|g| Bootstrap {
            group: group_id(g),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: policy.clone(),
            voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
        })
        .collect::<Vec<_>>();
    drop(reservations);
    let mut replicas = Vec::new();
    let mut traces = Vec::new();
    for n in 1..=3 {
        let directory = root.join(format!("replica{n}"));
        let create = mode == NativeOpenMode::Create;
        let trace = Trace::default();
        let log = if create {
            let mut log = ObservedLog {
                inner: checked(NativeLogStore::create(
                    ObservedIo {
                        inner: FileLogIo::create(&directory)?,
                        trace: trace.clone(),
                    },
                    store(n),
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
                        inner: FileLogIo::open(&directory)?,
                        trace: trace.clone(),
                    },
                    store(n),
                    LogLimits::default(),
                )?,
                trace: trace.clone(),
            }
        };
        let mut cores = Vec::new();
        let mut applications = BTreeMap::new();
        let mut snapshots = BTreeMap::new();
        for bootstrap in &bootstraps {
            if log.state(bootstrap.group)?.bootstrap != *bootstrap {
                return Err("recovered bootstrap differs from selected cluster".into());
            }
            let path = directory.join(format!("snapshot-{}", bootstrap.group.id.get()));
            let identity = SnapshotIdentity {
                store: store(n),
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
            cores.push(
                checked(recover_replica(
                    node(n),
                    bootstrap.group,
                    &log,
                    &mut snap,
                    &mut app,
                ))?
                .0,
            );
            applications.insert(bootstrap.group, app);
            snapshots.insert(bootstrap.group, snap);
        }
        let owner_id = RuntimeOwner {
            store: log.binding(),
            lane: ExecutionLaneId::new(1).unwrap(),
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
        let outbound = checked(NativeOutbound::new(
            OutboundBinding {
                node: node(n),
                store: owner_id.store,
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
                wire_version: 1,
                limits: PeerRosterLimits::default(),
                transport_limits: TransportLimits::default(),
            },
            remote.clone(),
            now,
        ))?;
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
        let factory = checked(NativeTransportFactory::new(
            checked(NativeWireCodec::new(Default::default()))?,
            Default::default(),
        ))?;
        let wake: Arc<dyn WorkerWake> = Arc::new(ThreadWake::current());
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
                    tls(n),
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
                    tls(n),
                    pins.into_iter()
                        .map(|(p, pin)| (p, (addresses[&p.get()], pin)))
                        .collect(),
                    UdpSocket::bind(addresses[&n])?,
                    now,
                ),
            )?)),
        };
        let parts = NodeParts {
            local: NodeLocalParts {
                owner,
                persistence,
                applications,
                results,
                clients,
                reads,
                outbound,
                snapshots: Some(NodeSnapshots { router, worker }),
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
        // Failed runs are invalid and retain their directory; only successful
        // runs advertise workers_joined after the explicit close/reopen gates.
        replicas.push(
            Node::from_parts(parts, NodeLimits::default(), now)
                .map_err(|e| format!("node assembly failed: {:?}", e.reason))?,
        );
        traces.push(trace);
    }
    Ok((replicas, traces))
}
