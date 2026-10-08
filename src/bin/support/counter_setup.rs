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
//! Explicit native assembly shared by the service example and its process tests.
use std::{collections::BTreeMap, net::TcpListener, path::Path, sync::Arc};
use voteboat::{
    application::*,
    connect::*,
    dial::*,
    identity::*,
    log::*,
    native::{
        connect::*, dial::*, log_store::*, node::*, outbound::*, runtime::*, snapshot_store::*,
        snapshot_worker::*, tls::*, transport::*, wire::*, worker::*,
    },
    outbound::*,
    quorum::*,
    runtime::*,
    secure::*,
    snapshot::*,
    snapshot_worker::*,
    transport::*,
    worker::*,
};
pub type Failure = Box<dyn std::error::Error>;
pub type Service = NativeNode<Counter>;
pub fn checked<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, Failure> {
    value.map_err(|e| format!("{e:?}").into())
}
pub fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store_id(id: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id.into()).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn material(path: &Path) -> Result<Vec<u8>, Failure> {
    // Bounded before allocation, including files changed after metadata inspection.
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(65537)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > 65536 {
        return Err("invalid TLS material size".into());
    }
    Ok(bytes)
}
/// Each process has one explicit WAL, one snapshot store and three native workers.
/// `create` never falls back to recovery; recovery never creates a missing store.
pub fn open(root: &Path, id: u64, base: u16, tls: &Path, create: bool) -> Result<Service, Failure> {
    if !(1..=3).contains(&id) || base == 0 || base > 65432 {
        return Err("node must be 1..3 and base port must be 1..65432".into());
    }
    let peers = (1..=3)
        .map(|n| (node(n), store_id(n)))
        .collect::<BTreeMap<_, _>>();
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: checked(Policy::new(
            Tree::Majority(peers.keys().copied().map(Tree::Voter).collect()),
            Limits::default(),
        ))?,
        voter_stores: peers.clone(),
    };
    // Check credentials/listener before modifying storage or starting workers.
    let credentials = checked(NativeTlsConfig::new(TlsCredentials {
        roots: vec![material(&tls.join("ca.der"))?],
        certificate_chain: vec![material(&tls.join(format!("node{id}.der")))?],
        private_key: material(&tls.join(format!("node{id}-key.der")))?,
    }))?;
    let peer_certificates = (1..=3)
        .filter(|n| *n != id)
        .map(|n| Ok((node(n), material(&tls.join(format!("node{n}.der")))?)))
        .collect::<Result<BTreeMap<_, _>, Failure>>()?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, base + id as u16))?;
    let store = if create {
        let mut store =
            NativeLogStore::create(FileLogIo::create(root)?, store_id(id), LogLimits::default())?;
        let tickets = store.append_batch(vec![LogMutation::Create(bootstrap.clone())])?;
        store.barrier(&tickets)?;
        store
    } else {
        NativeLogStore::recover(FileLogIo::open(root)?, store_id(id), LogLimits::default())?
    };
    let state = store.state(group())?;
    if state.bootstrap != bootstrap {
        return Err("recovered bootstrap differs from selected cluster".into());
    }
    let mut snapshots = if create {
        NativeSnapshotStore::create(
            FileSnapshotIo::create(root.join("snapshots"))?,
            SnapshotIdentity {
                store: store_id(id),
                group: group(),
            },
            SnapshotLimits::default(),
        )?
    } else {
        NativeSnapshotStore::recover(
            FileSnapshotIo::open(root.join("snapshots"))?,
            SnapshotIdentity {
                store: store_id(id),
                group: group(),
            },
            SnapshotLimits::default(),
        )?
    };
    let mut app = checked(Counter::new(10000))?;
    let core = checked(recover_replica(
        node(id),
        group(),
        &store,
        &mut snapshots,
        &mut app,
    ))?
    .0;
    let owner_id = RuntimeOwner {
        store: store.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    // Store session makes each startup's transport generations disjoint.
    let first = owner_id
        .store
        .session
        .get()
        .checked_mul(10000)
        .ok_or("session exhausted")?;
    let last = first.checked_add(9999).ok_or("session exhausted")?;
    let local = LocalIdentity {
        node: node(id),
        store: owner_id.store,
    };
    let mut shard = checked(Shard::new(
        owner_id,
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        checked(FairScheduler::new(1))?,
    ))?;
    checked(shard.register(core))?;
    let timed = checked(TimedShard::new(
        shard,
        checked(DeadlineQueue::new(owner_id, 1))?,
        JitterEntropy::new(id * 17),
        TimerConfig::default(),
        MonoTime(0),
    ))?;
    let outbound = checked(NativeOutbound::new(
        OutboundBinding {
            node: node(id),
            store: owner_id.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        OutboundLimits::default(),
    ))?;
    let remote = peers
        .into_iter()
        .filter(|(n, _)| *n != node(id))
        .collect::<BTreeMap<_, _>>();
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
        MonoTime(0),
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
    let wake = Arc::new(ThreadWake::current());
    let dialer = checked(NativeTcpDialer::spawn(
        local,
        remote.clone(),
        DialLimits::default(),
        wake.clone(),
    ))?;
    let connector = NativePeerConnector::new(
        NativeConnectConfig {
            local,
            limits: ConnectLimits::default(),
            session: SessionLimits::default(),
        },
        credentials,
        remote
            .iter()
            .map(|(n, s)| {
                (
                    *n,
                    TlsPeer {
                        identity: PeerIdentity {
                            node: *n,
                            store: *s,
                        },
                        certificate: peer_certificates[n].clone(),
                        server_name: format!("node{}.voteboat.test", n.get()),
                    },
                )
            })
            .collect(),
        dialer,
        Some(listener),
        MonoTime(0),
    )
    .map_err(|r| format!("connector: {:?}", r.reason))?;
    let worker = checked(NativeLogWorker::spawn(
        store,
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        wake.clone(),
    ))?;
    let owner = checked(EffectOwner::new(
        timed,
        worker.binding(),
        EffectOwnerLimits::default(),
    ))?;
    let snapshot_worker = checked(NativeSnapshotWorker::spawn(
        [(group(), snapshots)].into(),
        SnapshotWorkerBinding {
            store: owner_id.store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        },
        SnapshotWorkLimits::default(),
        wake,
    ))?;
    let router = checked(SnapshotRouter::new(
        owner.identity(),
        snapshot_worker.binding(),
        SnapshotRouterLimits::default(),
    ))?;
    NativeNode::from_parts(
        NativeNodeParts {
            local: NativeLocalParts {
                owner,
                persistence: worker,
                applications: [(group(), app)].into(),
                results,
                clients,
                reads,
                outbound,
                snapshots: Some(NodeSnapshots {
                    router,
                    worker: snapshot_worker,
                }),
            },
            peers: Some(PeerParts {
                connector,
                roster,
                factory,
                ingress,
                routes: remote
                    .keys()
                    .map(|n| {
                        (
                            *n,
                            if node(id) < *n {
                                ConnectDirection::Dial(
                                    (std::net::Ipv4Addr::LOCALHOST, base + n.get() as u16).into(),
                                )
                            } else {
                                ConnectDirection::Accept
                            },
                        )
                    })
                    .collect(),
            }),
        },
        NodeLimits::default(),
        MonoTime(0),
    )
    .map_err(|r| format!("node: {:?}", r.reason).into())
}
/// Call only after Node has reached Drained. Polling while draining is the host's job.
pub fn join(service: Service) -> Result<(), Failure> {
    use std::time::{Duration, Instant};
    let mut parts = service.into_parts().map_err(|_| "node is not drained")?;
    let mut dialer = parts
        .peers
        .take()
        .ok_or("missing peers")?
        .connector
        .into_dialer()
        .map_err(|_| "connector not drained")?;
    let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut log_done = false;
    let mut snapshots_done = false;
    loop {
        let dial_done = checked(dialer.try_finish())?;
        if !log_done {
            log_done = parts.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshots_done {
            snapshots_done = snapshots.worker.try_reclaim()?.is_some();
        }
        if dial_done && log_done && snapshots_done {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("worker join timed out".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
