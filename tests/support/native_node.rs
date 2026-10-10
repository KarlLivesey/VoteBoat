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

use voteboat::native::{node::*, transport::NativeTransportFactory, wire::NativeWireCodec};
type Session =
    <voteboat::native::connect::NativePeerConnector as voteboat::connect::PeerConnector>::Session;
type Facade<F = NativeTransportFactory<NativeWireCodec>> =
    NativeNode<Counter, voteboat::native::connect::NativePeerConnector, F>;
fn facade_make(root: &std::path::Path, recover: bool) -> Vec<Facade> {
    facade_make_with(root, recover, |_| {
        NativeTransportFactory::new(
            NativeWireCodec::new(Default::default()).unwrap(),
            Default::default(),
        )
        .unwrap()
    })
}
fn facade_make_with<F: voteboat::transport::PeerTransportFactory<Session>>(
    root: &std::path::Path,
    recover: bool,
    factory: impl FnMut(&Node) -> F,
) -> Vec<Facade<F>> {
    let mut fixtures = (1..=3)
        .map(|id| make_snapshots(id, &root.join(id.to_string()), recover, true))
        .collect::<Vec<_>>();
    let network = mesh_parts(&mut fixtures, factory);
    fixtures
        .into_iter()
        .zip(network)
        .map(|(n, peers)| {
            NativeNode::from_parts(
                NativeNodeParts {
                    local: NativeLocalParts {
                        owner: n.owner,
                        persistence: n.worker,
                        applications: n.apps,
                        results: n.results,
                        clients: n.clients,
                        reads: n.read_requests,
                        outbound: n.outbound,
                        snapshots: Some(NodeSnapshots {
                            router: n.router.unwrap(),
                            worker: n.snapshots.unwrap(),
                        }),
                    },
                    peers: Some(peers),
                },
                NodeLimits::default(),
                MonoTime(0),
            )
            .unwrap_or_else(|r| panic!("node construction: {:?}", r.reason))
        })
        .collect()
}
fn facade_drive<F: voteboat::transport::PeerTransportFactory<Session>>(
    nodes: &mut [Facade<F>],
    mut done: impl FnMut(&mut [Facade<F>]) -> bool,
) {
    facade_drive_at(nodes, MonoTime(0), &mut done)
}
fn facade_drive_at<F: voteboat::transport::PeerTransportFactory<Session>>(
    nodes: &mut [Facade<F>],
    now: MonoTime,
    mut done: impl FnMut(&mut [Facade<F>]) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for n in nodes.iter_mut() {
            let progress = n
                .poll(
                    now,
                    NodePollBudget {
                        replica: ReplicaPollBudget {
                            steps: 100,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
            if let Some(p) = progress.replica {
                for step in p.steps {
                    assert!(step.error.is_none(), "{:?}", step.error);
                }
            }
        }
        if done(nodes) {
            return;
        }
        assert!(Instant::now() < deadline, "facade progress timed out");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn facade_proposals<F: voteboat::transport::PeerTransportFactory<Session>>(
    nodes: &mut [Facade<F>],
    operation: u128,
    delta: i64,
    expected: i64,
) {
    for g in 1..=100 {
        nodes[0]
            .propose(ClientRequest {
                group: group(g),
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
    }
    let mut replies = 0;
    facade_drive(nodes, |nodes| {
        while let Some(reply) = nodes[0].poll_client() {
            assert!(matches!(
                nodes[0].complete_client(reply).unwrap(),
                ClientOutcome::Applied { .. }
            ));
            replies += 1;
        }
        replies == 100
            && nodes.iter().all(|n| {
                n.local()
                    .applications
                    .values()
                    .all(|app| app.read_applied(app.applied_index()) == Ok(expected))
            })
    });
}
fn facade_close<F: voteboat::transport::PeerTransportFactory<Session>>(
    nodes: Vec<Facade<F>>,
) -> usize {
    facade_close_at(nodes, MonoTime(0))
}
fn facade_close_at<F: voteboat::transport::PeerTransportFactory<Session>>(
    mut nodes: Vec<Facade<F>>,
    now: MonoTime,
) -> usize {
    let mut reclaimed = 0;
    for n in &mut nodes {
        n.begin_shutdown();
    }
    facade_drive_at(&mut nodes, now, |nodes| {
        nodes.iter().all(|n| n.is_drained())
    });
    for n in nodes {
        let mut p = n
            .into_parts()
            .unwrap_or_else(|_| panic!("node not reclaimable"));
        let mut dialer = p
            .peers
            .take()
            .unwrap()
            .connector
            .into_dialer()
            .unwrap_or_else(|_| panic!("connector not drained"));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dialer.try_finish().unwrap() {
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let mut snapshots = p.local.snapshots.take().unwrap();
        loop {
            if let Some(stores) = snapshots.worker.try_reclaim().unwrap() {
                drop(stores);
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        loop {
            if let Some(mut store) = p.local.persistence.try_reclaim().unwrap() {
                let before = (1..=100)
                    .map(|g| store.state(group(g)).unwrap())
                    .collect::<Vec<_>>();
                let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
                reclaimed += report.before_bytes - report.after_bytes;
                for (g, expected) in (1..=100).zip(before) {
                    assert_eq!(store.state(group(g)).unwrap(), expected);
                }
                drop(store);
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    reclaimed
}
include!("native_pressure.rs");
#[test]
fn owning_native_facade_checkpoints_restarts_and_retries_hundred_groups() {
    let root = std::env::temp_dir().join(format!("voteboat-node-facade-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = facade_make(&root, false);
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        nodes.iter().all(|n| {
            n.local()
                .applications
                .values()
                .all(|a| a.read_applied(a.applied_index()) == Ok(7))
        })
    });
    facade_proposals(&mut nodes, 2, 3, 10);
    for g in 1..=100 {
        nodes[0].read(group(g), ()).unwrap();
    }
    let mut reads = 0;
    facade_drive(&mut nodes, |nodes| {
        while let Some(reply) = nodes[0].poll_read() {
            assert!(matches!(
                nodes[0].complete_read(reply).unwrap(),
                ReadOutcome::Read { result: Ok(10), .. }
            ));
            reads += 1;
        }
        reads == 100
    });
    for start in (1..=100).step_by(8) {
        for n in &mut nodes {
            for g in start..=(start + 7).min(100) {
                n.control(group(g), NodeControl::Checkpoint).unwrap();
            }
        }
        facade_drive(&mut nodes, |nodes| {
            nodes.iter().all(|n| {
                let local = n.local();
                local.owner.is_drained()
                    && n.replica_usage().leases() == 0
                    && local
                        .snapshots
                        .as_ref()
                        .is_some_and(|s| s.router.is_drained() && s.worker.is_drained())
                    && (start..=(start + 7).min(100)).all(|g| {
                        local.owner.core(group(g)).unwrap().state().base_index()
                            == local.applications[&group(g)].applied_index()
                    })
            })
        });
    }
    let maintenance = nodes
        .iter_mut()
        .map(|n| n.reclaim(LogLimits::default().max_wal_bytes).unwrap())
        .collect::<Vec<_>>();
    // New accepted commands continue through the same workers after their
    // queued replacement, without moving or closing any selected provider.
    facade_proposals(&mut nodes, 3, 4, 14);
    let mut reclaimed = 0;
    facade_drive(&mut nodes, |nodes| {
        nodes
            .iter()
            .all(|n| n.replica_usage().reclaims == 1 && n.local().persistence.is_drained())
    });
    for (n, ticket) in nodes.iter_mut().zip(maintenance) {
        let event = n.poll_reclaim().unwrap();
        assert_eq!(event.request, ticket);
        let report = event.result.unwrap();
        reclaimed += report.before_bytes - report.after_bytes;
    }
    assert!(reclaimed > 0);
    facade_scheduled_reclaim(&mut nodes);
    let bindings = nodes
        .iter()
        .map(|n| n.local().owner.identity().store)
        .collect::<Vec<_>>();
    facade_close_at(nodes, MonoTime(1));
    let mut nodes = facade_make(&root, true);
    for (n, old) in nodes.iter().zip(bindings) {
        assert_ne!(n.local().owner.identity().store, old);
    }
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        (1..=100).all(|g| nodes[0].local().owner.core(group(g)).unwrap().role() == Role::Leader)
    });
    facade_proposals(&mut nodes, 2, 3, 14);
    facade_proposals(&mut nodes, 3, 4, 14);
    facade_proposals(&mut nodes, 4, 1, 15);
    facade_close(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

fn facade_scheduled_reclaim(nodes: &mut [Facade]) {
    for n in nodes.iter_mut() {
        n.configure_wal_maintenance(Some(WalMaintenancePolicy {
            interval_ms: 1, retry_ms: 1, max_bytes: LogLimits::default().max_wal_bytes,
        })).unwrap();
    }
    facade_drive_at(nodes, MonoTime(1), |nodes| nodes.iter().all(|n| n.wal_maintenance().last_completion.is_some()));
    let mut reclaimed = 0;
    for n in nodes {
        let state = n.wal_maintenance();
        let report = state.last_completion.as_ref().unwrap().result.as_ref().unwrap();
        reclaimed += report.before_bytes - report.after_bytes;
        assert!(state.pending.is_none());
        assert_eq!(state.next_deadline, Some(MonoTime(2)));
        assert_eq!(n.replica_usage().reclaims, 0);
        assert!(n.poll_reclaim().is_none());
    }
    assert!(reclaimed > 0);
}

include!("native_checkpoints.rs");
