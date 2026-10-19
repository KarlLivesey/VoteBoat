// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{serial, Cluster},
    host::Host,
    setup,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::*,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    runtime::*,
    worker::PersistenceWorker,
};
fn write(host: &mut Host<'_>, duplicate: bool, outage: bool) {
    let durable = host.parents[0]
        .owner
        .core(setup::mapping(0).parent)
        .unwrap()
        .state()
        .clone();
    if outage {
        host.paused_child = Some(0);
    }
    let parent = &mut host.parents[0];
    let ticket = parent
        .routers
        .clients
        .submit(
            &mut parent.owner,
            &parent.apps[&parent.mapping.parent],
            ClientRequest {
                group: parent.mapping.parent,
                operation: Counter::operation(1, 1).unwrap(),
                bytes: Counter::command(0, 5),
            },
        )
        .unwrap();
    if outage {
        host.until(|h| h.parents[0].worker.usage().requests == 1);
        for _ in 0..128 {
            host.poll();
            assert_eq!(
                host.parents[0]
                    .owner
                    .core(setup::mapping(0).parent)
                    .unwrap()
                    .state(),
                &durable
            );
            assert_eq!(host.parents[0].worker.usage().requests, 1);
            assert!(host.parents[0].routers.clients.poll().is_none());
            assert_eq!(
                host.parents[0].apps[&setup::mapping(0).parent].applied_index(),
                durable.commit_index
            );
        }
        host.paused_child = None;
    }
    let end = Instant::now() + Duration::from_secs(15);
    let output = loop {
        host.poll();
        if let Some(output) = host.parents[0].routers.clients.poll() {
            break output;
        }
        assert!(Instant::now() < end, "actual logical parent client timeout");
    };
    assert_eq!(output.ticket(), ticket);
    let charged = host.parents[0].routers.clients.usage();
    for _ in 0..8 {
        host.poll();
        assert_eq!(host.parents[0].routers.clients.usage(), charged);
    }
    let ClientOutcome::Applied { position, receipt } =
        host.parents[0].routers.clients.complete(output).unwrap()
    else {
        panic!("actual original client success");
    };
    assert_eq!(receipt.operation, ticket.operation);
    assert_eq!(receipt.outcome, CounterOutcome::Value(5));
    assert_eq!(receipt.duplicate, duplicate);
    assert_eq!(receipt.index, position.index);
    host.until(|h| {
        h.is_drained()
            && h.parents
                .iter()
                .all(|p| p.apps[&p.mapping.parent].applied_index() >= position.index)
    });
    for parent in &host.parents {
        assert_eq!(
            parent.apps[&parent.mapping.parent]
                .read_at(position.index, ())
                .unwrap(),
            5
        );
    }
}
fn read(host: &mut Host<'_>) {
    let parent = &mut host.parents[0];
    let ticket = parent
        .routers
        .reads
        .submit(
            &mut parent.owner,
            &parent.apps[&parent.mapping.parent],
            parent.mapping.parent,
            (),
        )
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    let output = loop {
        host.poll();
        if let Some(output) = host.parents[0].routers.reads.poll() {
            break output;
        }
        assert!(Instant::now() < end, "actual logical quorum read timeout");
    };
    assert_eq!(output.ticket(), ticket);
    let charged = host.parents[0].routers.reads.usage();
    for _ in 0..8 {
        host.poll();
        assert_eq!(host.parents[0].routers.reads.usage(), charged);
    }
    let ReadOutcome::Read {
        barrier,
        result: Ok(value),
    } = host.parents[0].routers.reads.complete(output).unwrap()
    else {
        panic!("actual fresh read");
    };
    assert_eq!(barrier.group(), ticket.group);
    assert_eq!(barrier.request(), ticket.request);
    assert_eq!(
        barrier.index(),
        host.parents[0].apps[&setup::mapping(0).parent].applied_index()
    );
    assert_eq!(value, 5);
    host.until(Host::is_drained);
}
fn assembly_refusals(child: &mut Cluster) {
    let mapping = setup::mapping(child.slot);
    let mut empty = Vec::<super::cluster::ChildNode>::new();
    assert!(matches!(
        voteboat::logical_voter::LogicalChildBridge::new(&mut empty, mapping, child.representative),
        Err(voteboat::logical_voter::LogicalBridgeError::InvalidAssembly)
    ));
    let before = child
        .nodes
        .iter()
        .map(|n| {
            (
                n.local().owner.identity(),
                n.local().clients.usage(),
                n.local().reads.usage(),
                n.local().owner.core(mapping.child).unwrap().state().clone(),
            )
        })
        .collect::<Vec<_>>();
    let missing = child
        .nodes
        .iter()
        .position(|n| {
            n.local().owner.core(mapping.child).unwrap().local_node() != child.representative
        })
        .unwrap();
    assert!(matches!(
        voteboat::logical_voter::LogicalChildBridge::new(
            &mut child.nodes[missing..missing + 1],
            mapping,
            child.representative
        ),
        Err(voteboat::logical_voter::LogicalBridgeError::InvalidAssembly)
    ));
    let after = child
        .nodes
        .iter()
        .map(|n| {
            (
                n.local().owner.identity(),
                n.local().clients.usage(),
                n.local().reads.usage(),
                n.local().owner.core(mapping.child).unwrap().state().clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(before, after);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut children = std::array::from_fn(|slot| Cluster::new(protocol, slot));
    assembly_refusals(&mut children[0]);
    let mut host = Host::new(&mut children, protocol, 9201);
    host.elect(0);
    write(&mut host, false, true);
    host.close();
    for child in &mut children {
        child.checkpoint();
    }
    let bases = children
        .iter()
        .map(|c| {
            c.all()
                .map(|n| {
                    n.local()
                        .owner
                        .core(setup::mapping(c.slot).child)
                        .unwrap()
                        .state()
                        .base_index()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut suffix = Host::new(&mut children, protocol, 9251);
    suffix.elect(0);
    write(&mut suffix, true, false);
    read(&mut suffix);
    let original_representative = suffix.representatives[0];
    let previous = suffix.close();
    for (child, bases) in children.iter_mut().zip(bases) {
        child.catch_up();
        for (node, base) in child.all().zip(bases) {
            let state = node
                .local()
                .owner
                .core(setup::mapping(child.slot).child)
                .unwrap()
                .state();
            assert_eq!(state.base_index(), base);
            assert!(state.last_index() > base);
        }
        child.close();
        if child.slot == 0 {
            child.representative = super::support::node(2);
        }
        child.open(NativeOpenMode::Recover);
    }
    let mut recovered = Host::new(&mut children, protocol, 9301);
    let replacement = recovered.representatives[0];
    assert_eq!(replacement.physical.node, super::support::node(2));
    assert_eq!(replacement.physical.store, super::support::identity(1002));
    assert_ne!(replacement.physical, original_representative.physical);
    assert!(replacement.epoch.get() > original_representative.epoch.get());
    assert!(replacement.logical_session > original_representative.logical_session);
    assert_eq!(recovered.parents[0].mapping, setup::mapping(0));
    for (parent, expected) in recovered.parents.iter().zip(&previous) {
        assert_eq!(
            parent.owner.core(parent.mapping.parent).unwrap().state(),
            expected
        );
        assert_eq!(
            parent.apps[&parent.mapping.parent]
                .read_at(expected.commit_index, ())
                .unwrap(),
            5
        );
    }
    recovered.elect(0);
    write(&mut recovered, true, false);
    read(&mut recovered);
    recovered.close();
    for child in children {
        child.finish();
    }
}
#[test]
fn tcp_three_logical_voters_use_nine_native_child_owners_and_real_quorum_results() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_three_logical_voters_use_nine_native_child_owners_and_real_quorum_results() {
    history(NativePeerProtocol::Quic);
}
