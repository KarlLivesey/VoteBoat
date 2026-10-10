// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "metadata_endpoint_source.rs"]
mod source;
use source::{now, View};
fn moved(env: &Environment<'_>) -> (Vec<Node<Owner>>, NativeManifestCache, Vec<OfflineMetadata>) {
    let (mut a, _) = initialize_source(env);
    let mut parent = initialize_parent(env);
    let mut owners = initialize_owner(env);
    let mut cache = initialize_cache();
    write(&mut owners, env.clock, &cache, 1, 7);
    let mut b = first_move(env, &mut a);
    cache_first(env, &mut b.nodes, &mut cache);
    parent_refresh(
        env,
        &mut parent,
        &mut cache,
        b.plan.clone(),
        b.activation,
        400,
    );
    owner_adopt(env, &mut owners, b.plan.clone(), b.activation, 300);
    write(&mut owners, env.clock, &cache, 2, 3);
    let mut c = second_move(env, &mut b);
    cache_second(env, &mut c.nodes, &mut cache);
    parent_refresh(
        env,
        &mut parent,
        &mut cache,
        c.plan.clone(),
        c.activation,
        401,
    );
    owner_adopt(env, &mut owners, c.plan, c.activation, 301);
    let records = vec![
        OfflineMetadata::capture(env, &mut a, 1),
        OfflineMetadata::capture(env, &mut b.nodes, 9),
        OfflineMetadata::capture(env, &mut c.nodes, 11),
        OfflineMetadata::capture(env, &mut parent, 100),
    ];
    (owners, cache, records)
}
fn binding(node: &Node<Owner>) -> Option<voteboat::secure::SessionBinding> {
    node.peers().unwrap().roster().binding(support::node(3))
}
fn write_checked(
    env: &Environment<'_>,
    nodes: &mut [Node<Owner>],
    cache: &NativeManifestCache,
    operation: u128,
    delta: i64,
    expected: i64,
    duplicate: bool,
) {
    campaign(nodes, env.clock, 20);
    let bytes = encode_routed(
        hint(cache),
        &[1],
        &encode_add(&[1], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let receipt = propose_recovering(nodes, env.clock, 20, operation, bytes);
    let RoutedOutcome::Applied(receipt) = receipt.outcome else {
        panic!("{receipt:?}")
    };
    assert_eq!(receipt.operation, OperationId::new(operation).unwrap());
    assert_eq!(
        receipt.outcome,
        voteboat::bucket_counter::BucketOutcome::Value(expected)
    );
    assert_eq!(receipt.duplicate, duplicate);
}
fn refresh(
    env: &Environment<'_>,
    owners: &mut Vec<Node<Owner>>,
    addresses: &mut BTreeMap<NodeId, std::net::SocketAddr>,
    view: &View,
) {
    owners.sort_by_key(|n| n.local().owner.core(group(20)).unwrap().local_node());
    drive(owners, env.clock, |nodes| {
        nodes[..2].iter().all(|n| binding(n).is_some())
    });
    let old = owners[..2]
        .iter()
        .map(|n| binding(n).unwrap().generation)
        .collect::<Vec<_>>();
    view.disconnect_sources();
    let high = owners.pop().unwrap();
    close(vec![high], env.clock, 20, || {
        drive(owners, env.clock, |_| true);
    });
    let mut changed = configuration(env.root, 20, &[1, 2, 3], NativeOpenMode::Recover)
        .pop()
        .unwrap();
    for (peer, config) in &mut changed.peers {
        config.address = addresses[peer];
    }
    addresses.insert(changed.node, changed.listen);
    view.update(&changed, 1);
    let changed_peer = (changed.node, changed.store, changed.listen);
    owners.push(source::open(changed, view, env));
    for node in &mut owners[..2] {
        node.disconnect(support::node(3), now(env.clock)).unwrap();
    }
    drive(owners, env.clock, |_| view.lower_peers_refused_stale());
    assert!(view.lower_peers_repaired());
    assert!(owners[..2].iter().all(|n| binding(n).is_none()));
    view.publish(changed_peer, 3);
    drive(owners, env.clock, |nodes| {
        nodes[..2]
            .iter()
            .zip(&old)
            .all(|(n, old)| binding(n).is_some_and(|b| b.generation > *old))
    });
}
fn run(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-moved-endpoints-{}-{protocol:?}-{checkpoint}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let env = Environment {
        root: &root,
        clock: &clock,
        protocol,
        checkpoint,
    };
    let (mut owners, cache, records) = moved(&env);
    if checkpoint {
        compact(&mut owners, &clock, 20);
    }
    let original = owners[0].local().owner.identity().store;
    close(owners, &clock, 20, || {});
    let configs = configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover);
    let mut addresses = configs.iter().map(|c| (c.node, c.listen)).collect();
    let view = View::new(&configs);
    let mut owners = configs
        .into_iter()
        .map(|c| source::open(c, &view, &env))
        .collect::<Vec<_>>();
    assert_ne!(
        owners[0].local().owner.identity().store.session,
        original.session
    );
    for node in &owners {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .grant()
                .input()
                .authority,
            group(11)
        );
    }
    write_checked(&env, &mut owners, &cache, 1, 7, 7, true);
    assert_eq!(value(&mut owners, &clock, &cache), 10);
    refresh(&env, &mut owners, &mut addresses, &view);
    write_checked(&env, &mut owners, &cache, 1, 7, 7, true);
    write_checked(&env, &mut owners, &cache, 3, 5, 15, false);
    assert_eq!(value(&mut owners, &clock, &cache), 15);
    if checkpoint {
        compact(&mut owners, &clock, 20);
    }
    close(owners, &clock, 20, || {});
    let mut owners = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        owner,
    );
    write_checked(&env, &mut owners, &cache, 1, 7, 7, true);
    write_checked(&env, &mut owners, &cache, 2, 3, 10, true);
    write_checked(&env, &mut owners, &cache, 3, 5, 15, true);
    assert_eq!(value(&mut owners, &clock, &cache), 15);
    for node in &owners {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count(),
            3
        );
    }
    close(owners, &clock, 20, || {});
    for record in records {
        record.verify(&env);
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn moved_metadata_endpoint_refresh_tcp_wal() {
    run(NativePeerProtocol::TcpTls, false);
}
#[test]
fn moved_metadata_endpoint_refresh_tcp_checkpoint() {
    run(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn moved_metadata_endpoint_refresh_quic_wal() {
    run(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn moved_metadata_endpoint_refresh_quic_checkpoint() {
    run(NativePeerProtocol::Quic, true);
}
