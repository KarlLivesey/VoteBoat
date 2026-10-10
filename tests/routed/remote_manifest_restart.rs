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
use super::*;
use std::path::{Path, PathBuf};

struct Started {
    children: Vec<Node<Child>>,
    parents: Vec<Node<Directory>>,
    client: Remote,
    server: Server,
    route: RouteHint,
    grant: ResponsibilityManifest,
    cache: NativeManifestCache,
}
fn start(root: &Path, clock: &Instant, protocol: NativePeerProtocol) -> Started {
    let mut parents = initialized_directory(root, clock, protocol);
    let (mut client, mut server) = remote_pair(parents.remove(0), protocol, clock);
    let mut cache = NativeManifestCache::new(limits()).unwrap();
    let route = remote_route(&mut client, &mut server, &mut parents, clock, &mut cache);
    assert_eq!(client.usage().manifests, 3);
    let grant = cache.get(responsibility(2)).unwrap().clone();
    let mut children = open(
        configuration(root, 20, &[1, 2], NativeOpenMode::Create),
        clock,
        protocol,
        || child(20, grant.clone()),
    );
    campaign(&mut children, clock, 20);
    assert_eq!(
        propose(
            &mut children,
            clock,
            20,
            10000,
            child(20, grant.clone())
                .bootstrap_command(MAX_ROUTED_COMMAND_BYTES)
                .unwrap()
        )
        .outcome,
        RoutedOutcome::Bootstrapped
    );
    serve_cached_child(&mut cache, route, &mut children, clock);
    Started {
        children,
        parents,
        client,
        server,
        route,
        grant,
        cache,
    }
}
fn parent_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    collect(root, &root.join("1"), &mut files);
    assert!(!files.is_empty());
    files
}
fn bindings(nodes: &[Node<Child>]) -> Vec<StoreBinding> {
    nodes
        .iter()
        .map(|n| n.local().reads.binding().owner.store)
        .collect()
}
fn reopen(
    root: &Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    grant: &ResponsibilityManifest,
    original: &[StoreBinding],
    base: u64,
    expected: i64,
) -> Vec<Node<Child>> {
    let mut children = open(
        configuration(root, 20, &[1, 2], NativeOpenMode::Recover),
        clock,
        protocol,
        || child(20, grant.clone()),
    );
    for node in &children {
        let app = &node.local().applications[&group(20)];
        assert!(app.is_initialized());
        assert_eq!(
            app.application().read_applied(app.applied_index()),
            Ok(expected)
        );
        assert_eq!(app.grant(), grant);
        let local = node.local().reads.binding().owner.store;
        let prior = original
            .iter()
            .find(|b| b.identity == local.identity)
            .unwrap();
        assert_ne!(prior.session, local.session);
        assert_eq!(
            node.local()
                .owner
                .core(group(20))
                .unwrap()
                .state()
                .base_index(),
            base
        );
    }
    campaign(&mut children, clock, 20);
    children
}
fn recovered_route(
    children: &[Node<Child>],
    clock: &Instant,
    expected: RouteHint,
    source: &mut Remote,
) -> RouteHint {
    let mut cache = NativeManifestCache::new(limits()).unwrap();
    // No ancestor manifest is rehydrated by a recovered child's local grant.
    let request = |start, max_hops, lookups| DiscoverRouteRequest {
        start,
        key: &[10],
        max_hops,
        lookups,
        now: now(clock),
    };
    assert_eq!(
        resolve_discovered(&mut cache, &HostPolicy, source, request(query(1), 3, 3)),
        Err(ManifestDiscoveryError::Closed)
    );
    let app = &children[0].local().applications[&group(20)];
    cache.admit(app.grant().clone()).unwrap();
    assert_eq!(cache.usage().manifests, 1);
    assert_eq!(
        resolve_discovered(&mut cache, &HostPolicy, source, request(query(1), 3, 3)),
        Err(ManifestDiscoveryError::Closed)
    );
    let route = resolve_discovered(
        &mut cache,
        &HostPolicy,
        &mut Offline,
        request(query(2), 1, 0),
    )
    .unwrap();
    assert_eq!(route, expected);
    route
}
fn write(
    children: &mut [Node<Child>],
    clock: &Instant,
    route: RouteHint,
    operation: u128,
    delta: i64,
    expected: i64,
    duplicate: bool,
) {
    let receipt = propose(
        children,
        clock,
        20,
        operation,
        encode_routed(route, &[10], &delta.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap(),
    );
    assert!(
        matches!(receipt.outcome,
        RoutedOutcome::Applied(CounterReceipt { outcome: CounterOutcome::Value(v), duplicate: d, .. })
        if v == expected && d == duplicate),
        "{receipt:?}"
    );
}
fn read_value(children: &mut [Node<Child>], clock: &Instant, route: RouteHint, expected: i64) {
    assert_eq!(
        read(
            children,
            clock,
            20,
            RoutedQuery {
                hint: route,
                key: vec![10],
                query: ()
            }
        ),
        RoutedRead::Served(expected)
    );
    reject_stale_hint(route, children, clock);
}
fn history(protocol: NativePeerProtocol, compact: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-offline-child-restart-{}-{protocol:?}-{compact}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let Started {
        mut children,
        mut parents,
        mut client,
        server,
        route,
        grant,
        cache,
    } = start(&root, &clock, protocol);
    assert_eq!(cache.usage().manifests, 3);
    finish_source(server, &mut client, &mut parents, &clock);
    close(parents, &clock, 1, || {
        drive(&mut children, &clock, |_| true);
    });
    client.close();
    assert_eq!(
        client
            .poll(now(&clock), SessionPollBudget::default())
            .unwrap()
            .unwrap()
            .result,
        Err(ManifestDiscoveryError::Closed.into())
    );
    drop(cache);
    let before = parent_files(&root);
    if compact {
        checkpoint(&mut children, &clock, 20);
    }
    let original = bindings(&children);
    let base = children[0]
        .local()
        .owner
        .core(group(20))
        .unwrap()
        .state()
        .base_index();
    assert_eq!(base > 0, compact);
    close(children, &clock, 20, || {});
    let mut children = reopen(&root, &clock, protocol, &grant, &original, base, 7);
    let route = recovered_route(&children, &clock, route, &mut client);
    write(&mut children, &clock, route, 1, 7, 7, true);
    write(&mut children, &clock, route, 2, 3, 10, false);
    read_value(&mut children, &clock, route, 10);
    if compact {
        checkpoint(&mut children, &clock, 20);
    }
    let original = bindings(&children);
    let base = children[0]
        .local()
        .owner
        .core(group(20))
        .unwrap()
        .state()
        .base_index();
    close(children, &clock, 20, || {});
    let mut children = reopen(&root, &clock, protocol, &grant, &original, base, 10);
    let route = recovered_route(&children, &clock, route, &mut client);
    // A retry returns its original receipt value, not the later application value.
    write(&mut children, &clock, route, 1, 7, 7, true);
    write(&mut children, &clock, route, 2, 3, 10, true);
    write(&mut children, &clock, route, 3, 5, 15, false);
    read_value(&mut children, &clock, route, 15);
    close(children, &clock, 20, || {});
    assert!(client.into_parts().is_ok());
    assert_eq!(
        parent_files(&root),
        before,
        "offline metadata files changed"
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_child_reopens_wal_twice_while_all_metadata_is_offline() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_child_reopens_checkpoints_twice_while_all_metadata_is_offline() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_child_reopens_wal_twice_while_all_metadata_is_offline() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_child_reopens_checkpoints_twice_while_all_metadata_is_offline() {
    history(NativePeerProtocol::Quic, true);
}
