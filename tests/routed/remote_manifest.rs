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
#[path = "remote_manifest_restart.rs"]
mod restart;
use voteboat::{native::remote_manifest::*, secure::*};
#[path = "../support/manifest_sessions.rs"]
pub(in crate::native) mod sessions;
// The session helper shares this test's existing TLS fixtures.
mod tls {
    pub use crate::support::tls::*;
}
type Remote = NativeRemoteManifestDiscovery<Box<dyn SecureSession>, NativeManifestCache>;
type Server =
    NativeManifestResponder<Box<dyn SecureSession>, NativeManifestLookup<Node<Directory>>>;
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: support::node(id),
        store: StoreBinding {
            identity: support::identity(id.into()),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn remote_route(
    client: &mut Remote,
    server: &mut Server,
    others: &mut [Node<Directory>],
    clock: &Instant,
    cache: &mut NativeManifestCache,
) -> RouteHint {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let t = now(clock);
        poll_nodes(server.source_mut().source_mut(), others, clock);
        match server.source_mut().poll(t) {
            Ok(_)
            | Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Unavailable)) => (),
            other => panic!("source lookup: {other:?}"),
        }
        server.poll(t, SessionPollBudget::default()).unwrap();
        if let Some(result) = client.poll(t, SessionPollBudget::default()).unwrap() {
            assert!(result.result.is_ok(), "{result:?}");
        }
        match resolve_discovered(
            cache,
            &HostPolicy,
            client,
            DiscoverRouteRequest {
                start: query(1),
                key: &[10],
                max_hops: 3,
                lookups: 3,
                now: t,
            },
        ) {
            Ok(route) => return route,
            Err(ManifestDiscoveryError::Unavailable) => (),
            other => panic!("remote routing: {other:?}"),
        }
        assert!(Instant::now() < deadline, "remote path lookup stalled");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn remote_pair(
    source: Node<Directory>,
    protocol: NativePeerProtocol,
    clock: &Instant,
) -> (Remote, Server) {
    let source = NativeManifestLookup::new(source, limits(), 60_000, 10_000, 1, now(clock))
        .unwrap_or_else(|_| panic!("lookup construction"));
    let (a, b) = sessions::pair(protocol, local(2), local(1), clock);
    let source_peer = PeerIdentity {
        node: local(1).node,
        store: local(1).store.identity,
    };
    let client_peer = PeerIdentity {
        node: local(2).node,
        store: local(2).store.identity,
    };
    let client = NativeRemoteManifestDiscovery::new(
        a,
        source_peer,
        group(1),
        NativeManifestCache::new(limits()).unwrap(),
        RemoteManifestConfig::default(),
        now(clock),
    )
    .ok()
    .unwrap();
    let server = NativeManifestResponder::new(
        b,
        client_peer,
        group(1),
        source,
        RemoteManifestConfig::default(),
        now(clock),
    )
    .ok()
    .unwrap();
    (client, server)
}
// Closing the selected view must retain its accepted Node read until receipt.
fn finish_source(
    mut server: Server,
    client: &mut Remote,
    parents: &mut Vec<Node<Directory>>,
    clock: &Instant,
) {
    assert!(client.lookup(query(3), now(clock)).is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    while server.source_mut().pending().is_none() {
        server
            .poll(now(clock), SessionPollBudget::default())
            .unwrap();
        assert!(client
            .poll(now(clock), SessionPollBudget::default())
            .unwrap()
            .is_none());
        assert!(Instant::now() < deadline);
    }
    assert_eq!(
        server
            .source_mut()
            .source_mut()
            .local()
            .reads
            .usage()
            .requests,
        1
    );
    server.close();
    let (_, mut source) = server.into_parts();
    assert!(!source.is_drained());
    while !source.is_drained() {
        poll_nodes(source.source_mut(), parents, clock);
        match source.poll(now(clock)) {
            Ok(false) | Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Closed)) => {
            }
            other => panic!("closed source drain: {other:?}"),
        }
        assert!(Instant::now() < deadline);
    }
    // Cancellation releases the selected wait first; core completion still owns
    // its request credit until the original quorum work has drained.
    while source.source_mut().local().reads.usage().requests != 0 {
        poll_nodes(source.source_mut(), parents, clock);
        assert!(Instant::now() < deadline);
    }
    parents.insert(
        0,
        source
            .into_source()
            .unwrap_or_else(|_| panic!("source read still owned")),
    );
}
fn reject_stale_hint(route: RouteHint, children: &mut [Node<Child>], clock: &Instant) {
    let stale = RouteHint {
        epoch: OwnershipEpoch::new(route.epoch.get() + 1).unwrap(),
        ..route
    };
    assert!(matches!(
        read(
            children,
            clock,
            20,
            RoutedQuery {
                hint: stale,
                key: vec![10],
                query: ()
            }
        ),
        RoutedRead::Rejected(_)
    ));
}
fn history(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-remote-manifest-{}-{protocol:?}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut parents = initialized_directory(&root, &clock, protocol);
    let (mut client, mut server) = remote_pair(parents.remove(0), protocol, &clock);
    let mut cache = NativeManifestCache::new(limits()).unwrap();
    let route = remote_route(&mut client, &mut server, &mut parents, &clock, &mut cache);
    assert_eq!(route.group, group(20));
    assert_eq!(
        client.usage().manifests,
        3,
        "only root, selected service and child were fetched"
    );
    assert_eq!(
        server
            .source_mut()
            .source_mut()
            .local()
            .reads
            .usage()
            .requests,
        0
    );
    let grant = cache.get(responsibility(2)).unwrap().clone();
    let mut children = open(
        configuration(&root, 20, &[1, 2], NativeOpenMode::Create),
        &clock,
        protocol,
        || child(20, grant.clone()),
    );
    campaign(&mut children, &clock, 20);
    assert_eq!(
        propose(
            &mut children,
            &clock,
            20,
            10000,
            child(20, grant.clone())
                .bootstrap_command(MAX_ROUTED_COMMAND_BYTES)
                .unwrap()
        )
        .outcome,
        RoutedOutcome::Bootstrapped
    );
    finish_source(server, &mut client, &mut parents, &clock);
    let before = close(parents, &clock, 1, || {
        drive(&mut children, &clock, |_| true);
    });
    let cached = resolve_discovered(
        &mut cache,
        &HostPolicy,
        &mut client,
        DiscoverRouteRequest {
            start: query(1),
            key: &[10],
            max_hops: 3,
            lookups: 0,
            now: now(&clock),
        },
    )
    .unwrap();
    assert_eq!(cached, route);
    serve_cached_child(&mut cache, route, &mut children, &clock);
    reject_stale_hint(route, &mut children, &clock);
    close(children, &clock, 20, || {});
    client.close();
    let closed = client
        .poll(now(&clock), SessionPollBudget::default())
        .unwrap()
        .unwrap();
    assert_eq!(closed.result, Err(ManifestDiscoveryError::Closed.into()));
    assert!(client.into_parts().is_ok());
    for (n, log) in before.into_iter().enumerate() {
        let store = NativeLogStore::recover(
            FileLogIo::open(root.join(format!("1/{}", n + 1))).unwrap(),
            support::identity((n + 1) as u128),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(store.state(group(1)).unwrap(), log);
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_remote_manifest_route_uses_original_quorum_reads_and_child_fencing() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_remote_manifest_route_uses_original_quorum_reads_and_child_fencing() {
    history(NativePeerProtocol::Quic);
}
