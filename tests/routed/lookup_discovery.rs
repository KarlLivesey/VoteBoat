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
use voteboat::native::lookup_discovery::*;
#[path = "remote_manifest.rs"]
mod remote_manifest;
fn now(clock: &Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
fn limits() -> ManifestCacheLimits {
    ManifestCacheLimits {
        manifests: 8,
        bytes: 64 * 1024,
    }
}
fn query(id: u128) -> ManifestLookup {
    ManifestLookup {
        locator: AuthorityLocator {
            responsibility: responsibility(id),
            authority: group(1),
        },
        minimum_epoch: None,
        minimum_generation: None,
    }
}
fn poll_nodes(source: &mut Node<Directory>, others: &mut [Node<Directory>], clock: &Instant) {
    let budget = NodePollBudget {
        replica: ReplicaPollBudget {
            steps: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    source.poll(now(clock), budget).unwrap();
    for node in others {
        node.poll(now(clock), budget).unwrap();
    }
}
fn resolve_auto(
    driver: &mut NativeManifestLookup<Node<Directory>>,
    others: &mut [Node<Directory>],
    clock: &Instant,
    cache: &mut NativeManifestCache,
) -> RouteHint {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(driver.source_mut(), others, clock);
        match driver.poll(now(clock)) {
            Ok(_)
            | Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Unavailable)) => (),
            other => panic!("automatic read: {other:?}"),
        }
        match resolve_discovered(
            cache,
            &HostPolicy,
            driver,
            DiscoverRouteRequest {
                start: query(1),
                key: &[10],
                max_hops: 3,
                lookups: 3,
                now: now(clock),
            },
        ) {
            Ok(hint) => return hint,
            Err(ManifestDiscoveryError::Unavailable) => (),
            other => panic!("automatic route: {other:?}"),
        }
        assert!(Instant::now() < deadline, "automatic lookup stalled");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
// Independent host source keeps the real Node read alive after cancellation.
// This models best-effort cancellation with an actual late positive receipt.
struct RetainedRead {
    node: Node<Directory>,
    cancellations: usize,
}
impl ManifestReadSource for RetainedRead {
    type ReadResult = Option<ResponsibilityManifest>;
    fn binding(&self) -> ReadInvocationBinding {
        ManifestReadSource::binding(&self.node)
    }
    fn pending_reads(&self) -> usize {
        ManifestReadSource::pending_reads(&self.node)
    }
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<ResponsibilityIdentity>> {
        ManifestReadSource::submit(&mut self.node, request)
    }
    fn poll_result(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<
        Option<ReadOutcome<Option<ResponsibilityManifest>>>,
        ReadCompletionRejected<Option<ResponsibilityManifest>>,
    > {
        let result = ManifestReadSource::poll_result(&mut self.node, ticket)?;
        if let Some(outcome) = &result {
            assert!(matches!(
                outcome,
                ReadOutcome::Read {
                    result: Ok(Some(_)),
                    ..
                }
            ));
        }
        Ok(result)
    }
    fn cancel(&mut self, _: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.cancellations += 1;
        Ok(())
    }
}
fn initialized_directory(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
) -> Vec<Node<Directory>> {
    let mut nodes = open(
        configuration(root, 1, &[1, 2, 3], NativeOpenMode::Create),
        clock,
        protocol,
        directory,
    );
    campaign(&mut nodes, clock, 1);
    propose(
        &mut nodes,
        clock,
        1,
        10000,
        directory()
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
    );
    for (i, manifest) in manifests().into_iter().enumerate() {
        propose(
            &mut nodes,
            clock,
            1,
            10001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest,
            }
            .encode(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
        );
    }
    nodes
}
fn refresh_and_refuse_floor(
    driver: &mut NativeManifestLookup<Node<Directory>>,
    nodes: &mut [Node<Directory>],
    clock: &Instant,
) {
    // Expiry of unchanged metadata requires another original quorum read.
    let mut observations = Vec::new();
    for _ in 0..2 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            poll_nodes(driver.source_mut(), nodes, clock);
            driver.poll(now(clock)).unwrap();
            match driver.lookup(query(2), now(clock)) {
                Ok(observation) => {
                    observations.push(observation.observation);
                    break;
                }
                Err(ManifestDiscoveryError::Unavailable) => (),
                other => panic!("expiry refresh: {other:?}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        std::thread::park_timeout(Duration::from_millis(12));
        assert_eq!(
            driver.lookup(query(2), now(clock)).err(),
            Some(ManifestDiscoveryError::Unavailable)
        );
    }
    assert_ne!(observations[0], observations[1]);
    // Drain the refresh initiated by the final expiry before requesting a floor.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !driver.is_drained() {
        poll_nodes(driver.source_mut(), nodes, clock);
        driver.poll(now(clock)).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    // Below-floor demands initiate a fresh read and refuse the actual older data.
    let mut stricter = query(1);
    stricter.minimum_epoch = OwnershipEpoch::new(999);
    assert_eq!(
        driver.lookup(stricter, now(clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(driver.source_mut(), nodes, clock);
        match driver.poll(now(clock)) {
            Ok(false) => (),
            Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Routing(
                RoutingError::EpochRegression,
            ))) => break,
            other => panic!("floor read: {other:?}"),
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn cancel_and_recover(
    mut driver: NativeManifestLookup<Node<Directory>>,
    nodes: &mut Vec<Node<Directory>>,
    clock: &Instant,
) {
    // The source is returned without shutting down the metadata Node.
    driver.close();
    assert!(driver.is_drained());
    let source = driver
        .into_source()
        .unwrap_or_else(|_| panic!("source still held"));
    let source = RetainedRead {
        node: source,
        cancellations: 0,
    };
    let mut driver = NativeManifestLookup::new(source, limits(), 1000, 10000, 1, now(clock))
        .unwrap_or_else(|_| panic!("host source"));
    assert_eq!(
        driver.lookup(query(1), now(clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    driver.cancel_pending().unwrap();
    assert!(!driver.is_drained());
    assert_eq!(driver.source_mut().cancellations, 1);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(&mut driver.source_mut().node, nodes, clock);
        match driver.poll(now(clock)) {
            Ok(false) => (),
            Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Cancelled)) => break,
            other => panic!("late cancellation: {other:?}"),
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(driver.is_drained());
    assert_eq!(driver.source_mut().node.local().reads.usage().requests, 0);
    // A cancelled positive result was not cached: after retry delay a new read starts.
    std::thread::park_timeout(Duration::from_millis(2));
    assert_eq!(
        driver.lookup(query(1), now(clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let pending = driver.pending().unwrap();
    let (mut source, unresolved) = driver.into_recovery();
    assert_eq!(unresolved.unwrap().ticket, pending.ticket);
    // Recovery handoff keeps original Node ownership; explicit cancellation and
    // terminal receipt consumption release it before ordinary shutdown/join.
    source.node.cancel_read(pending.ticket).unwrap();
    loop {
        poll_nodes(&mut source.node, nodes, clock);
        if let Some(reply) = source.node.poll_read() {
            source.node.complete_read(reply).unwrap();
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    nodes.insert(0, source.node);
}
struct Offline;
impl ManifestDiscovery for Offline {
    fn lookup(
        &mut self,
        _: ManifestLookup,
        _: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        panic!("cached child unexpectedly contacted offline metadata");
    }
    fn invalidate(&mut self, _: AuthorityLocator, _: ManifestObservationId) -> bool {
        false
    }
    fn close(&mut self) {}
}
fn serve_cached_child(
    cache: &mut NativeManifestCache,
    route: RouteHint,
    children: &mut [Node<Child>],
    clock: &Instant,
) {
    let cached = resolve_discovered(
        cache,
        &HostPolicy,
        &mut Offline,
        DiscoverRouteRequest {
            start: query(2),
            key: &[10],
            max_hops: 1,
            lookups: 0,
            now: now(clock),
        },
    )
    .unwrap();
    assert_eq!(cached, route);
    let bytes =
        encode_routed(cached, &[10], &7i64.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap();
    for duplicate in [false, true] {
        assert!(matches!(
            propose(children, clock, 20, 1, bytes.clone()).outcome,
            RoutedOutcome::Applied(CounterReceipt { outcome: CounterOutcome::Value(7), duplicate: d, .. }) if d == duplicate
        ));
    }
    assert_eq!(
        read(
            children,
            clock,
            20,
            RoutedQuery {
                hint: cached,
                key: vec![10],
                query: ()
            }
        ),
        RoutedRead::Served(7)
    );
}
fn recover_lookup(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    original_bindings: &[(StoreIdentity, ReadInvocationBinding)],
    route: RouteHint,
    grant: &ResponsibilityManifest,
) -> RouteHint {
    // Fresh original read bindings and an empty cache after durable restart.
    let mut parents = open(
        configuration(root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        clock,
        protocol,
        directory,
    );
    campaign(&mut parents, clock, 1);
    let source = parents.remove(0);
    let fresh_binding = source.local().reads.binding();
    let (_, original_binding) = original_bindings
        .iter()
        .find(|(identity, _)| *identity == fresh_binding.owner.store.identity)
        .unwrap();
    assert_ne!(
        fresh_binding.owner.store.session,
        original_binding.owner.store.session
    );
    let mut driver = NativeManifestLookup::new(source, limits(), 60_000, 10000, 1, now(clock))
        .unwrap_or_else(|_| panic!("reopened lookup"));
    let mut fresh_cache = NativeManifestCache::new(limits()).unwrap();
    let fresh = resolve_auto(&mut driver, &mut parents, clock, &mut fresh_cache);
    assert_eq!(fresh, route);
    assert_eq!(fresh_cache.get(responsibility(2)), Some(grant));
    driver.close();
    parents.insert(
        0,
        driver
            .into_source()
            .unwrap_or_else(|_| panic!("reopened lookup held")),
    );
    close(parents, clock, 1, || {});
    fresh
}
fn recover_child(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    grant: ResponsibilityManifest,
    fresh: RouteHint,
) {
    let mut children = open(
        configuration(root, 20, &[1, 2], NativeOpenMode::Recover),
        clock,
        protocol,
        || child(20, grant.clone()),
    );
    campaign(&mut children, clock, 20);
    assert!(matches!(
        propose(
            &mut children,
            clock,
            20,
            1,
            encode_routed(fresh, &[10], &7i64.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap()
        )
        .outcome,
        RoutedOutcome::Applied(CounterReceipt {
            outcome: CounterOutcome::Value(7),
            duplicate: true,
            ..
        })
    ));
    assert!(matches!(
        propose(
            &mut children,
            clock,
            20,
            2,
            encode_routed(fresh, &[10], &3i64.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap()
        )
        .outcome,
        RoutedOutcome::Applied(CounterReceipt {
            outcome: CounterOutcome::Value(10),
            duplicate: false,
            ..
        })
    ));
    assert_eq!(
        read(
            &mut children,
            clock,
            20,
            RoutedQuery {
                hint: fresh,
                key: vec![10],
                query: ()
            }
        ),
        RoutedRead::Served(10)
    );
    close(children, clock, 20, || {});
}
fn automatic(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-automatic-manifest-{}-{protocol:?}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = initialized_directory(&root, &clock, protocol);
    let source = nodes.remove(0);
    let mut driver = NativeManifestLookup::new(source, limits(), 10, 10000, 1, now(&clock))
        .unwrap_or_else(|_| panic!("lookup construction"));
    assert_eq!(
        driver.lookup(query(1), now(&clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let original = driver.pending().unwrap();
    assert_eq!(
        driver.lookup(query(1), now(&clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    assert_eq!(driver.pending().unwrap().ticket, original.ticket);
    assert_eq!(
        driver.lookup(query(3), now(&clock)).err(),
        Some(ManifestDiscoveryError::Overloaded)
    );
    let mut cache = NativeManifestCache::new(limits()).unwrap();
    let route = resolve_auto(&mut driver, &mut nodes, &clock, &mut cache);
    assert_eq!(route.group, group(20));
    assert_eq!(driver.source_mut().local().reads.usage().requests, 0);
    refresh_and_refuse_floor(&mut driver, &mut nodes, &clock);
    cancel_and_recover(driver, &mut nodes, &clock);
    close(nodes, &clock, 1, || {});
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn automatic_manifest_cache_miss_and_late_cancel_tcp() {
    automatic(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn automatic_manifest_cache_miss_and_late_cancel_quic() {
    automatic(NativePeerProtocol::Quic);
}

fn checkpoint<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128)
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    for node in nodes.iter_mut() {
        node.control(group(g), NodeControl::Checkpoint).unwrap();
    }
    drive(nodes, clock, |ns| {
        ns.iter().all(|n| {
            n.local().owner.core(group(g)).unwrap().state().base_index()
                == n.local().applications[&group(g)].applied_index()
        })
    });
}
fn service(protocol: NativePeerProtocol, compact: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-automatic-routed-service-{}-{protocol:?}-{compact}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut parents = initialized_directory(&root, &clock, protocol);
    if compact {
        checkpoint(&mut parents, &clock, 1);
    }
    let original_bindings = parents
        .iter()
        .map(|n| {
            let binding = n.local().reads.binding();
            (binding.owner.store.identity, binding)
        })
        .collect::<Vec<_>>();
    let source = parents.remove(0);
    let mut driver = NativeManifestLookup::new(source, limits(), 60_000, 10000, 1, now(&clock))
        .unwrap_or_else(|_| panic!("service lookup"));
    let mut cache = NativeManifestCache::new(limits()).unwrap();
    let route = resolve_auto(&mut driver, &mut parents, &clock, &mut cache);
    let grant = cache.get(responsibility(2)).unwrap().clone();
    // An explicit stale-route signal invalidates only the observed generation.
    // Resolution then obtains another actual read instead of republishing a receipt.
    let observed = driver.lookup(query(2), now(&clock)).unwrap().observation;
    assert!(driver.invalidate(query(2).locator, observed));
    assert!(cache.invalidate(responsibility(2), grant.input().generation));
    let refreshed = resolve_auto(&mut driver, &mut parents, &clock, &mut cache);
    assert_eq!(refreshed, route);
    assert_ne!(
        driver.lookup(query(2), now(&clock)).unwrap().observation,
        observed
    );
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
    driver.close();
    assert!(driver.is_drained());
    parents.insert(
        0,
        driver
            .into_source()
            .unwrap_or_else(|_| panic!("lookup held")),
    );
    let stores = parents
        .iter()
        .map(|n| n.local().reads.binding().owner.store.identity.id.get())
        .collect::<Vec<_>>();
    let parent_logs = close(parents, &clock, 1, || {
        drive(&mut children, &clock, |_| true);
    });
    // Cached child routes work after every metadata owner has shut down.
    serve_cached_child(&mut cache, route, &mut children, &clock);
    if compact {
        checkpoint(&mut children, &clock, 20);
    }
    close(children, &clock, 20, || {});
    for (store, before) in stores.into_iter().zip(parent_logs) {
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join(format!("1/{store}"))).unwrap(),
            support::identity(store),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), before);
    }
    let fresh = recover_lookup(&root, &clock, protocol, &original_bindings, route, &grant);
    recover_child(&root, &clock, protocol, grant, fresh);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn automatic_routed_service_tcp_reopens_wal_and_checkpoint() {
    for compact in [false, true] {
        service(NativePeerProtocol::TcpTls, compact);
    }
}
#[cfg(feature = "quic")]
#[test]
fn automatic_routed_service_quic_reopens_wal_and_checkpoint() {
    for compact in [false, true] {
        service(NativePeerProtocol::Quic, compact);
    }
}
