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
fn automatic(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-automatic-manifest-{}-{protocol:?}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        directory,
    );
    campaign(&mut nodes, &clock, 1);
    propose(
        &mut nodes,
        &clock,
        1,
        10000,
        directory()
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
    );
    for (i, manifest) in manifests().into_iter().enumerate() {
        propose(
            &mut nodes,
            &clock,
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
    // Expiry of unchanged metadata requires another original quorum read.
    let mut observations = Vec::new();
    for _ in 0..2 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            poll_nodes(driver.source_mut(), &mut nodes, &clock);
            driver.poll(now(&clock)).unwrap();
            match driver.lookup(query(2), now(&clock)) {
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
            driver.lookup(query(2), now(&clock)).err(),
            Some(ManifestDiscoveryError::Unavailable)
        );
    }
    assert_ne!(observations[0], observations[1]);
    // Drain the refresh initiated by the final expiry before requesting a floor.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !driver.is_drained() {
        poll_nodes(driver.source_mut(), &mut nodes, &clock);
        driver.poll(now(&clock)).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    // Below-floor demands initiate a fresh read and refuse the actual older data.
    let mut stricter = query(1);
    stricter.minimum_epoch = OwnershipEpoch::new(999);
    assert_eq!(
        driver.lookup(stricter, now(&clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(driver.source_mut(), &mut nodes, &clock);
        match driver.poll(now(&clock)) {
            Ok(false) => (),
            Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Routing(
                RoutingError::EpochRegression,
            ))) => break,
            other => panic!("floor read: {other:?}"),
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
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
    let mut driver = NativeManifestLookup::new(source, limits(), 1000, 10000, 1, now(&clock))
        .unwrap_or_else(|_| panic!("host source"));
    assert_eq!(
        driver.lookup(query(1), now(&clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    driver.cancel_pending().unwrap();
    assert!(!driver.is_drained());
    assert_eq!(driver.source_mut().cancellations, 1);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(&mut driver.source_mut().node, &mut nodes, &clock);
        match driver.poll(now(&clock)) {
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
        driver.lookup(query(1), now(&clock)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let pending = driver.pending().unwrap();
    let (mut source, unresolved) = driver.into_recovery();
    assert_eq!(unresolved.unwrap().ticket, pending.ticket);
    // Recovery handoff keeps original Node ownership; explicit cancellation and
    // terminal receipt consumption release it before ordinary shutdown/join.
    source.node.cancel_read(pending.ticket).unwrap();
    loop {
        poll_nodes(&mut source.node, &mut nodes, &clock);
        if let Some(reply) = source.node.poll_read() {
            source.node.complete_read(reply).unwrap();
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    nodes.insert(0, source.node);
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
