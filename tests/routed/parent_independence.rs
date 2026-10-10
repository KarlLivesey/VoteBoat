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
use voteboat::native::authority_discovery::NativeAuthorityDiscovery;

struct DiscoveredChildren {
    cache: NativeManifestCache,
    discovery: NativeAuthorityDiscovery,
    orders: RouteHint,
    jobs: RouteHint,
}
fn lookup(id: u128) -> ManifestLookup {
    ManifestLookup {
        locator: AuthorityLocator {
            responsibility: responsibility(id),
            authority: group(1),
        },
        minimum_epoch: None,
        minimum_generation: None,
    }
}
fn initialize_directory(parents: &mut [Node<Directory>], clock: &Instant) {
    campaign(parents, clock, 1);
    let boot = directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
    assert_eq!(
        propose(parents, clock, 1, 10000, boot).outcome,
        DirectoryOutcome::Initialized
    );
    for (i, manifest) in manifests().into_iter().enumerate() {
        let bytes = DirectoryCommand {
            expected: None,
            manifest,
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
        assert!(matches!(
            propose(parents, clock, 1, 10001 + i as u128, bytes).outcome,
            DirectoryOutcome::Published(_)
        ));
    }
}

impl DiscoveredChildren {
    fn read(parents: &mut [Node<Directory>], clock: &Instant) -> Self {
        let mut cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 8,
            bytes: 64 * 1024,
        })
        .unwrap();
        let mut discovery = voteboat::native::authority_discovery::NativeAuthorityDiscovery::new(
            ManifestCacheLimits {
                manifests: 8,
                bytes: 64 * 1024,
            },
            60_000,
            parents[0].local().reads.binding(),
            MonoTime(0),
        )
        .unwrap();
        for id in 1..=5 {
            let query = lookup(id);
            let (ticket, outcome) = directory_observation(parents, clock, responsibility(id));
            if id == 1 {
                verify_root_observation(parents, clock, &mut discovery, query, ticket, outcome);
            } else {
                discovery
                    .observe(query, ticket, outcome, MonoTime(1))
                    .unwrap();
            }
        }
        let request = |key| DiscoverRouteRequest {
            start: lookup(1),
            key,
            max_hops: 3,
            lookups: 3,
            now: MonoTime(1),
        };
        let orders =
            resolve_discovered(&mut cache, &HostPolicy, &mut discovery, request(&[10])).unwrap();
        let jobs =
            resolve_discovered(&mut cache, &HostPolicy, &mut discovery, request(&[200])).unwrap();
        discovery.close(); // Subsequent child operation cannot depend on this source.
        assert_eq!(
            discovery.lookup(lookup(1), MonoTime(1)).err(),
            Some(ManifestDiscoveryError::Closed)
        );
        assert_ne!(orders.group, jobs.group);
        DiscoveredChildren {
            cache,
            discovery,
            orders,
            jobs,
        }
    }
    fn verify_cached_children(&mut self) {
        self.cache
            .invalidate(responsibility(1), RouteGeneration::new(1).unwrap());
        self.cache
            .invalidate(responsibility(4), RouteGeneration::new(1).unwrap());
        self.cache
            .invalidate(responsibility(5), RouteGeneration::new(1).unwrap());
        assert!(resolve(&self.cache, &HostPolicy, responsibility(1), &[10], 3).is_err());
        assert_eq!(
            resolve_discovered(
                &mut self.cache,
                &HostPolicy,
                &mut self.discovery,
                DiscoverRouteRequest {
                    start: lookup(2),
                    key: &[10],
                    max_hops: 1,
                    lookups: 0,
                    now: MonoTime(1)
                }
            )
            .unwrap(),
            self.orders
        );
        assert_eq!(
            resolve_discovered(
                &mut self.cache,
                &HostPolicy,
                &mut self.discovery,
                DiscoverRouteRequest {
                    start: lookup(3),
                    key: &[200],
                    max_hops: 1,
                    lookups: 0,
                    now: MonoTime(1)
                }
            )
            .unwrap(),
            self.jobs
        );
    }
}
fn verify_expired_observation(
    binding: ReadInvocationBinding,
    query: ManifestLookup,
    ticket: ReadInvocationTicket,
    outcome: ReadOutcome<Option<ResponsibilityManifest>>,
) {
    let ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest)),
    } = outcome
    else {
        panic!("manifest observation")
    };
    let original = || ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest.clone())),
    };
    let mut expired = voteboat::native::authority_discovery::NativeAuthorityDiscovery::new(
        ManifestCacheLimits {
            manifests: 1,
            bytes: 64 * 1024,
        },
        1,
        binding,
        MonoTime(0),
    )
    .unwrap();
    expired
        .observe(query, ticket, original(), MonoTime(0))
        .unwrap();
    assert_eq!(
        expired.lookup(query, MonoTime(1)).err(),
        Some(ManifestDiscoveryError::Expired)
    );
    assert_eq!(
        expired
            .observe(query, ticket, original(), MonoTime(1))
            .unwrap_err()
            .0,
        ManifestDiscoveryError::StaleObservation
    );
}
fn verify_wrong_authority(
    discovery: &mut NativeAuthorityDiscovery,
    query: ManifestLookup,
    ticket: ReadInvocationTicket,
    outcome: ReadOutcome<Option<ResponsibilityManifest>>,
) {
    let mut wrong = query;
    wrong.locator.authority = group(999);
    let rejected = outcome;
    let pointer = match &rejected {
        ReadOutcome::Read {
            result: Ok(Some(m)),
            ..
        } => match &m.input().execution {
            ExecutionMode::Delegated(entries) => entries.as_ptr(),
            _ => panic!("root routes"),
        },
        _ => panic!("read result"),
    };
    let (error, returned) = discovery
        .observe(wrong, ticket, rejected, MonoTime(0))
        .unwrap_err();
    assert_eq!(error, ManifestDiscoveryError::WrongAuthority);
    match returned {
        ReadOutcome::Read {
            result: Ok(Some(m)),
            ..
        } => match &m.input().execution {
            ExecutionMode::Delegated(entries) => assert_eq!(entries.as_ptr(), pointer),
            _ => panic!("returned root routes"),
        },
        _ => panic!("returned read result"),
    }
}
fn verify_root_observation(
    parents: &mut [Node<Directory>],
    clock: &Instant,
    discovery: &mut NativeAuthorityDiscovery,
    query: ManifestLookup,
    ticket: ReadInvocationTicket,
    outcome: ReadOutcome<Option<ResponsibilityManifest>>,
) {
    let ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest)),
    } = outcome
    else {
        panic!("manifest observation");
    };
    let original = || ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest.clone())),
    };
    verify_expired_observation(
        parents[0].local().reads.binding(),
        query,
        ticket,
        original(),
    );
    let first = discovery
        .observe(query, ticket, original(), MonoTime(0))
        .unwrap();
    assert_eq!(
        discovery
            .observe(query, ticket, original(), MonoTime(0))
            .unwrap(),
        first
    );
    verify_wrong_authority(discovery, query, ticket, original());
    assert!(discovery.invalidate(query.locator, first));
    assert_eq!(
        discovery
            .observe(query, ticket, original(), MonoTime(1))
            .unwrap_err()
            .0,
        ManifestDiscoveryError::StaleObservation
    );
    let (fresh_ticket, fresh) = directory_observation(parents, clock, query.locator.responsibility);
    let next = discovery
        .observe(query, fresh_ticket, fresh, MonoTime(1))
        .unwrap();
    assert_ne!(first, next);
    assert_eq!(
        discovery
            .observe(query, ticket, original(), MonoTime(1))
            .unwrap_err()
            .0,
        ManifestDiscoveryError::StaleObservation
    );
    assert!(!discovery.invalidate(query.locator, first));
    let mut newer = query;
    newer.minimum_epoch = Some(OwnershipEpoch::new(2).unwrap());
    assert_eq!(
        discovery.lookup(newer, MonoTime(1)).err(),
        Some(ManifestDiscoveryError::Routing(
            RoutingError::EpochRegression
        ))
    );
}
struct IndependentChild {
    group: u128,
    voters: Vec<u64>,
    manifest: ResponsibilityManifest,
    hint: RouteHint,
    key: u8,
    delta: i64,
}
impl IndependentChild {
    fn open(&self, root: &Path, clock: &Instant, protocol: NativePeerProtocol) -> Vec<Node<Child>> {
        open(
            configuration(root, self.group, &self.voters, NativeOpenMode::Create),
            clock,
            protocol,
            || child(self.group, self.manifest.clone()),
        )
    }
    fn bootstrap(&self, nodes: &mut [Node<Child>], clock: &Instant) {
        let bytes = child(self.group, self.manifest.clone())
            .bootstrap_command(MAX_ROUTED_COMMAND_BYTES)
            .unwrap();
        assert_eq!(
            propose(nodes, clock, self.group, 10000, bytes).outcome,
            RoutedOutcome::Bootstrapped
        );
    }
    fn exercise(&self, nodes: &mut [Node<Child>], clock: &Instant, compact: bool) {
        let (g, hint, key, delta) = (self.group, self.hint, self.key, self.delta);
        let bytes =
            encode_routed(hint, &[key], &delta.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap();
        assert!(
            matches!(propose(nodes, clock, g, 1, bytes.clone()).outcome, RoutedOutcome::Applied(CounterReceipt {outcome: CounterOutcome::Value(v), duplicate: false, ..}) if v == delta)
        );
        assert!(matches!(
            propose(nodes, clock, g, 1, bytes).outcome,
            RoutedOutcome::Applied(CounterReceipt {
                duplicate: true,
                ..
            })
        ));
        assert_eq!(
            read(
                nodes,
                clock,
                g,
                RoutedQuery {
                    hint,
                    key: vec![key],
                    query: ()
                }
            ),
            RoutedRead::Served(delta)
        );
        if compact {
            for n in nodes.iter_mut() {
                n.control(group(g), NodeControl::Checkpoint).unwrap();
            }
            drive(nodes, clock, |ns| {
                ns.iter().all(|n| {
                    n.local().owner.core(group(g)).unwrap().state().base_index()
                        == n.local().applications[&group(g)].applied_index()
                })
            });
        }
    }
    fn reject_changed_bindings(&self, root: &Path, clock: &Instant, protocol: NativePeerProtocol) {
        // Both WAL-only and checkpoint recovery must reject a changed owner
        // grant or semantic-retention envelope before exposing an application.
        for changed_grant in [false, true] {
            let mut input = self.manifest.clone().into_input();
            let mut selected_limits = limits();
            if changed_grant {
                input.epoch = OwnershipEpoch::new(2).unwrap();
            } else {
                selected_limits.semantic_bytes -= 1;
            }
            let app = RoutedApplication::new(
                group(self.group),
                ResponsibilityManifest::new(input).unwrap(),
                Counter::new(64).unwrap(),
                HostPolicy,
                selected_limits,
            )
            .unwrap_or_else(|r| panic!("{:?}", r.error));
            let config =
                configuration(root, self.group, &self.voters, NativeOpenMode::Recover).remove(0);
            let mut rejected = match config.open_with_protocol(
                protocol,
                app,
                Arc::new(ThreadWake::current()),
                MonoTime(clock.elapsed().as_millis() as u64),
            ) {
                Ok(_) => panic!("changed ownership binding accepted"),
                Err(r) => r,
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while !rejected.try_cleanup().unwrap() {
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            assert_eq!(rejected.application.take().unwrap().applied_index(), 0);
        }
    }
    fn recover(&self, root: &Path, clock: &Instant, protocol: NativePeerProtocol) {
        self.reject_changed_bindings(root, clock, protocol);
        let (hint, key, delta) = (self.hint, self.key, self.delta);
        let mut ns = open(
            configuration(root, self.group, &self.voters, NativeOpenMode::Recover),
            clock,
            protocol,
            || child(self.group, self.manifest.clone()),
        );
        campaign(&mut ns, clock, self.group);
        let bytes =
            encode_routed(hint, &[key], &delta.to_le_bytes(), MAX_ROUTED_COMMAND_BYTES).unwrap();
        assert!(
            matches!(propose(&mut ns, clock, self.group, 1, bytes).outcome, RoutedOutcome::Applied(CounterReceipt {outcome: CounterOutcome::Value(v), duplicate: true, ..}) if v == delta)
        );
        assert_eq!(
            read(
                &mut ns,
                clock,
                self.group,
                RoutedQuery {
                    hint,
                    key: vec![key],
                    query: ()
                }
            ),
            RoutedRead::Served(delta)
        );
        close(ns, clock, self.group, || {});
    }
}
pub(super) fn run(protocol: NativePeerProtocol, compact: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-routed-native-{}-{:?}",
        std::process::id(),
        protocol
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        directory,
    );
    initialize_directory(&mut parents, &clock);
    let mut routes = DiscoveredChildren::read(&mut parents, &clock);
    let orders = IndependentChild {
        group: 20,
        voters: vec![1, 2],
        manifest: routes.cache.get(responsibility(2)).unwrap().clone(),
        hint: routes.orders,
        key: 10,
        delta: 7,
    };
    let jobs = IndependentChild {
        group: 30,
        voters: vec![2, 3],
        manifest: routes.cache.get(responsibility(3)).unwrap().clone(),
        hint: routes.jobs,
        key: 200,
        delta: 11,
    };
    let mut children = orders.open(&root, &clock, protocol);
    let mut other = jobs.open(&root, &clock, protocol);
    campaign(&mut children, &clock, 20);
    campaign(&mut other, &clock, 30);
    orders.bootstrap(&mut children, &clock);
    jobs.bootstrap(&mut other, &clock);
    // The host continues polling live children while the parent roles drain.
    let parent_logs = close(parents, &clock, 1, || {
        drive(&mut children, &clock, |_| true);
        drive(&mut other, &clock, |_| true);
    });
    routes.verify_cached_children();
    orders.exercise(&mut children, &clock, compact);
    jobs.exercise(&mut other, &clock, compact);
    close(children, &clock, 20, || {
        drive(&mut other, &clock, |_| true);
    });
    close(other, &clock, 30, || {});
    orders.recover(&root, &clock, protocol);
    jobs.recover(&root, &clock, protocol);
    for (n, before) in (1..=3).zip(parent_logs) {
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join(format!("1/{n}"))).unwrap(),
            support::identity(n),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), before);
    }
    std::fs::remove_dir_all(root).unwrap();
}
