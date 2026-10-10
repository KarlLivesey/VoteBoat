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
type Application = target_fixture::Target;
type Nodes<P> = Vec<Node<<P as TargetProfile>::Application>>;
fn groups(operation: u128) -> (&'static [u128], &'static [u128]) {
    match operation {
        501 => (&[31], &[41, 42]),
        601 => (&[41, 42], &[43]),
        _ => panic!("operation"),
    }
}
fn later(intent: &TransferIntent, g: u128, operation: u128) -> Application {
    let routes = intent.targets();
    let r = routes
        .iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap();
    Application::new(
        group(g),
        source_fixture::op(operation),
        intent.clone(),
        BucketCounter::new(
            r.scope,
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("binding {:?}", e.0))
}
fn profile_later<P: TargetProfile>(
    intent: &TransferIntent,
    g: u128,
    operation: u128,
) -> P::Application {
    P::wrap(later(intent, g, operation))
}
fn route(m: &ResponsibilityManifest, key: u8) -> RouteHint {
    let (g, scope) = match &m.input().execution {
        ExecutionMode::Single(g) => (*g, m.input().scope),
        ExecutionMode::Partitioned(routes) => {
            let r = routes
                .iter()
                .find(|r| r.scope.start() <= key as u16 && (key as u16) < r.scope.end())
                .unwrap();
            let RouteTarget::Group(g) = r.target else {
                panic!("owner")
            };
            (g, r.scope)
        }
        _ => panic!("concrete manifest"),
    };
    let mut h = hint(m, g.id.get(), key);
    h.scope = scope;
    h
}
fn request_data(m: &ResponsibilityManifest, key: u8, value: i64) -> Vec<u8> {
    encode_routed(
        route(m, key),
        &[key],
        &encode_add(&[key], value, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn request_query(m: &ResponsibilityManifest, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: route(m, key),
        key: vec![key],
        query: vec![key],
    })
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Reserve,
    Intent,
    Stage(u128),
    Fence(u128),
    Import(u128),
    Publish,
    Refresh,
    Activate(u128),
}
struct Retry {
    phase: Phase,
    group: u128,
    operation: u128,
    bytes: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct State {
    reservation: Option<DelegationReservationStatus>,
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    completion: Option<DelegationPublicationStatus>,
    root: ResponsibilityManifest,
    parent: ResponsibilityManifest,
    child: ResponsibilityManifest,
    sibling: ResponsibilityManifest,
    sources: Vec<(u128, TargetStatus, Option<SourceFreezeStatus>)>,
    targets: Vec<(u128, Option<TargetStatus>)>,
}
struct Moves<P: TargetProfile = Raw> {
    base: Nested<P>,
    later: BTreeMap<u128, Nodes<P>>,
    root: ResponsibilityManifest,
    original: TargetStatus,
    keep_unread: bool,
}
impl<P: TargetProfile> Moves<P> {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let mut base: Nested<P> = Nested::new(protocol, checkpoint);
        let b = data(base.plan.before(), 21, 40, 3);
        let r = propose_recovering(&mut base.source, &base.clock, 21, 81, b);
        assert!(
            matches!(&r.outcome,TargetOutcome::Applied(v) if v.outcome==BucketOutcome::Value(3)),
            "setup write: {r:?}"
        );
        assert_eq!(
            split::observe(
                &mut base.source,
                &base.clock,
                21,
                query(base.plan.before(), 21, 40)
            ),
            TargetRead::Data(3)
        );
        assert_eq!(
            base.source[0].local().applications[&group(21)]
                .application()
                .outbox()
                .count(),
            2
        );
        while base.resume_with_delivery(false).is_some() {}
        let state = base.observed();
        let intent = base.bound.clone().unwrap();
        for (i, g) in [(0, 31), (1, 32)] {
            let activation = TargetActivation {
                metadata_configuration: Self::configuration(&base.parent, 1),
                decision: state.publication.clone().unwrap(),
            };
            let b = P::owner(&base.targets[i][0].local().applications[&group(g)])
                .activation_command(&activation, 100000)
                .unwrap();
            let r = propose_target::<P>(&mut base.targets[i], &base.clock, g, 300, b);
            assert!(matches!(r.outcome, TargetOutcome::Activated(_)));
            let m = intent.target_manifest(group(g)).unwrap();
            let key = if g == 31 { 1 } else { 80 };
            assert_eq!(
                observe_target::<P>(&mut base.targets[i], &base.clock, g, query(m, g, key)),
                TargetRead::Data(if g == 31 { 7 } else { 5 })
            );
        }
        let original = P::owner(&base.targets[0][0].local().applications[&group(31)]).status();
        Self {
            base,
            later: BTreeMap::new(),
            root: state.parent,
            original,
            keep_unread: true,
        }
    }
    fn configuration<A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine>(
        ns: &[Node<A>],
        g: u128,
    ) -> ConfigurationId
    where
        A::Receipt: ApplicationReceipt,
    {
        ns[0]
            .local()
            .owner
            .core(group(g))
            .unwrap()
            .state()
            .bootstrap
            .configuration
    }
    fn nodes(&mut self, g: u128) -> &mut Nodes<P> {
        match g {
            31 => &mut self.base.targets[0],
            32 => &mut self.base.targets[1],
            _ => self.later.get_mut(&g).unwrap(),
        }
    }
    fn meta(&mut self, q: DirectoryQuery) -> DirectoryRead {
        split::observe(&mut self.base.parent, &self.base.clock, 1, q)
    }
    fn manifest(&mut self, id: ResponsibilityIdentity) -> ResponsibilityManifest {
        let DirectoryRead::Manifest(Some(m)) = self.meta(DirectoryQuery::Manifest(id)) else {
            panic!("manifest")
        };
        m
    }
    fn reservation(&mut self, operation: u128) -> Option<DelegationReservationStatus> {
        let DirectoryRead::DelegationReservation(r) = self.meta(
            DirectoryQuery::DelegationReservation(source_fixture::op(operation - 1)),
        ) else {
            panic!("reservation")
        };
        r
    }
    fn bound(&mut self, operation: u128) -> TransferIntent {
        let configuration = Self::configuration(&self.base.parent, 1);
        self.reservation(operation)
            .unwrap()
            .child_intent(configuration)
            .unwrap()
    }
    fn observed(&mut self, operation: u128) -> State {
        let reservation = self.reservation(operation);
        if reservation.is_some() {
            let intent = self.bound(operation);
            for &g in groups(operation).1 {
                if !self.later.contains_key(&g) {
                    let probe = configuration_for(&self.base.root, g);
                    let mode = if probe[0].directory.exists() {
                        NativeOpenMode::Recover
                    } else {
                        NativeOpenMode::Create
                    };
                    let ns = open(
                        configuration(&self.base.root, g, &[1, 2, 3], mode),
                        &self.base.clock,
                        self.base.protocol,
                        || profile_later::<P>(&intent, g, operation),
                    );
                    self.later.insert(g, ns);
                }
            }
        }
        let DirectoryRead::Transfer(intent) =
            self.meta(DirectoryQuery::Transfer(source_fixture::op(operation)))
        else {
            panic!("intent")
        };
        let DirectoryRead::Publication(publication) =
            self.meta(DirectoryQuery::Publication(source_fixture::op(operation)))
        else {
            panic!("publication")
        };
        let DirectoryRead::DelegationPublication(completion) = self.meta(
            DirectoryQuery::DelegationPublication(source_fixture::op(operation - 1)),
        ) else {
            panic!("completion")
        };
        let root = self.manifest(self.base.original.before().input().responsibility);
        let parent = self.manifest(self.base.plan.before().input().responsibility);
        let child = self.manifest(responsibility(31));
        let sibling = self.manifest(responsibility(32));
        let clock = self.base.clock;
        let sources = groups(operation)
            .0
            .iter()
            .map(|&g| {
                let TargetRead::Status(s) =
                    observe_target::<P>(self.nodes(g), &clock, g, TargetQuery::Status)
                else {
                    panic!("source status")
                };
                let TargetRead::Freeze(f) =
                    observe_target::<P>(self.nodes(g), &clock, g, TargetQuery::Freeze)
                else {
                    panic!("source fence")
                };
                (g, s, f)
            })
            .collect();
        let targets = groups(operation)
            .1
            .iter()
            .map(|&g| {
                let s = if self.later.contains_key(&g) {
                    let TargetRead::Status(s) =
                        observe_target::<P>(self.nodes(g), &clock, g, TargetQuery::Status)
                    else {
                        panic!("target status")
                    };
                    Some(s)
                } else {
                    None
                };
                (g, s)
            })
            .collect();
        State {
            reservation,
            intent,
            publication,
            completion,
            root,
            parent,
            child,
            sibling,
            sources,
            targets,
        }
    }
    fn metadata_command(
        &mut self,
        operation: u128,
        bytes: Vec<u8>,
        done: impl Fn(&LifecycleDirectory) -> bool,
    ) {
        deliver(
            self.keep_unread,
            &mut self.base.parent,
            &self.base.clock,
            1,
            operation,
            bytes,
            done,
        );
    }
    fn target_command(
        &mut self,
        g: u128,
        operation: u128,
        bytes: Vec<u8>,
        done: impl Fn(&Application) -> bool,
    ) {
        let clock = self.base.clock;
        let keep_unread = self.keep_unread;
        deliver(
            keep_unread,
            self.nodes(g),
            &clock,
            g,
            operation,
            bytes,
            |a| done(P::owner(a)),
        );
    }
    fn resume(&mut self, operation: u128) -> Option<Retry> {
        let s = self.observed(operation);
        if s.reservation.is_none() {
            return self.reserve(s, operation);
        }
        let intent = self.bound(operation);
        if s.intent.is_none() {
            let b = intent.encode(100000).unwrap();
            self.metadata_command(operation, b.clone(), |a| {
                a.directory()
                    .transfer_intent_at(a.applied_index(), source_fixture::op(operation))
                    .unwrap()
                    .is_some()
            });
            return Some(Retry {
                phase: Phase::Intent,
                group: 1,
                operation,
                bytes: b,
            });
        }
        assert_eq!(s.intent.as_ref().unwrap().intent, intent);
        if let Some(retry) = self.stage_targets(&s, operation, &intent) {
            return Some(retry);
        }
        for (g, _, f) in &s.sources {
            if f.is_none() {
                let b = P::owner(&self.nodes(*g)[0].local().applications[&group(*g)])
                    .freeze_command(&intent, 65536, 100000)
                    .unwrap();
                self.target_command(*g, operation, b.clone(), |a| a.fence().is_some());
                return Some(Retry {
                    phase: Phase::Fence(*g),
                    group: *g,
                    operation,
                    bytes: b,
                });
            }
        }
        if let Some(retry) = self.import_targets(&s, operation, &intent) {
            return Some(retry);
        }
        if s.publication.is_none() {
            return self.publish(&s, operation, intent);
        }
        let decision = s.publication.unwrap();
        if s.completion.is_none() {
            let configuration = Self::configuration(&self.base.parent, 1);
            let r = s.reservation.unwrap();
            let c = DelegationCompletion {
                reservation: r.operation,
                reservation_index: r.index,
                parent_configuration: configuration,
                child_configuration: configuration,
                decision,
            };
            let b = c.encode(100000).unwrap();
            self.metadata_command(operation + 2, b.clone(), |a| {
                a.directory()
                    .delegation_publication_at(a.applied_index(), source_fixture::op(operation - 1))
                    .unwrap()
                    .is_some()
            });
            return Some(Retry {
                phase: Phase::Refresh,
                group: 1,
                operation: operation + 2,
                bytes: b,
            });
        }
        for (g, status) in s.targets {
            if status.unwrap().activated.is_none() {
                let a = TargetActivation {
                    metadata_configuration: Self::configuration(&self.base.parent, 1),
                    decision: decision.clone(),
                };
                let b = P::owner(&self.nodes(g)[0].local().applications[&group(g)])
                    .activation_command(&a, 100000)
                    .unwrap();
                self.target_command(g, operation, b.clone(), |a| a.status().activated.is_some());
                return Some(Retry {
                    phase: Phase::Activate(g),
                    group: g,
                    operation,
                    bytes: b,
                });
            }
        }
        None
    }
    fn stage_targets(
        &mut self,
        s: &State,
        operation: u128,
        intent: &TransferIntent,
    ) -> Option<Retry> {
        for (g, status) in &s.targets {
            if status.as_ref().unwrap().staged_index.is_none() {
                let b = later(intent, *g, operation)
                    .bootstrap_command(100000)
                    .unwrap();
                self.target_command(*g, operation, b.clone(), |a| {
                    a.status().staged_index.is_some()
                });
                return Some(Retry {
                    phase: Phase::Stage(*g),
                    group: *g,
                    operation,
                    bytes: b,
                });
            }
        }
        None
    }
    fn reserve(&mut self, s: State, operation: u128) -> Option<Retry> {
        let op = source_fixture::op(operation);
        let mut after = s.child.clone().into_input();
        after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
        after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
        after.execution = if operation == 501 {
            ExecutionMode::Partitioned(vec![
                RouteEntry {
                    scope: source_fixture::range(0, 32),
                    target: RouteTarget::Group(group(41)),
                },
                RouteEntry {
                    scope: source_fixture::range(32, 64),
                    target: RouteTarget::Group(group(42)),
                },
            ])
        } else {
            ExecutionMode::Single(group(43))
        };
        let p = DelegationPlan::new(
            s.parent,
            s.child,
            ResponsibilityManifest::new(after).unwrap(),
            op,
        )
        .unwrap();
        let b = p.encode(100000).unwrap();
        self.metadata_command(operation - 1, b.clone(), |a| {
            a.directory()
                .delegation_reservation_at(a.applied_index(), source_fixture::op(operation - 1))
                .unwrap()
                .is_some()
        });
        Some(Retry {
            phase: Phase::Reserve,
            group: 1,
            operation: operation - 1,
            bytes: b,
        })
    }
    fn import_targets(
        &mut self,
        s: &State,
        operation: u128,
        intent: &TransferIntent,
    ) -> Option<Retry> {
        let op = source_fixture::op(operation);
        for (g, status) in &s.targets {
            if status.as_ref().unwrap().imported.is_none() {
                let mut images = Vec::new();
                for (source, _, f) in &s.sources {
                    let f = f.as_ref().unwrap();
                    if let Some(e) = f.exports.iter().find(|e| e.target == group(*g)) {
                        let configuration = Self::configuration(self.nodes(*source), *source);
                        let image =
                            P::owner(&self.nodes(*source)[0].local().applications[&group(*source)])
                                .export_target(group(*g), 65536)
                                .unwrap();
                        images.push(SourceImport {
                            fence: f.fence,
                            configuration,
                            image,
                            digest: e.digest,
                        });
                    }
                }
                let import = TargetImport::new(op, intent.clone(), group(*g), images)
                    .unwrap_or_else(|e| panic!("import {:?}", e.0));
                let b = P::owner(&self.nodes(*g)[0].local().applications[&group(*g)])
                    .import_command(&import, 100000)
                    .unwrap();
                self.target_command(*g, operation, b.clone(), |a| a.status().imported.is_some());
                return Some(Retry {
                    phase: Phase::Import(*g),
                    group: *g,
                    operation,
                    bytes: b,
                });
            }
        }
        None
    }
    fn publish(&mut self, s: &State, operation: u128, intent: TransferIntent) -> Option<Retry> {
        let op = source_fixture::op(operation);
        let sources = s
            .sources
            .iter()
            .map(|(g, _, f)| {
                SourceFenceEvidence::from_status(
                    Self::configuration(self.nodes(*g), *g),
                    f.clone().unwrap(),
                )
                .unwrap_or_else(|e| panic!("source {:?}", e.0))
            })
            .collect();
        let targets = s
            .targets
            .iter()
            .map(|(g, t)| {
                TargetReadyEvidence::from_status(
                    Self::configuration(self.nodes(*g), *g),
                    t.clone().unwrap(),
                )
                .unwrap_or_else(|e| panic!("target {:?}", e.0))
            })
            .collect();
        let p = TransferPublication::new(op, intent, sources, targets)
            .unwrap_or_else(|e| panic!("publication {:?}", e.0));
        let b = p.encode(100000).unwrap();
        self.metadata_command(operation + 1, b.clone(), |a| {
            a.directory()
                .transfer_publication_at(a.applied_index(), op)
                .unwrap()
                .is_some()
        });
        Some(Retry {
            phase: Phase::Publish,
            group: 1,
            operation: operation + 1,
            bytes: b,
        })
    }
    fn check_targets(&mut self, s: &State, intent: &TransferIntent) {
        let clock = self.base.clock;
        for (g, t) in &s.targets {
            let (key, value) = if *g == 42 {
                (40, 3)
            } else {
                (1, if *g == 43 { 9 } else { 7 })
            };
            let active = t.as_ref().unwrap().activated.is_some();
            assert_eq!(
                observe_target::<P>(
                    self.nodes(*g),
                    &clock,
                    *g,
                    request_query(intent.after(), key)
                ),
                if active {
                    TargetRead::Data(value)
                } else {
                    TargetRead::NotActive
                }
            );
            if !active {
                assert!(self.nodes(*g)[0]
                    .propose(ClientRequest {
                        group: group(*g),
                        operation: source_fixture::op(9999),
                        bytes: request_data(intent.after(), key, 1)
                    })
                    .is_err());
            }
        }
    }
    fn check(&mut self, operation: u128, s: &State) {
        assert_eq!(s.root, self.root);
        assert_eq!(s.parent.input().epoch, self.base.plan.after().input().epoch);
        assert_eq!(
            s.sibling,
            self.base
                .bound
                .as_ref()
                .unwrap()
                .target_manifest(group(32))
                .unwrap()
                .clone()
        );
        if operation == 501 {
            assert_eq!(s.sources[0].1, self.original);
        }
        let mut cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 4,
            bytes: 65536,
        })
        .unwrap();
        for m in [&s.root, &s.parent, &s.child, &s.sibling] {
            cache.admit(m.clone()).unwrap();
        }
        for key in [1, 40] {
            let result = resolve(
                &cache,
                &source_fixture::Policy,
                s.root.input().responsibility,
                &[key],
                4,
            );
            if s.publication.is_some() && s.completion.is_none() {
                assert_eq!(result, Err(RoutingError::WrongChild));
            } else {
                assert_eq!(result.unwrap(), route(&s.child, key));
            }
        }
        assert_eq!(
            resolve(
                &cache,
                &source_fixture::Policy,
                s.root.input().responsibility,
                &[80],
                4
            )
            .unwrap(),
            hint(&s.sibling, 32, 80)
        );
        let intent = s.reservation.as_ref().map(|r| {
            r.child_intent(Self::configuration(&self.base.parent, 1))
                .unwrap()
        });
        let before = intent.as_ref().map_or(&s.child, |i| i.before());
        let clock = self.base.clock;
        for (g, _, f) in &s.sources {
            let (key, value) = if *g == 42 {
                (40, 3)
            } else {
                (1, if *g == 41 { 9 } else { 7 })
            };
            assert_eq!(
                observe_target::<P>(self.nodes(*g), &clock, *g, request_query(before, key)),
                if f.is_some() {
                    TargetRead::Rejected(RoutingError::Fenced)
                } else {
                    TargetRead::Data(value)
                }
            );
            if f.is_some() {
                assert!(self.nodes(*g)[0]
                    .propose(ClientRequest {
                        group: group(*g),
                        operation: source_fixture::op(9999),
                        bytes: request_data(before, key, 1)
                    })
                    .is_err());
            }
        }
        if let Some(intent) = intent {
            self.check_targets(s, &intent);
        }
    }
    fn stop(&mut self) {
        self.base.stop();
        for (&g, ns) in &mut self.later {
            if self.base.checkpoint {
                split::compact(ns, &self.base.clock, g);
            }
            Insertion::stop(
                std::mem::take(ns),
                &self.base.clock,
                g,
                self.base.checkpoint,
            );
        }
    }
    fn restart(&mut self) {
        let keys = self.later.keys().copied().collect::<Vec<_>>();
        self.stop();
        self.later.clear();
        self.base.parent = open(
            configuration_for(&self.base.root, 1),
            &self.base.clock,
            self.base.protocol,
            metadata,
        );
        self.base.source = open(
            configuration_for(&self.base.root, 21),
            &self.base.clock,
            self.base.protocol,
            || super::super::target(&self.base.original, 21),
        );
        let configuration = Self::configuration(&self.base.parent, 1);
        let DirectoryRead::DelegationReservation(Some(r)) = self.meta(
            DirectoryQuery::DelegationReservation(source_fixture::op(400)),
        ) else {
            panic!("original reservation")
        };
        let original = r.child_intent(configuration).unwrap();
        assert_eq!(self.base.bound.as_ref(), Some(&original));
        for (i, g) in [(0, 31), (1, 32)] {
            self.base.targets[i] = open(
                configuration_for(&self.base.root, g),
                &self.base.clock,
                self.base.protocol,
                || profile_target::<P>(&original, g),
            );
        }
        for g in keys {
            let operation = if g == 43 { 601 } else { 501 };
            let intent = self.bound(operation);
            self.later.insert(
                g,
                open(
                    configuration_for(&self.base.root, g),
                    &self.base.clock,
                    self.base.protocol,
                    || profile_later::<P>(&intent, g, operation),
                ),
            );
        }
        for g in [21, 31, 32] {
            for c in configuration_for(&self.base.root, g) {
                let mut b = FileCreationBindings::open(&c.directory).unwrap();
                assert_eq!(b.load().unwrap().unwrap(), self.base.bindings[&(g, c.node)]);
            }
        }
    }
    fn retry(&mut self, r: &Retry, s: &State) {
        if r.group == 1 {
            let receipt = propose_recovering(
                &mut self.base.parent,
                &self.base.clock,
                1,
                r.operation,
                r.bytes.clone(),
            );
            assert!(receipt.duplicate);
        } else {
            let clock = self.base.clock;
            campaign(self.nodes(r.group), &clock, r.group);
            let receipt = propose_target::<P>(
                self.nodes(r.group),
                &clock,
                r.group,
                r.operation,
                r.bytes.clone(),
            );
            match r.phase {
                Phase::Fence(g) => assert!(
                    matches!(receipt.outcome,TargetOutcome::Frozen(f) if s.sources.iter().find(|(x,_,_)|*x==g).unwrap().2.as_ref().unwrap().fence==f)
                ),
                Phase::Stage(g) => assert!(
                    matches!(receipt.outcome,TargetOutcome::Staged {index} if s.targets.iter().find(|(x,_)|*x==g).unwrap().1.as_ref().unwrap().staged_index==Some(index))
                ),
                Phase::Import(g) => assert!(
                    matches!(receipt.outcome,TargetOutcome::Imported {index,digest} if s.targets.iter().find(|(x,_)|*x==g).unwrap().1.as_ref().unwrap().imported.as_ref().is_some_and(|i|i.index==index&&i.digest==digest))
                ),
                Phase::Activate(g) => assert!(
                    matches!(receipt.outcome,TargetOutcome::Activated(a) if s.targets.iter().find(|(x,_)|*x==g).unwrap().1.as_ref().unwrap().activated==Some(a))
                ),
                _ => panic!("retry"),
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        g: u128,
        m: &ResponsibilityManifest,
        id: u128,
        key: u8,
        delta: i64,
        value: i64,
        duplicate: bool,
    ) {
        let clock = self.base.clock;
        campaign(self.nodes(g), &clock, g);
        let r = propose_target::<P>(self.nodes(g), &clock, g, id, request_data(m, key, delta));
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if v.duplicate==duplicate&&v.outcome==BucketOutcome::Value(value))
        );
    }
}
fn verify_independent_service<P: TargetProfile>(rig: &mut Moves<P>, final_state: &State) {
    let checkpoint = rig.base.checkpoint;
    let m = final_state.child.clone();
    if checkpoint {
        split::compact(&mut rig.base.parent, &rig.base.clock, 1);
        split::compact(&mut rig.base.source, &rig.base.clock, 21);
        split::compact(&mut rig.base.targets[0], &rig.base.clock, 31);
    }
    let metadata_logs = Insertion::stop(
        std::mem::take(&mut rig.base.parent),
        &rig.base.clock,
        1,
        checkpoint,
    );
    Insertion::stop(
        std::mem::take(&mut rig.base.source),
        &rig.base.clock,
        21,
        checkpoint,
    );
    Insertion::stop(
        std::mem::take(&mut rig.base.targets[0]),
        &rig.base.clock,
        31,
        checkpoint,
    );
    for g in [41, 42] {
        let mut ns = rig.later.remove(&g).unwrap();
        if checkpoint {
            split::compact(&mut ns, &rig.base.clock, g);
        }
        Insertion::stop(ns, &rig.base.clock, g, checkpoint);
    }
    let mut stopped = BTreeMap::new();
    for g in [1, 21, 31, 41, 42] {
        stopped.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    for (id, key, delta, value) in [(1, 1, 7, 7), (81, 40, 3, 3), (82, 1, 2, 9)] {
        rig.write(43, &m, id, key, delta, value, true);
    }
    rig.write(43, &m, 83, 40, 4, 7, false);
    let sibling = final_state.sibling.clone();
    rig.write(32, &sibling, 80, 80, 5, 5, true);
    let mut current = BTreeMap::new();
    for g in [1, 21, 31, 41, 42] {
        current.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    assert_eq!(current, stopped);
    let mut old = durable_files(&rig.base.root.join("20"));
    old.extend(durable_files(&rig.base.root.join("22")));
    assert_eq!(old, rig.base.stopped);
    for c in configuration_for(&rig.base.root, 1) {
        let logs = NativeLogStore::recover(
            FileLogIo::open(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(logs.state(group(1)).unwrap(), metadata_logs[&c.node]);
    }
}
fn history<P: TargetProfile>(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Moves::<P>::new(protocol, checkpoint);
    for operation in [501, 601] {
        let (sources, targets) = groups(operation);
        let phases = std::iter::once(Phase::Reserve)
            .chain([Phase::Intent])
            .chain(targets.iter().copied().map(Phase::Stage))
            .chain(sources.iter().copied().map(Phase::Fence))
            .chain(targets.iter().copied().map(Phase::Import))
            .chain([Phase::Publish, Phase::Refresh])
            .chain(targets.iter().copied().map(Phase::Activate));
        for phase in phases {
            eprintln!("nested moves {protocol:?} checkpoint={checkpoint} operation={operation} phase={phase:?}");
            let r = rig.resume(operation).unwrap();
            assert_eq!(r.phase, phase);
            let s = rig.observed(operation);
            rig.check(operation, &s);
            rig.restart();
            let recovered = rig.observed(operation);
            assert_eq!(recovered, s);
            rig.check(operation, &recovered);
            rig.retry(&r, &s);
            assert_eq!(rig.observed(operation), s);
        }
        assert!(rig.resume(operation).is_none());
        let s = rig.observed(operation);
        if operation == 501 {
            rig.write(41, &s.child, 1, 1, 7, 7, true);
            rig.write(42, &s.child, 81, 40, 3, 3, true);
            rig.write(41, &s.child, 82, 1, 2, 9, false);
        }
    }
    let final_state = rig.observed(601);
    let m = final_state.child.clone();
    verify_independent_service(&mut rig, &final_state);
    // Reopen every original binding, including owners stopped before final writes.
    rig.base.parent = open(
        configuration_for(&rig.base.root, 1),
        &rig.base.clock,
        protocol,
        metadata,
    );
    rig.base.source = open(
        configuration_for(&rig.base.root, 21),
        &rig.base.clock,
        protocol,
        || super::super::target(&rig.base.original, 21),
    );
    let original = rig.base.bound.clone().unwrap();
    rig.base.targets[0] = open(
        configuration_for(&rig.base.root, 31),
        &rig.base.clock,
        protocol,
        || profile_target::<P>(&original, 31),
    );
    let split = rig.bound(501);
    for g in [41, 42] {
        rig.later.insert(
            g,
            open(
                configuration_for(&rig.base.root, g),
                &rig.base.clock,
                protocol,
                || profile_later::<P>(&split, g, 501),
            ),
        );
    }
    rig.restart();
    assert_eq!(rig.observed(601), final_state);
    assert!(rig.resume(601).is_none());
    let clock = rig.base.clock;
    for (key, value) in [(1, 9), (40, 7)] {
        assert_eq!(
            observe_target::<P>(rig.nodes(43), &clock, 43, request_query(&m, key)),
            TargetRead::Data(value)
        );
    }
    assert!(rig
        .nodes(43)
        .iter()
        .all(|n| P::owner(&n.local().applications[&group(43)])
            .application()
            .outbox()
            .count()
            == 4));
    let TargetRead::Freeze(Some(_)) =
        split::observe(&mut rig.base.source, &clock, 21, TargetQuery::Freeze)
    else {
        panic!("original fence")
    };
    for g in [31, 41, 42] {
        let TargetRead::Freeze(Some(_)) =
            observe_target::<P>(rig.nodes(g), &clock, g, TargetQuery::Freeze)
        else {
            panic!("source fence")
        };
    }
    rig.stop();
    std::fs::remove_dir_all(rig.base.root).unwrap();
}
#[test]
fn tcp_nested_later_moves_recover_unread_phases_from_wal() {
    history::<Raw>(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_nested_later_moves_recover_unread_phases_from_checkpoint() {
    history::<Raw>(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_later_moves_recover_unread_phases_from_wal() {
    history::<Raw>(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_later_moves_recover_unread_phases_from_checkpoint() {
    history::<Raw>(NativePeerProtocol::Quic, true)
}

#[path = "nested_retirement.rs"]
mod retirement;
