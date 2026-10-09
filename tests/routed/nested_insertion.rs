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
use voteboat::delegation::*;
#[path = "target_profile.rs"]
mod profile;
use profile::*;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Data,
    Reserve,
    Intent,
    Stage(u128),
    Fence,
    Import(u128),
    Publish,
    Refresh,
}
struct Request {
    step: Step,
    group: u128,
    operation: u128,
    bytes: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Facts {
    reservation: Option<DelegationReservationStatus>,
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    completion: Option<DelegationPublicationStatus>,
    parent: ResponsibilityManifest,
    child: ResponsibilityManifest,
    grandchildren: [Option<ResponsibilityManifest>; 2],
    source: TargetStatus,
    fence: Option<SourceFreezeStatus>,
    targets: [Option<TargetStatus>; 2],
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        super::metadata()
            .into_directory()
            .with_recursive_insertion()
            .unwrap_or_else(|_| panic!("schema6")),
    )
}
fn hint(m: &ResponsibilityManifest, g: u128, key: u8) -> RouteHint {
    let m = m.input();
    RouteHint {
        responsibility: m.responsibility,
        group: group(g),
        application: m.application,
        scheme: m.scheme,
        scope: m.scope,
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    }
}
fn query(m: &ResponsibilityManifest, g: u128, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: hint(m, g, key),
        key: vec![key],
        query: vec![key],
    })
}
fn data(m: &ResponsibilityManifest, g: u128, key: u8, value: i64) -> Vec<u8> {
    encode_routed(
        hint(m, g, key),
        &[key],
        &encode_add(&[key], value, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn target(intent: &TransferIntent, g: u128) -> target_fixture::Target {
    TransferTarget::new(
        group(g),
        source_fixture::op(300),
        intent.clone(),
        BucketCounter::new(
            intent.target_manifest(group(g)).unwrap().input().scope,
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
struct Nested<P: TargetProfile = Raw> {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    original: TransferIntent,
    plan: DelegationPlan,
    creations: [GroupCreationStatus; 2],
    bound: Option<TransferIntent>,
    parent: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<target_fixture::Target>>,
    targets: [Vec<Node<P::Application>>; 2],
    bindings: BTreeMap<(u128, NodeId), Vec<u8>>,
    stopped: BTreeMap<std::path::PathBuf, Vec<u8>>,
}
impl<P: TargetProfile> Nested<P> {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let mut base = Insertion::with_metadata(protocol, checkpoint, metadata);
        // Consume setup results; interruption evidence for this root path already exists.
        while base.resume_with_delivery(false).is_some() {}
        let root_facts = base.observed();
        assert!(root_facts.targets.iter().all(|t| t.activated.is_some()));
        if checkpoint {
            split::compact(&mut base.source, &base.clock, 20);
            split::compact(&mut base.targets[1], &base.clock, 22);
        }
        Insertion::stop(
            std::mem::take(&mut base.source),
            &base.clock,
            20,
            checkpoint,
        );
        Insertion::stop(
            std::mem::take(&mut base.targets[1]),
            &base.clock,
            22,
            checkpoint,
        );
        let mut stopped = durable_files(&base.root.join("20"));
        stopped.extend(durable_files(&base.root.join("22")));
        let configs: [Vec<NativeStartup>; 2] = std::array::from_fn(|i| {
            configuration(
                &base.root,
                31 + i as u128,
                &[1, 2, 3],
                NativeOpenMode::Recover,
            )
        });
        let before = base.intent.insertion_children().unwrap()[0]
            .manifest
            .clone();
        let creations: [GroupCreationStatus; 2] = std::array::from_fn(|i| {
            let g = 31 + i as u128;
            let value = GroupCreationIntent {
                authority: group(1),
                parent: before.input().responsibility,
                expected: before.input().generation,
                responsibility: responsibility(g),
                bootstrap: configs[i][0].bootstrap.clone(),
                application: before.input().application,
                mode: GroupCreationMode::Staging,
            };
            assert_eq!(
                propose_recovering(
                    &mut base.parent,
                    &base.clock,
                    1,
                    g * 10,
                    value.encode(100000).unwrap()
                )
                .outcome,
                DirectoryOutcome::CreationReserved
            );
            let _ = split::observe(
                &mut base.parent,
                &base.clock,
                1,
                DirectoryQuery::Manifest(before.input().responsibility),
            );
            let core = base.parent[0].local().owner.core(group(1)).unwrap();
            base.parent[0].local().applications[&group(1)]
                .directory()
                .group_creation_at(core.state().commit_index, group(g))
                .unwrap()
                .unwrap()
        });
        let mut children = Vec::new();
        let mut bindings = BTreeMap::new();
        for c in configuration(&base.root, 21, &[1, 2, 3], NativeOpenMode::Recover) {
            let mut b = FileCreationBindings::open(&c.directory).unwrap();
            bindings.insert((21, c.node), b.load().unwrap().unwrap());
        }
        for i in 0..2 {
            for c in &configs[i] {
                bindings.insert(
                    (31 + i as u128, c.node),
                    establish(&base.parent, c, &creations[i]),
                );
            }
            let mut m = before.clone().into_input();
            m.responsibility = responsibility(31 + i as u128);
            m.parent = Some(ParentAuthority {
                responsibility: before.input().responsibility,
                group: group(1),
            });
            m.scope =
                source_fixture::range(if i == 0 { 0 } else { 64 }, if i == 0 { 64 } else { 128 });
            m.execution = ExecutionMode::Single(group(31 + i as u128));
            children.push(
                InsertionChild::from_creation(
                    ResponsibilityManifest::new(m).unwrap(),
                    &creations[i],
                )
                .unwrap(),
            );
        }
        let mut after = before.clone().into_input();
        after.epoch = OwnershipEpoch::new(2).unwrap();
        after.generation = RouteGeneration::new(2).unwrap();
        after.execution = ExecutionMode::Delegated(
            children
                .iter()
                .map(|c| RouteEntry {
                    scope: c.manifest.input().scope,
                    target: RouteTarget::Child(ChildAuthority {
                        responsibility: c.manifest.input().responsibility,
                        group: group(1),
                        epoch: c.manifest.input().epoch,
                    }),
                })
                .collect(),
        );
        let plan = DelegationPlan::insertion(
            base.intent.after().clone(),
            before,
            ResponsibilityManifest::new(after).unwrap(),
            children,
            source_fixture::op(300),
        )
        .unwrap();
        Self {
            root: base.root,
            clock: base.clock,
            protocol,
            checkpoint,
            original: base.intent,
            plan,
            creations,
            bound: None,
            parent: base.parent,
            source: std::mem::take(&mut base.targets[0]),
            targets: std::array::from_fn(|_| Vec::new()),
            bindings,
            stopped,
        }
    }
    fn observed(&mut self) -> Facts {
        let DirectoryRead::DelegationReservation(reservation) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::DelegationReservation(source_fixture::op(400)),
        ) else {
            panic!("reservation")
        };
        if let Some(r) = &reservation {
            assert_eq!(r.plan, self.plan);
            let core = self.parent[0].local().owner.core(group(1)).unwrap();
            let bound = r
                .child_intent(core.state().bootstrap.configuration)
                .unwrap();
            if let Some(old) = &self.bound {
                assert_eq!(old, &bound);
            } else {
                self.bound = Some(bound);
            }
            for i in 0..2 {
                if self.targets[i].is_empty() {
                    let intent = self.bound.as_ref().unwrap();
                    self.targets[i] = open(
                        configuration(
                            &self.root,
                            31 + i as u128,
                            &[1, 2, 3],
                            NativeOpenMode::Recover,
                        ),
                        &self.clock,
                        self.protocol,
                        || profile_target::<P>(intent, 31 + i as u128),
                    );
                }
            }
        } else {
            assert!(self.bound.is_none() && self.targets.iter().all(Vec::is_empty));
        }
        let DirectoryRead::Transfer(intent) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Transfer(source_fixture::op(300)),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(publication) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(300)),
        ) else {
            panic!("publication")
        };
        let DirectoryRead::DelegationPublication(completion) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::DelegationPublication(source_fixture::op(400)),
        ) else {
            panic!("completion")
        };
        let DirectoryRead::Manifest(Some(parent)) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Manifest(self.original.before().input().responsibility),
        ) else {
            panic!("parent")
        };
        let DirectoryRead::Manifest(Some(child)) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Manifest(self.plan.before().input().responsibility),
        ) else {
            panic!("child")
        };
        let grandchildren = std::array::from_fn(|i| {
            let DirectoryRead::Manifest(m) = split::observe(
                &mut self.parent,
                &self.clock,
                1,
                DirectoryQuery::Manifest(responsibility(31 + i as u128)),
            ) else {
                panic!("grandchild")
            };
            let core = self.parent[0].local().owner.core(group(1)).unwrap();
            assert_eq!(
                self.parent[0].local().applications[&group(1)]
                    .directory()
                    .group_creation_at(core.state().commit_index, group(31 + i as u128))
                    .unwrap()
                    .as_ref(),
                Some(&self.creations[i])
            );
            m
        });
        if publication.is_some() {
            assert_eq!(&child, self.plan.after());
            let mut cache = NativeManifestCache::new(ManifestCacheLimits {
                manifests: 4,
                bytes: 65536,
            })
            .unwrap();
            cache.admit(parent.clone()).unwrap();
            cache.admit(child.clone()).unwrap();
            for (i, m) in grandchildren.iter().enumerate() {
                assert_eq!(
                    m.as_ref(),
                    Some(&self.plan.insertion_children().unwrap()[i].manifest)
                );
                cache.admit(m.clone().unwrap()).unwrap();
            }
            for (g, key) in [(31, 1), (32, 80)] {
                let result = resolve(
                    &cache,
                    &source_fixture::Policy,
                    parent.input().responsibility,
                    &[key],
                    4,
                );
                if completion.is_some() {
                    assert_eq!(
                        result.unwrap(),
                        hint(
                            self.bound
                                .as_ref()
                                .unwrap()
                                .target_manifest(group(g))
                                .unwrap(),
                            g,
                            key
                        )
                    );
                } else {
                    assert_eq!(result, Err(RoutingError::WrongChild));
                }
            }
        } else {
            assert_eq!(&child, self.plan.before());
            assert!(grandchildren.iter().all(Option::is_none));
        }
        if completion.is_some() {
            assert_eq!(parent.input().generation, RouteGeneration::new(3).unwrap());
        } else {
            assert_eq!(&parent, self.plan.parent());
        }
        let TargetRead::Status(source) =
            split::observe(&mut self.source, &self.clock, 21, TargetQuery::Status)
        else {
            panic!("source status")
        };
        let TargetRead::Freeze(fence) =
            split::observe(&mut self.source, &self.clock, 21, TargetQuery::Freeze)
        else {
            panic!("source fence")
        };
        let targets = std::array::from_fn(|i| {
            if self.targets[i].is_empty() {
                None
            } else {
                let TargetRead::Status(s) = observe_target::<P>(
                    &mut self.targets[i],
                    &self.clock,
                    31 + i as u128,
                    TargetQuery::Status,
                ) else {
                    panic!("target status")
                };
                Some(s)
            }
        });
        Facts {
            reservation,
            intent,
            publication,
            completion,
            parent,
            child,
            grandchildren,
            source,
            fence,
            targets,
        }
    }
    fn resume(&mut self) -> Option<Request> {
        self.resume_with_delivery(true)
    }
    fn resume_with_delivery(&mut self, keep_unread: bool) -> Option<Request> {
        let f = self.observed();
        if f.fence.is_none()
            && self.source[0].local().applications[&group(21)]
                .application()
                .value(&[80])
                .unwrap()
                == 0
        {
            let b = data(self.plan.before(), 21, 80, 5);
            deliver(
                keep_unread,
                &mut self.source,
                &self.clock,
                21,
                80,
                b.clone(),
                |a| a.application().value(&[80]).unwrap() == 5,
            );
            return Some(Request {
                step: Step::Data,
                group: 21,
                operation: 80,
                bytes: b,
            });
        }
        if f.reservation.is_none() {
            let b = self.plan.encode(100000).unwrap();
            deliver(
                keep_unread,
                &mut self.parent,
                &self.clock,
                1,
                400,
                b.clone(),
                |a| {
                    a.directory()
                        .delegation_reservation_at(a.applied_index(), source_fixture::op(400))
                        .unwrap()
                        .is_some()
                },
            );
            return Some(Request {
                step: Step::Reserve,
                group: 1,
                operation: 400,
                bytes: b,
            });
        }
        let intent = self.bound.as_ref().unwrap();
        if f.intent.is_none() {
            let b = intent.encode(100000).unwrap();
            deliver(
                keep_unread,
                &mut self.parent,
                &self.clock,
                1,
                300,
                b.clone(),
                |a| {
                    a.directory()
                        .transfer_intent_at(a.applied_index(), source_fixture::op(300))
                        .unwrap()
                        .is_some()
                },
            );
            return Some(Request {
                step: Step::Intent,
                group: 1,
                operation: 300,
                bytes: b,
            });
        }
        assert_eq!(&f.intent.as_ref().unwrap().intent, intent);
        for i in 0..2 {
            if f.targets[i].as_ref().unwrap().staged_index.is_none() {
                let g = 31 + i as u128;
                let b = target(intent, g).bootstrap_command(100000).unwrap();
                deliver(
                    keep_unread,
                    &mut self.targets[i],
                    &self.clock,
                    g,
                    300,
                    b.clone(),
                    |a| P::owner(a).status().staged_index.is_some(),
                );
                return Some(Request {
                    step: Step::Stage(g),
                    group: g,
                    operation: 300,
                    bytes: b,
                });
            }
        }
        if f.fence.is_none() {
            let b = self.source[0].local().applications[&group(21)]
                .freeze_command(intent, 65536, 100000)
                .unwrap();
            deliver(
                keep_unread,
                &mut self.source,
                &self.clock,
                21,
                300,
                b.clone(),
                |a| a.fence().is_some(),
            );
            return Some(Request {
                step: Step::Fence,
                group: 21,
                operation: 300,
                bytes: b,
            });
        }
        let fence = f.fence.as_ref().unwrap();
        assert_eq!(&fence.intent, intent);
        let configuration = self.source[0]
            .local()
            .owner
            .core(group(21))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        for i in 0..2 {
            if f.targets[i].as_ref().unwrap().imported.is_none() {
                let g = 31 + i as u128;
                let import = TargetImport::new(
                    source_fixture::op(300),
                    intent.clone(),
                    group(g),
                    vec![SourceImport {
                        fence: fence.fence,
                        configuration,
                        image: self.source[0].local().applications[&group(21)]
                            .export_target(group(g), 65536)
                            .unwrap(),
                        digest: fence
                            .exports
                            .iter()
                            .find(|e| e.target == group(g))
                            .unwrap()
                            .digest,
                    }],
                )
                .unwrap_or_else(|e| panic!("{:?}", e.0));
                let b = P::owner(&self.targets[i][0].local().applications[&group(g)])
                    .import_command(&import, 100000)
                    .unwrap();
                deliver(
                    keep_unread,
                    &mut self.targets[i],
                    &self.clock,
                    g,
                    300,
                    b.clone(),
                    |a| P::owner(a).status().imported.is_some(),
                );
                return Some(Request {
                    step: Step::Import(g),
                    group: g,
                    operation: 300,
                    bytes: b,
                });
            }
        }
        if f.publication.is_none() {
            let source = SourceFenceEvidence::from_status(configuration, fence.clone())
                .unwrap_or_else(|e| panic!("{:?}", e.0));
            let targets = f
                .targets
                .into_iter()
                .enumerate()
                .map(|(i, t)| {
                    TargetReadyEvidence::from_status(
                        self.creations[i].intent.bootstrap.configuration,
                        t.unwrap(),
                    )
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
                })
                .collect();
            let p = TransferPublication::new(
                source_fixture::op(300),
                intent.clone(),
                vec![source],
                targets,
            )
            .unwrap_or_else(|e| panic!("{:?}", e.0));
            let b = p.encode(100000).unwrap();
            deliver(
                keep_unread,
                &mut self.parent,
                &self.clock,
                1,
                301,
                b.clone(),
                |a| {
                    a.directory()
                        .transfer_publication_at(a.applied_index(), source_fixture::op(300))
                        .unwrap()
                        .is_some()
                },
            );
            return Some(Request {
                step: Step::Publish,
                group: 1,
                operation: 301,
                bytes: b,
            });
        }
        if f.completion.is_none() {
            let configuration = self.parent[0]
                .local()
                .owner
                .core(group(1))
                .unwrap()
                .state()
                .bootstrap
                .configuration;
            let completion = DelegationCompletion {
                reservation: source_fixture::op(400),
                reservation_index: f.reservation.unwrap().index,
                parent_configuration: configuration,
                child_configuration: configuration,
                decision: f.publication.unwrap(),
            };
            let b = completion.encode(100000).unwrap();
            deliver(
                keep_unread,
                &mut self.parent,
                &self.clock,
                1,
                403,
                b.clone(),
                |a| {
                    a.directory()
                        .delegation_publication_at(a.applied_index(), source_fixture::op(400))
                        .unwrap()
                        .is_some()
                },
            );
            return Some(Request {
                step: Step::Refresh,
                group: 1,
                operation: 403,
                bytes: b,
            });
        }
        None
    }
    fn service(&mut self, f: &Facts) {
        let before = self.plan.before();
        assert_eq!(
            split::observe(&mut self.source, &self.clock, 21, query(before, 21, 1)),
            if f.fence.is_some() {
                TargetRead::Rejected(RoutingError::Fenced)
            } else {
                TargetRead::Data(7)
            }
        );
        if f.fence.is_some() {
            assert!(self.source[0]
                .propose(ClientRequest {
                    group: group(21),
                    operation: source_fixture::op(900),
                    bytes: data(before, 21, 1, 1)
                })
                .is_err());
        }
        if let Some(intent) = &self.bound {
            for i in 0..2 {
                let g = 31 + i as u128;
                let key = if i == 0 { 1 } else { 80 };
                let m = intent.target_manifest(group(g)).unwrap();
                assert_eq!(
                    observe_target::<P>(&mut self.targets[i], &self.clock, g, query(m, g, key)),
                    TargetRead::NotActive
                );
                assert!(self.targets[i][0]
                    .propose(ClientRequest {
                        group: group(g),
                        operation: source_fixture::op(900),
                        bytes: data(m, g, key, 1)
                    })
                    .is_err());
            }
        }
    }
    fn stop(&mut self) {
        if self.checkpoint {
            if !self.parent.is_empty() {
                split::compact(&mut self.parent, &self.clock, 1);
            }
            if !self.source.is_empty() {
                split::compact(&mut self.source, &self.clock, 21);
            }
            for i in 0..2 {
                if !self.targets[i].is_empty() {
                    split::compact(&mut self.targets[i], &self.clock, 31 + i as u128);
                }
            }
        }
        if !self.parent.is_empty() {
            Insertion::stop(
                std::mem::take(&mut self.parent),
                &self.clock,
                1,
                self.checkpoint,
            );
        }
        if !self.source.is_empty() {
            Insertion::stop(
                std::mem::take(&mut self.source),
                &self.clock,
                21,
                self.checkpoint,
            );
        }
        for i in 0..2 {
            if !self.targets[i].is_empty() {
                Insertion::stop(
                    std::mem::take(&mut self.targets[i]),
                    &self.clock,
                    31 + i as u128,
                    self.checkpoint,
                );
            }
        }
    }
    fn restart(&mut self) {
        self.stop();
        self.parent = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            metadata,
        );
        self.source = open(
            configuration(&self.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || super::target(&self.original, 21),
        );
        // Re-derive the binding from quorum-observed metadata; no cached phase drives startup.
        self.bound = None;
        let _ = self.observed();
        for g in [21, 31, 32] {
            for c in configuration(&self.root, g, &[1, 2, 3], NativeOpenMode::Recover) {
                let mut b = FileCreationBindings::open(&c.directory).unwrap();
                assert_eq!(b.load().unwrap().unwrap(), self.bindings[&(g, c.node)]);
            }
        }
    }
    fn retry(&mut self, r: &Request, f: &Facts) {
        if r.group == 1 {
            let result = propose_recovering(
                &mut self.parent,
                &self.clock,
                1,
                r.operation,
                r.bytes.clone(),
            );
            assert!(result.duplicate);
        } else if r.group == 21 {
            let result = propose_recovering(
                &mut self.source,
                &self.clock,
                21,
                r.operation,
                r.bytes.clone(),
            );
            match r.step {
                Step::Data => assert!(
                    matches!(result.outcome,TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(5))
                ),
                Step::Fence => assert!(
                    matches!(result.outcome,TargetOutcome::Frozen(v) if v==f.fence.as_ref().unwrap().fence)
                ),
                _ => panic!("source retry"),
            }
        } else {
            let i = (r.group - 31) as usize;
            let result = propose_target::<P>(
                &mut self.targets[i],
                &self.clock,
                r.group,
                r.operation,
                r.bytes.clone(),
            );
            let status = f.targets[i].as_ref().unwrap();
            match r.step {
                Step::Stage(_) => assert!(
                    matches!(result.outcome,TargetOutcome::Staged {index} if Some(index)==status.staged_index)
                ),
                Step::Import(_) => assert!(
                    matches!(result.outcome,TargetOutcome::Imported {index,digest} if status.imported.as_ref().is_some_and(|s|s.index==index&&s.digest==digest))
                ),
                _ => panic!("target retry"),
            }
        }
    }
}
fn history<P: TargetProfile>(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Nested::<P>::new(protocol, checkpoint);
    let source_original = rig.source[0].local().applications[&group(21)].status();
    for step in [
        Step::Data,
        Step::Reserve,
        Step::Intent,
        Step::Stage(31),
        Step::Stage(32),
        Step::Fence,
        Step::Import(31),
        Step::Import(32),
        Step::Publish,
        Step::Refresh,
    ] {
        eprintln!("nested insertion {protocol:?} checkpoint={checkpoint} step={step:?}");
        let request = rig.resume().unwrap();
        assert_eq!(request.step, step);
        let before = rig.observed();
        assert_eq!(before.source, source_original);
        rig.service(&before);
        rig.restart();
        let recovered = rig.observed();
        assert_eq!(recovered, before, "recovery {step:?}");
        rig.service(&recovered);
        rig.retry(&request, &before);
        assert_eq!(rig.observed(), before, "retry {step:?}");
    }
    assert!(rig.resume().is_none());
    let mut original = rig.observed();
    let publication = original.publication.clone().unwrap();
    let configuration = rig.parent[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .state()
        .bootstrap
        .configuration;
    if checkpoint {
        split::compact(&mut rig.parent, &rig.clock, 1);
        split::compact(&mut rig.source, &rig.clock, 21);
    }
    let parent_logs = Insertion::stop(std::mem::take(&mut rig.parent), &rig.clock, 1, checkpoint);
    Insertion::stop(std::mem::take(&mut rig.source), &rig.clock, 21, checkpoint);
    let parent_files = durable_files(&rig.root.join("1"));
    let source_files = durable_files(&rig.root.join("21"));
    let intent = rig.bound.clone().unwrap();
    for (i, g, key, old_op, value) in [(0, 31, 1, 1, 7), (1, 32, 80, 80, 5)] {
        let m = intent.target_manifest(group(g)).unwrap();
        let activation = TargetActivation {
            metadata_configuration: configuration,
            decision: publication.clone(),
        };
        let bytes = P::owner(&rig.targets[i][0].local().applications[&group(g)])
            .activation_command(&activation, 100000)
            .unwrap();
        unread(
            &mut rig.targets[i],
            &rig.clock,
            g,
            300,
            bytes.clone(),
            |a| P::owner(a).status().activated.is_some(),
        );
        let TargetRead::Status(status) =
            observe_target::<P>(&mut rig.targets[i], &rig.clock, g, TargetQuery::Status)
        else {
            panic!("activation")
        };
        original.targets[i] = Some(status.clone());
        if checkpoint {
            split::compact(&mut rig.targets[i], &rig.clock, g);
        }
        Insertion::stop(
            std::mem::take(&mut rig.targets[i]),
            &rig.clock,
            g,
            checkpoint,
        );
        rig.targets[i] = open(
            configuration_for(&rig.root, g),
            &rig.clock,
            protocol,
            || profile_target::<P>(&intent, g),
        );
        campaign(&mut rig.targets[i], &rig.clock, g);
        let retry = propose_target::<P>(&mut rig.targets[i], &rig.clock, g, 300, bytes);
        assert!(matches!(retry.outcome,TargetOutcome::Activated(v) if Some(v)==status.activated));
        assert_eq!(
            observe_target::<P>(&mut rig.targets[i], &rig.clock, g, query(m, g, key)),
            TargetRead::Data(value)
        );
        if i == 0 {
            assert_eq!(
                observe_target::<P>(
                    &mut rig.targets[1],
                    &rig.clock,
                    32,
                    query(intent.target_manifest(group(32)).unwrap(), 32, 80)
                ),
                TargetRead::NotActive
            );
        }
        let retry = propose_target::<P>(
            &mut rig.targets[i],
            &rig.clock,
            g,
            old_op,
            data(m, g, key, value),
        );
        assert!(
            matches!(retry.outcome,TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(value))
        );
        let write = propose_target::<P>(
            &mut rig.targets[i],
            &rig.clock,
            g,
            100 + old_op,
            data(m, g, key, 2),
        );
        assert!(
            matches!(write.outcome,TargetOutcome::Applied(v) if !v.duplicate&&v.outcome==BucketOutcome::Value(value+2))
        );
    }
    assert_eq!(durable_files(&rig.root.join("1")), parent_files);
    assert_eq!(durable_files(&rig.root.join("21")), source_files);
    let mut old_files = durable_files(&rig.root.join("20"));
    old_files.extend(durable_files(&rig.root.join("22")));
    assert_eq!(old_files, rig.stopped);
    for c in configuration_for(&rig.root, 1) {
        let log = NativeLogStore::recover(
            FileLogIo::open(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), parent_logs[&c.node]);
    }
    rig.restart();
    assert_eq!(rig.observed(), original);
    assert!(rig.resume().is_none());
    for (i, g, key, value) in [(0, 31, 1, 9), (1, 32, 80, 7)] {
        let m = intent.target_manifest(group(g)).unwrap();
        assert_eq!(
            observe_target::<P>(&mut rig.targets[i], &rig.clock, g, query(m, g, key)),
            TargetRead::Data(value)
        );
        assert!(rig.targets[i]
            .iter()
            .all(|n| P::owner(&n.local().applications[&group(g)])
                .application()
                .outbox()
                .count()
                == 2));
        assert_eq!(
            observe_target::<P>(
                &mut rig.targets[i],
                &rig.clock,
                g,
                query(rig.plan.before(), g, key)
            ),
            TargetRead::Rejected(RoutingError::WrongIdentity)
        );
    }
    assert_eq!(
        split::observe(
            &mut rig.source,
            &rig.clock,
            21,
            query(rig.plan.before(), 21, 1)
        ),
        TargetRead::Rejected(RoutingError::Fenced)
    );
    rig.stop();
    std::fs::remove_dir_all(rig.root).unwrap();
}
fn configuration_for(root: &Path, g: u128) -> Vec<NativeStartup> {
    configuration(root, g, &[1, 2, 3], NativeOpenMode::Recover)
}
#[test]
fn tcp_nested_insertion_recovers_unread_phases_from_wal() {
    history::<Raw>(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_nested_insertion_recovers_unread_phases_from_checkpoint() {
    history::<Raw>(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_insertion_recovers_unread_phases_from_wal() {
    history::<Raw>(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_insertion_recovers_unread_phases_from_checkpoint() {
    history::<Raw>(NativePeerProtocol::Quic, true);
}

#[test]
fn tcp_guarded_nested_insertion_recovers_unread_phases_from_wal() {
    history::<Guarded>(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_guarded_nested_insertion_recovers_unread_phases_from_checkpoint() {
    history::<Guarded>(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_guarded_nested_insertion_recovers_unread_phases_from_wal() {
    history::<Guarded>(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_guarded_nested_insertion_recovers_unread_phases_from_checkpoint() {
    history::<Guarded>(NativePeerProtocol::Quic, true);
}

#[path = "nested_moves.rs"]
mod moves;
