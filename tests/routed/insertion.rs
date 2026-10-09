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
use voteboat::{
    bucket_counter::{encode_add, BucketCounter, BucketOutcome},
    group_creation::*,
    native::{group_creation::*, snapshot_store::*},
    snapshot::{SnapshotIdentity, SnapshotLimits},
    transfer::*,
    transfer_publication::*,
    transfer_source::*,
    transfer_target::*,
};
type Source = source_fixture::Source;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Data(u128),
    Intent,
    Stage(u128),
    Fence,
    Import(u128),
    Publish,
    Activate(u128),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    parent: ResponsibilityManifest,
    children: [Option<ResponsibilityManifest>; 2],
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    source: Option<SourceFreezeStatus>,
    targets: [TargetStatus; 2],
}
struct Retry {
    phase: Phase,
    group: u128,
    operation: u128,
    bytes: Vec<u8>,
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_responsibility_insertion()
        .unwrap_or_else(|_| panic!("schema5")),
    )
}
fn source_query(key: u8) -> SourceQuery<Vec<u8>> {
    SourceQuery::Data(RoutedQuery {
        hint: source_fixture::hint(key),
        key: vec![key],
        query: vec![key],
    })
}
fn target(intent: &TransferIntent, g: u128) -> target_fixture::Target {
    TransferTarget::new(
        group(g),
        source_fixture::op(200),
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
fn target_hint(key: u8) -> RouteHint {
    let mut hint = source_fixture::hint(key);
    hint.group = group(if key < 128 { 21 } else { 22 });
    hint.scope = source_fixture::range(
        if key < 128 { 0 } else { 128 },
        if key < 128 { 128 } else { 256 },
    );
    hint.responsibility = responsibility(if key < 128 { 21 } else { 22 });
    hint.epoch = OwnershipEpoch::new(1).unwrap();
    hint.generation = RouteGeneration::new(1).unwrap();
    hint
}
fn target_data(key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        target_hint(key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
use super::creation_source::{durable_files, unread};
struct Insertion {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    intent: TransferIntent,
    creations: [GroupCreationStatus; 2],
    parent: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    targets: [Vec<Node<target_fixture::Target>>; 2],
    creation_records: BTreeMap<(u128, NodeId), Vec<u8>>,
}
impl Insertion {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-insertion-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut parent = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            metadata,
        );
        campaign(&mut parent, &clock, 1);
        propose_recovering(
            &mut parent,
            &clock,
            1,
            1000,
            metadata().directory().bootstrap_command(100000).unwrap(),
        );
        propose_recovering(
            &mut parent,
            &clock,
            1,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: source_fixture::grant(),
            }
            .encode(100000)
            .unwrap(),
        );
        let mut source = open(
            configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            source_fixture::fresh,
        );
        campaign(&mut source, &clock, 20);
        propose_recovering(
            &mut source,
            &clock,
            20,
            100,
            source_fixture::fresh().bootstrap_command(100000).unwrap(),
        );
        let mut configs: [Vec<NativeStartup>; 2] = std::array::from_fn(|i| {
            configuration(&root, 21 + i as u128, &[1, 2, 3], NativeOpenMode::Recover)
        });
        let creations: [GroupCreationStatus; 2] = std::array::from_fn(|i| {
            let g = 21 + i as u128;
            let reservation = GroupCreationIntent {
                authority: group(1),
                parent: source_fixture::grant().input().responsibility,
                expected: RouteGeneration::new(1).unwrap(),
                responsibility: responsibility(g),
                bootstrap: configs[i][0].bootstrap.clone(),
                application: source_fixture::grant().input().application,
                mode: GroupCreationMode::Staging,
            };
            assert_eq!(
                propose_recovering(
                    &mut parent,
                    &clock,
                    1,
                    1002 + i as u128,
                    reservation.encode(100000).unwrap()
                )
                .outcome,
                DirectoryOutcome::CreationReserved
            );
            // Quorum read before relying on this local immutable reservation.
            let _ = split::observe(
                &mut parent,
                &clock,
                1,
                DirectoryQuery::Manifest(source_fixture::grant().input().responsibility),
            );
            let core = parent[0].local().owner.core(group(1)).unwrap();
            parent[0].local().applications[&group(1)]
                .directory()
                .group_creation_at(core.state().commit_index, group(g))
                .unwrap()
                .unwrap()
        });
        let establish = |parents: &[Node<LifecycleDirectory>],
                         c: &NativeStartup,
                         status: &GroupCreationStatus| {
            std::fs::create_dir_all(c.directory.parent().unwrap()).unwrap();
            let verified = VerifiedGroupCreation::verify(
                &LocalCreationAuthority {
                    core: parents[0].local().owner.core(group(1)).unwrap(),
                    directory: parents[0].local().applications[&group(1)].directory(),
                },
                status.clone(),
                c.node,
                c.store,
                status.intent.application,
            )
            .unwrap();
            let exists = c.directory.exists();
            let mut log = if exists {
                NativeLogStore::recover(
                    FileLogIo::open(&c.directory).unwrap(),
                    c.store,
                    LogLimits::default(),
                )
                .unwrap()
            } else {
                NativeLogStore::create(
                    FileLogIo::create(&c.directory).unwrap(),
                    c.store,
                    LogLimits::default(),
                )
                .unwrap()
            };
            let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
            establish_created_group(&verified, &mut log, &mut bindings).unwrap();
            assert_eq!(
                log.state(c.bootstrap.group).unwrap().bootstrap,
                status.intent.bootstrap
            );
            let id = SnapshotIdentity {
                store: c.store,
                group: c.bootstrap.group,
            };
            if exists {
                NativeSnapshotStore::recover(
                    FileSnapshotIo::open(c.directory.join("snapshots")).unwrap(),
                    id,
                    SnapshotLimits::default(),
                )
                .unwrap();
            } else {
                NativeSnapshotStore::create(
                    FileSnapshotIo::create(c.directory.join("snapshots")).unwrap(),
                    id,
                    SnapshotLimits::default(),
                )
                .unwrap();
            }
            bindings.load().unwrap().unwrap()
        };
        // Partial assigned provisioning: only two stores in child21 exist.
        for c in &configs[0][..2] {
            establish(&parent, c, &creations[0]);
        }
        assert!(!configs[0][2].directory.exists());
        assert!(!configs[1][0].directory.exists());
        assert!(parent.iter().all(|p| p.local().applications[&group(1)]
            .directory()
            .manifest(responsibility(21))
            .is_none()));
        close(parent, &clock, 1, || {
            drive(&mut source, &clock, |_| true);
        });
        parent = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &clock,
            protocol,
            metadata,
        );
        campaign(&mut parent, &clock, 1);
        let mut records = BTreeMap::new();
        let mut children = Vec::new();
        for i in 0..2 {
            let r = propose_recovering(
                &mut parent,
                &clock,
                1,
                1002 + i as u128,
                creations[i].intent.encode(100000).unwrap(),
            );
            assert!(r.duplicate);
            let _ = split::observe(
                &mut parent,
                &clock,
                1,
                DirectoryQuery::Manifest(source_fixture::grant().input().responsibility),
            );
            for c in &configs[i] {
                records.insert(
                    (21 + i as u128, c.node),
                    establish(&parent, c, &creations[i]),
                );
            }
            let mut m = source_fixture::grant().into_input();
            m.responsibility = responsibility(21 + i as u128);
            m.parent = Some(ParentAuthority {
                responsibility: source_fixture::grant().input().responsibility,
                group: group(1),
            });
            m.scope =
                source_fixture::range(if i == 0 { 0 } else { 128 }, if i == 0 { 128 } else { 256 });
            m.execution = ExecutionMode::Single(group(21 + i as u128));
            children.push(
                InsertionChild::from_creation(
                    ResponsibilityManifest::new(m).unwrap(),
                    &creations[i],
                )
                .unwrap(),
            );
        }
        let mut after = source_fixture::grant().into_input();
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
        let intent = TransferIntent::insert_children(
            source_fixture::grant(),
            ResponsibilityManifest::new(after).unwrap(),
            children,
        )
        .unwrap();
        let targets = std::array::from_fn(|i| {
            open(std::mem::take(&mut configs[i]), &clock, protocol, || {
                target(&intent, 21 + i as u128)
            })
        });
        Self {
            root,
            clock,
            protocol,
            checkpoint,
            intent,
            creations,
            parent,
            source,
            targets,
            creation_records: records,
        }
    }
    fn observed(&mut self) -> Observed {
        let DirectoryRead::Transfer(intent) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Transfer(source_fixture::op(200)),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(publication) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(200)),
        ) else {
            panic!("publication")
        };
        let DirectoryRead::Manifest(Some(parent)) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Manifest(source_fixture::grant().input().responsibility),
        ) else {
            panic!("parent manifest")
        };
        let children = std::array::from_fn(|i| {
            let DirectoryRead::Manifest(child) = split::observe(
                &mut self.parent,
                &self.clock,
                1,
                DirectoryQuery::Manifest(responsibility(21 + i as u128)),
            ) else {
                panic!("child manifest")
            };
            let core = self.parent[0].local().owner.core(group(1)).unwrap();
            assert_eq!(
                self.parent[0].local().applications[&group(1)]
                    .directory()
                    .group_creation_at(core.state().commit_index, group(21 + i as u128))
                    .unwrap()
                    .as_ref(),
                Some(&self.creations[i])
            );
            child
        });
        if publication.is_some() {
            assert_eq!(parent, *self.intent.after());
            let mut cache = NativeManifestCache::new(ManifestCacheLimits {
                manifests: 3,
                bytes: 65536,
            })
            .unwrap();
            cache.admit(parent.clone()).unwrap();
            for (i, child) in children.iter().enumerate() {
                assert_eq!(
                    child.as_ref(),
                    Some(&self.intent.insertion_children().unwrap()[i].manifest)
                );
                cache.admit(child.clone().unwrap()).unwrap();
            }
            for key in [1, 200] {
                assert_eq!(
                    resolve(
                        &cache,
                        &source_fixture::Policy,
                        parent.input().responsibility,
                        &[key],
                        3
                    )
                    .unwrap(),
                    target_hint(key)
                );
            }
        } else {
            assert_eq!(parent, source_fixture::grant());
            assert!(children.iter().all(Option::is_none));
        }
        let source = match split::observe(&mut self.source, &self.clock, 20, SourceQuery::Freeze) {
            SourceRead::Freeze(s) => s,
            _ => panic!("freeze status"),
        };
        let targets = std::array::from_fn(|i| {
            let TargetRead::Status(s) = split::observe(
                &mut self.targets[i],
                &self.clock,
                21 + i as u128,
                TargetQuery::Status,
            ) else {
                panic!("target status")
            };
            s
        });
        Observed {
            parent,
            children,
            intent,
            publication,
            source,
            targets,
        }
    }
    fn resume_one(&mut self) -> Option<Retry> {
        let state = self.observed();
        for (op, key, value) in [(1, 1u8, 7), (2, 200u8, 11)] {
            if self.source[0].local().applications[&group(20)]
                .routed()
                .application()
                .value(&[key])
                .unwrap()
                == 0
                && state.source.is_none()
            {
                let b = source_fixture::data(key, value);
                unread(&mut self.source, &self.clock, 20, op, b.clone(), |a| {
                    a.routed().application().value(&[key]).unwrap() == value
                });
                return Some(Retry {
                    phase: Phase::Data(op),
                    group: 20,
                    operation: op,
                    bytes: b,
                });
            }
        }
        if state.intent.is_none() {
            let b = self
                .intent
                .clone()
                .encode(MAX_TRANSFER_INTENT_BYTES)
                .unwrap();
            unread(&mut self.parent, &self.clock, 1, 200, b.clone(), |a| {
                a.directory()
                    .transfer_intent_at(a.applied_index(), source_fixture::op(200))
                    .unwrap()
                    .is_some()
            });
            return Some(Retry {
                phase: Phase::Intent,
                group: 1,
                operation: 200,
                bytes: b,
            });
        }
        assert_eq!(state.intent.as_ref().unwrap().intent, self.intent.clone());
        for i in 0..2 {
            if state.targets[i].staged_index.is_none() {
                let g = 21 + i as u128;
                let b = target(&self.intent, g).bootstrap_command(100000).unwrap();
                unread(&mut self.targets[i], &self.clock, g, 200, b.clone(), |a| {
                    a.status().staged_index.is_some()
                });
                return Some(Retry {
                    phase: Phase::Stage(g),
                    group: g,
                    operation: 200,
                    bytes: b,
                });
            }
        }
        if state.source.is_none() {
            let b = Source::freeze_command(&self.intent, 100000).unwrap();
            unread(&mut self.source, &self.clock, 20, 200, b.clone(), |a| {
                a.fence().is_some()
            });
            return Some(Retry {
                phase: Phase::Fence,
                group: 20,
                operation: 200,
                bytes: b,
            });
        }
        let status = state.source.as_ref().unwrap();
        let configuration = self.source[0]
            .local()
            .owner
            .core(group(20))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        for i in 0..2 {
            if state.targets[i].imported.is_none() {
                let g = 21 + i as u128;
                let digest = status
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest;
                let import = TargetImport::new(
                    source_fixture::op(200),
                    self.intent.clone(),
                    group(g),
                    vec![SourceImport {
                        fence: status.fence,
                        configuration,
                        image: self.source[0].local().applications[&group(20)]
                            .export_target(group(g), 65536)
                            .unwrap(),
                        digest,
                    }],
                )
                .unwrap_or_else(|e| panic!("import {:?}", e.0));
                let b = self.targets[i][0].local().applications[&group(g)]
                    .import_command(&import, 100000)
                    .unwrap();
                unread(&mut self.targets[i], &self.clock, g, 200, b.clone(), |a| {
                    a.status().imported.is_some()
                });
                return Some(Retry {
                    phase: Phase::Import(g),
                    group: g,
                    operation: 200,
                    bytes: b,
                });
            }
        }
        if state.publication.is_none() {
            let source = SourceFenceEvidence::from_status(configuration, state.source.unwrap())
                .unwrap_or_else(|e| panic!("source {:?}", e.0));
            let targets = state
                .targets
                .into_iter()
                .enumerate()
                .map(|(i, t)| {
                    TargetReadyEvidence::from_status(
                        self.targets[i][0]
                            .local()
                            .owner
                            .core(group(21 + i as u128))
                            .unwrap()
                            .state()
                            .bootstrap
                            .configuration,
                        t,
                    )
                    .unwrap_or_else(|e| panic!("ready {:?}", e.0))
                })
                .collect();
            let p = TransferPublication::new(
                source_fixture::op(200),
                self.intent.clone(),
                vec![source],
                targets,
            )
            .unwrap_or_else(|e| panic!("publication {:?}", e.0));
            let b = p.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
            unread(&mut self.parent, &self.clock, 1, 201, b.clone(), |a| {
                a.directory()
                    .transfer_publication_at(a.applied_index(), source_fixture::op(200))
                    .unwrap()
                    .is_some()
            });
            return Some(Retry {
                phase: Phase::Publish,
                group: 1,
                operation: 201,
                bytes: b,
            });
        }
        for i in 0..2 {
            if state.targets[i].activated.is_none() {
                let g = 21 + i as u128;
                let activation = TargetActivation {
                    metadata_configuration: self.parent[0]
                        .local()
                        .owner
                        .core(group(1))
                        .unwrap()
                        .state()
                        .bootstrap
                        .configuration,
                    decision: state.publication.clone().unwrap(),
                };
                let b = self.targets[i][0].local().applications[&group(g)]
                    .activation_command(&activation, 100000)
                    .unwrap();
                unread(&mut self.targets[i], &self.clock, g, 200, b.clone(), |a| {
                    a.status().activated.is_some()
                });
                return Some(Retry {
                    phase: Phase::Activate(g),
                    group: g,
                    operation: 200,
                    bytes: b,
                });
            }
        }
        None
    }
    fn serving(&mut self, state: &Observed) {
        let expected = if state.source.is_some() {
            SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
        } else {
            SourceRead::Data(RoutedRead::Served(
                self.source[0].local().applications[&group(20)]
                    .routed()
                    .application()
                    .value(&[1])
                    .unwrap(),
            ))
        };
        assert_eq!(
            split::observe(&mut self.source, &self.clock, 20, source_query(1)),
            expected
        );
        if state.source.is_some() {
            assert!(self.source[0]
                .propose(ClientRequest {
                    group: group(20),
                    operation: source_fixture::op(900),
                    bytes: source_fixture::data(1, 1)
                })
                .is_err());
        }
        for (i, key, value) in [(0, 1u8, 7), (1, 200u8, 11)] {
            let g = 21 + i as u128;
            let result = split::observe(
                &mut self.targets[i],
                &self.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: target_hint(key),
                    key: vec![key],
                    query: vec![key],
                }),
            );
            if state.targets[i].activated.is_some() {
                assert!(state.source.is_some() && state.publication.is_some());
                assert_eq!(result, TargetRead::Data(value));
            } else {
                assert_eq!(result, TargetRead::NotActive);
                assert!(self.targets[i][0]
                    .propose(ClientRequest {
                        group: group(g),
                        operation: source_fixture::op(900),
                        bytes: target_data(key, 1)
                    })
                    .is_err());
            }
        }
    }
    fn stop<A>(
        nodes: Vec<Node<A>>,
        _clock: &Instant,
        g: u128,
        checkpoint: bool,
    ) -> BTreeMap<NodeId, GroupLog>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let logs = creation::abandon(nodes, g);
        assert!(logs.values().all(|s| if checkpoint {
            s.base_index() > 0
        } else {
            s.base_index() == 0
        }));
        logs
    }
    fn stop_all(&mut self) {
        if self.checkpoint {
            if !self.parent.is_empty() {
                split::compact(&mut self.parent, &self.clock, 1);
            }
            if !self.source.is_empty() {
                split::compact(&mut self.source, &self.clock, 20);
            }
            for i in 0..2 {
                if !self.targets[i].is_empty() {
                    split::compact(&mut self.targets[i], &self.clock, 21 + i as u128);
                }
            }
        }
        if !self.parent.is_empty() {
            Self::stop(
                std::mem::take(&mut self.parent),
                &self.clock,
                1,
                self.checkpoint,
            );
        }
        if !self.source.is_empty() {
            Self::stop(
                std::mem::take(&mut self.source),
                &self.clock,
                20,
                self.checkpoint,
            );
        }
        for i in 0..2 {
            if !self.targets[i].is_empty() {
                Self::stop(
                    std::mem::take(&mut self.targets[i]),
                    &self.clock,
                    21 + i as u128,
                    self.checkpoint,
                );
            }
        }
    }
    fn restart(&mut self) {
        self.stop_all();
        self.parent = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            metadata,
        );
        self.source = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            source_fixture::fresh,
        );
        self.targets = std::array::from_fn(|i| {
            open(
                configuration(
                    &self.root,
                    21 + i as u128,
                    &[1, 2, 3],
                    NativeOpenMode::Recover,
                ),
                &self.clock,
                self.protocol,
                || target(&self.intent, 21 + i as u128),
            )
        });
        for i in 0..2 {
            for c in configuration(
                &self.root,
                21 + i as u128,
                &[1, 2, 3],
                NativeOpenMode::Recover,
            ) {
                let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
                assert_eq!(
                    bindings.load().unwrap().unwrap(),
                    self.creation_records[&(21 + i as u128, c.node)]
                );
            }
        }
    }

    fn retry(&mut self, retry: &Retry, expected: &Observed) {
        match retry.group {
            1 => {
                assert!(
                    propose_recovering(
                        &mut self.parent,
                        &self.clock,
                        1,
                        retry.operation,
                        retry.bytes.clone()
                    )
                    .duplicate
                );
            }
            20 => {
                let r = propose_recovering(
                    &mut self.source,
                    &self.clock,
                    20,
                    retry.operation,
                    retry.bytes.clone(),
                );
                match retry.phase {
                    Phase::Data(_) => {
                        assert!(matches!(r.outcome,RoutedOutcome::Applied(v) if v.duplicate))
                    }
                    Phase::Fence => assert!(
                        matches!(r.outcome,RoutedOutcome::Fenced(f) if f==expected.source.as_ref().unwrap().fence)
                    ),
                    _ => panic!("unexpected source retry"),
                }
            }
            g => {
                let i = (g - 21) as usize;
                let r = propose_recovering(
                    &mut self.targets[i],
                    &self.clock,
                    g,
                    retry.operation,
                    retry.bytes.clone(),
                );
                match retry.phase {
                    Phase::Stage(_) => assert!(
                        matches!(r.outcome,TargetOutcome::Staged {index} if Some(index)==expected.targets[i].staged_index)
                    ),
                    Phase::Import(_) => assert!(
                        matches!(r.outcome,TargetOutcome::Imported {index,digest} if expected.targets[i].imported.as_ref().is_some_and(|s|s.index==index&&s.digest==digest))
                    ),
                    Phase::Activate(_) => assert!(
                        matches!(r.outcome,TargetOutcome::Activated(ref s) if Some(s)==expected.targets[i].activated.as_ref())
                    ),
                    _ => panic!("unexpected target retry"),
                }
            }
        }
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Insertion::new(protocol, checkpoint);
    let phases = [
        Phase::Data(1),
        Phase::Data(2),
        Phase::Intent,
        Phase::Stage(21),
        Phase::Stage(22),
        Phase::Fence,
        Phase::Import(21),
        Phase::Import(22),
        Phase::Publish,
    ];
    for phase in phases {
        eprintln!("insertion {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        let retry = rig.resume_one().unwrap();
        assert_eq!(retry.phase, phase);
        let before = rig.observed();
        rig.serving(&before);
        rig.restart();
        let after = rig.observed();
        assert_eq!(after, before, "recovery {phase:?}");
        rig.serving(&after);
        rig.retry(&retry, &before);
        assert_eq!(rig.observed(), before, "original retry {phase:?}");
    }
    let mut original = rig.observed();
    assert!(original.targets.iter().all(|s| s.activated.is_none()));
    let publication = original.publication.clone().unwrap();
    let metadata_configuration = rig.parent[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .state()
        .bootstrap
        .configuration;
    if checkpoint {
        split::compact(&mut rig.parent, &rig.clock, 1);
        split::compact(&mut rig.source, &rig.clock, 20);
    }
    let parent_logs = Insertion::stop(std::mem::take(&mut rig.parent), &rig.clock, 1, checkpoint);
    Insertion::stop(std::mem::take(&mut rig.source), &rig.clock, 20, checkpoint);
    let parent_files = durable_files(&rig.root.join("1"));
    // Publication is already quorum-observed. Both original activations now run
    // with source and metadata fully stopped, using the retained exact decision.
    for i in 0..2 {
        let g = 21 + i as u128;
        let key = if i == 0 { 1 } else { 200 };
        let value = if i == 0 { 7 } else { 11 };
        let activation = TargetActivation {
            metadata_configuration,
            decision: publication.clone(),
        };
        let bytes = rig.targets[i][0].local().applications[&group(g)]
            .activation_command(&activation, 100000)
            .unwrap();
        unread(
            &mut rig.targets[i],
            &rig.clock,
            g,
            200,
            bytes.clone(),
            |a| a.status().activated.is_some(),
        );
        let TargetRead::Status(status) =
            split::observe(&mut rig.targets[i], &rig.clock, g, TargetQuery::Status)
        else {
            panic!("activation status")
        };
        original.targets[i] = status;
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
            configuration(&rig.root, g, &[1, 2, 3], NativeOpenMode::Recover),
            &rig.clock,
            protocol,
            || target(&rig.intent, g),
        );
        campaign(&mut rig.targets[i], &rig.clock, g);
        let r = propose_recovering(&mut rig.targets[i], &rig.clock, g, 200, bytes);
        assert!(
            matches!(r.outcome,TargetOutcome::Activated(ref s) if Some(s)==original.targets[i].activated.as_ref())
        );
        assert_eq!(
            split::observe(
                &mut rig.targets[i],
                &rig.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: target_hint(key),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Data(value)
        );
        if i == 0 {
            assert_eq!(
                split::observe(
                    &mut rig.targets[1],
                    &rig.clock,
                    22,
                    TargetQuery::Data(RoutedQuery {
                        hint: target_hint(200),
                        key: vec![200],
                        query: vec![200]
                    })
                ),
                TargetRead::NotActive
            );
        }
        let old_op = 1 + i as u128;
        let r = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            old_op,
            target_data(key, value),
        );
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(value))
        );
        let r = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            100 + old_op,
            target_data(key, 2),
        );
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if !v.duplicate&&v.outcome==BucketOutcome::Value(value+2))
        );
    }
    assert_eq!(durable_files(&rig.root.join("1")), parent_files);
    for c in configuration(&rig.root, 1, &[1, 2, 3], NativeOpenMode::Recover) {
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
    assert!(rig.resume_one().is_none());
    for (i, key, value) in [(0, 1u8, 9), (1, 200u8, 13)] {
        let g = 21 + i as u128;
        assert_eq!(
            split::observe(
                &mut rig.targets[i],
                &rig.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: target_hint(key),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Data(value)
        );
        assert!(rig.targets[i]
            .iter()
            .all(|n| n.local().applications[&group(g)]
                .application()
                .outbox()
                .count()
                == 2));
        assert!(matches!(
            split::observe(
                &mut rig.targets[i],
                &rig.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: source_fixture::hint(key),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Rejected(_)
        ));
    }
    assert_eq!(
        split::observe(&mut rig.source, &rig.clock, 20, source_query(1)),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        rig.source[0].local().applications[&group(20)]
            .export_target(group(21), 65536)
            .unwrap()
            .source_applied(),
        original.source.as_ref().unwrap().fence.index
    );
    rig.stop_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_insertion_recovers_unread_phases_from_wal() {
    history(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_insertion_recovers_unread_phases_from_checkpoint() {
    history(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_insertion_recovers_unread_phases_from_wal() {
    history(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_insertion_recovers_unread_phases_from_checkpoint() {
    history(NativePeerProtocol::Quic, true)
}
