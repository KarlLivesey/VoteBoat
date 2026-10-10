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
    namespace_creation::*,
    native::{group_creation::*, snapshot_store::*},
    snapshot::{SnapshotIdentity, SnapshotLimits},
    transfer::*,
    transfer_publication::*,
    transfer_source::*,
    transfer_target::*,
};
type Source = CreatedNamespaceSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Ready,
    NamespacePublish,
    NamespaceActivate,
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
    namespace: NamespaceStatus,
    namespace_publication: Option<NamespacePublicationStatus>,
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
fn anchor() -> ResponsibilityManifest {
    let mut m = source_fixture::grant().into_input();
    m.responsibility = responsibility(1);
    m.execution = ExecutionMode::Single(group(30));
    ResponsibilityManifest::new(m).unwrap()
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![anchor()]).unwrap(),
            DirectoryLimits {
                operations: 16,
                history_bytes: 65536,
            },
        )
        .unwrap()
        .with_namespace_transfers()
        .unwrap_or_else(|_| panic!("fresh schema4")),
    )
}
fn source(plan: &NamespacePlan) -> Source {
    Source::from_source(plan.clone(), source_fixture::fresh())
        .unwrap_or_else(|e| panic!("source binding {:?}", e.0))
}
fn source_query(key: u8) -> SourceNamespaceQuery<Vec<u8>> {
    SourceNamespaceQuery::Data(SourceQuery::Data(RoutedQuery {
        hint: source_fixture::hint(key),
        key: vec![key],
        query: vec![key],
    }))
}
fn target_hint(key: u8) -> RouteHint {
    let mut hint = source_fixture::hint(key);
    hint.group = group(if key < 128 { 21 } else { 22 });
    hint.scope = source_fixture::range(
        if key < 128 { 0 } else { 128 },
        if key < 128 { 128 } else { 256 },
    );
    hint.epoch = OwnershipEpoch::new(2).unwrap();
    hint.generation = RouteGeneration::new(2).unwrap();
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
// Exact byte oracle for this fixture's stopped metadata files, including its
// selected WAL/manifests and snapshots. No native worker owns them during use.
pub(super) fn durable_files(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(path: &Path, files: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(&path, files);
            } else {
                files.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, &mut files);
    files
}
// Accepted requests are intentionally left unconsumed. Only committed/applied
// application facts drive progress; the original client result is lost on reopen.
pub(super) fn unread<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    operation: u128,
    bytes: Vec<u8>,
    done: impl Fn(&A) -> bool,
) where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    campaign(nodes, clock, g);
    assert_eq!(nodes[0].local().clients.usage().requests, 0);
    let ticket = nodes[0]
        .propose(ClientRequest {
            group: group(g),
            operation: OperationId::new(operation).unwrap(),
            bytes,
        })
        .unwrap();
    drive(nodes, clock, |ns| {
        ns.iter().all(|n| done(&n.local().applications[&group(g)]))
    });
    assert_eq!(ticket.operation, OperationId::new(operation).unwrap());
    assert_eq!(nodes[0].local().clients.usage().requests, 1);
}
struct CreatedSplit {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    plan: NamespacePlan,
    parent: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    targets: [Vec<Node<target_fixture::Target>>; 2],
    creation_records: BTreeMap<NodeId, Vec<u8>>,
}
fn establish_source_replicas(
    parent: &[Node<LifecycleDirectory>],
    configs: &[NativeStartup],
    intent: &GroupCreationIntent,
    creation: &GroupCreationStatus,
) -> BTreeMap<NodeId, Vec<u8>> {
    let mut records = BTreeMap::new();
    for c in configs {
        let verified = VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: parent[0].local().owner.core(group(1)).unwrap(),
                directory: parent[0].local().applications[&group(1)].directory(),
            },
            creation.clone(),
            c.node,
            c.store,
            intent.application,
        )
        .unwrap();
        let mut log = NativeLogStore::create(
            FileLogIo::create(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
        establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        records.insert(c.node, bindings.load().unwrap().unwrap());
        NativeSnapshotStore::create(
            FileSnapshotIo::create(c.directory.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: c.store,
                group: group(20),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
    records
}
impl CreatedSplit {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-created-source-{}-{protocol:?}-{checkpoint}",
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
            metadata()
                .directory()
                .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
                .unwrap(),
        );
        propose_recovering(
            &mut parent,
            &clock,
            1,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: anchor(),
            }
            .encode(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
        );
        let configs = configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover);
        let intent = GroupCreationIntent {
            authority: group(1),
            parent: responsibility(1),
            expected: RouteGeneration::new(1).unwrap(),
            responsibility: source_fixture::grant().input().responsibility,
            bootstrap: configs[0].bootstrap.clone(),
            application: source_fixture::grant().input().application,
            mode: GroupCreationMode::Empty,
        };
        assert_eq!(
            propose_recovering(
                &mut parent,
                &clock,
                1,
                1002,
                intent.encode(MAX_GROUP_CREATION_BYTES).unwrap()
            )
            .outcome,
            DirectoryOutcome::CreationReserved
        );
        let creation = parent[0].local().applications[&group(1)]
            .directory()
            .group_creation_at(0, group(20))
            .unwrap()
            .unwrap();
        let plan = NamespacePlan {
            creation: creation.clone(),
            manifest: source_fixture::grant(),
        };
        std::fs::create_dir_all(root.join("20")).unwrap();
        let records = establish_source_replicas(&parent, &configs, &intent, &creation);
        let source = open(configs, &clock, protocol, || source(&plan));
        let targets = std::array::from_fn(|i| {
            open(
                configuration(&root, 21 + i as u128, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                || target_fixture::fresh_for(21 + i as u128),
            )
        });
        Self {
            root,
            clock,
            protocol,
            checkpoint,
            plan,
            parent,
            source,
            targets,
            creation_records: records,
        }
    }
    fn observed(&mut self) -> Observed {
        let NamespaceOwnerRead::Status(namespace) = split::observe(
            &mut self.source,
            &self.clock,
            20,
            SourceNamespaceQuery::Status,
        ) else {
            panic!("namespace status")
        };
        let DirectoryRead::Manifest(manifest) = split::observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Manifest(self.plan.manifest.input().responsibility),
        ) else {
            panic!("manifest")
        };
        let core = self.parent[0].local().owner.core(group(1)).unwrap();
        let d = self.parent[0].local().applications[&group(1)].directory();
        let namespace_publication = d
            .namespace_publication_at(core.state().commit_index, self.plan.creation.operation)
            .unwrap();
        if let Some(p) = &namespace_publication {
            assert!(p.index <= core.state().commit_index);
            assert!(manifest.is_some());
        }
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
        let source = match split::observe(
            &mut self.source,
            &self.clock,
            20,
            SourceNamespaceQuery::Data(SourceQuery::Freeze),
        ) {
            NamespaceOwnerRead::NotActive => None,
            NamespaceOwnerRead::Data(SourceRead::Freeze(s)) => s,
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
            namespace,
            namespace_publication,
            intent,
            publication,
            source,
            targets,
        }
    }
    fn resume_one(&mut self) -> Option<Retry> {
        let state = self.observed();
        if let Some(retry) = self.resume_namespace(&state) {
            return Some(retry);
        }
        if let Some(retry) = self.resume_source_freeze(&state) {
            return Some(retry);
        }
        if let Some(retry) = self.resume_target_import(&state) {
            return Some(retry);
        }
        if state.publication.is_none() {
            return self.resume_publication(state);
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
    fn resume_namespace(&mut self, state: &Observed) -> Option<Retry> {
        if state.namespace.ready_index.is_none() {
            let b = source(&self.plan)
                .initialization_command(MAX_ROUTED_COMMAND_BYTES)
                .unwrap();
            unread(&mut self.source, &self.clock, 20, 1002, b.clone(), |a| {
                a.status().ready_index.is_some()
            });
            return Some(Retry {
                phase: Phase::Ready,
                group: 20,
                operation: 1002,
                bytes: b,
            });
        }
        if state.namespace_publication.is_none() {
            let publication =
                NamespacePublication::from_status(&self.plan, state.namespace).unwrap();
            let b = publication.encode(MAX_NAMESPACE_PUBLICATION_BYTES).unwrap();
            unread(&mut self.parent, &self.clock, 1, 1003, b.clone(), |a| {
                a.directory()
                    .manifest(source_fixture::grant().input().responsibility)
                    .is_some()
            });
            return Some(Retry {
                phase: Phase::NamespacePublish,
                group: 1,
                operation: 1003,
                bytes: b,
            });
        }
        if state.namespace.activation_index.is_none() {
            let p = state.namespace_publication.as_ref().unwrap();
            assert_eq!(p.publication.manifest, self.plan.manifest);
            let b = self.source[0].local().applications[&group(20)]
                .activation_command(p, MAX_ROUTED_COMMAND_BYTES)
                .unwrap();
            unread(&mut self.source, &self.clock, 20, 1003, b.clone(), |a| {
                a.status().activation_index.is_some()
            });
            return Some(Retry {
                phase: Phase::NamespaceActivate,
                group: 20,
                operation: 1003,
                bytes: b,
            });
        }
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
        None
    }
    fn resume_source_freeze(&mut self, state: &Observed) -> Option<Retry> {
        if state.intent.is_none() {
            let b = source_fixture::intent()
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
        assert_eq!(
            state.intent.as_ref().unwrap().intent,
            source_fixture::intent()
        );
        for i in 0..2 {
            if state.targets[i].staged_index.is_none() {
                let g = 21 + i as u128;
                let b = target_fixture::fresh_for(g)
                    .bootstrap_command(100000)
                    .unwrap();
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
            let b = source_fixture::freeze();
            unread(&mut self.source, &self.clock, 20, 200, b.clone(), |a| {
                a.owner().fence().is_some()
            });
            return Some(Retry {
                phase: Phase::Fence,
                group: 20,
                operation: 200,
                bytes: b,
            });
        }
        None
    }
    fn resume_target_import(&mut self, state: &Observed) -> Option<Retry> {
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
                    source_fixture::intent(),
                    group(g),
                    vec![SourceImport {
                        fence: status.fence,
                        configuration,
                        image: self.source[0].local().applications[&group(20)]
                            .owner()
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
        None
    }
    fn resume_publication(&mut self, state: Observed) -> Option<Retry> {
        let configuration = self.source[0]
            .local()
            .owner
            .core(group(20))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
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
            source_fixture::intent(),
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
        Some(Retry {
            phase: Phase::Publish,
            group: 1,
            operation: 201,
            bytes: b,
        })
    }
    fn serving(&mut self, state: &Observed) {
        let expected = if state.namespace.activation_index.is_none() {
            NamespaceOwnerRead::NotActive
        } else if state.source.is_some() {
            NamespaceOwnerRead::Data(SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced)))
        } else {
            NamespaceOwnerRead::Data(SourceRead::Data(RoutedRead::Served(
                self.source[0].local().applications[&group(20)]
                    .routed()
                    .application()
                    .value(&[1])
                    .unwrap(),
            )))
        };
        assert_eq!(
            split::observe(&mut self.source, &self.clock, 20, source_query(1)),
            expected
        );
        if state.source.is_some() || state.namespace.activation_index.is_none() {
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
            || source(&self.plan),
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
                || target_fixture::fresh_for(21 + i as u128),
            )
        });
        for c in configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover) {
            let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
            assert_eq!(
                bindings.load().unwrap().unwrap(),
                self.creation_records[&c.node]
            );
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
                    Phase::Ready => assert_eq!(r.outcome, NamespaceOutcome::Ready),
                    Phase::NamespaceActivate => assert_eq!(r.outcome, NamespaceOutcome::Activated),
                    Phase::Data(_) => assert!(
                        matches!(r.outcome,NamespaceOutcome::Data(RoutedReceipt {outcome:RoutedOutcome::Applied(v),..}) if v.duplicate)
                    ),
                    Phase::Fence => assert!(
                        matches!(r.outcome,NamespaceOutcome::Data(RoutedReceipt {outcome:RoutedOutcome::Fenced(f),..}) if f==expected.source.as_ref().unwrap().fence)
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
impl CreatedSplit {
    fn activate_offline_target(
        &mut self,
        i: usize,
        metadata_configuration: ConfigurationId,
        publication: &TransferPublicationStatus,
        original: &mut Observed,
    ) {
        let g = 21 + i as u128;
        let activation = TargetActivation {
            metadata_configuration,
            decision: publication.clone(),
        };
        let bytes = self.targets[i][0].local().applications[&group(g)]
            .activation_command(&activation, 100000)
            .unwrap();
        unread(
            &mut self.targets[i],
            &self.clock,
            g,
            200,
            bytes.clone(),
            |a| a.status().activated.is_some(),
        );
        let TargetRead::Status(status) =
            split::observe(&mut self.targets[i], &self.clock, g, TargetQuery::Status)
        else {
            panic!("activation status")
        };
        original.targets[i] = status;
        if self.checkpoint {
            split::compact(&mut self.targets[i], &self.clock, g);
        }
        CreatedSplit::stop(
            std::mem::take(&mut self.targets[i]),
            &self.clock,
            g,
            self.checkpoint,
        );
        self.targets[i] = open(
            configuration(&self.root, g, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || target_fixture::fresh_for(g),
        );
        campaign(&mut self.targets[i], &self.clock, g);
        let r = propose_recovering(&mut self.targets[i], &self.clock, g, 200, bytes);
        assert!(
            matches!(r.outcome,TargetOutcome::Activated(ref s) if Some(s)==original.targets[i].activated.as_ref())
        );
    }
    fn exercise_offline_target(&mut self, i: usize) {
        let g = 21 + i as u128;
        let key = if i == 0 { 1 } else { 200 };
        let value = if i == 0 { 7 } else { 11 };
        assert_eq!(
            split::observe(
                &mut self.targets[i],
                &self.clock,
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
                    &mut self.targets[1],
                    &self.clock,
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
            &mut self.targets[i],
            &self.clock,
            g,
            old_op,
            target_data(key, value),
        );
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(value))
        );
        let r = propose_recovering(
            &mut self.targets[i],
            &self.clock,
            g,
            100 + old_op,
            target_data(key, 2),
        );
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if !v.duplicate&&v.outcome==BucketOutcome::Value(value+2))
        );
    }
    fn verify_completed(&mut self, original: &Observed) {
        for (i, key, value) in [(0, 1u8, 9), (1, 200u8, 13)] {
            let g = 21 + i as u128;
            assert_eq!(
                split::observe(
                    &mut self.targets[i],
                    &self.clock,
                    g,
                    TargetQuery::Data(RoutedQuery {
                        hint: target_hint(key),
                        key: vec![key],
                        query: vec![key]
                    })
                ),
                TargetRead::Data(value)
            );
            assert!(self.targets[i]
                .iter()
                .all(|n| n.local().applications[&group(g)]
                    .application()
                    .outbox()
                    .count()
                    == 2));
            assert!(matches!(
                split::observe(
                    &mut self.targets[i],
                    &self.clock,
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
            split::observe(&mut self.source, &self.clock, 20, source_query(1)),
            NamespaceOwnerRead::Data(SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced)))
        );
        assert_eq!(
            self.source[0].local().applications[&group(20)]
                .owner()
                .export_target(group(21), 65536)
                .unwrap()
                .source_applied(),
            original.source.as_ref().unwrap().fence.index
        );
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = CreatedSplit::new(protocol, checkpoint);
    let phases = [
        Phase::Ready,
        Phase::NamespacePublish,
        Phase::NamespaceActivate,
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
        eprintln!("created source {protocol:?} checkpoint={checkpoint} phase={phase:?}");
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
    let parent_logs =
        CreatedSplit::stop(std::mem::take(&mut rig.parent), &rig.clock, 1, checkpoint);
    CreatedSplit::stop(std::mem::take(&mut rig.source), &rig.clock, 20, checkpoint);
    let parent_files = durable_files(&rig.root.join("1"));
    // Publication is already quorum-observed. Both original activations now run
    // with source and metadata fully stopped, using the retained exact decision.
    for i in 0..2 {
        rig.activate_offline_target(i, metadata_configuration, &publication, &mut original);
        rig.exercise_offline_target(i);
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
    rig.verify_completed(&original);
    rig.stop_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_created_namespace_split_recovers_unread_phases_from_wal() {
    history(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_created_namespace_split_recovers_unread_phases_from_checkpoint() {
    history(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_created_namespace_split_recovers_unread_phases_from_wal() {
    history(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_created_namespace_split_recovers_unread_phases_from_checkpoint() {
    history(NativePeerProtocol::Quic, true)
}
