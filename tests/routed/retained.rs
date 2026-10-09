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
use super::super::{creation, creation_source::durable_files, insertion::establish};
use super::*;
use voteboat::{
    bucket_counter::BucketCounter, group_creation::CreationBindings,
    native::group_creation::FileCreationBindings, scoped_source::*,
};
type Source = ScopedTransferSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;
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
        .with_retained_insertion()
        .unwrap_or_else(|_| panic!("schema8")),
    )
}
fn source() -> Source {
    let r = RoutedApplication::new(
        group(20),
        source_fixture::grant(),
        BucketCounter::new(
            source_fixture::range(0, 256),
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: source_fixture::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error))
    .with_scoped_fencing(2)
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    Source::new(r, 65536)
        .unwrap_or_else(|e| panic!("{:?}", e.0))
        .with_retained_insertion()
        .unwrap_or_else(|e| panic!("{:?}", e.0))
        .with_retained_grants()
        .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn target(intent: &TransferIntent) -> target_fixture::Target {
    TransferTarget::new(
        group(21),
        source_fixture::op(200),
        intent.clone(),
        BucketCounter::new(
            source_fixture::range(0, 128),
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Facts {
    manifest: ResponsibilityManifest,
    child: Option<ResponsibilityManifest>,
    intent: Option<TransferIntentStatus>,
    decision: Option<TransferPublicationStatus>,
    frozen: Option<ScopedExportStatus>,
    grant: ResponsibilityManifest,
    adoption: Option<RetainedGrantStatus>,
    target: TargetStatus,
}
struct Retained {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    metadata: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    target: Vec<Node<target_fixture::Target>>,
    intent: TransferIntent,
    creation: GroupCreationStatus,
    bindings: BTreeMap<NodeId, Vec<u8>>,
}
impl Retained {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-retained-native-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut metadata_nodes = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            metadata,
        );
        initialize(&mut metadata_nodes, &clock, 1, source_fixture::grant());
        let mut source_nodes = open(
            configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            source,
        );
        campaign(&mut source_nodes, &clock, 20);
        propose_recovering(
            &mut source_nodes,
            &clock,
            20,
            100,
            source().bootstrap_command(100000).unwrap(),
        );
        propose_recovering(&mut source_nodes, &clock, 20, 1, source_fixture::data(1, 7));
        propose_recovering(
            &mut source_nodes,
            &clock,
            20,
            2,
            source_fixture::data(200, 11),
        );
        let configs = configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Recover);
        let request = GroupCreationIntent {
            authority: group(1),
            parent: source_fixture::grant().input().responsibility,
            expected: RouteGeneration::new(1).unwrap(),
            responsibility: fixture::id(21),
            bootstrap: configs[0].bootstrap.clone(),
            application: source_fixture::grant().input().application,
            mode: GroupCreationMode::Staging,
        };
        campaign(&mut metadata_nodes, &clock, 1);
        assert_eq!(
            propose_recovering(
                &mut metadata_nodes,
                &clock,
                1,
                21,
                request.encode(100000).unwrap()
            )
            .outcome,
            DirectoryOutcome::CreationReserved
        );
        let _ = observe(
            &mut metadata_nodes,
            &clock,
            1,
            DirectoryQuery::Manifest(source_fixture::grant().input().responsibility),
        );
        let core = metadata_nodes[0].local().owner.core(group(1)).unwrap();
        let creation = metadata_nodes[0].local().applications[&group(1)]
            .directory()
            .group_creation_at(core.state().commit_index, group(21))
            .unwrap()
            .unwrap();
        for c in &configs[..2] {
            establish(&metadata_nodes, c, &creation);
        }
        assert!(!configs[2].directory.exists());
        if checkpoint {
            compact(&mut metadata_nodes, &clock, 1);
        }
        creation::abandon(metadata_nodes, 1);
        metadata_nodes = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &clock,
            protocol,
            metadata,
        );
        campaign(&mut metadata_nodes, &clock, 1);
        assert!(
            propose_recovering(
                &mut metadata_nodes,
                &clock,
                1,
                21,
                request.encode(100000).unwrap()
            )
            .duplicate
        );
        let _ = observe(
            &mut metadata_nodes,
            &clock,
            1,
            DirectoryQuery::Manifest(source_fixture::grant().input().responsibility),
        );
        let bindings = configs
            .iter()
            .map(|c| (c.node, establish(&metadata_nodes, c, &creation)))
            .collect();
        let mut child = source_fixture::grant().into_input();
        child.responsibility = fixture::id(21);
        child.parent = Some(ParentAuthority {
            responsibility: source_fixture::grant().input().responsibility,
            group: group(1),
        });
        child.scope = source_fixture::range(0, 128);
        child.execution = ExecutionMode::Single(group(21));
        let child = ResponsibilityManifest::new(child).unwrap();
        let mut after = source_fixture::grant().into_input();
        after.epoch = OwnershipEpoch::new(2).unwrap();
        after.generation = RouteGeneration::new(2).unwrap();
        after.execution = ExecutionMode::Delegated(vec![
            RouteEntry {
                scope: child.input().scope,
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: child.input().responsibility,
                    group: group(1),
                    epoch: child.input().epoch,
                }),
            },
            RouteEntry {
                scope: source_fixture::range(128, 256),
                target: RouteTarget::Group(group(20)),
            },
        ]);
        let intent = TransferIntent::insert_retained_child(
            source_fixture::grant(),
            ResponsibilityManifest::new(after).unwrap(),
            InsertionChild::from_creation(child, &creation).unwrap(),
        )
        .unwrap();
        let target_nodes = open(configs, &clock, protocol, || target(&intent));
        Self {
            root,
            clock,
            protocol,
            checkpoint,
            metadata: metadata_nodes,
            source: source_nodes,
            target: target_nodes,
            intent,
            creation,
            bindings,
        }
    }
    fn facts(&mut self) -> Facts {
        let DirectoryRead::Manifest(Some(manifest)) = observe(
            &mut self.metadata,
            &self.clock,
            1,
            DirectoryQuery::Manifest(self.intent.before().input().responsibility),
        ) else {
            panic!("manifest")
        };
        let DirectoryRead::Manifest(child) = observe(
            &mut self.metadata,
            &self.clock,
            1,
            DirectoryQuery::Manifest(fixture::id(21)),
        ) else {
            panic!("child")
        };
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.metadata,
            &self.clock,
            1,
            DirectoryQuery::Transfer(source_fixture::op(200)),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(decision) = observe(
            &mut self.metadata,
            &self.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(200)),
        ) else {
            panic!("decision")
        };
        let ScopedSourceRead::Frozen(frozen) = observe(
            &mut self.source,
            &self.clock,
            20,
            ScopedSourceQuery::Frozen(source_fixture::op(200)),
        ) else {
            panic!("frozen")
        };
        let ScopedSourceRead::Grant(adoption) = observe(
            &mut self.source,
            &self.clock,
            20,
            ScopedSourceQuery::Grant(source_fixture::op(300)),
        ) else {
            panic!("adoption")
        };
        let grant = self.source[0].local().applications[&group(20)]
            .grant()
            .clone();
        let TargetRead::Status(target) =
            observe(&mut self.target, &self.clock, 21, TargetQuery::Status)
        else {
            panic!("target")
        };
        Facts {
            manifest,
            child,
            intent,
            decision,
            frozen,
            grant,
            adoption,
            target,
        }
    }
    fn reopen(&mut self) {
        if self.checkpoint {
            compact(&mut self.metadata, &self.clock, 1);
            compact(&mut self.source, &self.clock, 20);
            compact(&mut self.target, &self.clock, 21);
        }
        creation::abandon(std::mem::take(&mut self.metadata), 1);
        creation::abandon(std::mem::take(&mut self.source), 20);
        creation::abandon(std::mem::take(&mut self.target), 21);
        self.metadata = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            metadata,
        );
        self.source = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            source,
        );
        let intent = &self.intent;
        self.target = open(
            configuration(&self.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || target(intent),
        );
    }
    fn phase(&mut self, g: u128, op: u128, bytes: Vec<u8>) {
        match g {
            1 => {
                campaign(&mut self.metadata, &self.clock, g);
                phase_write(true, &mut self.metadata, &self.clock, g, op, bytes.clone())
            }
            20 => {
                campaign(&mut self.source, &self.clock, g);
                phase_write(true, &mut self.source, &self.clock, g, op, bytes.clone())
            }
            21 => {
                campaign(&mut self.target, &self.clock, g);
                phase_write(true, &mut self.target, &self.clock, g, op, bytes.clone())
            }
            _ => unreachable!(),
        }
        let original = self.facts();
        self.reopen();
        assert_eq!(self.facts(), original);
        match g {
            1 => {
                campaign(&mut self.metadata, &self.clock, g);
                propose_recovering(&mut self.metadata, &self.clock, g, op, bytes);
            }
            20 => {
                campaign(&mut self.source, &self.clock, g);
                propose_recovering(&mut self.source, &self.clock, g, op, bytes);
            }
            21 => {
                campaign(&mut self.target, &self.clock, g);
                propose_recovering(&mut self.target, &self.clock, g, op, bytes);
            }
            _ => unreachable!(),
        }
        assert_eq!(self.facts(), original);
        let _ = observe(
            &mut self.metadata,
            &self.clock,
            1,
            DirectoryQuery::Manifest(self.intent.before().input().responsibility),
        );
        let core = self.metadata[0].local().owner.core(group(1)).unwrap();
        assert_eq!(
            self.metadata[0].local().applications[&group(1)]
                .directory()
                .group_creation_at(core.state().commit_index, group(21))
                .unwrap()
                .unwrap(),
            self.creation
        );
        for (node, bytes) in &self.bindings {
            let mut binding =
                FileCreationBindings::open(self.root.join(format!("21/{}", node.get()))).unwrap();
            assert_eq!(binding.load().unwrap().as_ref(), Some(bytes));
        }
    }
}
fn retained_hint() -> RouteHint {
    let mut h = source_fixture::hint(200);
    h.scope = source_fixture::range(128, 256);
    h.epoch = OwnershipEpoch::new(2).unwrap();
    h.generation = RouteGeneration::new(2).unwrap();
    h
}
fn child_hint() -> RouteHint {
    let mut h = source_fixture::hint(1);
    h.group = group(21);
    h.responsibility = fixture::id(21);
    h.scope = source_fixture::range(0, 128);
    h
}
fn data(hint: RouteHint, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn child_read(nodes: &mut [Node<target_fixture::Target>], clock: &Instant) -> TargetRead<i64> {
    observe(
        nodes,
        clock,
        21,
        TargetQuery::Data(RoutedQuery {
            hint: child_hint(),
            key: vec![1],
            query: vec![1],
        }),
    )
}
fn source_read(
    nodes: &mut [Node<Source>],
    clock: &Instant,
    hint: RouteHint,
    key: u8,
) -> ScopedSourceRead<i64> {
    observe(
        nodes,
        clock,
        20,
        ScopedSourceQuery::Data(RoutedQuery {
            hint,
            key: vec![key],
            query: vec![key],
        }),
    )
}
fn run(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut rig = Retained::new(protocol, checkpoint);
    rig.phase(1, 200, rig.intent.encode(100000).unwrap());
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    let boot = target(&rig.intent).bootstrap_command(100000).unwrap();
    rig.phase(21, 200, boot);
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    rig.phase(20, 200, rig.intent.encode(100000).unwrap());
    let facts = rig.facts();
    let frozen = facts.frozen.unwrap();
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, source_fixture::hint(1), 1),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, source_fixture::hint(200), 200),
        ScopedSourceRead::Data(RoutedRead::Served(11))
    );
    let image = rig.source[0].local().applications[&group(20)]
        .export(source_fixture::op(200), 65536)
        .unwrap();
    assert_eq!(image.source_applied(), frozen.fence.fence.index);
    let source_configuration = rig.source[0]
        .local()
        .owner
        .core(group(20))
        .unwrap()
        .membership()
        .id();
    let import = TargetImport::new(
        source_fixture::op(200),
        rig.intent.clone(),
        group(21),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: source_configuration,
            image: image.clone(),
            digest: frozen.digest,
        }],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let bytes = target(&rig.intent).import_command(&import, 100000).unwrap();
    rig.phase(21, 200, bytes);
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    let facts = rig.facts();
    let target_configuration = rig.target[0]
        .local()
        .owner
        .core(group(21))
        .unwrap()
        .membership()
        .id();
    let publication = TransferPublication::new(
        source_fixture::op(200),
        rig.intent.clone(),
        vec![
            SourceFenceEvidence::from_scoped_status(source_configuration, frozen, &rig.intent)
                .unwrap(),
        ],
        vec![TargetReadyEvidence::from_status(target_configuration, facts.target).unwrap()],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    rig.phase(1, 201, publication.encode(100000).unwrap());
    let facts = rig.facts();
    assert_eq!(facts.manifest, *rig.intent.after());
    assert!(facts.child.is_some());
    let metadata_configuration = rig.metadata[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .membership()
        .id();
    let decision = facts.decision.unwrap();
    let adoption = RetainedGrantAdoption {
        metadata_configuration,
        decision: decision.clone(),
    };
    rig.phase(20, 300, adoption.encode(100000).unwrap());
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, retained_hint(), 200),
        ScopedSourceRead::Data(RoutedRead::Served(11))
    );
    let mut moved = source_fixture::hint(1);
    moved.epoch = OwnershipEpoch::new(2).unwrap();
    moved.generation = RouteGeneration::new(2).unwrap();
    moved.scope = source_fixture::range(0, 128);
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, moved, 1),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::WrongOwner))
    );
    assert!(rig.source[0]
        .propose(ClientRequest {
            group: group(20),
            operation: source_fixture::op(99),
            bytes: data(moved, 1, 100)
        })
        .is_err());
    assert_eq!(rig.source[0].local().clients.usage().requests, 0);
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, source_fixture::hint(200), 200),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::EpochMismatch))
    );
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    let activation = TargetActivation {
        metadata_configuration,
        decision,
    };
    let bytes = rig.target[0].local().applications[&group(21)]
        .activation_command(&activation, 100000)
        .unwrap();
    rig.phase(21, 200, bytes.clone());
    let original = rig.facts();
    assert_eq!(original.grant, *rig.intent.after());
    assert!(original.adoption.is_some());
    assert!(original.target.activated.is_some());
    let metadata_logs = creation::abandon(std::mem::take(&mut rig.metadata), 1);
    let metadata_files = durable_files(&rig.root.join("1"));
    assert_eq!(child_read(&mut rig.target, &rig.clock), TargetRead::Data(7));
    campaign(&mut rig.target, &rig.clock, 21);
    let r = propose_recovering(&mut rig.target, &rig.clock, 21, 200, bytes.clone());
    assert!(
        matches!(r.outcome,TargetOutcome::Activated(ref s) if Some(s)==original.target.activated.as_ref())
    );
    let r = propose_recovering(&mut rig.target, &rig.clock, 21, 1, data(child_hint(), 1, 7));
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(r) if r.duplicate && r.outcome==BucketOutcome::Value(7))
    );
    let r = propose_recovering(
        &mut rig.target,
        &rig.clock,
        21,
        30,
        data(child_hint(), 1, 2),
    );
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(r) if !r.duplicate && r.outcome==BucketOutcome::Value(9))
    );
    campaign(&mut rig.source, &rig.clock, 20);
    let r = propose_recovering(
        &mut rig.source,
        &rig.clock,
        20,
        2,
        data(retained_hint(), 200, 11),
    );
    assert!(matches!(r.outcome,RoutedOutcome::Applied(r) if r.duplicate));
    let r = propose_recovering(
        &mut rig.source,
        &rig.clock,
        20,
        3,
        data(retained_hint(), 200, 3),
    );
    assert!(
        matches!(r.outcome,RoutedOutcome::Applied(r) if !r.duplicate && r.outcome==BucketOutcome::Value(14))
    );
    assert!(rig.target.iter().all(|n| n.local().applications[&group(21)]
        .application()
        .outbox()
        .count()
        == 2));
    assert_eq!(
        rig.source[0].local().applications[&group(20)]
            .export(source_fixture::op(200), 65536)
            .unwrap(),
        image
    );
    if checkpoint {
        compact(&mut rig.source, &rig.clock, 20);
        compact(&mut rig.target, &rig.clock, 21);
    }
    creation::abandon(std::mem::take(&mut rig.source), 20);
    creation::abandon(std::mem::take(&mut rig.target), 21);
    rig.source = open(
        configuration(&rig.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &rig.clock,
        protocol,
        source,
    );
    rig.target = open(
        configuration(&rig.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        &rig.clock,
        protocol,
        || target(&rig.intent),
    );
    assert_eq!(
        source_read(&mut rig.source, &rig.clock, retained_hint(), 200),
        ScopedSourceRead::Data(RoutedRead::Served(14))
    );
    assert_eq!(child_read(&mut rig.target, &rig.clock), TargetRead::Data(9));
    let ScopedSourceRead::Frozen(recovered_frozen) = observe(
        &mut rig.source,
        &rig.clock,
        20,
        ScopedSourceQuery::Frozen(source_fixture::op(200)),
    ) else {
        panic!("frozen")
    };
    assert_eq!(recovered_frozen, original.frozen);
    let ScopedSourceRead::Grant(recovered_adoption) = observe(
        &mut rig.source,
        &rig.clock,
        20,
        ScopedSourceQuery::Grant(source_fixture::op(300)),
    ) else {
        panic!("adoption")
    };
    assert_eq!(recovered_adoption, original.adoption);
    assert_eq!(
        rig.source[0].local().applications[&group(20)]
            .export(source_fixture::op(200), 65536)
            .unwrap(),
        image
    );
    assert_eq!(durable_files(&rig.root.join("1")), metadata_files);
    for c in configuration(&rig.root, 1, &[1, 2, 3], NativeOpenMode::Recover) {
        let log = NativeLogStore::recover(
            FileLogIo::open(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), metadata_logs[&c.node]);
    }
    creation::abandon(std::mem::take(&mut rig.source), 20);
    creation::abandon(std::mem::take(&mut rig.target), 21);
    std::fs::remove_dir_all(&rig.root).unwrap();
}
#[test]
fn tcp_root_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_root_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_root_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_root_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::Quic, true);
}
