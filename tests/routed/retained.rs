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
fn before(foreign: bool) -> ResponsibilityManifest {
    if foreign {
        fixture::before()
    } else {
        source_fixture::grant()
    }
}
fn retained_parent_profile(moves: bool) -> LifecycleDirectory {
    let base = Directory::new(
        DirectoryPlan::new(group(100), vec![fixture::parent()]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_retained_insertion()
    .unwrap_or_else(|_| panic!("parent schema8"));
    LifecycleDirectory::new(if moves {
        base.with_cross_authority_reparenting()
            .unwrap_or_else(|_| panic!("parent schema13"))
    } else {
        base
    })
}
pub(super) fn metadata_profile(foreign: bool, moves: bool) -> LifecycleDirectory {
    let base = Directory::new(
        DirectoryPlan::new(group(1), vec![before(foreign)]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_retained_insertion()
    .unwrap_or_else(|_| panic!("schema8"));
    LifecycleDirectory::new(if moves {
        base.with_cross_authority_reparenting()
            .unwrap_or_else(|_| panic!("schema13"))
    } else {
        base
    })
}
pub(super) fn source(foreign: bool) -> Source {
    source_profile(foreign, false)
}
fn source_profile(foreign: bool, moves: bool) -> Source {
    let r = RoutedApplication::new(
        group(20),
        before(foreign),
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
    let base = Source::new(r, 65536)
        .unwrap_or_else(|e| panic!("{:?}", e.0))
        .with_retained_insertion()
        .unwrap_or_else(|e| panic!("{:?}", e.0))
        .with_retained_grants()
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    if moves {
        base.with_parent_slot_adoption(4)
            .unwrap_or_else(|_| panic!("scoped parent profile"))
    } else {
        base
    }
}
pub(super) fn target(intent: &TransferIntent) -> target_fixture::Target {
    target_profile(intent, false)
}
fn target_profile(intent: &TransferIntent, moves: bool) -> target_fixture::Target {
    let base = TransferTarget::new(
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
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    if moves {
        base.with_parent_adoption(4)
            .unwrap_or_else(|_| panic!("imported parent profile"))
    } else {
        base
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct ParentFacts {
    manifest: ResponsibilityManifest,
    reservation: Option<DelegationReservationStatus>,
    publication: Option<DelegationPublicationStatus>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Facts {
    parent: Option<ParentFacts>,
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
    foreign: bool,
    parent_moves: bool,
    parent: Vec<Node<LifecycleDirectory>>,
    metadata: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    target: Vec<Node<target_fixture::Target>>,
    intent: TransferIntent,
    creation: GroupCreationStatus,
    bindings: BTreeMap<NodeId, Vec<u8>>,
}
fn initialize_source(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    foreign: bool,
    parent_moves: bool,
) -> Vec<Node<Source>> {
    let mut source_nodes = open(
        configuration(root, 20, &[1, 2, 3], NativeOpenMode::Create),
        clock,
        protocol,
        || source_profile(foreign, parent_moves),
    );
    campaign(&mut source_nodes, clock, 20);
    propose_recovering(
        &mut source_nodes,
        clock,
        20,
        100,
        source_profile(foreign, parent_moves)
            .bootstrap_command(100000)
            .unwrap(),
    );
    propose_recovering(&mut source_nodes, clock, 20, 1, source_fixture::data(1, 7));
    propose_recovering(
        &mut source_nodes,
        clock,
        20,
        2,
        source_fixture::data(200, 11),
    );
    source_nodes
}
fn reserve_target(
    metadata_nodes: &mut [Node<LifecycleDirectory>],
    configs: &[NativeStartup],
    clock: &Instant,
    foreign: bool,
) -> (GroupCreationIntent, GroupCreationStatus) {
    let request = GroupCreationIntent {
        authority: group(1),
        parent: before(foreign).input().responsibility,
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: fixture::id(21),
        bootstrap: configs[0].bootstrap.clone(),
        application: before(foreign).input().application,
        mode: GroupCreationMode::Staging,
    };
    campaign(metadata_nodes, clock, 1);
    assert_eq!(
        propose_recovering(
            metadata_nodes,
            clock,
            1,
            21,
            request.encode(100000).unwrap()
        )
        .outcome,
        DirectoryOutcome::CreationReserved
    );
    let _ = observe(
        metadata_nodes,
        clock,
        1,
        DirectoryQuery::Manifest(before(foreign).input().responsibility),
    );
    let core = metadata_nodes[0].local().owner.core(group(1)).unwrap();
    let creation = metadata_nodes[0].local().applications[&group(1)]
        .directory()
        .group_creation_at(core.state().commit_index, group(21))
        .unwrap()
        .unwrap();
    (request, creation)
}
pub(super) fn retained_shape(
    foreign: bool,
    creation: &GroupCreationStatus,
) -> (ResponsibilityManifest, InsertionChild) {
    let mut child = before(foreign).into_input();
    child.responsibility = fixture::id(21);
    child.parent = Some(ParentAuthority {
        responsibility: before(foreign).input().responsibility,
        group: group(1),
    });
    child.scope = source_fixture::range(0, 128);
    child.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(child).unwrap();
    let mut after = before(foreign).into_input();
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
    let after = ResponsibilityManifest::new(after).unwrap();
    let child = InsertionChild::from_creation(child, creation).unwrap();
    (after, child)
}
fn initialize_parent(
    root: &std::path::Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    parent_moves: bool,
    after: ResponsibilityManifest,
    child: InsertionChild,
) -> (Vec<Node<LifecycleDirectory>>, TransferIntent) {
    let mut parent_nodes = open(
        configuration(root, 100, &[1, 2, 3], NativeOpenMode::Create),
        clock,
        protocol,
        || retained_parent_profile(parent_moves),
    );
    initialize(&mut parent_nodes, clock, 100, fixture::parent());
    let plan = DelegationPlan::retained_insertion(
        fixture::parent(),
        before(true),
        after,
        child,
        source_fixture::op(200),
    )
    .unwrap();
    let bytes = plan.encode(100000).unwrap();
    campaign(&mut parent_nodes, clock, 100);
    phase_write(true, &mut parent_nodes, clock, 100, 400, bytes.clone());
    let DirectoryRead::DelegationReservation(Some(original)) = observe(
        &mut parent_nodes,
        clock,
        100,
        DirectoryQuery::DelegationReservation(source_fixture::op(400)),
    ) else {
        panic!("reservation")
    };
    if checkpoint {
        compact(&mut parent_nodes, clock, 100);
    }
    creation::abandon(parent_nodes, 100);
    parent_nodes = open(
        configuration(root, 100, &[1, 2, 3], NativeOpenMode::Recover),
        clock,
        protocol,
        || retained_parent_profile(parent_moves),
    );
    campaign(&mut parent_nodes, clock, 100);
    assert!(propose_recovering(&mut parent_nodes, clock, 100, 400, bytes).duplicate);
    assert_eq!(
        observe(
            &mut parent_nodes,
            clock,
            100,
            DirectoryQuery::DelegationReservation(source_fixture::op(400))
        ),
        DirectoryRead::DelegationReservation(Some(original.clone()))
    );
    let cfg = parent_nodes[0]
        .local()
        .owner
        .core(group(100))
        .unwrap()
        .membership()
        .id();
    let intent = original.child_intent(cfg).unwrap();
    (parent_nodes, intent)
}
impl Retained {
    fn new(protocol: NativePeerProtocol, checkpoint: bool, foreign: bool) -> Self {
        Self::profile(protocol, checkpoint, foreign, false, "legacy")
    }
    fn profile(
        protocol: NativePeerProtocol,
        checkpoint: bool,
        foreign: bool,
        parent_moves: bool,
        case: &str,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-retained-native-{}-{protocol:?}-{checkpoint}-{foreign}-{parent_moves}-{case}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut metadata_nodes = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            || metadata_profile(foreign, parent_moves),
        );
        initialize(&mut metadata_nodes, &clock, 1, before(foreign));
        let source_nodes = initialize_source(&root, &clock, protocol, foreign, parent_moves);
        let configs = configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Recover);
        let (request, creation) = reserve_target(&mut metadata_nodes, &configs, &clock, foreign);
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
            || metadata_profile(foreign, parent_moves),
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
            DirectoryQuery::Manifest(before(foreign).input().responsibility),
        );
        let bindings = configs
            .iter()
            .map(|c| (c.node, establish(&metadata_nodes, c, &creation)))
            .collect();
        let (after, child) = retained_shape(foreign, &creation);
        let mut parent_nodes = Vec::new();
        let intent = if foreign {
            let (nodes, intent) = initialize_parent(
                &root,
                &clock,
                protocol,
                checkpoint,
                parent_moves,
                after,
                child,
            );
            parent_nodes = nodes;
            intent
        } else {
            TransferIntent::insert_retained_child(before(foreign), after, child).unwrap()
        };
        let target_nodes = open(configs, &clock, protocol, || {
            target_profile(&intent, parent_moves)
        });
        Self {
            root,
            clock,
            protocol,
            checkpoint,
            foreign,
            parent_moves,
            parent: parent_nodes,
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
        let parent = if self.foreign {
            let DirectoryRead::Manifest(Some(manifest)) = observe(
                &mut self.parent,
                &self.clock,
                100,
                DirectoryQuery::Manifest(fixture::id(500)),
            ) else {
                panic!("parent")
            };
            let DirectoryRead::DelegationReservation(reservation) = observe(
                &mut self.parent,
                &self.clock,
                100,
                DirectoryQuery::DelegationReservation(source_fixture::op(400)),
            ) else {
                panic!("reservation")
            };
            let DirectoryRead::DelegationPublication(publication) = observe(
                &mut self.parent,
                &self.clock,
                100,
                DirectoryQuery::DelegationPublication(source_fixture::op(400)),
            ) else {
                panic!("parent publication")
            };
            Some(ParentFacts {
                manifest,
                reservation,
                publication,
            })
        } else {
            None
        };
        Facts {
            parent,
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
        let foreign = self.foreign;
        let moves = self.parent_moves;
        if self.checkpoint {
            if foreign {
                compact(&mut self.parent, &self.clock, 100);
            }
            compact(&mut self.metadata, &self.clock, 1);
            compact(&mut self.source, &self.clock, 20);
            compact(&mut self.target, &self.clock, 21);
        }
        if foreign {
            creation::abandon(std::mem::take(&mut self.parent), 100);
            self.parent = open(
                configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
                &self.clock,
                self.protocol,
                || retained_parent_profile(moves),
            );
        }
        creation::abandon(std::mem::take(&mut self.metadata), 1);
        creation::abandon(std::mem::take(&mut self.source), 20);
        creation::abandon(std::mem::take(&mut self.target), 21);
        self.metadata = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || metadata_profile(foreign, moves),
        );
        self.source = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || source_profile(foreign, moves),
        );
        let intent = &self.intent;
        self.target = open(
            configuration(&self.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || target_profile(intent, moves),
        );
    }
    fn phase(&mut self, g: u128, op: u128, bytes: Vec<u8>) {
        match g {
            100 => {
                campaign(&mut self.parent, &self.clock, g);
                phase_write(true, &mut self.parent, &self.clock, g, op, bytes.clone())
            }
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
            100 => {
                campaign(&mut self.parent, &self.clock, g);
                propose_recovering(&mut self.parent, &self.clock, g, op, bytes);
            }
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
fn prepare_scoped_import(
    rig: &mut Retained,
) -> (
    ScopedExportStatus,
    voteboat::scope::ScopeImage,
    ConfigurationId,
) {
    rig.phase(1, 200, rig.intent.encode(100000).unwrap());
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    let boot = target_profile(&rig.intent, rig.parent_moves)
        .bootstrap_command(100000)
        .unwrap();
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
    let bytes = target_profile(&rig.intent, rig.parent_moves)
        .import_command(&import, 100000)
        .unwrap();
    rig.phase(21, 200, bytes);
    assert_eq!(
        child_read(&mut rig.target, &rig.clock),
        TargetRead::NotActive
    );
    (frozen, image, source_configuration)
}
fn complete_parent(
    rig: &mut Retained,
    original_parent: ParentFacts,
    metadata_configuration: ConfigurationId,
    decision: &TransferPublicationStatus,
) {
    let reservation = original_parent.reservation.unwrap();
    let completion = DelegationCompletion {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: rig.parent[0]
            .local()
            .owner
            .core(group(100))
            .unwrap()
            .membership()
            .id(),
        child_configuration: metadata_configuration,
        decision: decision.clone(),
    };
    rig.phase(
        100,
        401,
        completion.encode(MAX_DELEGATION_COMPLETION_BYTES).unwrap(),
    );
    let parent = rig.facts().parent.unwrap();
    let mut expected = fixture::parent().into_input();
    expected.generation = RouteGeneration::new(2).unwrap();
    expected.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: expected.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: rig.intent.after().input().responsibility,
            group: group(1),
            epoch: rig.intent.after().input().epoch,
        }),
    }]);
    assert_eq!(
        parent.manifest,
        ResponsibilityManifest::new(expected).unwrap()
    );
    assert_eq!(parent.publication.unwrap().completion, completion);
    assert_eq!(parent.reservation, Some(reservation));
}
fn write_while_metadata_stopped(rig: &mut Retained, original: &Facts, bytes: &[u8]) {
    assert_eq!(child_read(&mut rig.target, &rig.clock), TargetRead::Data(7));
    campaign(&mut rig.target, &rig.clock, 21);
    let r = propose_recovering(&mut rig.target, &rig.clock, 21, 200, bytes.to_vec());
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
}
fn activate(rig: &mut Retained) -> (Facts, Vec<u8>, voteboat::scope::ScopeImage) {
    let foreign = rig.foreign;
    let (frozen, image, source_configuration) = prepare_scoped_import(rig);
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
    if foreign {
        complete_parent(
            rig,
            facts.parent.unwrap(),
            metadata_configuration,
            &decision,
        );
    }
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
    (original, bytes, image)
}
fn run(protocol: NativePeerProtocol, checkpoint: bool, foreign: bool) {
    let mut rig = Retained::new(protocol, checkpoint, foreign);
    let (original, bytes, image) = activate(&mut rig);
    let metadata_logs = creation::abandon(std::mem::take(&mut rig.metadata), 1);
    let metadata_files = durable_files(&rig.root.join("1"));
    let parent_stopped = if foreign {
        let logs = creation::abandon(std::mem::take(&mut rig.parent), 100);
        Some((logs, durable_files(&rig.root.join("100"))))
    } else {
        None
    };
    write_while_metadata_stopped(&mut rig, &original, &bytes);
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
        || source(foreign),
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
    if let Some((logs, files)) = parent_stopped {
        assert_eq!(durable_files(&rig.root.join("100")), files);
        for c in configuration(&rig.root, 100, &[1, 2, 3], NativeOpenMode::Recover) {
            let log = NativeLogStore::recover(
                FileLogIo::open(&c.directory).unwrap(),
                c.store,
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(log.state(group(100)).unwrap(), logs[&c.node]);
        }
    }
    creation::abandon(std::mem::take(&mut rig.source), 20);
    creation::abandon(std::mem::take(&mut rig.target), 21);
    std::fs::remove_dir_all(&rig.root).unwrap();
}
#[test]
fn tcp_root_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::TcpTls, false, false);
}
#[test]
fn tcp_root_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::TcpTls, true, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_root_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::Quic, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_root_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::Quic, true, false);
}

#[test]
fn tcp_foreign_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::TcpTls, false, true);
}
#[test]
fn tcp_foreign_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::TcpTls, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_foreign_retained_scopes_recover_unread_phases_from_wal() {
    run(NativePeerProtocol::Quic, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_foreign_retained_scopes_recover_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::Quic, true, true);
}

#[path = "retained_parent.rs"]
mod parent_moves;
