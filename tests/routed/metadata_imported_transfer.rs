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

pub(super) struct Handoff {
    pub(super) targets: BTreeMap<u128, Vec<Node<Target>>>,
    pub(super) intent: TransferIntent,
    pub(super) operation: u128,
    pub(super) decision: TransferPublicationStatus,
    pub(super) configuration: ConfigurationId,
    scoped_image: Option<ScopeImage>,
}
fn directory_phase(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    id: u128,
    bytes: Vec<u8>,
    q: MetadataServingQuery,
) -> MetadataServingRead {
    phase(env, &mut moved.nodes, 9, id, bytes, q, || {
        moved.template.clone()
    })
}
fn manifest(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    id: ResponsibilityIdentity,
) -> ResponsibilityManifest {
    let MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))) = observe(
        &mut moved.nodes,
        env.clock,
        9,
        MetadataServingQuery::Directory(DirectoryQuery::Manifest(id)),
    ) else {
        panic!("current manifest")
    };
    m
}
fn reserve(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    plan: DelegationPlan,
    id: u128,
) -> (TransferIntent, DelegationReservationStatus) {
    let MetadataServingRead::Directory(DirectoryRead::DelegationReservation(Some(r))) =
        directory_phase(
            env,
            moved,
            id,
            plan.encode(1000000).unwrap(),
            MetadataServingQuery::Directory(DirectoryQuery::DelegationReservation(
                source_fixture::op(id),
            )),
        )
    else {
        panic!("delegation reservation")
    };
    let intent = r.child_intent(config(&moved.nodes, 9)).unwrap();
    let operation = intent.delegation().unwrap().child_operation;
    let MetadataServingRead::Directory(DirectoryRead::Transfer(Some(status))) = directory_phase(
        env,
        moved,
        operation.get(),
        intent.encode(1000000).unwrap(),
        MetadataServingQuery::Directory(DirectoryQuery::Transfer(operation)),
    ) else {
        panic!("delegated intent")
    };
    assert_eq!(status.intent, intent);
    (intent, r)
}
fn freeze(
    env: &Environment<'_>,
    rig: &mut Rig,
    intent: &TransferIntent,
    operation: u128,
    scoped: bool,
) -> (SourceFenceEvidence, BTreeMap<u128, ScopeImage>) {
    let cfg = config(&rig.owner, 21);
    let evidence = if scoped {
        let TargetRead::ScopedFreeze(Some(s)) = rig.owner_phase(
            env,
            operation,
            intent.encode(1000000).unwrap(),
            TargetQuery::ScopedFreeze(source_fixture::op(operation)),
        ) else {
            panic!("scoped freeze")
        };
        SourceFenceEvidence::from_scoped_status(cfg, s, intent).unwrap()
    } else {
        let bytes = rig.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap()
            .freeze_command(intent, 65536, 1000000)
            .unwrap();
        let RetirementRead::Freeze(Some(s)) = phase(
            env,
            &mut rig.owner,
            21,
            operation,
            bytes,
            RetirementQuery::Freeze,
            || guard(&rig.first, rig.partial),
        ) else {
            panic!("remaining freeze")
        };
        SourceFenceEvidence::from_status(cfg, s).unwrap()
    };
    assert!(matches!(
        observe(
            &mut rig.owner,
            env.clock,
            21,
            RetirementQuery::Owner(query(intent.before(), 21, if scoped { 1 } else { 100 }))
        ),
        RetirementRead::Owner(TargetRead::Rejected(_))
    ));
    let mut images = BTreeMap::new();
    for route in intent.targets() {
        let RouteTarget::Group(g) = route.target else {
            panic!("target group")
        };
        let owner = rig.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap();
        let image = if scoped {
            owner
                .export_scoped(source_fixture::op(operation), 65536)
                .unwrap()
        } else {
            owner.export_target(g, 65536).unwrap()
        };
        assert_eq!(image.source_applied(), evidence.fence.index);
        images.insert(g.id.get(), image);
    }
    (evidence, images)
}
fn imports(
    env: &Environment<'_>,
    intent: &TransferIntent,
    operation: u128,
    evidence: &SourceFenceEvidence,
    images: BTreeMap<u128, ScopeImage>,
) -> BTreeMap<u128, Vec<Node<Target>>> {
    let mut targets = BTreeMap::new();
    for (g, image) in images {
        let template = target(intent, g, operation);
        let query = query(template.grant(), g, image.scope().start() as u8);
        let mut nodes = open(
            configuration(env.root, g, &[1, 2, 3], NativeOpenMode::Create),
            env.clock,
            env.protocol,
            || template.clone(),
        );
        phase(
            env,
            &mut nodes,
            g,
            operation,
            template.bootstrap_command(1000000).unwrap(),
            TargetQuery::Status,
            || template.clone(),
        );
        let import = TargetImport::new(
            source_fixture::op(operation),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: evidence.fence,
                configuration: evidence.configuration,
                digest: ContentDigest::scope_image(&image),
                image,
            }],
        )
        .unwrap();
        phase(
            env,
            &mut nodes,
            g,
            operation,
            template.import_command(&import, 1000000).unwrap(),
            TargetQuery::Status,
            || template.clone(),
        );
        assert_eq!(
            observe(&mut nodes, env.clock, g, query,),
            TargetRead::NotActive
        );
        targets.insert(g, nodes);
    }
    targets
}
fn publish(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    intent: &TransferIntent,
    operation: u128,
    source: SourceFenceEvidence,
    targets: &mut BTreeMap<u128, Vec<Node<Target>>>,
) -> TransferPublicationStatus {
    let mut evidence = Vec::new();
    for (g, nodes) in targets {
        let TargetRead::Status(s) = observe(nodes, env.clock, *g, TargetQuery::Status) else {
            panic!("ready successor")
        };
        evidence.push(TargetReadyEvidence::from_status(config(nodes, *g), s).unwrap());
    }
    let p = TransferPublication::new(
        source_fixture::op(operation),
        intent.clone(),
        vec![source],
        evidence,
    )
    .unwrap();
    let MetadataServingRead::Directory(DirectoryRead::Publication(Some(decision))) =
        directory_phase(
            env,
            moved,
            operation + 100,
            p.encode(1000000).unwrap(),
            MetadataServingQuery::Directory(DirectoryQuery::Publication(source_fixture::op(
                operation,
            ))),
        )
    else {
        panic!("handoff publication")
    };
    decision
}
fn complete(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    reservation: &DelegationReservationStatus,
    decision: &TransferPublicationStatus,
) {
    let cfg = config(&moved.nodes, 9);
    let completion = DelegationCompletion {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decision: decision.clone(),
    };
    let result = directory_phase(
        env,
        moved,
        reservation.operation.get() + 1,
        completion.encode(1000000).unwrap(),
        MetadataServingQuery::Directory(DirectoryQuery::DelegationPublication(
            reservation.operation,
        )),
    );
    assert!(matches!(
        result,
        MetadataServingRead::Directory(DirectoryRead::DelegationPublication(Some(_)))
    ));
}
fn activate(env: &Environment<'_>, handoff: &mut Handoff) {
    for (g, nodes) in &mut handoff.targets {
        let bytes = nodes[0].local().applications[&group(*g)]
            .activation_command(
                &TargetActivation {
                    metadata_configuration: handoff.configuration,
                    decision: handoff.decision.clone(),
                },
                1000000,
            )
            .unwrap();
        let TargetRead::Status(status) = phase(
            env,
            nodes,
            *g,
            handoff.operation,
            bytes,
            TargetQuery::Status,
            || target(&handoff.intent, *g, handoff.operation),
        ) else {
            panic!("activated successor")
        };
        assert!(status.activated.is_some());
    }
}
fn execute(
    env: &Environment<'_>,
    rig: &mut Rig,
    moved: &mut MovedMetadata,
    plan: DelegationPlan,
    reservation: u128,
    operation: u128,
    scoped: bool,
) -> Handoff {
    let (intent, reservation) = reserve(env, moved, plan, reservation);
    let (evidence, images) = freeze(env, rig, &intent, operation, scoped);
    let scoped_image = scoped.then(|| images.values().next().unwrap().clone());
    let mut targets = imports(env, &intent, operation, &evidence, images);
    let decision = publish(env, moved, &intent, operation, evidence, &mut targets);
    complete(env, moved, &reservation, &decision);
    let configuration = config(&moved.nodes, 9);
    if scoped {
        let retained = RetainedGrantAdoption {
            metadata_configuration: configuration,
            decision: decision.clone(),
        };
        assert!(matches!(
            rig.owner_phase(
                env,
                operation + 200,
                retained.encode(1000000).unwrap(),
                TargetQuery::RetainedGrant(source_fixture::op(operation + 200))
            ),
            TargetRead::RetainedGrant(Some(_))
        ));
    }
    let mut handoff = Handoff {
        targets,
        intent,
        operation,
        decision,
        configuration,
        scoped_image,
    };
    activate(env, &mut handoff);
    handoff
}
fn child(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    before: &ResponsibilityManifest,
) -> InsertionChild {
    let mut m = before.clone().into_input();
    m.responsibility = fixture::id(30);
    m.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: group(9),
    });
    m.scope = source_fixture::range(0, 64);
    m.epoch = OwnershipEpoch::new(1).unwrap();
    m.generation = RouteGeneration::new(1).unwrap();
    m.execution = ExecutionMode::Single(group(30));
    let intent = GroupCreationIntent {
        authority: group(9),
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: m.responsibility,
        bootstrap: configuration(env.root, 30, &[1, 2, 3], NativeOpenMode::Create)[0]
            .bootstrap
            .clone(),
        application: m.application,
        mode: GroupCreationMode::Staging,
    };
    let MetadataServingRead::Creation(Some(status)) = directory_phase(
        env,
        moved,
        30,
        intent.encode(1000000).unwrap(),
        MetadataServingQuery::Creation(group(30)),
    ) else {
        panic!("nested child creation")
    };
    InsertionChild::from_creation(
        ResponsibilityManifest::new(m).unwrap(),
        &status.decode().unwrap(),
    )
    .unwrap()
}
pub(super) fn delegate(env: &Environment<'_>, rig: &mut Rig, moved: &mut MovedMetadata) -> Handoff {
    let before = rig.grant();
    let child = child(env, moved, &before);
    let parent = manifest(env, moved, source_fixture::grant().input().responsibility);
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: source_fixture::range(0, 64),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: fixture::id(30),
                group: group(9),
                epoch: OwnershipEpoch::new(1).unwrap(),
            }),
        },
        RouteEntry {
            scope: source_fixture::range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let plan = DelegationPlan::retained_insertion(
        parent,
        before,
        ResponsibilityManifest::new(after).unwrap(),
        child,
        source_fixture::op(2000),
    )
    .unwrap();
    let handoff = execute(env, rig, moved, plan, 6000, 2000, true);
    let current = rig.grant();
    assert_eq!(
        observe(
            &mut rig.owner,
            env.clock,
            21,
            RetirementQuery::Owner(query(&current, 21, 100))
        ),
        RetirementRead::Owner(TargetRead::Data(9))
    );
    handoff
}
pub(super) fn remaining(
    env: &Environment<'_>,
    rig: &mut Rig,
    moved: &mut MovedMetadata,
) -> Handoff {
    let before = rig.grant();
    let parent = manifest(env, moved, source_fixture::grant().input().responsibility);
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    let plan = if rig.partial {
        let ExecutionMode::Delegated(routes) = &mut after.execution else {
            panic!("partial grant")
        };
        for r in routes {
            if r.target == RouteTarget::Group(group(21)) {
                r.target = RouteTarget::Group(group(40));
            }
        }
        DelegationPlan::move_remaining(
            parent,
            before,
            ResponsibilityManifest::new(after).unwrap(),
            source_fixture::op(3000),
        )
    } else {
        after.execution = ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: source_fixture::range(0, 64),
                target: RouteTarget::Group(group(40)),
            },
            RouteEntry {
                scope: source_fixture::range(64, 128),
                target: RouteTarget::Group(group(41)),
            },
        ]);
        DelegationPlan::new(
            parent,
            before,
            ResponsibilityManifest::new(after).unwrap(),
            source_fixture::op(3000),
        )
    }
    .unwrap();
    execute(env, rig, moved, plan, 6002, 3000, false)
}
pub(super) fn verify_scoped(rig: &Rig, child: &Handoff) {
    let owner = rig.owner[0].local().applications[&group(21)]
        .owner()
        .unwrap();
    assert_eq!(
        owner
            .export_scoped(source_fixture::op(2000), 65536)
            .unwrap(),
        *child.scoped_image.as_ref().unwrap()
    );
}
