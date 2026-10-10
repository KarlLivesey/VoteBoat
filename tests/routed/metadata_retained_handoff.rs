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
//! A later retained handoff uses the moved directory and survives its outage.
use super::*;

fn metadata_phase(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    id: u128,
    bytes: Vec<u8>,
    query: MetadataServingQuery,
) -> MetadataServingRead {
    phase(env, &mut moved.nodes, 9, id, bytes, query, || {
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
fn successor(intent: &TransferIntent) -> target_fixture::Target {
    TransferTarget::new(
        group(22),
        source_fixture::op(202),
        intent.clone(),
        BucketCounter::new(
            source_fixture::range(128, 192),
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|_| panic!("successor profile"))
}
fn child_manifest(before: &ResponsibilityManifest) -> ResponsibilityManifest {
    let mut input = before.clone().into_input();
    input.responsibility = fixture::id(22);
    input.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: group(9),
    });
    input.scope = source_fixture::range(128, 192);
    input.epoch = OwnershipEpoch::new(1).unwrap();
    input.generation = RouteGeneration::new(1).unwrap();
    input.execution = ExecutionMode::Single(group(22));
    ResponsibilityManifest::new(input).unwrap()
}
fn reserve(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    before: ResponsibilityManifest,
) -> TransferIntent {
    let child = child_manifest(&before);
    let configs = configuration(env.root, 22, &[1, 2, 3], NativeOpenMode::Create);
    let request = GroupCreationIntent {
        authority: group(9),
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: fixture::id(22),
        bootstrap: configs[0].bootstrap.clone(),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    let MetadataServingRead::Creation(Some(created)) = metadata_phase(
        env,
        moved,
        22,
        request.encode(1000000).unwrap(),
        MetadataServingQuery::Creation(group(22)),
    ) else {
        panic!("created successor")
    };
    let creation = created.decode().unwrap();
    assert_eq!(creation.intent, request);
    assert_eq!(created.authority, group(9));
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    let ExecutionMode::Delegated(routes) = &mut after.execution else {
        panic!("retained root")
    };
    routes[1] = RouteEntry {
        scope: child.input().scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: fixture::id(22),
            group: group(9),
            epoch: child.input().epoch,
        }),
    };
    routes.push(RouteEntry {
        scope: source_fixture::range(192, 256),
        target: RouteTarget::Group(group(20)),
    });
    let intent = TransferIntent::insert_retained_child(
        before,
        ResponsibilityManifest::new(after).unwrap(),
        InsertionChild::from_creation(child, &creation).unwrap(),
    )
    .unwrap();
    let MetadataServingRead::Directory(DirectoryRead::Transfer(Some(status))) = metadata_phase(
        env,
        moved,
        202,
        intent.encode(1000000).unwrap(),
        MetadataServingQuery::Directory(DirectoryQuery::Transfer(source_fixture::op(202))),
    ) else {
        panic!("later intent")
    };
    assert_eq!(status.intent, intent);
    intent
}
struct Handoff {
    intent: TransferIntent,
    nodes: Vec<Node<target_fixture::Target>>,
    image: ScopeImage,
    frozen: ScopedExportStatus,
}
fn freeze_and_import(
    env: &Environment<'_>,
    family: &mut Family,
    intent: TransferIntent,
    cache: &NativeManifestCache,
) -> Handoff {
    let ScopedSourceRead::Frozen(Some(frozen)) = phase(
        env,
        &mut family.owner,
        20,
        202,
        intent.encode(1000000).unwrap(),
        ScopedSourceQuery::Frozen(source_fixture::op(202)),
        retained_owner,
    ) else {
        panic!("second fence")
    };
    assert_eq!(
        observe(
            &mut family.owner,
            env.clock,
            20,
            ScopedSourceQuery::Data(data_query(cache, 150))
        ),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        observe(
            &mut family.owner,
            env.clock,
            20,
            ScopedSourceQuery::Data(data_query(cache, 200))
        ),
        ScopedSourceRead::Data(RoutedRead::Served(11))
    );
    let image = family.owner[0].local().applications[&group(20)]
        .export(source_fixture::op(202), 65536)
        .unwrap();
    assert_eq!(image.source_applied(), frozen.fence.fence.index);
    let template = successor(&intent);
    let mut nodes = open(
        configuration(env.root, 22, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || template.clone(),
    );
    phase(
        env,
        &mut nodes,
        22,
        202,
        template.bootstrap_command(1000000).unwrap(),
        TargetQuery::Status,
        || template.clone(),
    );
    let import = TargetImport::new(
        source_fixture::op(202),
        intent.clone(),
        group(22),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: config(&family.owner, 20),
            image: image.clone(),
            digest: frozen.digest,
        }],
    )
    .unwrap();
    phase(
        env,
        &mut nodes,
        22,
        202,
        template.import_command(&import, 1000000).unwrap(),
        TargetQuery::Status,
        || template.clone(),
    );
    assert_eq!(
        observe(
            &mut nodes,
            env.clock,
            22,
            TargetQuery::Data(data_query(cache, 150))
        ),
        TargetRead::NotActive
    );
    Handoff {
        intent,
        nodes,
        image,
        frozen,
    }
}
fn publish(
    env: &Environment<'_>,
    family: &mut Family,
    moved: &mut MovedMetadata,
    handoff: &mut Handoff,
) {
    let TargetRead::Status(status) =
        observe(&mut handoff.nodes, env.clock, 22, TargetQuery::Status)
    else {
        panic!("successor readiness")
    };
    let publication = TransferPublication::new(
        source_fixture::op(202),
        handoff.intent.clone(),
        vec![SourceFenceEvidence::from_scoped_status(
            config(&family.owner, 20),
            handoff.frozen,
            &handoff.intent,
        )
        .unwrap()],
        vec![TargetReadyEvidence::from_status(config(&handoff.nodes, 22), status).unwrap()],
    )
    .unwrap();
    let MetadataServingRead::Directory(DirectoryRead::Publication(Some(decision))) = metadata_phase(
        env,
        moved,
        203,
        publication.encode(1000000).unwrap(),
        MetadataServingQuery::Directory(DirectoryQuery::Publication(source_fixture::op(202))),
    ) else {
        panic!("later publication")
    };
    let retained = RetainedGrantAdoption {
        metadata_configuration: config(&moved.nodes, 9),
        decision: decision.clone(),
    };
    let ScopedSourceRead::Grant(Some(status)) = phase(
        env,
        &mut family.owner,
        20,
        301,
        retained.encode(1000000).unwrap(),
        ScopedSourceQuery::Grant(source_fixture::op(301)),
        retained_owner,
    ) else {
        panic!("later retained grant")
    };
    assert_eq!(status.transfer, source_fixture::op(202));
    assert_eq!(status.epoch, handoff.intent.after().input().epoch);
    let bytes = handoff.nodes[0].local().applications[&group(22)]
        .activation_command(
            &TargetActivation {
                metadata_configuration: config(&moved.nodes, 9),
                decision,
            },
            1000000,
        )
        .unwrap();
    let TargetRead::Status(status) = phase(
        env,
        &mut handoff.nodes,
        22,
        202,
        bytes,
        TargetQuery::Status,
        || successor(&handoff.intent),
    ) else {
        panic!("later activation")
    };
    assert!(status.activated.is_some());
}
fn refresh(
    env: &Environment<'_>,
    moved: &mut MovedMetadata,
    handoff: &Handoff,
    cache: &mut NativeManifestCache,
) {
    let root = manifest(env, moved, handoff.intent.before().input().responsibility);
    assert_eq!(root, *handoff.intent.after());
    let child = manifest(env, moved, fixture::id(22));
    assert_eq!(child.input().authority, group(9));
    cache.admit(root).unwrap();
    cache.admit(child).unwrap();
    assert_eq!(hint_for(cache, 150).group, group(22));
    assert_eq!(hint_for(cache, 200).group, group(20));
    assert_eq!(hint_for(cache, 1).group, group(21));
}
fn preserve(env: &Environment<'_>, family: &mut Family, handoff: &mut Handoff) {
    preserve_initial(env, family);
    assert_eq!(
        observe(
            &mut family.owner,
            env.clock,
            20,
            ScopedSourceQuery::Frozen(source_fixture::op(202))
        ),
        ScopedSourceRead::Frozen(Some(handoff.frozen))
    );
    for node in &family.owner {
        let owner = &node.local().applications[&group(20)];
        assert_eq!(
            owner.export(source_fixture::op(202), 65536).unwrap(),
            handoff.image
        );
        assert_eq!(owner.grant(), handoff.intent.after());
    }
    let bytes = handoff.nodes[0].local().applications[&group(22)]
        .bootstrap_command(1000000)
        .unwrap();
    phase(
        env,
        &mut handoff.nodes,
        22,
        202,
        bytes,
        TargetQuery::Status,
        || successor(&handoff.intent),
    );
}
fn serve_offline(
    env: &Environment<'_>,
    family: &mut Family,
    handoff: &mut Handoff,
    cache: &NativeManifestCache,
) {
    assert_eq!(
        phase(
            env,
            &mut handoff.nodes,
            22,
            5,
            command(cache, 150, 5),
            TargetQuery::Data(data_query(cache, 150)),
            || successor(&handoff.intent)
        ),
        TargetRead::Data(5)
    );
    assert_eq!(
        phase(
            env,
            &mut handoff.nodes,
            22,
            6,
            command(cache, 150, 2),
            TargetQuery::Data(data_query(cache, 150)),
            || successor(&handoff.intent)
        ),
        TargetRead::Data(7)
    );
    assert_eq!(
        phase(
            env,
            &mut family.owner,
            20,
            2,
            command(cache, 200, 11),
            ScopedSourceQuery::Data(data_query(cache, 200)),
            retained_owner
        ),
        ScopedSourceRead::Data(RoutedRead::Served(11))
    );
    assert_eq!(
        phase(
            env,
            &mut family.owner,
            20,
            7,
            command(cache, 200, 3),
            ScopedSourceQuery::Data(data_query(cache, 200)),
            retained_owner
        ),
        ScopedSourceRead::Data(RoutedRead::Served(14))
    );
    assert_eq!(
        phase(
            env,
            &mut family.child,
            21,
            1,
            command(cache, 1, 7),
            TargetQuery::Data(data_query(cache, 1)),
            || imported_owner(&family.intent, false)
        ),
        TargetRead::Data(7)
    );
    preserve(env, family, handoff);
    for node in &family.owner {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count(),
            4
        );
    }
    for node in &handoff.nodes {
        assert_eq!(
            node.local().applications[&group(22)]
                .application()
                .outbox()
                .count(),
            2
        );
    }
    for node in &family.child {
        assert_eq!(
            node.local().applications[&group(21)]
                .application()
                .outbox()
                .count(),
            1
        );
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _lock = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-metadata-retained-handoff-{}-{protocol:?}-{checkpoint}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let env = Environment {
        root: &root,
        clock: &clock,
        protocol,
        checkpoint,
    };
    let mut family = setup::family(&env, false);
    let mut moved = move_metadata(&env, &mut family);
    let mut cache = adopt(&env, &mut family, &mut moved);
    let original_adoptions = adoption_status(&env, &mut family);
    let original_metadata = OfflineMetadata::capture(&env, &mut family.metadata, 1);
    assert_eq!(
        phase(
            &env,
            &mut family.owner,
            20,
            5,
            command(&cache, 150, 5),
            ScopedSourceQuery::Data(data_query(&cache, 150)),
            retained_owner
        ),
        ScopedSourceRead::Data(RoutedRead::Served(5))
    );
    let before = manifest(
        &env,
        &mut moved,
        family.intent.before().input().responsibility,
    );
    let intent = reserve(&env, &mut moved, before);
    let mut handoff = freeze_and_import(&env, &mut family, intent, &cache);
    publish(&env, &mut family, &mut moved, &mut handoff);
    refresh(&env, &mut moved, &handoff, &mut cache);
    historical_publication(&env, &family, &mut moved);
    let moved_metadata = OfflineMetadata::capture(&env, &mut moved.nodes, 9);
    serve_offline(&env, &mut family, &mut handoff, &cache);
    assert_eq!(adoption_status(&env, &mut family), original_adoptions);
    original_metadata.verify(&env);
    moved_metadata.verify(&env);
    creation::abandon(family.owner, 20);
    creation::abandon(family.child, 21);
    creation::abandon(handoff.nodes, 22);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_later_retained_handoff_wal() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_later_retained_handoff_checkpoint() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_later_retained_handoff_wal() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_later_retained_handoff_checkpoint() {
    history(NativePeerProtocol::Quic, true);
}
