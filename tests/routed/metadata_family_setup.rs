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

pub(super) fn prepare_metadata(
    env: &Environment<'_>,
    make: fn() -> MetadataPublishingSource,
) -> Vec<Node<MetadataPublishingSource>> {
    let mut nodes = open(
        configuration(env.root, 1, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        make,
    );
    let q = DirectoryQuery::Manifest(source_fixture::grant().input().responsibility);
    metadata_phase(
        env,
        &mut nodes,
        1000,
        make().bootstrap_command(1000000).unwrap(),
        q,
        make,
    );
    metadata_phase(
        env,
        &mut nodes,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: source_fixture::grant(),
        }
        .encode(1000000)
        .unwrap(),
        q,
        make,
    );
    nodes
}
pub(super) fn reserve(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<MetadataPublishingSource>>,
    make: fn() -> MetadataPublishingSource,
) -> TransferIntent {
    let configs = configuration(env.root, 21, &[1, 2, 3], NativeOpenMode::Create);
    let before = source_fixture::grant();
    let request = GroupCreationIntent {
        authority: group(1),
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: fixture::id(21),
        bootstrap: configs[0].bootstrap.clone(),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    metadata_phase(
        env,
        nodes,
        21,
        request.encode(1000000).unwrap(),
        DirectoryQuery::Manifest(before.input().responsibility),
        make,
    );
    let core = nodes[0].local().owner.core(group(1)).unwrap();
    let created = nodes[0].local().applications[&group(1)]
        .source()
        .directory()
        .unwrap()
        .directory()
        .group_creation_at(core.state().commit_index, group(21))
        .unwrap()
        .unwrap();
    assert_eq!(created.intent, request);
    let (after, child) = retained::retained_shape(false, &created);
    let intent = TransferIntent::insert_retained_child(before, after, child).unwrap();
    assert!(matches!(
        metadata_phase(
            env,
            nodes,
            200,
            intent.encode(1000000).unwrap(),
            DirectoryQuery::Transfer(source_fixture::op(200)),
            make,
        ),
        DirectoryRead::Transfer(Some(_))
    ));
    intent
}
pub(super) fn prepare_owner(
    env: &Environment<'_>,
    intent: &TransferIntent,
) -> (Vec<Node<RetainedOwner>>, ScopedExportStatus) {
    let mut nodes = open(
        configuration(env.root, 20, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        retained_owner,
    );
    campaign(&mut nodes, env.clock, 20);
    propose_recovering(
        &mut nodes,
        env.clock,
        20,
        100,
        retained_owner().bootstrap_command(1000000).unwrap(),
    );
    propose_recovering(&mut nodes, env.clock, 20, 1, source_fixture::data(1, 7));
    propose_recovering(&mut nodes, env.clock, 20, 2, source_fixture::data(200, 11));
    let ScopedSourceRead::Frozen(Some(status)) = phase(
        env,
        &mut nodes,
        20,
        200,
        intent.encode(1000000).unwrap(),
        ScopedSourceQuery::Frozen(source_fixture::op(200)),
        retained_owner,
    ) else {
        panic!("scoped source fence")
    };
    assert_eq!(
        observe(
            &mut nodes,
            env.clock,
            20,
            ScopedSourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1],
            })
        ),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    (nodes, status)
}
fn import_child(
    env: &Environment<'_>,
    intent: &TransferIntent,
    owner: &[Node<RetainedOwner>],
    frozen: ScopedExportStatus,
    partial: bool,
) -> (Vec<Node<target_fixture::Target>>, ScopeImage) {
    let image = owner[0].local().applications[&group(20)]
        .export(source_fixture::op(200), 65536)
        .unwrap();
    assert_eq!(image.source_applied(), frozen.fence.fence.index);
    let template = imported_owner(intent, partial);
    let mut nodes = open(
        configuration(env.root, 21, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || template.clone(),
    );
    phase(
        env,
        &mut nodes,
        21,
        200,
        template.bootstrap_command(1000000).unwrap(),
        TargetQuery::Status,
        || template.clone(),
    );
    let import = TargetImport::new(
        source_fixture::op(200),
        intent.clone(),
        group(21),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: config(owner, 20),
            image: image.clone(),
            digest: frozen.digest,
        }],
    )
    .unwrap();
    phase(
        env,
        &mut nodes,
        21,
        200,
        template.import_command(&import, 1000000).unwrap(),
        TargetQuery::Status,
        || template.clone(),
    );
    assert_eq!(
        observe(
            &mut nodes,
            env.clock,
            21,
            TargetQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1],
            })
        ),
        TargetRead::NotActive
    );
    (nodes, image)
}
fn publish_transfer(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<MetadataPublishingSource>>,
    intent: &TransferIntent,
    owner: &[Node<RetainedOwner>],
    child: &mut [Node<target_fixture::Target>],
    frozen: ScopedExportStatus,
) -> RetainedGrantAdoption {
    let TargetRead::Status(status) = observe(child, env.clock, 21, TargetQuery::Status) else {
        panic!("imported child")
    };
    let publication = TransferPublication::new(
        source_fixture::op(200),
        intent.clone(),
        vec![SourceFenceEvidence::from_scoped_status(config(owner, 20), frozen, intent).unwrap()],
        vec![TargetReadyEvidence::from_status(config(child, 21), status).unwrap()],
    )
    .unwrap();
    let DirectoryRead::Publication(Some(decision)) = metadata_phase(
        env,
        nodes,
        201,
        publication.encode(1000000).unwrap(),
        DirectoryQuery::Publication(source_fixture::op(200)),
        metadata,
    ) else {
        panic!("transfer publication")
    };
    RetainedGrantAdoption {
        metadata_configuration: config(nodes, 1),
        decision,
    }
}
pub(super) fn family(env: &Environment<'_>, partial: bool) -> Family {
    let mut metadata = prepare_metadata(env, metadata);
    let intent = reserve(env, &mut metadata, super::metadata);
    let (mut owner, frozen) = prepare_owner(env, &intent);
    let (mut child, image) = import_child(env, &intent, &owner, frozen, partial);
    let retained = publish_transfer(env, &mut metadata, &intent, &owner, &mut child, frozen);
    phase(
        env,
        &mut owner,
        20,
        300,
        retained.encode(1000000).unwrap(),
        ScopedSourceQuery::Grant(source_fixture::op(300)),
        retained_owner,
    );
    let bytes = child[0].local().applications[&group(21)]
        .activation_command(
            &TargetActivation {
                metadata_configuration: retained.metadata_configuration,
                decision: retained.decision.clone(),
            },
            1000000,
        )
        .unwrap();
    let TargetRead::Status(initial_target) =
        phase(env, &mut child, 21, 200, bytes, TargetQuery::Status, || {
            imported_owner(&intent, partial)
        })
    else {
        panic!("active child")
    };
    assert!(initial_target.activated.is_some());
    Family {
        metadata,
        owner,
        child,
        intent,
        partial,
        retained,
        image,
        frozen,
        initial_target,
    }
}
