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

fn import(
    env: &Environment<'_>,
    first: &TransferIntent,
    source: &[Node<RetainedOwner>],
    frozen: ScopedExportStatus,
    partial: bool,
) -> (Vec<Node<Guard>>, ScopeImage) {
    let image = source[0].local().applications[&group(20)]
        .export(source_fixture::op(200), 65536)
        .unwrap();
    assert_eq!(image.source_applied(), frozen.fence.fence.index);
    let mut owner = open(
        configuration(env.root, 21, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || guard(first, partial),
    );
    phase(
        env,
        &mut owner,
        21,
        200,
        imported_owner(first, partial)
            .bootstrap_command(1000000)
            .unwrap(),
        RetirementQuery::Owner(TargetQuery::Status),
        || guard(first, partial),
    );
    let import = TargetImport::new(
        source_fixture::op(200),
        first.clone(),
        group(21),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: config(source, 20),
            image: image.clone(),
            digest: frozen.digest,
        }],
    )
    .unwrap();
    phase(
        env,
        &mut owner,
        21,
        200,
        imported_owner(first, partial)
            .import_command(&import, 1000000)
            .unwrap(),
        RetirementQuery::Owner(TargetQuery::Status),
        || guard(first, partial),
    );
    assert_eq!(
        observe(
            &mut owner,
            env.clock,
            21,
            RetirementQuery::Owner(query(first.after(), 21, 1))
        ),
        RetirementRead::Owner(TargetRead::NotActive)
    );
    (owner, image)
}
fn activate(
    env: &Environment<'_>,
    authority: &mut Vec<Node<MetadataPublishingSource>>,
    source: &mut Vec<Node<RetainedOwner>>,
    owner: &mut Vec<Node<Guard>>,
    first: &TransferIntent,
    frozen: ScopedExportStatus,
    partial: bool,
) -> TargetStatus {
    let RetirementRead::Owner(TargetRead::Status(status)) = observe(
        owner,
        env.clock,
        21,
        RetirementQuery::Owner(TargetQuery::Status),
    ) else {
        panic!("imported owner")
    };
    let p = TransferPublication::new(
        source_fixture::op(200),
        first.clone(),
        vec![SourceFenceEvidence::from_scoped_status(config(source, 20), frozen, first).unwrap()],
        vec![TargetReadyEvidence::from_status(config(owner, 21), status).unwrap()],
    )
    .unwrap();
    let DirectoryRead::Publication(Some(decision)) = metadata_phase(
        env,
        authority,
        201,
        p.encode(1000000).unwrap(),
        DirectoryQuery::Publication(source_fixture::op(200)),
        metadata,
    ) else {
        panic!("initial publication")
    };
    let cfg = config(authority, 1);
    let retained = RetainedGrantAdoption {
        metadata_configuration: cfg,
        decision: decision.clone(),
    };
    phase(
        env,
        source,
        20,
        300,
        retained.encode(1000000).unwrap(),
        ScopedSourceQuery::Grant(source_fixture::op(300)),
        retained_owner,
    );
    let bytes = owner[0].local().applications[&group(21)]
        .owner()
        .unwrap()
        .activation_command(
            &TargetActivation {
                metadata_configuration: cfg,
                decision,
            },
            1000000,
        )
        .unwrap();
    let RetirementRead::Owner(TargetRead::Status(status)) = phase(
        env,
        owner,
        21,
        200,
        bytes,
        RetirementQuery::Owner(TargetQuery::Status),
        || guard(first, partial),
    ) else {
        panic!("activated owner")
    };
    status
}
fn adopt(
    env: &Environment<'_>,
    source: &mut Vec<Node<RetainedOwner>>,
    owner: &mut Vec<Node<Guard>>,
    first: &TransferIntent,
    partial: bool,
    moved: &MovedMetadata,
) -> MetadataGrantStatus {
    let a = OwnerMetadataAdoption::new(
        moved.plan.clone(),
        source_fixture::grant().input().responsibility,
        moved.activation,
    )
    .unwrap();
    phase(
        env,
        source,
        20,
        701,
        a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap(),
        ScopedSourceQuery::MetadataAdoption(source_fixture::op(701)),
        retained_owner,
    );
    let a =
        OwnerMetadataAdoption::new(moved.plan.clone(), fixture::id(21), moved.activation).unwrap();
    let RetirementRead::Owner(TargetRead::MetadataAdoption(Some(status))) = phase(
        env,
        owner,
        21,
        701,
        a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap(),
        RetirementQuery::Owner(TargetQuery::MetadataAdoption(source_fixture::op(701))),
        || guard(first, partial),
    ) else {
        panic!("imported metadata adoption")
    };
    assert_eq!(status.activation, moved.activation);
    for node in owner {
        assert_eq!(
            node.local().applications[&group(21)]
                .owner()
                .unwrap()
                .grant(),
            &a.after()
        );
    }
    status
}
pub(super) fn build(env: &Environment<'_>, partial: bool) -> (Rig, MovedMetadata) {
    let mut authority = setup::prepare_metadata(env, metadata);
    let first = setup::reserve(env, &mut authority, metadata);
    let (mut source, frozen) = setup::prepare_owner(env, &first);
    let (mut owner, original_image) = import(env, &first, &source, frozen, partial);
    let original_target = activate(
        env,
        &mut authority,
        &mut source,
        &mut owner,
        &first,
        frozen,
        partial,
    );
    let moved = move_metadata(env, &mut authority, metadata);
    let metadata_adoption = adopt(env, &mut source, &mut owner, &first, partial, &moved);
    let mut rig = Rig {
        authority,
        source,
        owner,
        first,
        partial,
        original_target,
        original_image,
        metadata_adoption,
    };
    let grant = rig.grant();
    assert_eq!(
        rig.owner_phase(env, 1, data(&grant, 21, 1, 7), query(&grant, 21, 1)),
        TargetRead::Data(7)
    );
    assert_eq!(
        rig.owner_phase(env, 44, data(&grant, 21, 100, 9), query(&grant, 21, 100)),
        TargetRead::Data(9)
    );
    rig.verify_history(env);
    (rig, moved)
}
