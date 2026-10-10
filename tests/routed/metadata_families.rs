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
//! Retained and imported owners keep serving after their metadata moves.
use super::super::retained;
use super::*;
use voteboat::{scope::ScopeImage, scoped_source::*};
type RetainedOwner =
    ScopedTransferSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;

fn metadata() -> MetadataPublishingSource {
    let d = retained::metadata_profile(false, false);
    let bound = d.directory().readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(d, bound).unwrap_or_else(|_| panic!("metadata profile")),
    )
    .unwrap()
}
fn retained_owner() -> RetainedOwner {
    retained::source(false)
        .with_metadata_authority_adoption(2)
        .unwrap_or_else(|_| panic!("retained metadata profile"))
}
fn imported_owner(intent: &TransferIntent, partial: bool) -> target_fixture::Target {
    let t = retained::target(intent)
        .with_metadata_authority_adoption(2)
        .unwrap_or_else(|_| panic!("imported metadata profile"));
    if partial {
        t.with_partial_delegation(2, 65536)
            .unwrap_or_else(|_| panic!("partial profile"))
    } else {
        t
    }
}
fn directory_query(q: DirectoryQuery) -> MetadataPublishingQuery {
    MetadataPublishingQuery::Source(MetadataSourceQuery::Directory(q))
}
fn metadata_phase(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<MetadataPublishingSource>>,
    id: u128,
    bytes: Vec<u8>,
    q: DirectoryQuery,
) -> DirectoryRead {
    let MetadataPublishingRead::Source(MetadataSourceRead::Directory(r)) =
        phase(env, nodes, 1, id, bytes, directory_query(q), metadata)
    else {
        panic!("live metadata")
    };
    r
}
struct Family {
    metadata: Vec<Node<MetadataPublishingSource>>,
    owner: Vec<Node<RetainedOwner>>,
    child: Vec<Node<target_fixture::Target>>,
    intent: TransferIntent,
    partial: bool,
    retained: RetainedGrantAdoption,
    image: ScopeImage,
    frozen: ScopedExportStatus,
    initial_target: TargetStatus,
}
#[path = "metadata_family_setup.rs"]
mod setup;

struct MovedMetadata {
    nodes: Vec<Node<MetadataServingTarget>>,
    template: MetadataServingTarget,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
}
fn freeze(env: &Environment<'_>, family: &mut Family) -> (MetadataMovePlan, MetadataImage) {
    let plan = family.metadata[0].local().applications[&group(1)]
        .source()
        .plan(group(9))
        .unwrap();
    let bytes = family.metadata[0].local().applications[&group(1)]
        .source()
        .freeze_command(&plan, 1000000)
        .unwrap();
    phase(
        env,
        &mut family.metadata,
        1,
        700,
        bytes,
        MetadataPublishingQuery::Source(MetadataSourceQuery::Status),
        metadata,
    );
    assert_eq!(
        observe(
            &mut family.metadata,
            env.clock,
            1,
            directory_query(DirectoryQuery::Manifest(
                family.intent.before().input().responsibility
            ))
        ),
        MetadataPublishingRead::Source(MetadataSourceRead::Fenced)
    );
    let image = source_image(&mut family.metadata, env.clock, 1);
    (plan, image)
}
fn import_metadata(
    env: &Environment<'_>,
    family: &Family,
    plan: &MetadataMovePlan,
    image: &MetadataImage,
) -> (Vec<Node<MetadataServingTarget>>, MetadataServingTarget) {
    let source_cfg = config(&family.metadata, 1);
    let template = MetadataServingTarget::new(
        metadata(),
        plan.clone(),
        source_fixture::op(700),
        source_cfg,
        ConfigurationId::new(1).unwrap(),
    )
    .unwrap();
    let mut nodes = open(
        configuration(env.root, 9, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || template.clone(),
    );
    assert_eq!(config(&nodes, 9), ConfigurationId::new(1).unwrap());
    phase(
        env,
        &mut nodes,
        9,
        700,
        template.bootstrap_command(1000000).unwrap(),
        MetadataServingQuery::Status,
        || template.clone(),
    );
    phase(
        env,
        &mut nodes,
        9,
        700,
        template.import_command(image, source_cfg, 1000000).unwrap(),
        MetadataServingQuery::Status,
        || template.clone(),
    );
    assert_eq!(
        observe(
            &mut nodes,
            env.clock,
            9,
            MetadataServingQuery::Directory(DirectoryQuery::Manifest(
                family.intent.before().input().responsibility
            ))
        ),
        MetadataServingRead::NotActive
    );
    (nodes, template)
}
fn move_metadata(env: &Environment<'_>, family: &mut Family) -> MovedMetadata {
    let (plan, image) = freeze(env, family);
    let (mut nodes, template) = import_metadata(env, family, &plan, &image);
    let MetadataServingRead::Status(status) =
        observe(&mut nodes, env.clock, 9, MetadataServingQuery::Status)
    else {
        panic!("imported metadata")
    };
    let bytes = family.metadata[0].local().applications[&group(1)]
        .publication_command(status.target.imported.unwrap(), config(&nodes, 9), 1000000)
        .unwrap();
    let MetadataPublishingRead::Publication(Some(publication)) = phase(
        env,
        &mut family.metadata,
        1,
        700,
        bytes,
        MetadataPublishingQuery::Publication,
        metadata,
    ) else {
        panic!("metadata publication")
    };
    let bytes = nodes[0].local().applications[&group(9)]
        .activation_command(publication, 1000000)
        .unwrap();
    let MetadataServingRead::Status(status) = phase(
        env,
        &mut nodes,
        9,
        700,
        bytes,
        MetadataServingQuery::Status,
        || template.clone(),
    ) else {
        panic!("active metadata")
    };
    MovedMetadata {
        nodes,
        template,
        plan,
        activation: status.activation.unwrap(),
    }
}
fn adopt(
    env: &Environment<'_>,
    family: &mut Family,
    moved: &mut MovedMetadata,
) -> NativeManifestCache {
    let root = family.intent.after().input().responsibility;
    let child = fixture::id(21);
    let a = OwnerMetadataAdoption::new(moved.plan.clone(), root, moved.activation).unwrap();
    let b = OwnerMetadataAdoption::new(moved.plan.clone(), child, moved.activation).unwrap();
    let ScopedSourceRead::MetadataAdoption(Some(s)) = phase(
        env,
        &mut family.owner,
        20,
        701,
        a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap(),
        ScopedSourceQuery::MetadataAdoption(source_fixture::op(701)),
        retained_owner,
    ) else {
        panic!("retained adoption")
    };
    assert_eq!(s.activation, moved.activation);
    let TargetRead::MetadataAdoption(Some(s)) = phase(
        env,
        &mut family.child,
        21,
        701,
        b.encode(MAX_METADATA_ADOPTION_BYTES).unwrap(),
        TargetQuery::MetadataAdoption(source_fixture::op(701)),
        || imported_owner(&family.intent, family.partial),
    ) else {
        panic!("imported adoption")
    };
    assert_eq!(s.activation, moved.activation);
    for node in &family.owner {
        assert_eq!(node.local().applications[&group(20)].grant(), &a.after());
    }
    for node in &family.child {
        assert_eq!(node.local().applications[&group(21)].grant(), &b.after());
    }
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 4,
        bytes: 1000000,
    })
    .unwrap()
    .with_metadata_authority_moves()
    .unwrap_or_else(|_| panic!("cache profile"));
    cache.admit(family.intent.after().clone()).unwrap();
    cache.admit(b.before().clone()).unwrap();
    for adoption in [&a, &b] {
        let MetadataServingRead::Directory(DirectoryRead::Manifest(Some(manifest))) = observe(
            &mut moved.nodes,
            env.clock,
            9,
            MetadataServingQuery::Directory(DirectoryQuery::Manifest(
                adoption.after().input().responsibility,
            )),
        ) else {
            panic!("moved directory manifest")
        };
        assert_eq!(manifest, adoption.after());
        cache.admit(manifest).unwrap();
    }
    assert_eq!(a.before().input().epoch, a.after().input().epoch);
    assert_eq!(b.before().input().epoch, b.after().input().epoch);
    cache
}
fn hint_for(cache: &NativeManifestCache, key: u8) -> RouteHint {
    resolve(
        cache,
        &source_fixture::Policy,
        source_fixture::grant().input().responsibility,
        &[key],
        3,
    )
    .unwrap()
}
fn command(cache: &NativeManifestCache, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint_for(cache, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn data_query(cache: &NativeManifestCache, key: u8) -> RoutedQuery<Vec<u8>> {
    RoutedQuery {
        hint: hint_for(cache, key),
        key: vec![key],
        query: vec![key],
    }
}
fn adoption_status(
    env: &Environment<'_>,
    family: &mut Family,
) -> (ScopedSourceRead<i64>, TargetRead<i64>) {
    let source = observe(
        &mut family.owner,
        env.clock,
        20,
        ScopedSourceQuery::MetadataAdoption(source_fixture::op(701)),
    );
    let target = observe(
        &mut family.child,
        env.clock,
        21,
        TargetQuery::MetadataAdoption(source_fixture::op(701)),
    );
    assert!(matches!(
        source,
        ScopedSourceRead::MetadataAdoption(Some(_))
    ));
    assert!(matches!(target, TargetRead::MetadataAdoption(Some(_))));
    (source, target)
}
fn preserve_initial(env: &Environment<'_>, family: &mut Family) {
    assert_eq!(
        observe(
            &mut family.owner,
            env.clock,
            20,
            ScopedSourceQuery::Frozen(source_fixture::op(200))
        ),
        ScopedSourceRead::Frozen(Some(family.frozen))
    );
    assert_eq!(
        observe(&mut family.child, env.clock, 21, TargetQuery::Status),
        TargetRead::Status(family.initial_target.clone())
    );
    for node in &family.owner {
        assert_eq!(
            node.local().applications[&group(20)]
                .export(source_fixture::op(200), 65536)
                .unwrap(),
            family.image
        );
    }
    phase(
        env,
        &mut family.owner,
        20,
        300,
        family.retained.encode(1000000).unwrap(),
        ScopedSourceQuery::Grant(source_fixture::op(300)),
        retained_owner,
    );
}
fn serve_offline(env: &Environment<'_>, family: &mut Family, cache: &NativeManifestCache) {
    let adoptions = adoption_status(env, family);
    assert_eq!(hint_for(cache, 200).group, group(20));
    assert_eq!(hint_for(cache, 1).group, group(21));
    // Original operations use current routing but keep their original semantic bodies.
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
            &mut family.child,
            21,
            1,
            command(cache, 1, 7),
            TargetQuery::Data(data_query(cache, 1)),
            || imported_owner(&family.intent, family.partial)
        ),
        TargetRead::Data(7)
    );
    assert_eq!(
        phase(
            env,
            &mut family.owner,
            20,
            3,
            command(cache, 200, 5),
            ScopedSourceQuery::Data(data_query(cache, 200)),
            retained_owner
        ),
        ScopedSourceRead::Data(RoutedRead::Served(16))
    );
    assert_eq!(
        phase(
            env,
            &mut family.child,
            21,
            4,
            command(cache, 1, 3),
            TargetQuery::Data(data_query(cache, 1)),
            || imported_owner(&family.intent, family.partial)
        ),
        TargetRead::Data(10)
    );
    preserve_initial(env, family);
    assert_eq!(adoption_status(env, family), adoptions);
    for node in &family.owner {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count(),
            3
        );
    }
    for node in &family.child {
        assert_eq!(
            node.local().applications[&group(21)]
                .application()
                .outbox()
                .count(),
            2
        );
    }
}
fn historical_publication(env: &Environment<'_>, family: &Family, moved: &mut MovedMetadata) {
    let MetadataServingRead::Historical {
        source,
        through,
        value,
    } = observe(
        &mut moved.nodes,
        env.clock,
        9,
        MetadataServingQuery::Historical(DirectoryQuery::Publication(source_fixture::op(200))),
    )
    else {
        panic!("historical transfer publication")
    };
    assert_eq!(source, group(1));
    assert!(through >= family.retained.decision.index);
    assert_eq!(
        value,
        DirectoryRead::Publication(Some(family.retained.decision.clone()))
    );
}
fn history(protocol: NativePeerProtocol, checkpoint: bool, partial: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-metadata-families-{}-{protocol:?}-{checkpoint}-{partial}",
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
    let mut family = setup::family(&env, partial);
    let mut moved = move_metadata(&env, &mut family);
    let cache = adopt(&env, &mut family, &mut moved);
    historical_publication(&env, &family, &mut moved);
    let original = OfflineMetadata::capture(&env, &mut family.metadata, 1);
    let destination = OfflineMetadata::capture(&env, &mut moved.nodes, 9);
    serve_offline(&env, &mut family, &cache);
    original.verify(&env);
    destination.verify(&env);
    creation::abandon(family.owner, 20);
    creation::abandon(family.child, 21);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_retained_and_imported_metadata_move_wal() {
    for partial in [false, true] {
        history(NativePeerProtocol::TcpTls, false, partial);
    }
}
#[test]
fn tcp_retained_and_imported_metadata_move_checkpoint() {
    for partial in [false, true] {
        history(NativePeerProtocol::TcpTls, true, partial);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_retained_and_imported_metadata_move_wal() {
    for partial in [false, true] {
        history(NativePeerProtocol::Quic, false, partial);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_retained_and_imported_metadata_move_checkpoint() {
    for partial in [false, true] {
        history(NativePeerProtocol::Quic, true, partial);
    }
}

#[path = "metadata_retained_handoff.rs"]
mod retained_handoff;
