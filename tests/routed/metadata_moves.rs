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
//! Native repeated metadata movement, foreign locator updates and owner recovery.
use super::super::{creation, creation_source::durable_files};
use super::*;
use std::fmt::Debug;
use voteboat::{bucket_counter::BucketCounter, metadata_transfer::*};
type Owner = TransferSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;

fn root_manifest() -> ResponsibilityManifest {
    let mut m = source_fixture::grant().into_input();
    m.parent = Some(ParentAuthority {
        responsibility: fixture::id(50),
        group: group(100),
    });
    ResponsibilityManifest::new(m).unwrap()
}
fn parent_manifest() -> ResponsibilityManifest {
    let root = root_manifest();
    let mut p = source_fixture::grant().into_input();
    p.authority = group(100);
    p.responsibility = fixture::id(50);
    p.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: p.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: root.input().responsibility,
            group: group(1),
            epoch: root.input().epoch,
        }),
    }]);
    ResponsibilityManifest::new(p).unwrap()
}
fn directory(m: ResponsibilityManifest) -> Directory {
    Directory::new(
        DirectoryPlan::new(m.input().authority, vec![m]).unwrap(),
        DirectoryLimits {
            operations: 8,
            history_bytes: 100000,
        },
    )
    .unwrap()
    .with_metadata_locator_updates()
    .unwrap_or_else(|_| panic!("directory profile"))
}
fn original() -> MetadataPublishingSource {
    let d = directory(root_manifest());
    let bytes = d.readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(LifecycleDirectory::new(d), bytes)
            .unwrap_or_else(|_| panic!("source")),
    )
    .unwrap()
}
fn repeated(
    plan: MetadataMovePlan,
    source_cfg: ConfigurationId,
    target_cfg: ConfigurationId,
) -> MetadataPublishingSource {
    let target = MetadataServingTarget::new(
        original(),
        plan,
        source_fixture::op(200),
        source_cfg,
        target_cfg,
    )
    .unwrap();
    let bytes = target.readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::from_serving(target, bytes)
            .unwrap_or_else(|_| panic!("repeat profile")),
    )
    .unwrap()
}
fn owner() -> Owner {
    let routed = RoutedApplication::new(
        group(20),
        root_manifest(),
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
    .unwrap_or_else(|_| panic!("owner"));
    TransferSource::new(
        routed
            .with_metadata_authority_adoption(2)
            .unwrap_or_else(|_| panic!("adoption profile")),
        65536,
    )
    .unwrap_or_else(|_| panic!("source owner"))
}
fn config<A>(nodes: &[Node<A>], g: u128) -> ConfigurationId
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0]
        .local()
        .owner
        .core(group(g))
        .unwrap()
        .membership()
        .id()
}
#[derive(Clone, Copy)]
struct Environment<'a> {
    root: &'a Path,
    clock: &'a Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
}
fn phase<A>(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<A>>,
    g: u128,
    id: u128,
    bytes: Vec<u8>,
    query: A::Query,
    make: impl Fn() -> A,
) -> A::ReadResult
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    A::Query: Clone,
    A::ReadResult: PartialEq + Debug,
{
    let Environment {
        root,
        clock,
        protocol,
        checkpoint,
    } = *env;
    campaign(nodes, clock, g);
    phase_write(true, nodes, clock, g, id, bytes.clone());
    let original = observe(nodes, clock, g, query.clone());
    if checkpoint {
        compact(nodes, clock, g);
    }
    creation::abandon(std::mem::take(nodes), g);
    *nodes = open(
        configuration(root, g, &[1, 2, 3], NativeOpenMode::Recover),
        clock,
        protocol,
        make,
    );
    assert_eq!(observe(nodes, clock, g, query.clone()), original);
    propose_recovering(nodes, clock, g, id, bytes);
    assert_eq!(observe(nodes, clock, g, query), original);
    original
}
fn source_status(
    nodes: &mut [Node<MetadataPublishingSource>],
    clock: &Instant,
    g: u128,
) -> MetadataSourceStatus {
    let MetadataPublishingRead::Source(MetadataSourceRead::Status(Some(status))) = observe(
        nodes,
        clock,
        g,
        MetadataPublishingQuery::Source(MetadataSourceQuery::Status),
    ) else {
        panic!("source status")
    };
    status
}
fn source_image(
    nodes: &mut [Node<MetadataPublishingSource>],
    clock: &Instant,
    g: u128,
) -> MetadataImage {
    let status = source_status(nodes, clock, g);
    let image = nodes[0].local().applications[&group(g)]
        .source()
        .export(1000000)
        .unwrap();
    assert_eq!(image.status(), status);
    image
}
fn active_b(
    nodes: &mut [Node<MetadataPublishingSource>],
    clock: &Instant,
) -> MetadataServingStatus {
    let MetadataPublishingRead::Source(MetadataSourceRead::Serving(MetadataServingRead::Status(s))) =
        observe(
            nodes,
            clock,
            9,
            MetadataPublishingQuery::Source(MetadataSourceQuery::Serving(
                MetadataServingQuery::Status,
            )),
        )
    else {
        panic!("B status")
    };
    s
}
fn publish<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    id: u128,
    manifest: ResponsibilityManifest,
) -> A::Receipt
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    campaign(nodes, clock, g);
    propose_recovering(
        nodes,
        clock,
        g,
        id,
        DirectoryCommand {
            expected: None,
            manifest,
        }
        .encode(1000000)
        .unwrap(),
    )
}
fn hint(cache: &NativeManifestCache) -> RouteHint {
    resolve(cache, &source_fixture::Policy, fixture::id(50), &[1], 3).unwrap()
}
fn write(
    nodes: &mut [Node<Owner>],
    clock: &Instant,
    cache: &NativeManifestCache,
    id: u128,
    delta: i64,
) {
    campaign(nodes, clock, 20);
    let bytes = encode_routed(
        hint(cache),
        &[1],
        &encode_add(&[1], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        propose_recovering(nodes, clock, 20, id, bytes).outcome,
        RoutedOutcome::Applied(_)
    ));
}
fn value(nodes: &mut [Node<Owner>], clock: &Instant, cache: &NativeManifestCache) -> i64 {
    let SourceRead::Data(RoutedRead::Served(v)) = observe(
        nodes,
        clock,
        20,
        SourceQuery::Data(RoutedQuery {
            hint: hint(cache),
            key: vec![1],
            query: vec![1],
        }),
    ) else {
        panic!("owner value")
    };
    v
}
fn parent_refresh(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<LifecycleDirectory>>,
    cache: &mut NativeManifestCache,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
    id: u128,
) {
    let clock = env.clock;
    let DirectoryRead::Manifest(Some(before)) =
        observe(nodes, clock, 100, DirectoryQuery::Manifest(fixture::id(50)))
    else {
        panic!("parent")
    };
    let update = MetadataLocatorUpdate::new(before, plan, activation).unwrap();
    let status = phase(
        env,
        nodes,
        100,
        id,
        update.encode(MAX_METADATA_LOCATOR_BYTES).unwrap(),
        DirectoryQuery::MetadataLocator(source_fixture::op(id)),
        || LifecycleDirectory::new(directory(parent_manifest())),
    );
    assert!(matches!(status, DirectoryRead::MetadataLocator(Some(_))));
    let DirectoryRead::Manifest(Some(m)) =
        observe(nodes, clock, 100, DirectoryQuery::Manifest(fixture::id(50)))
    else {
        panic!("updated parent")
    };
    assert_eq!(m, update.after());
    cache.admit(m).unwrap();
}
fn owner_adopt(
    env: &Environment<'_>,
    nodes: &mut Vec<Node<Owner>>,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
    id: u128,
) -> MetadataGrantStatus {
    let adoption =
        OwnerMetadataAdoption::new(plan, root_manifest().input().responsibility, activation)
            .unwrap();
    let status = phase(
        env,
        nodes,
        20,
        id,
        adoption.encode(MAX_METADATA_ADOPTION_BYTES).unwrap(),
        SourceQuery::MetadataAdoption(source_fixture::op(id)),
        owner,
    );
    let SourceRead::MetadataAdoption(Some(status)) = status else {
        panic!("owner adoption")
    };
    for node in nodes {
        assert_eq!(
            node.local().applications[&group(20)].routed().grant(),
            &adoption.after()
        );
    }
    status
}
struct FirstMove {
    nodes: Vec<Node<MetadataPublishingSource>>,
    template: MetadataPublishingSource,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
}
struct SecondMove {
    nodes: Vec<Node<MetadataServingTarget>>,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
}
struct OfflineMetadata {
    group: u128,
    files: BTreeMap<std::path::PathBuf, Vec<u8>>,
    logs: BTreeMap<NodeId, GroupLog>,
}
fn initialize_source(
    env: &Environment<'_>,
) -> (Vec<Node<MetadataPublishingSource>>, DirectoryReceipt) {
    let mut a = open(
        configuration(env.root, 1, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        original,
    );
    campaign(&mut a, env.clock, 1);
    propose_recovering(
        &mut a,
        env.clock,
        1,
        1000,
        original().bootstrap_command(1000000).unwrap(),
    );
    let original_receipt = publish(&mut a, env.clock, 1, 1001, root_manifest());
    let MetadataPublishingOutcome::Source(MetadataSourceOutcome::Directory(original_receipt)) =
        original_receipt.outcome
    else {
        panic!("original manifest")
    };
    (a, original_receipt)
}
fn initialize_parent(env: &Environment<'_>) -> Vec<Node<LifecycleDirectory>> {
    let mut parent = open(
        configuration(env.root, 100, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || LifecycleDirectory::new(directory(parent_manifest())),
    );
    campaign(&mut parent, env.clock, 100);
    propose_recovering(
        &mut parent,
        env.clock,
        100,
        1000,
        directory(parent_manifest())
            .bootstrap_command(1000000)
            .unwrap(),
    );
    publish(&mut parent, env.clock, 100, 1001, parent_manifest());
    parent
}
fn initialize_owner(env: &Environment<'_>) -> Vec<Node<Owner>> {
    let mut owners = open(
        configuration(env.root, 20, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        owner,
    );
    campaign(&mut owners, env.clock, 20);
    propose_recovering(
        &mut owners,
        env.clock,
        20,
        100,
        owner().bootstrap_command(1000000).unwrap(),
    );
    owners
}
fn initialize_cache() -> NativeManifestCache {
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 4,
        bytes: 1000000,
    })
    .unwrap()
    .with_metadata_authority_moves()
    .unwrap_or_else(|_| panic!("move cache"))
    .with_metadata_locator_updates()
    .unwrap_or_else(|_| panic!("locator cache"));
    cache.admit(root_manifest()).unwrap();
    cache.admit(parent_manifest()).unwrap();
    cache
}
fn freeze_initial(
    env: &Environment<'_>,
    a: &mut Vec<Node<MetadataPublishingSource>>,
) -> (MetadataMovePlan, MetadataImage) {
    let first_plan = a[0].local().applications[&group(1)]
        .source()
        .plan(group(9))
        .unwrap();
    let freeze = a[0].local().applications[&group(1)]
        .source()
        .freeze_command(&first_plan, 1000000)
        .unwrap();
    phase(
        env,
        a,
        1,
        200,
        freeze,
        MetadataPublishingQuery::Source(MetadataSourceQuery::Status),
        original,
    );
    let first_image = source_image(a, env.clock, 1);
    assert_eq!(
        observe(
            a,
            env.clock,
            1,
            MetadataPublishingQuery::Source(MetadataSourceQuery::Directory(
                DirectoryQuery::Manifest(root_manifest().input().responsibility)
            ))
        ),
        MetadataPublishingRead::Source(MetadataSourceRead::Fenced)
    );
    (first_plan, first_image)
}
fn import_first(
    env: &Environment<'_>,
    a: &[Node<MetadataPublishingSource>],
    first_plan: &MetadataMovePlan,
    first_image: &MetadataImage,
) -> (
    Vec<Node<MetadataPublishingSource>>,
    MetadataPublishingSource,
) {
    let a_cfg = config(a, 1);
    // All selected groups have their explicit original bootstrap configuration.
    let b_cfg = ConfigurationId::new(1).unwrap();
    let b_template = repeated(first_plan.clone(), a_cfg, b_cfg);
    let mut b = open(
        configuration(env.root, 9, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || b_template.clone(),
    );
    assert_eq!(config(&b, 9), b_cfg);
    let b_query =
        MetadataPublishingQuery::Source(MetadataSourceQuery::Serving(MetadataServingQuery::Status));
    phase(
        env,
        &mut b,
        9,
        200,
        b_template.bootstrap_command(1000000).unwrap(),
        b_query.clone(),
        || b_template.clone(),
    );
    phase(
        env,
        &mut b,
        9,
        200,
        b_template
            .source()
            .serving_target()
            .unwrap()
            .import_command(first_image, a_cfg, 1000000)
            .unwrap(),
        b_query.clone(),
        || b_template.clone(),
    );
    (b, b_template)
}
fn activate_first(
    env: &Environment<'_>,
    a: &mut Vec<Node<MetadataPublishingSource>>,
    b: &mut Vec<Node<MetadataPublishingSource>>,
    b_template: &MetadataPublishingSource,
) -> MetadataActivationStatus {
    let b_cfg = config(b, 9);
    let b_query =
        MetadataPublishingQuery::Source(MetadataSourceQuery::Serving(MetadataServingQuery::Status));
    let imported = active_b(b, env.clock).target.imported.unwrap();
    let publication = a[0].local().applications[&group(1)]
        .publication_command(imported, b_cfg, 1000000)
        .unwrap();
    let observed = phase(
        env,
        a,
        1,
        200,
        publication,
        MetadataPublishingQuery::Publication,
        original,
    );
    let MetadataPublishingRead::Publication(Some(first_publication)) = observed else {
        panic!("A publication")
    };
    let activation_command = b[0].local().applications[&group(9)]
        .source()
        .serving_target()
        .unwrap()
        .activation_command(first_publication, 1000000)
        .unwrap();
    phase(env, b, 9, 200, activation_command, b_query, || {
        b_template.clone()
    });
    active_b(b, env.clock).activation.unwrap()
}
fn first_move(env: &Environment<'_>, a: &mut Vec<Node<MetadataPublishingSource>>) -> FirstMove {
    let (first_plan, first_image) = freeze_initial(env, a);
    let (mut b, b_template) = import_first(env, a, &first_plan, &first_image);
    let first_activation = activate_first(env, a, &mut b, &b_template);
    FirstMove {
        nodes: b,
        template: b_template,
        plan: first_plan,
        activation: first_activation,
    }
}
fn cache_first(
    env: &Environment<'_>,
    b: &mut [Node<MetadataPublishingSource>],
    cache: &mut NativeManifestCache,
) {
    let MetadataPublishingRead::Source(MetadataSourceRead::Serving(
        MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))),
    )) = observe(
        b,
        env.clock,
        9,
        MetadataPublishingQuery::Source(MetadataSourceQuery::Directory(DirectoryQuery::Manifest(
            root_manifest().input().responsibility,
        ))),
    )
    else {
        panic!("B manifest")
    };
    cache.admit(m).unwrap();
    assert!(resolve(cache, &source_fixture::Policy, fixture::id(50), &[1], 3).is_err());
}
fn freeze_repeated(
    env: &Environment<'_>,
    b: &mut Vec<Node<MetadataPublishingSource>>,
    b_template: &MetadataPublishingSource,
) -> (MetadataMovePlan, MetadataImage) {
    let second_plan = b[0].local().applications[&group(9)]
        .source()
        .plan(group(11))
        .unwrap();
    let freeze = b[0].local().applications[&group(9)]
        .source()
        .freeze_command(&second_plan, 1000000)
        .unwrap();
    phase(
        env,
        b,
        9,
        201,
        freeze,
        MetadataPublishingQuery::Source(MetadataSourceQuery::Status),
        || b_template.clone(),
    );
    let second_image = source_image(b, env.clock, 9);
    (second_plan, second_image)
}
fn import_second(
    env: &Environment<'_>,
    b_template: &MetadataPublishingSource,
    b_cfg: ConfigurationId,
    second_plan: &MetadataMovePlan,
    second_image: &MetadataImage,
) -> (Vec<Node<MetadataServingTarget>>, MetadataServingTarget) {
    let c_cfg = ConfigurationId::new(1).unwrap();
    let c_template = MetadataServingTarget::new(
        b_template.clone(),
        second_plan.clone(),
        source_fixture::op(201),
        b_cfg,
        c_cfg,
    )
    .unwrap();
    let mut c = open(
        configuration(env.root, 11, &[1, 2, 3], NativeOpenMode::Create),
        env.clock,
        env.protocol,
        || c_template.clone(),
    );
    assert_eq!(config(&c, 11), c_cfg);
    phase(
        env,
        &mut c,
        11,
        201,
        c_template.bootstrap_command(1000000).unwrap(),
        MetadataServingQuery::Status,
        || c_template.clone(),
    );
    phase(
        env,
        &mut c,
        11,
        201,
        c_template
            .import_command(second_image, b_cfg, 1000000)
            .unwrap(),
        MetadataServingQuery::Status,
        || c_template.clone(),
    );
    assert_eq!(
        observe(
            &mut c,
            env.clock,
            11,
            MetadataServingQuery::Directory(DirectoryQuery::Manifest(
                root_manifest().input().responsibility
            ))
        ),
        MetadataServingRead::NotActive
    );
    (c, c_template)
}
fn activate_second(
    env: &Environment<'_>,
    b: &mut Vec<Node<MetadataPublishingSource>>,
    b_template: &MetadataPublishingSource,
    c: &mut Vec<Node<MetadataServingTarget>>,
    c_template: &MetadataServingTarget,
) -> MetadataActivationStatus {
    let c_cfg = config(c, 11);
    let MetadataServingRead::Status(c_status) =
        observe(c, env.clock, 11, MetadataServingQuery::Status)
    else {
        panic!()
    };
    let publication = b[0].local().applications[&group(9)]
        .publication_command(c_status.target.imported.unwrap(), c_cfg, 1000000)
        .unwrap();
    let MetadataPublishingRead::Publication(Some(second_publication)) = phase(
        env,
        b,
        9,
        201,
        publication,
        MetadataPublishingQuery::Publication,
        || b_template.clone(),
    ) else {
        panic!()
    };
    let activate = c[0].local().applications[&group(11)]
        .activation_command(second_publication, 1000000)
        .unwrap();
    let MetadataServingRead::Status(status) = phase(
        env,
        c,
        11,
        201,
        activate,
        MetadataServingQuery::Status,
        || c_template.clone(),
    ) else {
        panic!()
    };
    status.activation.unwrap()
}
fn second_move(env: &Environment<'_>, b: &mut FirstMove) -> SecondMove {
    let (second_plan, second_image) = freeze_repeated(env, &mut b.nodes, &b.template);
    let (mut c, c_template) = import_second(
        env,
        &b.template,
        config(&b.nodes, 9),
        &second_plan,
        &second_image,
    );
    let second_activation = activate_second(env, &mut b.nodes, &b.template, &mut c, &c_template);
    SecondMove {
        nodes: c,
        plan: second_plan,
        activation: second_activation,
    }
}
fn cache_second(
    env: &Environment<'_>,
    c: &mut [Node<MetadataServingTarget>],
    cache: &mut NativeManifestCache,
) {
    let MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))) = observe(
        c,
        env.clock,
        11,
        MetadataServingQuery::Directory(DirectoryQuery::Manifest(
            root_manifest().input().responsibility,
        )),
    ) else {
        panic!()
    };
    cache.admit(m).unwrap();
    assert!(resolve(cache, &source_fixture::Policy, fixture::id(50), &[1], 3).is_err());
}
fn verify_historical(
    env: &Environment<'_>,
    c: &mut [Node<MetadataServingTarget>],
    original_command: Vec<u8>,
    original_receipt: DirectoryReceipt,
) {
    campaign(c, env.clock, 11);
    assert_eq!(
        propose_recovering(c, env.clock, 11, 1001, original_command).outcome,
        MetadataServingOutcome::Historical {
            source: group(1),
            index: original_receipt.index,
            outcome: original_receipt.outcome
        }
    );
    assert!(
        matches!(observe(c,env.clock,11,MetadataServingQuery::Historical(DirectoryQuery::Manifest(root_manifest().input().responsibility))),MetadataServingRead::Historical{source,value:DirectoryRead::Manifest(Some(m)),..} if source==group(9) && m.input().authority==group(9))
    );
}
fn recover_owner(
    env: &Environment<'_>,
    owners: &mut Vec<Node<Owner>>,
    cache: &NativeManifestCache,
    owner_status: MetadataGrantStatus,
) {
    write(owners, env.clock, cache, 4, 2);
    assert_eq!(value(owners, env.clock, cache), 17);
    if env.checkpoint {
        compact(owners, env.clock, 20);
    }
    creation::abandon(std::mem::take(owners), 20);
    *owners = open(
        configuration(env.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        env.clock,
        env.protocol,
        owner,
    );
    assert_eq!(
        observe(
            owners,
            env.clock,
            20,
            SourceQuery::MetadataAdoption(source_fixture::op(301))
        ),
        SourceRead::MetadataAdoption(Some(owner_status))
    );
    for (id, delta) in [(1, 7), (2, 3), (3, 5), (4, 2)] {
        write(owners, env.clock, cache, id, delta);
    }
    assert_eq!(value(owners, env.clock, cache), 17);
    for node in owners {
        let app = node.local().applications[&group(20)].routed();
        assert_eq!(app.grant().input().authority, group(11));
        assert_eq!(app.grant().input().epoch, root_manifest().input().epoch);
        assert_eq!(app.application().outbox().count(), 4);
    }
}
impl OfflineMetadata {
    fn capture<A>(env: &Environment<'_>, nodes: &mut Vec<Node<A>>, g: u128) -> Self
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let logs = creation::abandon(std::mem::take(nodes), g);
        let files = durable_files(&env.root.join(g.to_string()));
        Self {
            group: g,
            logs,
            files,
        }
    }
    fn verify(self, env: &Environment<'_>) {
        let g = self.group;
        assert_eq!(durable_files(&env.root.join(g.to_string())), self.files);
        for cfg in configuration(env.root, g, &[1, 2, 3], NativeOpenMode::Recover) {
            let log = NativeLogStore::recover(
                FileLogIo::open(&cfg.directory).unwrap(),
                cfg.store,
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(log.state(group(g)).unwrap(), self.logs[&cfg.node]);
        }
    }
}
fn run(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-metadata-move-{}-{protocol:?}-{checkpoint}",
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
    let (mut a, original_receipt) = initialize_source(&env);
    let original_command = DirectoryCommand {
        expected: None,
        manifest: root_manifest(),
    }
    .encode(1000000)
    .unwrap();
    let mut parent = initialize_parent(&env);
    let mut owners = initialize_owner(&env);
    let mut cache = initialize_cache();
    write(&mut owners, &clock, &cache, 1, 7);
    assert_eq!(value(&mut owners, &clock, &cache), 7);
    let mut b = first_move(&env, &mut a);
    cache_first(&env, &mut b.nodes, &mut cache);
    parent_refresh(
        &env,
        &mut parent,
        &mut cache,
        b.plan.clone(),
        b.activation,
        400,
    );
    owner_adopt(&env, &mut owners, b.plan.clone(), b.activation, 300);
    write(&mut owners, &clock, &cache, 2, 3);
    assert_eq!(value(&mut owners, &clock, &cache), 10);
    let a_record = OfflineMetadata::capture(&env, &mut a, 1);
    let mut c = second_move(&env, &mut b);
    cache_second(&env, &mut c.nodes, &mut cache);
    parent_refresh(
        &env,
        &mut parent,
        &mut cache,
        c.plan.clone(),
        c.activation,
        401,
    );
    let owner_status = owner_adopt(&env, &mut owners, c.plan, c.activation, 301);
    write(&mut owners, &clock, &cache, 3, 5);
    assert_eq!(value(&mut owners, &clock, &cache), 15);
    verify_historical(&env, &mut c.nodes, original_command, original_receipt);
    let b_record = OfflineMetadata::capture(&env, &mut b.nodes, 9);
    let c_record = OfflineMetadata::capture(&env, &mut c.nodes, 11);
    let parent_record = OfflineMetadata::capture(&env, &mut parent, 100);
    recover_owner(&env, &mut owners, &cache, owner_status);
    for record in [a_record, b_record, c_record, parent_record] {
        record.verify(&env);
    }
    creation::abandon(owners, 20);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_metadata_moves_recover_wal_and_serve_without_ancestors() {
    run(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_metadata_moves_recover_checkpoints_and_original_observations() {
    run(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_metadata_moves_recover_wal_and_serve_without_ancestors() {
    run(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_metadata_moves_recover_checkpoints_and_original_observations() {
    run(NativePeerProtocol::Quic, true)
}

#[path = "metadata_families.rs"]
mod families;

#[path = "metadata_lookup.rs"]
mod automatic_metadata;
