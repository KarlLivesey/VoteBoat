// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "preview/provider.rs"]
mod provider;
use std::collections::BTreeMap;
use voteboat::{
    membership::Configuration,
    placement::*,
    quorum::{Limits, Policy, Tree},
    transfer::*,
};
fn group(n: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n).unwrap(),
        incarnation: GroupIncarnation::new(7).unwrap(),
    }
}
fn intent() -> TransferIntent {
    let before = ResponsibilityManifest::new(ManifestInput {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(10).unwrap(),
            incarnation: ResponsibilityIncarnation::new(4).unwrap(),
        },
        parent: None,
        authority: group(9),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(77).unwrap(),
            version: 1,
        },
        scheme: HostPolicy.scheme(),
        scope: range(0, 256),
        epoch: OwnershipEpoch::new(2).unwrap(),
        generation: RouteGeneration::new(3).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(group(20)),
    })
    .unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(4).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(21)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(22)),
        },
    ]);
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
fn configuration() -> (Configuration, BTreeMap<NodeId, ReplicaPlacement>) {
    let replicas: BTreeMap<_, _> = (1..=3)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                ReplicaPlacement {
                    store: StoreIdentity {
                        id: StoreId::new(u128::from(n) + 400).unwrap(),
                        incarnation: StoreIncarnation::new(6).unwrap(),
                    },
                    domain: FailureDomainId::new(n).unwrap(),
                },
            )
        })
        .collect();
    let policy = Policy::new(
        Tree::Majority(
            replicas
                .keys()
                .map(|n| Tree::Majority(vec![Tree::Voter(*n)]))
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    let config = Configuration::new(
        ConfigurationId::new(8).unwrap(),
        policy,
        replicas.iter().map(|(&n, p)| (n, p.store)).collect(),
        BTreeMap::new(),
    )
    .unwrap();
    (config, replicas)
}
fn source<'a, A>(intent: &TransferIntent, app: &'a A) -> PreviewSource<'a, A> {
    PreviewSource {
        group: group(20),
        adapter: intent.before().input().application,
        application: app,
        export_bytes: 100000,
    }
}
fn targets<'a, A>(
    intent: &TransferIntent,
    apps: &'a [A; 2],
    config: &'a Configuration,
    replicas: &'a BTreeMap<NodeId, ReplicaPlacement>,
) -> [PreviewTarget<'a, A>; 2] {
    std::array::from_fn(|i| PreviewTarget {
        group: group(21 + i as u128),
        adapter: intent.before().input().application,
        application: &apps[i],
        configuration: config,
        replicas,
        import_bytes: 100000,
    })
}
fn exercise<A: ScopeStateMachine>(mut source_app: A, target_apps: [A; 2]) {
    source_app
        .apply_batch(&[entry(1, 5, bytes(1, 7, b"outbox"))])
        .unwrap();
    let before = source_app.checkpoint(100000).unwrap();
    let target_before: Vec<_> = target_apps
        .iter()
        .map(|a| a.checkpoint(100000).unwrap())
        .collect();
    let intent = intent();
    let (config, replicas) = configuration();
    let source = source(&intent, &source_app);
    let targets = targets(&intent, &target_apps, &config, &replicas);
    let p = preview_transfer(&intent, &[source], &targets, 100000).unwrap();
    assert_eq!(p.sources[0].observed_applied, 1);
    assert!(p.sources[0].retained_scopes.is_empty());
    assert_eq!(
        p.exports.iter().map(|e| e.scope).collect::<Vec<_>>(),
        vec![range(0, 128), range(128, 256)]
    );
    assert_eq!(
        p.payload_bytes,
        2 * source_app.export_scope_bound(range(0, 128)).unwrap()
    );
    assert_eq!(
        p.targets[0].replicas[0].placement.store.incarnation.get(),
        6
    );
    assert_eq!(
        p.intent_digest,
        ContentDigest::sha256(&intent.encode(32768).unwrap())
    );
    assert_eq!(source_app.checkpoint(100000).unwrap(), before);
    assert!(source_app.contains_operation(op(5)));
    for (a, b) in target_apps.iter().zip(target_before) {
        assert_eq!(a.checkpoint(100000).unwrap(), b);
    }
    assert_eq!(
        preview_transfer(&intent, &[], &targets, 100000),
        Err(PreviewError::Assignments)
    );
    assert_eq!(source_app.checkpoint(100000).unwrap(), before);
}
#[test]
fn preview_uses_native_application_and_host_scope_contract_without_mutation() {
    exercise(
        fresh(range(0, 256)),
        [fresh(range(0, 128)), fresh(range(128, 256))],
    );
    exercise(
        HostScope(fresh(range(0, 256))),
        [
            HostScope(fresh(range(0, 128))),
            HostScope(fresh(range(128, 256))),
        ],
    );
}
#[test]
fn preview_rejects_wrong_assignments_scope_schema_placement_and_budgets() {
    let intent = intent();
    let source_app = fresh(range(0, 256));
    let apps = [fresh(range(0, 128)), fresh(range(128, 256))];
    let (config, mut replicas) = configuration();
    let sources = [source(&intent, &source_app)];
    let bound = source_app.export_scope_bound(range(0, 128)).unwrap();
    assert_eq!(
        preview_transfer(
            &intent,
            &sources,
            &targets(&intent, &apps, &config, &replicas),
            2 * bound - 1
        ),
        Err(PreviewError::Budget)
    );
    let mut ts = targets(&intent, &apps, &config, &replicas);
    ts[0].import_bytes = bound - 1;
    assert_eq!(
        preview_transfer(&intent, &sources, &ts, 100000),
        Err(PreviewError::Budget)
    );
    ts[0].import_bytes = bound;
    ts[0].adapter.version = 9;
    assert_eq!(
        preview_transfer(&intent, &sources, &ts, 100000),
        Err(PreviewError::IncompatibleApplication(group(21)))
    );
    let wrong = [fresh(range(0, 127)), fresh(range(128, 256))];
    assert_eq!(
        preview_transfer(
            &intent,
            &sources,
            &targets(&intent, &wrong, &config, &replicas),
            100000
        ),
        Err(PreviewError::IncompatibleApplication(group(21)))
    );
    let mut duplicate = targets(&intent, &apps, &config, &replicas);
    duplicate[1].group = group(21);
    assert_eq!(
        preview_transfer(&intent, &sources, &duplicate, 100000),
        Err(PreviewError::Assignments)
    );
    replicas
        .values_mut()
        .for_each(|p| p.domain = FailureDomainId::new(1).unwrap());
    assert_eq!(
        preview_transfer(
            &intent,
            &sources,
            &targets(&intent, &apps, &config, &replicas),
            100000
        ),
        Err(PreviewError::Placement(
            group(21),
            PlacementError::TooFewVotingDomains
        ))
    );
}
#[test]
fn merge_sums_each_source_export_at_target_and_keeps_original_scopes() {
    let split = intent();
    let mut after = split.after().clone().into_input();
    after.epoch = OwnershipEpoch::new(4).unwrap();
    after.generation = RouteGeneration::new(5).unwrap();
    after.execution = ExecutionMode::Single(group(23));
    let merge = TransferIntent::new(
        split.after().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    let a = fresh(range(0, 128));
    let b = fresh(range(128, 256));
    let target_app = fresh(range(0, 256));
    let (config, replicas) = configuration();
    let sources = [
        PreviewSource {
            group: group(21),
            ..source(&merge, &a)
        },
        PreviewSource {
            group: group(22),
            ..source(&merge, &b)
        },
    ];
    let targets = [PreviewTarget {
        group: group(23),
        adapter: merge.before().input().application,
        application: &target_app,
        configuration: &config,
        replicas: &replicas,
        import_bytes: 100000,
    }];
    let p = preview_transfer(&merge, &sources, &targets, 100000).unwrap();
    assert_eq!(p.exports.len(), 2);
    assert_eq!(p.targets[0].payload_bytes, p.payload_bytes);
    assert_eq!(p.exports[0].scope, range(0, 128));
    assert_eq!(p.exports[1].scope, range(128, 256));
}
