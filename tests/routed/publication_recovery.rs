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
    bucket_counter::{encode_add, BucketOutcome},
    transfer::*,
    transfer_publication::*,
    transfer_source::*,
    transfer_target::*,
};
struct Environment {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    compact: bool,
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 3,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
fn bootstrap_metadata(parents: &mut [Node<LifecycleDirectory>], clock: &Instant) {
    campaign(parents, clock, 1);
    propose_recovering(
        parents,
        clock,
        1,
        1000,
        metadata().directory().bootstrap_command(65536).unwrap(),
    );
    propose_recovering(
        parents,
        clock,
        1,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: source_fixture::grant(),
        }
        .encode(32768)
        .unwrap(),
    );
    propose_recovering(
        parents,
        clock,
        1,
        200,
        source_fixture::intent().encode(32768).unwrap(),
    );
    assert!(parents.iter().all(|p| p.local().applications[&group(1)]
        .directory()
        .remaining_operations()
        == 0));
}
fn bootstrap_targets(
    left: &mut [Node<target_fixture::Target>],
    right: &mut [Node<target_fixture::Target>],
    clock: &Instant,
) {
    campaign(left, clock, 21);
    propose_recovering(
        left,
        clock,
        21,
        200,
        target_fixture::fresh().bootstrap_command(65536).unwrap(),
    );
    campaign(right, clock, 22);
    propose_recovering(
        right,
        clock,
        22,
        200,
        target_fixture::fresh_for(22)
            .bootstrap_command(65536)
            .unwrap(),
    );
}
fn freeze_source(
    sources: &mut [Node<source_fixture::Source>],
    clock: &Instant,
) -> (ConfigurationId, SourceFenceEvidence) {
    campaign(sources, clock, 20);
    propose_recovering(
        sources,
        clock,
        20,
        100,
        source_fixture::fresh().bootstrap_command(65536).unwrap(),
    );
    propose_recovering(sources, clock, 20, 1, source_fixture::data(1, 7));
    propose_recovering(sources, clock, 20, 2, source_fixture::data(200, 11));
    propose_recovering(sources, clock, 20, 200, source_fixture::freeze());
    let SourceRead::Freeze(Some(source_status)) =
        read_recovering(sources, clock, 20, SourceQuery::Freeze)
    else {
        panic!("source status")
    };
    let source_configuration = sources[0]
        .local()
        .owner
        .core(group(20))
        .unwrap()
        .state()
        .bootstrap
        .configuration;
    let source_evidence = SourceFenceEvidence::from_status(source_configuration, source_status)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    (source_configuration, source_evidence)
}
fn prepare_publication(
    sources: &mut [Node<source_fixture::Source>],
    left: &mut [Node<target_fixture::Target>],
    right: &mut [Node<target_fixture::Target>],
    clock: &Instant,
) -> TransferPublication {
    let (source_configuration, source_evidence) = freeze_source(sources, clock);
    let mut targets = Vec::new();
    for (nodes, g) in [(left, 21u128), (right, 22u128)] {
        campaign(nodes, clock, g);
        let import = target_fixture::from_source(
            &sources[0].local().applications[&group(20)],
            g,
            source_configuration,
        );
        propose_recovering(
            nodes,
            clock,
            g,
            200,
            target_fixture::fresh_for(g)
                .import_command(&import, 65536)
                .unwrap(),
        );
        let TargetRead::Status(status) = read_recovering(nodes, clock, g, TargetQuery::Status)
        else {
            panic!("target status")
        };
        let configuration = nodes[0]
            .local()
            .owner
            .core(group(g))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        targets.push(
            TargetReadyEvidence::from_status(configuration, status)
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
        assert_eq!(
            read_recovering(
                nodes,
                clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: source_fixture::hint(if g == 21 { 1 } else { 200 }),
                    key: vec![if g == 21 { 1 } else { 200 }],
                    query: vec![if g == 21 { 1 } else { 200 }]
                })
            ),
            TargetRead::NotActive
        );
    }
    TransferPublication::new(
        OperationId::new(200).unwrap(),
        source_fixture::intent(),
        vec![source_evidence],
        targets,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn publish_and_reopen(
    env: &Environment,
    mut parents: Vec<Node<LifecycleDirectory>>,
    left: &mut [Node<target_fixture::Target>],
    right: &mut [Node<target_fixture::Target>],
    publication: TransferPublication,
) -> (Vec<Node<LifecycleDirectory>>, TransferPublicationStatus) {
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    // Driving the other groups can leave metadata timers unpolled long enough
    // for leadership to change. Establish the submitting node's authority
    // explicitly rather than assuming the earlier campaign still holds.
    campaign(&mut parents, &env.clock, 1);
    let _ = propose_recovering(&mut parents, &env.clock, 1, 201, bytes.clone()); // discard the publication observation
    let DirectoryRead::Publication(Some(original)) = read_recovering(
        &mut parents,
        &env.clock,
        1,
        DirectoryQuery::Publication(OperationId::new(200).unwrap()),
    ) else {
        panic!("publication status")
    };
    assert_eq!(original.publication, publication);
    assert_eq!(
        read_recovering(
            &mut parents,
            &env.clock,
            1,
            DirectoryQuery::Manifest(source_fixture::grant().input().responsibility)
        ),
        DirectoryRead::Manifest(Some(source_fixture::intent().after().clone()))
    );
    if env.compact {
        for node in &mut parents {
            node.control(group(1), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut parents, &env.clock, |ns| {
            ns.iter().all(|n| {
                n.local().owner.core(group(1)).unwrap().state().base_index()
                    == n.local().applications[&group(1)].applied_index()
            })
        });
    }
    close(parents, &env.clock, 1, || {
        drive(left, &env.clock, |_| true);
        drive(right, &env.clock, |_| true);
    });
    let mut parents = open(
        configuration(&env.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &env.clock,
        env.protocol,
        metadata,
    );
    campaign(&mut parents, &env.clock, 1);
    let retry = propose_recovering(&mut parents, &env.clock, 1, 201, bytes);
    assert!(retry.duplicate);
    assert_eq!(
        retry.outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(
        read_recovering(
            &mut parents,
            &env.clock,
            1,
            DirectoryQuery::Publication(OperationId::new(200).unwrap())
        ),
        DirectoryRead::Publication(Some(original.clone()))
    );
    (parents, original)
}
fn verify_inactive_targets(
    left: &mut [Node<target_fixture::Target>],
    right: &mut [Node<target_fixture::Target>],
    clock: &Instant,
) {
    for (nodes, g, key) in [(left, 21u128, 1u8), (right, 22u128, 200u8)] {
        campaign(nodes, clock, g);
        assert_eq!(
            read_recovering(
                nodes,
                clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: source_fixture::hint(key),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::NotActive
        );
    }
}
fn begin_activation(
    parents: &[Node<LifecycleDirectory>],
    left: &mut [Node<target_fixture::Target>],
    right: &mut [Node<target_fixture::Target>],
    clock: &Instant,
    original: TransferPublicationStatus,
) -> (Vec<u8>, ActivationStatus) {
    let activation = TargetActivation {
        metadata_configuration: parents[0]
            .local()
            .owner
            .core(group(1))
            .unwrap()
            .state()
            .bootstrap
            .configuration,
        decision: original.clone(),
    };
    let command = left[0].local().applications[&group(21)]
        .activation_command(&activation, 65536)
        .unwrap();
    campaign(left, clock, 21);
    let receipt = propose_recovering(left, clock, 21, 200, command.clone());
    let TargetOutcome::Activated(activated) = receipt.outcome else {
        panic!("activation receipt")
    };
    // Right remains inactive while left has durable independent authority.
    campaign(right, clock, 22);
    assert_eq!(
        read_recovering(
            right,
            clock,
            22,
            TargetQuery::Data(RoutedQuery {
                hint: source_fixture::hint(200),
                key: vec![200],
                query: vec![200]
            })
        ),
        TargetRead::NotActive
    );
    (command, activated)
}
fn activated_hint() -> RouteHint {
    let mut hint = source_fixture::hint(1);
    hint.group = group(21);
    hint.scope = source_fixture::range(0, 128);
    hint.epoch = OwnershipEpoch::new(2).unwrap();
    hint.generation = RouteGeneration::new(2).unwrap();
    hint
}
fn data(hint: RouteHint, delta: i64) -> Vec<u8> {
    voteboat::routed::encode_routed(
        hint,
        &[1],
        &encode_add(&[1], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn exercise_activated(
    left: &mut [Node<target_fixture::Target>],
    clock: &Instant,
    hint: RouteHint,
    compact: bool,
) {
    campaign(left, clock, 21);
    let TargetOutcome::Applied(retry) =
        propose_recovering(left, clock, 21, 1, data(hint, 7)).outcome
    else {
        panic!("imported retry")
    };
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, BucketOutcome::Value(7));
    let TargetOutcome::Applied(write) =
        propose_recovering(left, clock, 21, 3, data(hint, 2)).outcome
    else {
        panic!("new write")
    };
    assert_eq!(write.outcome, BucketOutcome::Value(9));
    if compact {
        for node in left.iter_mut() {
            node.control(group(21), NodeControl::Checkpoint).unwrap();
        }
        drive(left, clock, |ns| {
            ns.iter().all(|n| {
                n.local()
                    .owner
                    .core(group(21))
                    .unwrap()
                    .state()
                    .base_index()
                    == n.local().applications[&group(21)].applied_index()
            })
        });
    }
}
fn verify_recovered_activation(
    left: &mut [Node<target_fixture::Target>],
    clock: &Instant,
    command: Vec<u8>,
    activated: ActivationStatus,
    hint: RouteHint,
) {
    campaign(left, clock, 21);
    assert_eq!(
        propose_recovering(left, clock, 21, 200, command).outcome,
        TargetOutcome::Activated(activated)
    );
    assert_eq!(
        read_recovering(
            left,
            clock,
            21,
            TargetQuery::Data(RoutedQuery {
                hint,
                key: vec![1],
                query: vec![1]
            })
        ),
        TargetRead::Data(9)
    );
    let TargetOutcome::Applied(retry) =
        propose_recovering(left, clock, 21, 3, data(hint, 2)).outcome
    else {
        panic!("recovered write retry")
    };
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, BucketOutcome::Value(9));
    assert!(left.iter().all(|n| n.local().applications[&group(21)]
        .application()
        .outbox()
        .count()
        == 2));
}
fn verify_reopened_source(env: &Environment, left: &mut [Node<target_fixture::Target>]) {
    // Reopening the old source cannot restore its old serving authority.
    let mut sources = open(
        configuration(&env.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &env.clock,
        env.protocol,
        source_fixture::fresh,
    );
    campaign(&mut sources, &env.clock, 20);
    assert_eq!(
        read_recovering(
            &mut sources,
            &env.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1]
            })
        ),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert!(sources[0]
        .propose(ClientRequest {
            group: group(20),
            operation: OperationId::new(4).unwrap(),
            bytes: source_fixture::data(1, 1)
        })
        .is_err());
    close(sources, &env.clock, 20, || {
        drive(left, &env.clock, |_| true);
    });
}
fn activate_and_recover(
    env: &Environment,
    parents: Vec<Node<LifecycleDirectory>>,
    mut left: Vec<Node<target_fixture::Target>>,
    mut right: Vec<Node<target_fixture::Target>>,
    original: TransferPublicationStatus,
) {
    let (command, activated) =
        begin_activation(&parents, &mut left, &mut right, &env.clock, original);
    close(parents, &env.clock, 1, || {
        drive(&mut left, &env.clock, |_| true);
        drive(&mut right, &env.clock, |_| true);
    });
    let hint = activated_hint();
    exercise_activated(&mut left, &env.clock, hint, env.compact);
    close(left, &env.clock, 21, || {
        drive(&mut right, &env.clock, |_| true);
    });
    close(right, &env.clock, 22, || {});
    let mut left = open(
        configuration(&env.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        &env.clock,
        env.protocol,
        target_fixture::fresh,
    );
    verify_recovered_activation(&mut left, &env.clock, command, activated, hint);
    verify_reopened_source(env, &mut left);
    campaign(&mut left, &env.clock, 21);
    assert_eq!(
        read_recovering(
            &mut left,
            &env.clock,
            21,
            TargetQuery::Data(RoutedQuery {
                hint,
                key: vec![1],
                query: vec![1]
            })
        ),
        TargetRead::Data(9)
    );
    close(left, &env.clock, 21, || {});
}
fn verify_inactive_reopen(
    env: &Environment,
    mut parents: Vec<Node<LifecycleDirectory>>,
    left: Vec<Node<target_fixture::Target>>,
    mut right: Vec<Node<target_fixture::Target>>,
    original: TransferPublicationStatus,
) {
    close(left, &env.clock, 21, || {
        drive(&mut parents, &env.clock, |_| true);
        drive(&mut right, &env.clock, |_| true);
    });
    close(right, &env.clock, 22, || {
        drive(&mut parents, &env.clock, |_| true);
    });
    close(parents, &env.clock, 1, || {});
    let parents = open(
        configuration(&env.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &env.clock,
        env.protocol,
        metadata,
    );
    for parent in &parents {
        assert_eq!(
            parent.local().applications[&group(1)]
                .directory()
                .transfer_publication_at(
                    parent.local().applications[&group(1)].applied_index(),
                    OperationId::new(200).unwrap()
                )
                .unwrap(),
            Some(original.clone())
        );
    }
    close(parents, &env.clock, 1, || {});
}
pub(super) fn run(protocol: NativePeerProtocol, compact: bool, activate: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let env = Environment {
        root: std::env::temp_dir().join(format!(
            "voteboat-publication-{}-{protocol:?}-{compact}-{activate}",
            std::process::id()
        )),
        clock: Instant::now(),
        protocol,
        compact,
    };
    std::fs::create_dir_all(&env.root).unwrap();
    let mut parents = open(
        configuration(&env.root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &env.clock,
        env.protocol,
        metadata,
    );
    let mut sources = open(
        configuration(&env.root, 20, &[1, 2, 3], NativeOpenMode::Create),
        &env.clock,
        env.protocol,
        source_fixture::fresh,
    );
    let mut left = open(
        configuration(&env.root, 21, &[1, 2, 3], NativeOpenMode::Create),
        &env.clock,
        env.protocol,
        target_fixture::fresh,
    );
    let mut right = open(
        configuration(&env.root, 22, &[1, 2, 3], NativeOpenMode::Create),
        &env.clock,
        env.protocol,
        || target_fixture::fresh_for(22),
    );
    bootstrap_metadata(&mut parents, &env.clock);
    bootstrap_targets(&mut left, &mut right, &env.clock);
    let publication = prepare_publication(&mut sources, &mut left, &mut right, &env.clock);
    // The source can be offline after its durable fence; no parent write can revive it.
    close(sources, &env.clock, 20, || {
        drive(&mut parents, &env.clock, |_| true);
        drive(&mut left, &env.clock, |_| true);
        drive(&mut right, &env.clock, |_| true);
    });
    let (parents, original) = publish_and_reopen(&env, parents, &mut left, &mut right, publication);
    verify_inactive_targets(&mut left, &mut right, &env.clock);
    if activate {
        activate_and_recover(&env, parents, left, right, original);
    } else {
        verify_inactive_reopen(&env, parents, left, right, original);
    }
    std::fs::remove_dir_all(&env.root).unwrap();
}
