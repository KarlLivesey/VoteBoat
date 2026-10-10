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
//! Imported owners retain provenance through later handoff and retirement.
use super::*;
use voteboat::retirement::*;
type Target = target_fixture::Target;
type Guard = RetirementGuard<Target>;

fn metadata() -> MetadataPublishingSource {
    let d = LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 48,
                history_bytes: 300000,
            },
        )
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("remaining profile")),
    );
    let bound = d.directory().readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(d, bound).unwrap_or_else(|_| panic!("metadata profile")),
    )
    .unwrap()
}
fn guard(first: &TransferIntent, partial: bool) -> Guard {
    RetirementGuard::new(imported_owner(first, partial)).unwrap_or_else(|_| panic!("guard profile"))
}
fn target(intent: &TransferIntent, g: u128, operation: u128) -> Target {
    let scope = intent
        .targets()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap()
        .scope;
    TransferTarget::new(
        group(g),
        source_fixture::op(operation),
        intent.clone(),
        BucketCounter::new(
            scope,
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|_| panic!("successor profile"))
}
fn query(manifest: &ResponsibilityManifest, g: u128, key: u8) -> TargetQuery<Vec<u8>> {
    let m = manifest.input();
    let scope = match &m.execution {
        ExecutionMode::Single(_) => m.scope,
        ExecutionMode::Partitioned(routes) | ExecutionMode::Delegated(routes) => {
            routes
                .iter()
                .find(|r| r.scope.contains(u16::from(key)))
                .unwrap()
                .scope
        }
    };
    TargetQuery::Data(RoutedQuery {
        hint: RouteHint {
            responsibility: m.responsibility,
            group: group(g),
            application: m.application,
            scheme: m.scheme,
            scope,
            bucket: u16::from(key),
            epoch: m.epoch,
            generation: m.generation,
        },
        key: vec![key],
        query: vec![key],
    })
}
fn data(manifest: &ResponsibilityManifest, g: u128, key: u8, delta: i64) -> Vec<u8> {
    let TargetQuery::Data(q) = query(manifest, g, key) else {
        unreachable!()
    };
    encode_routed(
        q.hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
struct Rig {
    authority: Vec<Node<MetadataPublishingSource>>,
    source: Vec<Node<RetainedOwner>>,
    owner: Vec<Node<Guard>>,
    first: TransferIntent,
    partial: bool,
    original_target: TargetStatus,
    original_image: ScopeImage,
    metadata_adoption: MetadataGrantStatus,
}
impl Rig {
    fn grant(&self) -> ResponsibilityManifest {
        self.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap()
            .grant()
            .clone()
    }
    fn owner_phase(
        &mut self,
        env: &Environment<'_>,
        id: u128,
        bytes: Vec<u8>,
        q: TargetQuery<Vec<u8>>,
    ) -> TargetRead<i64> {
        let RetirementRead::Owner(result) = phase(
            env,
            &mut self.owner,
            21,
            id,
            bytes,
            RetirementQuery::Owner(q),
            || guard(&self.first, self.partial),
        ) else {
            panic!("live owner")
        };
        result
    }
    fn verify_history(&mut self, env: &Environment<'_>) {
        assert_eq!(
            observe(
                &mut self.owner,
                env.clock,
                21,
                RetirementQuery::Owner(TargetQuery::Status)
            ),
            RetirementRead::Owner(TargetRead::Status(self.original_target.clone()))
        );
        assert_eq!(
            observe(
                &mut self.owner,
                env.clock,
                21,
                RetirementQuery::Owner(TargetQuery::MetadataAdoption(source_fixture::op(701)))
            ),
            RetirementRead::Owner(TargetRead::MetadataAdoption(Some(self.metadata_adoption)))
        );
        for node in &self.source {
            assert_eq!(
                node.local().applications[&group(20)]
                    .export(source_fixture::op(200), 65536)
                    .unwrap(),
                self.original_image
            );
        }
    }
}
#[path = "metadata_imported_setup.rs"]
mod initial;
#[path = "metadata_imported_transfer.rs"]
mod transfer;

fn retire(
    env: &Environment<'_>,
    rig: &mut Rig,
    handoff: &mut transfer::Handoff,
) -> (RetirementStatus, Vec<u8>, Vec<u8>) {
    let mut activations = Vec::new();
    for (g, nodes) in &mut handoff.targets {
        let TargetRead::Status(status) = observe(nodes, env.clock, *g, TargetQuery::Status) else {
            panic!("successor activation")
        };
        activations.push(TargetActivationEvidence::from_status(config(nodes, *g), status).unwrap());
    }
    let live = &rig.owner[0].local().applications[&group(21)];
    let lineage = live.owner().unwrap().retirement_lineage().unwrap();
    let frozen = live.freeze_status().unwrap().unwrap();
    let proof = RetirementProof {
        metadata_configuration: handoff.configuration,
        decision: handoff.decision.clone(),
        targets: activations,
        release: RetentionRelease {
            source: group(21),
            operation: source_fixture::op(3000),
            fence_index: frozen.fence.index,
            release: source_fixture::op(9900),
        },
    };
    let mut incomplete = proof.clone();
    incomplete.targets.clear();
    assert!(live
        .retirement_command(&incomplete, MAX_RETIREMENT_COMMAND_BYTES)
        .is_err());
    let bytes = live
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    // Even checkpoint histories first recover the retirement from the live WAL tail.
    let wal = Environment {
        checkpoint: false,
        ..*env
    };
    let RetirementRead::Status(Some(status)) = phase(
        &wal,
        &mut rig.owner,
        21,
        3000,
        bytes.clone(),
        RetirementQuery::Status,
        || guard(&rig.first, rig.partial),
    ) else {
        panic!("retired")
    };
    for node in &rig.owner {
        let log = node.local().owner.core(group(21)).unwrap().state();
        assert!(log.base_index() < status.index);
        assert!(log.entries.iter().any(|entry| entry.index == status.index
            && matches!(entry.payload, EntryPayload::Command { .. })));
        let retired = &node.local().applications[&group(21)];
        assert!(retired.owner().is_none());
        assert_eq!(retired.retired_lineage(), Some(lineage.as_slice()));
        assert_eq!(retired.freeze_status().unwrap(), Some(frozen.clone()));
        assert!(retired.export_target(group(40), 65536).is_err());
    }
    assert_eq!(
        observe(
            &mut rig.owner,
            env.clock,
            21,
            RetirementQuery::Owner(TargetQuery::Status)
        ),
        RetirementRead::Retired
    );
    assert!(rig.owner[0]
        .propose(ClientRequest {
            group: group(21),
            operation: source_fixture::op(9999),
            bytes: data(handoff.decision.publication.intent().before(), 21, 100, 1)
        })
        .is_err());
    (status, bytes, lineage)
}
fn reclaim(
    env: &Environment<'_>,
    rig: &mut Rig,
    status: RetirementStatus,
    bytes: Vec<u8>,
    lineage: &[u8],
) {
    compact(&mut rig.owner, env.clock, 21);
    let tickets: Vec<_> = rig
        .owner
        .iter_mut()
        .map(|n| n.reclaim(LogLimits::default().max_wal_bytes).unwrap())
        .collect();
    let mut done = [false; 3];
    drive(&mut rig.owner, env.clock, |nodes| {
        for (j, node) in nodes.iter_mut().enumerate() {
            if let Some(result) = node.poll_reclaim() {
                assert_eq!(result.request, tickets[j]);
                let report = result.result.unwrap();
                assert!(report.after_bytes < report.before_bytes);
                done[j] = true;
            }
        }
        done.iter().all(|x| *x)
    });
    let logs = creation::abandon(std::mem::take(&mut rig.owner), 21);
    for log in logs.values() {
        assert!(log.base_index() >= status.index);
        assert!(log
            .entries
            .iter()
            .all(|e| !matches!(e.payload, EntryPayload::Command { .. })));
    }
    rig.owner = open(
        configuration(env.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        env.clock,
        env.protocol,
        || guard(&rig.first, rig.partial),
    );
    assert_eq!(
        observe(&mut rig.owner, env.clock, 21, RetirementQuery::Status),
        RetirementRead::Status(Some(status))
    );
    assert_eq!(
        propose_recovering(&mut rig.owner, env.clock, 21, 3000, bytes).outcome,
        RetirementOutcome::Retired(status)
    );
    for node in &rig.owner {
        assert_eq!(
            node.local().applications[&group(21)].retired_lineage(),
            Some(lineage)
        );
    }
}
fn independent_writes(
    env: &Environment<'_>,
    handoff: &mut transfer::Handoff,
    child: &mut Option<transfer::Handoff>,
) {
    let writes = if child.is_some() {
        [(40, 100, 44, 9), (30, 1, 1, 7)]
    } else {
        [(40, 1, 1, 7), (41, 100, 44, 9)]
    };
    for (g, key, id, value) in writes {
        let selected = if g == 30 {
            child.as_mut().unwrap()
        } else {
            &mut *handoff
        };
        let nodes = selected.targets.get_mut(&g).unwrap();
        let m = nodes[0].local().applications[&group(g)].grant().clone();
        assert_eq!(
            phase(
                env,
                nodes,
                g,
                id,
                data(&m, g, key, value),
                query(&m, g, key),
                || target(&selected.intent, g, selected.operation)
            ),
            TargetRead::Data(value)
        );
        assert_eq!(
            phase(
                env,
                nodes,
                g,
                9001,
                data(&m, g, key, 2),
                query(&m, g, key),
                || target(&selected.intent, g, selected.operation)
            ),
            TargetRead::Data(value + 2)
        );
        for node in nodes {
            assert_eq!(
                node.local().applications[&group(g)]
                    .application()
                    .outbox()
                    .count(),
                2
            );
        }
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool, partial: bool) {
    let _lock = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-imported-retirement-{}-{protocol:?}-{checkpoint}-{partial}",
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
    let (mut rig, mut moved) = initial::build(&env, partial);
    let old = OfflineMetadata::capture(&env, &mut rig.authority, 1);
    let mut child = if partial {
        Some(transfer::delegate(&env, &mut rig, &mut moved))
    } else {
        None
    };
    let mut handoff = transfer::remaining(&env, &mut rig, &mut moved);
    if let Some(child) = &child {
        transfer::verify_scoped(&rig, child);
    }
    rig.verify_history(&env);
    let metadata = OfflineMetadata::capture(&env, &mut moved.nodes, 9);
    let (status, bytes, lineage) = retire(&env, &mut rig, &mut handoff);
    if checkpoint {
        reclaim(&env, &mut rig, status, bytes, &lineage);
    }
    independent_writes(&env, &mut handoff, &mut child);
    old.verify(&env);
    metadata.verify(&env);
    creation::abandon(rig.owner, 21);
    creation::abandon(rig.source, 20);
    for (g, nodes) in handoff.targets {
        creation::abandon(nodes, g);
    }
    if let Some(child) = child {
        for (g, nodes) in child.targets {
            creation::abandon(nodes, g);
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_imported_retirement_wal() {
    for partial in [false, true] {
        history(NativePeerProtocol::TcpTls, false, partial);
    }
}
#[test]
fn tcp_imported_retirement_checkpoint() {
    for partial in [false, true] {
        history(NativePeerProtocol::TcpTls, true, partial);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_imported_retirement_wal() {
    for partial in [false, true] {
        history(NativePeerProtocol::Quic, false, partial);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_imported_retirement_checkpoint() {
    for partial in [false, true] {
        history(NativePeerProtocol::Quic, true, partial);
    }
}
