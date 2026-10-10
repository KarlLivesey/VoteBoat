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
use voteboat::{group_creation::*, native::group_creation::*};
fn metadata(parent: bool) -> LifecycleDirectory {
    LifecycleDirectory::new(
        fixture::fresh_directory(parent, 32)
            .into_directory()
            .with_cross_authority_insertion()
            .unwrap_or_else(|_| panic!("schema7")),
    )
}
fn parent_metadata() -> LifecycleDirectory {
    metadata(true)
}
fn child_metadata() -> LifecycleDirectory {
    metadata(false)
}
pub(super) fn hint(m: &ResponsibilityManifest, key: u8) -> RouteHint {
    let m = m.input();
    let ExecutionMode::Single(g) = m.execution else {
        panic!("child owner");
    };
    RouteHint {
        responsibility: m.responsibility,
        group: g,
        application: m.application,
        scheme: m.scheme,
        scope: m.scope,
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    }
}
struct Cross {
    rig: Delegated,
    creations: [GroupCreationStatus; 2],
    bindings: BTreeMap<(u128, NodeId), Vec<u8>>,
}
impl Cross {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let mut rig =
            Delegated::with_metadata(protocol, checkpoint, parent_metadata, child_metadata);
        rig.cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 6,
            bytes: 65536,
        })
        .unwrap();
        let configurations: [Vec<NativeStartup>; 2] = std::array::from_fn(|i| {
            configuration(
                &rig.root,
                21 + i as u128,
                &[1, 2, 3],
                NativeOpenMode::Recover,
            )
        });
        let creations =
            std::array::from_fn(|i| Self::reserve_creation(&mut rig, &configurations[i], i));
        for c in &configurations[0][..2] {
            establish(&rig.child, c, &creations[0]);
        }
        assert!(!configurations[0][2].directory.exists());
        assert!(!configurations[1][0].directory.exists());
        // Reopen the actual child authority before finishing its original assignments.
        if checkpoint {
            compact(&mut rig.child, &rig.clock, 1);
        }
        creation::abandon(std::mem::take(&mut rig.child), 1);
        rig.child = open(
            configuration(&rig.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &rig.clock,
            protocol,
            child_metadata,
        );
        let mut bindings = BTreeMap::new();
        let mut children = Vec::new();
        for i in 0..2 {
            campaign(&mut rig.child, &rig.clock, 1);
            assert!(
                propose_recovering(
                    &mut rig.child,
                    &rig.clock,
                    1,
                    1002 + i as u128,
                    creations[i].intent.encode(100000).unwrap()
                )
                .duplicate
            );
            let _ = manifest(
                &mut rig.child,
                &rig.clock,
                1,
                fixture::before().input().responsibility,
            );
            for c in &configurations[i] {
                bindings.insert(
                    (21 + i as u128, c.node),
                    establish(&rig.child, c, &creations[i]),
                );
            }
            let mut m = fixture::before().into_input();
            m.responsibility = fixture::id(21 + i as u128);
            m.parent = Some(ParentAuthority {
                responsibility: fixture::before().input().responsibility,
                group: group(1),
            });
            m.scope =
                source_fixture::range(if i == 0 { 0 } else { 128 }, if i == 0 { 128 } else { 256 });
            m.execution = ExecutionMode::Single(group(21 + i as u128));
            children.push(
                InsertionChild::from_creation(
                    ResponsibilityManifest::new(m).unwrap(),
                    &creations[i],
                )
                .unwrap(),
            );
        }
        rig.insertion_plan = Some(Self::insertion_plan(children, rig.transfer_operation));
        Self {
            rig,
            creations,
            bindings,
        }
    }
    fn reserve_creation(
        rig: &mut Delegated,
        configurations: &[NativeStartup],
        i: usize,
    ) -> GroupCreationStatus {
        campaign(&mut rig.child, &rig.clock, 1);
        let c = GroupCreationIntent {
            authority: group(1),
            parent: fixture::before().input().responsibility,
            expected: fixture::before().input().generation,
            responsibility: fixture::id(21 + i as u128),
            bootstrap: configurations[0].bootstrap.clone(),
            application: fixture::before().input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            propose_recovering(
                &mut rig.child,
                &rig.clock,
                1,
                1002 + i as u128,
                c.encode(100000).unwrap()
            )
            .outcome,
            DirectoryOutcome::CreationReserved
        );
        let _ = manifest(
            &mut rig.child,
            &rig.clock,
            1,
            fixture::before().input().responsibility,
        );
        let core = rig.child[0].local().owner.core(group(1)).unwrap();
        rig.child[0].local().applications[&group(1)]
            .directory()
            .group_creation_at(core.state().commit_index, group(21 + i as u128))
            .unwrap()
            .unwrap()
    }
    fn insertion_plan(children: Vec<InsertionChild>, transfer_operation: u128) -> DelegationPlan {
        let mut after = fixture::before().into_input();
        after.epoch = OwnershipEpoch::new(2).unwrap();
        after.generation = RouteGeneration::new(2).unwrap();
        after.execution = ExecutionMode::Delegated(
            children
                .iter()
                .map(|c| RouteEntry {
                    scope: c.manifest.input().scope,
                    target: RouteTarget::Child(ChildAuthority {
                        responsibility: c.manifest.input().responsibility,
                        group: group(1),
                        epoch: c.manifest.input().epoch,
                    }),
                })
                .collect(),
        );

        DelegationPlan::cross_authority_insertion(
            fixture::parent(),
            fixture::before(),
            ResponsibilityManifest::new(after).unwrap(),
            children,
            OperationId::new(transfer_operation).unwrap(),
        )
        .unwrap()
    }
    fn retry_metadata(
        &mut self,
        phase: Phase,
        state: &Observed,
        intent: &TransferIntent,
        operation: u128,
    ) {
        let (g, id, bytes) = match phase {
            Phase::Reserve => (
                100,
                self.rig.reservation_operation,
                state
                    .reservation
                    .as_ref()
                    .unwrap()
                    .plan
                    .encode(65536)
                    .unwrap(),
            ),
            Phase::Intent => (1, operation, intent.encode(65536).unwrap()),
            Phase::ChildPublication => (
                1,
                operation + 1,
                state
                    .decision
                    .as_ref()
                    .unwrap()
                    .publication
                    .encode(65536)
                    .unwrap(),
            ),
            Phase::ParentPublication => (
                100,
                self.rig.reservation_operation + 1,
                state
                    .parent_publication
                    .as_ref()
                    .unwrap()
                    .completion
                    .encode(MAX_DELEGATION_COMPLETION_BYTES)
                    .unwrap(),
            ),
            _ => unreachable!(),
        };
        let nodes = if g == 100 {
            &mut self.rig.parent
        } else {
            &mut self.rig.child
        };
        campaign(nodes, &self.rig.clock, g);
        assert!(propose_recovering(nodes, &self.rig.clock, g, id, bytes).duplicate);
    }
    fn serve_targets(
        &mut self,
        intent: &TransferIntent,
        decision: &TransferPublicationStatus,
        child_cfg: ConfigurationId,
    ) {
        let checkpoint = self.rig.checkpoint;
        let protocol = self.rig.protocol;
        for (i, g, key, operation, value) in [(0, 21, 1, 1, 7), (1, 22, 200, 2, 11)] {
            let m = intent.target_manifest(group(g)).unwrap();
            let h = hint(m, key);
            assert_eq!(
                observe(
                    &mut self.rig.targets[i],
                    &self.rig.clock,
                    g,
                    TargetQuery::Data(RoutedQuery {
                        hint: h,
                        key: vec![key],
                        query: vec![key]
                    })
                ),
                TargetRead::NotActive
            );
            let bytes = self.rig.targets[i][0].local().applications[&group(g)]
                .activation_command(
                    &TargetActivation {
                        metadata_configuration: child_cfg,
                        decision: decision.clone(),
                    },
                    65536,
                )
                .unwrap();
            campaign(&mut self.rig.targets[i], &self.rig.clock, g);
            phase_write(
                true,
                &mut self.rig.targets[i],
                &self.rig.clock,
                g,
                self.rig.transfer_operation,
                bytes.clone(),
            );
            let TargetRead::Status(activated) = observe(
                &mut self.rig.targets[i],
                &self.rig.clock,
                g,
                TargetQuery::Status,
            ) else {
                panic!("activated status");
            };
            if checkpoint {
                compact(&mut self.rig.targets[i], &self.rig.clock, g);
            }
            creation::abandon(std::mem::take(&mut self.rig.targets[i]), g);
            self.rig.targets[i] = open(
                configuration(&self.rig.root, g, &[1, 2, 3], NativeOpenMode::Recover),
                &self.rig.clock,
                protocol,
                || fixture::fresh_target_for(g, intent, self.rig.transfer_operation),
            );
            campaign(&mut self.rig.targets[i], &self.rig.clock, g);
            assert!(
                matches!(propose_recovering(&mut self.rig.targets[i],&self.rig.clock,g,
            self.rig.transfer_operation,bytes).outcome,TargetOutcome::Activated(v) if Some(v)==activated.activated)
            );
            let write = |delta| {
                encode_routed(
                    h,
                    &[key],
                    &encode_add(&[key], delta, b"effect", 1024).unwrap(),
                    4096,
                )
                .unwrap()
            };
            assert!(
                matches!(propose_recovering(&mut self.rig.targets[i],&self.rig.clock,g,operation,write(value)).outcome,
            TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(value))
            );
            assert_eq!(
                self.rig.targets[i][0].local().applications[&group(g)]
                    .application()
                    .outbox()
                    .count(),
                1
            );
            assert!(
                matches!(propose_recovering(&mut self.rig.targets[i],&self.rig.clock,g,100+operation,write(2)).outcome,
            TargetOutcome::Applied(v) if !v.duplicate&&v.outcome==BucketOutcome::Value(value+2))
            );
            assert_eq!(
                observe(
                    &mut self.rig.targets[i],
                    &self.rig.clock,
                    g,
                    TargetQuery::Data(RoutedQuery {
                        hint: h,
                        key: vec![key],
                        query: vec![key]
                    })
                ),
                TargetRead::Data(value + 2)
            );
        }
    }
    fn observed(&mut self) -> (Observed, [Option<ResponsibilityManifest>; 2]) {
        let DirectoryRead::DelegationReservation(reservation) = observe(
            &mut self.rig.parent,
            &self.rig.clock,
            100,
            DirectoryQuery::DelegationReservation(
                OperationId::new(self.rig.reservation_operation).unwrap(),
            ),
        ) else {
            panic!("selected parent reservation");
        };
        if let Some(reservation) = reservation {
            let cfg = self.rig.parent[0]
                .local()
                .owner
                .core(group(100))
                .unwrap()
                .state()
                .bootstrap
                .configuration;
            let intent = reservation.child_intent(cfg).unwrap();
            for i in 0..2 {
                if self.rig.targets[i].is_empty() {
                    let g = 21 + i as u128;
                    self.rig.targets[i] = open(
                        configuration(&self.rig.root, g, &[1, 2, 3], NativeOpenMode::Recover),
                        &self.rig.clock,
                        self.rig.protocol,
                        || fixture::fresh_target_for(g, &intent, self.rig.transfer_operation),
                    );
                }
            }
        }
        let state = self.rig.observed();
        let children = std::array::from_fn(|i| {
            let DirectoryRead::Manifest(m) = observe(
                &mut self.rig.child,
                &self.rig.clock,
                1,
                DirectoryQuery::Manifest(fixture::id(21 + i as u128)),
            ) else {
                panic!("grandchild manifest");
            };
            let core = self.rig.child[0].local().owner.core(group(1)).unwrap();
            assert_eq!(
                self.rig.child[0].local().applications[&group(1)]
                    .directory()
                    .group_creation_at(core.state().commit_index, group(21 + i as u128))
                    .unwrap()
                    .as_ref(),
                Some(&self.creations[i])
            );
            if state.decision.is_some() {
                assert_eq!(
                    m.as_ref(),
                    Some(
                        &self
                            .rig
                            .insertion_plan
                            .as_ref()
                            .unwrap()
                            .insertion_children()
                            .unwrap()[i]
                            .manifest
                    )
                );
            } else {
                assert!(m.is_none());
            }
            m
        });
        (state, children)
    }
    fn check_bindings(&self) {
        for ((g, node), expected) in &self.bindings {
            let mut b =
                FileCreationBindings::open(self.rig.root.join(format!("{g}/{}", node.get())))
                    .unwrap();
            assert_eq!(b.load().unwrap().as_ref(), Some(expected));
        }
    }
    fn retry(&mut self, phase: Phase, state: &Observed) {
        let operation = self.rig.transfer_operation;
        let intent = state
            .reservation
            .as_ref()
            .unwrap()
            .child_intent(
                self.rig.parent[0]
                    .local()
                    .owner
                    .core(group(100))
                    .unwrap()
                    .state()
                    .bootstrap
                    .configuration,
            )
            .unwrap();
        match phase {
            Phase::Reserve | Phase::Intent | Phase::ChildPublication | Phase::ParentPublication => {
                self.retry_metadata(phase, state, &intent, operation);
            }
            Phase::Stage(g) | Phase::Import(g) => {
                let i = (g - 21) as usize;
                let bytes = if matches!(phase, Phase::Stage(_)) {
                    self.rig.targets[i][0].local().applications[&group(g)]
                        .bootstrap_command(65536)
                        .unwrap()
                } else {
                    let imported = &state.targets[i]
                        .as_ref()
                        .unwrap()
                        .imported
                        .as_ref()
                        .unwrap()
                        .sources[0];
                    let import = TargetImport::new(
                        OperationId::new(operation).unwrap(),
                        intent,
                        group(g),
                        vec![SourceImport {
                            fence: imported.fence,
                            configuration: imported.configuration,
                            digest: imported.digest,
                            image: self.rig.source[0].local().applications[&group(20)]
                                .export_target(group(g), 65536)
                                .unwrap(),
                        }],
                    )
                    .unwrap_or_else(|e| panic!("{:?}", e.0));
                    self.rig.targets[i][0].local().applications[&group(g)]
                        .import_command(&import, 65536)
                        .unwrap()
                };
                campaign(&mut self.rig.targets[i], &self.rig.clock, g);
                let receipt = propose_recovering(
                    &mut self.rig.targets[i],
                    &self.rig.clock,
                    g,
                    operation,
                    bytes,
                );
                match receipt.outcome {
                    TargetOutcome::Staged { index } => {
                        assert_eq!(Some(index), state.targets[i].as_ref().unwrap().staged_index)
                    }
                    TargetOutcome::Imported { index, digest } => {
                        let original = state.targets[i]
                            .as_ref()
                            .unwrap()
                            .imported
                            .as_ref()
                            .unwrap();
                        assert_eq!((index, digest), (original.index, original.digest));
                    }
                    other => panic!("target phase retry: {other:?}"),
                }
            }
            Phase::Fence => {
                campaign(&mut self.rig.source, &self.rig.clock, 20);
                let receipt = propose_recovering(
                    &mut self.rig.source,
                    &self.rig.clock,
                    20,
                    operation,
                    source_fixture::Source::freeze_command(&intent, 65536).unwrap(),
                );
                assert!(
                    matches!(receipt.outcome,RoutedOutcome::Fenced(f) if f==state.source.as_ref().unwrap().fence)
                );
            }
            _ => unreachable!(),
        }
    }
    fn restart(&mut self) {
        if self.rig.checkpoint {
            for (g, nodes) in [
                (200, &mut self.rig.grandparent),
                (100, &mut self.rig.parent),
                (1, &mut self.rig.child),
            ] {
                compact(nodes, &self.rig.clock, g);
            }
            compact(&mut self.rig.source, &self.rig.clock, 20);
            for (i, t) in self.rig.targets.iter_mut().enumerate() {
                if !t.is_empty() {
                    compact(t, &self.rig.clock, 21 + i as u128);
                }
            }
        }
        for (g, nodes) in [
            (200, &mut self.rig.grandparent),
            (100, &mut self.rig.parent),
            (1, &mut self.rig.child),
        ] {
            let logs = creation::abandon(std::mem::take(nodes), g);
            assert!(logs.values().all(|s| if self.rig.checkpoint {
                s.base_index() > 0
            } else {
                s.base_index() == 0
            }));
        }
        creation::abandon(std::mem::take(&mut self.rig.source), 20);
        for (i, t) in self.rig.targets.iter_mut().enumerate() {
            if !t.is_empty() {
                creation::abandon(std::mem::take(t), 21 + i as u128);
            }
        }
        self.rig.reopen();
        self.check_bindings();
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut cross = Cross::new(protocol, checkpoint);
    for phase in [
        Phase::Reserve,
        Phase::Intent,
        Phase::Stage(21),
        Phase::Stage(22),
        Phase::Fence,
        Phase::Import(21),
        Phase::Import(22),
        Phase::ChildPublication,
        Phase::ParentPublication,
    ] {
        eprintln!("cross-authority insertion {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        assert_eq!(cross.rig.resume_one(), phase);
        let original = cross.observed();
        cross.rig.check_service(&original.0);
        cross.restart();
        let recovered = cross.observed();
        assert_eq!(recovered, original, "recovered cross-authority {phase:?}");
        cross.rig.check_service(&recovered.0);
        cross.retry(phase, &recovered.0);
        assert_eq!(
            cross.observed(),
            original,
            "original retry cross-authority {phase:?}"
        );
    }
    let final_state = cross.observed();
    let intent = final_state.0.intent.as_ref().unwrap().intent.clone();
    let decision = final_state.0.decision.as_ref().unwrap().clone();
    let child_cfg = cross.rig.child[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .state()
        .bootstrap
        .configuration;
    // No metadata/source owner is available for activation or ordinary successor work.
    for (g, nodes) in [
        (200, &mut cross.rig.grandparent),
        (100, &mut cross.rig.parent),
        (1, &mut cross.rig.child),
    ] {
        creation::abandon(std::mem::take(nodes), g);
    }
    creation::abandon(std::mem::take(&mut cross.rig.source), 20);
    let stopped = [200, 100, 1, 20]
        .into_iter()
        .map(|g| (g, durable_files(&cross.rig.root.join(g.to_string()))))
        .collect::<Vec<_>>();
    cross.serve_targets(&intent, &decision, child_cfg);
    for (g, files) in &stopped {
        assert_eq!(&durable_files(&cross.rig.root.join(g.to_string())), files);
    }
    cross.check_bindings();
    // Recovered source remains fenced; no stale-grant service is revived.
    cross.rig.source = open(
        configuration(&cross.rig.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &cross.rig.clock,
        protocol,
        fixture::fresh_source,
    );
    assert_eq!(
        observe(
            &mut cross.rig.source,
            &cross.rig.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1]
            })
        ),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    creation::abandon(std::mem::take(&mut cross.rig.source), 20);
    for (i, t) in cross.rig.targets.iter_mut().enumerate() {
        creation::abandon(std::mem::take(t), 21 + i as u128);
    }
    std::fs::remove_dir_all(cross.rig.root).unwrap();
}
#[test]
fn tcp_cross_authority_insertion_unread_phases_wal() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_cross_authority_insertion_unread_phases_checkpoint() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_cross_authority_insertion_unread_phases_wal() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_cross_authority_insertion_unread_phases_checkpoint() {
    history(NativePeerProtocol::Quic, true);
}
