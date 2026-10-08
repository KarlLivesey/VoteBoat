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
use super::split::{compact, observe};
use super::*;
#[path = "../delegation/fixtures.rs"]
mod fixture;
use voteboat::{
    bucket_counter::{encode_add, BucketOutcome},
    delegation::*,
    transfer::*,
    transfer_publication::*,
    transfer_source::*,
    transfer_target::*,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Reserve,
    Intent,
    Stage(u128),
    Fence,
    Import(u128),
    ChildPublication,
    ParentPublication,
    Activate(u128),
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    root: ResponsibilityManifest,
    parent: ResponsibilityManifest,
    child: ResponsibilityManifest,
    reservation: Option<DelegationReservationStatus>,
    parent_publication: Option<DelegationPublicationStatus>,
    intent: Option<TransferIntentStatus>,
    decision: Option<TransferPublicationStatus>,
    source: Option<SourceFreezeStatus>,
    targets: [Option<TargetStatus>; 2],
}
fn root_metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(200), vec![fixture::grandparent()]).unwrap(),
            DirectoryLimits {
                operations: 2,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
fn parent_metadata() -> LifecycleDirectory {
    fixture::fresh_directory(true, 3)
}
fn child_metadata() -> LifecycleDirectory {
    fixture::fresh_directory(false, 3)
}
fn manifest(
    nodes: &mut [Node<LifecycleDirectory>],
    clock: &Instant,
    g: u128,
    id: ResponsibilityIdentity,
) -> ResponsibilityManifest {
    let DirectoryRead::Manifest(Some(m)) = observe(nodes, clock, g, DirectoryQuery::Manifest(id))
    else {
        panic!("manifest {g}")
    };
    m
}
fn initialize(
    nodes: &mut [Node<LifecycleDirectory>],
    clock: &Instant,
    g: u128,
    grant: ResponsibilityManifest,
) {
    campaign(nodes, clock, g);
    let boot = nodes[0].local().applications[&group(g)]
        .directory()
        .bootstrap_command(65536)
        .unwrap();
    propose_recovering(nodes, clock, g, 1000, boot);
    propose_recovering(
        nodes,
        clock,
        g,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: grant,
        }
        .encode(65536)
        .unwrap(),
    );
}
struct Delegated {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    grandparent: Vec<Node<LifecycleDirectory>>,
    parent: Vec<Node<LifecycleDirectory>>,
    child: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<source_fixture::Source>>,
    targets: [Vec<Node<target_fixture::Target>>; 2],
    cache: NativeManifestCache,
}
impl Delegated {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-delegated-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut rig = Self {
            grandparent: open(
                configuration(&root, 200, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                root_metadata,
            ),
            parent: open(
                configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                parent_metadata,
            ),
            child: open(
                configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                child_metadata,
            ),
            source: open(
                configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                fixture::fresh_source,
            ),
            targets: std::array::from_fn(|_| Vec::new()),
            cache: NativeManifestCache::new(ManifestCacheLimits {
                manifests: 4,
                bytes: 65536,
            })
            .unwrap(),
            root,
            clock,
            protocol,
            checkpoint,
        };
        initialize(
            &mut rig.grandparent,
            &rig.clock,
            200,
            fixture::grandparent(),
        );
        initialize(&mut rig.parent, &rig.clock, 100, fixture::parent());
        initialize(&mut rig.child, &rig.clock, 1, fixture::before());
        campaign(&mut rig.source, &rig.clock, 20);
        propose_recovering(
            &mut rig.source,
            &rig.clock,
            20,
            100,
            fixture::fresh_source().bootstrap_command(65536).unwrap(),
        );
        propose_recovering(
            &mut rig.source,
            &rig.clock,
            20,
            1,
            source_fixture::data(1, 7),
        );
        propose_recovering(
            &mut rig.source,
            &rig.clock,
            20,
            2,
            source_fixture::data(200, 11),
        );
        rig
    }
    fn observed(&mut self) -> Observed {
        let root = manifest(&mut self.grandparent, &self.clock, 200, fixture::id(600));
        let parent = manifest(&mut self.parent, &self.clock, 100, fixture::id(500));
        let child = manifest(
            &mut self.child,
            &self.clock,
            1,
            fixture::before().input().responsibility,
        );
        let DirectoryRead::DelegationReservation(reservation) = observe(
            &mut self.parent,
            &self.clock,
            100,
            DirectoryQuery::DelegationReservation(OperationId::new(400).unwrap()),
        ) else {
            panic!("parent reservation")
        };
        let DirectoryRead::DelegationPublication(parent_publication) = observe(
            &mut self.parent,
            &self.clock,
            100,
            DirectoryQuery::DelegationPublication(OperationId::new(400).unwrap()),
        ) else {
            panic!("parent publication")
        };
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.child,
            &self.clock,
            1,
            DirectoryQuery::Transfer(OperationId::new(200).unwrap()),
        ) else {
            panic!("child intent")
        };
        let DirectoryRead::Publication(decision) = observe(
            &mut self.child,
            &self.clock,
            1,
            DirectoryQuery::Publication(OperationId::new(200).unwrap()),
        ) else {
            panic!("child publication")
        };
        let SourceRead::Freeze(source) =
            observe(&mut self.source, &self.clock, 20, SourceQuery::Freeze)
        else {
            panic!("source fence")
        };
        let targets = std::array::from_fn(|i| {
            if self.targets[i].is_empty() {
                return None;
            }
            let TargetRead::Status(s) = observe(
                &mut self.targets[i],
                &self.clock,
                21 + i as u128,
                TargetQuery::Status,
            ) else {
                panic!("target status")
            };
            Some(s)
        });
        Observed {
            root,
            parent,
            child,
            reservation,
            parent_publication,
            intent,
            decision,
            source,
            targets,
        }
    }
    fn resume_one(&mut self) -> Phase {
        let state = self.observed();
        if state.reservation.is_none() {
            campaign(&mut self.parent, &self.clock, 100);
            let _ = propose_recovering(
                &mut self.parent,
                &self.clock,
                100,
                400,
                fixture::plan().encode(65536).unwrap(),
            );
            return Phase::Reserve;
        }
        let reservation = state.reservation.unwrap();
        let cfg = self.parent[0]
            .local()
            .owner
            .core(group(100))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        let intent = reservation.child_intent(cfg).unwrap();
        if state.intent.is_none() {
            campaign(&mut self.child, &self.clock, 1);
            let _ = propose_recovering(
                &mut self.child,
                &self.clock,
                1,
                200,
                intent.encode(65536).unwrap(),
            );
            return Phase::Intent;
        }
        assert_eq!(state.intent.unwrap().intent, intent);
        for i in 0..2 {
            if state.targets[i]
                .as_ref()
                .is_none_or(|t| t.staged_index.is_none())
            {
                let g = 21 + i as u128;
                if self.targets[i].is_empty() {
                    self.targets[i] = open(
                        configuration(&self.root, g, &[1, 2, 3], NativeOpenMode::Create),
                        &self.clock,
                        self.protocol,
                        || fixture::fresh_target(g, &intent),
                    );
                }
                let bytes = self.targets[i][0].local().applications[&group(g)]
                    .bootstrap_command(65536)
                    .unwrap();
                campaign(&mut self.targets[i], &self.clock, g);
                let _ = propose_recovering(&mut self.targets[i], &self.clock, g, 200, bytes);
                return Phase::Stage(g);
            }
        }
        if state.source.is_none() {
            campaign(&mut self.source, &self.clock, 20);
            let _ = propose_recovering(
                &mut self.source,
                &self.clock,
                20,
                200,
                source_fixture::Source::freeze_command(&intent, 65536).unwrap(),
            );
            return Phase::Fence;
        }
        let frozen = state.source.unwrap();
        let source_cfg = self.source[0]
            .local()
            .owner
            .core(group(20))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        for i in 0..2 {
            if state.targets[i].as_ref().unwrap().imported.is_none() {
                let g = 21 + i as u128;
                let digest = frozen
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest;
                let import = TargetImport::new(
                    OperationId::new(200).unwrap(),
                    intent.clone(),
                    group(g),
                    vec![SourceImport {
                        fence: frozen.fence,
                        configuration: source_cfg,
                        image: self.source[0].local().applications[&group(20)]
                            .export_target(group(g), 65536)
                            .unwrap(),
                        digest,
                    }],
                )
                .unwrap_or_else(|e| panic!("{:?}", e.0));
                let bytes = self.targets[i][0].local().applications[&group(g)]
                    .import_command(&import, 65536)
                    .unwrap();
                campaign(&mut self.targets[i], &self.clock, g);
                let _ = propose_recovering(&mut self.targets[i], &self.clock, g, 200, bytes);
                return Phase::Import(g);
            }
        }
        if state.decision.is_none() {
            let ready = state
                .targets
                .into_iter()
                .enumerate()
                .map(|(i, t)| {
                    let cfg = self.targets[i][0]
                        .local()
                        .owner
                        .core(group(21 + i as u128))
                        .unwrap()
                        .state()
                        .bootstrap
                        .configuration;
                    TargetReadyEvidence::from_status(cfg, t.unwrap())
                        .unwrap_or_else(|e| panic!("{:?}", e.0))
                })
                .collect();
            let publication = TransferPublication::new(
                OperationId::new(200).unwrap(),
                intent,
                vec![SourceFenceEvidence::from_status(source_cfg, frozen)
                    .unwrap_or_else(|e| panic!("{:?}", e.0))],
                ready,
            )
            .unwrap_or_else(|e| panic!("{:?}", e.0));
            campaign(&mut self.child, &self.clock, 1);
            let _ = propose_recovering(
                &mut self.child,
                &self.clock,
                1,
                201,
                publication.encode(65536).unwrap(),
            );
            return Phase::ChildPublication;
        }
        let decision = state.decision.unwrap();
        let child_cfg = self.child[0]
            .local()
            .owner
            .core(group(1))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        if state.parent_publication.is_none() {
            let completion = DelegationCompletion {
                reservation: reservation.operation,
                reservation_index: reservation.index,
                parent_configuration: cfg,
                child_configuration: child_cfg,
                decision,
            };
            campaign(&mut self.parent, &self.clock, 100);
            let _ = propose_recovering(
                &mut self.parent,
                &self.clock,
                100,
                401,
                completion.encode(MAX_DELEGATION_COMPLETION_BYTES).unwrap(),
            );
            return Phase::ParentPublication;
        }
        assert_eq!(
            state.parent_publication.unwrap().completion.decision,
            decision
        );
        for i in 0..2 {
            if state.targets[i].as_ref().unwrap().activated.is_none() {
                let g = 21 + i as u128;
                let bytes = self.targets[i][0].local().applications[&group(g)]
                    .activation_command(
                        &TargetActivation {
                            metadata_configuration: child_cfg,
                            decision: decision.clone(),
                        },
                        65536,
                    )
                    .unwrap();
                campaign(&mut self.targets[i], &self.clock, g);
                let _ = propose_recovering(&mut self.targets[i], &self.clock, g, 200, bytes);
                return Phase::Activate(g);
            }
        }
        Phase::Done
    }
    fn check_service(&mut self, state: &Observed) {
        assert_eq!(state.root, fixture::grandparent());
        assert_eq!(state.parent.input().epoch, fixture::parent().input().epoch);
        assert_eq!(
            state.parent.input().parent,
            fixture::parent().input().parent
        );
        for m in [&state.root, &state.parent, &state.child] {
            self.cache.admit(m.clone()).unwrap();
        }
        let cold = resolve(
            &self.cache,
            &source_fixture::Policy,
            fixture::id(600),
            &[1],
            4,
        );
        if state.decision.is_some() && state.parent_publication.is_none() {
            assert_eq!(cold, Err(RoutingError::WrongChild));
        } else {
            assert_eq!(
                cold.unwrap().group,
                group(if state.parent_publication.is_some() {
                    21
                } else {
                    20
                })
            );
        }
        let source = observe(
            &mut self.source,
            &self.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1],
            }),
        );
        assert_eq!(
            source,
            SourceRead::Data(if state.source.is_some() {
                RoutedRead::Rejected(RoutingError::Fenced)
            } else {
                RoutedRead::Served(7)
            })
        );
        for i in 0..2 {
            if self.targets[i].is_empty() {
                continue;
            }
            let key = if i == 0 { 1 } else { 200 };
            let q = target_query(key);
            let value = observe(&mut self.targets[i], &self.clock, 21 + i as u128, q);
            if state.targets[i].as_ref().unwrap().activated.is_some() {
                assert!(
                    state.source.is_some()
                        && state.decision.is_some()
                        && state.parent_publication.is_some()
                );
                assert_eq!(value, TargetRead::Data(if i == 0 { 7 } else { 11 }));
            } else {
                assert_eq!(value, TargetRead::NotActive);
            }
        }
    }
    fn close_parent(&mut self) {
        if self.checkpoint {
            compact(&mut self.parent, &self.clock, 100);
        }
        close(std::mem::take(&mut self.parent), &self.clock, 100, || {
            drive(&mut self.grandparent, &self.clock, |_| true);
            drive(&mut self.child, &self.clock, |_| true);
            drive(&mut self.source, &self.clock, |_| true);
            for t in &mut self.targets {
                if !t.is_empty() {
                    drive(t, &self.clock, |_| true);
                }
            }
        });
    }
    fn parent_outage(&mut self, frozen: bool) {
        self.close_parent();
        let SourceRead::Data(value) = observe(
            &mut self.source,
            &self.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1],
            }),
        ) else {
            panic!("source while parent unavailable")
        };
        if frozen {
            assert_eq!(value, RoutedRead::Rejected(RoutingError::Fenced));
            for i in 0..2 {
                assert_eq!(
                    observe(
                        &mut self.targets[i],
                        &self.clock,
                        21 + i as u128,
                        target_query(if i == 0 { 1 } else { 200 })
                    ),
                    TargetRead::NotActive
                );
            }
        } else {
            assert_eq!(value, RoutedRead::Served(7));
            let r = propose_recovering(
                &mut self.source,
                &self.clock,
                20,
                1,
                source_fixture::data(1, 7),
            );
            assert!(matches!(r.outcome,RoutedOutcome::Applied(r) if r.duplicate));
        }
        self.parent = open(
            configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            parent_metadata,
        );
    }
    fn close_all(&mut self) {
        if self.checkpoint {
            for (g, nodes) in [(200, &mut self.grandparent), (1, &mut self.child)] {
                if !nodes.is_empty() {
                    compact(nodes, &self.clock, g);
                }
            }
            if !self.source.is_empty() {
                compact(&mut self.source, &self.clock, 20);
            }
            for (i, t) in self.targets.iter_mut().enumerate() {
                if !t.is_empty() {
                    compact(t, &self.clock, 21 + i as u128);
                }
            }
        }
        if !self.parent.is_empty() {
            self.close_parent();
        }
        close(
            std::mem::take(&mut self.grandparent),
            &self.clock,
            200,
            || {
                drive(&mut self.child, &self.clock, |_| true);
                drive(&mut self.source, &self.clock, |_| true);
                for t in &mut self.targets {
                    if !t.is_empty() {
                        drive(t, &self.clock, |_| true);
                    }
                }
            },
        );
        close(std::mem::take(&mut self.child), &self.clock, 1, || {
            drive(&mut self.source, &self.clock, |_| true);
            for t in &mut self.targets {
                if !t.is_empty() {
                    drive(t, &self.clock, |_| true);
                }
            }
        });
        close(std::mem::take(&mut self.source), &self.clock, 20, || {
            for t in &mut self.targets {
                if !t.is_empty() {
                    drive(t, &self.clock, |_| true);
                }
            }
        });
        let left = std::mem::take(&mut self.targets[0]);
        close(left, &self.clock, 21, || {
            if !self.targets[1].is_empty() {
                drive(&mut self.targets[1], &self.clock, |_| true);
            }
        });
        close(std::mem::take(&mut self.targets[1]), &self.clock, 22, || {});
    }
    fn restart(&mut self) {
        self.close_all();
        self.grandparent = open(
            configuration(&self.root, 200, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            root_metadata,
        );
        self.parent = open(
            configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            parent_metadata,
        );
        self.child = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            child_metadata,
        );
        self.source = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            fixture::fresh_source,
        );
        let DirectoryRead::DelegationReservation(reservation) = observe(
            &mut self.parent,
            &self.clock,
            100,
            DirectoryQuery::DelegationReservation(OperationId::new(400).unwrap()),
        ) else {
            panic!("recovered reservation")
        };
        let cfg = self.parent[0]
            .local()
            .owner
            .core(group(100))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        for i in 0..2 {
            let g = 21 + i as u128;
            if self.root.join(format!("{g}/1")).exists() {
                let intent = reservation
                    .as_ref()
                    .expect("explicit target requires committed reservation")
                    .child_intent(cfg)
                    .unwrap();
                self.targets[i] = open(
                    configuration(&self.root, g, &[1, 2, 3], NativeOpenMode::Recover),
                    &self.clock,
                    self.protocol,
                    || fixture::fresh_target(g, &intent),
                );
            }
        }
    }
}
fn target_hint(key: u8) -> RouteHint {
    let mut hint = source_fixture::hint(key);
    hint.group = group(if key < 128 { 21 } else { 22 });
    hint.scope = if key < 128 {
        source_fixture::range(0, 128)
    } else {
        source_fixture::range(128, 256)
    };
    hint.epoch = OwnershipEpoch::new(2).unwrap();
    hint.generation = RouteGeneration::new(2).unwrap();
    hint
}
fn target_query(key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: target_hint(key),
        key: vec![key],
        query: vec![key],
    })
}
fn target_data(key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        target_hint(key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn interrupted(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Delegated::new(protocol, checkpoint);
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
        Phase::Activate(21),
        Phase::Activate(22),
    ] {
        eprintln!("delegated {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        assert_eq!(rig.resume_one(), phase);
        let original = rig.observed();
        rig.check_service(&original);
        if phase == Phase::Reserve {
            rig.parent_outage(false);
        }
        if phase == Phase::ChildPublication {
            rig.parent_outage(true);
        }
        rig.restart();
        let recovered = rig.observed();
        assert_eq!(recovered, original, "recovered delegated phase {phase:?}");
        rig.check_service(&recovered);
    }
    assert_eq!(rig.resume_one(), Phase::Done);
    let final_status = rig.observed();
    rig.close_parent();
    close(
        std::mem::take(&mut rig.grandparent),
        &rig.clock,
        200,
        || {
            for t in &mut rig.targets {
                drive(t, &rig.clock, |_| true);
            }
        },
    );
    close(std::mem::take(&mut rig.child), &rig.clock, 1, || {
        for t in &mut rig.targets {
            drive(t, &rig.clock, |_| true);
        }
    });
    close(std::mem::take(&mut rig.source), &rig.clock, 20, || {
        for t in &mut rig.targets {
            drive(t, &rig.clock, |_| true);
        }
    });
    for (i, key, id, value) in [(0, 1u8, 1, 7), (1, 200u8, 2, 11)] {
        let g = 21 + i as u128;
        campaign(&mut rig.targets[i], &rig.clock, g);
        let TargetOutcome::Applied(retry) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            id,
            target_data(key, value),
        )
        .outcome
        else {
            panic!("target retry")
        };
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, BucketOutcome::Value(value));
        let TargetOutcome::Applied(write) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            100 + id,
            target_data(key, 2),
        )
        .outcome
        else {
            panic!("target write")
        };
        assert_eq!(write.outcome, BucketOutcome::Value(value + 2));
    }
    rig.restart();
    assert_eq!(rig.resume_one(), Phase::Done);
    assert_eq!(rig.observed(), final_status);
    for (i, key, value) in [(0, 1u8, 9), (1, 200u8, 13)] {
        assert_eq!(
            observe(
                &mut rig.targets[i],
                &rig.clock,
                21 + i as u128,
                target_query(key)
            ),
            TargetRead::Data(value)
        );
    }
    rig.close_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_delegated_split_recovers_every_phase_from_wal() {
    interrupted(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_delegated_split_recovers_every_phase_from_checkpoints() {
    interrupted(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_delegated_split_recovers_every_phase_from_wal() {
    interrupted(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_delegated_split_recovers_every_phase_from_checkpoints() {
    interrupted(NativePeerProtocol::Quic, true);
}
