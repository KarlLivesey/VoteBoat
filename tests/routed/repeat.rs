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
use voteboat::bucket_counter::BucketCounter;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LaterPhase {
    Intent,
    Stage,
    Fence(usize),
    Import,
    Publish,
    Activate,
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct LaterObserved {
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    old_targets: [TargetStatus; 2],
    fences: [Option<SourceFreezeStatus>; 2],
    target: TargetStatus,
}
fn later_intent() -> TransferIntent {
    let before = source_fixture::intent().after().clone();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    after.execution = ExecutionMode::Single(group(23));
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
fn later_metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 8,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
fn merged_application() -> target_fixture::Target {
    TransferTarget::new(
        group(23),
        OperationId::new(300).unwrap(),
        later_intent(),
        BucketCounter::new(
            source_fixture::range(0, 256),
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn later_hint(key: u8, merged: bool) -> RouteHint {
    let mut h = source_fixture::hint(key);
    h.epoch = OwnershipEpoch::new(if merged { 3 } else { 2 }).unwrap();
    h.generation = RouteGeneration::new(if merged { 3 } else { 2 }).unwrap();
    h.group = group(if merged {
        23
    } else if key < 128 {
        21
    } else {
        22
    });
    if !merged {
        h.scope = if key < 128 {
            source_fixture::range(0, 128)
        } else {
            source_fixture::range(128, 256)
        };
    }
    h
}
fn later_data(key: u8, delta: i64, merged: bool) -> Vec<u8> {
    encode_routed(
        later_hint(key, merged),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn later_query(key: u8, merged: bool) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: later_hint(key, merged),
        key: vec![key],
        query: vec![key],
    })
}
struct Repeated {
    split: Split,
    merged: Vec<Node<target_fixture::Target>>,
}
impl Repeated {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let mut split = Split::with_metadata(protocol, checkpoint, later_metadata);
        for phase in [
            Phase::Intent,
            Phase::Stage(21),
            Phase::Stage(22),
            Phase::Fence,
            Phase::Import(21),
            Phase::Import(22),
            Phase::Publish,
            Phase::Activate(21),
            Phase::Activate(22),
        ] {
            assert_eq!(split.resume_one(), phase);
        }
        assert_eq!(split.resume_one(), Phase::Done);
        for (i, key, id, delta, value) in [(0, 1, 3, 2, 9), (1, 200, 4, 3, 14)] {
            campaign(&mut split.targets[i], &split.clock, 21 + i as u128);
            let TargetOutcome::Applied(r) = propose_recovering(
                &mut split.targets[i],
                &split.clock,
                21 + i as u128,
                id,
                later_data(key, delta, false),
            )
            .outcome
            else {
                panic!("active child write")
            };
            assert_eq!(r.outcome, BucketOutcome::Value(value));
        }
        let merged = open(
            configuration(&split.root, 23, &[1, 2, 3], NativeOpenMode::Create),
            &split.clock,
            protocol,
            merged_application,
        );
        Self { split, merged }
    }
    fn observed(&mut self) -> LaterObserved {
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.split.parent,
            &self.split.clock,
            1,
            DirectoryQuery::Transfer(OperationId::new(300).unwrap()),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(publication) = observe(
            &mut self.split.parent,
            &self.split.clock,
            1,
            DirectoryQuery::Publication(OperationId::new(300).unwrap()),
        ) else {
            panic!("publication")
        };
        let old_targets = std::array::from_fn(|i| {
            let TargetRead::Status(s) = observe(
                &mut self.split.targets[i],
                &self.split.clock,
                21 + i as u128,
                TargetQuery::Status,
            ) else {
                panic!("original target status")
            };
            s
        });
        let fences = std::array::from_fn(|i| {
            let TargetRead::Freeze(s) = observe(
                &mut self.split.targets[i],
                &self.split.clock,
                21 + i as u128,
                TargetQuery::Freeze,
            ) else {
                panic!("later fence")
            };
            s
        });
        let TargetRead::Status(target) =
            observe(&mut self.merged, &self.split.clock, 23, TargetQuery::Status)
        else {
            panic!("merged target")
        };
        LaterObserved {
            intent,
            publication,
            old_targets,
            fences,
            target,
        }
    }
    fn resume_one(&mut self) -> LaterPhase {
        let state = self.observed();
        let id = OperationId::new(300).unwrap();
        let intent = later_intent();
        if state.intent.is_none() {
            campaign(&mut self.split.parent, &self.split.clock, 1);
            let _ = propose_recovering(
                &mut self.split.parent,
                &self.split.clock,
                1,
                300,
                intent.encode(32768).unwrap(),
            );
            return LaterPhase::Intent;
        }
        assert_eq!(state.intent.as_ref().unwrap().intent, intent);
        if state.target.staged_index.is_none() {
            let bytes = self.merged[0].local().applications[&group(23)]
                .bootstrap_command(65536)
                .unwrap();
            campaign(&mut self.merged, &self.split.clock, 23);
            let _ = propose_recovering(&mut self.merged, &self.split.clock, 23, 300, bytes);
            return LaterPhase::Stage;
        }
        if state.target.activated.is_some() {
            assert!(state.publication.is_some());
            return LaterPhase::Done;
        }
        if let Some(decision) = state.publication {
            let configuration = self.split.parent[0]
                .local()
                .owner
                .core(group(1))
                .unwrap()
                .state()
                .bootstrap
                .configuration;
            let activation = TargetActivation {
                metadata_configuration: configuration,
                decision,
            };
            let bytes = self.merged[0].local().applications[&group(23)]
                .activation_command(&activation, 65536)
                .unwrap();
            campaign(&mut self.merged, &self.split.clock, 23);
            let _ = propose_recovering(&mut self.merged, &self.split.clock, 23, 300, bytes);
            return LaterPhase::Activate;
        }
        for i in 0..2 {
            if state.fences[i].is_none() {
                let g = 21 + i as u128;
                let bytes = self.split.targets[i][0].local().applications[&group(g)]
                    .freeze_command(&intent, 65536, 65536)
                    .unwrap();
                campaign(&mut self.split.targets[i], &self.split.clock, g);
                let _ = propose_recovering(
                    &mut self.split.targets[i],
                    &self.split.clock,
                    g,
                    300,
                    bytes,
                );
                return LaterPhase::Fence(i);
            }
        }
        if state.target.imported.is_none() {
            let sources = state
                .fences
                .iter()
                .enumerate()
                .map(|(i, status)| {
                    let status = status.as_ref().unwrap();
                    let g = 21 + i as u128;
                    let configuration = self.split.targets[i][0]
                        .local()
                        .owner
                        .core(group(g))
                        .unwrap()
                        .state()
                        .bootstrap
                        .configuration;
                    let image = self.split.targets[i][0].local().applications[&group(g)]
                        .export_target(group(23), 65536)
                        .unwrap();
                    SourceImport {
                        fence: status.fence,
                        configuration,
                        image,
                        digest: status.exports[0].digest,
                    }
                })
                .collect();
            let import = TargetImport::new(id, intent, group(23), sources)
                .unwrap_or_else(|e| panic!("{:?}", e.0));
            let bytes = self.merged[0].local().applications[&group(23)]
                .import_command(&import, 65536)
                .unwrap();
            campaign(&mut self.merged, &self.split.clock, 23);
            let _ = propose_recovering(&mut self.merged, &self.split.clock, 23, 300, bytes);
            return LaterPhase::Import;
        }
        let sources = state
            .fences
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let configuration = self.split.targets[i][0]
                    .local()
                    .owner
                    .core(group(21 + i as u128))
                    .unwrap()
                    .state()
                    .bootstrap
                    .configuration;
                SourceFenceEvidence::from_status(configuration, s.unwrap())
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect();
        let configuration = self.merged[0]
            .local()
            .owner
            .core(group(23))
            .unwrap()
            .state()
            .bootstrap
            .configuration;
        let target = TargetReadyEvidence::from_status(configuration, state.target)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        let publication = TransferPublication::new(id, intent, sources, vec![target])
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        campaign(&mut self.split.parent, &self.split.clock, 1);
        let _ = propose_recovering(
            &mut self.split.parent,
            &self.split.clock,
            1,
            301,
            publication.encode(65536).unwrap(),
        );
        LaterPhase::Publish
    }
    fn close_merged(&mut self) {
        if self.split.checkpoint {
            compact(&mut self.merged, &self.split.clock, 23);
        }
        close(
            std::mem::take(&mut self.merged),
            &self.split.clock,
            23,
            || {
                drive(&mut self.split.parent, &self.split.clock, |_| true);
                drive(&mut self.split.source, &self.split.clock, |_| true);
                for target in &mut self.split.targets {
                    drive(target, &self.split.clock, |_| true);
                }
            },
        );
    }
    fn restart(&mut self) {
        self.close_merged();
        self.split.restart();
        self.merged = open(
            configuration(&self.split.root, 23, &[1, 2, 3], NativeOpenMode::Recover),
            &self.split.clock,
            self.split.protocol,
            merged_application,
        );
    }
    fn serving(&mut self, state: &LaterObserved) {
        for (i, key, value) in [(0, 1, 9), (1, 200, 14)] {
            let g = 21 + i as u128;
            let result = observe(
                &mut self.split.targets[i],
                &self.split.clock,
                g,
                later_query(key, false),
            );
            if let Some(status) = &state.fences[i] {
                assert_eq!(result, TargetRead::Rejected(RoutingError::Fenced));
                assert!(self.split.targets[i][0]
                    .propose(ClientRequest {
                        group: group(g),
                        operation: OperationId::new(999).unwrap(),
                        bytes: later_data(key, 1, false)
                    })
                    .is_err());
                let app = &self.split.targets[i][0].local().applications[&group(g)];
                assert_eq!(app.application().applied_index(), status.fence.index);
                assert!(app.applied_index() >= status.fence.index);
            } else {
                assert_eq!(result, TargetRead::Data(value));
            }
            let result = observe(
                &mut self.merged,
                &self.split.clock,
                23,
                later_query(key, true),
            );
            if state.target.activated.is_some() {
                assert_eq!(result, TargetRead::Data(value));
            } else {
                assert_eq!(result, TargetRead::NotActive);
            }
        }
    }
}
fn repeated(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Repeated::new(protocol, checkpoint);
    let original = rig.observed().old_targets;
    for phase in [
        LaterPhase::Intent,
        LaterPhase::Stage,
        LaterPhase::Fence(0),
        LaterPhase::Fence(1),
        LaterPhase::Import,
        LaterPhase::Publish,
        LaterPhase::Activate,
    ] {
        eprintln!("repeat {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        assert_eq!(rig.resume_one(), phase);
        let before = rig.observed();
        assert_eq!(before.old_targets, original);
        rig.serving(&before);
        rig.restart();
        let after = rig.observed();
        assert_eq!(after, before, "recovered {phase:?}");
        rig.serving(&after);
    }
    assert_eq!(rig.resume_one(), LaterPhase::Done);
    let original = rig.observed();
    rig.split.close_all();
    campaign(&mut rig.merged, &rig.split.clock, 23);
    for (id, key, delta, value, duplicate) in [
        (1, 1, 7, 7, true),
        (2, 200, 11, 11, true),
        (3, 1, 2, 9, true),
        (4, 200, 3, 14, true),
        (5, 1, 3, 12, false),
        (6, 200, 4, 18, false),
    ] {
        let TargetOutcome::Applied(r) = propose_recovering(
            &mut rig.merged,
            &rig.split.clock,
            23,
            id,
            later_data(key, delta, true),
        )
        .outcome
        else {
            panic!("merged service")
        };
        assert_eq!(r.outcome, BucketOutcome::Value(value));
        assert_eq!(r.duplicate, duplicate);
    }
    rig.restart();
    assert_eq!(rig.observed(), original);
    for (key, value) in [(1, 12), (200, 18)] {
        assert_eq!(
            observe(
                &mut rig.merged,
                &rig.split.clock,
                23,
                later_query(key, true)
            ),
            TargetRead::Data(value)
        );
    }
    for i in 0..2 {
        assert_eq!(
            observe(
                &mut rig.split.targets[i],
                &rig.split.clock,
                21 + i as u128,
                later_query(if i == 0 { 1 } else { 200 }, false)
            ),
            TargetRead::Rejected(RoutingError::Fenced)
        );
    }
    assert_eq!(
        rig.merged[0].local().applications[&group(23)]
            .application()
            .outbox()
            .count(),
        6
    );
    assert_eq!(
        observe(
            &mut rig.split.source,
            &rig.split.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1]
            })
        ),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    rig.close_merged();
    rig.split.close_all();
    std::fs::remove_dir_all(rig.split.root).unwrap();
}
#[test]
fn tcp_repeated_transfer_preserves_activated_sources_through_wal_reopen() {
    repeated(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_repeated_transfer_preserves_activated_sources_through_checkpoint_reopen() {
    repeated(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_repeated_transfer_preserves_activated_sources_through_wal_reopen() {
    repeated(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_repeated_transfer_preserves_activated_sources_through_checkpoint_reopen() {
    repeated(NativePeerProtocol::Quic, true);
}
