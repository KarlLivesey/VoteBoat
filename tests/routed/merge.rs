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
use voteboat::{
    bucket_counter::BucketOutcome, transfer::*, transfer_publication::*, transfer_source::*,
    transfer_target::*,
};
#[path = "../transfer_merge/fixtures.rs"]
mod fixture;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Intent,
    Stage,
    Fence(u128),
    Import,
    Publish,
    Activate,
    Unavailable(u128),
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Test-only owning, bounded quorum-status ledger.
enum SourceObservation {
    Unavailable,
    Known(Option<SourceFreezeStatus>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    sources: [SourceObservation; 2],
    target: TargetStatus,
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![fixture::before()]).unwrap(),
            DirectoryLimits {
                operations: 3,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
struct Merge {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    parent: Vec<Node<LifecycleDirectory>>,
    sources: [Vec<Node<fixture::Source>>; 2],
    target: Vec<Node<fixture::Target>>,
    right_value: i64,
}
impl Merge {
    fn new(protocol: NativePeerProtocol, checkpoint: bool, collision: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-merge-resume-{}-{protocol:?}-{checkpoint}-{collision}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut rig = Self {
            parent: open(
                configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                metadata,
            ),
            sources: std::array::from_fn(|i| {
                open(
                    configuration(&root, 21 + i as u128, &[1, 2, 3], NativeOpenMode::Create),
                    &clock,
                    protocol,
                    || fixture::source(21 + i as u128),
                )
            }),
            target: open(
                configuration(&root, 23, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                fixture::target,
            ),
            root,
            clock,
            protocol,
            checkpoint,
            right_value: 11,
        };
        campaign(&mut rig.parent, &rig.clock, 1);
        propose_recovering(
            &mut rig.parent,
            &rig.clock,
            1,
            1000,
            metadata().directory().bootstrap_command(65536).unwrap(),
        );
        propose_recovering(
            &mut rig.parent,
            &rig.clock,
            1,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: fixture::before(),
            }
            .encode(32768)
            .unwrap(),
        );
        for (i, key, id, value) in [(0, 1, 1, 7), (1, 200, if collision { 1 } else { 2 }, 11)] {
            let g = 21 + i as u128;
            campaign(&mut rig.sources[i], &rig.clock, g);
            propose_recovering(
                &mut rig.sources[i],
                &rig.clock,
                g,
                100,
                fixture::source(g).bootstrap_command(65536).unwrap(),
            );
            propose_recovering(
                &mut rig.sources[i],
                &rig.clock,
                g,
                id,
                fixture::data(key, value, false),
            );
        }
        rig
    }
    fn observed(&mut self) -> Observed {
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Transfer(OperationId::new(200).unwrap()),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(publication) = observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Publication(OperationId::new(200).unwrap()),
        ) else {
            panic!("publication")
        };
        let sources = std::array::from_fn(|i| {
            if self.sources[i].is_empty() {
                return SourceObservation::Unavailable;
            }
            let SourceRead::Freeze(status) = observe(
                &mut self.sources[i],
                &self.clock,
                21 + i as u128,
                SourceQuery::Freeze,
            ) else {
                panic!("source")
            };
            SourceObservation::Known(status)
        });
        let TargetRead::Status(target) =
            observe(&mut self.target, &self.clock, 23, TargetQuery::Status)
        else {
            panic!("target")
        };
        Observed {
            intent,
            publication,
            sources,
            target,
        }
    }
    fn import_command(&self, state: &Observed) -> Vec<u8> {
        let images = state
            .sources
            .iter()
            .enumerate()
            .map(|(i, observation)| {
                let SourceObservation::Known(Some(status)) = observation else {
                    panic!("required source fence")
                };
                let configuration = self.sources[i][0]
                    .local()
                    .owner
                    .core(group(21 + i as u128))
                    .unwrap()
                    .state()
                    .bootstrap
                    .configuration;
                fixture::image(
                    &self.sources[i][0].local().applications[&group(21 + i as u128)],
                    status,
                    configuration,
                )
            })
            .collect();
        let import = TargetImport::new(
            OperationId::new(200).unwrap(),
            fixture::intent(),
            group(23),
            images,
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        self.target[0].local().applications[&group(23)]
            .import_command(&import, 65536)
            .unwrap()
    }
    fn resume_one(&mut self) -> Phase {
        let state = self.observed();
        if state.intent.is_none() {
            campaign(&mut self.parent, &self.clock, 1);
            let _ = propose_recovering(
                &mut self.parent,
                &self.clock,
                1,
                200,
                fixture::intent().encode(32768).unwrap(),
            );
            return Phase::Intent;
        }
        assert_eq!(state.intent.as_ref().unwrap().intent, fixture::intent());
        if state.target.staged_index.is_none() {
            let bytes = self.target[0].local().applications[&group(23)]
                .bootstrap_command(65536)
                .unwrap();
            campaign(&mut self.target, &self.clock, 23);
            let _ = propose_recovering(&mut self.target, &self.clock, 23, 200, bytes);
            return Phase::Stage;
        }
        if state.target.activated.is_some() {
            assert!(state.publication.is_some());
            return Phase::Done;
        }
        if let Some(decision) = state.publication {
            let activation = TargetActivation {
                metadata_configuration: self.parent[0]
                    .local()
                    .owner
                    .core(group(1))
                    .unwrap()
                    .state()
                    .bootstrap
                    .configuration,
                decision,
            };
            let bytes = self.target[0].local().applications[&group(23)]
                .activation_command(&activation, 65536)
                .unwrap();
            campaign(&mut self.target, &self.clock, 23);
            let _ = propose_recovering(&mut self.target, &self.clock, 23, 200, bytes);
            return Phase::Activate;
        }
        for (i, source) in state.sources.iter().enumerate() {
            let g = 21 + i as u128;
            match source {
                SourceObservation::Unavailable => return Phase::Unavailable(g),
                SourceObservation::Known(None) => {
                    campaign(&mut self.sources[i], &self.clock, g);
                    let _ = propose_recovering(
                        &mut self.sources[i],
                        &self.clock,
                        g,
                        200,
                        fixture::freeze(),
                    );
                    return Phase::Fence(g);
                }
                SourceObservation::Known(Some(status)) => {
                    assert_eq!(status.intent, fixture::intent())
                }
            }
        }
        if state.target.imported.is_none() {
            let bytes = self.import_command(&state);
            campaign(&mut self.target, &self.clock, 23);
            let _ = propose_recovering(&mut self.target, &self.clock, 23, 200, bytes);
            return Phase::Import;
        }
        if state.publication.is_none() {
            return self.publish(state);
        }
        unreachable!("a retained publication activates before querying source availability")
    }
    fn publish(&mut self, state: Observed) -> Phase {
        let sources = state
            .sources
            .into_iter()
            .enumerate()
            .map(|(i, source)| {
                let SourceObservation::Known(Some(status)) = source else {
                    panic!("source fence")
                };
                let configuration = self.sources[i][0]
                    .local()
                    .owner
                    .core(group(21 + i as u128))
                    .unwrap()
                    .state()
                    .bootstrap
                    .configuration;
                SourceFenceEvidence::from_status(configuration, status)
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect();
        let target = TargetReadyEvidence::from_status(
            self.target[0]
                .local()
                .owner
                .core(group(23))
                .unwrap()
                .state()
                .bootstrap
                .configuration,
            state.target,
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let publication = TransferPublication::new(
            OperationId::new(200).unwrap(),
            fixture::intent(),
            sources,
            vec![target],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        campaign(&mut self.parent, &self.clock, 1);
        let _ = propose_recovering(
            &mut self.parent,
            &self.clock,
            1,
            201,
            publication.encode(65536).unwrap(),
        );
        Phase::Publish
    }
    fn close_all(&mut self) {
        if self.checkpoint {
            compact(&mut self.parent, &self.clock, 1);
            for i in 0..2 {
                compact(&mut self.sources[i], &self.clock, 21 + i as u128);
            }
            compact(&mut self.target, &self.clock, 23);
        }
        close(std::mem::take(&mut self.parent), &self.clock, 1, || {
            for source in &mut self.sources {
                drive(source, &self.clock, |_| true);
            }
            drive(&mut self.target, &self.clock, |_| true);
        });
        let left = std::mem::take(&mut self.sources[0]);
        close(left, &self.clock, 21, || {
            drive(&mut self.sources[1], &self.clock, |_| true);
            drive(&mut self.target, &self.clock, |_| true);
        });
        close(
            std::mem::take(&mut self.sources[1]),
            &self.clock,
            22,
            || {
                drive(&mut self.target, &self.clock, |_| true);
            },
        );
        close(std::mem::take(&mut self.target), &self.clock, 23, || {});
    }
    fn restart(&mut self) {
        self.close_all();
        self.parent = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            metadata,
        );
        self.sources = std::array::from_fn(|i| {
            open(
                configuration(
                    &self.root,
                    21 + i as u128,
                    &[1, 2, 3],
                    NativeOpenMode::Recover,
                ),
                &self.clock,
                self.protocol,
                || fixture::source(21 + i as u128),
            )
        });
        self.target = open(
            configuration(&self.root, 23, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            fixture::target,
        );
    }
    fn serving(&mut self, state: &Observed) {
        for (i, key, value) in [(0, 1u8, 7), (1, 200u8, self.right_value)] {
            let g = 21 + i as u128;
            if state.sources[i] == SourceObservation::Unavailable {
                assert!(self.sources[i].is_empty());
                continue;
            }
            let result = observe(
                &mut self.sources[i],
                &self.clock,
                g,
                SourceQuery::Data(RoutedQuery {
                    hint: fixture::hint(key, false),
                    key: vec![key],
                    query: vec![key],
                }),
            );
            if matches!(state.sources[i], SourceObservation::Known(Some(_))) {
                assert_eq!(
                    result,
                    SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
                );
                assert!(self.sources[i][0]
                    .propose(ClientRequest {
                        group: group(g),
                        operation: OperationId::new(900).unwrap(),
                        bytes: fixture::data(key, 1, false)
                    })
                    .is_err());
            } else {
                assert_eq!(result, SourceRead::Data(RoutedRead::Served(value)));
            }
        }
        for (key, value) in [(1u8, 7), (200u8, self.right_value)] {
            let result = observe(
                &mut self.target,
                &self.clock,
                23,
                TargetQuery::Data(RoutedQuery {
                    hint: fixture::hint(key, true),
                    key: vec![key],
                    query: vec![key],
                }),
            );
            if state.target.activated.is_some() {
                assert!(state.target.imported.is_some());
                assert!(state.publication.is_some());
                assert_eq!(result, TargetRead::Data(value));
            } else {
                assert_eq!(result, TargetRead::NotActive);
            }
        }
    }
    fn pause_right(&mut self, original: &Observed) {
        assert!(matches!(
            original.sources[0],
            SourceObservation::Known(Some(_))
        ));
        assert_eq!(original.sources[1], SourceObservation::Known(None));
        close(
            std::mem::take(&mut self.sources[1]),
            &self.clock,
            22,
            || {
                drive(&mut self.parent, &self.clock, |_| true);
                drive(&mut self.sources[0], &self.clock, |_| true);
                drive(&mut self.target, &self.clock, |_| true);
            },
        );
        assert_eq!(self.resume_one(), Phase::Unavailable(22));
        let paused = self.observed();
        assert_eq!(paused.sources[0], original.sources[0]);
        assert_eq!(paused.sources[1], SourceObservation::Unavailable);
        assert_eq!(paused.target, original.target);
        assert!(paused.publication.is_none());
        self.serving(&paused);
        assert_eq!(
            observe(
                &mut self.parent,
                &self.clock,
                1,
                DirectoryQuery::Manifest(fixture::before().input().responsibility)
            ),
            DirectoryRead::Manifest(Some(fixture::before()))
        );
        self.sources[1] = open(
            configuration(&self.root, 22, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || fixture::source(22),
        );
        campaign(&mut self.sources[1], &self.clock, 22);
        let receipt = propose_recovering(
            &mut self.sources[1],
            &self.clock,
            22,
            3,
            fixture::data(200, 3, false),
        );
        let RoutedOutcome::Applied(r) = receipt.outcome else {
            panic!("unfrozen source write")
        };
        assert_eq!(r.outcome, BucketOutcome::Value(14));
        self.right_value = 14;
        let recovered = self.observed();
        assert_eq!(recovered.sources[0], original.sources[0]);
        assert_eq!(recovered.sources[1], SourceObservation::Known(None));
        self.serving(&recovered);
    }
}
fn reject_colliding_import(rig: &mut Merge) {
    let before = rig.observed();
    let bytes = rig.import_command(&before);
    campaign(&mut rig.target, &rig.clock, 23);
    let checkpoint = rig.target[0].local().applications[&group(23)]
        .checkpoint(200000)
        .unwrap();
    let rejected = rig.target[0]
        .propose(ClientRequest {
            group: group(23),
            operation: OperationId::new(200).unwrap(),
            bytes,
        })
        .unwrap_err();
    assert!(matches!(rejected.reason, ClientError::Application(_)));
    assert_eq!(
        rig.target[0].local().applications[&group(23)]
            .checkpoint(200000)
            .unwrap(),
        checkpoint
    );
    assert!(
        before.target.imported.is_none()
            && before.target.activated.is_none()
            && before.publication.is_none()
    );
    rig.restart();
    let recovered = rig.observed();
    assert_eq!(recovered, before);
    rig.serving(&recovered);
    assert!(
        TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), recovered.target)
            .is_err()
    );
}
fn write_merged_service(rig: &mut Merge) {
    close(std::mem::take(&mut rig.parent), &rig.clock, 1, || {
        for source in &mut rig.sources {
            drive(source, &rig.clock, |_| true);
        }
        drive(&mut rig.target, &rig.clock, |_| true);
    });
    for i in 0..2 {
        close(
            std::mem::take(&mut rig.sources[i]),
            &rig.clock,
            21 + i as u128,
            || {
                drive(&mut rig.target, &rig.clock, |_| true);
            },
        );
    }
    campaign(&mut rig.target, &rig.clock, 23);
    for (id, key, delta, original_value) in [(1, 1, 7, 7), (2, 200, 11, 11), (3, 200, 3, 14)] {
        let TargetOutcome::Applied(r) = propose_recovering(
            &mut rig.target,
            &rig.clock,
            23,
            id,
            fixture::data(key, delta, true),
        )
        .outcome
        else {
            panic!("imported retry")
        };
        assert!(r.duplicate);
        assert_eq!(r.outcome, BucketOutcome::Value(original_value));
    }
    for (id, key, value) in [(4, 1, 9), (5, 200, 16)] {
        let TargetOutcome::Applied(r) = propose_recovering(
            &mut rig.target,
            &rig.clock,
            23,
            id,
            fixture::data(key, 2, true),
        )
        .outcome
        else {
            panic!("write")
        };
        assert_eq!(r.outcome, BucketOutcome::Value(value));
    }
}
fn verify_merged_recovery(rig: &mut Merge) {
    for (key, value) in [(1, 9), (200, 16)] {
        assert_eq!(
            observe(
                &mut rig.target,
                &rig.clock,
                23,
                TargetQuery::Data(RoutedQuery {
                    hint: fixture::hint(key, true),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Data(value)
        );
        assert!(matches!(
            observe(
                &mut rig.target,
                &rig.clock,
                23,
                TargetQuery::Data(RoutedQuery {
                    hint: fixture::hint(key, false),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Rejected(_)
        ));
    }
    assert!(rig.target.iter().all(|n| n.local().applications[&group(23)]
        .application()
        .outbox()
        .count()
        == 5));
    let state = rig.observed();
    for (i, key) in [(0, 1), (1, 200)] {
        assert!(matches!(
            state.sources[i],
            SourceObservation::Known(Some(_))
        ));
        assert_eq!(
            observe(
                &mut rig.sources[i],
                &rig.clock,
                21 + i as u128,
                SourceQuery::Data(RoutedQuery {
                    hint: fixture::hint(key, false),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
        );
    }
}
fn interrupted(protocol: NativePeerProtocol, checkpoint: bool, collision: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Merge::new(protocol, checkpoint, collision);
    let phases = [
        Phase::Intent,
        Phase::Stage,
        Phase::Fence(21),
        Phase::Fence(22),
        Phase::Import,
        Phase::Publish,
        Phase::Activate,
    ];
    for phase in phases {
        eprintln!(
            "merge {protocol:?} checkpoint={checkpoint} collision={collision} phase={phase:?}"
        );
        if collision && phase == Phase::Import {
            reject_colliding_import(&mut rig);
            rig.close_all();
            std::fs::remove_dir_all(rig.root).unwrap();
            return;
        }
        let original_sources = if phase == Phase::Activate {
            let previous = rig.observed();
            for i in 0..2 {
                close(
                    std::mem::take(&mut rig.sources[i]),
                    &rig.clock,
                    21 + i as u128,
                    || {
                        drive(&mut rig.parent, &rig.clock, |_| true);
                        drive(&mut rig.target, &rig.clock, |_| true);
                    },
                );
            }
            Some(previous.sources)
        } else {
            None
        };
        assert_eq!(rig.resume_one(), phase);
        let mut before = rig.observed();
        rig.serving(&before);
        rig.restart();
        let recovered = rig.observed();
        if let Some(sources) = original_sources {
            assert!(before
                .sources
                .iter()
                .all(|source| *source == SourceObservation::Unavailable));
            before.sources = sources;
        }
        assert_eq!(recovered, before, "recovered {phase:?}");
        rig.serving(&recovered);
        if phase == Phase::Fence(21) {
            rig.pause_right(&recovered);
        }
    }
    assert_eq!(rig.resume_one(), Phase::Done);
    let original = rig.observed();
    write_merged_service(&mut rig);
    rig.restart();
    assert_eq!(rig.resume_one(), Phase::Done);
    assert_eq!(rig.observed(), original);
    verify_merged_recovery(&mut rig);
    rig.close_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_merge_resumes_partial_fences_and_every_phase_from_wal_status() {
    interrupted(NativePeerProtocol::TcpTls, false, false);
}
#[test]
fn tcp_merge_resumes_partial_fences_and_every_phase_from_checkpoint_status() {
    interrupted(NativePeerProtocol::TcpTls, true, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_merge_resumes_partial_fences_and_every_phase_from_wal_status() {
    interrupted(NativePeerProtocol::Quic, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_merge_resumes_partial_fences_and_every_phase_from_checkpoint_status() {
    interrupted(NativePeerProtocol::Quic, true, false);
}
#[test]
fn tcp_merge_collision_keeps_sources_fenced_and_target_inactive_after_reopen() {
    interrupted(NativePeerProtocol::TcpTls, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_merge_collision_keeps_sources_fenced_and_target_inactive_after_reopen() {
    interrupted(NativePeerProtocol::Quic, false, true);
}
